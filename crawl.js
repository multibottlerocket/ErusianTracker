import fs from "node:fs";
import path from "node:path";
import * as cheerio from "cheerio";

/**
 * Safe defaults tuned to avoid Substack rate limits:
 * - MAX_POSTS default 600 (raise gradually once stable)
 * - SLEEP_MS default 1500ms + jitter
 * - aggressive 429 handling with Retry-After + exponential backoff
 */
const SUBSTACK_URL = (process.env.SUBSTACK_URL || "https://www.astralcodexten.com").replace(/\/+$/, "");
const USERNAME_RAW = process.env.USERNAME || "Erusian";

// Safe values
const MAX_POSTS = Math.max(1, Math.min(Number(process.env.MAX_POSTS || "600"), 5000));
const SLEEP_MS = Math.max(250, Math.min(Number(process.env.SLEEP_MS || "1500"), 5000));

const OUT_PATH = path.join("docs", "data", "comments.json");

const wantedName = USERNAME_RAW.trim();
const wantedHandle = USERNAME_RAW.trim().toLowerCase().replace(/^@/, "");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function politePause(baseMs = SLEEP_MS) {
  // Jitter helps avoid looking like a perfect clockwork bot.
  const jitter = Math.floor(Math.random() * 350); // 0–350ms
  await sleep(baseMs + jitter);
}

function ua() {
  // A stable UA string reduces some anti-bot false positives vs default undici UA.
  return "ErusianTracker/1.1 (+https://github.com/)";
}

async function fetchWithRetry(url, { accept, kind }) {
  const MAX_RETRIES = 8;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const r = await fetch(url, {
      headers: {
        accept,
        "user-agent": ua()
      }
    });

    if (r.ok) return r;

    // Rate limit
    if (r.status === 429) {
      const retryAfter = r.headers.get("retry-after");
      const retryAfterMs = retryAfter && /^\d+$/.test(retryAfter) ? Number(retryAfter) * 1000 : 0;

      const backoffMs = Math.min(90_000, 1000 * Math.pow(2, attempt)); // caps at 90s
      const jitterMs = Math.floor(Math.random() * 800);
      const waitMs = Math.max(retryAfterMs, backoffMs) + jitterMs;

      console.log(
        `429 rate limit (${kind}) for ${url} — waiting ${waitMs}ms (attempt ${attempt + 1}/${MAX_RETRIES})`
      );
      await sleep(waitMs);
      continue;
    }

    // Transient server errors
    if (r.status >= 500 && r.status < 600 && attempt < MAX_RETRIES) {
      const backoffMs = Math.min(45_000, 750 * Math.pow(2, attempt));
      const jitterMs = Math.floor(Math.random() * 500);
      const waitMs = backoffMs + jitterMs;
      console.log(`HTTP ${r.status} (${kind}) for ${url} — retrying in ${waitMs}ms`);
      await sleep(waitMs);
      continue;
    }

    // Everything else: fail
    const text = await r.text().catch(() => "");
    throw new Error(`HTTP ${r.status} (${kind}) for ${url}${text ? ` — ${text.slice(0, 200)}` : ""}`);
  }

  throw new Error(`Retries exhausted (${kind}) for ${url}`);
}

async function fetchJson(url) {
  const r = await fetchWithRetry(url, {
    accept: "application/json,text/plain,*/*",
    kind: "json"
  });
  return r.json();
}

async function fetchHtml(url) {
  const r = await fetchWithRetry(url, {
    accept: "text/html,*/*",
    kind: "html"
  });
  return r.text();
}

// Substack commonly exposes an archive JSON feed at /api/v1/archive?limit=...&offset=...
// (Unofficial / subject to change.)
async function listPostsFromArchive(baseUrl, maxPosts) {
  const endpoint = `${baseUrl}/api/v1/archive`;
  const limit = 12;
  let offset = 0;
  const posts = [];

  while (posts.length < maxPosts) {
    const url = `${endpoint}?limit=${limit}&offset=${offset}`;
    const chunk = await fetchJson(url);

    if (!Array.isArray(chunk) || chunk.length === 0) break;

    for (const item of chunk) {
      const u = item?.canonical_url ? String(item.canonical_url) : null;
      if (!u) continue;

      const title = item?.title ? String(item.title) : null;

      // Some substacks give ISO strings, some give seconds; best-effort.
      let postDateMs = null;
      if (item?.post_date) {
        const ms = Date.parse(String(item.post_date));
        postDateMs = Number.isFinite(ms) ? ms : null;
      } else if (item?.published_at) {
        const ms = Number(item.published_at) * 1000;
        postDateMs = Number.isFinite(ms) ? ms : null;
      }

      posts.push({ url: u, title, postDateMs });
      if (posts.length >= maxPosts) break;
    }

    offset += chunk.length;
    // Be nice to the archive feed specifically
    await politePause(SLEEP_MS);
  }

  return posts;
}

function parseHandleFromHref(href) {
  try {
    const u = new URL(href);
    const m = u.pathname.match(/^\/@([^/]+)/);
    return m ? m[1] : null;
  } catch {
    return null;
  }
}

function normalizeText(s) {
  return (s || "").replace(/\s+/g, " ").trim();
}

function extractCommentText($, el) {
  const preferred = $(el)
    .find(".markup, [data-testid*=markup], [class*=markup], [class*=content], [class*=body]")
    .first();
  const t1 = normalizeText(preferred.text());
  if (t1) return t1;

  let t = $(el).text();
  t = t.replace(/\b(Reply|Share|Like|Likes|Collapse|Expand)\b/gi, " ");
  return normalizeText(t);
}

function extractLikesLoose($, el) {
  const text = normalizeText($(el).text());
  const m = text.match(/\b(?:Like|Likes)\s*[:\s]?\s*([0-9][0-9,]*)\b/i);
  if (!m) return 0;
  return Number(m[1].replace(/,/g, "")) || 0;
}

function extractDatetimeMs($, el) {
  const dt = $(el).find("time").first().attr("datetime");
  if (dt) {
    const ms = Date.parse(dt);
    return Number.isFinite(ms) ? ms : null;
  }
  return null;
}

function findLikelyCommentBlocks($) {
  const blocks = [];
  const seen = new Set();

  const candidates = $("[data-testid*=comment], article[class*=comment], div[class*=comment]");
  candidates.each((_, el) => {
    const key = el.attribs?.["data-testid"] || el.attribs?.class || String(el);
    if (seen.has(key)) return;
    seen.add(key);
    blocks.push(el);
  });

  // Fallback: any container that has an author profile link
  if (blocks.length < 10) {
    $("a[href^='https://substack.com/@']").each((_, a) => {
      const container = $(a).closest("article, div, li, section");
      if (!container || !container.length) return;
      const el = container.get(0);
      const key = el.attribs?.class || el.attribs?.id || normalizeText(container.text()).slice(0, 80);
      if (seen.has(key)) return;
      seen.add(key);
      blocks.push(el);
    });
  }

  return blocks;
}

async function crawl() {
  console.log(`Substack: ${SUBSTACK_URL}`);
  console.log(`User: ${wantedName} (handle match: @${wantedHandle})`);
  console.log(`Max posts: ${MAX_POSTS}`);
  console.log(`Sleep: ${SLEEP_MS}ms (+ jitter), retries enabled`);

  const posts = await listPostsFromArchive(SUBSTACK_URL, MAX_POSTS);
  console.log(`Found ${posts.length} posts via archive feed.`);

  const out = [];
  let i = 0;

  for (const post of posts) {
    i++;
    const commentsUrl = post.url.replace(/\/+$/, "") + "/comments";

    try {
      const html = await fetchHtml(commentsUrl);
      const $ = cheerio.load(html);

      const blocks = findLikelyCommentBlocks($);

      for (const el of blocks) {
        const authorA = $(el).find("a[href^='https://substack.com/@']").first();
        const authorName = normalizeText(authorA.text());
        if (!authorName) continue;

        const href = authorA.attr("href") || "";
        const authorHandle = parseHandleFromHref(href);

        const matches =
          authorName === wantedName ||
          (authorHandle && authorHandle.toLowerCase() === wantedHandle);

        if (!matches) continue;

        const text = extractCommentText($, el);
        if (!text) continue;

        const likes = extractLikesLoose($, el);
        const commentDateMs = extractDatetimeMs($, el);

        const commentUrl =
          $(el).find("a[href*='#comment'], a[href*='comment']").first().attr("href") || null;

        out.push({
          postUrl: post.url,
          postTitle: post.title,
          postDateMs: post.postDateMs,
          commentDateMs,
          likes,
          commentUrl,
          text
        });
      }
    } catch (e) {
      // Don’t kill the whole crawl on a single post.
      // Most common failures are temporary 429s or a missing comments page.
      // You can inspect logs if you want:
      // console.log(`Skip (${i}/${posts.length}) ${commentsUrl}: ${String(e?.message ?? e)}`);
    }

    if (i % 25 === 0) console.log(`Scanned ${i}/${posts.length} posts...`);
    await politePause(SLEEP_MS);
  }

  // Rough de-dupe
  const seen = new Set();
  const deduped = [];
  for (const r of out) {
    const k = `${r.postUrl}||${r.commentDateMs || ""}||${(r.text || "").slice(0, 200)}`;
    if (seen.has(k)) continue;
    seen.add(k);
    deduped.push(r);
  }

  const payload = {
    generatedAt: new Date().toISOString(),
    substackUrl: SUBSTACK_URL,
    username: wantedName,
    count: deduped.length,
    rows: deduped
  };

  fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });
  fs.writeFileSync(OUT_PATH, JSON.stringify(payload, null, 2), "utf8");

  console.log(`Wrote ${deduped.length} comments to ${OUT_PATH}`);
}

crawl().catch((e) => {
  console.error(e);
  process.exit(1);
});
