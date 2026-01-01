import fs from "node:fs";
import path from "node:path";
import * as cheerio from "cheerio";

const SUBSTACK_URL = (process.env.SUBSTACK_URL || "https://www.astralcodexten.com").replace(/\/+$/, "");
const USERNAME_RAW = process.env.USERNAME || "Erusian";
const MAX_POSTS = Math.max(1, Math.min(Number(process.env.MAX_POSTS || "1200"), 5000));
const SLEEP_MS = Math.max(50, Math.min(Number(process.env.SLEEP_MS || "200"), 2000));

const OUT_PATH = path.join("docs", "data", "comments.json");

const wantedName = USERNAME_RAW.trim();
const wantedHandle = USERNAME_RAW.trim().toLowerCase().replace(/^@/, "");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchJson(url) {
  const r = await fetch(url, { headers: { accept: "application/json,text/plain,*/*" } });
  if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
  return r.json();
}

async function fetchHtml(url) {
  const r = await fetch(url, { headers: { accept: "text/html,*/*" } });
  if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
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
    await sleep(SLEEP_MS);
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
  // Prefer "content-ish" areas first
  const preferred = $(el)
    .find(".markup, [data-testid*=markup], [class*=markup], [class*=content], [class*=body]")
    .first();
  const t1 = normalizeText(preferred.text());
  if (t1) return t1;

  // Fallback: element text, stripped a bit
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
  // Broad net; Substack changes classes often.
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

        // best-effort permalink
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
      // Some posts may not have comments pages or may error; ignore and continue.
    }

    if (i % 25 === 0) console.log(`Scanned ${i}/${posts.length} posts...`);
    await sleep(SLEEP_MS);
  }

  // De-dupe (rough)
  const seen = new Set();
  const deduped = [];
  for (const r of out) {
    const k = `${r.postUrl}||${r.commentDateMs || ""}||${r.text.slice(0, 200)}`;
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
