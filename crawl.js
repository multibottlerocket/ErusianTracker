import fs from "node:fs";
import path from "node:path";
import * as cheerio from "cheerio";

const SUBSTACK_URL = (process.env.SUBSTACK_URL || "https://www.astralcodexten.com").replace(/\/+$/, "");
const USERNAME_RAW = process.env.USERNAME || "Erusian";

// “Safe” defaults for Actions:
const SLEEP_MS = Math.max(300, Math.min(Number(process.env.SLEEP_MS || "1800"), 8000)); // slow
const MAX_TOTAL_POSTS = Math.max(1, Math.min(Number(process.env.MAX_POSTS || "2000"), 20000)); // eventual cap
const ARCHIVE_PAGE_LIMIT_PER_RUN = Math.max(1, Math.min(Number(process.env.ARCHIVE_PAGES_PER_RUN || "25"), 200));
// Each archive page returns 12 posts -> default 25 pages = ~300 posts/run

const OUT_PATH = path.join("docs", "data", "comments.json");
const STATE_PATH = path.join("docs", "data", "state.json");

const wantedName = USERNAME_RAW.trim();
const wantedHandle = USERNAME_RAW.trim().toLowerCase().replace(/^@/, "");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function politePause(baseMs = SLEEP_MS) {
  const jitter = Math.floor(Math.random() * 600); // 0–600ms
  await sleep(baseMs + jitter);
}

function ua() {
  return "ErusianTracker/2.0 (+github actions; incremental)";
}

function loadJson(file, fallback) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return fallback;
  }
}

function saveJson(file, obj) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(obj, null, 2), "utf8");
}

async function fetchWithRetry(url, { accept, kind }) {
  const MAX_RETRIES = 10;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const r = await fetch(url, {
      headers: { accept, "user-agent": ua() }
    });

    if (r.ok) return r;

    if (r.status === 429) {
      const retryAfter = r.headers.get("retry-after");
      const retryAfterMs = retryAfter && /^\d+$/.test(retryAfter) ? Number(retryAfter) * 1000 : 0;

      const backoffMs = Math.min(180_000, 1500 * Math.pow(2, attempt)); // cap 3 minutes
      const jitterMs = Math.floor(Math.random() * 1200);
      const waitMs = Math.max(retryAfterMs, backoffMs) + jitterMs;

      console.log(`429 (${kind}) ${url} — wait ${waitMs}ms (attempt ${attempt + 1}/${MAX_RETRIES})`);
      await sleep(waitMs);
      continue;
    }

    if (r.status >= 500 && r.status < 600 && attempt < MAX_RETRIES) {
      const backoffMs = Math.min(60_000, 1000 * Math.pow(2, attempt));
      const jitterMs = Math.floor(Math.random() * 800);
      const waitMs = backoffMs + jitterMs;
      console.log(`HTTP ${r.status} (${kind}) ${url} — retry in ${waitMs}ms`);
      await sleep(waitMs);
      continue;
    }

    const text = await r.text().catch(() => "");
    throw new Error(`HTTP ${r.status} (${kind}) for ${url}${text ? ` — ${text.slice(0, 200)}` : ""}`);
  }

  throw new Error(`Retries exhausted (${kind}) for ${url}`);
}

async function fetchJson(url) {
  const r = await fetchWithRetry(url, { accept: "application/json,text/plain,*/*", kind: "json" });
  return r.json();
}
async function fetchHtml(url) {
  const r = await fetchWithRetry(url, { accept: "text/html,*/*", kind: "html" });
  return r.text();
}

// ---- Archive paging (incremental) ----
// Unofficial endpoint pattern: /api/v1/archive?limit=12&offset=...
async function fetchArchivePage(baseUrl, offset) {
  const limit = 12;
  const url = `${baseUrl}/api/v1/archive?limit=${limit}&offset=${offset}`;
  const chunk = await fetchJson(url);
  if (!Array.isArray(chunk)) return [];

  return chunk
    .map((item) => {
      const u = item?.canonical_url ? String(item.canonical_url) : null;
      if (!u) return null;

      const title = item?.title ? String(item.title) : null;

      let postDateMs = null;
      if (item?.post_date) {
        const ms = Date.parse(String(item.post_date));
        postDateMs = Number.isFinite(ms) ? ms : null;
      } else if (item?.published_at) {
        const ms = Number(item.published_at) * 1000;
        postDateMs = Number.isFinite(ms) ? ms : null;
      }

      return { url: u, title, postDateMs };
    })
    .filter(Boolean);
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

function mergeRows(existingRows, newRows) {
  const seen = new Set();
  const out = [];

  for (const r of existingRows) {
    const k = `${r.postUrl}||${r.commentDateMs || ""}||${(r.text || "").slice(0, 200)}`;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(r);
  }
  for (const r of newRows) {
    const k = `${r.postUrl}||${r.commentDateMs || ""}||${(r.text || "").slice(0, 200)}`;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(r);
  }
  return out;
}

async function crawlOnce() {
  console.log(`Substack: ${SUBSTACK_URL}`);
  console.log(`User: ${wantedName} (handle match: @${wantedHandle})`);
  console.log(`MAX_TOTAL_POSTS: ${MAX_TOTAL_POSTS}`);
  console.log(`ARCHIVE_PAGES_PER_RUN: ${ARCHIVE_PAGE_LIMIT_PER_RUN} (≈ ${ARCHIVE_PAGE_LIMIT_PER_RUN * 12} posts/run)`);
  console.log(`SLEEP_MS: ${SLEEP_MS} (+ jitter), retries enabled`);

  const state = loadJson(STATE_PATH, {
    nextOffset: 0,
    done: false,
    totalPostsSeen: 0,
    lastRunAt: ""
  });

  const existing = loadJson(OUT_PATH, {
    generatedAt: "",
    substackUrl: SUBSTACK_URL,
    username: wantedName,
    count: 0,
    rows: []
  });

  if (state.done) {
    console.log("State says done=true; nothing to do.");
    // Still rewrite metadata so the site shows a recent timestamp.
    existing.generatedAt = new Date().toISOString();
    existing.substackUrl = SUBSTACK_URL;
    existing.username = wantedName;
    existing.count = (existing.rows || []).length;
    saveJson(OUT_PATH, existing);
    return;
  }

  const newComments = [];
  let pagesFetched = 0;
  let postsProcessedThisRun = 0;

  try {
    while (pagesFetched < ARCHIVE_PAGE_LIMIT_PER_RUN && state.totalPostsSeen < MAX_TOTAL_POSTS) {
      const offset = state.nextOffset;
      const posts = await fetchArchivePage(SUBSTACK_URL, offset);

      if (!posts.length) {
        console.log(`Archive returned 0 items at offset=${offset}; marking done.`);
        state.done = true;
        break;
      }

      pagesFetched++;
      state.nextOffset += posts.length; // usually 12
      state.totalPostsSeen += posts.length;

      for (const post of posts) {
        postsProcessedThisRun++;

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

            newComments.push({
              postUrl: post.url,
              postTitle: post.title,
              postDateMs: post.postDateMs,
              commentDateMs,
              likes,
              commentUrl,
              text
            });
          }
        } catch {
          // Skip per-post failures (rate limit / missing comments page)
        }

        await politePause(SLEEP_MS);
      }

      console.log(
        `Progress: pagesFetched=${pagesFetched}/${ARCHIVE_PAGE_LIMIT_PER_RUN}, ` +
        `nextOffset=${state.nextOffset}, totalPostsSeen=${state.totalPostsSeen}`
      );

      // Small pause between archive pages
      await politePause(SLEEP_MS);
    }
  } catch (e) {
    // Important: don’t fail the workflow; save partial progress + exit cleanly.
    console.log(`WARN: crawl loop ended early: ${String(e?.message ?? e)}`);
  }

  const merged = mergeRows(existing.rows || [], newComments);

  const payload = {
    generatedAt: new Date().toISOString(),
    substackUrl: SUBSTACK_URL,
    username: wantedName,
    count: merged.length,
    rows: merged
  };

  state.lastRunAt = payload.generatedAt;

  saveJson(OUT_PATH, payload);
  saveJson(STATE_PATH, state);

  console.log(`This run: processed ~${postsProcessedThisRun} posts, found ${newComments.length} new comments`);
  console.log(`Total stored comments: ${merged.length}`);
  console.log(`Saved: ${OUT_PATH}`);
  console.log(`Saved state: ${STATE_PATH}`);
}

crawlOnce().catch((e) => {
  // Still don’t hard-fail (Pages should keep serving).
  console.log(`FATAL (non-failing): ${String(e?.message ?? e)}`);
  process.exit(0);
});
