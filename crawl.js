import fs from "node:fs";
import path from "node:path";
import * as cheerio from "cheerio";

const SUBSTACK_URL = (process.env.SUBSTACK_URL || "https://www.astralcodexten.com").replace(/\/+$/, "");
const USERNAME_RAW = process.env.USERNAME || "Erusian";

// Safe-ish defaults (you can override via workflow inputs/env)
const MAX_POSTS = Math.max(1, Math.min(Number(process.env.MAX_POSTS || "20"), 5000));
const SLEEP_MS = Math.max(200, Math.min(Number(process.env.SLEEP_MS || "500"), 5000));

const OUT_PATH = path.join("docs", "data", "comments.json");

const wantedName = USERNAME_RAW.trim();
const wantedHandle = USERNAME_RAW.trim().toLowerCase().replace(/^@/, "");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function politePause(baseMs = SLEEP_MS) {
  const jitter = Math.floor(Math.random() * 350);
  await sleep(baseMs + jitter);
}

function ua() {
  return "ErusianTracker/1.2 (+GitHub Actions)";
}

function publicationSlugFromBaseUrl(baseUrl) {
  // https://www.astralcodexten.com -> astralcodexten
  const h = new URL(baseUrl).hostname.replace(/^www\./, "");
  return h.split(".")[0];
}

function toOpenCommentsUrl(postUrl, baseUrl) {
  const pub = publicationSlugFromBaseUrl(baseUrl);

  // Expecting something like: https://www.astralcodexten.com/p/open-thread-414
  const u = new URL(postUrl);
  const m = u.pathname.match(/^\/p\/([^/?#]+)/);
  if (!m) return null;
  const slug = m[1];

  // Server-rendered comments page:
  return `https://open.substack.com/pub/${pub}/p/${slug}?comments=true`;
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

    if (r.status === 429) {
      const retryAfter = r.headers.get("retry-after");
      const retryAfterMs = retryAfter && /^\d+$/.test(retryAfter) ? Number(retryAfter) * 1000 : 0;

      const backoffMs = Math.min(90_000, 1000 * Math.pow(2, attempt));
      const jitterMs = Math.floor(Math.random() * 800);
      const waitMs = Math.max(retryAfterMs, backoffMs) + jitterMs;

      console.log(`429 rate limit (${kind}) ${url} — wait ${waitMs}ms`);
      await sleep(waitMs);
      continue;
    }

    if (r.status >= 500 && r.status < 600 && attempt < MAX_RETRIES) {
      const backoffMs = Math.min(45_000, 750 * Math.pow(2, attempt));
      const jitterMs = Math.floor(Math.random() * 500);
      const waitMs = backoffMs + jitterMs;
      console.log(`HTTP ${r.status} (${kind}) ${url} — retry in ${waitMs}ms`);
      await sleep(waitMs);
      continue;
    }

    throw new Error(`HTTP ${r.status} for ${url}`);
  }

  throw new Error(`Retries exhausted for ${url}`);
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

function normalizeText(s) {
  return (s || "").replace(/\s+/g, " ").trim();
}

function parseHandleFromHref(href) {
  if (!href) return null;
  const m = String(href).match(/@([A-Za-z0-9_]+)/);
  return m ? m[1] : null;
}

function extractDatetimeMs($, container) {
  const dt = $(container).find("time").first().attr("datetime");
  if (!dt) return null;
  const ms = Date.parse(dt);
  return Number.isFinite(ms) ? ms : null;
}

function extractLikesLoose($, container) {
  const text = normalizeText($(container).text());
  const m = text.match(/\b(?:Like|Likes)\s*[:\s]?\s*([0-9][0-9,]*)\b/i);
  if (!m) return 0;
  return Number(m[1].replace(/,/g, "")) || 0;
}

function extractCommentText($, container) {
  // Prefer obvious rich-text containers if present
  const preferred = $(container)
    .find(".markup, [data-testid*=markup], [class*=markup], [class*=content], [class*=body]")
    .first();
  const t1 = normalizeText(preferred.text());
  if (t1) return t1;

  // Otherwise strip UI-ish words and return best-effort
  let t = normalizeText($(container).text());
  t = t.replace(/\b(Expand full comment|Reply|Share|Liked by|Edited)\b/gi, " ");
  return normalizeText(t);
}

function findAuthorAnchors($) {
  // Open Substack pages usually include author name as a link.
  // Be generous: any <a> whose visible text matches the username.
  const anchors = [];
  $("a").each((_, a) => {
    const name = normalizeText($(a).text());
    if (!name) return;

    if (name === wantedName) {
      anchors.push(a);
      return;
    }

    const href = $(a).attr("href") || "";
    const h = parseHandleFromHref(href);
    if (h && h.toLowerCase() === wantedHandle) anchors.push(a);
  });
  return anchors;
}

function pickCommentContainer($, a) {
  // Walk up to a reasonable container that should include the comment text.
  const $a = $(a);
  const container =
    $a.closest("article").get(0) ||
    $a.closest("div").get(0) ||
    $a.closest("li").get(0) ||
    $a.closest("section").get(0);

  return container || $a.parent().get(0);
}

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
      posts.push({
        url: u,
        title: item?.title ? String(item.title) : null
      });
      if (posts.length >= maxPosts) break;
    }

    offset += chunk.length;
    await politePause(SLEEP_MS);
  }
  return posts;
}

async function crawl() {
  console.log(`Substack: ${SUBSTACK_URL}`);
  console.log(`User: ${wantedName} (handle match: @${wantedHandle})`);
  console.log(`Max posts: ${MAX_POSTS}`);
  console.log(`Sleep: ${SLEEP_MS}ms (+ jitter)`);

  const posts = await listPostsFromArchive(SUBSTACK_URL, MAX_POSTS);
  console.log(`Found ${posts.length} posts from archive feed.`);

  const out = [];

  let i = 0;
  for (const post of po
