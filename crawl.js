import fs from "node:fs";
import path from "node:path";
import * as cheerio from "cheerio";

const SUBSTACK_URL = (process.env.SUBSTACK_URL || "https://www.astralcodexten.com").replace(/\/+$/, "");
const USERNAME_RAW = process.env.USERNAME || "Erusian";

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
  return "ErusianTracker/3.0 (+GitHub Actions; comments JSON)";
}

async function fetchWithRetry(url, { accept, kind }) {
  const MAX_RETRIES = 10;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const r = await fetch(url, {
      headers: {
        accept,
        "user-agent": ua(),
      },
    });

    if (r.ok) return r;

    if (r.status === 429) {
      const retryAfter = r.headers.get("retry-after");
      const retryAfterMs = retryAfter && /^\d+$/.test(retryAfter) ? Number(retryAfter) * 1000 : 0;

      const backoffMs = Math.min(120_000, 1000 * Math.pow(2, attempt));
      const jitterMs = Math.floor(Math.random() * 800);
      const waitMs = Math.max(retryAfterMs, backoffMs) + jitterMs;

      console.log(`429 (${kind}) ${url} — wait ${waitMs}ms`);
      await sleep(waitMs);
      continue;
    }

    if (r.status >= 500 && r.status < 600 && attempt < MAX_RETRIES) {
      const backoffMs = Math.min(60_000, 800 * Math.pow(2, attempt));
      const jitterMs = Math.floor(Math.random() * 600);
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

function slugFromPostUrl(postUrl) {
  try {
    const u = new URL(postUrl);
    const m = u.pathname.match(/^\/p\/([^/?#]+)/);
    return m ? m[1] : null;
  } catch {
    return null;
  }
}

function normalizeText(s) {
  return (s || "").replace(/\s+/g, " ").trim();
}

function htmlToText(html) {
  if (!html) return "";
  const $ = cheerio.load(`<div id="x">${html}</div>`);
  return normalizeText($("#x").text());
}

function getAuthorName(comment) {
  return (
    comment?.user?.name ||
    comment?.user?.profile?.name ||
    comment?.user_name ||
    comment?.commenter_name ||
    comment?.name ||
    ""
  );
}

function getAuthorHandle(comment) {
  return (
    comment?.user?.handle ||
    comment?.user?.username ||
    comment?.user?.slug ||
    comment?.handle ||
    ""
  );
}

function getBodyText(comment) {
  // Substack often provides body_html; sometimes body.
  const html = comment?.body_html || comment?.body || comment?.text || "";
  // body might already be plaintext; htmlToText is safe either way.
  return htmlToText(html);
}

function getCommentDateMs(comment) {
  const dt =
    comment?.date ||
    comment?.created_at ||
    comment?.createdAt ||
    comment?.published_at ||
    comment?.posted_at ||
    null;

  if (!dt) return null;

  if (typeof dt === "number") {
    // might be seconds
    const ms = dt > 10_000_000_000 ? dt : dt * 1000;
    return Number.isFinite(ms) ? ms : null;
  }

  const ms = Date.parse(String(dt));
  return Number.isFinite(ms) ? ms : null;
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
        title: item?.title ? String(item.title) : null,
      });
      if (posts.length >= maxPosts) break;
    }

    offset += chunk.length;
    await politePause(SLEEP_MS);
  }

  return posts;
}

async function getPostIdFromSlug(slug) {
  // Many substacks expose /api/v1/posts/<slug> returning JSON with id.
  const metaUrl = `${SUBSTACK_URL}/api/v1/posts/${encodeURIComponent(slug)}`;
  const meta = await fetchJson(metaUrl);

  const id =
    meta?.id ||
    meta?.post?.id ||
    meta?.post_id ||
    meta?.postId ||
    null;

  if (!id) {
    // Helpful for debugging in logs if Substack changes structure
    console.log(`Could not find post id in ${metaUrl}. Keys: ${Object.keys(meta || {}).slice(0, 30).join(", ")}`);
    return null;
  }
  return id;
}

async function getAllCommentsForPostId(postId) {
  // Endpoint used by ACX comments widget / known scrapers.
  const url =
    `${SUBSTACK_URL}/api/v1/post/${postId}/comments` +
    `?token=&all_comments=true&sort=oldest_first&last_comment_at`;
  const data = await fetchJson(url);

  // Some installs return an array; others wrap it.
  if (Array.isArray(data)) return data;
  if (Array.isArray(data?.comments)) return data.comments;
  return [];
}

async function crawl() {
  console.log(`Substack: ${SUBSTACK_URL}`);
  console.log(`User: ${wantedName} (handle match: @${wantedHandle})`);
  console.log(`Max posts: ${MAX_POSTS}`);
  console.log(`Sleep: ${SLEEP_MS}ms (+ jitter)`);

  const posts = await listPostsFromArchive(SUBSTACK_URL, MAX_POSTS);
  console.log(`Found ${posts.length} posts from archive feed.`);

  const out = [];

  let idx = 0;
  for (const post of posts) {
    idx++;
    const slug = slugFromPostUrl(post.url);
    if (!slug) {
      console.log(`Skip (no /p/slug): ${post.url}`);
      continue;
    }

    try {
      const postId = await getPostIdFromSlug(slug);
      if (!postId) {
        console.log(`Skip (no postId): ${post.url}`);
        continue;
      }

      const comments = await getAllCommentsForPostId(postId);

      for (const c of comments) {
        const name = normalizeText(getAuthorName(c));
        const handle = normalizeText(getAuthorHandle(c)).toLowerCase().replace(/^@/, "");

        const matches =
          name === wantedName ||
          (handle && handle === wantedHandle);

        if (!matches) continue;

        const text = getBodyText(c);
        if (!text) continue;

        const commentId = c?.id || c?.comment_id || c?.commentId || null;
        const commentUrl = commentId ? `${post.url.replace(/\/+$/, "")}/comment/${commentId}` : null;

        out.push({
          postUrl: post.url,
          postTitle: post.title,
          postId,
          commentId,
          commentUrl,
          commentDateMs: getCommentDateMs(c),
          likes: Number(c?.reactions?.like || c?.like_count || c?.likes || 0) || 0,
          text,
        });
      }
    } catch (e) {
      console.log(`Skip (${idx}/${posts.length}) ${post.url}: ${String(e?.message ?? e)}`);
    }

    await politePause(SLEEP_MS);
  }

  // De-dupe
  const seen = new Set();
  const deduped = [];
  for (const r of out) {
    const k = `${r.postUrl}||${r.commentId || ""}||${(r.text || "").slice(0, 200)}`;
    if (seen.has(k)) continue;
    seen.add(k);
    deduped.push(r);
  }

  const payload = {
    generatedAt: new Date().toISOString(),
    substackUrl: SUBSTACK_URL,
    username: wantedName,
    count: deduped.length,
    rows: deduped,
  };

  fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });
  fs.writeFileSync(OUT_PATH, JSON.stringify(payload, null, 2), "utf8");

  console.log(`Wrote ${deduped.length} comments to ${OUT_PATH}`);
}

crawl().catch((e) => {
  console.error(e);
  process.exit(1);
});
