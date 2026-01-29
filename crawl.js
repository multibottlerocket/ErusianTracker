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

function loadExistingComments() {
  try {
    if (fs.existsSync(OUT_PATH)) {
      const raw = fs.readFileSync(OUT_PATH, "utf8");
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed?.rows)) {
        console.log(`Loaded ${parsed.rows.length} existing comments from ${OUT_PATH}`);
        return parsed.rows;
      }
    }
  } catch (e) {
    console.log(`Could not load existing comments: ${e.message}`);
  }
  return [];
}
async function politePause(baseMs = SLEEP_MS) {
  const jitter = Math.floor(Math.random() * 350);
  await sleep(baseMs + jitter);
}

function ua() {
  return "ErusianTracker/3.8 (+GitHub Actions; parent links)";
}

async function fetchWithRetry(url, { accept, kind }) {
  const MAX_RETRIES = 10;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const r = await fetch(url, { headers: { accept, "user-agent": ua() } });
    if (r.ok) return r;

    if (r.status === 429) {
      const ra = r.headers.get("retry-after");
      const raMs = ra && /^\d+$/.test(ra) ? Number(ra) * 1000 : 0;
      const backoff = Math.min(120_000, 1000 * Math.pow(2, attempt));
      const jitter = Math.floor(Math.random() * 800);
      const wait = Math.max(raMs, backoff) + jitter;
      console.log(`429 (${kind}) ${url} — wait ${wait}ms`);
      await sleep(wait);
      continue;
    }

    if (r.status >= 500 && r.status < 600 && attempt < MAX_RETRIES) {
      const backoff = Math.min(60_000, 800 * Math.pow(2, attempt));
      const jitter = Math.floor(Math.random() * 600);
      const wait = backoff + jitter;
      console.log(`HTTP ${r.status} (${kind}) ${url} — retry in ${wait}ms`);
      await sleep(wait);
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

function normalizeTextOneLine(s) {
  return (s || "").replace(/\s+/g, " ").trim();
}

function normalizeLine(line) {
  return (line || "").replace(/[ \t]+/g, " ").trimEnd();
}

function htmlToTextPreserveFormatting(htmlOrText) {
  if (!htmlOrText) return "";
  const s = String(htmlOrText);

  if (!/[<][a-z!/]/i.test(s)) {
    const lines = s.replace(/\r/g, "").split("\n").map(normalizeLine);
    return lines.join("\n").replace(/\n{3,}/g, "\n\n").trim();
  }

  const $ = cheerio.load(`<div id="x">${s}</div>`, { decodeEntities: true });
  const root = $("#x");

  root.find("br").replaceWith("\n");

  root.find("blockquote").each((_, bq) => {
    const raw = $(bq).text().replace(/\r/g, "");
    const lines = raw.split("\n").map(l => l.replace(/[ \t]+/g, " ").trim()).filter(Boolean);
    const quoted = lines.map(l => `> ${l}`).join("\n");
    $(bq).replaceWith(`\n\n${quoted}\n\n`);
  });

  const out = [];
  const blocks = root.find("p, li, h1, h2, h3, h4, h5, h6");
  if (blocks.length) {
    blocks.each((_, el) => {
      const t = $(el).text().replace(/\r/g, "");
      const cleaned = t.split("\n").map(l => l.replace(/[ \t]+/g, " ").trim()).filter(Boolean).join("\n");
      if (cleaned) out.push(cleaned);
    });
  } else {
    const raw = root.text().replace(/\r/g, "");
    const cleaned = raw.split("\n").map(l => l.replace(/[ \t]+/g, " ").trim()).join("\n");
    if (cleaned.trim()) out.push(cleaned);
  }

  return out.join("\n\n").replace(/[ \t]+\n/g, "\n").replace(/\n{3,}/g, "\n\n").trim();
}

function getAuthorName(c) {
  return c?.user?.name || c?.user?.profile?.name || c?.user_name || c?.commenter_name || c?.name || "";
}
function getAuthorHandle(c) {
  return c?.user?.handle || c?.user?.username || c?.user?.slug || c?.handle || "";
}
function getBodyText(c) {
  return htmlToTextPreserveFormatting(c?.body_html || c?.body || c?.text || "");
}
function getCommentDateMs(c) {
  const dt = c?.date || c?.created_at || c?.createdAt || c?.published_at || c?.posted_at || null;
  if (!dt) return null;
  if (typeof dt === "number") return (dt > 10_000_000_000 ? dt : dt * 1000);
  const ms = Date.parse(String(dt));
  return Number.isFinite(ms) ? ms : null;
}
function getLikeCount(c) {
  const n = Number(c?.reactions?.like ?? c?.like_count ?? c?.likes ?? 0);
  return Number.isFinite(n) ? n : 0;
}

function getReplyId(c) {
  // “specific comment id” (reply id)
  return c?.comment_id ?? c?.commentId ?? null;
}
function getObjectId(c) {
  // “id” field (sometimes equals top-level thread id, sometimes equals comment id)
  return c?.id ?? null;
}

function buildCommentPageUrl(postUrl, id) {
  if (!id) return null;
  return `${postUrl.replace(/\/+$/, "")}/comment/${encodeURIComponent(id)}`;
}

/**
 * Flatten nested threads AND record parent relationships using reply IDs.
 * parentBy maps replyId -> parentReplyId (or null for roots).
 */
function flattenWithParents(rootList) {
  const flat = [];
  const parentBy = new Map();

  const stack = [];
  for (const c of (Array.isArray(rootList) ? rootList : [rootList])) {
    if (c) stack.push({ c, parentReplyId: null });
  }

  while (stack.length) {
    const { c, parentReplyId } = stack.pop();
    if (!c || typeof c !== "object") continue;

    flat.push(c);

    const rid = getReplyId(c) ?? getObjectId(c); // replyId if present, else id for top-levels
    if (rid != null && !parentBy.has(String(rid))) {
      parentBy.set(String(rid), parentReplyId);
    }

    const kids = c.children || c.replies || c.responses || c.thread || c.comments || null;
    if (Array.isArray(kids) && kids.length) {
      const thisRid = rid ?? parentReplyId;
      for (const k of kids) stack.push({ c: k, parentReplyId: thisRid ?? null });
    }
  }

  return { flat, parentBy };
}

/**
 * Determine:
 * - commentId (specific reply) = replyId if present else objectId
 * - topLevelCommentId (thread root) = objectId IF it differs from commentId
 *   else compute by walking parents to root (fallback)
 */
function computeIds(c, parentBy) {
  const commentId = getReplyId(c) ?? getObjectId(c);
  const objectId = getObjectId(c);

  // Best case: objectId is clearly a different “thread id” than the reply id
  if (commentId != null && objectId != null && String(objectId) !== String(commentId)) {
    return { commentId, topLevelCommentId: objectId };
  }

  // Otherwise: walk parent chain (using reply ids) to find the root reply
  if (commentId == null) return { commentId: null, topLevelCommentId: null };

  let cur = String(commentId);
  let safety = 0;
  while (safety++ < 500) {
    const pid = parentBy.get(cur);
    if (!pid) break;
    cur = String(pid);
  }

  return { commentId, topLevelCommentId: cur };
}

function computeParentId(commentId, parentBy) {
  if (commentId == null) return null;
  const pid = parentBy.get(String(commentId)) ?? null;
  return pid == null ? null : pid;
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
      posts.push({ url: u, title: item?.title ? String(item.title) : null });
      if (posts.length >= maxPosts) break;
    }

    offset += chunk.length;
    await politePause(SLEEP_MS);
  }
  return posts;
}

async function getPostIdFromSlug(slug) {
  const metaUrl = `${SUBSTACK_URL}/api/v1/posts/${encodeURIComponent(slug)}`;
  const meta = await fetchJson(metaUrl);
  return meta?.id || meta?.post?.id || meta?.post_id || meta?.postId || null;
}

async function getAllCommentsForPostId(postId) {
  const url =
    `${SUBSTACK_URL}/api/v1/post/${postId}/comments` +
    `?token=&all_comments=true&sort=oldest_first`;
  const data = await fetchJson(url);

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

  for (let idx = 0; idx < posts.length; idx++) {
    const post = posts[idx];
    console.log(`Scanning post ${idx + 1}/${posts.length}: ${post.url}`);

    const slug = slugFromPostUrl(post.url);
    if (!slug) continue;

    try {
      const postId = await getPostIdFromSlug(slug);
      if (!postId) continue;

      const tree = await getAllCommentsForPostId(postId);
      const { flat, parentBy } = flattenWithParents(tree);

      let matched = 0;

      for (const c of flat) {
        const name = normalizeTextOneLine(getAuthorName(c));
        const handle = normalizeTextOneLine(getAuthorHandle(c)).toLowerCase().replace(/^@/, "");
        const isMatch = name === wantedName || (handle && handle === wantedHandle);
        if (!isMatch) continue;

        const text = getBodyText(c);
        if (!text) continue;

        const { commentId, topLevelCommentId } = computeIds(c, parentBy);
        const parentCommentId = computeParentId(commentId, parentBy);

        out.push({
          postUrl: post.url,
          postTitle: post.title,
          postId,

          commentId,
          topLevelCommentId,
          parentCommentId,

          // All “slashy” pages (fast)
          commentUrl: buildCommentPageUrl(post.url, commentId),
          topLevelCommentUrl: buildCommentPageUrl(post.url, topLevelCommentId),
          parentCommentUrl: buildCommentPageUrl(post.url, parentCommentId),

          commentDateMs: getCommentDateMs(c),
          likes: getLikeCount(c),
          text
        });

        matched++;
      }

      console.log(`  Found ${matched} matching comments`);
    } catch (e) {
      console.log(`  Skip due to error: ${String(e?.message ?? e)}`);
    }

    await politePause(SLEEP_MS);
  }

  // Load existing comments and merge with newly scraped ones
  const existing = loadExistingComments();
  const combined = [...existing, ...out];

  // De-dupe (keeps first occurrence, so existing comments preserved, new ones added)
  const seen = new Set();
  const deduped = [];
  for (const r of combined) {
    const k = `${r.postUrl}||${r.commentId || ""}||${(r.text || "").slice(0, 200)}`;
    if (seen.has(k)) continue;
    seen.add(k);
    deduped.push(r);
  }

  // Sort by comment date (newest first) for consistent ordering
  deduped.sort((a, b) => (b.commentDateMs || 0) - (a.commentDateMs || 0));

  console.log(`Total comments after merge: ${deduped.length} (${existing.length} existing + ${out.length} new, minus duplicates)`);

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
