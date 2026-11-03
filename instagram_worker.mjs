/**
 * Editorial selection and feed→story orchestration for the active Cloudflare
 * news publisher. NO external publishing until a verified adapter is injected.
 * All timestamps and records are server-side; no credentials live in source.
 */
export const IG_ACCOUNT = "candidatory.ir";

const norm = text => String(text || "").normalize("NFKC").replace(/[\u200c\u200d]/g, " ").replace(/\s+/g, " ").trim().toLowerCase();

export function qualified(item) {
  return Boolean(item && item.status === "published" && item.id && /^https:\/\//.test(item.url || "") &&
    item.title && item.source && item.image_url?.startsWith("https://") &&
    item.source_verified === true && item.image_rights_confirmed === true);
}

export function selectionKey(item) {
  return norm(item.url).replace(/[?#].*$/, "").replace(/\/$/, "");
}

export function selectNews(items, { maximum = 4, now = Date.now() } = {}) {
  const seen = new Set();
  return items.filter(qualified).filter(item => {
    const key = selectionKey(item);
    if (seen.has(key)) return false;
    seen.add(key);
    const age = now - new Date(item.published_at).getTime();
    return Number.isFinite(age) && age >= 0 && age < 36 * 60 * 60 * 1000;
  }).sort((a, b) => {
    // Neutral editorial suitability: relevance, verification, recency.
    const score = x => Math.min(3, Number(x.topic_relevance) || 0) * 3 +
      Math.min(3, Number(x.source_quality) || 0) * 2 +
      Math.max(0, 36 - (now - new Date(x.published_at).getTime()) / 3600000) / 12;
    return score(b) - score(a);
  }).slice(0, Math.max(0, Math.min(maximum, 8)));
}

export async function ensureSchema(db) {
  await db.prepare(`CREATE TABLE IF NOT EXISTS instagram_jobs (
    article_id TEXT PRIMARY KEY,
    source_url TEXT NOT NULL,
    headline TEXT NOT NULL,
    source_name TEXT NOT NULL,
    image_url TEXT NOT NULL,
    feed_image_url TEXT,
    story_image_url TEXT,
    feed_media_id TEXT,
    story_media_id TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    attempts INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
  )`).run();
}

export async function enqueue(db, news) {
  if (!qualified(news)) return false;
  const result = await db.prepare(`INSERT OR IGNORE INTO instagram_jobs
    (article_id,source_url,headline,source_name,image_url)
    VALUES (?,?,?,?,?)`).bind(news.id, news.url, news.title, news.source, news.image_url).run();
  return Boolean(result.meta?.changes);
}

async function transition(db, id, expected, next, fields = {}) {
  const allowed = ["feed_image_url", "story_image_url", "feed_media_id", "story_media_id", "error_message"];
  const names = Object.keys(fields);
  if (names.some(n => !allowed.includes(n))) throw new Error("Unsafe state field");
  const set = ["status=?", "updated_at=CURRENT_TIMESTAMP", ...names.map(k => k + "=?")].join(",");
  const r = await db.prepare(`UPDATE instagram_jobs SET ${set} WHERE article_id=? AND status=?`)
    .bind(next, ...names.map(n => fields[n]), id, expected).run();
  return Boolean(r.meta?.changes);
}

async function getJob(db, id) {
  return db.prepare("SELECT * FROM instagram_jobs WHERE article_id=?").bind(id).first();
}

/**
 * Exactly-once-safe policy: ambiguous publishing requests require reconciliation,
 * never blindly retry. Adapter must implement renderAndHost, publishFeed,
 * confirmFeed, publishStory, confirmStory, each returning stable IDs/URLs.
 */
export async function processJob(db, articleId, adapter, { enabled = false } = {}) {
  if (!enabled) return { status: "disabled" };
  if (!adapter || ["renderAndHost","publishFeed","confirmFeed","publishStory","confirmStory"]
    .some(fn => typeof adapter[fn] !== "function")) throw new Error("Incomplete publishing adapter");
  let job = await getJob(db, articleId);
  if (!job) throw new Error("Job missing");
  if (job.status === "completed") return { status: "completed", duplicate: true };

  if (job.status === "queued") {
    if (!await transition(db, articleId, "queued", "rendering")) return { status: "busy" };
    try {
      const assets = await adapter.renderAndHost(job);
      if (!assets?.feed?.startsWith("https://") || !assets?.story?.startsWith("https://")) throw new Error("Public media URLs required");
      await transition(db, articleId, "rendering", "ready", { feed_image_url: assets.feed, story_image_url: assets.story });
    } catch(e) {
      await transition(db, articleId, "rendering", "render_failed", { error_message: String(e).slice(0,300) });
      return { status: "render_failed" };
    }
    job = await getJob(db, articleId);
  }

  if (job.status === "ready") {
    if (!await transition(db, articleId, "ready", "feed_request_pending")) return { status: "busy" };
    try {
      const feedId = await adapter.publishFeed(job);
      if (!feedId) throw new Error("Feed ID missing");
      await transition(db, articleId, "feed_request_pending", "feed_sent", { feed_media_id: String(feedId) });
    } catch(e) {
      await transition(db, articleId, "feed_request_pending", "feed_needs_reconciliation", { error_message: String(e).slice(0,300) });
      return { status: "feed_needs_reconciliation" };
    }
    job = await getJob(db, articleId);
  }

  if (job.status === "feed_sent") {
    if (!await adapter.confirmFeed(job.feed_media_id)) return { status: "feed_not_confirmed" };
    if (!await transition(db, articleId, "feed_sent", "feed_confirmed")) return { status: "busy" };
    job = await getJob(db, articleId);
  }
  if (job.status === "feed_confirmed") {
    if (!await transition(db, articleId, "feed_confirmed", "story_request_pending")) return { status: "busy" };
    try {
      const storyId = await adapter.publishStory(job);
      if (!storyId) throw new Error("Story ID missing");
      await transition(db, articleId, "story_request_pending", "story_sent", { story_media_id: String(storyId) });
    } catch(e) {
      await transition(db, articleId, "story_request_pending", "story_needs_reconciliation", { error_message: String(e).slice(0,300) });
      return { status: "story_needs_reconciliation" };
    }
    job = await getJob(db, articleId);
  }
  if (job.status === "story_sent") {
    if (!await adapter.confirmStory(job.story_media_id)) return { status: "story_not_confirmed" };
    await transition(db, articleId, "story_sent", "completed");
    return { status: "completed" };
  }
  return { status: job.status };
}
