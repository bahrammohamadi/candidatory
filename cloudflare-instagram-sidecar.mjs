/**
 * Cloudflare sidecar for Candidatory: safe news-to-Instagram intake.
 * Does not change or poll Telegram; reads verified published rows in NEWS_DB.
 * Fail closed: posting disabled until the render-and-publish adapter is wired.
 */
const CHANNEL = "@candidatoryiran";
const IG = "@candidatory.ir";
const MAX_CANDIDATES = 12;
const RELEVANT = /انتخابات|انتخاباتی|شورا|رأی|رای|شهرداری|مدیریت شهری|حقوق شهروندی|election|council|ballot/i;
const NON_CIVIC = /فوتبال|ورزش|مد|رمزارز/i;
export async function ensureSchema(db) {
  await db.prepare(`CREATE TABLE IF NOT EXISTS instagram_intake_jobs (
    article_id TEXT PRIMARY KEY,
    title TEXT NOT NULL, source TEXT NOT NULL, source_url TEXT NOT NULL,
    published_at TEXT, source_image_url TEXT,
    image_rights TEXT NOT NULL DEFAULT 'pending',
    status TEXT NOT NULL DEFAULT 'awaiting_media',
    feed_id TEXT, story_id TEXT, feed_url TEXT, story_url TEXT,
    last_error TEXT, attempts INTEGER DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
  )`).run();
}
function sortEligible(items) {
  return items.filter(i =>
    i.status === "published" && i.id && i.title && i.source && /^https:\/\//.test(i.url || "") &&
    RELEVANT.test(i.title) && !NON_CIVIC.test(i.title)
  ).sort((a,b) => String(b.created_at).localeCompare(String(a.created_at)));
}
export async function intake(db, { cap = 4 } = {}) {
  await ensureSchema(db);
  const rows = await db.prepare("SELECT id,title,url,source,published_at,created_at,status FROM articles WHERE status='published' AND created_at >= datetime('now','-36 hours') ORDER BY created_at DESC LIMIT 120").all();
  const news = sortEligible(rows.results || []);
  const seen = new Set();
  let added = 0;
  for (const item of news) {
    const normalized = new URL(item.url).origin + new URL(item.url).pathname.replace(/\/$/,"");
    if (seen.has(normalized)) continue;
    seen.add(normalized);
    const q=await db.prepare(`INSERT OR IGNORE INTO instagram_intake_jobs
       (article_id,title,source,source_url,published_at) VALUES (?,?,?,?,?)`)
       .bind(item.id,item.title,item.source,item.url,item.published_at).run();
    if (q.meta?.changes) added++;
    if (added >= cap) break;
  }
  return { inspected: news.length, added };
}
export default {
  async fetch(request, env) {
    const u = new URL(request.url);
    if (u.pathname === "/health") return Response.json({
      ok: true, worker: "candidatory-instagram-staging",
      telegram_source: CHANNEL, instagram_destination: IG,
      auto_publish: false,
      reason: "requires approved renderer, media hosting and server-side Composio authorization"
    });
    if (u.pathname === "/queue/status") {
      if (!env.NEWS_DB) return Response.json({error:"missing database"},{status:503});
      const all=await env.NEWS_DB.prepare("SELECT status,COUNT(*) AS count FROM instagram_intake_jobs GROUP BY status").all().catch(()=>({results:[]}));
      return Response.json({auto_publish:false,statuses:all.results});
    }
    return new Response("Not Found", {status:404});
  },
  async scheduled(event,env,ctx) {
    if (!env.NEWS_DB) { console.error("NEWS_DB missing"); return; }
    const result = await intake(env.NEWS_DB);
    console.info("Instagram intake only, never auto-published", result);
  }
};
