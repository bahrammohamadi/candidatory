/**
 * Cloudflare sidecar for Candidatory: safe news-to-Instagram intake.
 * Does not change or poll Telegram; reads verified published rows in NEWS_DB.
 * Fail closed: posting disabled until the render-and-publish adapter is wired.
 */
const CHANNEL = "@candidatoryiran";
const IG = "@candidatory.ir";
const MAX_CANDIDATES = 12;
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
    i.status === "published" && i.id && i.title && i.source && /^https:\/\//.test(i.url || "")
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

/** Read-only Telegram public channel discovery, avoids Bot API polling/webhook conflicts. */
const PUBLIC_CHANNEL = "https://t.me/s/candidatoryiran";
function unescapeHtml(s) {
  return String(s||"").replace(/&amp;/g,"&").replace(/&quot;/g,'"')
    .replace(/&#39;/g,"'").replace(/&lt;/g,"<").replace(/&gt;/g,">")
    .replace(/&#(\d+);/g,(_,n)=>String.fromCodePoint(Number(n)));
}
function stripMarkup(s) {
  return unescapeHtml(String(s||"").replace(/<br\s*\/?\s*>/gi,"\n")
    .replace(/<[^>]*>/g," ").replace(/\s+/g," ").trim());
}
export function parseTelegramPublicFeed(html) {
  const blocks=String(html||"").split(/<div\s+class=["']tgme_widget_message_wrap/).slice(1);
  const posts=[];
  for(const chunk of blocks){
    const id=chunk.match(/data-post=["']candidatoryiran\/(\d+)["']/i)?.[1];
    if(!id)continue;
    const textMatch=chunk.match(/<div\s+class=["'][^"']*tgme_widget_message_text[^"']*["'][^>]*>([\s\S]*?)<\/div>/i);
    const photo=chunk.match(/background-image:\s*url\(['"]?(\/\/|https?:\/\/)[^)'"]+['"]?\)/i)?.[0];
    const photoUrl=photo?.match(/url\(['"]?([^'")]+)/i)?.[1]||"";
    const time=chunk.match(/datetime=["']([^"']+)["']/i)?.[1]||null;
    const content=stripMarkup(textMatch?.[1]||"");
    if(!content)continue;
    const normalizedPhoto=photoUrl.startsWith("//")?"https:"+photoUrl:photoUrl;
    posts.push({telegram_message_id:Number(id),caption:content.slice(0,3500),
      photo_url:normalizedPhoto.startsWith("https://")?normalizedPhoto:null,
      telegram_url:"https://t.me/candidatoryiran/"+id,posted_at:time});
  }
  return posts.sort((a,b)=>b.telegram_message_id-a.telegram_message_id);
}
export async function ensureTelegramTable(db){
  await db.prepare(`CREATE TABLE IF NOT EXISTS instagram_telegram_posts (
    telegram_message_id INTEGER PRIMARY KEY,
    telegram_url TEXT NOT NULL,
    caption TEXT NOT NULL,
    photo_url TEXT,
    posted_at TEXT,
    status TEXT NOT NULL DEFAULT 'discovered',
    feed_media_id TEXT,story_media_id TEXT,last_error TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
  )`).run();
}
export async function syncTelegramChannel(db, fetchImpl=fetch){
  const response=await fetchImpl(PUBLIC_CHANNEL,{headers:{"User-Agent":"Mozilla/5.0 (CandidatoryNewsBridge/1.0)"},cf:{cacheTtl:300,cacheEverything:true}});
  if(!response.ok)throw new Error("Telegram public channel HTTP "+response.status);
  const parsed=parseTelegramPublicFeed(await response.text());
  if(!parsed.length)throw new Error("Telegram page contained no readable messages");
  await ensureTelegramTable(db);
  let added=0;
  for(const item of parsed){
    const x=await db.prepare(`INSERT INTO instagram_telegram_posts
      (telegram_message_id,telegram_url,caption,photo_url,posted_at)
      VALUES (?,?,?,?,?) ON CONFLICT(telegram_message_id) DO UPDATE SET
      photo_url=COALESCE(instagram_telegram_posts.photo_url,excluded.photo_url),
      posted_at=COALESCE(instagram_telegram_posts.posted_at,excluded.posted_at)`)
      .bind(item.telegram_message_id,item.telegram_url,item.caption,
       item.photo_url,item.posted_at).run();
    if(x.meta?.changes)added++;
  }
  return {seen:parsed.length,added,latest:parsed[0].telegram_message_id};
}

/** Server-side selection/queue. No ChatGPT presence is needed for these steps.
 * Candidate topics are unrestricted: politics, economy, culture, social issues, science, sport and more.\n * Publication is deliberately independent and disabled until private renderer
 * and Instagram/Composio server credentials are installed and verified.
 */
export const INSTAGRAM_CATEGORY_COLORS=Object.freeze({
  breaking:"#E31336",election:"#1E63E9",analysis:"#F39C12",general:"#20A39E"
});
export function classifyForInstagram(caption) {
  const s=String(caption||"").slice(0,700);
  const first=s.split(/#|@candidatoryiran|📰/)[0].replace(/^[^\p{L}\p{N}]*/u,"").trim();
  const title=first.slice(0,145);
  if(!title)return null;
  // Only explicitly marked, sourced breaking reports receive the urgent red seal.
  if(/^(?:خبر فوری|فوری|BREAKING)(?:[:：\s]|$)/i.test(title))return "breaking";
  // An incidental election reference does not turn an economy or diplomacy story into election coverage.
  if(/^(?:قیمت|سوخت|بنزین|نفت|ارز|هدف مذاکرات|ادعاهای اخیر درباره|افزایش قیمت|کاهش قیمت|تورم)/.test(title))return "general";
  if(/جریمندرینگ|گزارش تحلیلی|راهنمای|چگونه|چطور|پیامدها|چه پیامدهایی|راستی\s*آزمایی|تحلیل|توضیحی|چه می‌شود|what if/i.test(title))
    return "analysis";
  if(/انتخابات|رأی\s*گیری|رای\s*گیری|نامزد|مجلس نمایندگان|کنگره|شورای شهر|انتخاباتی|electoral|election/i.test(title))
    return "election";
  // Any other properly sourced news may be considered; topic does NOT block Instagram.
  return "general";
}

export async function ensurePublishingQueue(db) {
  await db.prepare(`CREATE TABLE IF NOT EXISTS instagram_scheduled_queue (
    telegram_message_id INTEGER PRIMARY KEY,
    category_id TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_image_url TEXT NOT NULL,
    headline TEXT NOT NULL,
    template_id TEXT NOT NULL DEFAULT 'candidatory-editorial-v9-all-news',
    status TEXT NOT NULL DEFAULT 'awaiting_renderer',
    attempts INTEGER NOT NULL DEFAULT 0,
    feed_media_id TEXT,story_media_id TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
  )`).run();
}
export async function queueEligibleTelegramNews(db, {maxNew=4}={}) {
  await ensurePublishingQueue(db);
  const rows=await db.prepare(`SELECT p.telegram_message_id,p.caption,p.telegram_url,
     p.photo_url,p.posted_at,p.status
     FROM instagram_telegram_posts p
     LEFT JOIN instagram_category_publications published
       ON published.telegram_message_id=p.telegram_message_id
     LEFT JOIN instagram_scheduled_queue queued
       ON queued.telegram_message_id=p.telegram_message_id
     WHERE p.status='discovered' AND p.photo_url IS NOT NULL
       AND p.posted_at IS NOT NULL AND published.telegram_message_id IS NULL
       AND queued.telegram_message_id IS NULL
       AND julianday(p.posted_at) >= julianday('now','-24 hours')
     ORDER BY p.telegram_message_id DESC LIMIT 60`).all();
  let added=0,eligible=0;
  for(const row of rows.results||[]) {
    const category=classifyForInstagram(row.caption);
    if(!category || !/^https:\/\//.test(row.photo_url))continue;
    // Only Telegram news having an explicit publisher are queued for verification.
    if(!/📰|منبع:|خبرگزاری|به گزارش|گاردین|ایسنا|مهر|انتخاب/.test(row.caption))continue;
    eligible++;
    const headline=String(row.caption).replace(/^[^\p{L}\p{N}]*/u,"").split(/#|@candidatoryiran|\n/)[0].trim().slice(0,260);
    if(headline.length<12)continue;
    const x=await db.prepare(`INSERT OR IGNORE INTO instagram_scheduled_queue
      (telegram_message_id,category_id,source_url,source_image_url,headline)
      VALUES(?,?,?,?,?)`).bind(row.telegram_message_id,category,row.telegram_url,
         row.photo_url,headline).run();
    if(x.meta?.changes)added++;
    if(added>=maxNew)break;
  }
  return {inspected:(rows.results||[]).length,eligible,queued:added,publishEnabled:false};
}

/** Reconcile immutable published IDs written by the independently scheduled
 * GitHub Actions renderer/publisher. Never claim published from a container ID.
 */
const GITHUB_PUBLICATION_STATE="https://raw.githubusercontent.com/bahrammohamadi/candidatory/main/data/instagram-published.json";
export async function reconcilePublishedState(db, fetchImpl=fetch) {
  const response=await fetchImpl(GITHUB_PUBLICATION_STATE,{cf:{cacheTtl:60,cacheEverything:true}});
  if(!response.ok)throw new Error("Publication state HTTP "+response.status);
  const payload=await response.json();
  if(payload.account!=="candidatory.ir" || typeof payload.posts!=="object")
    throw new Error("Unexpected Instagram account state");
  let synced=0;
  for(const [id,entry] of Object.entries(payload.posts)) {
    if(!/^\d+$/.test(id) || !entry || typeof entry!=="object")continue;
    const feed=String(entry.feed_media_id||"");
    const story=String(entry.story_media_id||"");
    if(!/^\d+$/.test(feed))continue;
    const completed=entry.status==="completed" && /^\d+$/.test(story);
    const status=completed?"completed":"feed_published";
    await db.prepare(`UPDATE instagram_telegram_posts
      SET status=?,feed_media_id=?,story_media_id=COALESCE(?,story_media_id),last_error=NULL
      WHERE telegram_message_id=? AND (feed_media_id IS NULL OR feed_media_id=?)`)
      .bind(status,feed,completed?story:null,Number(id),feed).run();
    await db.prepare(`UPDATE instagram_scheduled_queue
      SET status=?,feed_media_id=?,story_media_id=COALESCE(?,story_media_id),
      updated_at=CURRENT_TIMESTAMP
      WHERE telegram_message_id=? AND (feed_media_id IS NULL OR feed_media_id=?)`)
      .bind(status,feed,completed?story:null,Number(id),feed).run();
    if(entry.category && entry.feed_permalink && ["breaking","election","analysis","general"].includes(entry.category)){
      await db.prepare(`INSERT OR IGNORE INTO instagram_category_publications
        (telegram_message_id,category_id,template_id,feed_media_id,permalink)
        VALUES(?,?,?,?,?)`).bind(Number(id),entry.category,
         entry.template||"candidatory-editorial-v9-all-news",feed,entry.feed_permalink).run();
    }
    synced++;
  }
  return {synced};
}

export default {
  async fetch(request, env) {
    const u = new URL(request.url);
    if (u.pathname === "/health") return Response.json({
      ok: true, worker: "candidatory-instagram-staging",
      telegram_source: CHANNEL, instagram_destination: IG,
      auto_publish: false,
      publishing_runtime: "github-actions",
      publishing_schedule: "7,37 * * * *",
      reason: "Cloudflare collects and reconciles; separate GitHub Actions publisher is disabled by default until server credentials are configured"
    });
    if (u.pathname === "/channel/probe") {
      try { const r=await fetch(PUBLIC_CHANNEL,{cf:{cacheTtl:300,cacheEverything:true}});const p=parseTelegramPublicFeed(await r.text());return Response.json({http:r.status,messages:p.length,latest:p[0]?.telegram_message_id||null}); } catch(e){return Response.json({error:String(e).slice(0,100)},{status:502});}
    }
    if (u.pathname === "/queue/status") {
      if (!env.NEWS_DB) return Response.json({error:"missing database"},{status:503});
      const all=await env.NEWS_DB.prepare("SELECT status,COUNT(*) AS count FROM instagram_scheduled_queue GROUP BY status").all().catch(()=>({results:[]}));
      const last=await env.NEWS_DB.prepare("SELECT ran_at,telegram_seen,queued,error FROM instagram_scheduler_runs ORDER BY id DESC LIMIT 1").first().catch(()=>null);
      return Response.json({cloudflare_auto_publish:false,github_actions_publisher:"configured_disabled_by_default",cron:"*/30 * * * *",github_cron:"7,37 * * * *",categoryTemplate:"v9",statuses:all.results,lastScheduledRun:last,requiredGithubSecrets:["CANDIDATORY_COMPOSIO_API_KEY","CANDIDATORY_COMPOSIO_INSTAGRAM_CONNECTED_ACCOUNT_ID"],requiredGithubVariable:"CANDIDATORY_INSTAGRAM_AUTOPUBLISH_ENABLED"});
    }
    return new Response("Not Found", {status:404});
  },
  async scheduled(event,env,ctx) {
    if (!env.NEWS_DB) { console.error("NEWS_DB missing"); return; }
    // Telegram retrieval stays independent from the active bot webhook.
    let source={};try {source=await syncTelegramChannel(env.NEWS_DB)}
      catch(e){console.error("Telegram read-only fetch failed",String(e).slice(0,180));}
    let queue={};try {queue=await queueEligibleTelegramNews(env.NEWS_DB)}
      catch(e){console.error("Instagram queue failed",String(e).slice(0,180));}
    let reconciled={};try {reconciled=await reconcilePublishedState(env.NEWS_DB)}
      catch(e){console.warn("Published state reconciliation failed",String(e).slice(0,180))}
    let legacy={};try {legacy=await intake(env.NEWS_DB)}
      catch(e){console.warn("Legacy article intake failed",String(e).slice(0,180))}
    try {
      await env.NEWS_DB.prepare(`CREATE TABLE IF NOT EXISTS instagram_scheduler_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ran_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        telegram_seen INTEGER NOT NULL DEFAULT 0,
        queued INTEGER NOT NULL DEFAULT 0,
        error TEXT
      )`).run();
      await env.NEWS_DB.prepare(`INSERT INTO instagram_scheduler_runs(telegram_seen,queued,error)
        VALUES(?,?,?)`).bind(Number(source.seen||0),Number(queue.queued||0),
         source.seen ? null : "Telegram source unavailable").run();
    } catch(e){console.error("Scheduler audit write failed",String(e).slice(0,180))}
    console.info("Cloudflare scheduling: intake+queue+publication reconciliation; no direct Instagram publish",source,queue,reconciled,legacy);
  }
};
