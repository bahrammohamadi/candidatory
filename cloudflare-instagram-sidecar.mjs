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
 * Publication is deliberately independent and disabled until private renderer
 * and Instagram/Composio server credentials are installed and verified.
 */
export const INSTAGRAM_CATEGORY_COLORS=Object.freeze({
  breaking:"#E31336",election:"#1E63E9",analysis:"#F39C12"
});
export function classifyForInstagram(caption) {
  const s=String(caption||"").slice(0,400);
  if(/(?:^|\s)(?:خبر فوری|فوری|BREAKING)(?:\s|:|؛|$)/i.test(s)) return "breaking";
  if(/گزارش تحلیلی|راهنمای|چگونه|چطور|پیامدها|چه پیامدهایی|جریمندرینگ|راستی\s*آزمایی|تحلیل|توضیحی|what if/i.test(s)) return "analysis";
  if(/انتخابات|رأی\s*گیری|رای\s*گیری|نامزد|کنگره|شورای شهر|شورا|electoral|election/i.test(s)) return "election";
  return null;
}
export async function ensurePublishingQueue(db) {
  await db.prepare(`CREATE TABLE IF NOT EXISTS instagram_scheduled_queue (
    telegram_message_id INTEGER PRIMARY KEY,
    category_id TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_image_url TEXT NOT NULL,
    headline TEXT NOT NULL,
    template_id TEXT NOT NULL DEFAULT 'candidatory-editorial-v8-category-seals',
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

export default {
  async fetch(request, env) {
    const u = new URL(request.url);
    if (u.pathname === "/health") return Response.json({
      ok: true, worker: "candidatory-instagram-staging",
      telegram_source: CHANNEL, instagram_destination: IG,
      auto_publish: false,
      reason: "requires approved renderer, media hosting and server-side Composio authorization"
    });
    if (u.pathname === "/channel/probe") {
      try { const r=await fetch(PUBLIC_CHANNEL,{cf:{cacheTtl:300,cacheEverything:true}});const p=parseTelegramPublicFeed(await r.text());return Response.json({http:r.status,messages:p.length,latest:p[0]?.telegram_message_id||null}); } catch(e){return Response.json({error:String(e).slice(0,100)},{status:502});}
    }
    if (u.pathname === "/queue/status") {
      if (!env.NEWS_DB) return Response.json({error:"missing database"},{status:503});
      const all=await env.NEWS_DB.prepare("SELECT status,COUNT(*) AS count FROM instagram_scheduled_queue GROUP BY status").all().catch(()=>({results:[]}));
      const last=await env.NEWS_DB.prepare("SELECT ran_at,telegram_seen,queued,error FROM instagram_scheduler_runs ORDER BY id DESC LIMIT 1").first().catch(()=>null);
      return Response.json({auto_publish:false,cron:"*/30 * * * *",categoryTemplate:"v8",statuses:all.results,lastScheduledRun:last,missing:["CANDIDATORY_RENDERER","INSTAGRAM_SERVER_AUTH"]});
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
    console.info("Cloudflare scheduling: intake+queue only; no Instagram publish",source,queue,legacy);
  }
};
