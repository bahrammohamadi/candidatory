function fixEncoding(s){if(!/[ØÙÛÃ]/.test(s))return s;try{const bytes=Uint8Array.from([...s].map(ch=>ch.charCodeAt(0)&255));const t=new TextDecoder('utf-8',{fatal:true}).decode(bytes);return /[\u0600-\u06ff]/.test(t)?t:s}catch{return s}}
const FEEDS=[["ISNA","https://www.isna.ir/rss"],["Mehr","https://www.mehrnews.com/rss"],["BBCPersian","https://feeds.bbci.co.uk/persian/rss.xml"]]; // Unreachable feeds removed until verified
const KEYWORDS=[/انتخابات/i,/انتخاباتی/i,/نامزد/i,/کاندیدا/i,/رای.?گیری/i,/رأی.?گیری/i,/صلاحیت/i,/شورای شهر/i,/شوراهای اسلامی/i,/election/i,/candidate/i];
const LOCAL_ELECTION=/انتخابات (شورا|مجلس|ریاست.?جمهوری|خبرگان)|شوراهای اسلامی|شورای (اسلامی|شهر|روستا)|ستاد انتخابات|هیئت اجرایی انتخابات|صلاحیت (داوطلبان|نامزدها)|ثبت.?نام (داوطلبان|نامزدها)|کاندیداتوری|نامزدهای انتخابات|فرایند انتخابات/i;
const REJECT=[/فوتبال/i,/رمزارز/i,/آشپزی/i,/سریال تلویزیونی/i];
function decode(s){return s.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g,'$1').replace(/<[^>]*>/g,' ').replace(/&amp;/g,'&').replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/&lt;/g,'<').replace(/&gt;/g,'>').trim()}
function tag(xml,name){const re=new RegExp('<'+name+'(?:\\s[^>]*)?>([\\s\\S]*?)<\\/'+name+'>','i');const m=xml.match(re);return m?fixEncoding(decode(m[1])):''}
function parse(xml,source){return [...xml.matchAll(/<item(?:\s[^>]*)?>([\s\S]*?)<\/item>/gi)].slice(0,100).map(m=>({title:tag(m[1],'title'),link:tag(m[1],'link'),date:tag(m[1],'pubDate'),source})).filter(x=>x.title&&/^https?:\/\//.test(x.link))}
export default {async fetch(request,env){const u=new URL(request.url);if(u.pathname==='/health')return Response.json({ok:true,mode:'news-worker',publishing:env.PUBLISH_ENABLED==='true',telegramConfigured:!!env.TELEGRAM_BOT_TOKEN});if(u.pathname==='/bot-identity'){if(!env.TELEGRAM_BOT_TOKEN)return Response.json({ok:false,error:'missing_token'},{status:503});const r=await fetch('https://api.telegram.org/bot'+env.TELEGRAM_BOT_TOKEN+'/getMe');const j=await r.json();return Response.json({ok:j.ok,username:j.result?.username,botId:j.result?.id,error:j.description},{status:j.ok?200:502})}if(u.pathname==='/test-publish'){const key=request.headers.get('x-test-key');if(!env.TEST_PUBLISH_KEY||key!==env.TEST_PUBLISH_KEY)return new Response('Unauthorized',{status:401});if(!env.TELEGRAM_BOT_TOKEN)return Response.json({ok:false,error:'Missing Telegram secret'},{status:503});const payload={chat_id:'@candidatoryiran',text:'🧪 پیام آزمایشی کاندیداتوری\n\nاتصال Cloudflare Workers به کانال تلگرام در حال بررسی است. این پیام خبر نیست و صرفاً برای آزمایش ارسال شده است.\n\n@candidatoryiran'};const response=await fetch('https://api.telegram.org/bot'+env.TELEGRAM_BOT_TOKEN+'/sendMessage',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(payload)});const data=await response.json();return Response.json({ok:!!data.ok,messageId:data.result?.message_id,errorCode:data.error_code,description:data.ok?undefined:data.description},{status:data.ok?200:502})}if(u.pathname==='/diagnostics'){const checks=await Promise.all(FEEDS.map(async ([source,url])=>{try{const r=await fetch(url,{signal:AbortSignal.timeout(6500)});const raw=new TextDecoder('utf-8').decode(await r.arrayBuffer());const items=parse(raw,source);return {source,http:r.status,items:items.length,matching:items.filter(x=>isElection(x.title)).length,contentType:r.headers.get('content-type'),sample:items[0]?.title||raw.slice(0,70)}}catch(e){return {source,error:String(e)}}}));return Response.json({checks})}return new Response('Not found',{status:404})},async scheduled(event,env,ctx){ctx.waitUntil(run(env,event.scheduledTime))}};
async function run(env,time){await checkTelegramOnce(env);let totalFound=0,totalAccepted=0;for(const [source,url] of FEEDS){let items=[];try{const r=await fetch(url,{signal:AbortSignal.timeout(7000),headers:{'user-agent':'CandidatoryNewsBot/7.0 (+RSS monitoring)'}});if(!r.ok)throw Error('RSS HTTP '+r.status);items=parse(new TextDecoder('utf-8').decode(await r.arrayBuffer()),source)}catch(e){console.error('fetch failed',source,String(e));continue}totalFound+=items.length;let accepted=0;for(const item of items){if(!isElection(item.title))continue;const id=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(item.link));const hash=[...new Uint8Array(id)].map(x=>x.toString(16).padStart(2,'0')).join('');if(env.NEWS_DB){const result=await env.NEWS_DB.prepare('INSERT OR IGNORE INTO articles (id,title,url,source,published_at,status) VALUES (?,?,?,?,?,?)').bind(hash,item.title,item.link,item.source,item.date||null,'pending_review').run();if(result.meta?.changes){accepted++;totalAccepted++}}}console.log(JSON.stringify({source,found:items.length,accepted}))}if(env.PUBLISH_ENABLED==='true'&&env.TELEGRAM_BOT_TOKEN){await publishOne(env)}console.log(JSON.stringify({totalFound,totalAccepted,publishing:env.PUBLISH_ENABLED==='true'}))}

function escapeHtml(s){return String(s??'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;')}
async function publishOne(env){
 const row=await env.NEWS_DB.prepare("SELECT id,title,url,source FROM articles WHERE status='pending_review' ORDER BY created_at ASC LIMIT 1").first();
 if(!row)return;
 const title=escapeHtml(fixEncoding(row.title));
 const source=escapeHtml(row.source);
 let link;try{const u=new URL(row.url);if(!['http:','https:'].includes(u.protocol))throw Error('protocol');link=u.href}catch(e){console.error('Invalid article URL',row.id);return}
 const text='🗳 <b>اخبار انتخابات | کاندیداتوری</b>\n\n<b>'+title+'</b>\n\n📰 <b>منبع:</b> '+source+'\n🔗 <a href="'+escapeHtml(link)+'">مطالعه خبر در منبع اصلی</a>\n\n#اخبار_انتخابات #کاندیداتوری\n@candidatoryiran';
 const payload={chat_id:'@candidatoryiran',text,parse_mode:'HTML',link_preview_options:{is_disabled:true},reply_markup:{inline_keyboard:[[{text:'📖 مشاهده متن خبر',url:link}]]}};
 try{
  const response=await fetch('https://api.telegram.org/bot'+env.TELEGRAM_BOT_TOKEN+'/sendMessage',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(payload)});
  const data=await response.json();
  if(!data.ok){console.error('Telegram publish failed',data.error_code,data.description);return}
  await env.NEWS_DB.prepare("UPDATE articles SET status='published' WHERE id=?").bind(row.id).run();
  console.log('Published formatted news',row.id,data.result?.message_id)
 }catch(e){console.error('Telegram publish exception',String(e))}
}

async function checkTelegramOnce(env){
 if(!env.NEWS_DB||!env.TELEGRAM_BOT_TOKEN){console.error('missing binding');return}
 await env.NEWS_DB.prepare("CREATE TABLE IF NOT EXISTS news_diagnostics (name TEXT PRIMARY KEY, result TEXT, updated_at TEXT DEFAULT CURRENT_TIMESTAMP)").run();
 const old=await env.NEWS_DB.prepare("SELECT result FROM news_diagnostics WHERE name='telegram_connection_20261028'").first();
 if(old)return;
 const claim=await env.NEWS_DB.prepare("INSERT OR IGNORE INTO news_diagnostics(name,result) VALUES('telegram_connection_20261028','started')").run();
 if(!claim.meta?.changes)return;
 let result;
 try{
  const r=await fetch('https://api.telegram.org/bot'+env.TELEGRAM_BOT_TOKEN+'/sendMessage',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({chat_id:'@candidatoryiran',text:'🧪 آزمایش اتصال سامانه خبری کاندیداتوری\n\nاین پیام فقط برای تأیید اتصال Cloudflare به کانال است و خبر واقعی نیست.\n\n@candidatoryiran'})});
  const j=await r.json();result=j.ok?'sent:'+j.result.message_id:'telegram_error:'+j.error_code+':'+j.description
 }catch(e){result='exception:'+String(e)}
 await env.NEWS_DB.prepare("UPDATE news_diagnostics SET result=?,updated_at=CURRENT_TIMESTAMP WHERE name='telegram_connection_20261028'").bind(result).run();
 console.log('Telegram connectivity:',result)
}

function isElection(title){const t=fixEncoding(title).normalize('NFKC').replace(/[ي]/g,'ی').replace(/[ك]/g,'ک').replace(/[‌ـ]/g,' ');return !REJECT.some(r=>r.test(t))&&/(انتخابات|انتخاباتی|انتخاب رئیس|انتخاب رییس|رأی گیری|رای گیری|نامزدهای انتخاباتی|کاندیدای انتخابات|election|electoral|presidential vote|parliamentary vote)/i.test(t)}
