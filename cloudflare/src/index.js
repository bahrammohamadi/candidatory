function fixEncoding(s){if(!/[ØÙÛÃ]/.test(s))return s;try{const bytes=Uint8Array.from([...s].map(ch=>ch.charCodeAt(0)&255));const t=new TextDecoder('utf-8',{fatal:true}).decode(bytes);return /[\u0600-\u06ff]/.test(t)?t:s}catch{return s}}
const FEEDS=[["ISNA","https://www.isna.ir/rss"],["Mehr","https://www.mehrnews.com/rss"],["BBCPersian","https://feeds.bbci.co.uk/persian/rss.xml"]]; // Unreachable feeds removed until verified
const KEYWORDS=[/انتخابات/i,/انتخاباتی/i,/نامزد/i,/کاندیدا/i,/رای.?گیری/i,/رأی.?گیری/i,/صلاحیت/i,/شورای شهر/i,/شوراهای اسلامی/i,/election/i,/candidate/i];
const LOCAL_ELECTION=/انتخابات (شورا|مجلس|ریاست.?جمهوری|خبرگان)|شوراهای اسلامی|شورای (اسلامی|شهر|روستا)|ستاد انتخابات|هیئت اجرایی انتخابات|صلاحیت (داوطلبان|نامزدها)|ثبت.?نام (داوطلبان|نامزدها)|کاندیداتوری|نامزدهای انتخابات|فرایند انتخابات/i;
const REJECT=[/فوتبال/i,/رمزارز/i,/آشپزی/i,/سریال تلویزیونی/i];
function decode(s){return s.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g,'$1').replace(/<[^>]*>/g,' ').replace(/&amp;/g,'&').replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/&lt;/g,'<').replace(/&gt;/g,'>').trim()}
function tag(xml,name){const re=new RegExp('<'+name+'(?:\\s[^>]*)?>([\\s\\S]*?)<\\/'+name+'>','i');const m=xml.match(re);return m?fixEncoding(decode(m[1])):''}
function parse(xml,source){return [...xml.matchAll(/<item(?:\s[^>]*)?>([\s\S]*?)<\/item>/gi)].slice(0,100).map(m=>({title:tag(m[1],'title'),link:tag(m[1],'link'),date:tag(m[1],'pubDate'),source})).filter(x=>x.title&&/^https?:\/\//.test(x.link))}
export default {async fetch(request,env){const u=new URL(request.url);if(u.pathname==='/health')return Response.json({ok:true,mode:'news-worker',publishing:env.PUBLISH_ENABLED==='true',telegramConfigured:!!env.TELEGRAM_BOT_TOKEN});if(u.pathname==='/bot-identity'){if(!env.TELEGRAM_BOT_TOKEN)return Response.json({ok:false,error:'missing_token'},{status:503});const r=await fetch('https://api.telegram.org/bot'+env.TELEGRAM_BOT_TOKEN+'/getMe');const j=await r.json();return Response.json({ok:j.ok,username:j.result?.username,botId:j.result?.id,error:j.description},{status:j.ok?200:502})}if(u.pathname==='/test-publish'){const key=request.headers.get('x-test-key');if(!env.TEST_PUBLISH_KEY||key!==env.TEST_PUBLISH_KEY)return new Response('Unauthorized',{status:401});if(!env.TELEGRAM_BOT_TOKEN)return Response.json({ok:false,error:'Missing Telegram secret'},{status:503});const payload={chat_id:'@candidatoryiran',text:'🧪 پیام آزمایشی کاندیداتوری\n\nاتصال Cloudflare Workers به کانال تلگرام در حال بررسی است. این پیام خبر نیست و صرفاً برای آزمایش ارسال شده است.\n\n@candidatoryiran'};const response=await fetch('https://api.telegram.org/bot'+env.TELEGRAM_BOT_TOKEN+'/sendMessage',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(payload)});const data=await response.json();return Response.json({ok:!!data.ok,messageId:data.result?.message_id,errorCode:data.error_code,description:data.ok?undefined:data.description},{status:data.ok?200:502})}if(u.pathname==='/diagnostics'){const checks=await Promise.all(FEEDS.map(async ([source,url])=>{try{const r=await fetch(url,{signal:AbortSignal.timeout(6500)});const raw=new TextDecoder('utf-8').decode(await r.arrayBuffer());const items=parse(raw,source);return {source,http:r.status,items:items.length,matching:items.filter(x=>isElection(x.title)).length,contentType:r.headers.get('content-type'),sample:items[0]?.title||raw.slice(0,70)}}catch(e){return {source,error:String(e)}}}));return Response.json({checks})}return new Response('Not found',{status:404})},async scheduled(event,env,ctx){ctx.waitUntil(Promise.allSettled([run(env,event.scheduledTime),runDailyContent(env,event.scheduledTime)]))}};
async function run(env,time){await checkTelegramOnce(env);let totalFound=0,totalAccepted=0;for(const [source,url] of FEEDS){let items=[];try{const r=await fetch(url,{signal:AbortSignal.timeout(7000),headers:{'user-agent':'CandidatoryNewsBot/7.0 (+RSS monitoring)'}});if(!r.ok)throw Error('RSS HTTP '+r.status);items=parse(new TextDecoder('utf-8').decode(await r.arrayBuffer()),source)}catch(e){console.error('fetch failed',source,String(e));continue}totalFound+=items.length;let accepted=0;for(const item of items){if(!isElection(item.title))continue;const id=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(item.link));const hash=[...new Uint8Array(id)].map(x=>x.toString(16).padStart(2,'0')).join('');if(env.NEWS_DB){const result=await env.NEWS_DB.prepare('INSERT OR IGNORE INTO articles (id,title,url,source,published_at,status) VALUES (?,?,?,?,?,?)').bind(hash,item.title,item.link,item.source,item.date||null,'pending_review').run();if(result.meta?.changes){accepted++;totalAccepted++}}}console.log(JSON.stringify({source,found:items.length,accepted}))}if(env.PUBLISH_ENABLED==='true'&&env.TELEGRAM_BOT_TOKEN){await publishOne(env)}console.log(JSON.stringify({totalFound,totalAccepted,publishing:env.PUBLISH_ENABLED==='true'}))}

function escapeHtml(s){return String(s??'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;')}
function getMeta(html,key){const tags=html.match(/<meta\b[^>]*>/gi)||[];for(const t of tags){const attrs={};for(const m of t.matchAll(/([\w:-]+)\s*=\s*(?:"([^"]*)"|'([^']*)')/g))attrs[m[1].toLowerCase()]=m[2]??m[3];if(attrs.property===key||attrs.name===key)return attrs.content||''}return ''}
function cleanSummary(s){return fixEncoding(String(s||'').replace(/<[^>]*>/g,' ').replace(/&amp;/g,'&').replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/\s+/g,' ').trim())}
async function publishOne(env){
 const row=await env.NEWS_DB.prepare("SELECT id,title,url,source FROM articles WHERE status='pending_review' ORDER BY created_at ASC LIMIT 1").first();
 if(!row)return;
 let link;try{const u=new URL(row.url);if(!['http:','https:'].includes(u.protocol))throw Error('invalid protocol');link=u.href}catch{return}
 let summary='',photo='';
 try{
  const r=await fetch(link,{signal:AbortSignal.timeout(5000),headers:{'user-agent':'Mozilla/5.0 (compatible; CandidatoryNewsBot/1.0)'}});
  if(r.ok){const html=(await r.text()).slice(0,250000);summary=cleanSummary(getMeta(html,'og:description')||getMeta(html,'description'));photo=getMeta(html,'og:image');if(photo)photo=new URL(photo,link).href}
 }catch(e){console.warn('Article metadata unavailable',row.source,String(e))}
 const title=escapeHtml(fixEncoding(row.title));
 const source=escapeHtml(row.source);
 const separator='━━━━━━━━━━━━━━';
 const footer='🇮🇷 کانال خبری کاندیداتوری\n🆔 @candidatoryiran\n🆔 Instagram.com/candidatory.ir';
 const lead='💠 <b>'+title+'</b>\n\n';
 const ending='\n\n📰 '+source+'\n\n'+footer+'\n\n'+separator+'\n🔗 <a href="'+escapeHtml(link)+'">مشاهده خبر در منبع اصلی</a>';
 const maxSummary=photo?Math.max(0,920-(lead+ending).length):Math.max(0,3800-(lead+ending).length);
 if(summary.length>maxSummary)summary=summary.slice(0,Math.max(0,maxSummary-1)).trimEnd()+'…';
 const text=lead+(summary?escapeHtml(summary):'')+ending;
 const base={chat_id:'@candidatoryiran',parse_mode:'HTML',link_preview_options:{is_disabled:true}};
 async function send(method,payload){const r=await fetch('https://api.telegram.org/bot'+env.TELEGRAM_BOT_TOKEN+'/'+method,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(payload)});return r.json()}
 try{
  let data;
  if(photo&&/^https:\/\//.test(photo)&&text.length<1024)data=await send('sendPhoto',{chat_id:base.chat_id,photo,caption:text,parse_mode:'HTML'});
  if(!data?.ok){if(data)console.warn('Photo send failed; text fallback',data.description);data=await send('sendMessage',{...base,text})}
  if(!data.ok){console.error('Telegram publish failed',data.error_code,data.description);return}
  await env.NEWS_DB.prepare("UPDATE articles SET status='published' WHERE id=?").bind(row.id).run();
  console.log('Published news',row.id,data.result?.message_id,photo?'photo-attempted':'text')
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

const DAILY_SLOTS=[
 ['08:00','morning','☀️ صبح بخیر | انرژی مثبت'],
 ['09:30','calendar','📅 تقویم و مناسبت امروز'],
 ['11:00','quote','📜 سخن بزرگان'],
 ['12:30','fact','🧠 آیا می‌دانستید؟'],
 ['14:30','selfcare','🌿 یک دقیقه برای خودت'],
 ['16:00','story','📖 داستان کوتاه روز'],
 ['18:30','question','💬 سؤال روز'],
 ['21:30','night','🌙 شب بخیر']
];
const DAILY_LIBRARY={
 morning:['امروز یک فرصت تازه است؛ حتی یک قدم کوچک هم می‌تواند مسیر روزت را تغییر دهد.','برای شروع خوب، لازم نیست همه‌چیز عالی باشد. از همان کاری شروع کن که امروز در توان توست.','امروز به خودت فرصت یادگرفتن، تجربه‌کردن و بهترشدن بده.'],
 calendar:['امروز فرصتی است برای مرور رویدادهای روز و یادگرفتن چیزی تازه. برای مناسبت‌ها تنها به تقویم رسمی و منابع قابل استناد رجوع می‌کنیم.'],
 quote:['یادآوری امروز: پیش از نقل هر سخن از بزرگان، باید متن و گوینده آن از منبع معتبر تأیید شود. امروز به جای نقل‌قول بدون منبع، یک فکر ساده داریم: شنیدن با دقت، آغاز گفت‌وگوی خوب است.','اندیشه روز: پرسیدن یک سؤال خوب گاهی از عجله برای دادن یک پاسخ مهم‌تر است.'],
 fact:['یک دانستنی زبانی: «همدلی» به معنی تلاش برای درک احساس و تجربه دیگری است؛ با موافقت‌کردن با همه نظرهای او تفاوت دارد.','یک نکته کاربردی: یادداشت‌کردن کارهای مهم روز کمک می‌کند آن‌ها را از کارهای فوری اما کم‌اهمیت جدا کنیم.'],
 selfcare:['برای یک دقیقه، از صفحه فاصله بگیر؛ شانه‌هایت را رها کن و چند نفس آرام بکش.','یک استراحت کوتاه میان کارها می‌تواند فرصتی برای تجدید تمرکز باشد.'],
 story:['داستانک امروز (تخیلی): دو دوست برای رسیدن به مقصد راه افتادند. یکی تمام مسیر را از طولانی‌بودن راه گفت؛ دیگری هر چند قدم، منظره‌ای تازه پیدا کرد. هر دو به مقصد رسیدند، اما خاطره‌های متفاوتی با خود بردند.','داستانک امروز (تخیلی): باغبانی هر روز به نهالی آب می‌داد. رهگذری پرسید چرا هنوز سایه‌ای ندارد؟ باغبان گفت: برای سایه فردا، باید امروز مراقبش باشم.'],
 question:['سؤال امروز: کدام عادت کوچک، بیشترین تأثیر مثبت را روی روز شما گذاشته است؟','اگر بتوانید فقط یک مهارت تازه یاد بگیرید، کدام را انتخاب می‌کنید؟','چه کتابی را دوست دارید دیگران هم بخوانند؟'],
 night:['امشب لازم نیست تمام کارهای ناتمام را با خودت به خواب ببری. فردا فرصتی تازه است. شب آرام.','روز با همه فراز و نشیب‌هایش گذشت. برای تلاش‌های امروزت ارزش قائل باش. شب خوش.']
};
function tehranClock(time){const parts=new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Tehran',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(new Date(time));const p=Object.fromEntries(parts.map(x=>[x.type,x.value]));return {date:p.year+'-'+p.month+'-'+p.day,clock:p.hour+':'+p.minute,day:Number(p.day)}}
function normalizeDaily(s){return String(s).normalize('NFKC').replace(/[ي]/g,'ی').replace(/[ك]/g,'ک').replace(/\s+/g,' ').trim()}
function validDaily(s,kind){if(typeof s!=='string'||s.length<40||s.length>1100||!/[\u0600-\u06FF]/.test(s))return false;if(/https?:\/\/|@\w+|#[\w\u0600-\u06FF]+/.test(s))return false;if(/\b(?:vote for|رأی بدهید به|رای بدهید به|حمایت از نامزد)\b/i.test(s))return false;if(kind==='quote'&&/«|»|گفت:|به گفته/.test(s))return false;return true}
function selectDaily(kind,day){const a=DAILY_LIBRARY[kind];return a[(day+DAILY_SLOTS.findIndex(x=>x[1]===kind))%a.length]}
const DAILY_PROMPTS=['یک کار نیمه‌تمام کوچک را تمام کن.','امروز به کسی برای کمکش تشکر کن.','ده دقیقه به یادگیری چیزی تازه اختصاص بده.','یک گفت‌وگوی خوب را با گوش‌دادن شروع کن.','یک کار مهم را پیش از کارهای کم‌اهمیت انجام بده.','برای استراحت کوتاه در برنامه‌ات جا باز کن.','یک نکته خوب از امروز را یادداشت کن.','یک پیام محبت‌آمیز به دوستی بفرست.','کاری را که مدت‌ها عقب انداخته‌ای به قدم‌های کوچک تقسیم کن.','یک سؤال تازه درباره موضوع مورد علاقه‌ات بپرس.','امروز با حوصله بیشتری به دیگران گوش بده.','یک پیاده‌روی کوتاه در صورت امکان داشته باش.','برای یک هدف کوچک زمان مشخص تعیین کن.','یک مهارت ساده را تمرین کن.','یک خاطره خوب را با کسی مرور کن.','فضای کار یا مطالعه‌ات را کمی مرتب کن.','برای فردا یک اولویت روشن انتخاب کن.','امروز یک کار را بدون عجله انجام بده.','یک کتاب یا مطلب مفید به کسی معرفی کن.','برای یک نفر آرزوی روز خوبی داشته باش.','به پیشرفت‌های کوچک خودت توجه کن.','یک ایده تازه را روی کاغذ بنویس.','یک عادت مفید را حتی برای پنج دقیقه تمرین کن.','برای پرسیدن حال یک دوست وقت بگذار.','یک کار خلاقانه هرچند کوچک انجام بده.','یک موضوع تازه را از منبع معتبر بررسی کن.','برای خودت یک فرصت کوتاه سکوت بساز.','یک هدف دور را به یک اقدام امروز تبدیل کن.','یک تجربه تازه را با کسی به اشتراک بگذار.','یک نکته آموزنده از اشتباه گذشته یادداشت کن.','امروز به یک چیز ساده و خوب توجه کن.'];
async function runDailyContent(env,scheduledTime){
 if(env.PUBLISH_ENABLED!=='true'||!env.TELEGRAM_BOT_TOKEN||!env.NEWS_DB)return;
 const local=tehranClock(scheduledTime);const slot=DAILY_SLOTS.find(x=>x[0]===local.clock);if(!slot)return;
 await env.NEWS_DB.prepare("CREATE TABLE IF NOT EXISTS daily_content (id TEXT PRIMARY KEY, kind TEXT NOT NULL, content TEXT NOT NULL, status TEXT NOT NULL, message_id INTEGER, error TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP, sent_at TEXT)").run();
 const id=local.date+':'+slot[1];const existing=await env.NEWS_DB.prepare('SELECT status FROM daily_content WHERE id=?').bind(id).first();if(existing?.status==='published'||existing?.status==='sending')return;
 let content=selectDaily(slot[1],local.day);if(slot[1]==='calendar'){const fa=new Intl.DateTimeFormat('fa-IR-u-ca-persian',{timeZone:'Asia/Tehran',weekday:'long',day:'numeric',month:'long',year:'numeric'}).format(new Date(scheduledTime));content='📅 امروز '+fa+' است. مناسبت‌های رسمی تنها پس از بررسی در منابع معتبر اعلام می‌شوند. هر روز فرصتی تازه برای شناخت بهتر تاریخ و فرهنگ ماست.'}else{content+='\n\n✨ پیشنهاد کوچک امروز: '+DAILY_PROMPTS[(local.day-1)%DAILY_PROMPTS.length]}
 // Optional Gemini enrichment. Without a configured key, publish only locally reviewed, attributed-as-original material.
 if(env.GEMINI_API_KEY&&slot[1]!=='calendar'&&slot[1]!=='quote'){
  try{
   const instruction='یک متن کوتاه، دقیق، مثبت، بی‌طرف، فارسی و اصیل برای بخش '+slot[2]+' بنویس. ۴۰ تا ۱۰۰ کلمه. فقط متن. بدون نقل قول از اشخاص، ادعای علمی/تاریخی غیرقابل بررسی، هشتگ، لینک، سیاست یا تبلیغ.';
   const response=await fetch('https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent',{method:'POST',headers:{'content-type':'application/json','x-goog-api-key':env.GEMINI_API_KEY},body:JSON.stringify({contents:[{parts:[{text:instruction}]}],generationConfig:{temperature:0.65,maxOutputTokens:350}}),signal:AbortSignal.timeout(8000)});
   if(response.ok){const j=await response.json();const candidate=j.candidates?.[0]?.content?.parts?.map(p=>p.text||'').join('')?.trim();if(validDaily(candidate,slot[1]))content=candidate}
  }catch(e){console.warn('Gemini unavailable; curated fallback',String(e))}
 }
 content=normalizeDaily(content);
 if(!validDaily(content,slot[1])){console.error('daily content rejected',slot[1]);return}
 const previous=await env.NEWS_DB.prepare("SELECT id FROM daily_content WHERE content=? AND status='published' LIMIT 1").bind(content).first();
 if(previous){console.warn('duplicate daily content rejected',id);return}
 const insert=await env.NEWS_DB.prepare("INSERT OR IGNORE INTO daily_content (id,kind,content,status) VALUES (?,?,?,'sending')").bind(id,slot[1],content).run();
 if(!insert.meta?.changes)return;
 const msg=slot[2]+'\n\n'+escapeHtml(content)+'\n\n━━━━━━━━━━━━━━\n🇮🇷 کانال خبری کاندیداتوری\n🆔 @candidatoryiran';
 try{
  const response=await fetch('https://api.telegram.org/bot'+env.TELEGRAM_BOT_TOKEN+'/sendMessage',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({chat_id:'@candidatoryiran',text:msg,parse_mode:'HTML',link_preview_options:{is_disabled:true}}),signal:AbortSignal.timeout(10000)});
  const data=await response.json();
  await env.NEWS_DB.prepare('UPDATE daily_content SET status=?,message_id=?,error=?,sent_at=CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END WHERE id=?').bind(data.ok?'published':'failed',data.result?.message_id??null,data.ok?null:String(data.description||'telegram error'),data.ok?1:0,id).run();
  console.log('daily content',id,data.ok,data.result?.message_id);
 }catch(e){await env.NEWS_DB.prepare("UPDATE daily_content SET status='failed',error=? WHERE id=?").bind(String(e),id).run();console.error('daily send failed',id,String(e))}
}
