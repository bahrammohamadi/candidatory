function fixEncoding(s){if(!/[ØÙÛÃ]/.test(s))return s;try{const bytes=Uint8Array.from([...s].map(ch=>ch.charCodeAt(0)&255));const t=new TextDecoder('utf-8',{fatal:true}).decode(bytes);return /[\u0600-\u06ff]/.test(t)?t:s}catch{return s}}
const FEEDS=[["ISNA","https://www.isna.ir/rss"],["Mehr","https://www.mehrnews.com/rss"],["BBCPersian","https://feeds.bbci.co.uk/persian/rss.xml"]]; // Unreachable feeds removed until verified
const KEYWORDS=[/انتخابات/i,/انتخاباتی/i,/نامزد/i,/کاندیدا/i,/رای.?گیری/i,/رأی.?گیری/i,/صلاحیت/i,/شورای شهر/i,/شوراهای اسلامی/i,/election/i,/candidate/i];
const LOCAL_ELECTION=/انتخابات (شورا|مجلس|ریاست.?جمهوری|خبرگان)|شوراهای اسلامی|شورای (اسلامی|شهر|روستا)|ستاد انتخابات|هیئت اجرایی انتخابات|صلاحیت (داوطلبان|نامزدها)|ثبت.?نام (داوطلبان|نامزدها)|کاندیداتوری|نامزدهای انتخابات|فرایند انتخابات/i;
const REJECT=[/فوتبال/i,/رمزارز/i,/آشپزی/i,/سریال تلویزیونی/i];
function decode(s){return s.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g,'$1').replace(/<[^>]*>/g,' ').replace(/&amp;/g,'&').replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/&lt;/g,'<').replace(/&gt;/g,'>').trim()}
function tag(xml,name){const re=new RegExp('<'+name+'(?:\\s[^>]*)?>([\\s\\S]*?)<\\/'+name+'>','i');const m=xml.match(re);return m?fixEncoding(decode(m[1])):''}
function parse(xml,source){return [...xml.matchAll(/<item(?:\s[^>]*)?>([\s\S]*?)<\/item>/gi)].slice(0,100).map(m=>({title:tag(m[1],'title'),link:tag(m[1],'link'),date:tag(m[1],'pubDate'),source})).filter(x=>x.title&&/^https?:\/\//.test(x.link))}
export default {async fetch(request,env){const u=new URL(request.url);if(u.pathname==='/health')return Response.json({ok:true,mode:'news-worker',publishing:env.PUBLISH_ENABLED==='true',telegramConfigured:!!env.TELEGRAM_BOT_TOKEN});if(u.pathname==='/bot-identity'){if(!env.TELEGRAM_BOT_TOKEN)return Response.json({ok:false,error:'missing_token'},{status:503});const r=await fetch('https://api.telegram.org/bot'+env.TELEGRAM_BOT_TOKEN+'/getMe');const j=await r.json();return Response.json({ok:j.ok,username:j.result?.username,botId:j.result?.id,error:j.description},{status:j.ok?200:502})}if(u.pathname==='/test-publish'){const key=request.headers.get('x-test-key');if(!env.TEST_PUBLISH_KEY||key!==env.TEST_PUBLISH_KEY)return new Response('Unauthorized',{status:401});if(!env.TELEGRAM_BOT_TOKEN)return Response.json({ok:false,error:'Missing Telegram secret'},{status:503});const payload={chat_id:'@candidatoryiran',text:'🧪 پیام آزمایشی کاندیداتوری\n\nاتصال Cloudflare Workers به کانال تلگرام در حال بررسی است. این پیام خبر نیست و صرفاً برای آزمایش ارسال شده است.\n\n@candidatoryiran'};const response=await fetch('https://api.telegram.org/bot'+env.TELEGRAM_BOT_TOKEN+'/sendMessage',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(payload)});const data=await response.json();return Response.json({ok:!!data.ok,messageId:data.result?.message_id,errorCode:data.error_code,description:data.ok?undefined:data.description},{status:data.ok?200:502})}if(u.pathname==='/diagnostics'){const checks=await Promise.all(FEEDS.map(async ([source,url])=>{try{const r=await fetch(url,{signal:AbortSignal.timeout(6500)});const raw=new TextDecoder('utf-8').decode(await r.arrayBuffer());const items=parse(raw,source);return {source,http:r.status,items:items.length,matching:items.filter(x=>isElection(x.title)).length,contentType:r.headers.get('content-type'),sample:items[0]?.title||raw.slice(0,70)}}catch(e){return {source,error:String(e)}}}));return Response.json({checks})}return new Response('Not found',{status:404})},async scheduled(event,env,ctx){ctx.waitUntil(Promise.allSettled([run(env,event.scheduledTime),runDailyContent(env,event.scheduledTime),probeGeminiOnce(env)]))}};
const NEWS_MAX_AGE_HOURS=36;
function canonicalNewsUrl(raw){try{const u=new URL(raw);u.hash='';u.hostname=u.hostname.toLowerCase().replace(/^www\./,'');for(const k of [...u.searchParams.keys()])if(/^(utm_|fbclid$|gclid$|ref$|source$|output$)/i.test(k))u.searchParams.delete(k);u.searchParams.sort();u.pathname=u.pathname.replace(/\/$/,'')||'/';return u.href}catch{return raw}}
function normalizedHeadline(s){return fixEncoding(s).normalize('NFKC').toLowerCase().replace(/[ي]/g,'ی').replace(/[ك]/g,'ک').replace(/[أإآ]/g,'ا').replace(/[^\p{L}\p{N}]+/gu,' ').replace(/\s+/g,' ').trim()}
function newsTokens(s){const stop=new Set(['از','در','به','با','برای','که','این','آن','یک','شد','است','را','و','یا','های','درباره','خبر','اعلام','گفت','کرد','بر','the','and','for','with','news']);return new Set(normalizedHeadline(s).split(' ').filter(w=>w.length>2&&!stop.has(w)))}
function similarity(a,b){const x=newsTokens(a),y=newsTokens(b);if(!x.size||!y.size)return 0;let common=0;for(const w of x)if(y.has(w))common++;return common/Math.max(1,Math.min(x.size,y.size))}
function isFreshNews(raw,now){if(!raw)return false;const ts=Date.parse(raw);return Number.isFinite(ts)&&ts<=now+15*60*1000&&ts>=now-NEWS_MAX_AGE_HOURS*3600000}
async function run(env,time){
 await checkTelegramOnce(env);
 const now=Number.isFinite(time)?time:Date.now();
 let totalFound=0,totalAccepted=0,stale=0,duplicate=0;
 for(const [source,url] of FEEDS){
  let items=[];
  try{const r=await fetch(url,{signal:AbortSignal.timeout(7000),headers:{'user-agent':'CandidatoryNewsBot/8.0 (+RSS monitoring)'}});if(!r.ok)throw Error('RSS HTTP '+r.status);items=parse(new TextDecoder('utf-8').decode(await r.arrayBuffer()),source)}
  catch(e){console.error('fetch failed',source,String(e));continue}
  totalFound+=items.length;let accepted=0;
  for(const item of items){
   if(!isElection(item.title))continue;
   if(!isFreshNews(item.date,now)){stale++;continue}
   const canonical=canonicalNewsUrl(item.link);
   const id=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(canonical));
   const hash=[...new Uint8Array(id)].map(x=>x.toString(16).padStart(2,'0')).join('');
   if(!env.NEWS_DB)continue;
   const recent=await env.NEWS_DB.prepare("SELECT id,title,url FROM articles WHERE created_at >= datetime('now','-7 days') ORDER BY created_at DESC LIMIT 400").all();
   const duplicateArticle=(recent.results||[]).some(old=>canonicalNewsUrl(old.url)===canonical||normalizedHeadline(old.title)===normalizedHeadline(item.title)||similarity(old.title,item.title)>=0.82);
   if(duplicateArticle){duplicate++;continue}
   const result=await env.NEWS_DB.prepare('INSERT OR IGNORE INTO articles (id,title,url,source,published_at,status) VALUES (?,?,?,?,?,?)').bind(hash,item.title,item.link,item.source,new Date(Date.parse(item.date)).toISOString(),'pending_review').run();
   if(result.meta?.changes){accepted++;totalAccepted++}
  }
  console.log(JSON.stringify({source,found:items.length,accepted}))
 }
 if(env.PUBLISH_ENABLED==='true'&&env.TELEGRAM_BOT_TOKEN)await publishOne(env);
 console.log(JSON.stringify({totalFound,totalAccepted,stale,duplicate,publishing:env.PUBLISH_ENABLED==='true'}))
}

function escapeHtml(s){return String(s??'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;')}
function getMeta(html,key){const tags=html.match(/<meta\b[^>]*>/gi)||[];for(const t of tags){const attrs={};for(const m of t.matchAll(/([\w:-]+)\s*=\s*(?:"([^"]*)"|'([^']*)')/g))attrs[m[1].toLowerCase()]=m[2]??m[3];if(attrs.property===key||attrs.name===key)return attrs.content||''}return ''}
function cleanSummary(s){return fixEncoding(String(s||'').replace(/<[^>]*>/g,' ').replace(/&amp;/g,'&').replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/\s+/g,' ').trim())}
async function publishOne(env){
 const row=await env.NEWS_DB.prepare("SELECT id,title,url,source FROM articles WHERE status='pending_review' ORDER BY created_at ASC LIMIT 1").first();
 if(!row)return;
 const previous=await env.NEWS_DB.prepare("SELECT title,url FROM articles WHERE status='published' AND created_at >= datetime('now','-7 days') ORDER BY created_at DESC LIMIT 400").all();
 if((previous.results||[]).some(x=>canonicalNewsUrl(x.url)===canonicalNewsUrl(row.url)||normalizedHeadline(x.title)===normalizedHeadline(row.title)||similarity(x.title,row.title)>=0.82)){await env.NEWS_DB.prepare("UPDATE articles SET status='duplicate' WHERE id=?").bind(row.id).run();return}
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
 ['08:00','morning','☀️ صبح بخیر'],
 ['08:45','focus','🎯 شروع روز'],
 ['09:30','calendar','📅 تقویم و مناسبت امروز'],
 ['10:15','reflection','💭 یک فکر کوتاه'],
 ['11:00','quote','📜 جمله روز'],
 ['11:45','microlearn','📚 نکته یادگیری'],
 ['12:30','fact','🧠 آیا می‌دانستید؟'],
 ['13:30','conversation','🗣️ مهارت گفت‌وگو'],
 ['14:30','selfcare','🌿 یک دقیقه برای خودت'],
 ['15:15','creativity','💡 ایده روز'],
 ['16:00','story','📖 داستان کوتاه روز'],
 ['17:15','habit','🌱 عادت کوچک'],
 ['18:30','question','💬 سؤال روز'],
 ['20:00','evening','🍃 یک مکث کوتاه'],
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
 night:['امروز هم گذشت؛ با همه خوب و بدش. امیدوارم امشب ذهنت کمی آرام بگیرد و فردا را با حال بهتری شروع کنی. شب‌تون آروم 🤍','دیگه وقتشه گوشی رو کنار بذاریم و یه کم استراحت کنیم. امیدوارم شب خوبی داشته باشین و فردا خبرهای بهتری بشنویم. شب بخیر 🤍','هر جور که امروز گذشت، حالا وقت استراحته. امیدوارم امشب خواب راحتی داشته باشین. شب‌تون خوش 🌙','امیدوارم امشب برای همه‌تون آروم بگذره. مراقب خودتون باشین و خوب استراحت کنین. شب بخیر 🤍']
};
const EXTRA_LIBRARY={
 focus:['برای شروع روز لازم نیست همه کارها را با هم انجام بدهیم. یک کار مهم را انتخاب کنیم و همان را با حوصله پیش ببریم.','گاهی بهترین شروع، ساده‌ترین شروع است. امروز از کاری آغاز کنیم که واقعاً در توانمان است.'],
 reflection:['بعضی گفت‌وگوها با یک سؤال خوب شروع می‌شوند، نه با یک جواب آماده.','لازم نیست درباره همه‌چیز نظر قطعی داشته باشیم؛ گاهی شنیدن، خودش یک انتخاب خوب است.'],
 microlearn:['برای یادگرفتن یک موضوع، آن را با زبان ساده برای خودمان توضیح بدهیم. هرجا توضیح سخت شد، همان‌جا جای مطالعه بیشتر است.','وقتی چیزی می‌خوانیم، یک سؤال از متن بسازیم. این کار کمک می‌کند فعال‌تر به موضوع فکر کنیم.'],
 conversation:['در گفت‌وگوی امروز، پیش از پاسخ دادن یک لحظه مکث کنیم و مطمئن شویم منظور طرف مقابل را درست فهمیده‌ایم.','برای مخالفت محترمانه می‌شود از این جمله شروع کرد: من موضوع را کمی متفاوت می‌بینم.'],
 creativity:['یک وسیله معمولی اطرافت را نگاه کن و فکر کن چه استفاده تازه‌ای می‌توان برایش پیدا کرد.','اگر قرار بود یک کار تکراری را ساده‌تر کنی، اولین تغییری که می‌دادی چه بود؟'],
 habit:['یک عادت کوچک را انتخاب کن که انجام دادنش کمتر از دو دقیقه زمان ببرد. همین شروع ساده کافی است.','برای کاری که دوست داری به عادت تبدیل شود، زمان و جای مشخصی در نظر بگیر.'],
 evening:['این ساعت از روز، یک مکث کوتاه هم غنیمت است. امیدوارم تا اینجا روز خوبی داشته باشین.','گاهی چند دقیقه دورشدن از شلوغی روز، حال آدم را بهتر می‌کند. امیدوارم عصر آرومی داشته باشین.']
};
const CONTENT_PROMPTS={
 morning:'صبح‌بخیر صمیمی، کوتاه، شاد و بدون شعار؛ ۲ تا ۳ جمله.',
 focus:'یک پیشنهاد عملی بسیار کوچک برای شروع روز؛ بدون نصیحت یا کلیشه.',
 reflection:'یک تأمل کوتاه و اصیل درباره زندگی روزمره؛ بدون انتساب به شخصیت واقعی.',
 quote:'یک جمله تأمل‌برانگیز اصیل؛ هیچ نام یا نقل‌قول ساختگی نیاور.',
 microlearn:'یک تکنیک ساده و بی‌خطر یادگیری با مثال کوتاه؛ بدون ادعای علمی.',
 fact:'یک دانستنی عمومی کم‌ریسک و روشن؛ هیچ عدد، آمار، تاریخ یا ادعای تأییدنشده نیاور.',
 conversation:'یک پیشنهاد کوتاه برای گفت‌وگوی محترمانه و گوش‌دادن.',
 selfcare:'یک استراحت ساده و بی‌خطر در یک دقیقه؛ بدون ادعای درمانی.',
 creativity:'یک تمرین خلاقیت خیلی کوچک و جذاب برای امروز.',
 story:'یک داستانک تخیلی اصیل ۴ تا ۶ جمله‌ای با پایان ظریف، بدون موعظه.',
 habit:'یک عادت کوچک قابل انجام و مشخص، بدون شعار.',
 question:'یک سؤال مشخص و جذاب که مخاطبان بتوانند در ذهن خود پاسخ بدهند؛ بدون جهت‌دهی سیاسی.',
 evening:'یک پیام کوتاه و طبیعی برای عصر، بدون شعار و کلیشه.',
 night:'شب‌بخیر گرم و خودمانی در ۲ تا ۳ جمله کوتاه؛ بدون توصیه، هدف‌گذاری یا جملات انگیزشی.'
};
const GENERIC_REJECT=/مناسبت‌های رسمی تنها|پس از بررسی در منابع معتبر|به جای نقل‌قول بدون منبع|پیشنهاد کوچک امروز|فرصتی تازه برای شناخت|به عنوان یک مدل زبان|هوش مصنوعی|در این بخش|متن پیشنهادی|پرامپت|تولید محتوا/i;
function contentQuality(s,kind){return validDaily(s,kind)&&!GENERIC_REJECT.test(s)&&(!['night','evening'].includes(kind)||s.length<=340)}
function persianDateParts(time){
 const p=Object.fromEntries(new Intl.DateTimeFormat('en-US-u-ca-persian',{timeZone:'Asia/Tehran',year:'numeric',month:'numeric',day:'numeric'}).formatToParts(new Date(time)).map(x=>[x.type,x.value]));
 return {year:Number(p.year),month:Number(p.month),day:Number(p.day)};
}
async function calendarEvents(time){
 const {year,month,day}=persianDateParts(time);
 const date=[year,String(month).padStart(2,'0'),String(day).padStart(2,'0')].join('-');
 const sources=[
  {name:'Persian Calendar',url:'https://www.persian-calendar.com/api/events?year='+year+'&month_sh='+month+'&day_sh='+day},
  {name:'FarsiUI',url:'https://raw.githubusercontent.com/MiladJoodi/FarsiUI/main/calendar/years/'+year+'/events.json'}
 ];
 const collected=[];
 for(const source of sources){
  try{
   const r=await fetch(source.url,{headers:{'Accept':'application/json','User-Agent':'CandidatoryCalendar/1.0'},signal:AbortSignal.timeout(5500)});
   if(!r.ok)throw Error('HTTP '+r.status);
   const json=await r.json();
   const records=Array.isArray(json.data)?json.data:Array.isArray(json.items)?json.items:[];
   for(const item of records){
    const match=source.name==='Persian Calendar'?Number(item.year_sh)===year&&Number(item.month_sh)===month&&Number(item.day_sh)===day:String(item.date)===date;
    const title=source.name==='Persian Calendar'?item.title_fa:item.title?.fa;
    const category=String(item.category||'');
    if(match&&typeof title==='string'&&title.trim().length>2&&title.length<110&&!/https?:|\n|\r/.test(title)&&!['fun','countries'].includes(category))collected.push({title:title.trim(),source:source.name,holiday:!!item.is_holiday});
   }
   console.log('calendar source',source.name,'records',records.length,'matched',collected.length);
  }catch(e){console.warn('calendar source failed',source.name,String(e))}
 }
 const unique=new Map();
 for(const event of collected){const key=event.title.normalize('NFKC').replace(/\s+/g,' ').replace(/[ي]/g,'ی').replace(/[ك]/g,'ک');if(!unique.has(key))unique.set(key,event)}
 return [...unique.values()].slice(0,5);
}
async function verifiedCalendar(time){
 const d=new Date(time);
 const solar=new Intl.DateTimeFormat('fa-IR-u-ca-persian',{timeZone:'Asia/Tehran',weekday:'long',day:'numeric',month:'long',year:'numeric'}).format(d);
 const greg=new Intl.DateTimeFormat('fa-IR-u-ca-gregory',{timeZone:'Asia/Tehran',day:'numeric',month:'long',year:'numeric'}).format(d);
 const islamic=new Intl.DateTimeFormat('fa-IR-u-ca-islamic-civil',{timeZone:'Asia/Tehran',day:'numeric',month:'long',year:'numeric'}).format(d);
 const events=await calendarEvents(time);
 const lines=['📅 '+solar,'🗓 میلادی: '+greg,'🌙 قمری (محاسباتی): '+islamic];
 if(events.length){lines.push('','📌 مناسبت‌های امروز:');for(const event of events)lines.push('• '+event.title+(event.holiday?' (تعطیل رسمی)':''))}
 return lines.join('\n');
}
function tehranClock(time){const parts=new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Tehran',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(new Date(time));const p=Object.fromEntries(parts.map(x=>[x.type,x.value]));return {date:p.year+'-'+p.month+'-'+p.day,clock:p.hour+':'+p.minute,day:Number(p.day)}}
function normalizeDaily(s){return String(s).normalize('NFKC').replace(/[ي]/g,'ی').replace(/[ك]/g,'ک').replace(/\s+/g,' ').trim()}
function validDaily(s,kind){if(typeof s!=='string'||s.length<40||s.length>1100||!/[\u0600-\u06FF]/.test(s))return false;if(/https?:\/\/|@\w+|#[\w\u0600-\u06FF]+/.test(s))return false;if(/\b(?:vote for|رأی بدهید به|رای بدهید به|حمایت از نامزد)\b/i.test(s))return false;if(kind==='quote'&&/«|»|گفت:|به گفته/.test(s))return false;return true}
function selectDaily(kind,day){const a=DAILY_LIBRARY[kind]||EXTRA_LIBRARY[kind];return a[(day+DAILY_SLOTS.findIndex(x=>x[1]===kind))%a.length]}
const DAILY_PROMPTS=['یک کار نیمه‌تمام کوچک را تمام کن.','امروز به کسی برای کمکش تشکر کن.','ده دقیقه به یادگیری چیزی تازه اختصاص بده.','یک گفت‌وگوی خوب را با گوش‌دادن شروع کن.','یک کار مهم را پیش از کارهای کم‌اهمیت انجام بده.','برای استراحت کوتاه در برنامه‌ات جا باز کن.','یک نکته خوب از امروز را یادداشت کن.','یک پیام محبت‌آمیز به دوستی بفرست.','کاری را که مدت‌ها عقب انداخته‌ای به قدم‌های کوچک تقسیم کن.','یک سؤال تازه درباره موضوع مورد علاقه‌ات بپرس.','امروز با حوصله بیشتری به دیگران گوش بده.','یک پیاده‌روی کوتاه در صورت امکان داشته باش.','برای یک هدف کوچک زمان مشخص تعیین کن.','یک مهارت ساده را تمرین کن.','یک خاطره خوب را با کسی مرور کن.','فضای کار یا مطالعه‌ات را کمی مرتب کن.','برای فردا یک اولویت روشن انتخاب کن.','امروز یک کار را بدون عجله انجام بده.','یک کتاب یا مطلب مفید به کسی معرفی کن.','برای یک نفر آرزوی روز خوبی داشته باش.','به پیشرفت‌های کوچک خودت توجه کن.','یک ایده تازه را روی کاغذ بنویس.','یک عادت مفید را حتی برای پنج دقیقه تمرین کن.','برای پرسیدن حال یک دوست وقت بگذار.','یک کار خلاقانه هرچند کوچک انجام بده.','یک موضوع تازه را از منبع معتبر بررسی کن.','برای خودت یک فرصت کوتاه سکوت بساز.','یک هدف دور را به یک اقدام امروز تبدیل کن.','یک تجربه تازه را با کسی به اشتراک بگذار.','یک نکته آموزنده از اشتباه گذشته یادداشت کن.','امروز به یک چیز ساده و خوب توجه کن.'];
async function runDailyContent(env,scheduledTime){
 if(env.PUBLISH_ENABLED!=='true'||!env.TELEGRAM_BOT_TOKEN||!env.NEWS_DB)return;
 const local=tehranClock(scheduledTime);const slot=DAILY_SLOTS.find(x=>x[0]===local.clock);if(!slot)return;
 await env.NEWS_DB.prepare("CREATE TABLE IF NOT EXISTS daily_content (id TEXT PRIMARY KEY, kind TEXT NOT NULL, content TEXT NOT NULL, status TEXT NOT NULL, message_id INTEGER, error TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP, sent_at TEXT)").run();
 const id=local.date+':'+slot[1];const existing=await env.NEWS_DB.prepare('SELECT status FROM daily_content WHERE id=?').bind(id).first();if(existing?.status==='published'||existing?.status==='sending')return;
 let content=slot[1]==='calendar'?await verifiedCalendar(scheduledTime):selectDaily(slot[1],local.day);
 if(env.GEMINI_API_KEY&&slot[1]!=='calendar'){
  try{
   const instruction='تو سردبیر یک کانال فارسی با لحن طبیعی، صمیمی، دقیق و بی‌طرف هستی. موضوع: '+slot[2]+'. هدف: '+CONTENT_PROMPTS[slot[1]]+' فقط متن نهایی قابل انتشار را بنویس. نه عنوان، نه توضیح فرایند، نه برچسب، نه نقل قول از افراد واقعی، نه هشتگ، نه لینک، نه ادعای تاریخی یا علمی تاییدنشده. هر پیام باید متفاوت و طبیعی باشد. متن ۴۰ تا ۹۰ کلمه، به جز شب‌بخیر که حداکثر ۵۵ کلمه باشد. تاریخ روز: '+local.date+'.';
   const response=await fetch('https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent',{method:'POST',headers:{'content-type':'application/json','x-goog-api-key':env.GEMINI_API_KEY},body:JSON.stringify({contents:[{parts:[{text:instruction}]}],generationConfig:{temperature:0.85,maxOutputTokens:420}}),signal:AbortSignal.timeout(10000)});
   if(response.ok){const j=await response.json();const candidate=j.candidates?.[0]?.content?.parts?.map(p=>p.text||'').join('')?.trim();if(contentQuality(candidate,slot[1]))content=candidate}
  }catch(e){console.warn('Gemini unavailable; fallback',String(e))}
 }
 content=normalizeDaily(content);
 if(!contentQuality(content,slot[1])){console.error('daily content rejected',slot[1]);return}
 const previous=await env.NEWS_DB.prepare("SELECT id FROM daily_content WHERE content=? AND status='published' LIMIT 1").bind(content).first();
 if(previous){console.warn('duplicate daily content rejected',id);return}
 const insert=await env.NEWS_DB.prepare("INSERT OR IGNORE INTO daily_content (id,kind,content,status) VALUES (?,?,?,'sending')").bind(id,slot[1],content).run();
 if(!insert.meta?.changes)return;
 const msg=slot[2]+'\n\n'+escapeHtml(content);
 try{
  const response=await fetch('https://api.telegram.org/bot'+env.TELEGRAM_BOT_TOKEN+'/sendMessage',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({chat_id:'@candidatoryiran',text:msg,parse_mode:'HTML',link_preview_options:{is_disabled:true}}),signal:AbortSignal.timeout(10000)});
  const data=await response.json();
  await env.NEWS_DB.prepare('UPDATE daily_content SET status=?,message_id=?,error=?,sent_at=CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END WHERE id=?').bind(data.ok?'published':'failed',data.result?.message_id??null,data.ok?null:String(data.description||'telegram error'),data.ok?1:0,id).run();
  console.log('daily content',id,data.ok,data.result?.message_id);
 }catch(e){await env.NEWS_DB.prepare("UPDATE daily_content SET status='failed',error=? WHERE id=?").bind(String(e),id).run();console.error('daily send failed',id,String(e))}
}

async function probeGeminiOnce(env){
 if(!env.NEWS_DB)return;
 await env.NEWS_DB.prepare("CREATE TABLE IF NOT EXISTS news_diagnostics (name TEXT PRIMARY KEY, result TEXT, updated_at TEXT DEFAULT CURRENT_TIMESTAMP)").run();
 const key='gemini_probe_20261028_v1';
 const claim=await env.NEWS_DB.prepare("INSERT OR IGNORE INTO news_diagnostics(name,result) VALUES(?,'started')").bind(key).run();
 if(!claim.meta?.changes)return;
 let result='unknown';
 if(!env.GEMINI_API_KEY)result='missing_secret';
 else try{
  const response=await fetch('https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent',{method:'POST',headers:{'content-type':'application/json','x-goog-api-key':env.GEMINI_API_KEY},body:JSON.stringify({contents:[{parts:[{text:'یک جمله کوتاه و مثبت به فارسی بنویس؛ فقط متن و بدون نقل قول.'}]}],generationConfig:{maxOutputTokens:160}}),signal:AbortSignal.timeout(10000)});
  const data=await response.json();
  const output=data.candidates?.[0]?.content?.parts?.map(p=>p.text||'').join('')||'';
  result=response.ok?(validDaily(output,'morning')?'ok_valid':'ok_but_validation_failed'):'http_'+response.status+':'+String(data.error?.message||'').slice(0,220);
 }catch(e){result='error:'+String(e).slice(0,220)}
 await env.NEWS_DB.prepare('UPDATE news_diagnostics SET result=?,updated_at=CURRENT_TIMESTAMP WHERE name=?').bind(result,key).run();
 console.log('Gemini probe result',result)
}
