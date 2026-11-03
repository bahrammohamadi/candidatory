import test from "node:test";
import assert from "node:assert/strict";
import {classifyForInstagram,queueEligibleTelegramNews,INSTAGRAM_CATEGORY_COLORS} from "../cloudflare-instagram-sidecar.mjs";

test("news labels are conservative, disjoint and from approved branding",()=>{
 assert.equal(classifyForInstagram("خبر فوری: نتایج انتخابات اعلام شد"),"breaking");
 assert.equal(classifyForInstagram("انتخابات کنگره و رأی گیری"),"election");
 assert.equal(classifyForInstagram("پیروزی احتمالی چه پیامدهایی دارد؟"),"analysis");
 assert.equal(classifyForInstagram("گزارش درباره فوتبال لیگ"),null);
 assert.equal(classifyForInstagram("قیمت بالای سوخت در روز انتخابات کنگره بیشتر شد"),null);
 assert.equal(classifyForInstagram("هدف مذاکرات کنونی آمریکا با ایران / ترامپ در انتخابات شکست می‌خورد"),null);
 assert.equal(classifyForInstagram("ادعاهای اخیر درباره افکارسنجی #انتخابات"),null);
 assert.equal(classifyForInstagram("جریمندرینگ: مهندسی حوزه‌های رای‌گیری"),"analysis");
 assert.deepEqual(Object.keys(INSTAGRAM_CATEGORY_COLORS).sort(),["analysis","breaking","election"]);
});
test("queue only verified unposted source-image news and obeys cap",async()=>{
 const inserts=[];
 const db={
  prepare(sql){const stmt={
   bind(...args){stmt.args=args;return stmt},
   async run(){if(sql.includes("INSERT OR IGNORE INTO instagram_scheduled_queue"))inserts.push(stmt.args);return {meta:{changes:1}}},
   async all(){return {results:[
     {telegram_message_id:4028,caption:"💠 خبر فوری: نتیجه جدید انتخابات اعلام شد 📰 مهر",telegram_url:"https://t.me/candidatoryiran/4028",photo_url:"https://telesco.pe/file/image.jpg",posted_at:"2026-11-03T19:00:00Z",status:"discovered"},
     {telegram_message_id:4027,caption:"دیدار فوتبال و ورزش 📰 مهر",telegram_url:"https://t.me/candidatoryiran/4027",photo_url:"https://telesco.pe/file/other.jpg",posted_at:"2026-11-03T19:00:00Z",status:"discovered"},
     {telegram_message_id:4026,caption:"انتخابات مجلس بدون استناد",telegram_url:"https://t.me/candidatoryiran/4026",photo_url:"https://telesco.pe/file/image.jpg",posted_at:"2026-11-03T19:00:00Z",status:"discovered"}]}}
  };return stmt;}
 };
 const result=await queueEligibleTelegramNews(db,{maxNew:1});
 assert.equal(result.queued,1);
 assert.equal(inserts.length,1);
 assert.deepEqual(inserts[0].slice(0,2),[4028,"breaking"]);
 assert.equal(result.publishEnabled,false);
});
