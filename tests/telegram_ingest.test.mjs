import test from "node:test";
import assert from "node:assert/strict";
import {parseTelegramPublicFeed,syncTelegramChannel} from "../cloudflare-instagram-sidecar.mjs";

const HTML=`<html><div class="tgme_widget_message_wrap"><div class="tgme_widget_message" data-post="candidatoryiran/4015">
<div class="tgme_widget_message_text js-message_text">خبر &amp; متن<br>دوم</div>
<a class="tgme_widget_message_photo_wrap" style="background-image:url('https://cdn4.telesco.pe/file/photo01.jpg')"></a>
<time datetime="2026-11-03T10:00:00+00:00"></time></div></div>
<div class="tgme_widget_message_wrap"><div class="tgme_widget_message" data-post="candidatoryiran/4016"><div class="tgme_widget_message_text js-message_text">خبر شورای شهر</div>
<a class="tgme_widget_message_photo_wrap" style="background-image:url('https://cdn4.telesco.pe/file/photo02.jpg')"></a>
</div></div></html>`;
test("parse ids, attribution and media in newest-first order",()=>{
 const p=parseTelegramPublicFeed(HTML);
 assert.deepEqual(p.map(x=>x.telegram_message_id),[4016,4015]);
 assert.equal(p[0].caption,"خبر شورای شهر");
 assert.equal(p[0].photo_url,"https://cdn4.telesco.pe/file/photo02.jpg");
 assert.match(p[1].caption,/خبر & متن/);
});
test("sync is idempotent via INSERT OR IGNORE",async()=>{
 let inserted=0;
 const db={prepare(sql){
   return {bind(...a){return this},async run(){if(sql.includes("INSERT OR IGNORE")) inserted++;return {meta:{changes:1}}}};
 }};
 const fake=async()=>({ok:true,text:async()=>HTML});
 const v=await syncTelegramChannel(db,fake);
 assert.equal(v.seen,2);
 assert.equal(v.added,2);
 assert.equal(inserted,2);
});
