import test from "node:test";
import assert from "node:assert/strict";
import {reconcilePublishedState} from "../cloudflare-instagram-sidecar.mjs";

test("reconciles verified published IDs only; never trusts a container or wrong account",async()=>{
 const sql=[];
 const db={prepare(stmt){
   return {bind(...args){sql.push({stmt,args});return this},
           async run(){return {meta:{changes:1}}}};
 }};
 const state={account:"candidatory.ir",posts:{
  "5010":{status:"completed",feed_media_id:"181237890123",story_media_id:"179123456789",
          category:"general",feed_permalink:"https://www.instagram.com/p/test/"},
  "5011":{status:"feed_publishing",feed_container_id:"18012345123456"},
  "not-id":{status:"completed",feed_media_id:"1",story_media_id:"2"},
 }};
 const response=async()=>({ok:true,json:async()=>state});
 const result=await reconcilePublishedState(db,response);
 assert.equal(result.synced,1);
 assert.equal(sql.length,3);
 assert.equal(sql[0].args[0],"completed");
 assert.equal(sql[0].args[1],"181237890123");
 assert.equal(sql[2].args[1],"general");
});
test("rejects another Instagram account or forged checkpoint",async()=>{
 const db={prepare(){throw Error("never write")}};
 const wrong=async()=>({ok:true,json:async()=>({account:"hirka.wear",posts:{"5012":{}}})});
 await assert.rejects(()=>reconcilePublishedState(db,wrong),/Unexpected Instagram account/);
});
