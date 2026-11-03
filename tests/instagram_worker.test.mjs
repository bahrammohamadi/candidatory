import test from "node:test";
import assert from "node:assert/strict";
import { qualified, selectNews, processJob, IG_ACCOUNT } from "../instagram_worker.mjs";
const candidate = {id:"x",status:"published",title:"گزارش رسمی",source:"ISNA",url:"https://example.com/news",image_url:"https://example.com/p.jpg",source_verified:true,image_rights_confirmed:true,published_at:"2026-11-03T09:00:00Z",topic_relevance:3,source_quality:3};
test("proper account and sourced rights gates",()=>{
  assert.equal(IG_ACCOUNT,"candidatory.ir");
  assert.ok(qualified(candidate));
  assert.equal(qualified({...candidate,image_rights_confirmed:false}),false);
  assert.equal(qualified({...candidate,status:"pending_review"}),false);
});
test("dedup, latest and maximum",()=>{
  const chosen=selectNews([candidate,{...candidate,id:"y"}, {...candidate,id:"z",url:"https://example.com/different",published_at:"2020-01-01T00:00:00Z"}],{now:Date.parse("2026-11-03T10:00:00Z")});
  assert.deepEqual(chosen.map(x=>x.id),["x"]);
});
test("cannot publish when disabled",async()=>{
  assert.deepEqual(await processJob(null,"x",null,{enabled:false}),{status:"disabled"});
});
