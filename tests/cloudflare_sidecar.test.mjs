import test from "node:test";
import assert from "node:assert/strict";
import { intake } from "../cloudflare-instagram-sidecar.mjs";
function mockDb(){
  const calls=[];
  return {
    calls,
    prepare(sql){
      return {
        bind(...args){calls.push({sql,args});return this},
        async run(){return {meta:{changes:1}}},
        async all(){return {results:[
          {id:"a",title:"انتخابات شورای شهر",url:"https://news.example/a",source:"ISNA",status:"published",created_at:"2026-11-03 11:00:00"},
          {id:"b",title:"مسابقه فوتبال",url:"https://news.example/b",source:"ISNA",status:"published",created_at:"2026-11-03 10:00:00"}]}}
      }
    }
  };
}
test("intake only queues relevant already-published news",async()=>{
  const db=mockDb();
  const result=await intake(db,{cap:4});
  assert.equal(result.added,1);
  assert.equal(db.calls.length,1);
  assert.equal(db.calls[0].args[0],"a");
});
