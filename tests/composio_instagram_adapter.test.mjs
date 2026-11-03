import test from "node:test";
import assert from "node:assert/strict";
import { createComposioAdapter } from "../composio_instagram_adapter.mjs";

test("requires credentials and renderer", () => {
  assert.throws(() => createComposioAdapter({}), /Missing Composio/);
  assert.throws(() => createComposioAdapter({apiKey:"x",connectedAccountId:"ca_x"}), /renderer/);
});

test("feed first and story separately with correct account", async () => {
  const calls=[];
  let next=0;
  const mockedFetch=async (url, opts)=>{
    const args=JSON.parse(opts.body).arguments;
    calls.push({url,args,account:JSON.parse(opts.body).connected_account_id});
    const responses=[
      {data:{id:"123"}}, {data:{id:"456"}},
      {data:{id:"456"}}, {data:{id:"789"}}, {data:{id:"987"}}, {data:{id:"987"}}
    ];
    return {ok:true,status:200,json:async()=>responses[next++]};
  };
  const adapter=createComposioAdapter({
    apiKey:"test",connectedAccountId:"ca_test",fetchImpl:mockedFetch,
    renderAndHost:async()=>({feed:"https://example.com/f.jpg",story:"https://example.com/s.jpg"}),
  });
  const assets=await adapter.renderAndHost({});
  assert.ok(assets.feed.endsWith("f.jpg"));
  const job={headline:"نمونه",source_name:"منبع",source_url:"https://example.com/item",feed_image_url:assets.feed,story_image_url:assets.story};
  const feed=await adapter.publishFeed(job);
  assert.equal(feed,"456");
  assert.equal(await adapter.confirmFeed(feed),true);
  const story=await adapter.publishStory(job);
  assert.equal(story,"987");
  assert.equal(await adapter.confirmStory(story),true);
  assert.equal(calls.length,6);
  assert.equal(calls[0].args.ig_user_id,"28851602321171831");
  assert.equal(calls[0].args.image_url,"https://example.com/f.jpg");
  assert.equal(calls[3].args.media_type,"STORIES");
  assert.equal(calls[3].args.image_url,"https://example.com/s.jpg");
  assert.ok(calls.every(x=>x.account==="ca_test"));
});
