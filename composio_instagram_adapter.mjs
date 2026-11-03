/**
 * Server-side Composio publishing adapter for candidatory.ir.
 *
 * Required secrets: COMPOSIO_API_KEY and COMPOSIO_CONNECTED_ACCOUNT_ID
 * (the connected account ID from the same Composio project).
 * No user credentials or tokens should be committed to Git.
 *
 * This adapter intentionally requires renderAndHost to be supplied by an
 * image-rendering/media-hosting service; it never posts unbranded images.
 */
const BASE = "https://backend.composio.dev/api/v3.1/tools/execute/";
const IG_USER_ID = "28851602321171831";

export function createComposioAdapter({ apiKey, connectedAccountId, renderAndHost, fetchImpl = fetch }) {
  if (!apiKey || !connectedAccountId) throw new Error("Missing Composio server credentials");
  if (typeof renderAndHost !== "function") throw new Error("Missing branded image renderer/hosting");
  const execute = async (slug, args) => {
    const response = await fetchImpl(BASE + slug, {
      method: "POST",
      headers: { "x-api-key": apiKey, "content-type": "application/json" },
      body: JSON.stringify({
        connected_account_id: connectedAccountId, version: "latest", arguments: args,
      }),
      signal: AbortSignal.timeout(20000),
    });
    const payload = await response.json();
    if (!response.ok || payload?.successful === false || payload?.error) {
      throw new Error(`Composio ${slug} HTTP ${response.status}: ${JSON.stringify(payload?.error || payload?.message || {}).slice(0,200)}`);
    }
    const result = payload?.data?.data || payload?.data || payload;
    if (result?.error || result?.successful === false) throw new Error(`Composio ${slug} rejected request`);
    return result;
  };

  async function publishImage(job, story) {
    const input = {
      ig_user_id: IG_USER_ID,
      image_url: story ? job.story_image_url : job.feed_image_url,
    };
    if (story) input.media_type = "STORIES";
    else input.caption = [job.headline, "", "منبع: " + job.source_name, job.source_url,
      "@candidatory.ir"].join("\n");
    if (!input.image_url?.startsWith("https://")) throw new Error("Missing branded HTTPS image");
    const container = await execute("INSTAGRAM_POST_IG_USER_MEDIA", input);
    const creationId = container.id || container.data?.id;
    if (!creationId) throw new Error("Composio did not return a media container ID");
    const published = await execute("INSTAGRAM_POST_IG_USER_MEDIA_PUBLISH", {
      ig_user_id: IG_USER_ID, creation_id: String(creationId), max_wait_seconds: 90,
    });
    const publishedId = published.id || published.data?.id;
    if (!publishedId) throw new Error("Composio did not return a published media ID");
    return String(publishedId);
  }

  async function confirm(id) {
    if (!id) return false;
    const media = await execute("INSTAGRAM_GET_IG_MEDIA", { ig_media_id: String(id), fields: "id,media_type,permalink" });
    const foundId = media.id || media.data?.id;
    return String(foundId || "") === String(id);
  }

  return {
    renderAndHost,
    publishFeed: job => publishImage(job, false),
    publishStory: job => publishImage(job, true),
    confirmFeed: confirm,
    confirmStory: confirm,
  };
}
