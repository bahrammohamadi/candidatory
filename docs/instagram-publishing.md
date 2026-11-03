# Candidatory Instagram publishing — staged rollout

## Status
The design renderer in `instagram_branding.py` is implemented on a feature branch, **not deployed**.
Production Telegram and Bale publishing remain unchanged. Instagram automation is **disabled**.

## Confirmed account identity
- Source Telegram channel: `@candidatoryiran`; bot `@Candidatory_bot`
- Destination Instagram Business account: `@candidatory.ir`
- Never use `@hirka.wear` / `@Hirka_Bot` credentials for this integration.

## Rendering
- Feed: 1080 x 1350 JPEG (4:5).
- Story: 1080 x 1920 JPEG (9:16).
- Navy/white/red brand; same verified headline and credited source; watermark/handle.
- Both images produced from the **same licensed/authorized original news image**, no AI-generated pseudo-photographs for factual events.
- `render_pair(source_bytes, headline, category, source_name, font_path)` returns (feed_bytes, story_bytes).
- Install: `pip install -r instagram-requirements.txt`.
- Configure a legally usable Persian .ttf at runtime; font file is not committed.
- Contrast and clipping require visual review on real photos before release.

## Required workflow, disabled until verified
1. **Tap existing news publisher output**, not Telegram getUpdates polling; the active bot may already use a webhook.
2. Only articles confirmed **successfully posted to Telegram** enter Instagram selection queue.
3. Persist an immutable article ID, source URL, original Telegram message ID (if available), picture source, licensing status, and publication timestamp.
4. In periodic windows select distinct verified eligible items. Do not rate political actors/candidates or choose political winners. Editorial selection uses topical relevance, source verification, timeliness, novelty, and source balance.
5. Generate a feed and story image; upload both to public/Meta-fetchable HTTPS URLs (e.g. controlled object storage with an appropriately scoped access policy).
6. Publish **feed first** via Composio Instagram create-container then media-publish (correct `candidatory.ir` connection).
7. Confirm returned feed media ID and processing success, **then** publish story as a separate container using its pre-rendered URL. Native `Add post to story` interactive resharing is not an exposed Composio action.
8. Persist dedupe/transaction records with state `queued -> feed_publishing -> feed_published -> story_publishing -> completed`, plus `failed` and retriable states. Feed retries must first check whether it was already published to avoid duplicates.
9. Use the source article ID and asset hashes for idempotency; cap attempts with backoff and operator-visible dead-letter logs.
10. Do not auto-publish unverified or rights-unclear articles. Do not use the branded example news headline as a factual current report.

## Remaining integration prerequisites
- Set up a **server-side automation runtime** with secure Composio app auth (or a permitted Meta API backend credential); ChatGPT's live Composio connector does not automatically hand a reusable publishing token to an Appwrite Function.
- Controlled image storage and persistent job queue/status store.
- Confirm the active deployed Appwrite code actually matches this repository (it currently uses a 30s timeout); avoid blocking Telegram or Bale sends while rendering/publishing.
- Test a **single approved sample** end-to-end, then explicitly enable unattended posting.

## Smoke test
`python -m unittest discover -s tests -p 'test_instagram_branding.py'`

## Safety
All keys/tokens should be configured as secrets. Never include them in GitHub.

## Verified November 3, 2026 deployment findings
- Live news publisher is Cloudflare Worker `candidatory-news-bot` with the `NEWS_DB` D1 binding.
- `instagram_jobs` table has now been created **in the actual bound D1 database** and confirmed via `sqlite_master`.
- The existing Worker has `TELEGRAM_BOT_TOKEN`, `BALE_BOT_TOKEN`, `GEMINI_API_KEY` but **no Composio server API credential or hosted image storage binding**. Do not treat the ChatGPT Composio connection as a Worker API credential.
- `composio_instagram_adapter.mjs` shows the Composio v3.1 tool endpoint using server-side `x-api-key` and `connected_account_id`; it cannot execute until a project API key and matching connected account ID have been securely configured in the Worker.
- Both backend and branded-image tests run in GitHub Actions; live Instagram publishing remains off.
- The existing Python `main.py` is not the currently deployed Worker runtime; avoid assuming it directly controls live Telegram publishing.

## Next production integration
1. Confirm the selected Composio **project** can use the active `candidatory.ir` account from server-side `/api/v3.1/tools/execute`.
2. Store a scoped Composio project API key and the server-side connected-account ID as Cloudflare secrets. Never commit them, use the ChatGPT connector's opaque alias as an API account ID, or expose them to browser code.
3. Deploy the branded JPEG rendering service and public HTTPS media hosting, with a licensed Persian font.
4. Wire a new event from the **actual Worker** after Telegram delivery is confirmed, writing verified/rights-cleared article metadata to `instagram_jobs`.
5. Use a separate scheduled consumer to select jobs and run the adapter with `enabled=false` until one approved end-to-end feed + story sample succeeds.
