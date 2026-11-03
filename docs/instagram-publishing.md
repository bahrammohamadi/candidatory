# Candidatory Instagram — operation & safety (2026-11-03)

## Verified external accounts
- Telegram news bot @Candidatory_bot, news channel @candidatoryiran
- Instagram Business @candidatory.ir, Instagram user ID 28851602321171831
- Do not use the unrelated Hirka accounts.

## Current architecture
- **Existing** Cloudflare `candidatory-news-bot`: continues sending Telegram+Bale without modification.
- **New** Cloudflare `candidatory-instagram-staging` (public health endpoint): read-only intake of published news from D1, cron `*/30 * * * *`.
- Shared D1 database `candidatory-news`: `articles` holds Telegram publication state, `instagram_intake_jobs` is a separate news intake table. Existing `instagram_jobs` is reserved for the optional publishing state machine; do not confuse the schemas.
- **Scheduled ChatGPT automation** `انتشار منتخب اخبار کاندیداتوری`: hourly selected-news verification and conditional publication using connected Composio Instagram account. It may skip publication when rights, photo, source, template, or app access fails. This is not a standalone Cloudflare -> Composio deployment.
- Staging `/queue/status` is read-only. Staging Worker returns `auto_publish:false`; this is intentional.


## Direct channel extraction upgrade
The Cloudflare staging Worker now also reads the **public Telegram channel HTML** at `https://t.me/s/candidatoryiran` every 30 minutes. This avoids conflicts with the active bot's webhook and captures newly published messages, their Telegram message IDs, captions and image URLs even when the older `articles` database is incomplete. It stores these in a separate table, `instagram_telegram_posts` (initial state `discovered`), which the scheduled ChatGPT publisher checks first. The first five messages (4012–4016) were ingested and the Worker probe confirmed 20 readable messages, latest ID 4016. This is a read-only news intake, not proof of unattended Instagram publication.

## Fixed branding code
- `instagram_branding.py`: two JPEG outputs 1080x1350 and 1080x1920, with fixed navy/red/cream composition and safe areas.
- `render_instagram_news.py`: CLI wrapper; **requires licensed/authorized photograph** and explicit `--rights-confirmed`. Downloads open-license Vazirmatn into a temporary private font cache.
- `instagram-requirements.txt` includes Pillow, arabic-reshaper, and python-bidi.
- Before every publish inspect actual output for legibility, image rights, accurate title/date, and no content overlap. The initial published test used an earlier, less polished fallback; **do not reuse it**.

## Release gates
1. Verify that the item was **published to Telegram**, sourced, recent, and not already posted on Instagram.
2. Ensure the original image is accessible AND licensed; otherwise remain `awaiting_media`. No synthetic photorealistic image presented as authentic news.
3. Generate a pair using repository templates, inspect the exact JPEGs.
4. Publish the feed first through the verified Composio `candidatory.ir` account; save the published Instagram media ID, not the container ID.
5. Confirm the feed with Instagram GET_IG_MEDIA before publishing the story.
6. Publish the story independently (native Add Post To Story is unavailable from the connected API).
7. Confirm and persist both IDs, permalinks, final state, and errors. Never blindly retry an ambiguous request.

## Current important limitation
The Composio connection visible inside ChatGPT does **not** automatically install a server API key into a Cloudflare Worker. The existing news Worker has no `COMPOSIO_API_KEY` binding. Cloudflare standalone autonomous publishing is therefore **not enabled**; hourly ChatGPT automation is the available conditional runtime. To switch to a standalone Cloudflare backend later, configure a project-scoped server-side Composio API key and the matching connected-account ID as encrypted Worker secrets, then deploy and test the publishing adapter. Never commit credentials or paste tokens into a public log.

## Checks
- GitHub Actions: `node --test tests/*.test.mjs`
- GitHub Actions: `python -m unittest discover -s tests -p 'test_instagram_branding.py'`
- Cloudflare health: `https://candidatory-instagram-staging.lasvaram-candidatory.workers.dev/health`
- Cloudflare queue status: `https://candidatory-instagram-staging.lasvaram-candidatory.workers.dev/queue/status`

## Confirmed one-time test
A branded feed and independent story were published successfully using the connected Composio account and their IDs were persisted in D1:
- Feed: https://www.instagram.com/p/DfRi3GfldgC/
- Story: https://www.instagram.com/stories/candidatory.ir/4022149654762589346

This manual one-time test is not proof that a future scheduled automated run has published a new article.
