# Autonomous Candidatory Instagram — GitHub Actions execution

## Live components

- Cloudflare Worker `candidatory-instagram-staging`: reads Telegram `@candidatoryiran` every 30 minutes and stores safe, deduplicated intake in D1. Its publish flag remains off.
- GitHub Actions `.github/workflows/instagram-autopublish.yml`: independently scheduled **:07 and :37 each hour (UTC)**, runs `scripts/instagram_autopublisher.py`.
- Python renderer `instagram_branding.py`: version 9, user's approved logo, B Titr for headlines and Vazirmatn for body, branded 4:5 feed and 9:16 story, all four color categories.
- `data/instagram-published.json`: records **previous real Instagram media IDs** and is updated after each publication stage. Cloudflare reconciles them into D1. No ChatGPT scheduled task is needed after activation.
- `instagram-media/{telegram_id}-{feed|story}.jpg`: temporary publicly fetchable JPEGs served via GitHub raw content. **Note:** this makes images public and grows repo size; move to a proper bucket/CDN when R2 becomes available.

## Activation — required account-holder actions

The self-running workflow exists but **publishing is safely disabled** until it has authorized server credentials.

In the GitHub repository `bahrammohamadi/candidatory` open **Settings → Secrets and variables → Actions**.

Add two **repository secrets**:

1. `CANDIDATORY_COMPOSIO_API_KEY`: scoped API key for **your own Composio project**. Do not copy it into chat or repository files. It must permit Instagram tool execution.
2. `CANDIDATORY_COMPOSIO_INSTAGRAM_CONNECTED_ACCOUNT_ID`: the *connected account ID in that same project* representing Instagram Business **@candidatory.ir** (numeric IG user ID `28851602321171831`). Do not use the unrelated `@hirka.wear` account. The ChatGPT-side account alias `instagram_impi-darg` is **not guaranteed** to be the project connected-account ID.

Then, under **Variables → Actions**, create repository variable:

- `CANDIDATORY_INSTAGRAM_AUTOPUBLISH_ENABLED` = `true`.

Do not enable until the target Composio project connection is independently tested; it may require reauthorizing the account in that project.

The API tools used are `INSTAGRAM_POST_IG_USER_MEDIA`, `INSTAGRAM_POST_IG_USER_MEDIA_PUBLISH` and `INSTAGRAM_GET_IG_MEDIA`, invoked with the project API key per [Composio docs](https://docs.composio.dev/reference/v3/api-reference/tools).

## Processing rules

- Select most recent **sourced** news with image from the public Telegram channel, any subject (not election-only), at most 18 hours old.
- No news from a different Telegram channel; no duplicate Telegram ID.
- Only between 09:00 and 23:00 Tehran local time, maximum 15 feed posts per calendar day, minimum 50 minutes between feeds.
- Use the same verified subject photo for feed and Story, with distinct sizes. Story is an independently posted visual, **not** a native clickable feed resharing sticker.
- The approved template requires its exact logo and fonts. B Titr downloaded for runtime only (not checked into GitHub); Vazirmatn from Google Fonts.
- Build the images, commit them to GitHub so Instagram has public HTTPS JPEG URLs, wait until fetch confirms them, then create/publish/verify the feed. Only after feed verification create/publish/verify a separate Story.
- Persist a checkpoint to `data/instagram-published.json` before and after each mutation. Ambiguous or failed publishing stages **require manual reconciliation** (no blind retry; no duplicate). If API keys are absent, do nothing and report BLOCKED.
- Confirm image rights and source integrity as a newsroom responsibility before enabling unattended republishing of third-party images.

## Manual dry run

GitHub → Actions → **Candidatory Instagram autonomous publisher** → Run workflow, leave `dry_run=true`. This renders a real news JPEG in the runner but does not publish. Production mode needs the secrets and variable above.

## Current constraints

Cloudflare R2 returns `10042 Please enable R2`, so the repository-hosted public JPEG route is the fallback. The Composio integration inside ChatGPT **does not expose** a reusable server API key. Cloudflare's 30-minute collector is deployed; the GitHub autonomous publisher is configured but will **not publish** without explicit server authentication.

Do not claim full unattended publishing until a **GitHub Actions scheduled run** successfully posts and verifies both feed and story, then its IDs appear in `data/instagram-published.json` and D1.
