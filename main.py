import os
import asyncio
import feedparser
from datetime import datetime, timedelta, timezone
from telegram import Bot, LinkPreviewOptions

async def main(event=None, context=None):
    print("[INFO] شروع")

    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHANNEL_ID')

    if not token or not chat_id:
        print("[ERROR] توکن یا chat_id نیست")
        return {"status": "error"}

    bot = Bot(token=token)

    rss_feeds = [
        {"site": "فارس",      "url": "https://www.farsnews.ir/rss"},
        {"site": "انتخاب",    "url": "https://www.entekhab.ir/fa/rss/allnews"},
        {"site": "ایسنا",     "url": "https://www.isna.ir/rss"},
        {"site": "تسنیم",     "url": "https://www.tasnimnews.com/fa/rss/feed/0/0/0"},
        {"site": "مهر",       "url": "https://www.mehrnews.com/rss"},
    ]

    now = datetime.now(timezone.utc)
    time_threshold = now - timedelta(hours=24)

    posted = False

    for feed in rss_feeds:
        if posted:
            break

        try:
            feed_data = feedparser.parse(feed["url"])
            if not feed_data.entries:
                print(f"[INFO] خالی: {feed['site']}")
                continue

            for entry in feed_data.entries:
                if posted:
                    break

                pub = entry.get('published_parsed') or entry.get('updated_parsed')
                if not pub:
                    continue

                pub_date = datetime(*pub[:6], tzinfo=timezone.utc)
                if pub_date < time_threshold:
                    continue

                title = (entry.title or "").strip()
                link = (entry.link or "").strip()
                if not title or not link:
                    continue

                desc = (entry.get('summary') or entry.get('description') or "").strip()

                # ساخت متن نهایی به ترتیب درخواستی
                final_text = (
                    f"{title}\n\n"
                    f"@{os.environ.get('TELEGRAM_CHANNEL_USERNAME', 'irnewsbot')}\n\n"   # ایدی کانال
                    f"{desc}\n\n"
                    f"@{os.environ.get('TELEGRAM_CHANNEL_USERNAME', 'irnewsbot')} - کانال خبری"
                )

                image_url = None
                if 'enclosure' in entry and entry.enclosure.get('type', '').startswith('image/'):
                    image_url = entry.enclosure.href
                elif 'media_content' in entry:
                    for media in entry.media_content:
                        if media.get('medium') == 'image' and media.get('url'):
                            image_url = media['url']
                            break

                try:
                    if image_url:
                        await bot.send_photo(
                            chat_id=chat_id,
                            photo=image_url,
                            caption=final_text,
                            parse_mode='HTML',
                            disable_notification=True
                        )
                    else:
                        await bot.send_message(
                            chat_id=chat_id,
                            text=final_text,
                            parse_mode='HTML',
                            link_preview_options=LinkPreviewOptions(is_disabled=False),
                            disable_notification=True
                        )

                    posted = True
                    print(f"[SUCCESS] ارسال شد: {title[:60]}")

                except Exception as e:
                    print(f"[ERROR] ارسال شکست: {str(e)}")

        except Exception as e:
            print(f"[ERROR] فید {feed['site']}: {str(e)}")

    print(f"[INFO] پایان - ارسال شد: {posted}")
    return {"status": "ok", "posted": posted}


if __name__ == "__main__":
    asyncio.run(main())
