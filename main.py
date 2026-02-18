import os
import asyncio
import feedparser
from datetime import datetime, timedelta, timezone
from telegram import Bot, LinkPreviewOptions

# فقط importهای لازم بدون appwrite (فعلاً برای تست)
# بعداً اگر کار کرد appwrite رو اضافه می‌کنیم

async def main(event=None, context=None):
    print("[INFO] شروع")

    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHANNEL_ID')

    if not token or not chat_id:
        print("[ERROR] توکن یا chat_id نیست")
        return {"status": "error"}

    bot = Bot(token=token)

    rss_feeds = [
        "https://www.farsnews.ir/rss",
        "https://www.entekhab.ir/fa/rss/allnews",
        "https://www.isna.ir/rss",
        "https://www.tasnimnews.com/fa/rss/feed/0/0/0",
        "https://www.mehrnews.com/rss",
    ]

    now = datetime.now(timezone.utc)
    time_threshold = now - timedelta(hours=24)

    posted = False

    for url in rss_feeds:
        if posted:
            break

        try:
            feed = feedparser.parse(url)
            if not feed.entries:
                continue

            for entry in feed.entries:
                if posted:
                    break

                published = entry.get('published_parsed') or entry.get('updated_parsed')
                if not published:
                    continue

                pub_date = datetime(*published[:6], tzinfo=timezone.utc)
                if pub_date < time_threshold:
                    continue

                title = (entry.title or "").strip()
                link = (entry.link or "").strip()
                if not title or not link:
                    continue

                description = (entry.get('summary') or entry.get('description') or "").strip()

                final_text = (
                    f"{title}\n\n"
                    f"@candidatoryiran\n\n"
                    f"{description}\n\n"
                    f"@candidatoryiran - کانال خبری کاندیداتوری"
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
                    print(f"[SUCCESS] ارسال شد: {title[:70]}")

                    # اینجا فقط چاپ می‌کنیم (دیتابیس فعلاً کامنت)
                    print(f"[DB] لینک باید ذخیره شود: {link}")

                except Exception as e:
                    print(f"[ERROR] ارسال شکست: {str(e)}")

        except Exception as e:
            print(f"[ERROR] فید {url}: {str(e)}")

    print(f"[INFO] پایان - ارسال شد: {posted}")
    return {"status": "success", "posted": posted}


if __name__ == "__main__":
    asyncio.run(main())
