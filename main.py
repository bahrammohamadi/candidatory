import os
import asyncio
import feedparser
from datetime import datetime, timedelta, timezone
from telegram import Bot, LinkPreviewOptions

async def main(event=None, context=None):
    print("[START] زمان شروع:", datetime.utcnow().isoformat())

    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHANNEL_ID')

    if not token or not chat_id:
        print("[ERROR] توکن یا chat_id موجود نیست")
        return {"status": "error", "reason": "missing env vars"}

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

        print(f"[FEED] شروع پردازش: {url}")
        try:
            feed = await asyncio.wait_for(feedparser.parse(url), timeout=10)  # حداکثر ۱۰ ثانیه برای هر فید
            if not feed.entries:
                print(f"[FEED] خالی: {url}")
                continue

            for entry in feed.entries[:5]:  # فقط ۵ خبر اول هر فید (برای سرعت)
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
                    print(f"[SEND] تلاش برای ارسال: {title[:50]}...")
                    if image_url:
                        await asyncio.wait_for(
                            bot.send_photo(
                                chat_id=chat_id,
                                photo=image_url,
                                caption=final_text,
                                parse_mode='HTML',
                                disable_notification=True
                            ),
                            timeout=15
                        )
                    else:
                        await asyncio.wait_for(
                            bot.send_message(
                                chat_id=chat_id,
                                text=final_text,
                                parse_mode='HTML',
                                link_preview_options=LinkPreviewOptions(is_disabled=False),
                                disable_notification=True
                            ),
                            timeout=15
                        )

                    posted = True
                    print(f"[SUCCESS] ارسال موفق: {title[:70]} (لینک: {link})")

                except asyncio.TimeoutError:
                    print("[TIMEOUT] ارسال به تلگرام تایم‌اوت شد")
                except Exception as send_err:
                    print(f"[ERROR] خطا در ارسال: {str(send_err)}")

        except asyncio.TimeoutError:
            print(f"[TIMEOUT] فید {url} تایم‌اوت شد")
        except Exception as feed_err:
            print(f"[ERROR] مشکل در فید {url}: {str(feed_err)}")

    print(f"[INFO] پایان اجرا - ارسال شد: {posted}")
    return {"status": "success", "posted": posted}


if __name__ == "__main__":
    asyncio.run(main())
