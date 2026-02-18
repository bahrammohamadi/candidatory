import os
import asyncio
import feedparser
from datetime import datetime, timedelta, timezone
from telegram import Bot, LinkPreviewOptions
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
from appwrite.exception import AppwriteException

async def main(event=None, context=None):
    print("[INFO] شروع اجرای اتوماسیون اخبار از سایت‌های مشخص‌شده")

    # خواندن متغیرهای محیطی
    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHANNEL_ID')
    endpoint = os.environ.get('APPWRITE_ENDPOINT', 'https://cloud.appwrite.io/v1')
    project = os.environ.get('APPWRITE_PROJECT_ID')
    key = os.environ.get('APPWRITE_API_KEY')
    database_id = os.environ.get('APPWRITE_DATABASE_ID')
    collection_id = 'history'  # اگر candidatable گذاشتی → 'candidatable' بنویس

    if not all([token, chat_id, project, key, database_id]):
        print("[ERROR] متغیرهای محیطی ناقص هستند")
        return {"status": "error", "reason": "missing environment variables"}

    # ساخت کلاینت Appwrite
    client = Client()
    client.set_endpoint(endpoint)
    client.set_project(project)
    client.set_key(key)

    # لیست سایت‌ها و RSSها (فقط همان‌هایی که خواستی)
    rss_feeds = [
        {"site": "Farsnews",   "url": "https://www.farsnews.ir/rss"},
        {"site": "Entekhab",   "url": "https://www.entekhab.ir/fa/rss/allnews"},
        {"site": "Isna",       "url": "https://www.isna.ir/rss"},
        {"site": "Tasnim",     "url": "https://www.tasnimnews.com/fa/rss/feed/0/0/0"},
        {"site": "Moi",        "url": "https://www.moi.ir/fa/rss"},  # اگر کار نکرد حذف کن
        {"site": "Mehrnews",   "url": "https://www.mehrnews.com/rss"},
        # اگر Shoraha.org.ir RSS داشت اضافه کن
    ]

    now = datetime.now(timezone.utc)
    time_threshold = now - timedelta(hours=24)  # اخبار ۲۴ ساعت اخیر

    posted_count = 0

    for feed in rss_feeds:
        try:
            parsed = feedparser.parse(feed["url"])
            if not parsed.entries:
                print(f"[INFO] فید خالی: {feed['site']}")
                continue

            for entry in parsed.entries:
                pub_parsed = entry.get('published_parsed') or entry.get('updated_parsed')
                if not pub_parsed:
                    continue

                pub_date = datetime(*pub_parsed[:6], tzinfo=timezone.utc)
                if pub_date < time_threshold:
                    continue

                title = (entry.title or "").strip()
                link = (entry.link or "").strip()
                if not title or not link:
                    continue

                description = (entry.get('summary') or entry.get('description') or "").strip()

                # چک تکراری
                is_duplicate = False
                try:
                    res = client.databases.list_documents(
                        database_id=database_id,
                        collection_id=collection_id,
                        queries=[Query.equal("link", link)],
                        limit=1
                    )
                    if res.get('total', 0) > 0:
                        is_duplicate = True
                        print(f"[SKIP] تکراری: {title[:70]}")
                except AppwriteException as e:
                    print(f"[WARN] خطا در چک تکراری {feed['site']}: {e.message}")
                except Exception as e:
                    print(f"[WARN] خطای عمومی چک تکراری: {str(e)}")

                if is_duplicate:
                    continue

                # متن نهایی پست
                final_text = f"📰 {title}\n\n{description[:500]}...\n\n🔗 منبع: {link}\n\n@irelections"   # ← @ کانال خودت رو اینجا بگذار

                # ارسال به تلگرام
                image_url = None
                if 'enclosure' in entry and entry.enclosure.get('type', '').startswith('image/'):
                    image_url = entry.enclosure.href
                elif 'media_content' in entry:
                    for media in entry.media_content:
                        if media.get('medium') == 'image' and media.get('url'):
                            image_url = media['url']
                            break

                try:
                    bot = Bot(token=token)
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

                    posted_count += 1
                    print(f"[SUCCESS] پست ارسال شد → {title[:70]}")

                    # ذخیره در Appwrite
                    try:
                        client.databases.create_document(
                            database_id=database_id,
                            collection_id=collection_id,
                            document_id='unique()',
                            data={
                                'link': link,
                                'title': title,
                                'site': feed['site'],
                                'created_at': now.isoformat()
                            }
                        )
                    except Exception as save_err:
                        print(f"[WARN] خطا در ذخیره سند: {str(save_err)}")

                except Exception as send_err:
                    print(f"[ERROR] خطا در ارسال به تلگرام: {str(send_err)}")

        except Exception as feed_err:
            print(f"[ERROR] خطا در پردازش فید {feed['site']}: {str(feed_err)}")

    print(f"[INFO] پایان اجرا — تعداد پست‌های ارسال‌شده: {posted_count}")
    return {"status": "success", "posted": posted_count}


if __name__ == "__main__":
    asyncio.run(main())
