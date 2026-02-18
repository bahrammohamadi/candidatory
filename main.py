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

    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHANNEL_ID')
    appwrite_endpoint = os.environ.get('APPWRITE_ENDPOINT', 'https://cloud.appwrite.io/v1')
    appwrite_project = os.environ.get('APPWRITE_PROJECT_ID')
    appwrite_key = os.environ.get('APPWRITE_API_KEY')
    database_id = os.environ.get('APPWRITE_DATABASE_ID')
    collection_id = 'history'

    if not all([token, chat_id, appwrite_project, appwrite_key, database_id]):
        print("[ERROR] متغیرهای محیطی ناقص هستند!")
        return {"status": "error"}

    client = Client()
    client.set_endpoint(appwrite_endpoint)
    client.set_project(appwrite_project)
    client.set_key(appwrite_key)

    databases = Databases(client)

    bot = Bot(token=token)

    rss_feeds = [
        {"site": "Farsnews", "url": "https://www.farsnews.ir/rss"},
        {"site": "Entekhab", "url": "https://www.entekhab.ir/fa/rss/allnews"},
        {"site": "Isna", "url": "https://www.isna.ir/rss"},
        {"site": "Tasnim", "url": "https://www.tasnimnews.com/fa/rss/feed/0/0/0"},
        {"site": "Moi", "url": "https://www.moi.ir/fa/rss"},
        {"site": "Mehrnews", "url": "https://www.mehrnews.com/rss"},
    ]

    now = datetime.now(timezone.utc)
    time_threshold = now - timedelta(hours=24)
    posted = False

    for feed in rss_feeds:
        if posted:
            break
        try:
            parsed = feedparser.parse(feed["url"])
            if not parsed.entries:
                print(f"[INFO] فید خالی: {feed['site']}")
                continue

            for entry in parsed.entries:
                if posted:
                    break

                published = entry.get('published_parsed') or entry.get('updated_parsed')
                if not published:
                    continue

                pub_date = datetime(*published[:6], tzinfo=timezone.utc)
                if pub_date < time_threshold:
                    continue

                title = entry.title.strip()
                link = entry.link.strip()
                description = (entry.get('summary') or entry.get('description') or '').strip()

                try:
                    existing = databases.list_documents(
                        database_id=database_id,
                        collection_id=collection_id,
                        queries=[Query.equal("link", link)]
                    )
                    if existing['total'] > 0:
                        print(f"[INFO] تکراری رد شد: {title[:60]}")
                        continue
                except AppwriteException as db_err:
                    print(f"[WARN] خطا در چک دیتابیس (ادامه بدون چک): {str(db_err)}")

                final_text = f"{title}\n\n{description}\n\n@irfashionnews - مد و فشن ایرانی"

                try:
                    image_url = None
                    if 'enclosure' in entry and entry.enclosure.get('type', '').startswith('image/'):
                        image_url = entry.enclosure.href
                    if 'media_content' in entry:
                        for media in entry.media_content:
                            if media.get('medium') == 'image' and media.get('url'):
                                image_url = media.get('url')

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
                            link_preview_options=LinkPreviewOptions(is_disabled=True),
                            disable_notification=True
                        )
                    posted = True
                    print(f"[SUCCESS] پست موفق ارسال شد: {title[:60]}")
                    try:
                        databases.create_document(
                            database_id=database_id,
                            collection_id=collection_id,
                            document_id='unique()',
                            data={
                                'link': link,
                                'title': title,
                                'published_at': now.isoformat(),
                                'feed_url': feed['url'],
                                'created_at': now.isoformat()
                            }
                        )
                        print("[SUCCESS] ذخیره در دیتابیس موفق")
                    except Exception as save_err:
                        print(f"[WARN] خطا در ذخیره دیتابیس: {str(save_err)}")
                except Exception as send_err:
                    print(f"[ERROR] خطا در ارسال پست: {str(send_err)}")
        except Exception as feed_err:
            print(f"[ERROR] خطا در پردازش فید {feed['site']}: {str(feed_err)}")

    print(f"[INFO] پایان اجرا - پست ارسال شد: {posted}")
    return {"status": "success", "posted": posted}

if __name__ == "__main__":
    asyncio.run(main())
