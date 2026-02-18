import os
import asyncio
import feedparser
from datetime import datetime, timedelta, timezone
from telegram import Bot, LinkPreviewOptions
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query

async def main(event=None, context=None):
    print("[INFO] شروع اجرای اتوماسیون اخبار")
    
    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHANNEL_ID')
    appwrite_endpoint = os.environ.get('APPWRITE_ENDPOINT', 'https://cloud.appwrite.io/v1')
    appwrite_project = os.environ.get('APPWRITE_PROJECT_ID')
    appwrite_key = os.environ.get('APPWRITE_API_KEY')
    database_id = os.environ.get('APPWRITE_DATABASE_ID')
    collection_id = 'history'  # یا 'candidatable' اگر تغییر دادی

    if not all([token, chat_id, appwrite_project, appwrite_key, database_id]):
        print("[ERROR] متغیرهای محیطی ناقص!")
        return {"status": "error"}

    bot = Bot(token=token)
    aw_client = Client()
    aw_client.set_endpoint(appwrite_endpoint)
    aw_client.set_project(appwrite_project)
    aw_client.set_key(appwrite_key)
    databases = Databases(aw_client)

    rss_feeds = [
        {"site": "Farsnews", "url": "https://www.farsnews.ir/rss"},
        {"site": "Entekhab", "url": "https://www.entekhab.ir/fa/rss/allnews"},
        {"site": "Isna", "url": "https://www.isna.ir/rss"},
        {"site": "Tasnim", "url": "https://www.tasnimnews.com/fa/rss/feed/0/0/0"},
        {"site": "Mehrnews", "url": "https://www.mehrnews.com/rss"},
    ]

    now = datetime.now(timezone.utc)
    time_threshold = now - timedelta(hours=24)

    posted_count = 0

    for feed in rss_feeds:
        try:
            feed_data = feedparser.parse(feed["url"])
            if not feed_data.entries:
                continue

            for entry in feed_data.entries:
                published = entry.get('published_parsed') or entry.get('updated_parsed')
                if not published:
                    continue
                pub_date = datetime(*published[:6], tzinfo=timezone.utc)
                if pub_date < time_threshold:
                    continue

                title = (entry.title or "").strip()
                link = (entry.link or "").strip()
                if not link or not title:
                    continue

                description = (entry.get('summary') or entry.get('description') or "").strip()

                is_duplicate = False
                try:
                    existing = databases.list_documents(
                        database_id=database_id,
                        collection_id=collection_id,
                        queries=[Query.equal("link", link)],
                        limit=1
                    )
                    if existing.get('total', 0) > 0:
                        is_duplicate = True
                except:
                    pass  # اگر خطا داد ادامه بده

                if is_duplicate:
                    continue

                final_text = f"📰 {title}\n\n{description[:500]}...\n\n🔗 {link}\n\n@irelections"  # @ کانالت رو عوض کن

                image_url = None
                if 'enclosure' in entry and entry.enclosure.get('type', '').startswith('image/'):
                    image_url = entry.enclosure.href
                # ... (بقیه چک عکس اگر خواستی اضافه کن)

                try:
                    if image_url:
                        await bot.send_photo(chat_id=chat_id, photo=image_url, caption=final_text, parse_mode='HTML')
                    else:
                        await bot.send_message(chat_id=chat_id, text=final_text, parse_mode='HTML', link_preview_options=LinkPreviewOptions(is_disabled=False))

                    posted_count += 1

                    databases.create_document(
                        database_id=database_id,
                        collection_id=collection_id,
                        document_id='unique()',
                        data={'link': link, 'title': title, 'created_at': now.isoformat()}
                    )
                except Exception as e:
                    print(f"[ERROR] ارسال: {str(e)}")

        except Exception as e:
            print(f"[ERROR] فید {feed['site']}: {str(e)}")

    return {"status": "success", "posted": posted_count}

if __name__ == "__main__":
    asyncio.run(main())
