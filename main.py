import os
import asyncio
import feedparser
from datetime import datetime, timedelta, timezone
from telegram import Bot, LinkPreviewOptions
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query

async def main(event=None, context=None):
    print("[INFO] شروع اجرای اتوماسیون")

    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHANNEL_ID')
    endpoint = os.environ.get('APPWRITE_ENDPOINT', 'https://cloud.appwrite.io/v1')
    project = os.environ.get('APPWRITE_PROJECT_ID')
    key = os.environ.get('APPWRITE_API_KEY')
    database_id = os.environ.get('APPWRITE_DATABASE_ID')
    collection_id = 'history'  # یا 'candidatable'

    if not all([token, chat_id, project, key, database_id]):
        print("[ERROR] متغیرهای محیطی ناقص")
        return {"status": "error"}

    client = Client()
    client.set_endpoint(endpoint)
    client.set_project(project)
    client.set_key(key)

    databases = Databases(client)

    bot = Bot(token=token)

    rss_feeds = [
        {"site": "Farsnews", "url": "https://www.farsnews.ir/rss"},
        {"site": "Entekhab", "url": "https://www.entekhab.ir/fa/rss/allnews"},
        {"site": "Isna", "url": "https://www.isna.ir/rss"},
        {"site": "Tasnim", "url": "https://www.tasnimnews.com/fa/rss/feed/0/0/0"},
        {"site": "Mehrnews", "url": "https://www.mehrnews.com/rss"},
    ]

    now = datetime.now(timezone.utc)
    time_threshold = now - timedelta(hours=24)
    posted = 0

    for feed in rss_feeds:
        try:
            feed_data = feedparser.parse(feed["url"])
            if not feed_data.entries:
                continue

            for entry in feed_data.entries:
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

                # چک تکراری
                duplicate = False
                try:
                    result = databases.list_documents(
                        database_id=database_id,
                        collection_id=collection_id,
                        queries=[Query.equal("link", link)],
                        limit=1
                    )
                    if result.get('total', 0) > 0:
                        duplicate = True
                        print(f"[SKIP] تکراری: {title[:60]}")
                except Exception as e:
                    print(f"[WARN] چک تکراری شکست خورد: {str(e)}")

                if duplicate:
                    continue

                text = f"📰 {title}\n\n{desc[:450]}...\n\n🔗 {link}\n\n@irnewsbot"   # ← آیدی کانال خودت

                image = None
                if 'enclosure' in entry and entry.enclosure.get('type', '').startswith('image'):
                    image = entry.enclosure.href

                try:
                    if image:
                        await bot.send_photo(chat_id=chat_id, photo=image, caption=text, parse_mode='HTML')
                    else:
                        await bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML', link_preview_options=LinkPreviewOptions(is_disabled=False))

                    posted += 1
                    print(f"[OK] پست شد: {title[:60]}")

                    databases.create_document(
                        database_id=database_id,
                        collection_id=collection_id,
                        document_id='unique()',
                        data={
                            'link': link,
                            'title': title,
                            'created_at': now.isoformat()
                        }
                    )
                except Exception as e:
                    print(f"[ERROR] ارسال پست: {str(e)}")

        except Exception as e:
            print(f"[ERROR] فید {feed['site']}: {str(e)}")

    print(f"[INFO] پایان - پست‌ها: {posted}")
    return {"status": "success", "posted": posted}


if __name__ == "__main__":
    asyncio.run(main())
