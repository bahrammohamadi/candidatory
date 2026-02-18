import os
import asyncio
import feedparser
from datetime import datetime, timedelta, timezone
from telegram import Bot, LinkPreviewOptions
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query

async def main(event=None, context=None):
    print("[INFO] شروع")

    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHANNEL_ID')

    # برای Appwrite
    endpoint = os.environ.get('APPWRITE_ENDPOINT', 'https://cloud.appwrite.io/v1')
    project = os.environ.get('APPWRITE_PROJECT_ID')
    key = os.environ.get('APPWRITE_API_KEY')
    database_id = os.environ.get('APPWRITE_DATABASE_ID')
    collection_id = 'history'

    if not token or not chat_id:
        print("[ERROR] توکن یا chat_id نیست")
        return {"status": "error"}

    bot = Bot(token=token)

    client = Client()
    client.set_endpoint(endpoint)
    client.set_project(project)
    client.set_key(key)
    databases = Databases(client)

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
                print(f"[INFO] فید خالی: {url}")
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

                # چک تکراری قبل از ارسال
                is_duplicate = False
                try:
                    res = databases.list_documents(
                        database_id=database_id,
                        collection_id=collection_id,
                        queries=[Query.equal("link", link)],
                        limit=1
                    )
                    if res.get('total', 0) > 0:
                        is_duplicate = True
                        print(f"[SKIP] تکراری: {title[:70]}")
                except Exception as e:
                    print(f"[WARN] چک تکراری شکست خورد: {str(e)}")

                if is_duplicate:
                    continue

                # فرمت دقیق درخواستی
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

                    # ذخیره در دیتابیس
                    try:
                        databases.create_document(
                            database_id=database_id,
                            collection_id=collection_id,
                            document_id='unique()',
                            data={
                                'link': link,
                                'created_at': now.isoformat()
                            }
                        )
                        print("[DB] لینک ذخیره شد")
                    except Exception as save_err:
                        print(f"[WARN] خطا در ذخیره دیتابیس: {str(save_err)}")

                except Exception as send_err:
                    print(f"[ERROR] خطا در ارسال: {str(send_err)}")

        except Exception as e:
            print(f"[ERROR] مشکل در فید {url}: {str(e)}")

    print(f"[INFO] پایان - ارسال شد: {posted}")
    return {"status": "success", "posted": posted}


if __name__ == "__main__":
    asyncio.run(main())
