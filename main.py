import os
import asyncio
import feedparser
import requests
import hashlib
from datetime import datetime, timedelta, timezone
from telegram import Bot, LinkPreviewOptions

# اگر بعداً ابزار search_images رو در محیط واقعی داشتی، اینجا استفاده می‌شه
# فعلاً فرض می‌کنیم لینک تصویر از RSS یا پیش‌فرض می‌گیریم
DEFAULT_IMAGE = "https://example.com/your-fallback-news-image.jpg"  # ← لینک ثابت بگذار

async def main(event=None, context=None):
    print("[INFO] شروع")

    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHANNEL_ID')
    endpoint = os.environ.get('APPWRITE_ENDPOINT', 'https://cloud.appwrite.io/v1')
    project = os.environ.get('APPWRITE_PROJECT_ID')
    key = os.environ.get('APPWRITE_API_KEY')
    database_id = os.environ.get('APPWRITE_DATABASE_ID')
    collection_id = 'history'

    if not all([token, chat_id, endpoint, project, key, database_id]):
        print("[ERROR] متغیرهای محیطی ناقص")
        return {"status": "error"}

    bot = Bot(token=token)

    headers = {
        'Content-Type': 'application/json',
        'X-Appwrite-Project': project,
        'X-Appwrite-Key': key,
    }

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

                content_for_hash = (title.lower().strip() + " " + description[:150].lower().strip())
                content_hash = hashlib.sha256(content_for_hash.encode('utf-8')).hexdigest()

                is_duplicate = False
                try:
                    params_link = {'queries[0]': f'equal("link", ["{link}"])', 'limit': 1}
                    res_link = requests.get(
                        f"{endpoint}/databases/{database_id}/collections/{collection_id}/documents",
                        headers=headers,
                        params=params_link
                    )
                    if res_link.status_code == 200:
                        data_link = res_link.json()
                        if data_link.get('total', 0) > 0:
                            is_duplicate = True
                            print(f"[SKIP] تکراری (لینک): {title[:70]}")

                    if not is_duplicate:
                        params_hash = {'queries[0]': f'equal("content_hash", ["{content_hash}"])', 'limit': 1}
                        res_hash = requests.get(
                            f"{endpoint}/databases/{database_id}/collections/{collection_id}/documents",
                            headers=headers,
                            params=params_hash
                        )
                        if res_hash.status_code == 200:
                            data_hash = res_hash.json()
                            if data_hash.get('total', 0) > 0:
                                is_duplicate = True
                                print(f"[SKIP] تکراری (محتوا): {title[:70]}")
                except Exception as e:
                    print(f"[WARN] خطا چک تکراری: {str(e)}")

                if is_duplicate:
                    continue

                final_text = (
                    f"<b>{title}</b>\n\n"
                    f"@candidatoryiran\n\n"
                    f"{description}\n\n"
                    f"_____________\n"
                    f"کانال خبری کاندیداتوری\n"
                    f"🆔 @candidatoryiran\n"
                    f"🆔 Instagram.com/candidatory.ir\n"
                )

                image_url = None
                if 'enclosure' in entry and entry.enclosure.get('type', '').startswith('image/'):
                    image_url = entry.enclosure.href
                elif 'media_content' in entry:
                    for media in entry.media_content:
                        if media.get('medium') == 'image' and media.get('url'):
                            image_url = media['url']
                            break

                # اگر RSS عکس نداشت → اینجا می‌تونی جستجوی تصویر اضافه کنی
                if not image_url:
                    # جستجوی تصویر با تیتر (در محیط واقعی با ابزار search_images)
                    # فعلاً از عکس ثابت استفاده می‌کنیم
                    image_url = DEFAULT_IMAGE
                    print(f"[FALLBACK] عکس پیش‌فرض برای: {title[:50]}")

                try:
                    await bot.send_photo(
                        chat_id=chat_id,
                        photo=image_url,
                        caption=final_text,
                        parse_mode='HTML',
                        disable_notification=True
                    )

                    posted = True
                    print(f"[SUCCESS] ارسال موفق با عکس: {title[:70]}")

                    # ذخیره
                    try:
                        payload = {
                            'documentId': 'unique()',
                            'data': {
                                'link': link,
                                'title': title[:300],
                                'content_hash': content_hash,
                                'created_at': now.isoformat()
                            }
                        }
                        res = requests.post(
                            f"{endpoint}/databases/{database_id}/collections/{collection_id}/documents",
                            headers=headers,
                            json=payload
                        )
                        if res.status_code in (200, 201):
                            print("[DB] ذخیره موفق")
                        else:
                            print(f"[WARN] ذخیره شکست: {res.status_code} - {res.text}")
                    except Exception as save_err:
                        print(f"[WARN] خطا ذخیره: {str(save_err)}")

                except Exception as send_err:
                    print(f"[ERROR] خطا ارسال: {str(send_err)}")

        except Exception as feed_err:
            print(f"[ERROR] مشکل فید {url}: {str(feed_err)}")

    print(f"[INFO] پایان - ارسال شد: {posted}")
    return {"status": "success", "posted": posted}


if __name__ == "__main__":
    asyncio.run(main())
