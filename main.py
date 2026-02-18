import os
import asyncio
import feedparser
from datetime import datetime, timedelta, timezone
from telegram import Bot, LinkPreviewOptions

# فقط برای تست، Appwrite رو فعلاً کامنت می‌کنیم
# from appwrite.client import Client
# from appwrite.services.databases import Databases
# from appwrite.query import Query

async def main(event=None, context=None):
    print("[INFO] شروع تست ساده بدون دیتابیس")

    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHANNEL_ID')

    if not token or not chat_id:
        print("[ERROR] توکن یا chat_id نیست")
        return {"status": "error"}

    bot = Bot(token=token)

    # تست پیام
    try:
        await bot.send_message(
            chat_id=chat_id,
            text="تست بدون دیتابیس - فقط ارسال پیام\nاگر این پیام رو دیدی یعنی بات کار می‌کنه"
        )
        print("[SUCCESS] پیام تست ارسال شد")
    except Exception as e:
        print(f"[ERROR] ارسال پیام شکست خورد: {str(e)}")

    print("[INFO] پایان تست ساده")
    return {"status": "ok"}


if __name__ == "__main__":
    asyncio.run(main())
