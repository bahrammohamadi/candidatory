import os
import asyncio
from telegram import Bot

async def main(event=None, context=None):
    print("[TEST] اجرای تابع تست شروع شد")
    print("[TEST] تاریخ فعلی:", os.popen('date').read().strip())

    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHANNEL_ID')

    if not token or not chat_id:
        print("[TEST] توکن یا chat_id تنظیم نشده")
        print("[TEST] متغیرهای محیطی موجود:", list(os.environ.keys()))
        return {"status": "missing env vars"}

    print("[TEST] توکن و chat_id پیدا شدند")

    bot = Bot(token=token)

    try:
        await bot.send_message(
            chat_id=chat_id,
            text="تست موفق از Appwrite Function\n\nتاریخ: ۱۴۰۴/۱۱/۲۹\nاین پیام یعنی ارتباط بات → کانال برقرار است"
        )
        print("[TEST] پیام تست با موفقیت به کانال ارسال شد")
    except Exception as e:
        print("[TEST] خطا در ارسال پیام به تلگرام:", str(e))
        print("[TEST] chat_id فعلی:", chat_id)

    print("[TEST] پایان اجرای تابع تست")
    return {"status": "ok"}


if __name__ == "__main__":
    asyncio.run(main())
