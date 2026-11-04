import datetime as dt
import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

REPO=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(REPO))
SCRIPT=REPO/"scripts"/"instagram_autopublisher.py"
spec=importlib.util.spec_from_file_location("candidatory_autopublisher",SCRIPT)
publisher=importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)


class AutonomousNewsTests(unittest.TestCase):
    def test_parses_realistic_telegram_decorative_headline(self):
        msg="💠\nانتشار گزارش جدید قیمت بنزین در بازار\nتوضیح این خبر اقتصادی\n📰\nMehr\n🇮🇷\nکانال خبری"
        self.assertEqual(publisher.headline(msg),"انتشار گزارش جدید قیمت بنزین در بازار")
        self.assertEqual(publisher.source_name(msg),"مهر")
        self.assertEqual(publisher.classify(msg),"general")

    def test_all_sourced_topics_included_not_just_elections(self):
        self.assertEqual(publisher.classify("💠\nجام جهانی فوتبال در تهران برگزار شد"),"general")
        self.assertEqual(publisher.classify("💠\nانتخابات شورای شهر با حضور نامزدها"),"election")
        self.assertEqual(publisher.classify("💠\nچه پیامدهایی برای اقتصاد دارد؟"),"analysis")
        self.assertEqual(publisher.classify("💠\nقیمت سوخت در روز انتخابات افزایش یافت"),"general")

    def test_source_not_misidentified_from_word_entekhabat(self):
        self.assertIsNone(publisher.source_name("آخرین خبرهای انتخابات آمریکا\nنظرسنجی جدید"))
        self.assertEqual(publisher.source_name("خبر مهم\n📰\nEntekhab"),"انتخاب")

    def test_rejects_duplicated_or_unsourced_items(self):
        item={"id":5001,"text":"💠\nخبر اقتصادی\n📰\nMehr",
              "image_url":"https://example.com/photo.jpg","posted_at":publisher.utcnow(),
              "telegram_url":"https://t.me/candidatoryiran/5001"}
        self.assertFalse(publisher.eligible(item,{"posts":{"5001":{"status":"completed"}}}))
        self.assertTrue(publisher.eligible(item,{"posts":{}}))
        item["text"]="💠\nخبر بدون منبع قابل بررسی"
        self.assertFalse(publisher.eligible(item,{"posts":{}}))

    def test_parses_telegram_photo_and_timestamp(self):
        html='''<div class="tgme_widget_message_wrap"><div class="tgme_widget_message" data-post="candidatoryiran/5002">
        <div class="tgme_widget_message_text">💠<br>خبر اقتصادی<br>📰<br>Mehr</div>
        <a class="tgme_widget_message_photo_wrap" style="background-image:url('https://cdn.example.com/photo.jpg')"></a>
        <time datetime="2026-11-04T07:00:00+00:00"></time></div></div>'''
        posts=publisher.parse_posts(html)
        self.assertEqual(len(posts),1)
        self.assertEqual(posts[0]["id"],5002)
        self.assertIn("خبر اقتصادی",posts[0]["text"])

    def test_requires_private_server_credentials(self):
        with patch.dict("os.environ",{"INSTAGRAM_AUTOPUBLISH_ENABLED":"false"},clear=True):
            with patch.object(publisher.SESSION,"get",side_effect=AssertionError("must not access feed")):
                self.assertEqual(publisher.main(),0)


if __name__=="__main__":
    unittest.main()
