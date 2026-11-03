import io
import unittest
from unittest.mock import patch

from PIL import Image
from instagram_branding import render_pair, rtl


class BrandingTests(unittest.TestCase):
    def setUp(self):
        self.source = io.BytesIO()
        Image.new("RGB", (1200, 900), (80, 110, 140)).save(self.source, "JPEG")

    def test_empty_headline_rejected(self):
        with self.assertRaises(ValueError):
            render_pair(self.source.getvalue(), "", "انتخابات", "IRNA", "/missing.ttf")

    def test_missing_source_rejected(self):
        with self.assertRaises(ValueError):
            render_pair(self.source.getvalue(), "خبر نمونه", "انتخابات", "", "/missing.ttf")

    def test_small_image_rejected(self):
        b = io.BytesIO()
        Image.new("RGB", (120, 120)).save(b, "JPEG")
        with self.assertRaises(ValueError):
            render_pair(b.getvalue(), "خبر نمونه", "انتخابات", "IRNA", "/missing.ttf")

    def test_missing_font_is_explicit(self):
        with self.assertRaises(FileNotFoundError):
            render_pair(self.source.getvalue(), "خبر نمونه", "انتخابات", "IRNA", "/missing.ttf")


    def test_rendered_jpegs_have_instagram_dimensions(self):
        font = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
        if not __import__("os").path.exists(font):
            self.skipTest("No test font available")
        feed_bytes, story_bytes = render_pair(
            self.source.getvalue(), "اطلاعیه خبری نمونه", "اخبار", "IRNA", font
        )
        with Image.open(io.BytesIO(feed_bytes)) as feed:
            self.assertEqual(feed.size, (1080, 1350))
            self.assertEqual(feed.format, "JPEG")
        with Image.open(io.BytesIO(story_bytes)) as story:
            self.assertEqual(story.size, (1080, 1920))
            self.assertEqual(story.format, "JPEG")

    def test_dimensions_and_story_safe_footer(self):
        from pathlib import Path
        font = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
        if not Path(font).exists():
            self.skipTest("CI system font unavailable")
        feed_bytes, story_bytes = render_pair(
            self.source.getvalue(),
            "آغاز رأی گیری انتخابات",
            "خبر انتخابات",
            "خبرگزاری مهر",
            font,
        )
        feed = Image.open(io.BytesIO(feed_bytes))
        story = Image.open(io.BytesIO(story_bytes))
        self.assertEqual(feed.size, (1080, 1350))
        self.assertEqual(story.size, (1080, 1920))
        # Safe lower strip is not filled with a giant CTA behind reply controls.
        self.assertTrue(all(abs(a-b) <= 3 for a,b in zip(story.getpixel((500, 1850)), (11, 29, 62))))

    def test_stale_oversized_story_summary_rejected(self):
        import os
        font = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
        if not os.path.exists(font):
            self.skipTest("No system font")
        with self.assertRaises(ValueError):
            render_pair(
                self.source.getvalue(),
                "اعلام آخرین نتایج برنامه‌ریزی مسابقات ملی و مرحله استانی دانشجویان",
                "خبر آموزشی", "ایسنا", font,
                summary="خلاصه بسیار بلند و نامناسب برای طراحی استوری " * 12,
            )

    def test_persian_is_not_double_reversed(self):
        # Critical regression: Pillow/libraqm owns RTL shaping and bidi reordering.
        self.assertEqual(rtl("کاندیداتوری"), "کاندیداتوری")
        self.assertEqual(rtl("خبر کامل در آخرین پست"), "خبر کامل در آخرین پست")

    def test_long_headline_fails_instead_of_overprinting(self):
        font = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
        if not __import__("os").path.exists(font):
            self.skipTest("No test font available")
        with self.assertRaises(ValueError):
            render_pair(self.source.getvalue(), "خبر " * 100, "شورا", "ایسنا", font)


if __name__ == "__main__":
    unittest.main()
