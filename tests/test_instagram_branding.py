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

    def test_bidi_text_nonempty(self):
        self.assertTrue(rtl("اخبار انتخابات"))


if __name__ == "__main__":
    unittest.main()
