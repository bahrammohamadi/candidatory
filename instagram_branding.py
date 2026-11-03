"""Reusable branded 4:5 feed and 9:16 story images for Candidatory.

Render only verified news and licensed/authorized source images.
No network calls or automatic publishing occur in this module.
Requires Pillow with libraqm and a licensed Persian TTF font.
"""
from __future__ import annotations

from io import BytesIO
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageOps, features

NAVY = "#10234A"
RED = "#D80D2C"
WHITE = "#FFFFFF"
LIGHT = "#F0F3FA"
HANDLE = "@candidatory.ir"
BRAND = "کاندیداتوری"
TEMPLATE_VERSION = "candidatory-editorial-v3-raqm"


def rtl(value: str) -> str:
    """Keep logical Unicode order; libraqm performs shaping once during draw."""
    return str(value)


def _font(path: str, size: int) -> ImageFont.FreeTypeFont:
    if not Path(path).is_file():
        raise FileNotFoundError("A licensed Persian font must be configured via font_path")
    return ImageFont.truetype(path, size)


def _width(draw, content, font):
    bbox = draw.textbbox((0, 0), content, font=font, direction="rtl")
    return bbox[2] - bbox[0]


def _wrap(text: str, draw: ImageDraw.ImageDraw, font, max_width: int, limit: int = 3):
    words = str(text).split()
    lines, current = [], ""
    for word in words:
        candidate = (current + " " + word).strip()
        if current and _width(draw, candidate, font) > max_width:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current:
        lines.append(current)
    if len(lines) > limit or any(_width(draw, line, font) > max_width for line in lines):
        raise ValueError("Headline does not fit the approved template")
    return lines


def _right(draw, x, y, label, font, fill, max_width=None):
    label = rtl(label)
    if max_width and _width(draw, label, font) > max_width:
        raise ValueError("Text exceeds available width")
    draw.text((x, y), label, font=font, fill=fill, anchor="ra", direction="rtl")


def _base(source: Image.Image, size: tuple[int, int], is_story: bool):
    """Branded press image with reserved story UI safe areas.

    No generic ballot-box fallback: caller must provide a verified image.
    """
    width, height = size
    picture_top = 375 if is_story else 185
    picture_bottom = 1060 if is_story else 840
    canvas = Image.new("RGB", size, NAVY)
    draw = ImageDraw.Draw(canvas)
    draw.polygon(((0, 0), (160, 0), (0, 240)), fill=RED)
    picture = ImageOps.fit(source.convert("RGB"), (width - 72, picture_bottom - picture_top), method=Image.Resampling.LANCZOS, centering=(0.5, 0.42))
    canvas.paste(picture, (36, picture_top))
    draw.rounded_rectangle((34, picture_top, width-34, picture_bottom), radius=18, outline=WHITE, width=4)
    draw.rectangle((width - 17, picture_bottom + 32, width - 7, picture_bottom + 285), fill=RED)
    return canvas, draw, picture_bottom


def render(source: Image.Image, headline: str, category: str, source_name: str, font_path: str, *, story=False) -> Image.Image:
    """Use one editorial layout for both outputs; the input must be source-verified."""
    if not headline.strip() or not source_name.strip():
        raise ValueError("A sourced headline and publisher are required")
    if not features.check("raqm"):
        raise RuntimeError("Pillow must support libraqm for Persian text")
    w, h = (1080, 1920) if story else (1080, 1350)
    canvas, draw, lower = _base(source, (w, h), story)
    brand_font = _font(font_path, 65)
    label_font = _font(font_path, 32)
    small_font = _font(font_path, 27)
    # Clear cream banner below story controls, with brand mark at a fixed position.
    banner_top = 230 if story else 22
    banner_bottom = 355 if story else 160
    draw.rounded_rectangle((35, banner_top, w - 35, banner_bottom), radius=28, fill=WHITE)
    draw.rounded_rectangle((55, banner_top + 26, 147, banner_top + 106), radius=16, outline=RED, width=7)
    draw.line(((72, banner_top + 68), (92, banner_top + 86), (129, banner_top + 43)), fill=RED, width=9)
    _right(draw, w - 65, banner_top + 30, BRAND, brand_font, NAVY)
    draw.text((46, banner_bottom + 14), HANDLE, font=ImageFont.load_default(size=29), fill=LIGHT)
    label = category.strip() or "خبر منتخب"
    draw.rounded_rectangle((w - 378, lower - 54, w - 50, lower + 15), radius=18, fill=RED)
    _right(draw, w - 72, lower - 42, label, label_font, WHITE)
    y = lower + 70
    lines = None
    title_font = None
    for size in range(57 if not story else 54, 37, -2):
        candidate_font = _font(font_path, size)
        try:
            candidate_lines = _wrap(headline, draw, candidate_font, w - 138, limit=3 if story else 2)
        except ValueError:
            continue
        lines, title_font = candidate_lines, candidate_font
        break
    if lines is None:
        raise ValueError("Headline too long: shorten it before publishing")
    for line in lines:
        _right(draw, w - 65, y, line, title_font, WHITE)
        y += 91 if story else 88
    # Always show attribution and a clear editorial footer.
    if story:
        _right(draw, w - 68, 1470, "منبع", small_font, LIGHT)
        _right(draw, w - 180, 1470, source_name, small_font, LIGHT)
        draw.rounded_rectangle((78, 1540, w - 78, 1635), radius=25, fill=RED)
        _right(draw, w - 126, 1560, "خبر کامل در آخرین پست", label_font, WHITE)
        # The final 285px stay clear for Instagram's reply controls.
    else:
        _right(draw, w - 65, h - 115, "منبع", small_font, LIGHT)
        _right(draw, w - 175, h - 115, source_name, small_font, LIGHT)
        draw.line((40, h - 67, w - 40, h - 67), fill=RED, width=4)
        draw.text((45, h - 57), HANDLE, font=ImageFont.load_default(size=29), fill=WHITE)
    return canvas


def render_pair(image_bytes: bytes, headline: str, category: str, source_name: str, font_path: str) -> tuple[bytes, bytes]:
    """Return (feed_jpeg, story_jpeg) suitable for uploading to public HTTPS media URLs."""
    if len(image_bytes) > 15_000_000:
        raise ValueError("Source image exceeds 15 MB")
    with Image.open(BytesIO(image_bytes)) as img:
        if img.width < 320 or img.height < 320:
            raise ValueError("Source image resolution is too low")
        feed = render(img, headline, category, source_name, font_path)
        story = render(img, headline, category, source_name, font_path, story=True)
    def encode(image):
        buffer = BytesIO()
        image.save(buffer, format="JPEG", quality=88, optimize=True, subsampling=0)
        return buffer.getvalue()
    return encode(feed), encode(story)
