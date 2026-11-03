"""Reusable branded 4:5 feed and 9:16 story images for Candidatory.

Render only verified news and licensed/authorized source images.
No network calls or automatic publishing occur in this module.
Requires Pillow, arabic-reshaper, python-bidi and a licensed Persian TTF font.
"""
from __future__ import annotations

from io import BytesIO
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageOps
import arabic_reshaper
from bidi.algorithm import get_display

NAVY = "#10234A"
RED = "#D80D2C"
WHITE = "#FFFFFF"
LIGHT = "#F0F3FA"
HANDLE = "@candidatory.ir"
BRAND = "کاندیداتوری"


def rtl(value: str) -> str:
    return get_display(arabic_reshaper.reshape(str(value)))


def _font(path: str, size: int) -> ImageFont.FreeTypeFont:
    if not Path(path).is_file():
        raise FileNotFoundError("A licensed Persian font must be configured via font_path")
    return ImageFont.truetype(path, size)


def _wrap(text: str, draw: ImageDraw.ImageDraw, font, max_width: int, limit: int = 3):
    words = text.split()
    lines, current = [], ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if current and draw.textbbox((0, 0), rtl(candidate), font=font)[2] > max_width:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current:
        lines.append(current)
    if len(lines) > limit:
        lines = lines[:limit]
        lines[-1] = lines[-1].rstrip("…") + "…"
    return lines


def _right(draw, x, y, label, font, fill, max_width=None):
    shaped = rtl(label)
    if max_width and draw.textbbox((0, 0), shaped, font=font)[2] > max_width:
        raise ValueError("Text exceeds available width")
    draw.text((x, y), shaped, font=font, fill=fill, anchor="ra")


def _base(source: Image.Image, size: tuple[int, int], is_story: bool):
    width, height = size
    picture_top = int(height * (0.13 if is_story else 0.14))
    picture_bottom = int(height * (0.68 if is_story else 0.74))
    picture = ImageOps.fit(source.convert("RGB"), (width, picture_bottom - picture_top), method=Image.Resampling.LANCZOS, centering=(0.5, 0.38))
    canvas = Image.new("RGB", size, NAVY)
    canvas.paste(picture, (0, picture_top))
    draw = ImageDraw.Draw(canvas)
    draw.rectangle((0, picture_bottom, width, height), fill=WHITE if is_story else NAVY)
    draw.rectangle((width - 16, picture_bottom, width, height - 24), fill=RED)
    return canvas, draw, picture_bottom


def render(source: Image.Image, headline: str, category: str, source_name: str, font_path: str, *, story=False) -> Image.Image:
    """Create a single branded image. Neither headline nor category is AI-invented."""
    if not headline.strip() or not source_name.strip():
        raise ValueError("A sourced headline and publisher are required")
    w, h = (1080, 1920) if story else (1080, 1350)
    canvas, draw, lower = _base(source, (w, h), story)
    brand_font = _font(font_path, 76)
    label_font = _font(font_path, 33)
    title_font = _font(font_path, 61 if story else 65)
    small_font = _font(font_path, 27)
    _right(draw, w - 55, 35, BRAND, brand_font, WHITE)
    draw.rectangle((34, 116, w - 34, 121), fill=RED)
    draw.text((45, 135), HANDLE, font=small_font, fill=WHITE)
    label = category.strip() or "خبر منتخب"
    draw.rounded_rectangle((w - 365, lower - 82, w - 35, lower - 16), radius=15, fill=RED)
    _right(draw, w - 55, lower - 73, label, label_font, WHITE)

    headline_color = NAVY if story else WHITE
    y = lower + 57
    lines = _wrap(headline, draw, title_font, w - 130, limit=3)
    for line in lines:
        _right(draw, w - 55, y, line, title_font, headline_color)
        y += 92 if story else 91
    footer_color = NAVY if story else LIGHT
    footer_y = h - (225 if story else 90)
    _right(draw, w - 55, footer_y, "منبع: " + source_name, small_font, footer_color)
    if story:
        draw.rounded_rectangle((75, h - 155, w - 75, h - 60), radius=28, fill=RED)
        _right(draw, w - 120, h - 139, "خبر کامل در آخرین پست", label_font, WHITE)
    else:
        draw.text((45, h - 72), HANDLE, font=small_font, fill=WHITE)
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
