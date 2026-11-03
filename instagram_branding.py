"""Candidatory editorial news templates — v4.

Stable original-photo compositing with one-pass Persian RTL (Pillow libraqm).
Generate Instagram feed 4:5 and story 9:16; images only, never publishes.
Fail closed if the title does not fit; verify rights before passing an image.
"""
from __future__ import annotations

from io import BytesIO
import base64
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageOps, ImageFilter, features

NAVY = "#10234A"
DEEP = "#0B1D3E"
RED = "#E31336"
WHITE = "#FFFFFF"
CREAM = "#F6F7FA"
MUTED = "#D4DEED"
HANDLE = "@candidatory.ir"
BRAND = "کاندیداتوری"
TEMPLATE_VERSION = "candidatory-editorial-v7-news-type-polish"

def rtl(value: str) -> str:
    """Store Unicode in logical order; libraqm does shaping and bidi exactly once."""
    return str(value)

def _font(path: str, size: int, weight: int | None = None) -> ImageFont.FreeTypeFont:
    if not Path(path).is_file():
        raise FileNotFoundError("A licensed Persian font must be configured via font_path")
    font = ImageFont.truetype(path, size)
    if weight is not None:
        try:
            axes = font.get_variation_axes()
            if axes and any("Weight" in str(axis.get("name","")) for axis in axes):
                axis = axes[0]
                clamped = max(axis["minimum"], min(axis["maximum"], weight))
                font.set_variation_by_axes([clamped])
        except (OSError, AttributeError, ValueError, KeyError):
            pass
    return font

def _width(draw, text: str, font) -> int:
    box = draw.textbbox((0, 0), text, font=font, direction="rtl")
    return box[2] - box[0]

def _wrap(text: str, draw, font, limit_width: int, max_lines: int):
    words = str(text).strip().split()
    lines, line = [], ""
    for word in words:
        proposed = (line + " " + word).strip()
        if line and _width(draw, proposed, font) > limit_width:
            lines.append(line)
            line = word
        else:
            line = proposed
    if line:
        lines.append(line)
    if len(lines) > max_lines or any(_width(draw, t, font) > limit_width for t in lines):
        raise ValueError("Persian text exceeds approved template safe bounds")
    return lines

def _right(draw, x, y, text, font, color, max_width=None):
    value = rtl(text)
    if max_width is not None and _width(draw, value, font) > max_width:
        raise ValueError("RTL field does not fit its visual boundary")
    draw.text((x, y), value, font=font, fill=color, anchor="ra", direction="rtl")

def _fitted_lines(text, draw, font_path, *, max_width, max_lines, max_size, min_size, weight=None):
    for size in range(max_size, min_size - 1, -2):
        font = _font(font_path, size, weight)
        try:
            return _wrap(text, draw, font, max_width, max_lines), font
        except ValueError:
            pass
    raise ValueError("Text too long: editorial shortening is required")

def _brand_icon(canvas, draw, x, y, size):
    """Use the real Instagram Candidatory profile emblem, versioned in assets."""
    logo_path = Path(__file__).resolve().parent / "assets/candidatory-profile-logo.jpg.b64"
    if not logo_path.exists():
        raise FileNotFoundError("Authentic brand logo missing; publication forbidden")
    logo_bytes = base64.b64decode(logo_path.read_text().strip(), validate=True)
    with Image.open(BytesIO(logo_bytes)) as original:
        logo = ImageOps.fit(original.convert("RGB"), (size,size), method=Image.Resampling.LANCZOS)
    mask = Image.new("L", (size,size), 0)
    ImageDraw.Draw(mask).ellipse((0,0,size-1,size-1),fill=255)
    canvas.paste(logo,(x,y),mask)
    draw.ellipse((x,y,x+size,y+size),outline=WHITE,width=3)

def _photo_panel(canvas, source, left, top, right, bottom):
    pw, ph = right-left, bottom-top
    source = source.convert("RGB")
    # Fill unused space with blurred version; preserve the entire source photograph/poster.
    background = ImageOps.fit(source,(pw,ph),method=Image.Resampling.LANCZOS)
    background = background.filter(ImageFilter.GaussianBlur(20))
    canvas.paste(background,(left,top))
    foreground = ImageOps.contain(source, (pw,ph), method=Image.Resampling.LANCZOS)
    canvas.paste(foreground,(left+(pw-foreground.width)//2,top+(ph-foreground.height)//2))
    d=ImageDraw.Draw(canvas)
    d.rounded_rectangle((left,top,right,bottom),radius=20,outline=WHITE,width=4)

def _headline(draw, headline, font_path, y, *, story=False):
    lines, font = _fitted_lines(
        headline,draw,font_path,max_width=950,max_lines=3 if story else 2,
        max_size=58 if story else 61,min_size=38 if story else 43,weight=800)
    x=1004
    for line in lines:
        _right(draw,x,y,line,font,WHITE,max_width=950)
        y += font.size+31
    return y

def render(source: Image.Image, headline: str, category: str, source_name: str,
           font_path: str, *, story=False, summary: str | None=None,
           headline_font_path: str | None=None) -> Image.Image:
    if not headline.strip() or not source_name.strip():
        raise ValueError("A sourced headline and publisher are required")
    if headline_font_path is None:
        raise ValueError("B Titr Bold headline font must be explicitly supplied")
    if not features.check("raqm"):
        raise RuntimeError("Pillow libraqm required for correct Persian RTL rendering")
    w,h=(1080,1920) if story else (1080,1350)
    canvas=Image.new("RGB",(w,h),DEEP)
    d=ImageDraw.Draw(canvas)
    d.polygon(((0,0),(225,0),(0,280)),fill=RED)
    if story:
        header_y=236; photo_top=375; photo_end=1080
    else:
        header_y=37; photo_top=180; photo_end=819
    _brand_icon(canvas,d,54,header_y,91)
    _right(d,1007,header_y+7,BRAND,_font(headline_font_path or font_path,64,700),WHITE,max_width=600)
    d.text((160,header_y+77),"CANDIDATORY.IR",font=_font(font_path,24,550),fill=MUTED)

    _photo_panel(canvas,source,38,photo_top,1042,photo_end)
    d=ImageDraw.Draw(canvas)
    label=category.strip() or "خبر منتخب"
    label_font=_font(font_path,35,700)
    badge_w=max(230,min(400,_width(d,label,label_font)+100))
    bx=1030-badge_w
    d.rounded_rectangle((bx,photo_top-32,1030,photo_top+59),radius=24,fill=RED,outline=WHITE,width=3)
    _right(d,1003,photo_top-15,label,label_font,WHITE,max_width=badge_w-55)

    title_start=photo_end+(57 if story else 50)
    # Accent beside headline, not overlaid on source photograph.
    d.rounded_rectangle((1022,title_start-3,1036,title_start+225),radius=7,fill=RED)
    last_title_y=_headline(d,headline,headline_font_path or font_path,title_start,story=story)

    summary_y=max(last_title_y+10,1340 if story else 1053)
    if summary:
        lines,font=_fitted_lines(summary,d,font_path,max_width=944,max_lines=3 if story else 2,
                                 max_size=37 if story else 33,min_size=29,weight=550)
        summary_height = len(lines)*(font.size+24)
        if story and summary_y+summary_height > 1520:
            raise ValueError("Story summary overlaps fixed source/CTA area")
        if not story and summary_y+summary_height > 1228:
            raise ValueError("Feed summary overlaps source bar")
        for line in lines:
            _right(d,1003,summary_y,line,font,MUTED,max_width=944)
            summary_y += font.size+24

    if story:
        d.rounded_rectangle((64,1600,1016,1688),radius=27,fill=RED)
        _right(d,971,1617,"خبر کامل در آخرین پست",_font(font_path,39,750),WHITE,max_width=875)
        footer_y=1540
    else:
        footer_y=1246
    d.rounded_rectangle((39,footer_y,1040,footer_y+65),radius=22,outline=MUTED,width=2)
    d.text((61,footer_y+17),HANDLE,font=_font(font_path,29,500),fill=WHITE)
    _right(d,1009,footer_y+10,"منبع: "+source_name,_font(font_path,31,600),WHITE,max_width=550)
    return canvas

def render_pair(image_bytes: bytes, headline: str, category: str,
                source_name: str, font_path: str, *, summary: str | None=None,
                headline_font_path: str | None=None) -> tuple[bytes,bytes]:
    if len(image_bytes)>15_000_000:
        raise ValueError("Source image exceeds 15MB")
    with Image.open(BytesIO(image_bytes)) as im:
        if im.width<320 or im.height<320:
            raise ValueError("Source image resolution is too low")
        feed=render(im,headline,category,source_name,font_path,summary=summary,headline_font_path=headline_font_path)
        story=render(im,headline,category,source_name,font_path,story=True,summary=summary,headline_font_path=headline_font_path)
    def encode(im):
        b=BytesIO()
        im.save(b,format="JPEG",quality=91,optimize=True,subsampling=0)
        return b.getvalue()
    return encode(feed),encode(story)
