"""CLI for approved, repeatable Candidatory image rendering.

Usage:
 python render_instagram_news.py --photo licensed-photo.jpg --rights-confirmed \
   --headline "تیتر تأییدشده" --source "خبرگزاری" --category "خبر انتخابات" \
   --output-dir /tmp/candidatory-news

Never fetch or republish a source image without verified usage rights.
The font is downloaded to a private temporary cache and never committed.
"""
import argparse
import os
import tempfile
from pathlib import Path
from urllib.request import Request, urlopen

from instagram_branding import render_pair

FONT_URL = "https://raw.githubusercontent.com/google/fonts/main/ofl/vazirmatn/Vazirmatn%5Bwght%5D.ttf"
# B Titr is supplied from a privately licensed file; never commit its bytes.
HEADLINE_FONT_ENV = "CANDIDATORY_BTITR_PATH"
MAX_FONT_BYTES = 3_000_000


def resolve_font(path: str | None, *, headline: bool=False) -> str:
    if headline:
        approved = path or os.getenv(HEADLINE_FONT_ENV)
        if not approved or not Path(approved).is_file():
            raise FileNotFoundError("B Titr Bold missing: supply --headline-font or CANDIDATORY_BTITR_PATH")
        return approved
    if path:
        if not Path(path).is_file():
            raise FileNotFoundError(path)
        return path
    target = Path(tempfile.gettempdir()) / "candidatory-vazirmatn-variable.ttf"
    if target.is_file() and target.stat().st_size > 10000:
        return str(target)
    req = Request(FONT_URL, headers={"User-Agent": "CandidatoryEditorialRenderer/1.0"})
    with urlopen(req, timeout=15) as stream:
        data = stream.read(MAX_FONT_BYTES + 1)
    if not (10_000 < len(data) < MAX_FONT_BYTES):
        raise ValueError("Could not verify downloaded font")
    target.write_bytes(data)
    return str(target)


def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument("--photo", required=True, help="Local source photo you have rights to use")
    p.add_argument("--rights-confirmed", action="store_true", required=True)
    p.add_argument("--headline", required=True)
    p.add_argument("--source", required=True)
    p.add_argument("--category", default="خبر منتخب")
    p.add_argument("--summary", default=None)
    p.add_argument("--font", help="Optional licensed body typeface; defaults to Vazirmatn")
    p.add_argument("--headline-font", help="Required licensed B Titr Bold path, or set CANDIDATORY_BTITR_PATH")
    p.add_argument("--output-dir", required=True)
    args=p.parse_args(argv)
    if not args.rights_confirmed:
        raise SystemExit("Source image rights must be verified before use.")
    source=Path(args.photo)
    if not source.is_file() or source.stat().st_size > 15_000_000:
        raise SystemExit("Missing or oversized licensed image")
    feed,story=render_pair(source.read_bytes(),args.headline,args.category,args.source,resolve_font(args.font),summary=args.summary,headline_font_path=resolve_font(args.headline_font,headline=True))
    output=Path(args.output_dir);output.mkdir(parents=True,exist_ok=True)
    (output/"feed.jpg").write_bytes(feed)
    (output/"story.jpg").write_bytes(story)
    print("feed.jpg story.jpg")


if __name__=="__main__":
    main()
