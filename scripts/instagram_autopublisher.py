#!/usr/bin/env python3
"""Unattended Candidatory Telegram -> Instagram publisher (GitHub Actions runtime).

Uses repository's approved v9 Persian PIL renderer and the Composio *project*
API. Never exports tokens into Git history. Feed is confirmed before Story.
No credentials -> safe dry-run, not a fake published result.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from PIL import Image, ImageFont

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from instagram_branding import render_pair
STATE_FILE = ROOT / "data" / "instagram-published.json"
ASSET_DIR = ROOT / "instagram-media"
CHANNEL = "https://t.me/s/candidatoryiran"
IG_USER_ID = "28851602321171831"
IG_USERNAME = "candidatory.ir"
MEDIA_API = "https://backend.composio.dev/api/v3.1/tools/execute/"
DEFAULT_HISTORY = (4014, 4015, 4016, 4017, 4018, 4020, 4022)
MAX_CAPTION = 1800
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "CandidatoryScheduledPublisher/1.0"})


def utcnow():
    return dt.datetime.now(dt.timezone.utc)


def read_state():
    if not STATE_FILE.exists():
        return {"version": 1, "posts": {str(k): {"status": "historic"} for k in DEFAULT_HISTORY}}
    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    state.setdefault("posts", {})
    for k in DEFAULT_HISTORY:
        state["posts"].setdefault(str(k), {"status": "historic"})
    return state


def atomic_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    temp = STATE_FILE.with_suffix(".tmp")
    temp.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp, STATE_FILE)


def commit_state(reason, *files):
    """Save a checkpoint to git; first push images then immutable URLs can be used."""
    cmd = lambda *parts: subprocess.run(parts, cwd=ROOT, check=True, text=True, capture_output=True)
    paths = [str(STATE_FILE.relative_to(ROOT)), *(str(Path(f).relative_to(ROOT)) for f in files)]
    cmd("git", "add", *paths)
    changes = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=ROOT)
    if changes.returncode == 0:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()
    cmd("git", "commit", "-m", f"chore(instagram): {reason} [skip ci]")
    cmd("git", "push", "origin", "HEAD:main")
    return cmd("git", "rev-parse", "HEAD").stdout.strip()


def parse_posts(html):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for wrap in soup.select(".tgme_widget_message_wrap"):
        el = wrap.select_one(".tgme_widget_message")
        text = wrap.select_one(".tgme_widget_message_text")
        picture = wrap.select_one(".tgme_widget_message_photo_wrap")
        time_element = wrap.select_one("time[datetime]")
        if not (el and text and picture and time_element):
            continue
        match = re.fullmatch(r"candidatoryiran/(\d+)", el.get("data-post", ""))
        style = picture.get("style", "")
        url = re.search(r"background-image:\s*url\(['\"]?([^'\")]+)", style)
        if not (match and url):
            continue
        posted_at = dt.datetime.fromisoformat(time_element["datetime"].replace("Z", "+00:00"))
        if posted_at.tzinfo is None:
            posted_at = posted_at.replace(tzinfo=dt.timezone.utc)
        items.append({
            "id": int(match.group(1)), "text": text.get_text("\n", strip=True),
            "image_url": url.group(1), "posted_at": posted_at,
            "telegram_url": f"https://t.me/candidatoryiran/{match.group(1)}",
        })
    return sorted(items, key=lambda row: row["id"], reverse=True)


def headline(text):
    """Use first meaningful Telegram line, never a decorative emoji line."""
    for line in str(text).splitlines():
        value = line.strip().strip(" \u200c🔴🔵🟠📰💠🇮🇷▫️•")
        value = re.sub(r"^[^\w\u0600-\u06FF]+", "", value).strip()
        if not value or value.startswith(("#", "@", "کانال خبری", "منبع:")):
            continue
        value = re.sub(r"^(?:خبر فوری|فوری)\s*[:：]\s*", "", value)
        value = re.split(r"[#@]|\s+\|\s+", value)[0].strip()
        if 14 <= len(value) <= 160:
            return value
    return None


def classify(text):
    title = headline(text) or ""
    if re.match(r"^(?:خبر فوری|فوری|BREAKING)\b", title, re.I):
        return "breaking"
    if re.search(r"تحلیل|راهنما|جریمندرینگ|چگونه|پیامدها|راستی\s*آزمایی", title):
        return "analysis"
    if re.match(r"^(?:قیمت|سوخت|بنزین|نفت|ارز|هدف مذاکرات)", title):
        return "general"
    if re.search(r"انتخابات|رأی\s*گیری|رای\s*گیری|کنگره|شورای شهر", title):
        return "election"
    return "general"


def source_name(text):
    """Publisher names must come from the caption's source marker."""
    normalized = str(text).replace("\u200c", " ")
    lines = [x.strip(" \u200c▫️•📰") for x in normalized.splitlines()]
    labels = {
        "Mehr": "مهر", "ISNA": "ایسنا", "IRNA": "ایرنا",
        "Entekhab": "انتخاب", "Guardian": "گاردین",
        "YJC": "باشگاه خبرنگاران جوان",
    }
    for line in lines:
        if line in labels:
            return labels[line]
    patterns = [
        (r"خبرگزاری\s+مهر|به گزارش\s+مهر", "مهر"),
        (r"\bایسنا\b", "ایسنا"),
        (r"\bایرنا\b", "ایرنا"),
        (r"\bانتخاب\b(?!ات)", "انتخاب"),
        (r"\bباشگاه خبرنگاران(?: جوان)?\b", "باشگاه خبرنگاران جوان"),
        (r"\bهمشهری\b", "همشهری"),
        (r"\bگاردین\b", "گاردین"),
    ]
    for pattern, name in patterns:
        if re.search(pattern, normalized):
            return name
    m = re.search(r"(?:منبع\s*[:：]|به گزارش)\s*([^\n،؛]{3,42})", normalized)
    return m.group(1).strip() if m else None


def eligible(item, state, *, max_hours=18):
    if str(item["id"]) in state["posts"]:
        return False
    if not item["image_url"].startswith("https://"):
        return False
    if not (dt.timedelta(0) <= utcnow() - item["posted_at"] <= dt.timedelta(hours=max_hours)):
        return False
    if not source_name(item["text"]) or not headline(item["text"]):
        return False
    # This deliberately does not privilege elections over other trustworthy news.
    return True


def download_image(url):
    resp = SESSION.get(url, timeout=25)
    resp.raise_for_status()
    if len(resp.content) > 15_000_000:
        raise ValueError("Image too large")
    with Image.open(io.BytesIO(resp.content)) as im:
        if min(im.size) < 320:
            raise ValueError("Image resolution too low")
    return resp.content


def verified_fonts():
    body = os.environ.get("VAZIRMATN_PATH", "/tmp/candidatory-fonts/Vazirmatn.ttf")
    title = os.environ.get("CANDIDATORY_BTITR_PATH", "/tmp/candidatory-fonts/BTITRBD.TTF")
    family = ImageFont.truetype(title, 24).getname()[0].lower()
    if "titr" not in family:
        raise RuntimeError("B Titr is missing; refusing to render with substitute")
    ImageFont.truetype(body, 24)
    return body, title


def com_execute(slug, arguments):
    key = os.environ["COMPOSIO_API_KEY"]
    connected = os.environ["COMPOSIO_INSTAGRAM_CONNECTED_ACCOUNT_ID"]
    response = SESSION.post(
        MEDIA_API + slug,
        headers={"x-api-key": key, "Content-Type": "application/json"},
        json={"connected_account_id": connected, "version": "latest", "arguments": arguments},
        timeout=120,
    )
    response.raise_for_status()
    raw = response.json()
    if raw.get("successful") is False or raw.get("error"):
        raise RuntimeError(f"Composio {slug} failed: {str(raw.get('error'))[:300]}")
    result = raw.get("data", raw)
    if isinstance(result, dict) and isinstance(result.get("data"), dict):
        result = result["data"]
    return result


def checkpoint(state, item, status, **updates):
    rec = state["posts"].setdefault(str(item["id"]), {})
    rec.update(updates)
    rec.update({"status": status, "updated_at": utcnow().isoformat()})
    atomic_state(state)
    commit_state(f"{item['id']} {status}")


def media_id(data):
    value = data.get("id") if isinstance(data, dict) else None
    if not value:
        raise ValueError("Instagram did not return numeric media/container ID")
    value = str(value)
    if not value.isdigit():
        raise ValueError("Invalid Instagram numeric ID")
    return value


def publish(item, state, *, dry_run=False):
    category = classify(item["text"])
    title = headline(item["text"])
    source = source_name(item["text"])
    photo = download_image(item["image_url"])
    body_font, heading_font = verified_fonts()
    # Avoid invented summaries: source caption becomes Instagram caption; graphics carry title.
    feed, story = render_pair(photo, title, category, source, body_font,
                              headline_font_path=heading_font, summary=None)
    ASSET_DIR.mkdir(parents=True, exist_ok=True)
    feed_path = ASSET_DIR / f"{item['id']}-feed.jpg"
    story_path = ASSET_DIR / f"{item['id']}-story.jpg"
    feed_path.write_bytes(feed)
    story_path.write_bytes(story)
    if dry_run:
        print(f"DRY RUN {item['id']} {category}: {title}; feed={len(feed)} bytes story={len(story)} bytes")
        return

    state["posts"][str(item["id"])] = {
        "status": "images_committed", "category": category, "title": title,
        "source_url": item["telegram_url"], "template": "candidatory-editorial-v9-all-news",
        "created_at": utcnow().isoformat()
    }
    atomic_state(state)
    sha = commit_state(f"{item['id']} rendered media", feed_path, story_path)
    base = f"https://raw.githubusercontent.com/bahrammohamadi/candidatory/{sha}/instagram-media"
    urls = {"feed": f"{base}/{item['id']}-feed.jpg",
            "story": f"{base}/{item['id']}-story.jpg"}
    for kind in ("feed", "story"):
        for retry in range(8):
            resp = SESSION.get(urls[kind], timeout=20)
            if resp.ok and resp.content[:3] == b"\xff\xd8\xff":
                break
            time.sleep(5)
        else:
            raise RuntimeError(f"Published JPEG URL unavailable: {kind}")
    # The source news and its original permalink are included in the caption.
    caption = item["text"][:MAX_CAPTION].strip() + "\n\nمنبع: " + source + "\n" + item["telegram_url"]
    for kind in ("feed", "story"):
        if kind == "story" and not state["posts"][str(item["id"])].get("feed_media_id"):
            raise RuntimeError("Feed must be verified before story")
        args = {"ig_user_id": IG_USER_ID, "image_url": urls[kind]}
        if kind == "feed":
            args["caption"] = caption
        else:
            args["media_type"] = "STORIES"
        # State checkpoints prevent blind retry after partial/ambiguous publishing.
        checkpoint(state, item, f"{kind}_creating")
        container = media_id(com_execute("INSTAGRAM_POST_IG_USER_MEDIA", args))
        checkpoint(state, item, f"{kind}_ready", **{f"{kind}_container_id": container})
        checkpoint(state, item, f"{kind}_publishing")
        published = media_id(com_execute("INSTAGRAM_POST_IG_USER_MEDIA_PUBLISH",
                                         {"ig_user_id": IG_USER_ID,
                                          "creation_id": container, "max_wait_seconds": 90}))
        detail = com_execute("INSTAGRAM_GET_IG_MEDIA",
                             {"ig_media_id": published, "fields": "id,media_type,permalink,timestamp"})
        if media_id(detail) != published:
            raise RuntimeError("Published ID verification mismatch")
        checkpoint(state, item, f"{kind}_verified",
                   **{f"{kind}_media_id": published, f"{kind}_permalink": detail.get("permalink", "")})
    checkpoint(state, item, "completed")
    print("PUBLISHED", item["id"], state["posts"][str(item["id"])]["feed_permalink"],
          state["posts"][str(item["id"])]["story_permalink"])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    opts = p.parse_args()
    state = read_state()
    enabled = os.getenv("INSTAGRAM_AUTOPUBLISH_ENABLED", "").strip().lower() == "true"
    ready = enabled and all(os.environ.get(k) for k in
                            ("COMPOSIO_API_KEY", "COMPOSIO_INSTAGRAM_CONNECTED_ACCOUNT_ID"))
    if not ready and not opts.dry_run:
        print("BLOCKED: missing server authorization or INSTAGRAM_AUTOPUBLISH_ENABLED; no publication")
        return 0
    response = SESSION.get(CHANNEL, timeout=25)
    response.raise_for_status()
    candidates = parse_posts(response.text)
    candidates = [item for item in candidates if eligible(item, state)]
    if not candidates:
        print("No new eligible sourced Telegram news")
        return 0
    # Limit to one item per half-hour and check news chronology, not election topic.
    item = candidates[0]
    try:
        publish(item, state, dry_run=opts.dry_run)
    except Exception as exc:
        # Never auto-retry ambiguous Instagram publish: requires reconciliation.
        if str(item["id"]) in state["posts"] and not opts.dry_run:
            rec = state["posts"][str(item["id"])]
            rec["last_error"] = str(exc)[:500]
            rec["needs_manual_reconciliation"] = True
            atomic_state(state)
            commit_state(f"{item['id']} requires reconciliation")
        raise
    return 0


if __name__ == "__main__":
    sys.exit(main())
