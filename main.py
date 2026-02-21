# ============================================================
# Telegram Election News Bot — @candidatoryiran
# Version:    3.0 — High-Volume + Context-Sensitive Scoring
# Runtime:    Python 3.12 / Appwrite Cloud Functions
# Timeout:    30 seconds (Appwrite free plan limit)
#
# ARCHITECTURE:
#   Phase 1: Fetch all feeds in parallel (budget: 8s)
#   Phase 2: Load DB state once — history + buffer (budget: 3s)
#   Phase 3: Score + extract entities (in-memory, instant)
#   Phase 4: Dedup (in-memory, instant)
#   Phase 5: Triage — HIGH→immediate, MEDIUM→buffer, LOW→discard
#   Phase 6: Publish from queue (budget: remaining ~14s)
#
# SCORING (dual-layer):
#   Layer 1: Core election keywords (+3 title, +1 desc)
#   Layer 2: Contextual political patterns (+2 title, +1 desc)
#   Rejection keywords: instant discard (-100)
#   Score thresholds:
#     HIGH   ≥ 5  → publish immediately
#     MEDIUM ≥ 2  → buffer for 1-2 hours, publish if slot available
#     LOW    < 2  → discard
#
# DEDUP: SHA-256 of sorted normalized title tokens
#         content_hash[:36] = Appwrite document ID (atomic)
#         Save BEFORE post (race-condition proof)
#
# RATE CONTROL:
#   Dynamic hourly budget based on score distribution
#   HIGH items: unlimited (up to Telegram limits)
#   MEDIUM items: max 5/hour (configurable)
#   Burst protection: 1.5s inter-post delay
# ============================================================

import os
import re
import asyncio
import hashlib
import requests
import feedparser

from datetime import datetime, timedelta, timezone
from time import monotonic
from bs4 import BeautifulSoup
from telegram import Bot, InputMediaPhoto, LinkPreviewOptions
from telegram.error import TelegramError


# ═══════════════════════════════════════════════════════════
# SECTION 1 — CONFIGURATION
# ═══════════════════════════════════════════════════════════

RSS_SOURCES: list[tuple[str, str]] = [
    ("https://www.farsnews.ir/rss",                    "Fars"),
    ("https://www.isna.ir/rss",                        "ISNA"),
    ("https://www.tasnimnews.com/fa/rss/feed/0/0/0",   "Tasnim"),
    ("https://www.mehrnews.com/rss",                    "Mehr"),
    ("https://www.entekhab.ir/fa/rss/allnews",         "Entekhab"),
    ("https://www.irna.ir/rss/fa/8/",                  "IRNA"),
    ("https://www.yjc.ir/fa/rss/allnews",              "YJC"),
    ("https://www.tabnak.ir/fa/rss/allnews",           "Tabnak"),
    ("https://www.khabaronline.ir/rss",                "KhabarOnline"),
    ("https://www.hamshahrionline.ir/rss",             "Hamshahri"),
    ("https://www.ilna.ir/fa/rss",                     "ILNA"),
    ("https://feeds.bbci.co.uk/persian/rss.xml",       "BBCPersian"),
]

# ── Timing budgets (seconds) ──
GLOBAL_DEADLINE_SEC  = 27
FEED_FETCH_TIMEOUT   = 4
FEEDS_TOTAL_TIMEOUT  = 8
DB_TIMEOUT           = 3
IMAGE_SCRAPE_TIMEOUT = 3
TELEGRAM_TIMEOUT     = 5
INTER_POST_DELAY     = 1.5

# ── Content limits ──
MAX_IMAGES            = 5
MAX_DESCRIPTION_CHARS = 500
CAPTION_MAX           = 1024
HOURS_THRESHOLD       = 24
BUFFER_HOURS          = 2
FUZZY_THRESHOLD       = 0.55

# ── Score thresholds ──
SCORE_HIGH            = 5
SCORE_MEDIUM          = 2
MEDIUM_HOURLY_LIMIT   = 5
HIGH_HOURLY_LIMIT     = 20

# ── Image filters ──
IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.webp')
IMAGE_BLOCKLIST  = [
    'doubleclick', 'googletagmanager', 'analytics',
    'pixel', 'beacon', 'tracking', 'stat.', 'stats.',
]

# ═══════════════════════════════════════════════════════════
# SECTION 2 — KEYWORD SYSTEM (DUAL-LAYER)
# ═══════════════════════════════════════════════════════════

# ── Layer 1: Core election keywords (high weight) ──
LAYER1_KEYWORDS = [
    # Persian core
    "انتخابات", "انتخاباتی", "ریاست‌جمهوری", "ریاست جمهوری",
    "مجلس شورای اسلامی", "شورای شهر", "شورای اسلامی",
    "نامزد", "کاندیدا", "کاندیدای", "داوطلب",
    "ثبت‌نام", "ثبت نام", "ثبتنام",
    "رد صلاحیت", "تایید صلاحیت", "احراز صلاحیت", "بررسی صلاحیت",
    "صلاحیت", "هیئت نظارت", "شورای نگهبان",
    "ستاد انتخابات", "ستاد انتخاباتی",
    "حوزه انتخابیه", "تبلیغات انتخاباتی",
    "صندوق رای", "صندوق رأی", "رای‌گیری", "رایگیری",
    "مشارکت انتخاباتی", "دور دوم انتخابات",
    "انتخابات ریاست", "انتخابات مجلس", "انتخابات شورا",
    "لیست انتخاباتی", "ائتلاف انتخاباتی",
    # English core
    "election", "elections", "electoral", "candidate",
    "ballot", "vote", "voting", "presidential",
    "parliament", "parliamentary", "runoff",
    "iran election", "iranian election", "majles",
    "disqualification", "qualification",
]

# ── Layer 2: Contextual political keywords (medium weight) ──
LAYER2_KEYWORDS = [
    # Political context
    "رای", "رای دادن", "انتخاب", "منتخب",
    "نماینده", "نمایندگان", "وکیل مجلس",
    "اصلاح‌طلب", "اصلاحطلب", "اصولگرا", "اصولگرایان",
    "ائتلاف", "جبهه", "حزب", "فراکسیون",
    "ستاد", "مناظره", "تبلیغات",
    "مجلس", "شورا", "پارلمان",
    # Candidate behavior patterns
    "وعده", "وعده‌های", "وعده انتخاباتی",
    "برنامه", "برنامه‌های", "طرح",
    "بیانیه", "اعلامیه", "سخنرانی",
    "اگر انتخاب شوم", "در صورت انتخاب",
    "دولت آینده", "مجلس آینده", "شورای آینده",
    # Government/policy context
    "وزارت", "وزیر", "سیاست", "سیاسی",
    "دولت", "رئیس‌جمهور", "رئیس جمهور",
    "قانون", "لایحه", "مصوبه",
    "آموزش", "آموزش و پرورش", "بهداشت", "درمان",
    "ورزش", "جوانان", "فرهنگ",
    "اقتصاد", "اشتغال", "تورم", "گرانی",
    "مسکن", "حمل و نقل", "شهرداری",
    # English contextual
    "debate", "polling", "poll", "voter",
    "campaign", "promise", "policy", "ministry",
    "reform", "conservative", "coalition",
    "statement", "speech", "manifesto",
]

# ── Rejection keywords (instant discard) ──
REJECTION_KEYWORDS = [
    "فیلم سینمایی", "سریال", "بازیگر", "سینما",
    "فوتبال", "والیبال", "لیگ برتر", "جام جهانی",
    "بورس", "ارز دیجیتال", "بیت کوین", "رمزارز",
    "زلزله", "سیل", "آتش سوزی", "تصادف",
    "آشپزی", "رسپی", "مد و فشن",
    "فال روز", "طالع بینی", "هواشناسی",
]

# ── Candidate names (entity extraction + scoring boost) ──
KNOWN_CANDIDATES = [
    "پزشکیان", "جلیلی", "قالیباف", "زاکانی",
    "لاریجانی", "رئیسی", "روحانی", "احمدی‌نژاد",
    "همتی", "مهرعلیزاده", "رضایی", "هاشمی",
    "عارف", "جهانگیری", "واعظی", "ظریف",
    "میرسلیم", "پورمحمدی", "آملی لاریجانی",
]

# ── Topic categories for tagging ──
TOPIC_PATTERNS: dict[str, list[str]] = {
    "صلاحیت":    ["صلاحیت", "رد صلاحیت", "تایید صلاحیت", "احراز صلاحیت",
                   "بررسی صلاحیت", "شورای نگهبان", "هیئت نظارت"],
    "ثبت‌نام":   ["ثبت‌نام", "ثبت نام", "ثبتنام", "داوطلب", "نامزد"],
    "تبلیغات":   ["تبلیغات", "ستاد", "مناظره", "سخنرانی", "بیانیه"],
    "رای‌گیری":  ["رای", "رایگیری", "رای‌گیری", "صندوق", "مشارکت"],
    "نتایج":     ["نتایج", "شمارش", "پیروز", "منتخب", "آرا"],
    "مجلس":      ["مجلس", "نماینده", "نمایندگان", "فراکسیون", "پارلمان"],
    "شورا":      ["شورای شهر", "شورای اسلامی", "شورا", "شهرداری"],
    "اقتصاد":    ["اقتصاد", "اشتغال", "تورم", "گرانی", "مسکن", "بودجه"],
    "سیاست_خارجی": ["سیاست خارجی", "دیپلماسی", "برجام", "تحریم", "مذاکره"],
}

PERSIAN_STOPWORDS = {
    "و", "در", "به", "از", "که", "این", "را", "با", "های",
    "برای", "آن", "یک", "هم", "تا", "اما", "یا", "بود",
    "شد", "است", "می", "هر", "اگر", "بر", "ها", "نیز",
    "کرد", "خود", "هیچ", "پس", "باید", "نه", "ما", "شود",
    "the", "a", "an", "is", "are", "was", "of", "in",
    "to", "for", "and", "or", "but", "with", "on",
}

# ── Hashtag mapping ──
_HASHTAG_MAP = [
    ("انتخابات",       "#انتخابات"),
    ("ریاست",          "#ریاست_جمهوری"),
    ("ریاستجمهوری",    "#ریاست_جمهوری"),
    ("مجلس",           "#مجلس"),
    ("شورا",           "#شورای_شهر"),
    ("کاندیدا",        "#کاندیدا"),
    ("نامزد",          "#نامزد_انتخاباتی"),
    ("ثبتنام",         "#ثبت_نام"),
    ("صلاحیت",         "#صلاحیت"),
    ("شورای نگهبان",   "#شورای_نگهبان"),
    ("رای",            "#رأی"),
    ("مناظره",         "#مناظره"),
    ("تبلیغات",        "#تبلیغات_انتخاباتی"),
    ("مشارکت",         "#مشارکت"),
    ("صندوق",          "#صندوق_رأی"),
    ("نماینده",        "#نمایندگان"),
    ("اصلاحطلب",       "#اصلاح_طلبان"),
    ("اصولگرا",        "#اصولگرایان"),
    ("اقتصاد",         "#اقتصاد"),
    ("دیپلماسی",       "#سیاست_خارجی"),
    ("election",       "#Election"),
    ("candidate",      "#Candidate"),
    ("parliament",     "#Parliament"),
    ("vote",           "#Vote"),
    ("presidential",   "#Presidential"),
]


# ═══════════════════════════════════════════════════════════
# SECTION 3 — MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════

async def main(event=None, context=None):
    _t0 = monotonic()

    def _remaining() -> float:
        return GLOBAL_DEADLINE_SEC - (monotonic() - _t0)

    _log("══════════════════════════════")
    _log("Election Bot v3.0 started")
    _log(f"{datetime.now(timezone.utc).isoformat()}")
    _log("══════════════════════════════")

    config = _load_config()
    if not config:
        return {"status": "error", "reason": "missing_env_vars"}

    bot = Bot(token=config["token"])
    db  = _AppwriteDB(
        endpoint      = config["endpoint"],
        project       = config["project"],
        key           = config["key"],
        database_id   = config["database_id"],
        collection_id = config["collection_id"],
    )

    now            = datetime.now(timezone.utc)
    time_threshold = now - timedelta(hours=HOURS_THRESHOLD)
    buffer_cutoff  = now - timedelta(hours=BUFFER_HOURS)
    loop           = asyncio.get_event_loop()

    stats = {
        "fetched": 0, "skip_time": 0, "skip_topic": 0,
        "skip_dupe": 0, "posted_high": 0, "posted_medium": 0,
        "buffered": 0, "errors": 0,
    }

    # ══════════════════════════════════════════════════════
    # Phase 1: Fetch ALL feeds in parallel
    # ══════════════════════════════════════════════════════
    budget = min(FEEDS_TOTAL_TIMEOUT, _remaining() - 18)
    if budget < 3:
        _log("Not enough time for feed fetch.")
        return {"status": "success", "posted": 0}

    _log(f"Fetching {len(RSS_SOURCES)} feeds (budget={budget:.1f}s)...")
    try:
        all_entries: list[dict] = await asyncio.wait_for(
            _fetch_all_feeds_parallel(loop),
            timeout=budget,
        )
    except asyncio.TimeoutError:
        _log("Feed fetch timed out — using partial results.", level="WARN")
        all_entries = []

    _log(f"Entries collected: {len(all_entries)} ({_remaining():.1f}s left)")

    if not all_entries:
        return {"status": "success", "posted": 0}

    all_entries.sort(
        key=lambda x: x["pub_date"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    # ══════════════════════════════════════════════════════
    # Phase 2: Load DB state ONCE (history + buffer)
    # ══════════════════════════════════════════════════════
    known_links:   set[str]   = set()
    known_hashes:  set[str]   = set()
    fuzzy_records: list[dict] = []
    buffered_items: list[dict] = []
    hourly_post_count: dict[str, int] = {"high": 0, "medium": 0}

    db_budget = min(DB_TIMEOUT, _remaining() - 15)
    if db_budget > 1:
        try:
            raw_records = await asyncio.wait_for(
                loop.run_in_executor(None, db.load_recent, 500),
                timeout=db_budget,
            )
            for rec in raw_records:
                if rec.get("link"):
                    known_links.add(rec["link"])
                if rec.get("content_hash"):
                    known_hashes.add(rec["content_hash"])
                fuzzy_records.append({
                    "title":      rec.get("title", ""),
                    "title_norm": _normalize_text(rec.get("title", "")),
                })

                # Count recent posts for rate limiting
                created = rec.get("created_at", "")
                if created:
                    try:
                        ct = datetime.fromisoformat(created.replace("Z", "+00:00"))
                        if ct > now - timedelta(hours=1):
                            tier = rec.get("score_tier", "high")
                            if tier in hourly_post_count:
                                hourly_post_count[tier] += 1
                    except (ValueError, TypeError):
                        pass

            _log(f"Loaded {len(raw_records)} DB records → "
                 f"{len(known_links)} links, {len(known_hashes)} hashes, "
                 f"hourly: H={hourly_post_count['high']} M={hourly_post_count['medium']} "
                 f"({_remaining():.1f}s left)")
        except asyncio.TimeoutError:
            _log("DB load timed out — dedup limited.", level="WARN")

        # Load buffer
        try:
            buf_budget = min(DB_TIMEOUT, _remaining() - 13)
            if buf_budget > 1:
                buffered_items = await asyncio.wait_for(
                    loop.run_in_executor(None, db.load_buffer),
                    timeout=buf_budget,
                )
                _log(f"Buffer: {len(buffered_items)} items waiting")
        except asyncio.TimeoutError:
            _log("Buffer load timed out.", level="WARN")
    else:
        _log("No time budget for DB load.", level="WARN")

    posted_hashes: set[str] = set()

    # ══════════════════════════════════════════════════════
    # Phase 3+4: Score, Extract, Dedup, Triage
    # ══════════════════════════════════════════════════════
    publish_queue: list[dict] = []   # HIGH items → immediate
    buffer_queue:  list[dict] = []   # MEDIUM items → buffer

    for item in all_entries:
        title    = item["title"]
        link     = item["link"]
        desc     = item["desc"]
        source   = item["source"]
        feed_url = item["feed_url"]
        pub_date = item["pub_date"]

        stats["fetched"] += 1

        # ── Time filter ──
        if pub_date and pub_date < time_threshold:
            stats["skip_time"] += 1
            continue

        # ── Dual-layer scoring ──
        score_result = _score_article(title, desc)
        score      = score_result["score"]
        tier       = score_result["tier"]
        candidates = score_result["candidates"]
        topics     = score_result["topics"]

        if tier == "LOW":
            stats["skip_topic"] += 1
            continue

        # ── Dedup (all in-memory) ──
        content_hash = _make_hash(title)

        if content_hash in posted_hashes:
            stats["skip_dupe"] += 1
            _log_item("SKIP:dupe:local", source, title, score, tier)
            continue

        if content_hash in known_hashes:
            stats["skip_dupe"] += 1
            _log_item("SKIP:dupe:hash", source, title, score, tier)
            continue

        if link in known_links:
            stats["skip_dupe"] += 1
            _log_item("SKIP:dupe:link", source, title, score, tier)
            continue

        if _is_fuzzy_duplicate(title, fuzzy_records):
            stats["skip_dupe"] += 1
            _log_item("SKIP:dupe:fuzzy", source, title, score, tier)
            continue

        # ── Build enriched item ──
        enriched = {
            **item,
            "content_hash": content_hash,
            "score":        score,
            "tier":         tier,
            "candidates":   candidates,
            "topics":       topics,
        }

        # ── Triage ──
        if tier == "HIGH":
            publish_queue.append(enriched)
            _log_item("QUEUE:HIGH", source, title, score, tier,
                      candidates=candidates, topics=topics)
        elif tier == "MEDIUM":
            buffer_queue.append(enriched)
            _log_item("QUEUE:MEDIUM", source, title, score, tier,
                      candidates=candidates, topics=topics)

    _log(f"Triage complete: {len(publish_queue)} HIGH, "
         f"{len(buffer_queue)} MEDIUM to buffer")

    # ── Add mature buffer items to publish queue ──
    mature_buffer = [
        b for b in buffered_items
        if b.get("buffered_at", "") and _is_buffer_mature(b["buffered_at"], buffer_cutoff)
    ]
    if mature_buffer:
        _log(f"Buffer: {len(mature_buffer)} mature items ready to publish")

    # ══════════════════════════════════════════════════════
    # Phase 5: Publish
    # ══════════════════════════════════════════════════════

    # ── Publish HIGH items first (no hourly limit) ──
    for item in publish_queue:
        if _remaining() < 10:
            _log(f"Time budget low ({_remaining():.1f}s). Stopping HIGH publish.")
            break

        if hourly_post_count["high"] >= HIGH_HOURLY_LIMIT:
            _log("HIGH hourly limit reached. Remaining HIGH items buffered.")
            buffer_queue.append(item)
            continue

        success = await _publish_item(
            item, bot, config["chat_id"], db, loop,
            now, _remaining,
        )
        if success:
            stats["posted_high"] += 1
            hourly_post_count["high"] += 1
            posted_hashes.add(item["content_hash"])
            known_hashes.add(item["content_hash"])
            known_links.add(item["link"])
            fuzzy_records.append({
                "title":      item["title"],
                "title_norm": _normalize_text(item["title"]),
            })
            if _remaining() > 4:
                await asyncio.sleep(INTER_POST_DELAY)
        else:
            stats["errors"] += 1

    # ── Publish mature MEDIUM items (within hourly limit) ──
    medium_candidates = mature_buffer + [
        b for b in buffer_queue
        if b.get("score", 0) >= SCORE_HIGH - 1
    ]
    medium_candidates.sort(key=lambda x: x.get("score", 0), reverse=True)

    for item in medium_candidates:
        if _remaining() < 10:
            _log(f"Time budget low ({_remaining():.1f}s). Stopping MEDIUM publish.")
            break

        if hourly_post_count["medium"] >= MEDIUM_HOURLY_LIMIT:
            _log("MEDIUM hourly limit reached.")
            break

        if item.get("content_hash", "") in posted_hashes:
            continue
        if item.get("content_hash", "") in known_hashes:
            continue

        success = await _publish_item(
            item, bot, config["chat_id"], db, loop,
            now, _remaining,
        )
        if success:
            stats["posted_medium"] += 1
            hourly_post_count["medium"] += 1
            posted_hashes.add(item["content_hash"])
            known_hashes.add(item["content_hash"])
            known_links.add(item.get("link", ""))
            fuzzy_records.append({
                "title":      item.get("title", ""),
                "title_norm": _normalize_text(item.get("title", "")),
            })
            if _remaining() > 4:
                await asyncio.sleep(INTER_POST_DELAY)
        else:
            stats["errors"] += 1

    # ── Save remaining MEDIUM items to buffer ──
    remaining_buffer = [
        b for b in buffer_queue
        if b.get("content_hash", "") not in posted_hashes
        and b.get("content_hash", "") not in known_hashes
    ]
    for item in remaining_buffer[:10]:
        if _remaining() < 4:
            break
        try:
            await asyncio.wait_for(
                loop.run_in_executor(
                    None, db.save_buffer,
                    item.get("link", ""),
                    item.get("title", ""),
                    item.get("content_hash", ""),
                    item.get("source", ""),
                    item.get("feed_url", ""),
                    item.get("desc", ""),
                    item.get("score", 0),
                    item.get("tier", "MEDIUM"),
                    ",".join(item.get("candidates", [])),
                    ",".join(item.get("topics", [])),
                    now.isoformat(),
                ),
                timeout=2,
            )
            stats["buffered"] += 1
        except (asyncio.TimeoutError, Exception) as e:
            _log(f"Buffer save failed: {e}", level="WARN")

    # ── Summary ──
    elapsed = monotonic() - _t0
    total_posted = stats["posted_high"] + stats["posted_medium"]
    print(f"\n[INFO] ─────── SUMMARY ({elapsed:.1f}s) ───────")
    print(f"[INFO] Fetched      : {stats['fetched']}")
    print(f"[INFO] Skip/time    : {stats['skip_time']}")
    print(f"[INFO] Skip/topic   : {stats['skip_topic']}")
    print(f"[INFO] Skip/dupe    : {stats['skip_dupe']}")
    print(f"[INFO] Posted HIGH  : {stats['posted_high']}")
    print(f"[INFO] Posted MEDIUM: {stats['posted_medium']}")
    print(f"[INFO] Buffered     : {stats['buffered']}")
    print(f"[INFO] Errors       : {stats['errors']}")
    print(f"[INFO] Rate H={hourly_post_count['high']}/{HIGH_HOURLY_LIMIT} "
          f"M={hourly_post_count['medium']}/{MEDIUM_HOURLY_LIMIT}")
    print("[INFO] ──────────────────────────────")

    return {"status": "success", "posted": total_posted}


# ═══════════════════════════════════════════════════════════
# SECTION 4 — PUBLISH SINGLE ITEM
# ═══════════════════════════════════════════════════════════

async def _publish_item(
    item: dict,
    bot: Bot,
    chat_id: str,
    db: '_AppwriteDB',
    loop: asyncio.AbstractEventLoop,
    now: datetime,
    _remaining,
) -> bool:
    """
    Atomic publish: save to DB → collect images → build caption → post.
    If DB save fails, skip post entirely.
    """
    title        = item.get("title", "")
    link         = item.get("link", "")
    desc         = item.get("desc", "")
    source       = item.get("source", "")
    feed_url     = item.get("feed_url", "")
    content_hash = item.get("content_hash", "")
    score        = item.get("score", 0)
    tier         = item.get("tier", "HIGH")
    candidates   = item.get("candidates", [])
    topics       = item.get("topics", [])
    pub_date     = item.get("pub_date")

    # ── Save to DB BEFORE posting ──
    save_budget = min(DB_TIMEOUT, _remaining() - 7)
    if save_budget < 1:
        _log("No time for DB save.", level="WARN")
        return False

    pub_iso = pub_date.isoformat() if pub_date else now.isoformat()

    try:
        saved = await asyncio.wait_for(
            loop.run_in_executor(
                None, db.save,
                link, title, content_hash, source, feed_url,
                pub_iso, now.isoformat(), score, tier,
                ",".join(candidates), ",".join(topics),
            ),
            timeout=save_budget,
        )
    except asyncio.TimeoutError:
        _log("DB save timed out — skip post.", level="WARN")
        return False

    if not saved:
        _log(f"DB save failed for [{source}] {title[:40]}", level="WARN")
        return False

    # ── Collect images ──
    image_urls: list[str] = []
    img_budget = min(IMAGE_SCRAPE_TIMEOUT, _remaining() - 6)
    if img_budget > 1:
        entry = item.get("entry")
        if entry:
            try:
                image_urls = await asyncio.wait_for(
                    _collect_images_async(entry, link, loop),
                    timeout=img_budget,
                )
            except asyncio.TimeoutError:
                image_urls = []

    # ── Build caption ──
    hashtags = _generate_hashtags(title, desc, topics)
    caption  = _build_caption(title, desc, hashtags, candidates, source)

    # ── Post to Telegram ──
    tg_budget = min(TELEGRAM_TIMEOUT, _remaining() - 2)
    if tg_budget < 2:
        _log("No time for Telegram post.", level="WARN")
        return False

    try:
        success = await asyncio.wait_for(
            _post_to_telegram(bot, chat_id, image_urls, caption),
            timeout=tg_budget,
        )
    except asyncio.TimeoutError:
        _log("Telegram post timed out.", level="WARN")
        success = False

    if success:
        _log_item("POSTED", source, title, score, tier,
                  candidates=candidates, topics=topics)

    # Clean buffer entry if it came from buffer
    if success and item.get("buffer_doc_id"):
        try:
            await asyncio.wait_for(
                loop.run_in_executor(
                    None, db.delete_buffer_item, item["buffer_doc_id"]
                ),
                timeout=1,
            )
        except Exception:
            pass

    return success


# ═══════════════════════════════════════════════════════════
# SECTION 5 — DUAL-LAYER SCORING ENGINE
# ═══════════════════════════════════════════════════════════

def _score_article(title: str, desc: str) -> dict:
    """
    Dual-layer scoring with entity extraction.

    Returns:
        {
            "score": int,
            "tier": "HIGH" | "MEDIUM" | "LOW",
            "candidates": list[str],
            "topics": list[str],
        }
    """
    norm_title = _normalize_text(title)
    norm_desc  = _normalize_text(desc)
    combined   = norm_title + " " + norm_desc
    raw_combined = title + " " + desc

    # ── Rejection check ──
    for kw in REJECTION_KEYWORDS:
        if _normalize_text(kw) in combined:
            return {"score": -1, "tier": "LOW",
                    "candidates": [], "topics": []}

    score = 0

    # ── Layer 1: Core election keywords ──
    for kw in LAYER1_KEYWORDS:
        kw_n = _normalize_text(kw)
        if not kw_n:
            continue
        if kw_n in norm_title:
            score += 3
        elif kw_n in norm_desc:
            score += 1

    # ── Layer 2: Contextual political keywords ──
    for kw in LAYER2_KEYWORDS:
        kw_n = _normalize_text(kw)
        if not kw_n:
            continue
        if kw_n in norm_title:
            score += 2
        elif kw_n in norm_desc:
            score += 1

    # ── Candidate name boost ──
    candidates_found: list[str] = []
    for name in KNOWN_CANDIDATES:
        name_n = _normalize_text(name)
        if name_n in combined:
            candidates_found.append(name)
            if name_n in norm_title:
                score += 2
            else:
                score += 1

    # ── Topic extraction ──
    topics_found: list[str] = []
    for topic, patterns in TOPIC_PATTERNS.items():
        for pat in patterns:
            pat_n = _normalize_text(pat)
            if pat_n and pat_n in combined:
                if topic not in topics_found:
                    topics_found.append(topic)
                break

    # ── Multi-signal boost ──
    if candidates_found and topics_found:
        score += 2
    if len(candidates_found) >= 2:
        score += 1

    # ── Determine tier ──
    if score >= SCORE_HIGH:
        tier = "HIGH"
    elif score >= SCORE_MEDIUM:
        tier = "MEDIUM"
    else:
        tier = "LOW"

    return {
        "score":      score,
        "tier":       tier,
        "candidates": candidates_found,
        "topics":     topics_found,
    }


# ═══════════════════════════════════════════════════════════
# SECTION 6 — PARALLEL FEED FETCHER
# ═══════════════════════════════════════════════════════════

async def _fetch_all_feeds_parallel(loop: asyncio.AbstractEventLoop) -> list[dict]:
    tasks = [
        loop.run_in_executor(None, _fetch_one_feed, url, name)
        for url, name in RSS_SOURCES
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_entries: list[dict] = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            _log(f"{RSS_SOURCES[i][1]}: {result}", level="ERROR")
            continue
        if result:
            all_entries.extend(result)
    return all_entries


def _fetch_one_feed(url: str, source_name: str) -> list[dict]:
    try:
        resp = requests.get(
            url,
            timeout=FEED_FETCH_TIMEOUT,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; ElectionBot/3.0)",
                "Accept":     "application/rss+xml, application/xml, */*",
            },
        )
        if resp.status_code != 200:
            _log(f"{source_name}: HTTP {resp.status_code}", level="WARN")
            return []

        feed = feedparser.parse(resp.content)
        if feed.bozo and not feed.entries:
            _log(f"{source_name}: Malformed feed", level="WARN")
            return []

        entries = []
        for entry in feed.entries:
            title = _clean(entry.get("title", ""))
            link  = _clean(entry.get("link",  ""))
            if not title or not link:
                continue

            raw_html = (
                entry.get("summary")
                or entry.get("description")
                or ""
            )
            desc     = _truncate(_strip_html(raw_html), MAX_DESCRIPTION_CHARS)
            pub_date = _parse_date(entry)

            entries.append({
                "title":    title,
                "link":     link,
                "desc":     desc,
                "pub_date": pub_date,
                "source":   source_name,
                "feed_url": url,
                "entry":    entry,
            })

        _log(f"[FEED] {source_name}: {len(entries)} entries")
        return entries

    except requests.RequestException as e:
        _log(f"{source_name} fetch: {e}", level="ERROR")
        return []
    except Exception as e:
        _log(f"{source_name} parse: {e}", level="ERROR")
        return []


# ═══════════════════════════════════════════════════════════
# SECTION 7 — APPWRITE DATABASE CLIENT
# ═══════════════════════════════════════════════════════════

class _AppwriteDB:
    """
    Appwrite schema (collection "history"):
      link           string  700  required, indexed
      title          string  300
      site           string  100
      published_at   datetime
      created_at     datetime
      feed_url       string  500
      content_hash   string  128  indexed

    Buffer collection ("buffer"):
      link           string  700
      title          string  300
      content_hash   string  128  indexed
      site           string  100
      feed_url       string  500
      desc           string  500
      score          integer
      score_tier     string  10
      candidates     string  500
      topics         string  300
      buffered_at    datetime
    """

    def __init__(self, endpoint, project, key, database_id, collection_id):
        self._endpoint = endpoint
        self._database_id = database_id
        self._history_url = (
            f"{endpoint}/databases/{database_id}"
            f"/collections/{collection_id}/documents"
        )
        self._buffer_collection = os.environ.get(
            "APPWRITE_BUFFER_COLLECTION_ID", "buffer"
        )
        self._buffer_url = (
            f"{endpoint}/databases/{database_id}"
            f"/collections/{self._buffer_collection}/documents"
        )
        self._headers = {
            "Content-Type":       "application/json",
            "X-Appwrite-Project": project,
            "X-Appwrite-Key":     key,
        }

    def load_recent(self, limit: int = 500) -> list[dict]:
        try:
            resp = requests.get(
                self._history_url,
                headers=self._headers,
                params={"limit": str(limit), "orderType": "DESC"},
                timeout=DB_TIMEOUT,
            )
            if resp.status_code != 200:
                _log(f"DB load_recent: HTTP {resp.status_code}", level="WARN")
                return []
            docs = resp.json().get("documents", [])
            return [
                {
                    "link":         d.get("link", ""),
                    "title":        d.get("title", ""),
                    "content_hash": d.get("content_hash", ""),
                    "created_at":   d.get("created_at", ""),
                    "score_tier":   d.get("score_tier", "high"),
                }
                for d in docs
            ]
        except Exception as e:
            _log(f"DB load_recent: {e}", level="WARN")
            return []

    def load_buffer(self) -> list[dict]:
        try:
            resp = requests.get(
                self._buffer_url,
                headers=self._headers,
                params={"limit": "50", "orderType": "DESC"},
                timeout=DB_TIMEOUT,
            )
            if resp.status_code != 200:
                return []
            docs = resp.json().get("documents", [])
            return [
                {
                    "link":          d.get("link", ""),
                    "title":         d.get("title", ""),
                    "content_hash":  d.get("content_hash", ""),
                    "source":        d.get("site", ""),
                    "feed_url":      d.get("feed_url", ""),
                    "desc":          d.get("desc", ""),
                    "score":         d.get("score", 0),
                    "tier":          d.get("score_tier", "MEDIUM"),
                    "candidates":    (d.get("candidates", "") or "").split(","),
                    "topics":        (d.get("topics", "") or "").split(","),
                    "buffered_at":   d.get("buffered_at", ""),
                    "buffer_doc_id": d.get("$id", ""),
                }
                for d in docs
            ]
        except Exception as e:
            _log(f"DB load_buffer: {e}", level="WARN")
            return []

    def save(self, link: str, title: str, content_hash: str,
             site: str, feed_url: str, published_at: str,
             created_at: str, score: int = 0, score_tier: str = "",
             candidates: str = "", topics: str = "") -> bool:
        doc_id = content_hash[:36]
        data = {
            "link":         link[:700],
            "title":        title[:300],
            "content_hash": content_hash[:128],
            "site":         site[:100],
            "feed_url":     feed_url[:500],
            "published_at": published_at,
            "created_at":   created_at,
        }
        try:
            resp = requests.post(
                self._history_url,
                headers=self._headers,
                json={"documentId": doc_id, "data": data},
                timeout=DB_TIMEOUT,
            )
            if resp.status_code in (200, 201):
                return True
            if resp.status_code == 409:
                _log("DB 409 — already exists")
                return False
            _log(f"DB save {resp.status_code}: {resp.text[:200]}", level="WARN")
            return False
        except Exception as e:
            _log(f"DB save: {e}", level="WARN")
            return False

    def save_buffer(self, link: str, title: str, content_hash: str,
                    site: str, feed_url: str, desc: str,
                    score: int, score_tier: str,
                    candidates: str, topics: str,
                    buffered_at: str) -> bool:
        doc_id = f"buf_{content_hash[:30]}"
        data = {
            "link":         link[:700],
            "title":        title[:300],
            "content_hash": content_hash[:128],
            "site":         site[:100],
            "feed_url":     feed_url[:500],
            "desc":         desc[:500],
            "score":        score,
            "score_tier":   score_tier[:10],
            "candidates":   candidates[:500],
            "topics":       topics[:300],
            "buffered_at":  buffered_at,
        }
        try:
            resp = requests.post(
                self._buffer_url,
                headers=self._headers,
                json={"documentId": doc_id, "data": data},
                timeout=DB_TIMEOUT,
            )
            if resp.status_code in (200, 201):
                return True
            if resp.status_code == 409:
                return True  # already buffered
            _log(f"Buffer save {resp.status_code}: {resp.text[:100]}", level="WARN")
            return False
        except Exception as e:
            _log(f"Buffer save: {e}", level="WARN")
            return False

    def delete_buffer_item(self, doc_id: str) -> bool:
        try:
            resp = requests.delete(
                f"{self._buffer_url}/{doc_id}",
                headers=self._headers,
                timeout=DB_TIMEOUT,
            )
            return resp.status_code in (200, 204)
        except Exception:
            return False


# ═══════════════════════════════════════════════════════════
# SECTION 8 — CONFIG LOADER
# ═══════════════════════════════════════════════════════════

def _load_config() -> dict | None:
    cfg = {
        "token":         os.environ.get("TELEGRAM_BOT_TOKEN"),
        "chat_id":       os.environ.get("TELEGRAM_CHANNEL_ID"),
        "endpoint":      os.environ.get("APPWRITE_ENDPOINT",
                                        "https://cloud.appwrite.io/v1"),
        "project":       os.environ.get("APPWRITE_PROJECT_ID"),
        "key":           os.environ.get("APPWRITE_API_KEY"),
        "database_id":   os.environ.get("APPWRITE_DATABASE_ID"),
        "collection_id": os.environ.get("APPWRITE_COLLECTION_ID", "history"),
    }
    missing = [k for k, v in cfg.items() if not v]
    if missing:
        _log(f"Missing env vars: {missing}", level="ERROR")
        return None
    return cfg


# ═══════════════════════════════════════════════════════════
# SECTION 9 — TEXT UTILITIES
# ═══════════════════════════════════════════════════════════

def _clean(text: str) -> str:
    return (text or "").strip()


def _strip_html(html: str) -> str:
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script", "style", "iframe"]):
            tag.decompose()
        return " ".join(soup.get_text(separator=" ").split())
    except Exception:
        return re.sub(r"<[^>]+>", " ", html).strip()


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    cut        = text[:limit]
    last_space = cut.rfind(" ")
    if last_space > limit * 0.8:
        cut = cut[:last_space]
    return cut + "…"


def _parse_date(entry) -> datetime | None:
    for field in ("published_parsed", "updated_parsed"):
        parsed = entry.get(field)
        if parsed:
            try:
                return datetime(*parsed[:6], tzinfo=timezone.utc)
            except (ValueError, TypeError):
                continue
    return None


def _escape_html(text: str) -> str:
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
    )


def _normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("ي", "ی").replace("ك", "ک")
    text = text.replace("ة", "ه").replace("ؤ", "و")
    text = text.replace("إ", "ا").replace("أ", "ا")
    text = text.replace("ئ", "ی").replace("ى", "ی")
    text = re.sub(r"[\u064B-\u065F\u0670]", "", text)
    text = re.sub(r"[\u200c\u200d\u200e\u200f\ufeff]", "", text)
    text = text.lower()
    text = re.sub(r"[^\w\s\u0600-\u06FF]", " ", text)
    text = " ".join(text.split())
    tokens = [
        t for t in text.split()
        if t not in PERSIAN_STOPWORDS and len(t) >= 2
    ]
    return " ".join(tokens)


def _make_hash(title: str, desc: str = "") -> str:
    norm_title = _normalize_text(title)
    tokens = sorted(norm_title.split())
    canonical = " ".join(tokens)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ═══════════════════════════════════════════════════════════
# SECTION 10 — FUZZY DUPLICATE DETECTION
# ═══════════════════════════════════════════════════════════

def _is_fuzzy_duplicate(title: str, recent_records: list[dict]) -> bool:
    if not recent_records:
        return False
    incoming = set(_normalize_text(title).split())
    if len(incoming) < 2:
        return False
    for record in recent_records:
        stored = set(record.get("title_norm", "").split())
        if len(stored) < 2:
            continue
        inter = len(incoming & stored)
        if inter == 0:
            continue

        min_size = min(len(incoming), len(stored))
        overlap  = inter / min_size

        union   = len(incoming | stored)
        jaccard = inter / union

        if overlap >= 0.75 or jaccard >= FUZZY_THRESHOLD:
            return True
    return False


def _is_buffer_mature(buffered_at_str: str, cutoff: datetime) -> bool:
    try:
        bt = datetime.fromisoformat(buffered_at_str.replace("Z", "+00:00"))
        return bt <= cutoff
    except (ValueError, TypeError):
        return True


# ═══════════════════════════════════════════════════════════
# SECTION 11 — IMAGE COLLECTION
# ═══════════════════════════════════════════════════════════

async def _collect_images_async(
    entry, article_url: str, loop: asyncio.AbstractEventLoop,
) -> list[str]:
    images = _extract_rss_images(entry)
    if not images:
        try:
            og = await asyncio.wait_for(
                loop.run_in_executor(None, _fetch_og_image, article_url),
                timeout=3.0,
            )
            if og:
                images.append(og)
        except asyncio.TimeoutError:
            pass
    return images[:MAX_IMAGES]


def _extract_rss_images(entry) -> list[str]:
    images: list[str] = []
    seen:   set[str]  = set()

    def _add(url: str) -> None:
        url = (url or "").strip()
        if not url or not url.startswith("http") or url in seen:
            return
        lower = url.lower()
        if any(b in lower for b in IMAGE_BLOCKLIST):
            return
        base     = lower.split("?")[0]
        has_ext  = any(base.endswith(e) for e in IMAGE_EXTENSIONS)
        has_word = any(w in lower for w in
                       ["image", "photo", "img", "media", "cdn", "upload"])
        if not has_ext and not has_word:
            return
        seen.add(url)
        images.append(url)

    for m in entry.get("media_content", []):
        url    = m.get("url", "")    if isinstance(m, dict) else getattr(m, "url", "")
        medium = m.get("medium", "") if isinstance(m, dict) else getattr(m, "medium", "")
        if medium == "image" or any(url.lower().endswith(e) for e in IMAGE_EXTENSIONS):
            _add(url)

    enclosures = entry.get("enclosures", [])
    if not enclosures and hasattr(entry, "enclosure") and entry.enclosure:
        enclosures = [entry.enclosure]
    for enc in enclosures:
        mime = enc.get("type", "") if isinstance(enc, dict) else getattr(enc, "type", "")
        href = (enc.get("href") or enc.get("url", "")) if isinstance(enc, dict) \
               else (getattr(enc, "href", "") or getattr(enc, "url", ""))
        if mime.startswith("image/") and href:
            _add(href)

    for t in entry.get("media_thumbnail", []):
        url = t.get("url", "") if isinstance(t, dict) else getattr(t, "url", "")
        _add(url)

    if len(images) < MAX_IMAGES:
        raw_html = (
            entry.get("summary")
            or entry.get("description")
            or (entry.get("content") or [{}])[0].get("value", "")
        )
        if raw_html:
            try:
                soup = BeautifulSoup(raw_html, "lxml")
                for img_tag in soup.find_all("img"):
                    for attr in ("src", "data-src", "data-lazy-src"):
                        src = img_tag.get(attr, "")
                        if src and src.startswith("http"):
                            _add(src)
                            break
                    if len(images) >= MAX_IMAGES:
                        break
            except Exception:
                pass

    return images


def _fetch_og_image(url: str) -> str | None:
    try:
        resp = requests.get(
            url, timeout=3,
            headers={"User-Agent": "Mozilla/5.0"},
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "lxml")
        for prop in ("og:image", "twitter:image"):
            tag = (
                soup.find("meta", property=prop)
                or soup.find("meta", attrs={"name": prop})
            )
            if tag:
                content = tag.get("content", "").strip()
                if content.startswith("http"):
                    return content
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════
# SECTION 12 — CAPTION BUILDER + HASHTAGS
# ═══════════════════════════════════════════════════════════

def _generate_hashtags(title: str, desc: str,
                       topics: list[str] = None) -> list[str]:
    norm = _normalize_text(title + " " + desc)
    seen = set()
    tags = []

    # Add topic-based hashtags first
    if topics:
        topic_hashtags = {
            "صلاحیت":       "#صلاحیت",
            "ثبت‌نام":      "#ثبت_نام",
            "تبلیغات":      "#تبلیغات_انتخاباتی",
            "رای‌گیری":     "#رأی_گیری",
            "نتایج":        "#نتایج_انتخابات",
            "مجلس":         "#مجلس",
            "شورا":         "#شورای_شهر",
            "اقتصاد":       "#اقتصاد",
            "سیاست_خارجی":  "#سیاست_خارجی",
        }
        for topic in topics:
            ht = topic_hashtags.get(topic)
            if ht and ht not in seen:
                seen.add(ht)
                tags.append(ht)

    # Add keyword-based hashtags
    for keyword, hashtag in _HASHTAG_MAP:
        if len(tags) >= 5:
            break
        kw_norm = _normalize_text(keyword)
        if kw_norm and kw_norm in norm and hashtag not in seen:
            seen.add(hashtag)
            tags.append(hashtag)

    if "#انتخابات" not in seen:
        tags.insert(0, "#انتخابات")

    return tags[:6]


def _build_caption(title: str, desc: str, hashtags: list[str] = None,
                   candidates: list[str] = None,
                   source: str = "") -> str:
    safe_title   = _escape_html(title.strip())
    safe_desc    = _escape_html(desc.strip())
    hashtag_line = " ".join(hashtags) if hashtags else "#انتخابات"

    # Add candidate names as hashtags if present
    if candidates:
        for c in candidates[:3]:
            c_tag = f"#{_escape_html(c.replace(' ', '_'))}"
            if c_tag not in hashtag_line:
                hashtag_line += f" {c_tag}"

    source_line = f"📰 {_escape_html(source)}" if source else ""

    caption = (
        f"💠 <b>{safe_title}</b>\n\n"
        f"{hashtag_line}\n\n"
        f"@candidatoryiran\n\n"
        f"{safe_desc}\n\n"
    )

    if source_line:
        caption += f"{source_line}\n"

    caption += (
        f"🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷\n"
        f"کانال خبری کاندیداتوری\n"
        f"🆔 @candidatoryiran\n"
        f"🆔 Instagram.com/candidatory.ir"
    )

    if len(caption) > CAPTION_MAX:
        overflow  = len(caption) - CAPTION_MAX
        safe_desc = safe_desc[:max(0, len(safe_desc) - overflow - 5)] + "…"
        caption = (
            f"💠 <b>{safe_title}</b>\n\n"
            f"{hashtag_line}\n\n"
            f"@candidatoryiran\n\n"
            f"{safe_desc}\n\n"
        )
        if source_line:
            caption += f"{source_line}\n"
        caption += (
            f"🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷\n"
            f"کانال خبری کاندیداتوری\n"
            f"🆔 @candidatoryiran\n"
            f"🆔 Instagram.com/candidatory.ir"
        )

    return caption


# ═══════════════════════════════════════════════════════════
# SECTION 13 — TELEGRAM POSTING
# ═══════════════════════════════════════════════════════════

async def _post_to_telegram(
    bot: Bot, chat_id: str, image_urls: list[str], caption: str,
) -> bool:

    if len(image_urls) >= 2:
        try:
            media_group = []
            for i, url in enumerate(image_urls[:MAX_IMAGES]):
                if i == 0:
                    media_group.append(InputMediaPhoto(
                        media=url, caption=caption, parse_mode="HTML",
                    ))
                else:
                    media_group.append(InputMediaPhoto(media=url))

            sent = await bot.send_media_group(
                chat_id=chat_id,
                media=media_group,
                disable_notification=True,
            )
            return True
        except TelegramError as e:
            _log(f"Album failed: {e} — fallback", level="WARN")
            image_urls = image_urls[:1]

    if len(image_urls) == 1:
        try:
            await bot.send_photo(
                chat_id=chat_id,
                photo=image_urls[0],
                caption=caption,
                parse_mode="HTML",
                disable_notification=True,
            )
            return True
        except TelegramError as e:
            _log(f"Photo failed: {e} — fallback to text", level="WARN")

    try:
        await bot.send_message(
            chat_id=chat_id,
            text=caption,
            parse_mode="HTML",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            disable_notification=True,
        )
        return True
    except TelegramError as e:
        _log(f"Text send failed: {e}", level="ERROR")
        return False


# ═══════════════════════════════════════════════════════════
# SECTION 14 — LOGGING
# ═══════════════════════════════════════════════════════════

def _log(msg: str, level: str = "INFO"):
    print(f"[{level}] {msg}")


def _log_item(action: str, source: str, title: str,
              score: int, tier: str,
              candidates: list[str] = None,
              topics: list[str] = None):
    parts = [f"[{action}]", f"score={score}", f"tier={tier}",
             f"[{source}]", title[:50]]
    if candidates:
        parts.append(f"cand={','.join(candidates[:3])}")
    if topics:
        parts.append(f"topic={','.join(topics[:3])}")
    print(" ".join(parts))


# ═══════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    asyncio.run(main())
