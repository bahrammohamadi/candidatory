# ============================================================
# Telegram Election News Bot — @candidatoryiran
# Version:    4.0 — Precision Scoring + Batch + Retry + Resilience
# Runtime:    Python 3.12 / Appwrite Cloud Functions
# Timeout:    30 seconds (Appwrite free plan limit)
#
# v4.0 CHANGES:
#   ✓ Word-boundary matching (no more substring false positives)
#   ✓ Feed retry with exponential backoff (3 attempts)
#   ✓ Batch Telegram posting (up to 4 items per run)
#   ✓ DB timeout increased to 5s with local fallback
#   ✓ context.log() / context.error() for structured logging
#   ✓ Configurable batch size
#   ✓ Per-feed error isolation
#   ✓ Summary output with feed/tier/error breakdown
#
# ARCHITECTURE:
#   Phase 1: Fetch feeds in parallel with retry     (budget: 10s)
#   Phase 2: Load DB state once + local fallback    (budget: 5s)
#   Phase 3: Score with word-boundary matching      (instant)
#   Phase 4: Dedup (in-memory, instant)
#   Phase 5: Batch publish by priority              (remaining)
# ============================================================

import os
import re
import asyncio
import hashlib
import requests
import feedparser
import json

from datetime import datetime, timedelta, timezone
from time import monotonic, sleep
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

# ── Timing ──
GLOBAL_DEADLINE_SEC  = 27
FEED_FETCH_TIMEOUT   = 4
FEEDS_TOTAL_TIMEOUT  = 10
DB_TIMEOUT           = 5
IMAGE_SCRAPE_TIMEOUT = 3
TELEGRAM_TIMEOUT     = 5
INTER_POST_DELAY     = 1.0

# ── Retry ──
FEED_MAX_RETRIES     = 3
FEED_RETRY_BASE_SEC  = 0.5

# ── Batch + limits ──
PUBLISH_BATCH_SIZE   = 4
MAX_IMAGES           = 5
MAX_DESC_CHARS       = 500
CAPTION_MAX          = 1024
HOURS_THRESHOLD      = 24
FUZZY_THRESHOLD      = 0.55

# ── Score thresholds ──
SCORE_HIGH           = 6
SCORE_MEDIUM         = 3

# ── Image filters ──
IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.webp')
IMAGE_BLOCKLIST  = [
    'doubleclick', 'googletagmanager', 'analytics',
    'pixel', 'beacon', 'tracking', 'stat.', 'stats.',
]


# ═══════════════════════════════════════════════════════════
# SECTION 2 — KEYWORD SYSTEM (WORD-BOUNDARY MATCHING)
# ═══════════════════════════════════════════════════════════

# Layer 1: CORE election — must be whole-word match
# Each entry: (normalized_keyword, title_score, desc_score)
# These are pre-normalized at module load for speed.

_RAW_LAYER1 = [
    ("انتخابات", 4, 2),
    ("انتخاباتی", 4, 2),
    ("ریاست جمهوری", 4, 2),
    ("ریاستجمهوری", 4, 2),
    ("مجلس شورای اسلامی", 4, 2),
    ("نامزد انتخابات", 5, 3),
    ("نامزد انتخاباتی", 5, 3),
    ("کاندیدا", 4, 2),
    ("کاندیدای", 4, 2),
    ("داوطلب انتخابات", 5, 3),
    ("ثبتنام داوطلب", 5, 3),
    ("ثبتنام انتخابات", 5, 3),
    ("رد صلاحیت", 5, 3),
    ("تایید صلاحیت", 5, 3),
    ("احراز صلاحیت", 5, 3),
    ("بررسی صلاحیت", 4, 2),
    ("شورای نگهبان", 4, 2),
    ("هیئت نظارت", 4, 2),
    ("ستاد انتخابات", 4, 2),
    ("ستاد انتخاباتی", 4, 2),
    ("حوزه انتخابیه", 4, 2),
    ("تبلیغات انتخاباتی", 4, 2),
    ("صندوق رای", 4, 2),
    ("رایگیری", 4, 2),
    ("رای گیری", 4, 2),
    ("مشارکت انتخاباتی", 4, 2),
    ("دور دوم انتخابات", 5, 3),
    ("لیست انتخاباتی", 4, 2),
    ("ائتلاف انتخاباتی", 4, 2),
    ("شورای شهر", 3, 1),
    ("شورای اسلامی", 3, 1),
    ("شوراهای اسلامی", 3, 1),
    ("election", 4, 2),
    ("elections", 4, 2),
    ("electoral", 4, 2),
    ("candidate", 4, 2),
    ("ballot", 4, 2),
    ("voting", 4, 2),
    ("presidential", 4, 2),
    ("parliamentary", 4, 2),
    ("runoff", 4, 2),
    ("disqualification", 5, 3),
]

_RAW_LAYER2 = [
    ("نماینده مجلس", 2, 1),
    ("نمایندگان مجلس", 2, 1),
    ("فراکسیون", 2, 1),
    ("مناظره", 3, 1),
    ("مناظره انتخاباتی", 4, 2),
    ("وعده انتخاباتی", 4, 2),
    ("برنامه انتخاباتی", 4, 2),
    ("اگر انتخاب شوم", 5, 3),
    ("در صورت انتخاب", 5, 3),
    ("بیانیه انتخاباتی", 4, 2),
    ("اصلاحطلب", 2, 1),
    ("اصلاحطلبان", 2, 1),
    ("اصولگرا", 2, 1),
    ("اصولگرایان", 2, 1),
    ("حزب", 1, 0),
    ("جبهه", 1, 0),
    ("دولت آینده", 2, 1),
    ("مجلس آینده", 2, 1),
    ("رئیس جمهور", 2, 1),
    ("رئیسجمهور", 2, 1),
    ("نظرسنجی", 3, 2),
    ("نظرسنجی انتخاباتی", 4, 2),
    ("debate", 3, 1),
    ("polling", 3, 2),
    ("campaign", 2, 1),
    ("manifesto", 3, 2),
]

# ── Rejection: if ANY of these appear → score = -100 ──
_RAW_REJECTION = [
    "فیلم سینمایی", "سریال تلویزیونی", "بازیگر سینما",
    "لیگ برتر فوتبال", "جام جهانی فوتبال", "والیبال",
    "بیت کوین", "ارز دیجیتال", "رمزارز",
    "زلزله", "آتش سوزی", "تصادف رانندگی",
    "آشپزی", "رسپی غذا", "فال روز", "طالع بینی",
    "هواشناسی فردا",
]

# ── Candidate names ──
KNOWN_CANDIDATES = [
    "پزشکیان", "جلیلی", "قالیباف", "زاکانی",
    "لاریجانی", "روحانی", "احمدینژاد",
    "رضایی", "همتی", "مهرعلیزاده",
    "جهانگیری", "واعظی", "ظریف",
    "میرسلیم", "پورمحمدی",
]

# ── Topic patterns (for hashtag extraction only, NOT scoring) ──
TOPIC_PATTERNS: dict[str, list[str]] = {
    "صلاحیت":    ["رد صلاحیت", "تایید صلاحیت", "احراز صلاحیت",
                   "بررسی صلاحیت", "شورای نگهبان"],
    "ثبت‌نام":   ["ثبتنام", "داوطلب", "نامزد انتخابات"],
    "تبلیغات":   ["تبلیغات انتخاباتی", "ستاد انتخاباتی", "مناظره"],
    "رای‌گیری":  ["رایگیری", "رای گیری", "صندوق رای", "مشارکت انتخاباتی"],
    "نتایج":     ["نتایج انتخابات", "شمارش آرا", "پیروز انتخابات"],
    "مجلس":      ["مجلس شورای اسلامی", "نماینده مجلس", "نمایندگان مجلس",
                   "فراکسیون"],
    "شورا":      ["شورای شهر", "شورای اسلامی", "شوراهای اسلامی"],
}

# ── Pre-normalize all keywords at import time ──
def _pre_normalize(raw_text: str) -> str:
    """Normalize for matching — same as _normalize_text but no stopword removal."""
    t = raw_text
    t = t.replace("ي", "ی").replace("ك", "ک")
    t = t.replace("ة", "ه").replace("ؤ", "و")
    t = t.replace("إ", "ا").replace("أ", "ا")
    t = t.replace("ئ", "ی").replace("ى", "ی")
    t = re.sub(r"[\u064B-\u065F\u0670]", "", t)
    t = re.sub(r"[\u200c\u200d\u200e\u200f\ufeff]", "", t)
    t = t.lower()
    t = re.sub(r"[^\w\s\u0600-\u06FF]", " ", t)
    return " ".join(t.split())

LAYER1_COMPILED: list[tuple[re.Pattern, int, int]] = []
for kw, ts, ds in _RAW_LAYER1:
    nkw = _pre_normalize(kw)
    if nkw:
        pattern = re.compile(r'(?:^|\s)' + re.escape(nkw) + r'(?:\s|$)')
        LAYER1_COMPILED.append((pattern, ts, ds))

LAYER2_COMPILED: list[tuple[re.Pattern, int, int]] = []
for kw, ts, ds in _RAW_LAYER2:
    nkw = _pre_normalize(kw)
    if nkw:
        pattern = re.compile(r'(?:^|\s)' + re.escape(nkw) + r'(?:\s|$)')
        LAYER2_COMPILED.append((pattern, ts, ds))

REJECTION_COMPILED: list[re.Pattern] = []
for kw in _RAW_REJECTION:
    nkw = _pre_normalize(kw)
    if nkw:
        REJECTION_COMPILED.append(
            re.compile(r'(?:^|\s)' + re.escape(nkw) + r'(?:\s|$)')
        )

CANDIDATE_PATTERNS: list[tuple[re.Pattern, str]] = []
for name in KNOWN_CANDIDATES:
    nn = _pre_normalize(name)
    if nn:
        CANDIDATE_PATTERNS.append((
            re.compile(r'(?:^|\s)' + re.escape(nn) + r'(?:\s|$)'),
            name,
        ))

TOPIC_COMPILED: dict[str, list[re.Pattern]] = {}
for topic, patterns in TOPIC_PATTERNS.items():
    compiled = []
    for pat in patterns:
        np = _pre_normalize(pat)
        if np:
            compiled.append(
                re.compile(r'(?:^|\s)' + re.escape(np) + r'(?:\s|$)')
            )
    if compiled:
        TOPIC_COMPILED[topic] = compiled

PERSIAN_STOPWORDS = {
    "و", "در", "به", "از", "که", "این", "را", "با", "های",
    "برای", "آن", "یک", "هم", "تا", "اما", "یا", "بود",
    "شد", "است", "می", "هر", "اگر", "بر", "ها", "نیز",
    "کرد", "خود", "هیچ", "پس", "باید", "نه", "ما", "شود",
    "the", "a", "an", "is", "are", "was", "of", "in",
    "to", "for", "and", "or", "but", "with", "on",
}

_HASHTAG_MAP = [
    ("انتخابات",       "#انتخابات"),
    ("ریاست",          "#ریاست_جمهوری"),
    ("مجلس",           "#مجلس"),
    ("شورا",           "#شورای_شهر"),
    ("کاندیدا",        "#کاندیدا"),
    ("نامزد",          "#نامزد_انتخاباتی"),
    ("ثبتنام",         "#ثبت_نام"),
    ("صلاحیت",         "#صلاحیت"),
    ("شورای نگهبان",   "#شورای_نگهبان"),
    ("مناظره",         "#مناظره"),
    ("مشارکت",         "#مشارکت"),
    ("نمایندگان",      "#نمایندگان"),
    ("اصلاحطلب",       "#اصلاح_طلبان"),
    ("اصولگرا",        "#اصولگرایان"),
    ("election",       "#Election"),
    ("candidate",      "#Candidate"),
    ("parliament",     "#Parliament"),
    ("presidential",   "#Presidential"),
]


# ═══════════════════════════════════════════════════════════
# SECTION 3 — LOGGER (context-aware)
# ═══════════════════════════════════════════════════════════

class _Logger:
    """Uses context.log/error when available, falls back to print."""

    def __init__(self, context=None):
        self._ctx = context

    def info(self, msg: str):
        if self._ctx and hasattr(self._ctx, 'log'):
            self._ctx.log(f"[INFO] {msg}")
        else:
            print(f"[INFO] {msg}")

    def warn(self, msg: str):
        if self._ctx and hasattr(self._ctx, 'log'):
            self._ctx.log(f"[WARN] {msg}")
        else:
            print(f"[WARN] {msg}")

    def error(self, msg: str):
        if self._ctx and hasattr(self._ctx, 'error'):
            self._ctx.error(f"[ERROR] {msg}")
        else:
            print(f"[ERROR] {msg}")

    def item(self, action: str, source: str, title: str,
             score: int, tier: str,
             candidates: list[str] = None,
             topics: list[str] = None):
        parts = [f"[{action}]", f"s={score}", f"t={tier}",
                 f"[{source}]", title[:55]]
        if candidates:
            parts.append(f"c={','.join(candidates[:2])}")
        if topics:
            parts.append(f"tp={','.join(topics[:2])}")
        self.info(" ".join(parts))


# Global logger — set in main()
log = _Logger()


# ═══════════════════════════════════════════════════════════
# SECTION 4 — MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════

async def main(event=None, context=None):
    global log
    log = _Logger(context)
    _t0 = monotonic()

    def _remaining() -> float:
        return GLOBAL_DEADLINE_SEC - (monotonic() - _t0)

    log.info("══════════════════════════════")
    log.info("Election Bot v4.0 started")
    log.info(f"{datetime.now(timezone.utc).isoformat()}")
    log.info("══════════════════════════════")

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
    loop           = asyncio.get_event_loop()

    stats = {
        "feeds_ok": 0, "feeds_fail": 0, "feeds_retry": 0,
        "entries_total": 0, "skip_time": 0, "skip_topic": 0,
        "skip_dupe": 0,
        "queued_high": 0, "queued_medium": 0, "queued_low": 0,
        "posted": 0, "errors": 0,
        "db_timeout": False,
    }

    # ════════════════════════════════════════════════════
    # Phase 1: Fetch ALL feeds in parallel WITH retry
    # ════════════════════════════════════════════════════
    budget = min(FEEDS_TOTAL_TIMEOUT, _remaining() - 16)
    if budget < 3:
        log.warn("Not enough time for feed fetch.")
        return _build_response(stats)

    log.info(f"Fetching {len(RSS_SOURCES)} feeds (budget={budget:.1f}s)...")
    feed_results: list[dict] = []
    try:
        feed_results = await asyncio.wait_for(
            _fetch_all_feeds_with_retry(loop, stats),
            timeout=budget,
        )
    except asyncio.TimeoutError:
        log.warn(f"Feed fetch timed out after {budget:.1f}s — using partial")

    stats["entries_total"] = len(feed_results)
    log.info(f"Collected: {len(feed_results)} entries from "
             f"{stats['feeds_ok']}/{len(RSS_SOURCES)} feeds "
             f"({stats['feeds_fail']} failed, {stats['feeds_retry']} retries) "
             f"({_remaining():.1f}s left)")

    if not feed_results:
        return _build_response(stats)

    feed_results.sort(
        key=lambda x: x["pub_date"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    # ════════════════════════════════════════════════════
    # Phase 2: Load DB state ONCE (with fallback)
    # ════════════════════════════════════════════════════
    known_links:   set[str]   = set()
    known_hashes:  set[str]   = set()
    fuzzy_records: list[dict] = []

    db_budget = min(DB_TIMEOUT, _remaining() - 12)
    if db_budget > 2:
        try:
            raw = await asyncio.wait_for(
                loop.run_in_executor(None, db.load_recent, 500),
                timeout=db_budget,
            )
            for rec in raw:
                if rec.get("link"):
                    known_links.add(rec["link"])
                if rec.get("content_hash"):
                    known_hashes.add(rec["content_hash"])
                fuzzy_records.append({
                    "title":      rec.get("title", ""),
                    "title_norm": _normalize_text(rec.get("title", "")),
                })
            log.info(f"DB loaded: {len(raw)} records → "
                     f"{len(known_links)} links, {len(known_hashes)} hashes "
                     f"({_remaining():.1f}s left)")
        except asyncio.TimeoutError:
            stats["db_timeout"] = True
            log.warn("DB load timed out — using local-only dedup")
        except Exception as e:
            stats["db_timeout"] = True
            log.error(f"DB load failed: {e}")
    else:
        log.warn("Insufficient budget for DB load")

    posted_hashes: set[str] = set()

    # ════════════════════════════════════════════════════
    # Phase 3+4: Score + Dedup + Triage
    # ════════════════════════════════════════════════════
    publish_queue: list[dict] = []

    for item in feed_results:
        title    = item["title"]
        link     = item["link"]
        desc     = item["desc"]
        source   = item["source"]
        pub_date = item["pub_date"]

        # ── Time filter ──
        if pub_date and pub_date < time_threshold:
            stats["skip_time"] += 1
            continue

        # ── Dual-layer scoring (word-boundary) ──
        result = _score_article(title, desc)
        score      = result["score"]
        tier       = result["tier"]
        candidates = result["candidates"]
        topics     = result["topics"]

        if tier == "LOW":
            stats["skip_topic"] += 1
            stats["queued_low"] += 1
            continue

        # ── Dedup (all in-memory) ──
        content_hash = _make_hash(title)

        if content_hash in posted_hashes or content_hash in known_hashes:
            stats["skip_dupe"] += 1
            continue

        if link in known_links:
            stats["skip_dupe"] += 1
            continue

        if _is_fuzzy_duplicate(title, fuzzy_records):
            stats["skip_dupe"] += 1
            continue

        # ── Mark seen immediately (prevent same-run dupes) ──
        posted_hashes.add(content_hash)
        fuzzy_records.append({
            "title":      title,
            "title_norm": _normalize_text(title),
        })

        enriched = {
            **item,
            "content_hash": content_hash,
            "score":        score,
            "tier":         tier,
            "candidates":   candidates,
            "topics":       topics,
        }

        if tier == "HIGH":
            stats["queued_high"] += 1
        else:
            stats["queued_medium"] += 1

        publish_queue.append(enriched)
        log.item("QUEUED", source, title, score, tier,
                 candidates=candidates, topics=topics)

    # ── Sort by score descending ──
    publish_queue.sort(key=lambda x: x["score"], reverse=True)

    log.info(f"Queue: {len(publish_queue)} items "
             f"(H={stats['queued_high']} M={stats['queued_medium']}) "
             f"({_remaining():.1f}s left)")

    # ════════════════════════════════════════════════════
    # Phase 5: Batch publish (priority order)
    # ════════════════════════════════════════════════════
    batch_count = 0

    for item in publish_queue:
        if batch_count >= PUBLISH_BATCH_SIZE:
            log.info(f"Batch limit ({PUBLISH_BATCH_SIZE}) reached.")
            break

        if _remaining() < 8:
            log.info(f"Time budget low ({_remaining():.1f}s). Stopping publish.")
            break

        success = await _publish_item(
            item, bot, config["chat_id"], db, loop, now, _remaining,
        )

        if success:
            stats["posted"] += 1
            batch_count += 1
            known_hashes.add(item["content_hash"])
            known_links.add(item["link"])

            if _remaining() > 4 and batch_count < PUBLISH_BATCH_SIZE:
                await asyncio.sleep(INTER_POST_DELAY)
        else:
            stats["errors"] += 1

    # ── Summary ──
    elapsed = monotonic() - _t0
    log.info("─────── SUMMARY ───────")
    log.info(f"Time: {elapsed:.1f}s / {GLOBAL_DEADLINE_SEC}s")
    log.info(f"Feeds: {stats['feeds_ok']} ok, {stats['feeds_fail']} failed, "
             f"{stats['feeds_retry']} retries")
    log.info(f"Entries: {stats['entries_total']} total")
    log.info(f"Skipped: {stats['skip_time']} time, {stats['skip_topic']} topic, "
             f"{stats['skip_dupe']} dupe")
    log.info(f"Queued: H={stats['queued_high']} M={stats['queued_medium']} "
             f"L={stats['queued_low']}")
    log.info(f"Posted: {stats['posted']} | Errors: {stats['errors']}")
    if stats["db_timeout"]:
        log.warn("DB timeout occurred — some dedup may be incomplete")
    log.info("───────────────────────")

    return _build_response(stats)


def _build_response(stats: dict) -> dict:
    return {
        "status":         "success",
        "posted":         stats.get("posted", 0),
        "feeds_ok":       stats.get("feeds_ok", 0),
        "feeds_failed":   stats.get("feeds_fail", 0),
        "entries_total":  stats.get("entries_total", 0),
        "queued_high":    stats.get("queued_high", 0),
        "queued_medium":  stats.get("queued_medium", 0),
        "queued_low":     stats.get("queued_low", 0),
        "skipped_dupe":   stats.get("skip_dupe", 0),
        "skipped_topic":  stats.get("skip_topic", 0),
        "errors":         stats.get("errors", 0),
        "db_timeout":     stats.get("db_timeout", False),
    }


# ═══════════════════════════════════════════════════════════
# SECTION 5 — FEED FETCHER WITH RETRY
# ═══════════════════════════════════════════════════════════

async def _fetch_all_feeds_with_retry(
    loop: asyncio.AbstractEventLoop,
    stats: dict,
) -> list[dict]:
    """Fetch all feeds in parallel. Each feed retries up to 3 times."""
    tasks = [
        loop.run_in_executor(None, _fetch_one_feed_with_retry, url, name, stats)
        for url, name in RSS_SOURCES
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_entries: list[dict] = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            log.error(f"{RSS_SOURCES[i][1]}: Unhandled: {result}")
            stats["feeds_fail"] += 1
            continue
        if result is None:
            stats["feeds_fail"] += 1
            continue
        if result:
            all_entries.extend(result)
            stats["feeds_ok"] += 1
        else:
            stats["feeds_fail"] += 1

    return all_entries


def _fetch_one_feed_with_retry(
    url: str, source_name: str, stats: dict,
) -> list[dict] | None:
    """Blocking. Retries up to FEED_MAX_RETRIES with exponential backoff."""
    last_error = None

    for attempt in range(FEED_MAX_RETRIES):
        try:
            resp = requests.get(
                url,
                timeout=FEED_FETCH_TIMEOUT,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; ElectionBot/4.0)",
                    "Accept":     "application/rss+xml, application/xml, */*",
                },
            )

            if resp.status_code == 404:
                log.warn(f"{source_name}: HTTP 404 (not found)")
                return []

            if resp.status_code != 200:
                last_error = f"HTTP {resp.status_code}"
                if attempt < FEED_MAX_RETRIES - 1:
                    stats["feeds_retry"] += 1
                    sleep(FEED_RETRY_BASE_SEC * (2 ** attempt))
                    continue
                log.warn(f"{source_name}: {last_error} after {attempt+1} attempts")
                return []

            feed = feedparser.parse(resp.content)
            if feed.bozo and not feed.entries:
                log.warn(f"{source_name}: Malformed feed")
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
                desc     = _truncate(_strip_html(raw_html), MAX_DESC_CHARS)
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

            log.info(f"[FEED] {source_name}: {len(entries)} entries"
                     + (f" (attempt {attempt+1})" if attempt > 0 else ""))
            return entries

        except requests.exceptions.ConnectionError as e:
            last_error = str(e)[:100]
            if attempt < FEED_MAX_RETRIES - 1:
                stats["feeds_retry"] += 1
                sleep(FEED_RETRY_BASE_SEC * (2 ** attempt))
                continue

        except requests.exceptions.Timeout:
            last_error = "timeout"
            if attempt < FEED_MAX_RETRIES - 1:
                stats["feeds_retry"] += 1
                sleep(FEED_RETRY_BASE_SEC * (2 ** attempt))
                continue

        except Exception as e:
            last_error = str(e)[:100]
            break

    log.error(f"{source_name}: Failed after {FEED_MAX_RETRIES} attempts: {last_error}")
    return None


# ═══════════════════════════════════════════════════════════
# SECTION 6 — SCORING ENGINE (WORD-BOUNDARY)
# ═══════════════════════════════════════════════════════════

def _score_article(title: str, desc: str) -> dict:
    """
    Dual-layer scoring with word-boundary regex matching.
    No substring false positives.
    """
    norm_title = _pre_normalize(title)
    norm_desc  = _pre_normalize(desc)
    # Pad with spaces for boundary matching
    padded_title = f" {norm_title} "
    padded_desc  = f" {norm_desc} "
    padded_both  = f" {norm_title} {norm_desc} "

    # ── Rejection check ──
    for pattern in REJECTION_COMPILED:
        if pattern.search(padded_both):
            return {"score": -1, "tier": "LOW",
                    "candidates": [], "topics": []}

    score = 0

    # ── Layer 1: Core election ──
    for pattern, title_pts, desc_pts in LAYER1_COMPILED:
        if pattern.search(padded_title):
            score += title_pts
        elif pattern.search(padded_desc):
            score += desc_pts

    # ── Layer 2: Contextual ──
    for pattern, title_pts, desc_pts in LAYER2_COMPILED:
        if pattern.search(padded_title):
            score += title_pts
        elif pattern.search(padded_desc):
            score += desc_pts

    # ── Candidate detection ──
    candidates_found: list[str] = []
    for pattern, name in CANDIDATE_PATTERNS:
        if pattern.search(padded_both):
            candidates_found.append(name)
            if pattern.search(padded_title):
                score += 2
            else:
                score += 1

    # ── Topic extraction (no scoring, just tagging) ──
    topics_found: list[str] = []
    for topic, patterns in TOPIC_COMPILED.items():
        for pat in patterns:
            if pat.search(padded_both):
                if topic not in topics_found:
                    topics_found.append(topic)
                break

    # ── Multi-signal boost ──
    if candidates_found and score >= SCORE_MEDIUM:
        score += 2
    if len(topics_found) >= 2 and score >= SCORE_MEDIUM:
        score += 1

    # ── Tier ──
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
# SECTION 7 — PUBLISH SINGLE ITEM
# ═══════════════════════════════════════════════════════════

async def _publish_item(
    item: dict, bot: Bot, chat_id: str,
    db: '_AppwriteDB', loop: asyncio.AbstractEventLoop,
    now: datetime, _remaining,
) -> bool:
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
    save_budget = min(DB_TIMEOUT, _remaining() - 6)
    if save_budget < 1:
        log.warn("No time for DB save")
        return False

    pub_iso = pub_date.isoformat() if pub_date else now.isoformat()

    try:
        saved = await asyncio.wait_for(
            loop.run_in_executor(
                None, db.save,
                link, title, content_hash, source,
                feed_url, pub_iso, now.isoformat(),
            ),
            timeout=save_budget,
        )
    except asyncio.TimeoutError:
        log.warn(f"DB save timed out for [{source}] {title[:40]}")
        return False

    if not saved:
        log.info(f"DB save rejected (409/error) [{source}] {title[:40]}")
        return False

    # ── Collect images ──
    image_urls: list[str] = []
    img_budget = min(IMAGE_SCRAPE_TIMEOUT, _remaining() - 5)
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
        log.warn("No time for Telegram post")
        return False

    try:
        success = await asyncio.wait_for(
            _post_to_telegram(bot, chat_id, image_urls, caption),
            timeout=tg_budget,
        )
    except asyncio.TimeoutError:
        log.warn("Telegram post timed out")
        success = False

    if success:
        log.item("POSTED", source, title, score, tier,
                 candidates=candidates, topics=topics)

    return success


# ═══════════════════════════════════════════════════════════
# SECTION 8 — APPWRITE DB
# ═══════════════════════════════════════════════════════════

class _AppwriteDB:
    """
    Schema (collection "history"):
      link           string  700  required, indexed
      title          string  300
      site           string  100
      published_at   datetime
      created_at     datetime
      feed_url       string  500
      content_hash   string  128  indexed
    """

    def __init__(self, endpoint, project, key, database_id, collection_id):
        self._url = (
            f"{endpoint}/databases/{database_id}"
            f"/collections/{collection_id}/documents"
        )
        self._headers = {
            "Content-Type":       "application/json",
            "X-Appwrite-Project": project,
            "X-Appwrite-Key":     key,
        }

    def load_recent(self, limit: int = 500) -> list[dict]:
        try:
            resp = requests.get(
                self._url,
                headers=self._headers,
                params={"limit": str(limit), "orderType": "DESC"},
                timeout=DB_TIMEOUT,
            )
            if resp.status_code != 200:
                log.warn(f"DB load_recent: HTTP {resp.status_code}")
                return []
            docs = resp.json().get("documents", [])
            return [
                {
                    "link":         d.get("link", ""),
                    "title":        d.get("title", ""),
                    "content_hash": d.get("content_hash", ""),
                    "created_at":   d.get("created_at", ""),
                }
                for d in docs
            ]
        except requests.exceptions.Timeout:
            raise
        except Exception as e:
            log.error(f"DB load_recent: {e}")
            return []

    def save(self, link: str, title: str, content_hash: str,
             site: str, feed_url: str, published_at: str,
             created_at: str) -> bool:
        doc_id = content_hash[:36]
        try:
            resp = requests.post(
                self._url,
                headers=self._headers,
                json={
                    "documentId": doc_id,
                    "data": {
                        "link":         link[:700],
                        "title":        title[:300],
                        "content_hash": content_hash[:128],
                        "site":         site[:100],
                        "feed_url":     feed_url[:500],
                        "published_at": published_at,
                        "created_at":   created_at,
                    },
                },
                timeout=DB_TIMEOUT,
            )
            if resp.status_code in (200, 201):
                return True
            if resp.status_code == 409:
                log.info("DB 409 — already exists")
                return False
            log.warn(f"DB save {resp.status_code}: {resp.text[:150]}")
            return False
        except Exception as e:
            log.warn(f"DB save: {e}")
            return False


# ═══════════════════════════════════════════════════════════
# SECTION 9 — CONFIG
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
        log.error(f"Missing env vars: {missing}")
        return None
    return cfg


# ═══════════════════════════════════════════════════════════
# SECTION 10 — TEXT UTILITIES
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
    t = _pre_normalize(text)
    tokens = [
        tok for tok in t.split()
        if tok not in PERSIAN_STOPWORDS and len(tok) >= 2
    ]
    return " ".join(tokens)


def _make_hash(title: str, desc: str = "") -> str:
    norm = _normalize_text(title)
    tokens = sorted(norm.split())
    canonical = " ".join(tokens)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ═══════════════════════════════════════════════════════════
# SECTION 11 — FUZZY DEDUP
# ═══════════════════════════════════════════════════════════

def _is_fuzzy_duplicate(title: str, records: list[dict]) -> bool:
    if not records:
        return False
    incoming = set(_normalize_text(title).split())
    if len(incoming) < 2:
        return False
    for rec in records:
        stored = set(rec.get("title_norm", "").split())
        if len(stored) < 2:
            continue
        inter = len(incoming & stored)
        if inter == 0:
            continue
        min_sz  = min(len(incoming), len(stored))
        overlap = inter / min_sz
        union   = len(incoming | stored)
        jaccard = inter / union
        if overlap >= 0.75 or jaccard >= FUZZY_THRESHOLD:
            return True
    return False


# ═══════════════════════════════════════════════════════════
# SECTION 12 — IMAGE COLLECTION
# ═══════════════════════════════════════════════════════════

async def _collect_images_async(
    entry, url: str, loop: asyncio.AbstractEventLoop,
) -> list[str]:
    images = _extract_rss_images(entry)
    if not images:
        try:
            og = await asyncio.wait_for(
                loop.run_in_executor(None, _fetch_og_image, url),
                timeout=2.5,
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
        base    = lower.split("?")[0]
        has_ext = any(base.endswith(e) for e in IMAGE_EXTENSIONS)
        has_kw  = any(w in lower for w in
                      ["image", "photo", "img", "media", "cdn", "upload"])
        if not has_ext and not has_kw:
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
                for img in soup.find_all("img"):
                    for attr in ("src", "data-src", "data-lazy-src"):
                        src = img.get(attr, "")
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
            url, timeout=2.5,
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
# SECTION 13 — CAPTION + HASHTAGS
# ═══════════════════════════════════════════════════════════

def _generate_hashtags(title: str, desc: str,
                       topics: list[str] = None) -> list[str]:
    norm = _pre_normalize(title + " " + desc)
    padded = f" {norm} "
    seen = set()
    tags = []

    if topics:
        topic_ht = {
            "صلاحیت":       "#صلاحیت",
            "ثبت‌نام":      "#ثبت_نام",
            "تبلیغات":      "#تبلیغات_انتخاباتی",
            "رای‌گیری":     "#رأی_گیری",
            "نتایج":        "#نتایج_انتخابات",
            "مجلس":         "#مجلس",
            "شورا":         "#شورای_شهر",
        }
        for t in topics:
            ht = topic_ht.get(t)
            if ht and ht not in seen:
                seen.add(ht)
                tags.append(ht)

    for keyword, hashtag in _HASHTAG_MAP:
        if len(tags) >= 5:
            break
        kw_n = _pre_normalize(keyword)
        if kw_n and kw_n in norm and hashtag not in seen:
            seen.add(hashtag)
            tags.append(hashtag)

    if "#انتخابات" not in seen:
        tags.insert(0, "#انتخابات")

    return tags[:6]


def _build_caption(title: str, desc: str,
                   hashtags: list[str] = None,
                   candidates: list[str] = None,
                   source: str = "") -> str:
    safe_title   = _escape_html(title.strip())
    safe_desc    = _escape_html(desc.strip())
    hashtag_line = " ".join(hashtags) if hashtags else "#انتخابات"

    if candidates:
        for c in candidates[:2]:
            c_tag = f"#{_escape_html(c.replace(' ', '_'))}"
            if c_tag not in hashtag_line:
                hashtag_line += f" {c_tag}"

    source_line = f"📰 {_escape_html(source)}\n" if source else ""

    caption = (
        f"💠 <b>{safe_title}</b>\n\n"
        f"{hashtag_line}\n\n"
        f"@candidatoryiran\n\n"
        f"{safe_desc}\n\n"
        f"{source_line}"
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
            f"{source_line}"
            f"🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷\n"
            f"کانال خبری کاندیداتوری\n"
            f"🆔 @candidatoryiran\n"
            f"🆔 Instagram.com/candidatory.ir"
        )

    return caption


# ═══════════════════════════════════════════════════════════
# SECTION 14 — TELEGRAM POSTING
# ═══════════════════════════════════════════════════════════

async def _post_to_telegram(
    bot: Bot, chat_id: str, image_urls: list[str], caption: str,
) -> bool:

    if len(image_urls) >= 2:
        try:
            media = []
            for i, url in enumerate(image_urls[:MAX_IMAGES]):
                if i == 0:
                    media.append(InputMediaPhoto(
                        media=url, caption=caption, parse_mode="HTML",
                    ))
                else:
                    media.append(InputMediaPhoto(media=url))
            await bot.send_media_group(
                chat_id=chat_id, media=media,
                disable_notification=True,
            )
            return True
        except TelegramError as e:
            log.warn(f"Album failed: {e} — fallback")
            image_urls = image_urls[:1]

    if len(image_urls) == 1:
        try:
            await bot.send_photo(
                chat_id=chat_id, photo=image_urls[0],
                caption=caption, parse_mode="HTML",
                disable_notification=True,
            )
            return True
        except TelegramError as e:
            log.warn(f"Photo failed: {e} — text fallback")

    try:
        await bot.send_message(
            chat_id=chat_id, text=caption,
            parse_mode="HTML",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            disable_notification=True,
        )
        return True
    except TelegramError as e:
        log.error(f"Text send failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    asyncio.run(main())
