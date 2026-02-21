# ============================================================
# Telegram + Bale Election News Bot — @candidatoryiran
# Version:    6.0 — Dual Platform (Telegram + Bale)
# Runtime:    Python 3.12 / Appwrite Cloud Functions
# Timeout:    30 seconds (Appwrite free plan limit)
#
# PLATFORMS:
#   Telegram: python-telegram-bot library (existing)
#   Bale:     Raw HTTP to tapi.bale.ai (Telegram-compatible API)
#
# CHANGES v6.0:
#   ✓ Bale channel publishing (text + images)
#   ✓ BaleClient class using raw HTTP (tapi.bale.ai)
#   ✓ Parallel posting: Telegram + Bale simultaneously
#   ✓ Independent error handling per platform
#   ✓ All tokens/IDs from environment variables
#   ✓ All existing features preserved
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
from telegram.error import TelegramError, RetryAfter


# ═══════════════════════════════════════════════════════════
# SECTION 1 — CONFIGURATION
# ═══════════════════════════════════════════════════════════

def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, str(default)))
    except ValueError:
        return default

def _env_float(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, str(default)))
    except ValueError:
        return default

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
GLOBAL_DEADLINE_SEC   = _env_int("BOT_DEADLINE_SEC", 27)
FEED_FETCH_TIMEOUT    = _env_int("FEED_FETCH_TIMEOUT", 4)
FEEDS_TOTAL_TIMEOUT   = _env_int("FEEDS_TOTAL_TIMEOUT", 10)
DB_TIMEOUT            = _env_int("DB_TIMEOUT", 5)
IMAGE_SCRAPE_TIMEOUT  = _env_int("IMAGE_SCRAPE_TIMEOUT", 3)
TELEGRAM_TIMEOUT      = _env_int("TELEGRAM_TIMEOUT", 5)
BALE_TIMEOUT          = _env_int("BALE_TIMEOUT", 5)
INTER_POST_DELAY      = _env_float("INTER_POST_DELAY", 1.0)

# ── Retry ──
FEED_MAX_RETRIES      = _env_int("FEED_MAX_RETRIES", 3)
FEED_RETRY_BASE_SEC   = _env_float("FEED_RETRY_BASE", 0.3)
TG_POST_MAX_RETRIES   = _env_int("TG_POST_RETRIES", 2)

# ── Batch + limits ──
PUBLISH_BATCH_SIZE    = _env_int("PUBLISH_BATCH_SIZE", 4)
MAX_IMAGES            = _env_int("MAX_IMAGES", 5)
MAX_DESC_CHARS        = _env_int("MAX_DESC_CHARS", 500)
CAPTION_MAX           = _env_int("CAPTION_MAX", 1024)
HOURS_THRESHOLD       = _env_int("HOURS_THRESHOLD", 24)
FUZZY_THRESHOLD       = _env_float("FUZZY_THRESHOLD", 0.55)

# ── Rate limiting ──
RATE_LIMIT_PER_MINUTE = _env_int("RATE_LIMIT_PER_MIN", 8)

# ── Score thresholds ──
SCORE_HIGH            = _env_int("SCORE_HIGH", 6)
SCORE_MEDIUM          = _env_int("SCORE_MEDIUM", 3)

# ── Image filters ──
IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.webp')
IMAGE_BLOCKLIST  = [
    'doubleclick', 'googletagmanager', 'analytics',
    'pixel', 'beacon', 'tracking', 'stat.', 'stats.',
]

PERSIAN_STOPWORDS = {
    "و", "در", "به", "از", "که", "این", "را", "با", "های",
    "برای", "آن", "یک", "هم", "تا", "اما", "یا", "بود",
    "شد", "است", "می", "هر", "اگر", "بر", "ها", "نیز",
    "کرد", "خود", "هیچ", "پس", "باید", "نه", "ما", "شود",
    "the", "a", "an", "is", "are", "was", "of", "in",
    "to", "for", "and", "or", "but", "with", "on",
}


# ═══════════════════════════════════════════════════════════
# SECTION 2 — KEYWORD SYSTEM
# ═══════════════════════════════════════════════════════════

def _pre_normalize(text: str) -> str:
    if not text:
        return ""
    t = text
    t = t.replace("ي", "ی").replace("ك", "ک")
    t = t.replace("ة", "ه").replace("ؤ", "و")
    t = t.replace("إ", "ا").replace("أ", "ا")
    t = t.replace("ئ", "ی").replace("ى", "ی")
    t = re.sub(r"[\u064B-\u065F\u0670]", "", t)
    t = re.sub(r"[\u200c\u200d\u200e\u200f\ufeff]", "", t)
    t = t.lower()
    t = re.sub(r"[^\w\s\u0600-\u06FF]", " ", t)
    return " ".join(t.split())


def _compile_wb(keyword: str) -> re.Pattern | None:
    nk = _pre_normalize(keyword)
    if not nk:
        return None
    return re.compile(r'(?:^|\s)' + re.escape(nk) + r'(?:\s|$)')


_RAW_LAYER1 = [
    ("انتخابات", 4, 2), ("انتخاباتی", 4, 2),
    ("ریاست جمهوری", 4, 2), ("ریاستجمهوری", 4, 2),
    ("مجلس شورای اسلامی", 4, 2),
    ("نامزد انتخابات", 5, 3), ("نامزد انتخاباتی", 5, 3),
    ("کاندیدا", 4, 2), ("کاندیدای", 4, 2),
    ("داوطلب انتخابات", 5, 3),
    ("ثبتنام داوطلب", 5, 3), ("ثبتنام انتخابات", 5, 3),
    ("رد صلاحیت", 5, 3), ("تایید صلاحیت", 5, 3),
    ("احراز صلاحیت", 5, 3), ("بررسی صلاحیت", 4, 2),
    ("شورای نگهبان", 4, 2), ("هیئت نظارت", 4, 2),
    ("ستاد انتخابات", 4, 2), ("ستاد انتخاباتی", 4, 2),
    ("حوزه انتخابیه", 4, 2), ("تبلیغات انتخاباتی", 4, 2),
    ("صندوق رای", 4, 2), ("رایگیری", 4, 2), ("رای گیری", 4, 2),
    ("مشارکت انتخاباتی", 4, 2), ("دور دوم انتخابات", 5, 3),
    ("لیست انتخاباتی", 4, 2), ("ائتلاف انتخاباتی", 4, 2),
    ("شورای شهر", 3, 1), ("شورای اسلامی", 3, 1),
    ("شوراهای اسلامی", 3, 1),
    ("election", 4, 2), ("elections", 4, 2), ("electoral", 4, 2),
    ("candidate", 4, 2), ("ballot", 4, 2), ("voting", 4, 2),
    ("presidential", 4, 2), ("parliamentary", 4, 2),
    ("runoff", 4, 2), ("disqualification", 5, 3),
]

_RAW_LAYER2 = [
    ("نماینده مجلس", 2, 1), ("نمایندگان مجلس", 2, 1),
    ("فراکسیون", 2, 1), ("مناظره", 3, 1),
    ("مناظره انتخاباتی", 4, 2), ("وعده انتخاباتی", 4, 2),
    ("برنامه انتخاباتی", 4, 2), ("اگر انتخاب شوم", 5, 3),
    ("در صورت انتخاب", 5, 3), ("بیانیه انتخاباتی", 4, 2),
    ("اصلاحطلب", 2, 1), ("اصلاحطلبان", 2, 1),
    ("اصولگرا", 2, 1), ("اصولگرایان", 2, 1),
    ("حزب", 1, 0), ("جبهه", 1, 0),
    ("دولت آینده", 2, 1), ("مجلس آینده", 2, 1),
    ("رئیس جمهور", 2, 1), ("رئیسجمهور", 2, 1),
    ("نظرسنجی", 3, 2), ("نظرسنجی انتخاباتی", 4, 2),
    ("debate", 3, 1), ("polling", 3, 2),
    ("campaign", 2, 1), ("manifesto", 3, 2),
]

_RAW_REJECTION = [
    "فیلم سینمایی", "سریال تلویزیونی", "بازیگر سینما",
    "لیگ برتر فوتبال", "جام جهانی فوتبال", "والیبال",
    "بیت کوین", "ارز دیجیتال", "رمزارز",
    "زلزله", "آتش سوزی", "تصادف رانندگی",
    "آشپزی", "رسپی غذا", "فال روز", "طالع بینی",
    "هواشناسی فردا",
]

KNOWN_CANDIDATES = [
    "پزشکیان", "جلیلی", "قالیباف", "زاکانی",
    "لاریجانی", "روحانی", "احمدینژاد",
    "رضایی", "همتی", "مهرعلیزاده",
    "جهانگیری", "واعظی", "ظریف",
    "میرسلیم", "پورمحمدی",
]

TOPIC_PATTERNS: dict[str, list[str]] = {
    "صلاحیت":   ["رد صلاحیت", "تایید صلاحیت", "احراز صلاحیت",
                  "بررسی صلاحیت", "شورای نگهبان"],
    "ثبت‌نام":  ["ثبتنام", "داوطلب انتخابات", "نامزد انتخابات"],
    "تبلیغات":  ["تبلیغات انتخاباتی", "ستاد انتخاباتی", "مناظره"],
    "رای‌گیری": ["رایگیری", "رای گیری", "صندوق رای", "مشارکت انتخاباتی"],
    "نتایج":    ["نتایج انتخابات", "شمارش آرا", "پیروز انتخابات"],
    "مجلس":     ["مجلس شورای اسلامی", "نماینده مجلس", "نمایندگان مجلس"],
    "شورا":     ["شورای شهر", "شورای اسلامی", "شوراهای اسلامی"],
}

_HASHTAG_MAP = [
    ("انتخابات", "#انتخابات"), ("ریاست", "#ریاست_جمهوری"),
    ("مجلس", "#مجلس"), ("شورا", "#شورای_شهر"),
    ("کاندیدا", "#کاندیدا"), ("نامزد", "#نامزد_انتخاباتی"),
    ("ثبتنام", "#ثبت_نام"), ("صلاحیت", "#صلاحیت"),
    ("شورای نگهبان", "#شورای_نگهبان"), ("مناظره", "#مناظره"),
    ("مشارکت", "#مشارکت"), ("نمایندگان", "#نمایندگان"),
    ("اصلاحطلب", "#اصلاح_طلبان"), ("اصولگرا", "#اصولگرایان"),
    ("election", "#Election"), ("candidate", "#Candidate"),
    ("parliament", "#Parliament"), ("presidential", "#Presidential"),
]

# ── Pre-compile ──
def _compile_scored(raw):
    out = []
    for kw, ts, ds in raw:
        p = _compile_wb(kw)
        if p:
            out.append((p, ts, ds))
    return out

def _compile_simple(raw):
    out = []
    for kw in raw:
        p = _compile_wb(kw)
        if p:
            out.append(p)
    return out

LAYER1_COMPILED = _compile_scored(_RAW_LAYER1)
LAYER2_COMPILED = _compile_scored(_RAW_LAYER2)
REJECTION_COMPILED = _compile_simple(_RAW_REJECTION)

CANDIDATE_PATTERNS = []
for _n in KNOWN_CANDIDATES:
    _p = _compile_wb(_n)
    if _p:
        CANDIDATE_PATTERNS.append((_p, _n))

TOPIC_COMPILED = {}
for _t, _pats in TOPIC_PATTERNS.items():
    _c = [_compile_wb(p) for p in _pats]
    _c = [x for x in _c if x]
    if _c:
        TOPIC_COMPILED[_t] = _c


# ═══════════════════════════════════════════════════════════
# SECTION 3 — LOGGER
# ═══════════════════════════════════════════════════════════

class _Logger:
    def __init__(self, context=None):
        self._ctx = context
        self._metrics = {
            "fuzzy_match_count": 0,
            "hash_collisions": 0,
            "feed_latencies": {},
        }

    @property
    def metrics(self):
        return self._metrics

    def info(self, msg):
        o = f"[INFO] {msg}"
        if self._ctx and hasattr(self._ctx, 'log'):
            self._ctx.log(o)
        else:
            print(o)

    def warn(self, msg):
        o = f"[WARN] {msg}"
        if self._ctx and hasattr(self._ctx, 'log'):
            self._ctx.log(o)
        else:
            print(o)

    def error(self, msg):
        o = f"[ERROR] {msg}"
        if self._ctx and hasattr(self._ctx, 'error'):
            self._ctx.error(o)
        else:
            print(o)

    def item(self, action, source, title, score, tier,
             candidates=None, topics=None):
        parts = [f"[{action}]", f"s={score}", f"t={tier}",
                 f"[{source}]", title[:55]]
        if candidates:
            parts.append(f"c={','.join(candidates[:2])}")
        if topics:
            parts.append(f"tp={','.join(topics[:2])}")
        self.info(" ".join(parts))

    def feed_latency(self, source, ms):
        self._metrics["feed_latencies"][source] = round(ms, 1)

    def record_fuzzy_match(self):
        self._metrics["fuzzy_match_count"] += 1

    def record_hash_collision(self):
        self._metrics["hash_collisions"] += 1


log = _Logger()


# ═══════════════════════════════════════════════════════════
# SECTION 4 — BALE CLIENT (Telegram-compatible HTTP API)
# ═══════════════════════════════════════════════════════════

class _BaleClient:
    """
    Bale Messenger uses a Telegram-compatible Bot API.
    Base URL: https://tapi.bale.ai/bot{token}/
    Supports: sendMessage, sendPhoto, sendMediaGroup
    with same parameter names as Telegram Bot API.
    """

    BASE_URL = "https://tapi.bale.ai/bot"

    def __init__(self, token: str, chat_id: str):
        self._token = token
        self._chat_id = chat_id
        self._api = f"{self.BASE_URL}{token}"
        self._enabled = bool(token and chat_id)

    @property
    def enabled(self) -> bool:
        return self._enabled

    def _call(self, method: str, data: dict = None,
              files: dict = None, timeout: int = BALE_TIMEOUT) -> dict | None:
        """Make a synchronous API call to Bale."""
        url = f"{self._api}/{method}"
        try:
            if files:
                resp = requests.post(url, data=data, files=files,
                                     timeout=timeout)
            else:
                resp = requests.post(url, json=data, timeout=timeout)

            if resp.status_code == 200:
                result = resp.json()
                if result.get("ok"):
                    return result
                else:
                    log.warn(f"Bale API error: {result.get('description', 'unknown')}")
                    return None
            else:
                log.warn(f"Bale HTTP {resp.status_code}: {resp.text[:150]}")
                return None
        except requests.exceptions.Timeout:
            log.warn(f"Bale {method} timed out")
            return None
        except Exception as e:
            log.warn(f"Bale {method} error: {e}")
            return None

    def send_message(self, text: str, parse_mode: str = "HTML") -> bool:
        """Send text message to Bale channel."""
        result = self._call("sendMessage", {
            "chat_id":    self._chat_id,
            "text":       text,
            "parse_mode": parse_mode,
        })
        return result is not None

    def send_photo(self, photo_url: str, caption: str = "",
                   parse_mode: str = "HTML") -> bool:
        """Send single photo with caption to Bale channel."""
        result = self._call("sendPhoto", {
            "chat_id":    self._chat_id,
            "photo":      photo_url,
            "caption":    caption,
            "parse_mode": parse_mode,
        })
        return result is not None

    def send_media_group(self, image_urls: list[str],
                         caption: str = "",
                         parse_mode: str = "HTML") -> bool:
        """Send album (media group) to Bale channel."""
        media = []
        for i, url in enumerate(image_urls):
            item = {"type": "photo", "media": url}
            if i == 0 and caption:
                item["caption"] = caption
                item["parse_mode"] = parse_mode
            media.append(item)

        result = self._call("sendMediaGroup", {
            "chat_id": self._chat_id,
            "media":   json.dumps(media),
        })
        return result is not None

    def post(self, image_urls: list[str], caption: str) -> bool:
        """
        High-level post: images+caption or text-only.
        Mirrors Telegram posting logic.
        """
        if not self._enabled:
            return True  # silently skip if not configured

        if len(image_urls) >= 2:
            ok = self.send_media_group(image_urls[:MAX_IMAGES], caption)
            if ok:
                return True
            # Fallback to single photo
            image_urls = image_urls[:1]

        if len(image_urls) == 1:
            ok = self.send_photo(image_urls[0], caption)
            if ok:
                return True
            # Fallback to text

        return self.send_message(caption)


# ═══════════════════════════════════════════════════════════
# SECTION 5 — RATE LIMITER
# ═══════════════════════════════════════════════════════════

class _RateLimiter:
    def __init__(self, max_per_minute: int = RATE_LIMIT_PER_MINUTE):
        self._max = max_per_minute
        self._timestamps: list[float] = []

    def can_post(self) -> bool:
        now = monotonic()
        self._timestamps = [t for t in self._timestamps if now - t < 60.0]
        return len(self._timestamps) < self._max

    def record_post(self):
        self._timestamps.append(monotonic())

    @property
    def remaining(self) -> int:
        now = monotonic()
        recent = [t for t in self._timestamps if now - t < 60.0]
        return max(0, self._max - len(recent))


# ═══════════════════════════════════════════════════════════
# SECTION 6 — MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════

async def main(event=None, context=None):
    global log
    log = _Logger(context)
    _t0 = monotonic()

    def _remaining() -> float:
        return GLOBAL_DEADLINE_SEC - (monotonic() - _t0)

    log.info("══════════════════════════════════════")
    log.info("Election Bot v6.0 — Telegram + Bale")
    log.info(f"Time: {datetime.now(timezone.utc).isoformat()}")
    log.info("══════════════════════════════════════")

    config = _load_config()
    if not config:
        return _response({"error": "missing_env_vars"})

    # ── Initialize platform clients ──
    tg_bot = Bot(token=config["telegram_token"])

    bale = _BaleClient(
        token=config.get("bale_token", ""),
        chat_id=config.get("bale_chat_id", ""),
    )

    platforms = ["Telegram"]
    if bale.enabled:
        platforms.append("Bale")
    log.info(f"Platforms: {', '.join(platforms)}")

    db = _AppwriteDB(
        endpoint      = config["endpoint"],
        project       = config["project"],
        key           = config["key"],
        database_id   = config["database_id"],
        collection_id = config["collection_id"],
    )
    rate_limiter = _RateLimiter()
    loop         = asyncio.get_event_loop()
    now          = datetime.now(timezone.utc)
    time_threshold = now - timedelta(hours=HOURS_THRESHOLD)

    stats = {
        "feeds_ok": 0, "feeds_fail": 0, "feeds_retry": 0,
        "entries_total": 0,
        "skip_time": 0, "skip_topic": 0, "skip_dupe": 0,
        "queued_high": 0, "queued_medium": 0, "queued_low": 0,
        "posted_tg": 0, "posted_bale": 0,
        "post_retries": 0, "post_fallbacks": 0,
        "errors": 0, "db_timeout": False, "overflow": 0,
    }

    # ════════════════════════════════════════════════════
    # PHASE 1: Fetch feeds
    # ════════════════════════════════════════════════════
    fetch_budget = min(FEEDS_TOTAL_TIMEOUT, _remaining() - 16)
    if fetch_budget < 3:
        log.warn("Insufficient time for feeds")
        return _response(stats)

    log.info(f"Phase 1: Fetching {len(RSS_SOURCES)} feeds "
             f"(budget={fetch_budget:.1f}s)")

    try:
        all_entries = await asyncio.wait_for(
            _fetch_all_feeds_retry(loop, stats),
            timeout=fetch_budget,
        )
    except asyncio.TimeoutError:
        log.warn("Feed fetch timed out — partial results")
        all_entries = []

    stats["entries_total"] = len(all_entries)
    log.info(f"Phase 1 done: {len(all_entries)} entries, "
             f"{stats['feeds_ok']}/{len(RSS_SOURCES)} feeds ok "
             f"[{_remaining():.1f}s left]")

    if not all_entries:
        return _response(stats)

    all_entries.sort(
        key=lambda x: x["pub_date"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    # ════════════════════════════════════════════════════
    # PHASE 2: Load DB state
    # ════════════════════════════════════════════════════
    known_links:  set[str]   = set()
    known_hashes: set[str]   = set()
    fuzzy_records: list[dict] = []

    db_budget = min(DB_TIMEOUT, _remaining() - 12)
    if db_budget > 2:
        log.info(f"Phase 2: DB load (budget={db_budget:.1f}s)")
        try:
            raw = await asyncio.wait_for(
                loop.run_in_executor(None, db.load_recent, 500),
                timeout=db_budget,
            )
            for rec in raw:
                lnk = rec.get("link", "")
                ch  = rec.get("content_hash", "")
                if lnk:
                    known_links.add(lnk)
                if ch:
                    if ch in known_hashes:
                        log.record_hash_collision()
                    known_hashes.add(ch)
                fuzzy_records.append({
                    "title":      rec.get("title", ""),
                    "title_norm": _normalize_text(rec.get("title", "")),
                })
            log.info(f"Phase 2 done: {len(raw)} records "
                     f"[{_remaining():.1f}s left]")
        except asyncio.TimeoutError:
            stats["db_timeout"] = True
            log.warn("DB load timed out — local-only dedup")
        except Exception as e:
            stats["db_timeout"] = True
            log.error(f"DB load failed: {e}")
    else:
        log.warn("No budget for DB load")

    posted_hashes: set[str] = set()

    # ════════════════════════════════════════════════════
    # PHASE 3: Score + Dedup + Triage
    # ════════════════════════════════════════════════════
    log.info("Phase 3: Scoring and dedup")
    publish_queue: list[dict] = []

    for item in all_entries:
        title    = item["title"]
        link     = item["link"]
        desc     = item["desc"]
        source   = item["source"]
        pub_date = item["pub_date"]

        if pub_date and pub_date < time_threshold:
            stats["skip_time"] += 1
            continue

        result = _score_article(title, desc)
        score      = result["score"]
        tier       = result["tier"]
        candidates = result["candidates"]
        topics     = result["topics"]

        if tier == "LOW":
            stats["skip_topic"] += 1
            stats["queued_low"] += 1
            continue

        content_hash = _make_hash(title)

        if content_hash in posted_hashes or content_hash in known_hashes:
            stats["skip_dupe"] += 1
            continue

        if link in known_links:
            stats["skip_dupe"] += 1
            continue

        if _is_fuzzy_duplicate(title, fuzzy_records):
            stats["skip_dupe"] += 1
            log.record_fuzzy_match()
            continue

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

    publish_queue.sort(key=lambda x: x["score"], reverse=True)

    log.info(f"Phase 3 done: queue={len(publish_queue)} "
             f"(H={stats['queued_high']} M={stats['queued_medium']}) "
             f"[{_remaining():.1f}s left]")

    # ════════════════════════════════════════════════════
    # PHASE 4: Publish to Telegram + Bale (parallel)
    # ════════════════════════════════════════════════════
    log.info(f"Phase 4: Publishing to {', '.join(platforms)}")

    batch_posted = 0

    for item in publish_queue:
        if _remaining() < 8:
            idx = publish_queue.index(item)
            stats["overflow"] += len(publish_queue) - idx
            log.info(f"Time low ({_remaining():.1f}s) — stopping")
            break

        if batch_posted >= PUBLISH_BATCH_SIZE:
            idx = publish_queue.index(item)
            stats["overflow"] += len(publish_queue) - idx
            log.info(f"Batch limit ({PUBLISH_BATCH_SIZE}) reached")
            break

        if not rate_limiter.can_post():
            idx = publish_queue.index(item)
            stats["overflow"] += len(publish_queue) - idx
            log.warn("Rate limit reached")
            break

        tg_ok, bale_ok = await _publish_dual(
            item, tg_bot, bale,
            config["telegram_chat_id"],
            db, loop, now, _remaining, stats,
        )

        if tg_ok:
            stats["posted_tg"] += 1
        if bale_ok:
            stats["posted_bale"] += 1

        if tg_ok or bale_ok:
            batch_posted += 1
            rate_limiter.record_post()
            known_hashes.add(item["content_hash"])
            known_links.add(item["link"])

            if _remaining() > 4 and batch_posted < PUBLISH_BATCH_SIZE:
                await asyncio.sleep(INTER_POST_DELAY)
        else:
            stats["errors"] += 1

    # ════════════════════════════════════════════════════
    # PHASE 5: Summary
    # ════════════════════════════════════════════════════
    elapsed = monotonic() - _t0
    total_posted = max(stats["posted_tg"], stats["posted_bale"])

    log.info("═══════════ SUMMARY ═══════════")
    log.info(f"Time: {elapsed:.1f}s / {GLOBAL_DEADLINE_SEC}s")
    log.info(f"Feeds: {stats['feeds_ok']} ok | "
             f"{stats['feeds_fail']} fail | {stats['feeds_retry']} retries")
    log.info(f"Entries: {stats['entries_total']}")
    log.info(f"Skipped: time={stats['skip_time']} "
             f"topic={stats['skip_topic']} dupe={stats['skip_dupe']}")
    log.info(f"Queue: H={stats['queued_high']} "
             f"M={stats['queued_medium']} L={stats['queued_low']}")
    log.info(f"Posted TG: {stats['posted_tg']} | "
             f"Bale: {stats['posted_bale']}")
    log.info(f"Retries: {stats['post_retries']} | "
             f"Fallbacks: {stats['post_fallbacks']}")
    log.info(f"Errors: {stats['errors']} | Overflow: {stats['overflow']}")
    if stats["db_timeout"]:
        log.warn("DB timeout occurred")
    log.info("═══════════════════════════════")

    return _response(stats)


def _response(stats: dict) -> dict:
    return {
        "status":         "success" if not stats.get("error") else "error",
        "posted_tg":      stats.get("posted_tg", 0),
        "posted_bale":    stats.get("posted_bale", 0),
        "feeds_ok":       stats.get("feeds_ok", 0),
        "feeds_failed":   stats.get("feeds_fail", 0),
        "entries_total":  stats.get("entries_total", 0),
        "queued_high":    stats.get("queued_high", 0),
        "queued_medium":  stats.get("queued_medium", 0),
        "errors":         stats.get("errors", 0),
        "overflow":       stats.get("overflow", 0),
        "db_timeout":     stats.get("db_timeout", False),
    }


# ═══════════════════════════════════════════════════════════
# SECTION 7 — DUAL-PLATFORM PUBLISH
# ═══════════════════════════════════════════════════════════

async def _publish_dual(
    item: dict, tg_bot: Bot, bale: _BaleClient,
    tg_chat_id: str, db: '_AppwriteDB',
    loop: asyncio.AbstractEventLoop,
    now: datetime, _remaining, stats: dict,
) -> tuple[bool, bool]:
    """
    Publish to both Telegram and Bale.
    DB save happens first (atomic dedup).
    Telegram and Bale post independently — one failure
    does not block the other.
    Returns (telegram_ok, bale_ok).
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
    save_budget = min(DB_TIMEOUT, _remaining() - 6)
    if save_budget < 1:
        log.warn("No time for DB save")
        return (False, False)

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
        log.warn(f"DB save timed out [{source}] {title[:40]}")
        return (False, False)

    if not saved:
        log.info(f"DB rejected [{source}] {title[:40]}")
        return (False, False)

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

    # ── Post to BOTH platforms in parallel ──
    tg_budget = min(TELEGRAM_TIMEOUT, _remaining() - 2)

    # Telegram (async)
    tg_task = asyncio.ensure_future(
        _post_telegram_with_retry(
            tg_bot, tg_chat_id, image_urls, caption,
            tg_budget, stats,
        )
    )

    # Bale (sync via executor, parallel with Telegram)
    bale_task = asyncio.ensure_future(
        loop.run_in_executor(
            None, bale.post, image_urls, caption,
        )
    ) if bale.enabled else None

    # Wait for Telegram
    tg_ok = False
    try:
        tg_ok = await asyncio.wait_for(tg_task, timeout=max(tg_budget, 2))
    except asyncio.TimeoutError:
        log.warn("Telegram post timed out")
    except Exception as e:
        log.error(f"Telegram post error: {e}")

    # Wait for Bale
    bale_ok = False
    if bale_task:
        try:
            bale_ok = await asyncio.wait_for(bale_task, timeout=3)
            if bale_ok:
                log.info(f"[BALE:OK] [{source}] {title[:40]}")
            else:
                log.warn(f"[BALE:FAIL] [{source}] {title[:40]}")
        except asyncio.TimeoutError:
            log.warn(f"[BALE:TIMEOUT] [{source}] {title[:40]}")
        except Exception as e:
            log.warn(f"[BALE:ERROR] {e}")
    else:
        bale_ok = True  # Not configured = not a failure

    if tg_ok:
        log.item("POSTED:TG", source, title, score, tier,
                 candidates=candidates, topics=topics)

    return (tg_ok, bale_ok)


async def _post_telegram_with_retry(
    bot: Bot, chat_id: str, image_urls: list[str],
    caption: str, budget: float, stats: dict,
) -> bool:
    """Telegram posting with retry + fallback."""
    for attempt in range(TG_POST_MAX_RETRIES):
        try:
            ok = await asyncio.wait_for(
                _post_to_telegram(bot, chat_id, image_urls, caption,
                                  attempt=attempt),
                timeout=budget,
            )
            if ok:
                return True
        except RetryAfter as e:
            wait = min(e.retry_after, 2.0)
            log.warn(f"TG RetryAfter: {e.retry_after}s")
            stats["post_retries"] += 1
            if budget > wait + 2:
                await asyncio.sleep(wait)
                continue
            return False
        except asyncio.TimeoutError:
            stats["post_retries"] += 1
            if attempt < TG_POST_MAX_RETRIES - 1:
                continue
            return False
        except TelegramError as e:
            stats["post_retries"] += 1
            if attempt == TG_POST_MAX_RETRIES - 1 and image_urls:
                stats["post_fallbacks"] += 1
                try:
                    return await _post_text_only(bot, chat_id, caption)
                except Exception:
                    pass
            elif attempt < TG_POST_MAX_RETRIES - 1:
                continue
    return False


async def _post_text_only(bot: Bot, chat_id: str, caption: str) -> bool:
    try:
        await bot.send_message(
            chat_id=chat_id, text=caption,
            parse_mode="HTML",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            disable_notification=True,
        )
        return True
    except TelegramError:
        return False


# ═══════════════════════════════════════════════════════════
# SECTION 8 — FEED FETCHER
# ═══════════════════════════════════════════════════════════

async def _fetch_all_feeds_retry(loop, stats):
    tasks = [
        loop.run_in_executor(None, _fetch_one_feed_retry, url, name, stats)
        for url, name in RSS_SOURCES
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_entries = []
    for i, result in enumerate(results):
        src = RSS_SOURCES[i][1]
        if isinstance(result, Exception):
            log.error(f"{src}: {result}")
            stats["feeds_fail"] += 1
        elif result is None:
            stats["feeds_fail"] += 1
        elif result:
            all_entries.extend(result)
            stats["feeds_ok"] += 1
        else:
            stats["feeds_ok"] += 1
    return all_entries


def _fetch_one_feed_retry(url, source, stats):
    last_error = None
    for attempt in range(FEED_MAX_RETRIES):
        t0 = monotonic()
        try:
            resp = requests.get(
                url, timeout=FEED_FETCH_TIMEOUT,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; ElectionBot/6.0)",
                    "Accept": "application/rss+xml, application/xml, */*",
                },
            )
            lat = (monotonic() - t0) * 1000
            log.feed_latency(source, lat)

            if resp.status_code == 404:
                log.warn(f"{source}: HTTP 404")
                return []
            if resp.status_code != 200:
                last_error = f"HTTP {resp.status_code}"
                if attempt < FEED_MAX_RETRIES - 1:
                    stats["feeds_retry"] += 1
                    sleep(FEED_RETRY_BASE_SEC * (2 ** attempt))
                    continue
                return None

            feed = feedparser.parse(resp.content)
            if feed.bozo and not feed.entries:
                log.warn(f"{source}: Malformed feed")
                return []

            entries = []
            for entry in feed.entries:
                title = _clean(entry.get("title", ""))
                link  = _clean(entry.get("link", ""))
                if not title or not link:
                    continue
                raw_html = entry.get("summary") or entry.get("description") or ""
                desc = _truncate(_strip_html(raw_html), MAX_DESC_CHARS)
                pub_date = _parse_date(entry)
                entries.append({
                    "title": title, "link": link, "desc": desc,
                    "pub_date": pub_date, "source": source,
                    "feed_url": url, "entry": entry,
                })

            log.info(f"[FEED] {source}: {len(entries)} entries ({lat:.0f}ms)")
            return entries

        except requests.exceptions.ConnectionError as e:
            last_error = f"Conn: {str(e)[:60]}"
        except requests.exceptions.Timeout:
            last_error = "Timeout"
        except Exception as e:
            log.error(f"{source}: {str(e)[:80]}")
            return None

        if attempt < FEED_MAX_RETRIES - 1:
            stats["feeds_retry"] += 1
            sleep(FEED_RETRY_BASE_SEC * (2 ** attempt))

    log.error(f"{source}: Failed after {FEED_MAX_RETRIES} attempts: {last_error}")
    return None


# ═══════════════════════════════════════════════════════════
# SECTION 9 — SCORING ENGINE
# ═══════════════════════════════════════════════════════════

def _score_article(title, desc):
    nt = _pre_normalize(title)
    nd = _pre_normalize(desc)
    pt = f" {nt} "
    pd = f" {nd} "
    pa = f" {nt} {nd} "

    for p in REJECTION_COMPILED:
        if p.search(pa):
            return {"score": -1, "tier": "LOW",
                    "candidates": [], "topics": []}

    score = 0
    for p, ts, ds in LAYER1_COMPILED:
        if p.search(pt):
            score += ts
        elif p.search(pd):
            score += ds

    for p, ts, ds in LAYER2_COMPILED:
        if p.search(pt):
            score += ts
        elif p.search(pd):
            score += ds

    candidates = []
    for p, name in CANDIDATE_PATTERNS:
        if p.search(pa):
            candidates.append(name)
            score += 2 if p.search(pt) else 1

    topics = []
    for topic, pats in TOPIC_COMPILED.items():
        for p in pats:
            if p.search(pa):
                if topic not in topics:
                    topics.append(topic)
                break

    if candidates and score >= SCORE_MEDIUM:
        score += 2
    if len(topics) >= 2 and score >= SCORE_MEDIUM:
        score += 1

    tier = "HIGH" if score >= SCORE_HIGH else (
           "MEDIUM" if score >= SCORE_MEDIUM else "LOW")

    return {"score": score, "tier": tier,
            "candidates": candidates, "topics": topics}


# ═══════════════════════════════════════════════════════════
# SECTION 10 — APPWRITE DB
# ═══════════════════════════════════════════════════════════

class _AppwriteDB:
    def __init__(self, endpoint, project, key, database_id, collection_id):
        self._url = (
            f"{endpoint}/databases/{database_id}"
            f"/collections/{collection_id}/documents"
        )
        self._headers = {
            "Content-Type": "application/json",
            "X-Appwrite-Project": project,
            "X-Appwrite-Key": key,
        }

    def load_recent(self, limit=500):
        try:
            resp = requests.get(
                self._url, headers=self._headers,
                params={"limit": str(limit), "orderType": "DESC"},
                timeout=DB_TIMEOUT,
            )
            if resp.status_code != 200:
                return []
            docs = resp.json().get("documents", [])
            return [
                {"link": d.get("link", ""),
                 "title": d.get("title", ""),
                 "content_hash": d.get("content_hash", ""),
                 "site": d.get("site", "")}
                for d in docs
            ]
        except requests.exceptions.Timeout:
            raise
        except Exception as e:
            log.error(f"DB load: {e}")
            return []

    def save(self, link, title, content_hash, site,
             feed_url, published_at, created_at):
        doc_id = content_hash[:36]
        try:
            resp = requests.post(
                self._url, headers=self._headers,
                json={
                    "documentId": doc_id,
                    "data": {
                        "link": link[:700],
                        "title": title[:300],
                        "content_hash": content_hash[:128],
                        "site": site[:100],
                        "feed_url": feed_url[:500],
                        "published_at": published_at,
                        "created_at": created_at,
                    },
                },
                timeout=DB_TIMEOUT,
            )
            if resp.status_code in (200, 201):
                return True
            if resp.status_code == 409:
                log.info("DB 409 — already exists")
                return False
            log.warn(f"DB save: HTTP {resp.status_code}")
            return False
        except Exception as e:
            log.warn(f"DB save: {e}")
            return False


# ═══════════════════════════════════════════════════════════
# SECTION 11 — CONFIG
# ═══════════════════════════════════════════════════════════

def _load_config() -> dict | None:
    cfg = {
        "telegram_token":   os.environ.get("TELEGRAM_BOT_TOKEN"),
        "telegram_chat_id": os.environ.get("TELEGRAM_CHANNEL_ID"),
        "bale_token":       os.environ.get("BLE_TOKEN", ""),
        "bale_chat_id":     os.environ.get("BLE_CHANNEL_ID", ""),
        "endpoint":         os.environ.get("APPWRITE_ENDPOINT",
                                           "https://cloud.appwrite.io/v1"),
        "project":          os.environ.get("APPWRITE_PROJECT_ID"),
        "key":              os.environ.get("APPWRITE_API_KEY"),
        "database_id":      os.environ.get("APPWRITE_DATABASE_ID"),
        "collection_id":    os.environ.get("APPWRITE_COLLECTION_ID", "history"),
    }

    # Telegram + Appwrite are required; Bale is optional
    required = ["telegram_token", "telegram_chat_id", "project",
                "key", "database_id"]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        log.error(f"Missing required env vars: {missing}")
        return None

    if not cfg["bale_token"] or not cfg["bale_chat_id"]:
        log.info("Bale not configured — Telegram-only mode")

    return cfg


# ═══════════════════════════════════════════════════════════
# SECTION 12 — TEXT UTILITIES
# ═══════════════════════════════════════════════════════════

def _clean(text):
    return (text or "").strip()

def _strip_html(html):
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script", "style", "iframe"]):
            tag.decompose()
        return " ".join(soup.get_text(separator=" ").split())
    except Exception:
        return re.sub(r"<[^>]+>", " ", html).strip()

def _truncate(text, limit):
    if len(text) <= limit:
        return text
    cut = text[:limit]
    ls = cut.rfind(" ")
    if ls > limit * 0.8:
        cut = cut[:ls]
    return cut + "…"

def _parse_date(entry):
    for f in ("published_parsed", "updated_parsed"):
        p = entry.get(f)
        if p:
            try:
                return datetime(*p[:6], tzinfo=timezone.utc)
            except (ValueError, TypeError):
                continue
    return None

def _escape_html(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def _normalize_text(text):
    if not text:
        return ""
    t = _pre_normalize(text)
    tokens = [tok for tok in t.split()
              if tok not in PERSIAN_STOPWORDS and len(tok) >= 2]
    return " ".join(tokens)

def _make_hash(title, desc=""):
    norm = _normalize_text(title)
    tokens = sorted(norm.split())
    return hashlib.sha256(" ".join(tokens).encode("utf-8")).hexdigest()


# ═══════════════════════════════════════════════════════════
# SECTION 13 — FUZZY DEDUP
# ═══════════════════════════════════════════════════════════

def _is_fuzzy_duplicate(title, records):
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
        if inter / min(len(incoming), len(stored)) >= 0.75:
            return True
        if inter / len(incoming | stored) >= FUZZY_THRESHOLD:
            return True
    return False


# ═══════════════════════════════════════════════════════════
# SECTION 14 — IMAGE COLLECTION
# ═══════════════════════════════════════════════════════════

async def _collect_images_async(entry, url, loop):
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

def _extract_rss_images(entry):
    images, seen = [], set()

    def _add(url):
        url = (url or "").strip()
        if not url or not url.startswith("http") or url in seen:
            return
        lower = url.lower()
        if any(b in lower for b in IMAGE_BLOCKLIST):
            return
        base = lower.split("?")[0]
        if not (any(base.endswith(e) for e in IMAGE_EXTENSIONS) or
                any(w in lower for w in
                    ["image", "photo", "img", "media", "cdn", "upload"])):
            return
        seen.add(url)
        images.append(url)

    for m in entry.get("media_content", []):
        u = m.get("url", "") if isinstance(m, dict) else getattr(m, "url", "")
        med = m.get("medium", "") if isinstance(m, dict) else getattr(m, "medium", "")
        if med == "image" or any(u.lower().endswith(e) for e in IMAGE_EXTENSIONS):
            _add(u)

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
        u = t.get("url", "") if isinstance(t, dict) else getattr(t, "url", "")
        _add(u)

    if len(images) < MAX_IMAGES:
        raw = (entry.get("summary") or entry.get("description") or
               (entry.get("content") or [{}])[0].get("value", ""))
        if raw:
            try:
                soup = BeautifulSoup(raw, "lxml")
                for img in soup.find_all("img"):
                    for attr in ("src", "data-src", "data-lazy-src"):
                        s = img.get(attr, "")
                        if s and s.startswith("http"):
                            _add(s)
                            break
                    if len(images) >= MAX_IMAGES:
                        break
            except Exception:
                pass
    return images

def _fetch_og_image(url):
    try:
        resp = requests.get(url, timeout=2.5,
                           headers={"User-Agent": "Mozilla/5.0"},
                           allow_redirects=True)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "lxml")
        for prop in ("og:image", "twitter:image"):
            tag = (soup.find("meta", property=prop) or
                   soup.find("meta", attrs={"name": prop}))
            if tag:
                c = tag.get("content", "").strip()
                if c.startswith("http"):
                    return c
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════
# SECTION 15 — CAPTION + HASHTAGS
# ═══════════════════════════════════════════════════════════

def _generate_hashtags(title, desc, topics=None):
    norm = _pre_normalize(title + " " + desc)
    seen, tags = set(), []

    if topics:
        topic_ht = {
            "صلاحیت": "#صلاحیت", "ثبت‌نام": "#ثبت_نام",
            "تبلیغات": "#تبلیغات_انتخاباتی",
            "رای‌گیری": "#رأی_گیری",
            "نتایج": "#نتایج_انتخابات",
            "مجلس": "#مجلس", "شورا": "#شورای_شهر",
        }
        for t in topics:
            ht = topic_ht.get(t)
            if ht and ht not in seen:
                seen.add(ht)
                tags.append(ht)

    for kw, ht in _HASHTAG_MAP:
        if len(tags) >= 5:
            break
        kn = _pre_normalize(kw)
        if kn and kn in norm and ht not in seen:
            seen.add(ht)
            tags.append(ht)

    if "#انتخابات" not in seen:
        tags.insert(0, "#انتخابات")
    return tags[:6]


def _build_caption(title, desc, hashtags=None,
                   candidates=None, source=""):
    st = _escape_html(title.strip())
    sd = _escape_html(desc.strip())
    hl = " ".join(hashtags) if hashtags else "#انتخابات"

    if candidates:
        for c in candidates[:2]:
            ct = f"#{_escape_html(c.replace(' ', '_'))}"
            if ct not in hl:
                hl += f" {ct}"

    sl = f"📰 {_escape_html(source)}\n" if source else ""

    caption = (
        f"💠 <b>{st}</b>\n\n"
        f"{hl}\n\n"
        f"@candidatoryiran\n\n"
        f"{sd}\n\n"
        f"{sl}"
        f"🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷\n"
        f"کانال خبری کاندیداتوری\n"
        f"🆔 @candidatoryiran\n"
        f"🆔 Instagram.com/candidatory.ir"
    )

    if len(caption) > CAPTION_MAX:
        overflow = len(caption) - CAPTION_MAX
        sd = sd[:max(0, len(sd) - overflow - 5)] + "…"
        caption = (
            f"💠 <b>{st}</b>\n\n"
            f"{hl}\n\n"
            f"@candidatoryiran\n\n"
            f"{sd}\n\n"
            f"{sl}"
            f"🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷\n"
            f"کانال خبری کاندیداتوری\n"
            f"🆔 @candidatoryiran\n"
            f"🆔 Instagram.com/candidatory.ir"
        )
    return caption


# ═══════════════════════════════════════════════════════════
# SECTION 16 — TELEGRAM POSTING
# ═══════════════════════════════════════════════════════════

async def _post_to_telegram(bot, chat_id, image_urls, caption,
                            attempt=0):
    imgs = image_urls
    if attempt > 0 and len(imgs) > 1:
        imgs = imgs[:1]

    if len(imgs) >= 2:
        try:
            media = []
            for i, url in enumerate(imgs[:MAX_IMAGES]):
                if i == 0:
                    media.append(InputMediaPhoto(
                        media=url, caption=caption, parse_mode="HTML"))
                else:
                    media.append(InputMediaPhoto(media=url))
            await bot.send_media_group(
                chat_id=chat_id, media=media,
                disable_notification=True)
            return True
        except TelegramError as e:
            log.warn(f"TG album failed: {e}")
            imgs = imgs[:1]

    if len(imgs) == 1:
        try:
            await bot.send_photo(
                chat_id=chat_id, photo=imgs[0],
                caption=caption, parse_mode="HTML",
                disable_notification=True)
            return True
        except TelegramError as e:
            log.warn(f"TG photo failed: {e}")
            if attempt > 0:
                return await _post_text_only(bot, chat_id, caption)

    return await _post_text_only(bot, chat_id, caption)


# ═══════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    asyncio.run(main())
