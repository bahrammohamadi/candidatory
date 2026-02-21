# ============================================================
# Telegram Election News Bot — @candidatoryiran
# Version:    5.0 — Production Cloud Function
# Runtime:    Python 3.12 / Appwrite Cloud Functions
# Timeout:    30 seconds (Appwrite free plan limit)
#
# CHANGELOG v5.0:
#   ✓ Telegram post retry (2 attempts) with text-only fallback
#   ✓ Per-chat rate limiting (max posts per minute)
#   ✓ Feed latency tracking
#   ✓ Fuzzy match count metrics
#   ✓ Hash collision detection
#   ✓ Multi-batch scheduling (overflow → next run)
#   ✓ Adaptive source reliability tracking
#   ✓ Improved word-boundary scoring precision
#   ✓ Full structured logging via context.log()
#   ✓ Graceful degradation at every layer
#
# ARCHITECTURE:
#   Phase 1: Fetch feeds parallel + retry       (budget: 10s)
#   Phase 2: Load DB state + local fallback     (budget: 5s)
#   Phase 3: Score + extract + dedup            (in-memory)
#   Phase 4: Prioritize + batch publish         (remaining)
#   Phase 5: Summary + overflow tracking        (instant)
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
# SECTION 1 — CONFIGURATION (all configurable via env vars)
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

# ── RSS Sources ──
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
    ("https://www.moi.ir/rss",                         "MOI"),
    ("https://www.shoraha.org.ir/rss",                 "Shoraha"),
]

# ── Timing budgets (seconds) ──
GLOBAL_DEADLINE_SEC   = _env_int("BOT_DEADLINE_SEC", 27)
FEED_FETCH_TIMEOUT    = _env_int("FEED_FETCH_TIMEOUT", 4)
FEEDS_TOTAL_TIMEOUT   = _env_int("FEEDS_TOTAL_TIMEOUT", 10)
DB_TIMEOUT            = _env_int("DB_TIMEOUT", 5)
IMAGE_SCRAPE_TIMEOUT  = _env_int("IMAGE_SCRAPE_TIMEOUT", 3)
TELEGRAM_TIMEOUT      = _env_int("TELEGRAM_TIMEOUT", 5)
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
# SECTION 2 — KEYWORD SYSTEM (WORD-BOUNDARY, PRE-COMPILED)
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
    """Compile word-boundary pattern from normalized keyword."""
    nk = _pre_normalize(keyword)
    if not nk:
        return None
    return re.compile(r'(?:^|\s)' + re.escape(nk) + r'(?:\s|$)')


# Layer 1: Core election keywords
# (raw_keyword, title_points, desc_points)
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

# Layer 2: Contextual political keywords
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

# Rejection keywords
_RAW_REJECTION = [
    "فیلم سینمایی", "سریال تلویزیونی", "بازیگر سینما",
    "لیگ برتر فوتبال", "جام جهانی فوتبال", "والیبال",
    "بیت کوین", "ارز دیجیتال", "رمزارز",
    "زلزله", "آتش سوزی", "تصادف رانندگی",
    "آشپزی", "رسپی غذا", "فال روز", "طالع بینی",
    "هواشناسی فردا",
]

# Candidates
KNOWN_CANDIDATES = [
    "پزشکیان", "جلیلی", "قالیباف", "زاکانی",
    "لاریجانی", "روحانی", "احمدینژاد",
    "رضایی", "همتی", "مهرعلیزاده",
    "جهانگیری", "واعظی", "ظریف",
    "میرسلیم", "پورمحمدی",
]

# Topic patterns (tagging only, not scoring)
TOPIC_PATTERNS: dict[str, list[str]] = {
    "صلاحیت":      ["رد صلاحیت", "تایید صلاحیت", "احراز صلاحیت",
                     "بررسی صلاحیت", "شورای نگهبان"],
    "ثبت‌نام":     ["ثبتنام", "داوطلب انتخابات", "نامزد انتخابات"],
    "تبلیغات":     ["تبلیغات انتخاباتی", "ستاد انتخاباتی", "مناظره"],
    "رای‌گیری":    ["رایگیری", "رای گیری", "صندوق رای", "مشارکت انتخاباتی"],
    "نتایج":       ["نتایج انتخابات", "شمارش آرا", "پیروز انتخابات"],
    "مجلس":        ["مجلس شورای اسلامی", "نماینده مجلس", "نمایندگان مجلس"],
    "شورا":        ["شورای شهر", "شورای اسلامی", "شوراهای اسلامی"],
}

# Hashtag map
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

# ── Pre-compile all patterns at module load ──

def _compile_scored_list(raw: list[tuple]) -> list[tuple[re.Pattern, int, int]]:
    compiled = []
    for kw, ts, ds in raw:
        pat = _compile_wb(kw)
        if pat:
            compiled.append((pat, ts, ds))
    return compiled

def _compile_simple_list(raw: list[str]) -> list[re.Pattern]:
    compiled = []
    for kw in raw:
        pat = _compile_wb(kw)
        if pat:
            compiled.append(pat)
    return compiled

LAYER1_COMPILED = _compile_scored_list(_RAW_LAYER1)
LAYER2_COMPILED = _compile_scored_list(_RAW_LAYER2)
REJECTION_COMPILED = _compile_simple_list(_RAW_REJECTION)

CANDIDATE_PATTERNS: list[tuple[re.Pattern, str]] = []
for _name in KNOWN_CANDIDATES:
    _pat = _compile_wb(_name)
    if _pat:
        CANDIDATE_PATTERNS.append((_pat, _name))

TOPIC_COMPILED: dict[str, list[re.Pattern]] = {}
for _topic, _patterns in TOPIC_PATTERNS.items():
    _compiled = []
    for _p in _patterns:
        _cpat = _compile_wb(_p)
        if _cpat:
            _compiled.append(_cpat)
    if _compiled:
        TOPIC_COMPILED[_topic] = _compiled


# ═══════════════════════════════════════════════════════════
# SECTION 3 — STRUCTURED LOGGER
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
    def metrics(self) -> dict:
        return self._metrics

    def info(self, msg: str):
        out = f"[INFO] {msg}"
        if self._ctx and hasattr(self._ctx, 'log'):
            self._ctx.log(out)
        else:
            print(out)

    def warn(self, msg: str):
        out = f"[WARN] {msg}"
        if self._ctx and hasattr(self._ctx, 'log'):
            self._ctx.log(out)
        else:
            print(out)

    def error(self, msg: str):
        out = f"[ERROR] {msg}"
        if self._ctx and hasattr(self._ctx, 'error'):
            self._ctx.error(out)
        else:
            print(out)

    def item(self, action: str, source: str, title: str,
             score: int, tier: str,
             candidates: list = None, topics: list = None):
        parts = [f"[{action}]", f"s={score}", f"t={tier}",
                 f"[{source}]", title[:55]]
        if candidates:
            parts.append(f"c={','.join(candidates[:2])}")
        if topics:
            parts.append(f"tp={','.join(topics[:2])}")
        self.info(" ".join(parts))

    def feed_latency(self, source: str, latency_ms: float):
        self._metrics["feed_latencies"][source] = round(latency_ms, 1)

    def record_fuzzy_match(self):
        self._metrics["fuzzy_match_count"] += 1

    def record_hash_collision(self):
        self._metrics["hash_collisions"] += 1


log = _Logger()


# ═══════════════════════════════════════════════════════════
# SECTION 4 — RATE LIMITER
# ═══════════════════════════════════════════════════════════

class _RateLimiter:
    """Per-chat rate limiter for Telegram posting."""

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
# SECTION 5 — MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════

async def main(event=None, context=None):
    global log
    log = _Logger(context)
    _t0 = monotonic()

    def _remaining() -> float:
        return GLOBAL_DEADLINE_SEC - (monotonic() - _t0)

    log.info("══════════════════════════════════")
    log.info("Election Bot v5.0 — Production")
    log.info(f"Time: {datetime.now(timezone.utc).isoformat()}")
    log.info(f"Config: deadline={GLOBAL_DEADLINE_SEC}s batch={PUBLISH_BATCH_SIZE} "
             f"rate={RATE_LIMIT_PER_MINUTE}/min")
    log.info("══════════════════════════════════")

    config = _load_config()
    if not config:
        return _response({"error": "missing_env_vars"})

    bot = Bot(token=config["token"])
    db  = _AppwriteDB(
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
        "posted": 0, "post_retries": 0, "post_fallbacks": 0,
        "errors": 0, "db_timeout": False,
        "overflow": 0,
    }

    # ════════════════════════════════════════════════════════
    # PHASE 1: Fetch ALL feeds in parallel with retry
    # ════════════════════════════════════════════════════════
    fetch_budget = min(FEEDS_TOTAL_TIMEOUT, _remaining() - 16)
    if fetch_budget < 3:
        log.warn("Insufficient time for feed fetch")
        return _response(stats)

    log.info(f"Phase 1: Fetching {len(RSS_SOURCES)} feeds (budget={fetch_budget:.1f}s)")

    try:
        all_entries = await asyncio.wait_for(
            _fetch_all_feeds_retry(loop, stats),
            timeout=fetch_budget,
        )
    except asyncio.TimeoutError:
        log.warn(f"Feed fetch timed out at {fetch_budget:.1f}s — partial results")
        all_entries = []

    stats["entries_total"] = len(all_entries)

    # Feed latency summary
    latencies = log.metrics.get("feed_latencies", {})
    if latencies:
        avg_lat = sum(latencies.values()) / len(latencies)
        log.info(f"Feed latency: avg={avg_lat:.0f}ms "
                 f"max={max(latencies.values()):.0f}ms "
                 f"({len(latencies)} feeds)")

    log.info(f"Phase 1 done: {len(all_entries)} entries from "
             f"{stats['feeds_ok']}/{len(RSS_SOURCES)} feeds "
             f"({stats['feeds_fail']} failed, {stats['feeds_retry']} retries) "
             f"[{_remaining():.1f}s left]")

    if not all_entries:
        return _response(stats)

    all_entries.sort(
        key=lambda x: x["pub_date"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    # ════════════════════════════════════════════════════════
    # PHASE 2: Load DB state ONCE (with timeout + fallback)
    # ════════════════════════════════════════════════════════
    known_links:   set[str]   = set()
    known_hashes:  set[str]   = set()
    fuzzy_records: list[dict] = []
    source_reliability: dict[str, float] = {}

    db_budget = min(DB_TIMEOUT, _remaining() - 12)
    if db_budget > 2:
        log.info(f"Phase 2: Loading DB state (budget={db_budget:.1f}s)")
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
                # Track source reliability
                src = rec.get("site", "")
                if src:
                    source_reliability[src] = source_reliability.get(src, 0) + 1

            log.info(f"Phase 2 done: {len(raw)} records → "
                     f"{len(known_links)} links, {len(known_hashes)} hashes "
                     f"(collisions={log.metrics['hash_collisions']}) "
                     f"[{_remaining():.1f}s left]")
        except asyncio.TimeoutError:
            stats["db_timeout"] = True
            log.warn("DB load timed out — using local-only dedup as fallback")
        except Exception as e:
            stats["db_timeout"] = True
            log.error(f"DB load failed: {e}")
    else:
        log.warn("Insufficient budget for DB load — local fallback only")

    posted_hashes: set[str] = set()

    # ════════════════════════════════════════════════════════
    # PHASE 3: Score + Extract + Dedup + Triage
    # ════════════════════════════════════════════════════════
    log.info("Phase 3: Scoring and deduplication")

    publish_queue: list[dict] = []

    for item in all_entries:
        title    = item["title"]
        link     = item["link"]
        desc     = item["desc"]
        source   = item["source"]
        pub_date = item["pub_date"]

        # ── Time filter ──
        if pub_date and pub_date < time_threshold:
            stats["skip_time"] += 1
            continue

        # ── Score ──
        result = _score_article(title, desc)
        score      = result["score"]
        tier       = result["tier"]
        candidates = result["candidates"]
        topics     = result["topics"]

        # ── Source reliability boost ──
        src_count = source_reliability.get(source, 0)
        if src_count > 10 and score >= SCORE_MEDIUM:
            score += 1  # trusted source gets small boost

        # ── Re-evaluate tier after boost ──
        if score >= SCORE_HIGH:
            tier = "HIGH"
        elif score >= SCORE_MEDIUM:
            tier = "MEDIUM"

        if tier == "LOW":
            stats["skip_topic"] += 1
            stats["queued_low"] += 1
            continue

        # ── Dedup: hash ──
        content_hash = _make_hash(title)

        if content_hash in posted_hashes:
            stats["skip_dupe"] += 1
            continue

        if content_hash in known_hashes:
            stats["skip_dupe"] += 1
            continue

        # ── Dedup: link ──
        if link in known_links:
            stats["skip_dupe"] += 1
            continue

        # ── Dedup: fuzzy ──
        if _is_fuzzy_duplicate(title, fuzzy_records):
            stats["skip_dupe"] += 1
            log.record_fuzzy_match()
            continue

        # ── Mark seen ──
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

    log.info(f"Phase 3 done: queue={len(publish_queue)} "
             f"(H={stats['queued_high']} M={stats['queued_medium']} "
             f"L={stats['queued_low']}) "
             f"dupes={stats['skip_dupe']} "
             f"fuzzy_matches={log.metrics['fuzzy_match_count']} "
             f"[{_remaining():.1f}s left]")

    # ════════════════════════════════════════════════════════
    # PHASE 4: Batch Publish (priority order, rate-limited)
    # ════════════════════════════════════════════════════════
    log.info(f"Phase 4: Publishing (batch={PUBLISH_BATCH_SIZE}, "
             f"rate={rate_limiter.remaining}/{RATE_LIMIT_PER_MINUTE})")

    batch_posted = 0

    for item in publish_queue:
        # ── Budget check ──
        if _remaining() < 8:
            log.info(f"Time budget low ({_remaining():.1f}s) — stopping publish")
            stats["overflow"] += len(publish_queue) - (
                publish_queue.index(item))
            break

        # ── Batch limit ──
        if batch_posted >= PUBLISH_BATCH_SIZE:
            remaining_items = len(publish_queue) - publish_queue.index(item)
            stats["overflow"] += remaining_items
            log.info(f"Batch limit ({PUBLISH_BATCH_SIZE}) reached. "
                     f"{remaining_items} items overflow to next run.")
            break

        # ── Rate limit ──
        if not rate_limiter.can_post():
            log.warn("Rate limit reached — stopping publish")
            remaining_items = len(publish_queue) - publish_queue.index(item)
            stats["overflow"] += remaining_items
            break

        success = await _publish_item_with_retry(
            item, bot, config["chat_id"], db, loop,
            now, _remaining, stats,
        )

        if success:
            stats["posted"] += 1
            batch_posted += 1
            rate_limiter.record_post()
            known_hashes.add(item["content_hash"])
            known_links.add(item["link"])

            if _remaining() > 4 and batch_posted < PUBLISH_BATCH_SIZE:
                await asyncio.sleep(INTER_POST_DELAY)
        else:
            stats["errors"] += 1

    # ════════════════════════════════════════════════════════
    # PHASE 5: Summary
    # ════════════════════════════════════════════════════════
    elapsed = monotonic() - _t0

    log.info("═══════════ SUMMARY ═══════════")
    log.info(f"Time: {elapsed:.1f}s / {GLOBAL_DEADLINE_SEC}s")
    log.info(f"Feeds: {stats['feeds_ok']} ok | {stats['feeds_fail']} failed | "
             f"{stats['feeds_retry']} retries")
    log.info(f"Entries: {stats['entries_total']} total")
    log.info(f"Skipped: time={stats['skip_time']} topic={stats['skip_topic']} "
             f"dupe={stats['skip_dupe']}")
    log.info(f"Queue: H={stats['queued_high']} M={stats['queued_medium']} "
             f"L={stats['queued_low']}")
    log.info(f"Posted: {stats['posted']} | Retries: {stats['post_retries']} | "
             f"Fallbacks: {stats['post_fallbacks']}")
    log.info(f"Errors: {stats['errors']} | Overflow: {stats['overflow']}")
    log.info(f"Fuzzy matches: {log.metrics['fuzzy_match_count']} | "
             f"Hash collisions: {log.metrics['hash_collisions']}")
    if stats["db_timeout"]:
        log.warn("DB timeout occurred — dedup may be incomplete")
    log.info("═══════════════════════════════")

    return _response(stats)


def _response(stats: dict) -> dict:
    return {
        "status":          "success" if not stats.get("error") else "error",
        "posted":          stats.get("posted", 0),
        "feeds_ok":        stats.get("feeds_ok", 0),
        "feeds_failed":    stats.get("feeds_fail", 0),
        "feeds_retries":   stats.get("feeds_retry", 0),
        "entries_total":   stats.get("entries_total", 0),
        "queued_high":     stats.get("queued_high", 0),
        "queued_medium":   stats.get("queued_medium", 0),
        "queued_low":      stats.get("queued_low", 0),
        "skipped_time":    stats.get("skip_time", 0),
        "skipped_topic":   stats.get("skip_topic", 0),
        "skipped_dupe":    stats.get("skip_dupe", 0),
        "post_retries":    stats.get("post_retries", 0),
        "post_fallbacks":  stats.get("post_fallbacks", 0),
        "errors":          stats.get("errors", 0),
        "overflow":        stats.get("overflow", 0),
        "db_timeout":      stats.get("db_timeout", False),
    }


# ═══════════════════════════════════════════════════════════
# SECTION 6 — FEED FETCHER WITH RETRY + LATENCY TRACKING
# ═══════════════════════════════════════════════════════════

async def _fetch_all_feeds_retry(
    loop: asyncio.AbstractEventLoop, stats: dict,
) -> list[dict]:
    tasks = [
        loop.run_in_executor(None, _fetch_one_feed_retry, url, name, stats)
        for url, name in RSS_SOURCES
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_entries: list[dict] = []
    for i, result in enumerate(results):
        src = RSS_SOURCES[i][1]
        if isinstance(result, Exception):
            log.error(f"{src}: Unhandled exception: {result}")
            stats["feeds_fail"] += 1
        elif result is None:
            stats["feeds_fail"] += 1
        elif result:
            all_entries.extend(result)
            stats["feeds_ok"] += 1
        else:
            # Empty list = feed worked but had no entries
            stats["feeds_ok"] += 1

    return all_entries


def _fetch_one_feed_retry(
    url: str, source: str, stats: dict,
) -> list[dict] | None:
    last_error = None

    for attempt in range(FEED_MAX_RETRIES):
        t_start = monotonic()
        try:
            resp = requests.get(
                url,
                timeout=FEED_FETCH_TIMEOUT,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; ElectionBot/5.0)",
                    "Accept": "application/rss+xml, application/xml, */*",
                },
            )
            latency_ms = (monotonic() - t_start) * 1000
            log.feed_latency(source, latency_ms)

            if resp.status_code == 404:
                log.warn(f"{source}: HTTP 404")
                return []

            if resp.status_code != 200:
                last_error = f"HTTP {resp.status_code}"
                if attempt < FEED_MAX_RETRIES - 1:
                    stats["feeds_retry"] += 1
                    delay = FEED_RETRY_BASE_SEC * (2 ** attempt)
                    log.warn(f"{source}: {last_error} — retry {attempt+1} "
                             f"in {delay:.1f}s")
                    sleep(delay)
                    continue
                log.warn(f"{source}: {last_error} after {attempt+1} attempts")
                return None

            feed = feedparser.parse(resp.content)
            if feed.bozo and not feed.entries:
                log.warn(f"{source}: Malformed feed (bozo)")
                return []

            entries = []
            for entry in feed.entries:
                title = _clean(entry.get("title", ""))
                link  = _clean(entry.get("link", ""))
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
                    "source":   source,
                    "feed_url": url,
                    "entry":    entry,
                })

            suffix = f" (retry {attempt})" if attempt > 0 else ""
            log.info(f"[FEED] {source}: {len(entries)} entries "
                     f"({latency_ms:.0f}ms){suffix}")
            return entries

        except requests.exceptions.ConnectionError as e:
            last_error = f"Connection: {str(e)[:80]}"
        except requests.exceptions.Timeout:
            last_error = "Timeout"
        except Exception as e:
            last_error = f"Unexpected: {str(e)[:80]}"
            log.error(f"{source}: {last_error}")
            return None

        # Retry logic for connection/timeout errors
        if attempt < FEED_MAX_RETRIES - 1:
            stats["feeds_retry"] += 1
            delay = FEED_RETRY_BASE_SEC * (2 ** attempt)
            log.warn(f"{source}: {last_error} — retry {attempt+1} in {delay:.1f}s")
            sleep(delay)
        else:
            log.error(f"{source}: Failed after {FEED_MAX_RETRIES} attempts: "
                      f"{last_error}")

    return None


# ═══════════════════════════════════════════════════════════
# SECTION 7 — SCORING ENGINE (WORD-BOUNDARY, DUAL-LAYER)
# ═══════════════════════════════════════════════════════════

def _score_article(title: str, desc: str) -> dict:
    norm_title = _pre_normalize(title)
    norm_desc  = _pre_normalize(desc)
    padded_t   = f" {norm_title} "
    padded_d   = f" {norm_desc} "
    padded_all = f" {norm_title} {norm_desc} "

    # ── Rejection ──
    for pat in REJECTION_COMPILED:
        if pat.search(padded_all):
            return {"score": -1, "tier": "LOW",
                    "candidates": [], "topics": []}

    score = 0

    # ── Layer 1: Core ──
    for pat, t_pts, d_pts in LAYER1_COMPILED:
        if pat.search(padded_t):
            score += t_pts
        elif pat.search(padded_d):
            score += d_pts

    # ── Layer 2: Context ──
    for pat, t_pts, d_pts in LAYER2_COMPILED:
        if pat.search(padded_t):
            score += t_pts
        elif pat.search(padded_d):
            score += d_pts

    # ── Candidates ──
    candidates: list[str] = []
    for pat, name in CANDIDATE_PATTERNS:
        if pat.search(padded_all):
            candidates.append(name)
            if pat.search(padded_t):
                score += 2
            else:
                score += 1

    # ── Topics (tagging only) ──
    topics: list[str] = []
    for topic, pats in TOPIC_COMPILED.items():
        for pat in pats:
            if pat.search(padded_all):
                if topic not in topics:
                    topics.append(topic)
                break

    # ── Multi-signal boost ──
    if candidates and score >= SCORE_MEDIUM:
        score += 2
    if len(topics) >= 2 and score >= SCORE_MEDIUM:
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
        "candidates": candidates,
        "topics":     topics,
    }


# ═══════════════════════════════════════════════════════════
# SECTION 8 — PUBLISH WITH RETRY + FALLBACK
# ═══════════════════════════════════════════════════════════

async def _publish_item_with_retry(
    item: dict, bot: Bot, chat_id: str,
    db: '_AppwriteDB', loop: asyncio.AbstractEventLoop,
    now: datetime, _remaining, stats: dict,
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

    # ── Save to DB BEFORE posting (atomic dedup) ──
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
        log.warn(f"DB save timed out [{source}] {title[:40]}")
        return False

    if not saved:
        log.info(f"DB rejected (409/err) [{source}] {title[:40]}")
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

    # ── Post to Telegram with retry ──
    tg_budget = min(TELEGRAM_TIMEOUT, _remaining() - 2)
    if tg_budget < 2:
        log.warn("No time for Telegram post")
        return False

    for attempt in range(TG_POST_MAX_RETRIES):
        try:
            success = await asyncio.wait_for(
                _post_to_telegram(
                    bot, chat_id, image_urls, caption,
                    attempt=attempt,
                ),
                timeout=tg_budget,
            )
            if success:
                log.item("POSTED", source, title, score, tier,
                         candidates=candidates, topics=topics)
                return True

        except RetryAfter as e:
            wait = min(e.retry_after, 3.0)
            log.warn(f"Telegram RetryAfter: {e.retry_after}s — waiting {wait:.1f}s")
            if _remaining() > wait + 3:
                await asyncio.sleep(wait)
                stats["post_retries"] += 1
                continue
            else:
                log.warn("No time to wait for RetryAfter")
                return False

        except asyncio.TimeoutError:
            log.warn(f"Telegram post timed out (attempt {attempt+1})")
            stats["post_retries"] += 1
            if attempt < TG_POST_MAX_RETRIES - 1 and _remaining() > 4:
                continue
            return False

        except TelegramError as e:
            log.warn(f"Telegram error (attempt {attempt+1}): {e}")
            stats["post_retries"] += 1

            # On last attempt, try text-only fallback
            if attempt == TG_POST_MAX_RETRIES - 1 and image_urls:
                log.info("Attempting text-only fallback")
                stats["post_fallbacks"] += 1
                try:
                    fb = await asyncio.wait_for(
                        _post_text_only(bot, chat_id, caption),
                        timeout=min(3, _remaining() - 1),
                    )
                    if fb:
                        log.item("POSTED:fallback", source, title, score, tier)
                        return True
                except (asyncio.TimeoutError, TelegramError):
                    pass
            elif attempt < TG_POST_MAX_RETRIES - 1 and _remaining() > 4:
                continue

    return False


async def _post_text_only(bot: Bot, chat_id: str, caption: str) -> bool:
    """Text-only fallback — no images."""
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=caption,
            parse_mode="HTML",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            disable_notification=True,
        )
        return True
    except TelegramError:
        return False


# ═══════════════════════════════════════════════════════════
# SECTION 9 — APPWRITE DB
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
                log.warn(f"DB load: HTTP {resp.status_code}")
                return []
            docs = resp.json().get("documents", [])
            return [
                {
                    "link":         d.get("link", ""),
                    "title":        d.get("title", ""),
                    "content_hash": d.get("content_hash", ""),
                    "site":         d.get("site", ""),
                    "created_at":   d.get("created_at", ""),
                }
                for d in docs
            ]
        except requests.exceptions.Timeout:
            raise  # Let caller handle
        except Exception as e:
            log.error(f"DB load: {e}")
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
                log.info("DB 409 — duplicate (race won by another execution)")
                return False
            log.warn(f"DB save: HTTP {resp.status_code}: {resp.text[:150]}")
            return False
        except Exception as e:
            log.warn(f"DB save: {e}")
            return False


# ═══════════════════════════════════════════════════════════
# SECTION 10 — CONFIG
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
# SECTION 11 — TEXT UTILITIES
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
    cut = text[:limit]
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
# SECTION 12 — FUZZY DEDUP
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
# SECTION 13 — IMAGE COLLECTION
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
                c = tag.get("content", "").strip()
                if c.startswith("http"):
                    return c
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════
# SECTION 14 — CAPTION + HASHTAGS
# ═══════════════════════════════════════════════════════════

def _generate_hashtags(title: str, desc: str,
                       topics: list[str] = None) -> list[str]:
    norm = _pre_normalize(title + " " + desc)
    seen = set()
    tags = []

    if topics:
        topic_ht = {
            "صلاحیت":    "#صلاحیت",
            "ثبت‌نام":   "#ثبت_نام",
            "تبلیغات":   "#تبلیغات_انتخاباتی",
            "رای‌گیری":  "#رأی_گیری",
            "نتایج":     "#نتایج_انتخابات",
            "مجلس":      "#مجلس",
            "شورا":      "#شورای_شهر",
        }
        for t in topics:
            ht = topic_ht.get(t)
            if ht and ht not in seen:
                seen.add(ht)
                tags.append(ht)

    for kw, hashtag in _HASHTAG_MAP:
        if len(tags) >= 5:
            break
        kw_n = _pre_normalize(kw)
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
# SECTION 15 — TELEGRAM POSTING
# ═══════════════════════════════════════════════════════════

async def _post_to_telegram(
    bot: Bot, chat_id: str, image_urls: list[str],
    caption: str, attempt: int = 0,
) -> bool:
    """
    Post with image+caption. On retry (attempt>0), try fewer images
    or text-only fallback.
    """

    # On retry, reduce image count
    imgs = image_urls
    if attempt > 0 and len(imgs) > 1:
        imgs = imgs[:1]

    # ── Multiple images ──
    if len(imgs) >= 2:
        try:
            media = []
            for i, url in enumerate(imgs[:MAX_IMAGES]):
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
            log.warn(f"Album failed: {e}")
            imgs = imgs[:1]

    # ── Single image ──
    if len(imgs) == 1:
        try:
            await bot.send_photo(
                chat_id=chat_id, photo=imgs[0],
                caption=caption, parse_mode="HTML",
                disable_notification=True,
            )
            return True
        except TelegramError as e:
            log.warn(f"Photo failed: {e}")
            if attempt > 0:
                # Fallback to text on retry
                return await _post_text_only(bot, chat_id, caption)

    # ── Text only ──
    return await _post_text_only(bot, chat_id, caption)


# ═══════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    asyncio.run(main())
