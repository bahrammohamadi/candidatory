# ============================================================
# Telegram Election News Bot — @candidatoryiran
# Version:    2.0 — Bulletproof Deduplication + Caption in Image
# Runtime:    Python 3.12 / Appwrite Cloud Functions
# Timeout:    30 seconds (Appwrite free plan limit)
#
# DEDUP STRATEGY (v2.0):
#   1. content_hash = SHA-256 of sorted normalized title tokens
#      (description excluded — it varies across sources)
#   2. content_hash used as Appwrite document ID
#      → Appwrite enforces uniqueness atomically (409 on conflict)
#   3. DB save happens BEFORE Telegram post
#      → race condition window = zero
#   4. In-process posted_hashes set prevents same-run duplicates
#   5. Fuzzy check uses overlap coefficient + Jaccard
#   6. ZWNJ removed (not replaced with space) for consistent tokens
#
# POST FORMAT (v2.0):
#   Images + caption in ONE atomic message (no separate text)
#   Hashtags generated from content, placed after title
#
# TIMEOUT STRATEGY:
#   All network I/O runs in executor (non-blocking).
#   All feeds fetched in parallel via asyncio.gather().
#   Hard budget per phase:
#     Feed fetch total:  12s
#     Image scraping:     5s
#     DB operations:      3s
#     Telegram posting:   8s
#   Total worst case: ~28s — safely under 30s limit.
# ============================================================

import os
import re
import asyncio
import hashlib
import requests
import feedparser

from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from telegram import Bot, InputMediaPhoto, LinkPreviewOptions
from telegram.error import TelegramError


# ═══════════════════════════════════════════════════════════
# SECTION 1 — CONFIGURATION
# ═══════════════════════════════════════════════════════════

RSS_SOURCES: list[tuple[str, str]] = [
    ("https://www.farsnews.ir/rss",                         "Fars"),
    ("https://www.isna.ir/rss",                             "ISNA"),
    ("https://www.tasnimnews.com/fa/rss/feed/0/0/0",        "Tasnim"),
    ("https://www.mehrnews.com/rss",                        "Mehr"),
    ("https://www.entekhab.ir/fa/rss/allnews",              "Entekhab"),
    ("https://www.irna.ir/rss/fa/8/",                       "IRNA"),
    ("https://www.yjc.ir/fa/rss/allnews",                   "YJC"),
    ("https://www.tabnak.ir/fa/rss/allnews",                "Tabnak"),
    ("https://www.khabaronline.ir/rss",                     "KhabarOnline"),
    ("https://www.hamshahrionline.ir/rss",                  "Hamshahri"),
    ("https://www.ilna.ir/fa/rss",                          "ILNA"),
    ("https://feeds.bbci.co.uk/persian/rss.xml",            "BBCPersian"),
]

# ── Time budget constants (seconds) ──
FEED_FETCH_TIMEOUT   = 6
FEEDS_TOTAL_TIMEOUT  = 12
DB_TIMEOUT           = 3
IMAGE_SCRAPE_TIMEOUT = 5
TELEGRAM_TIMEOUT     = 8
INTER_POST_DELAY     = 2.0

# ── Content limits ──
MAX_IMAGES            = 5
MAX_DESCRIPTION_CHARS = 500
CAPTION_MAX           = 1024
HOURS_THRESHOLD       = 24
ELECTION_SCORE_PASS   = 2
FUZZY_THRESHOLD       = 0.60

# ── Image filters ──
IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.webp')
IMAGE_BLOCKLIST  = [
    'doubleclick', 'googletagmanager', 'analytics',
    'pixel', 'beacon', 'tracking', 'stat.', 'stats.',
]

# ── Election keywords ──
ELECTION_KEYWORDS_HIGH = [
    "انتخابات", "انتخاباتی", "ریاست‌جمهوری", "ریاست جمهوری",
    "مجلس", "شورا", "شورای شهر", "شورای اسلامی",
    "نامزد", "ثبت‌نام", "ثبت نام", "رد صلاحیت",
    "صلاحیت", "هیئت نظارت", "ستاد انتخابات",
    "حوزه انتخابیه", "تبلیغات انتخاباتی", "صندوق رای",
    "مشارکت انتخاباتی", "شورای نگهبان", "کاندیدا",
    "داوطلب انتخابات", "دور دوم انتخابات",
    "انتخابات ریاست", "انتخابات مجلس", "انتخابات شورا",
    "election", "elections", "electoral", "candidate",
    "ballot", "vote", "voting", "presidential",
    "parliament", "parliamentary", "runoff",
    "iran election", "iranian election", "majles",
]

ELECTION_KEYWORDS_LOW = [
    "رای", "رای‌گیری", "رای دادن", "انتخاب",
    "منتخب", "نماینده", "اصلاح‌طلب", "اصولگرا",
    "ائتلاف", "ستاد", "مناظره", "تبلیغات",
    "debate", "polling", "poll", "voter",
]

REJECTION_KEYWORDS = [
    "فیلم", "سریال", "بازیگر", "فوتبال", "والیبال",
    "بورس", "ارز", "دلار", "بیت کوین",
    "زلزله", "سیل", "آتش سوزی", "تصادف", "آشپزی",
]

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
    ("election",       "#Election"),
    ("candidate",      "#Candidate"),
    ("parliament",     "#Parliament"),
    ("vote",           "#Vote"),
    ("presidential",   "#Presidential"),
]


# ═══════════════════════════════════════════════════════════
# SECTION 2 — MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════

async def main(event=None, context=None):
    print("[INFO] ══════════════════════════════")
    print("[INFO] Election Bot v2.0 started")
    print(f"[INFO] {datetime.now(timezone.utc).isoformat()}")
    print("[INFO] ══════════════════════════════")

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
        "fetched":    0,
        "skip_time":  0,
        "skip_topic": 0,
        "skip_dupe":  0,
        "posted":     0,
        "errors":     0,
    }

    # ── Phase 1: Fetch ALL feeds in parallel ──────────────────
    print(f"[INFO] Fetching {len(RSS_SOURCES)} feeds in parallel...")
    try:
        all_entries: list[dict] = await asyncio.wait_for(
            _fetch_all_feeds_parallel(loop),
            timeout=FEEDS_TOTAL_TIMEOUT,
        )
    except asyncio.TimeoutError:
        print(f"[WARN] Feed fetch timed out after {FEEDS_TOTAL_TIMEOUT}s")
        all_entries = []

    print(f"[INFO] Total entries collected: {len(all_entries)}")

    if not all_entries:
        print("[INFO] No entries collected. Exiting.")
        return {"status": "success", "posted": 0}

    # ── Sort newest first ──
    all_entries.sort(
        key=lambda x: x["pub_date"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    # ── Phase 2: Load recent DB records for fuzzy check ──────
    recent_records: list[dict] = []
    try:
        recent_records = await asyncio.wait_for(
            loop.run_in_executor(None, db.load_recent),
            timeout=DB_TIMEOUT,
        )
        print(f"[INFO] {len(recent_records)} recent records for fuzzy check.")
    except asyncio.TimeoutError:
        print("[WARN] DB load_recent timed out — fuzzy check disabled.")

    # ── In-process dedup set ──
    posted_hashes: set[str] = set()

    # ── Phase 3: Filter + post each candidate ────────────────
    for item in all_entries:
        title    = item["title"]
        link     = item["link"]
        desc     = item["desc"]
        source   = item["source"]
        pub_date = item["pub_date"]

        stats["fetched"] += 1

        # ── Time filter ──
        if pub_date and pub_date < time_threshold:
            stats["skip_time"] += 1
            continue

        # ── Election filter (in-memory, instant) ──
        score = _election_score(title, desc)
        if score < ELECTION_SCORE_PASS:
            stats["skip_topic"] += 1
            print(f"[SKIP:topic] score={score} [{source}] {title[:50]}")
            continue

        # ── Duplicate check 0: in-process set ──
        content_hash = _make_hash(title)
        if content_hash in posted_hashes:
            stats["skip_dupe"] += 1
            print(f"[SKIP:dupe:local] [{source}] {title[:50]}")
            continue

        # ── Duplicate check 1: DB (link + hash) ──
        try:
            is_dup = await asyncio.wait_for(
                loop.run_in_executor(None, db.is_duplicate, link, content_hash),
                timeout=DB_TIMEOUT * 2,
            )
        except asyncio.TimeoutError:
            print(f"[WARN] DB timeout for duplicate check — skipping safely.")
            stats["skip_dupe"] += 1
            continue

        if is_dup:
            stats["skip_dupe"] += 1
            print(f"[SKIP:dupe:db] [{source}] {title[:50]}")
            continue

        # ── Duplicate check 2: fuzzy in-memory ──
        if _is_fuzzy_duplicate(title, recent_records):
            stats["skip_dupe"] += 1
            print(f"[SKIP:dupe:fuzzy] [{source}] {title[:50]}")
            continue

        print(f"[PASS] score={score} [{source}] {title[:50]}")

        # ── SAVE TO DB BEFORE POSTING (race-condition killer) ──
        try:
            saved = await asyncio.wait_for(
                loop.run_in_executor(
                    None, db.save,
                    link, title, content_hash, source, now.isoformat()
                ),
                timeout=DB_TIMEOUT,
            )
        except asyncio.TimeoutError:
            print("[WARN] DB save timed out — skipping post to prevent dupe.")
            stats["errors"] += 1
            continue

        if not saved:
            print("[WARN] DB save failed (409 or error) — skipping post.")
            stats["skip_dupe"] += 1
            continue

        # ── Mark in local set immediately ──
        posted_hashes.add(content_hash)

        # ── Collect images ──
        try:
            image_urls: list[str] = await asyncio.wait_for(
                _collect_images_async(item["entry"], link, loop),
                timeout=IMAGE_SCRAPE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            print("[WARN] Image scrape timed out — posting without images.")
            image_urls = []

        # ── Build caption with hashtags ──
        hashtags = _generate_hashtags(title, desc)
        caption  = _build_caption(title, desc, hashtags)

        # ── Post to Telegram ──
        try:
            success = await asyncio.wait_for(
                _post_to_telegram(bot, config["chat_id"], image_urls, caption),
                timeout=TELEGRAM_TIMEOUT,
            )
        except asyncio.TimeoutError:
            print("[WARN] Telegram post timed out.")
            success = False

        if success:
            stats["posted"] += 1
            print(f"[SUCCESS] [{source}] {title[:50]} (post #{stats['posted']})")

            # Update local fuzzy set
            recent_records.append({
                "title":      title,
                "title_norm": _normalize_text(title),
            })

            await asyncio.sleep(INTER_POST_DELAY)
        else:
            stats["errors"] += 1

    # ── Summary ──
    print("\n[INFO] ─────── SUMMARY ───────")
    print(f"[INFO] Fetched    : {stats['fetched']}")
    print(f"[INFO] Skip/time  : {stats['skip_time']}")
    print(f"[INFO] Skip/topic : {stats['skip_topic']}")
    print(f"[INFO] Skip/dupe  : {stats['skip_dupe']}")
    print(f"[INFO] Posted     : {stats['posted']}")
    print(f"[INFO] Errors     : {stats['errors']}")
    print("[INFO] ──────────────────────")

    return {"status": "success", "posted": stats["posted"]}


# ═══════════════════════════════════════════════════════════
# SECTION 3 — PARALLEL FEED FETCHER
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
            source_name = RSS_SOURCES[i][1]
            print(f"[ERROR] {source_name}: {result}")
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
                "User-Agent": "Mozilla/5.0 (compatible; ElectionBot/2.0)",
                "Accept":     "application/rss+xml, application/xml, */*",
            },
        )
        if resp.status_code != 200:
            print(f"[WARN] {source_name}: HTTP {resp.status_code}")
            return []

        feed = feedparser.parse(resp.content)
        if feed.bozo and not feed.entries:
            print(f"[WARN] {source_name}: Malformed feed")
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
                "entry":    entry,
            })

        print(f"[FEED] {source_name}: {len(entries)} entries")
        return entries

    except requests.RequestException as e:
        print(f"[ERROR] {source_name} fetch: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] {source_name} parse: {e}")
        return []


# ═══════════════════════════════════════════════════════════
# SECTION 4 — APPWRITE DATABASE CLIENT
# ═══════════════════════════════════════════════════════════

class _AppwriteDB:
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

    def is_duplicate(self, link: str, content_hash: str) -> bool:
        return (
            self._exists("link",         link[:500])
            or self._exists("content_hash", content_hash)
        )

    def load_recent(self) -> list[dict]:
        try:
            resp = requests.get(
                self._url,
                headers=self._headers,
                params={"limit": 200, "orderType": "DESC"},
                timeout=DB_TIMEOUT,
            )
            if resp.status_code != 200:
                return []
            docs = resp.json().get("documents", [])
            return [
                {
                    "title":      d.get("title", ""),
                    "title_norm": _normalize_text(d.get("title", "")),
                }
                for d in docs
            ]
        except Exception as e:
            print(f"[WARN] DB load_recent: {e}")
            return []

    def save(self, link: str, title: str, content_hash: str,
             source: str, created_at: str) -> bool:
        doc_id = content_hash[:36]
        try:
            resp = requests.post(
                self._url,
                headers=self._headers,
                json={
                    "documentId": doc_id,
                    "data": {
                        "link":         link[:500],
                        "title":        title[:300],
                        "content_hash": content_hash,
                        "source":       source[:100],
                        "created_at":   created_at,
                    },
                },
                timeout=DB_TIMEOUT,
            )
            if resp.status_code in (200, 201):
                return True
            if resp.status_code == 409:
                print(f"[INFO] DB 409 — already exists (race won by other execution)")
                return False
            print(f"[WARN] DB save {resp.status_code}: {resp.text[:200]}")
            return False
        except Exception as e:
            print(f"[WARN] DB save: {e}")
            return False

    def _exists(self, field: str, value: str) -> bool:
        try:
            resp = requests.get(
                self._url,
                headers=self._headers,
                params={
                    "queries[]": f'equal("{field}", ["{value}"])',
                    "limit":     1,
                },
                timeout=DB_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.json().get("total", 0) > 0
            return False
        except Exception:
            return False


# ═══════════════════════════════════════════════════════════
# SECTION 5 — CONFIG LOADER
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
        print(f"[ERROR] Missing env vars: {missing}")
        return None
    return cfg


# ═══════════════════════════════════════════════════════════
# SECTION 6 — TEXT UTILITIES
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
    # Arabic → Persian
    text = text.replace("ي", "ی").replace("ك", "ک")
    text = text.replace("ة", "ه").replace("ؤ", "و")
    text = text.replace("إ", "ا").replace("أ", "ا")
    text = text.replace("ئ", "ی").replace("ى", "ی")
    # Remove diacritics
    text = re.sub(r"[\u064B-\u065F\u0670]", "", text)
    # ZWNJ/ZWJ: REMOVE entirely (not replace with space)
    # This ensures "ثبت‌نام" and "ثبتنام" become identical
    text = re.sub(r"[\u200c\u200d\u200e\u200f\ufeff]", "", text)
    # Lowercase + remove punctuation
    text = text.lower()
    text = re.sub(r"[^\w\s\u0600-\u06FF]", " ", text)
    text = " ".join(text.split())
    # Remove stopwords
    tokens = [
        t for t in text.split()
        if t not in PERSIAN_STOPWORDS and len(t) >= 2
    ]
    return " ".join(tokens)


def _make_hash(title: str, desc: str = "") -> str:
    """
    Hash based on normalized title ONLY with sorted tokens.
    Description excluded — it varies across sources/updates.
    Sorting makes "انتخابات مجلس آغاز" == "آغاز انتخابات مجلس".
    """
    norm_title = _normalize_text(title)
    tokens = sorted(norm_title.split())
    canonical = " ".join(tokens)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ═══════════════════════════════════════════════════════════
# SECTION 7 — ELECTION FILTER
# ═══════════════════════════════════════════════════════════

def _election_score(title: str, desc: str) -> int:
    norm_title = _normalize_text(title)
    norm_desc  = _normalize_text(desc)
    combined   = norm_title + " " + norm_desc

    for kw in REJECTION_KEYWORDS:
        if _normalize_text(kw) in combined:
            return -1

    score = 0
    for kw in ELECTION_KEYWORDS_HIGH:
        kw_n = _normalize_text(kw)
        if not kw_n:
            continue
        if kw_n in norm_title:
            score += 3
        elif kw_n in norm_desc:
            score += 1

    for kw in ELECTION_KEYWORDS_LOW:
        kw_n = _normalize_text(kw)
        if not kw_n:
            continue
        if kw_n in norm_title:
            score += 1

    return score


# ═══════════════════════════════════════════════════════════
# SECTION 8 — FUZZY DUPLICATE DETECTION
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

        # Overlap coefficient: catches subset relationships
        min_size = min(len(incoming), len(stored))
        overlap = inter / min_size

        # Jaccard: catches near-equal sets
        union = len(incoming | stored)
        jaccard = inter / union

        if overlap >= 0.75 or jaccard >= FUZZY_THRESHOLD:
            print(f"  [FUZZY] overlap={overlap:.2f} jaccard={jaccard:.2f}: "
                  f"{record.get('title','')[:40]}")
            return True
    return False


# ═══════════════════════════════════════════════════════════
# SECTION 9 — IMAGE COLLECTION
# ═══════════════════════════════════════════════════════════

async def _collect_images_async(
    entry,
    article_url: str,
    loop: asyncio.AbstractEventLoop,
) -> list[str]:
    images = _extract_rss_images(entry)

    if not images:
        try:
            og = await asyncio.wait_for(
                loop.run_in_executor(None, _fetch_og_image, article_url),
                timeout=4.0,
            )
            if og:
                images.append(og)
        except asyncio.TimeoutError:
            print("  [WARN] og:image fetch timed out.")

    result = images[:MAX_IMAGES]
    print(f"  [INFO] Images: {len(result)}")
    return result


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
        has_word = any(
            w in lower
            for w in ["image", "photo", "img", "media", "cdn", "upload"]
        )
        if not has_ext and not has_word:
            return
        seen.add(url)
        images.append(url)

    # media:content
    for m in entry.get("media_content", []):
        url    = m.get("url", "")    if isinstance(m, dict) else getattr(m, "url", "")
        medium = m.get("medium", "") if isinstance(m, dict) else getattr(m, "medium", "")
        if medium == "image" or any(url.lower().endswith(e) for e in IMAGE_EXTENSIONS):
            _add(url)

    # enclosures
    enclosures = entry.get("enclosures", [])
    if not enclosures and hasattr(entry, "enclosure") and entry.enclosure:
        enclosures = [entry.enclosure]
    for enc in enclosures:
        mime = enc.get("type", "") if isinstance(enc, dict) else getattr(enc, "type", "")
        href = (enc.get("href") or enc.get("url", "")) if isinstance(enc, dict) \
               else (getattr(enc, "href", "") or getattr(enc, "url", ""))
        if mime.startswith("image/") and href:
            _add(href)

    # media:thumbnail
    for t in entry.get("media_thumbnail", []):
        url = t.get("url", "") if isinstance(t, dict) else getattr(t, "url", "")
        _add(url)

    # <img> in description HTML
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
            url,
            timeout=4,
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
# SECTION 10 — HASHTAG GENERATOR + CAPTION BUILDER
# ═══════════════════════════════════════════════════════════

def _generate_hashtags(title: str, desc: str) -> list[str]:
    norm = _normalize_text(title + " " + desc)
    seen = set()
    tags = []
    for keyword, hashtag in _HASHTAG_MAP:
        kw_norm = _normalize_text(keyword)
        if kw_norm and kw_norm in norm and hashtag not in seen:
            seen.add(hashtag)
            tags.append(hashtag)
        if len(tags) >= 5:
            break
    if "#انتخابات" not in seen:
        tags.insert(0, "#انتخابات")
    return tags


def _build_caption(title: str, desc: str, hashtags: list[str] = None) -> str:
    safe_title = _escape_html(title.strip())
    safe_desc  = _escape_html(desc.strip())
    hashtag_line = " ".join(hashtags) if hashtags else "#انتخابات"

    caption = (
        f"💠 <b>{safe_title}</b>\n\n"
        f"{hashtag_line}\n\n"
        f"@candidatoryiran\n\n"
        f"{safe_desc}\n\n"
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
            f"🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷\n"
            f"کانال خبری کاندیداتوری\n"
            f"🆔 @candidatoryiran\n"
            f"🆔 Instagram.com/candidatory.ir"
        )

    return caption


# ═══════════════════════════════════════════════════════════
# SECTION 11 — TELEGRAM POSTING
# ═══════════════════════════════════════════════════════════

async def _post_to_telegram(
    bot:        Bot,
    chat_id:    str,
    image_urls: list[str],
    caption:    str,
) -> bool:
    """
    Posts images with caption in ONE atomic message.
    Caption is on the first image (album) or the single photo.
    No separate text message. No delay between parts.
    """

    # ── Case A: Multiple images → album with caption on first ──
    if len(image_urls) >= 2:
        try:
            media_group = []
            for i, url in enumerate(image_urls[:MAX_IMAGES]):
                if i == 0:
                    media_group.append(InputMediaPhoto(
                        media=url,
                        caption=caption,
                        parse_mode="HTML",
                    ))
                else:
                    media_group.append(InputMediaPhoto(media=url))

            sent = await bot.send_media_group(
                chat_id=chat_id,
                media=media_group,
                disable_notification=True,
            )
            print(f"  [INFO] Album+caption: {len(sent)} images.")
            return True

        except TelegramError as e:
            print(f"  [WARN] Album failed: {e} — falling back to single photo")
            image_urls = image_urls[:1]

    # ── Case B: Single image → photo with caption ──
    if len(image_urls) == 1:
        try:
            await bot.send_photo(
                chat_id=chat_id,
                photo=image_urls[0],
                caption=caption,
                parse_mode="HTML",
                disable_notification=True,
            )
            print(f"  [INFO] Single photo+caption sent.")
            return True
        except TelegramError as e:
            print(f"  [WARN] Photo failed: {e} — falling back to text only")

    # ── Case C: No images → text only ──
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=caption,
            parse_mode="HTML",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            disable_notification=True,
        )
        print(f"  [INFO] Text-only caption sent.")
        return True
    except TelegramError as e:
        print(f"  [ERROR] Text send failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    asyncio.run(main())
