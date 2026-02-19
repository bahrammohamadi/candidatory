# ============================================================
# Telegram Election News Bot — @candidatoryiran
# Version:    1.0 — Iranian Election News Engine
# Runtime:    Python 3.12 / Appwrite Cloud Functions
# Timeout:    120 seconds
#
# SCOPE: Presidential / Parliamentary / Council elections
# TIME WINDOW: Last 24 hours only
# POSTS PER RUN: ALL valid election news found (no limit)
#
# POST FLOW (guaranteed order):
#   ① fetch_all_feeds() — parallel RSS fetch
#   ② normalize + election_filter() — scoring-based
#   ③ time_filter() — last 24 hours
#   ④ detect_duplicate() — link + hash + fuzzy 75%
#   ⑤ extract_images() — 5-method priority chain
#   ⑥ send_media_group(images, NO caption)
#      → anchor_id = last_message.message_id
#   ⑦ asyncio.sleep(2.5s)
#   ⑧ send_message(caption, reply_to=anchor_id)
#   ⑨ save_to_db()
#   ⑩ asyncio.sleep(inter-post delay)
#
# CAPTION FORMAT (unchanged from original):
#   💠 <b>Title</b>
#   @candidatoryiran
#   Summary
#   🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷
#   کانال خبری کاندیداتوری
#   🆔 @candidatoryiran
#   🆔 Instagram.com/candidatory.ir
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
# SECTION 1 — ELECTION NEWS RSS SOURCES
# ═══════════════════════════════════════════════════════════

# Each entry: (url, source_name, has_election_category)
# has_election_category=True → feed is election-specific,
#   apply lighter keyword filter (title only)
# has_election_category=False → general feed,
#   apply strict keyword filter (title + description)

RSS_SOURCES: list[tuple[str, str, bool]] = [

    # ── IRNA (Islamic Republic News Agency) ──
    ("https://www.irna.ir/rss/fa/8/",      "IRNA",          False),
    # IRNA category 8 = politics/elections

    # ── ISNA ──
    ("https://www.isna.ir/rss",             "ISNA",          False),

    # ── Fars News ──
    ("https://www.farsnews.ir/rss",         "Fars",          False),

    # ── Tasnim News ──
    ("https://www.tasnimnews.com/fa/rss/feed/0/0/0",
                                            "Tasnim",        False),

    # ── Mehr News ──
    ("https://www.mehrnews.com/rss",        "Mehr",          False),

    # ── Entekhab ──
    ("https://www.entekhab.ir/fa/rss/allnews",
                                            "Entekhab",      False),

    # ── YJC (Young Journalists Club) ──
    ("https://www.yjc.ir/fa/rss/allnews",   "YJC",           False),

    # ── Tabnak ──
    ("https://www.tabnak.ir/fa/rss/allnews","Tabnak",        False),

    # ── Khabar Online ──
    ("https://www.khabaronline.ir/rss",     "KhabarOnline",  False),

    # ── Hamshahri ──
    ("https://www.hamshahrionline.ir/rss",  "Hamshahri",     False),

    # ── ILNA (Iranian Labour News Agency) ──
    ("https://www.ilna.ir/fa/rss",          "ILNA",          False),

    # ── Sarpoosh (election section) ──
    ("https://sarpoosh.com/feed/",          "Sarpoosh",      False),

    # ── BBC Persian (Iran elections only — filtered strictly) ──
    ("https://feeds.bbci.co.uk/persian/rss.xml",
                                            "BBCPersian",    False),

    # ── Radio Farda ──
    ("https://www.radiofarda.com/api/zrqitpqmit",
                                            "RadioFarda",    False),
]

# ── Timing ──
HOURS_THRESHOLD       = 24      # only news from last 24 hours
ALBUM_CAPTION_DELAY   = 2.5     # seconds between album and caption
INTER_POST_DELAY      = 3.0     # seconds between consecutive posts
                                 # (Telegram flood protection)

# ── Limits ──
MAX_IMAGES            = 5
MAX_DESCRIPTION_CHARS = 600     # election summaries can be longer
CAPTION_MAX           = 4096    # Telegram message limit (not caption)

# ── Timeouts ──
FEED_TIMEOUT          = 10
PAGE_TIMEOUT          = 8
DB_TIMEOUT            = 6

# ── Duplicate detection ──
FUZZY_SIMILARITY_THRESHOLD = 0.75   # 75% token overlap = duplicate

# ── Image filtering ──
IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.webp')
IMAGE_BLOCKLIST  = [
    'doubleclick', 'googletagmanager', 'googlesyndication',
    'facebook.com/tr', 'analytics', 'pixel', 'beacon',
    'tracking', 'stat.', 'stats.',
]

# ── Election scoring thresholds ──
ELECTION_SCORE_PASS   = 2   # minimum score to pass filter
                             # (title match = 3pts, desc match = 1pt)


# ═══════════════════════════════════════════════════════════
# SECTION 2 — ELECTION KEYWORD LISTS
# ═══════════════════════════════════════════════════════════

# High-weight: 3 points each (title match)
# Low-weight:  1 point each (description-only match)
# Any single title match = pass
# Description-only matches must accumulate ≥ 2 points

ELECTION_KEYWORDS_HIGH = [
    # Core Persian election terms
    "انتخابات",
    "انتخاباتی",
    "ریاست‌جمهوری",
    "ریاست جمهوری",
    "مجلس",
    "شورا",
    "شورای شهر",
    "شورای اسلامی",
    "نامزد",
    "ثبت‌نام",
    "ثبت نام",
    "رد صلاحیت",
    "صلاحیت",
    "هیئت نظارت",
    "ستاد انتخابات",
    "حوزه انتخابیه",
    "تبلیغات انتخاباتی",
    "صندوق رای",
    "مشارکت انتخاباتی",
    "شعبه اخذ رای",
    "تایید صلاحیت",
    "کاندیدا",
    "داوطلب انتخابات",
    "دور دوم انتخابات",
    "دور اول انتخابات",
    "انتخابات ریاست",
    "انتخابات مجلس",
    "انتخابات شورا",
    "انتخابات ۱۴",    # covers 1403, 1404, etc.
    "شورای نگهبان",
    "وزارت کشور",
    "فرمانداری",
    "بخشداری",
    "کمیته اجرایی",
    # English (for BBC/Radio Farda)
    "election",
    "elections",
    "electoral",
    "candidate",
    "candidates",
    "ballot",
    "vote",
    "voting",
    "presidential",
    "parliament",
    "parliamentary",
    "runoff",
    "iran election",
    "iranian election",
    "majles",
    "shora",
]

ELECTION_KEYWORDS_LOW = [
    # Supporting terms — alone insufficient but contribute to score
    "رای",
    "رای‌گیری",
    "رای دادن",
    "رای‌دهی",
    "انتخاب",
    "منتخب",
    "نماینده",
    "نمایندگی",
    "اصلاح‌طلب",
    "اصولگرا",
    "مستقل",
    "ائتلاف",
    "فهرست انتخاباتی",
    "ستاد",
    "تبلیغات",
    "مناظره",
    "debate",
    "polling",
    "poll",
    "constituency",
    "district",
    "voter",
]

# Hard reject: if any of these appear, skip regardless of score
REJECTION_KEYWORDS = [
    "فیلم",
    "سریال",
    "بازیگر",
    "فوتبال",
    "والیبال",
    "بورس",
    "ارز",
    "دلار",
    "بیت کوین",
    "زلزله",
    "سیل",
    "آتش سوزی",
    "تصادف",
    "حادثه",
    "کشته شد",
    "آشپزی",
    "رستوران",
    "موبایل",
    "آیفون",
]

# Persian stopwords for hash normalization
PERSIAN_STOPWORDS = {
    "و", "در", "به", "از", "که", "این", "را", "با", "های",
    "برای", "آن", "یک", "هم", "تا", "اما", "یا", "بود",
    "شد", "است", "می", "هر", "اگر", "بر", "ها", "نیز",
    "کرد", "خود", "هیچ", "پس", "باید", "نه", "ما", "شود",
    "بین", "بی", "وی", "او", "ما", "شما", "آنها", "اینکه",
    "the", "a", "an", "is", "are", "was", "were", "of",
    "in", "to", "for", "and", "or", "but", "with", "on",
}


# ═══════════════════════════════════════════════════════════
# SECTION 3 — MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════

async def main(event=None, context=None):
    print("[INFO] ══════════════════════════════════════")
    print("[INFO] Election News Bot v1.0 started")
    print(f"[INFO] {datetime.now(timezone.utc).isoformat()}")
    print(f"[INFO] Sources: {len(RSS_SOURCES)}")
    print("[INFO] ══════════════════════════════════════")

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

    stats = {
        "fetched":    0,
        "skip_time":  0,
        "skip_elect": 0,
        "skip_dupe":  0,
        "posted":     0,
        "errors":     0,
    }

    # ── Load recent titles for fuzzy duplicate check ──
    recent_records = db.load_recent(hours=HOURS_THRESHOLD * 2)
    print(f"[INFO] {len(recent_records)} recent records loaded for fuzzy check.")

    # ── Collect all candidates from all feeds ──
    all_candidates: list[dict] = []

    for (feed_url, source_name, is_election_feed) in RSS_SOURCES:
        entries = _fetch_feed(feed_url, source_name)
        for entry in entries:
            all_candidates.append({
                "entry":            entry,
                "source":           source_name,
                "is_election_feed": is_election_feed,
                "feed_url":         feed_url,
            })

    print(f"[INFO] Total raw entries collected: {len(all_candidates)}")

    # ── Sort by publish date (newest first) ──
    def _sort_key(item):
        parsed = (
            item["entry"].get("published_parsed")
            or item["entry"].get("updated_parsed")
        )
        if parsed:
            try:
                return datetime(*parsed[:6], tzinfo=timezone.utc)
            except Exception:
                pass
        return datetime.min.replace(tzinfo=timezone.utc)

    all_candidates.sort(key=_sort_key, reverse=True)

    # ── Process each candidate ──
    for item in all_candidates:
        entry  = item["entry"]
        source = item["source"]

        stats["fetched"] += 1

        # ── Parse basic fields ──
        title = _clean(entry.get("title", ""))
        link  = _clean(entry.get("link",  ""))
        if not title or not link:
            continue

        # ── Time filter ──
        pub_date = _parse_date(entry)
        if pub_date and pub_date < time_threshold:
            stats["skip_time"] += 1
            continue

        # ── Description ──
        raw_html = (
            entry.get("summary")
            or entry.get("description")
            or ""
        )
        desc = _truncate(
            _strip_html(raw_html),
            MAX_DESCRIPTION_CHARS,
        )

        # ── Election filter ──
        score = _election_score(title, desc, item["is_election_feed"])
        if score < ELECTION_SCORE_PASS:
            stats["skip_elect"] += 1
            print(f"[SKIP:topic] score={score} [{source}] {title[:55]}")
            continue

        # ── Strict duplicate check (link + hash) ──
        content_hash = _make_hash(title, desc)
        if db.is_duplicate(link, content_hash):
            stats["skip_dupe"] += 1
            print(f"[SKIP:dupe:db] [{source}] {title[:55]}")
            continue

        # ── Fuzzy duplicate check (against already-loaded records) ──
        if _is_fuzzy_duplicate(title, recent_records):
            stats["skip_dupe"] += 1
            print(f"[SKIP:dupe:fuzzy] [{source}] {title[:55]}")
            continue

        print(f"[PASS] score={score} [{source}] {title[:55]}")

        # ── Extract images ──
        image_urls = _collect_images(entry, link)

        # ── Build caption ──
        caption = _build_caption(title, desc)

        # ── Post to Telegram ──
        success = await _post_to_telegram(
            bot        = bot,
            chat_id    = config["chat_id"],
            image_urls = image_urls,
            caption    = caption,
        )

        if success:
            stats["posted"] += 1
            print(
                f"[SUCCESS] [{source}] {title[:55]} "
                f"(total posted: {stats['posted']})"
            )

            # Save to DB (after confirmed post)
            db.save(
                link         = link,
                title        = title,
                content_hash = content_hash,
                source       = source,
                created_at   = now.isoformat(),
            )

            # Add to local fuzzy set for this run
            recent_records.append({
                "title":       title,
                "title_norm":  _normalize_text(title),
            })

        else:
            stats["errors"] += 1

        # ── Inter-post delay (Telegram flood protection) ──
        if stats["posted"] > 0:
            await asyncio.sleep(INTER_POST_DELAY)

    # ── Summary ──
    print("\n[INFO] ─────────── SUMMARY ───────────")
    print(f"[INFO] Raw entries  : {stats['fetched']}")
    print(f"[INFO] Skip/time    : {stats['skip_time']}")
    print(f"[INFO] Skip/topic   : {stats['skip_elect']}")
    print(f"[INFO] Skip/dupe    : {stats['skip_dupe']}")
    print(f"[INFO] Posted       : {stats['posted']}")
    print(f"[INFO] Errors       : {stats['errors']}")
    print("[INFO] ──────────────────────────────")

    return {
        "status":  "success",
        "posted":  stats["posted"],
        "skipped": stats["skip_time"] + stats["skip_elect"] + stats["skip_dupe"],
    }


# ═══════════════════════════════════════════════════════════
# SECTION 4 — CONFIG LOADER
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
# SECTION 5 — APPWRITE DATABASE CLIENT
# ═══════════════════════════════════════════════════════════

class _AppwriteDB:
    """
    Raw Appwrite REST client — no SDK dependency.
    Checks: link (exact) + content_hash (SHA256).
    Fuzzy check: loads recent titles for in-memory comparison.
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

    def is_duplicate(self, link: str, content_hash: str) -> bool:
        return (
            self._exists("link",         link[:500])
            or self._exists("content_hash", content_hash)
        )

    def load_recent(self, hours: int = 48) -> list[dict]:
        """
        Load recent records for fuzzy in-memory comparison.
        Returns list of {title, title_norm} dicts.
        """
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
            print(f"[WARN] DB load_recent error: {e}")
            return []

    def save(self, link: str, title: str, content_hash: str,
             source: str, created_at: str) -> bool:
        doc_id = hashlib.md5(link.encode()).hexdigest()[:20]
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
            ok = resp.status_code in (200, 201)
            if ok:
                print("  [DB] Saved.")
            else:
                print(f"  [WARN] DB save {resp.status_code}: {resp.text[:100]}")
            return ok
        except requests.RequestException as e:
            print(f"  [WARN] DB save error: {e}")
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
                found = resp.json().get("total", 0) > 0
                if found:
                    print(f"  [DB] Duplicate by {field}.")
                return found
            return False
        except requests.RequestException as e:
            print(f"  [WARN] DB query error ({field}): {e}")
            return False


# ═══════════════════════════════════════════════════════════
# SECTION 6 — RSS FEED FETCHER
# ═══════════════════════════════════════════════════════════

def _fetch_feed(url: str, source_name: str) -> list:
    """
    Fetch via requests (timeout-safe) + parse with feedparser.
    Returns list of entries or [] on failure.
    """
    try:
        resp = requests.get(
            url,
            timeout=FEED_TIMEOUT,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; ElectionBot/1.0)",
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

        count = len(feed.entries)
        print(f"[FEED] {source_name}: {count} entries")
        return feed.entries

    except requests.RequestException as e:
        print(f"[ERROR] {source_name} fetch: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] {source_name} parse: {e}")
        return []


# ═══════════════════════════════════════════════════════════
# SECTION 7 — TEXT UTILITIES
# ═══════════════════════════════════════════════════════════

def _clean(text: str) -> str:
    return (text or "").strip()


def _strip_html(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "iframe"]):
        tag.decompose()
    return " ".join(soup.get_text(separator=" ").split())


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
    """
    Full Persian normalization for duplicate detection:
      1. Arabic ي/ك → Persian ی/ک
      2. Remove diacritics (harakat)
      3. Normalize whitespace
      4. Lowercase
      5. Remove punctuation
      6. Remove stopwords
    """
    if not text:
        return ""

    # Arabic to Persian character normalization
    text = text.replace("ي", "ی").replace("ك", "ک")
    text = text.replace("ة", "ه").replace("ؤ", "و")
    text = text.replace("إ", "ا").replace("أ", "ا")
    text = text.replace("ئ", "ی").replace("ى", "ی")

    # Remove Arabic diacritics (harakat)
    text = re.sub(r"[\u064B-\u065F\u0670]", "", text)

    # Zero-width characters
    text = re.sub(r"[\u200c\u200d\u200e\u200f\ufeff]", " ", text)

    # Lowercase
    text = text.lower()

    # Remove punctuation
    text = re.sub(r"[^\w\s\u0600-\u06FF]", " ", text)

    # Normalize whitespace
    text = " ".join(text.split())

    # Remove stopwords
    tokens = [
        t for t in text.split()
        if t not in PERSIAN_STOPWORDS and len(t) >= 2
    ]

    return " ".join(tokens)


def _make_hash(title: str, desc: str) -> str:
    """
    SHA256 of normalized title + first 200 chars of normalized description.
    Persian-normalized before hashing for cross-source duplicate detection.
    """
    norm_title = _normalize_text(title)
    norm_desc  = _normalize_text(desc[:200])
    raw        = f"{norm_title} {norm_desc}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ═══════════════════════════════════════════════════════════
# SECTION 8 — ELECTION FILTER (SCORING-BASED)
# ═══════════════════════════════════════════════════════════

def _election_score(
    title: str,
    desc: str,
    is_election_feed: bool,
) -> int:
    """
    Scoring rules:
      - Hard reject keywords in combined → score = -1 (always fail)
      - High-weight keyword in TITLE     → +3 points each
      - High-weight keyword in DESC only → +1 point each
      - Low-weight keyword in TITLE      → +1 point each
      - Low-weight keyword in DESC only  → +0.5 points (rounded)
      - is_election_feed = True          → +1 bonus point

    Threshold: ELECTION_SCORE_PASS (default 2)

    A single high-weight keyword in the title (3pts) always passes.
    Description-only matches need multiple hits.
    """
    norm_title = _normalize_text(title)
    norm_desc  = _normalize_text(desc)
    combined   = norm_title + " " + norm_desc

    # ── Hard reject ──
    for kw in REJECTION_KEYWORDS:
        kw_norm = _normalize_text(kw)
        if kw_norm in combined:
            return -1

    score = 0.0

    # ── High-weight keywords ──
    for kw in ELECTION_KEYWORDS_HIGH:
        kw_norm = _normalize_text(kw)
        if not kw_norm:
            continue
        if kw_norm in norm_title:
            score += 3
        elif kw_norm in norm_desc:
            score += 1

    # ── Low-weight keywords ──
    for kw in ELECTION_KEYWORDS_LOW:
        kw_norm = _normalize_text(kw)
        if not kw_norm:
            continue
        if kw_norm in norm_title:
            score += 1
        elif kw_norm in norm_desc:
            score += 0.5

    # ── Election-feed bonus ──
    if is_election_feed:
        score += 1

    return int(score)


# ═══════════════════════════════════════════════════════════
# SECTION 9 — FUZZY DUPLICATE DETECTION
# ═══════════════════════════════════════════════════════════

def _jaccard_similarity(tokens_a: set[str], tokens_b: set[str]) -> float:
    """Jaccard similarity between two token sets."""
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = len(tokens_a & tokens_b)
    union        = len(tokens_a | tokens_b)
    return intersection / union if union > 0 else 0.0


def _is_fuzzy_duplicate(
    title: str,
    recent_records: list[dict],
) -> bool:
    """
    Compare normalized title tokens against recent records.
    Returns True if any record has Jaccard similarity ≥ threshold.
    This catches same story from different sources with different titles.
    """
    if not recent_records:
        return False

    incoming_norm   = _normalize_text(title)
    incoming_tokens = set(incoming_norm.split())

    if not incoming_tokens:
        return False

    for record in recent_records:
        stored_norm   = record.get("title_norm", "")
        stored_tokens = set(stored_norm.split())
        sim = _jaccard_similarity(incoming_tokens, stored_tokens)
        if sim >= FUZZY_SIMILARITY_THRESHOLD:
            print(
                f"  [FUZZY] sim={sim:.2f} vs: "
                f"{record.get('title', '')[:45]}"
            )
            return True

    return False


# ═══════════════════════════════════════════════════════════
# SECTION 10 — IMAGE COLLECTION
#
# Priority chain (5 methods):
#   1. RSS <media:content medium="image">
#   2. RSS <enclosure type="image/*">
#   3. RSS <media:thumbnail>
#   4. <img> tags inside RSS description HTML
#   5. og:image / twitter:image from article page
# ═══════════════════════════════════════════════════════════

def _collect_images(entry, article_url: str) -> list[str]:
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
            for w in ["image", "photo", "img", "picture", "media",
                      "cdn", "upload", "content"]
        )
        if not has_ext and not has_word:
            return
        seen.add(url)
        images.append(url)

    # 1. media:content
    for m in entry.get("media_content", []):
        url    = m.get("url", "")    if isinstance(m, dict) else getattr(m, "url", "")
        medium = m.get("medium", "") if isinstance(m, dict) else getattr(m, "medium", "")
        if medium == "image" or any(url.lower().endswith(e) for e in IMAGE_EXTENSIONS):
            _add(url)

    # 2. enclosures
    enclosures = entry.get("enclosures", [])
    if not enclosures and hasattr(entry, "enclosure") and entry.enclosure:
        enclosures = [entry.enclosure]
    for enc in enclosures:
        if isinstance(enc, dict):
            mime = enc.get("type", "")
            href = enc.get("href") or enc.get("url", "")
        else:
            mime = getattr(enc, "type", "")
            href = getattr(enc, "href", "") or getattr(enc, "url", "")
        if mime.startswith("image/") and href:
            _add(href)

    # 3. media:thumbnail
    for t in entry.get("media_thumbnail", []):
        url = t.get("url", "") if isinstance(t, dict) else getattr(t, "url", "")
        _add(url)

    # 4. <img> in description HTML
    if len(images) < MAX_IMAGES:
        raw_html = (
            entry.get("summary")
            or entry.get("description")
            or (entry.get("content") or [{}])[0].get("value", "")
        )
        if raw_html:
            soup = BeautifulSoup(raw_html, "lxml")
            for img_tag in soup.find_all("img"):
                for attr in ("src", "data-src", "data-lazy-src", "data-original"):
                    src = img_tag.get(attr, "")
                    if src and src.startswith("http"):
                        _add(src)
                        break
                if len(images) >= MAX_IMAGES:
                    break

    # 5. og:image fallback
    if not images:
        og = _fetch_og_image(article_url)
        if og:
            _add(og)

    result = images[:MAX_IMAGES]
    print(f"  [INFO] Images: {len(result)}")
    return result


def _fetch_og_image(url: str) -> str | None:
    try:
        resp = requests.get(
            url,
            timeout=PAGE_TIMEOUT,
            headers={"User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36"
            )},
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
# SECTION 11 — CAPTION BUILDER
#
# EXACT format from original (unchanged):
#
#   💠 <b>Title</b>
#
#   @candidatoryiran
#
#   Summary text
#
#   🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷
#   کانال خبری کاندیداتوری
#   🆔 @candidatoryiran
#   🆔 Instagram.com/candidatory.ir
# ═══════════════════════════════════════════════════════════

def _build_caption(title: str, desc: str) -> str:
    safe_title = _escape_html(title.strip())
    safe_desc  = _escape_html(desc.strip())

    caption = (
        f"💠 <b>{safe_title}</b>\n\n"
        f"@candidatoryiran\n\n"
        f"{safe_desc}\n\n"
        f"🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷\n"
        f"کانال خبری کاندیداتوری\n"
        f"🆔 @candidatoryiran\n"
        f"🆔 Instagram.com/candidatory.ir"
    )

    # Trim description if over Telegram message limit
    if len(caption) > CAPTION_MAX:
        overflow  = len(caption) - CAPTION_MAX
        safe_desc = safe_desc[:max(0, len(safe_desc) - overflow - 5)] + "…"
        caption   = (
            f"💠 <b>{safe_title}</b>\n\n"
            f"@candidatoryiran\n\n"
            f"{safe_desc}\n\n"
            f"🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷🇮🇷\n"
            f"کانال خبری کاندیداتوری\n"
            f"🆔 @candidatoryiran\n"
            f"🆔 Instagram.com/candidatory.ir"
        )

    return caption


# ═══════════════════════════════════════════════════════════
# SECTION 12 — TELEGRAM POSTING
#
# ORDER GUARANTEE via reply_to_message_id:
#
#   ① send_media_group(all images, NO caption)
#      → anchor_id = last_message.message_id
#   ② asyncio.sleep(ALBUM_CAPTION_DELAY = 2.5s)
#   ③ send_message(caption, reply_to=anchor_id)
#      → protocol-level order guarantee
#
# EDGE CASES:
#   ≥2 images → send_media_group → anchor → reply caption
#    1 image  → send_photo (no caption) → anchor → reply caption
#    0 images → standalone send_message
#
# FALLBACK CHAIN:
#   send_media_group fails → try send_photo(images[0])
#   send_photo fails       → proceed without anchor
#   send_message fails     → return False
# ═══════════════════════════════════════════════════════════

async def _post_to_telegram(
    bot:        Bot,
    chat_id:    str,
    image_urls: list[str],
    caption:    str,
) -> bool:
    """
    Full post sequence. Returns True only if caption was delivered.
    """
    anchor_msg_id: int | None = None

    # ── Step ①: Send images (no caption) ──
    if len(image_urls) >= 2:
        try:
            media_group = [
                InputMediaPhoto(media=url)
                for url in image_urls[:MAX_IMAGES]
            ]
            sent_list = await bot.send_media_group(
                chat_id=chat_id,
                media=media_group,
                disable_notification=True,
            )
            anchor_msg_id = sent_list[-1].message_id
            print(
                f"  [INFO] ① Album: {len(sent_list)} images. "
                f"anchor={anchor_msg_id}"
            )
        except TelegramError as e:
            print(f"  [WARN] ① Album failed: {e}")
            if image_urls:
                try:
                    sent = await bot.send_photo(
                        chat_id=chat_id,
                        photo=image_urls[0],
                        disable_notification=True,
                    )
                    anchor_msg_id = sent.message_id
                    print(f"  [INFO] ① Fallback photo. anchor={anchor_msg_id}")
                except TelegramError as e2:
                    print(f"  [WARN] ① Fallback also failed: {e2}")

    elif len(image_urls) == 1:
        try:
            sent = await bot.send_photo(
                chat_id=chat_id,
                photo=image_urls[0],
                disable_notification=True,
            )
            anchor_msg_id = sent.message_id
            print(f"  [INFO] ① Single photo. anchor={anchor_msg_id}")
        except TelegramError as e:
            print(f"  [WARN] ① Single photo failed: {e}")

    else:
        print("  [INFO] ① No images — caption standalone.")

    # ── Step ②: Hard delay ──
    if anchor_msg_id is not None:
        await asyncio.sleep(ALBUM_CAPTION_DELAY)

    # ── Step ③: Send caption (reply to anchor) ──
    try:
        kwargs: dict = {
            "chat_id":              chat_id,
            "text":                 caption,
            "parse_mode":           "HTML",
            "link_preview_options": LinkPreviewOptions(is_disabled=True),
            "disable_notification": True,
        }
        if anchor_msg_id is not None:
            kwargs["reply_to_message_id"] = anchor_msg_id

        await bot.send_message(**kwargs)

        label = (
            f"reply_to={anchor_msg_id}"
            if anchor_msg_id is not None else "standalone"
        )
        print(f"  [INFO] ③ Caption sent ({label}).")
        return True

    except TelegramError as e:
        print(f"  [ERROR] ③ Caption failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    asyncio.run(main())