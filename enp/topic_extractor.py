"""
topic_extractor.py — embeddable text + auto_keywords (architecture §5.2, §5.3, §6.4, §7, §9.1; ENP module)
Echo Personal Memory System

Caller: enrichment_pipeline.prepare_item_for_embedding ->
build_embeddable_text(item, clean_text, headings) -> (embeddable_text, auto_keywords).

embeddable_text (what MiniLM embeds; also stored in embedding_index for auditing).
Same parts as the doc, short hints first, long text last:
  gmail    subject + keywords (own + thread, §5.3) + sender domain + body   (doc §5.2)
  chrome   title + headings + domain + content snippet                     (doc §6.4)
  youtube  title + channel + category + description + transcript snippet   (doc §7)

Why this order: all-MiniLM-L6-v2 reads only the first 256 tokens and silently
drops the rest. In the doc's order a long body pushed the hints after it
(thread keywords, sender, channel, category) out of that window. Order does
not otherwise matter: the model averages over all tokens (mean pooling).
The long parts are also capped (~5 chars per token on English prose), so the
stored text stays close to what the model actually saw.

auto_keywords (memory_items.auto_keywords; RSE matches queries against them):
the most frequent non-stopword words of the FULL text (not the cut one), ties
broken alphabetically. A heuristic, not authoritative (DB doc).

Pure functions, no shared state: safe from any thread. No logger (nothing here
can fail on valid input; the pipeline logs per item).
"""

from __future__ import annotations

from collections import Counter
import re
from typing import Any
from urllib.parse import urlparse

from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

from enp.text_cleaner import truncate

TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_+-]{2,}")
NON_WORD_RE = re.compile(r"[^a-z0-9]+")

# scikit-learn's 318 English stopwords ("the", "and", "for", "you", ...) plus
# words that appear in nearly every item of one source and so say nothing.
STOPWORDS = frozenset(ENGLISH_STOP_WORDS) | {
    "subject", "http", "https", "www", "com", "mail", "reply", "forward", "fwd",
    "video", "page", "thanks", "regards", "dear", "hello", "please",
}

KEYWORD_LIMIT = 8
GMAIL_OWN_KEYWORD_LIMIT = 6
THREAD_KEYWORD_LIMIT = 4

# Character caps that keep a normal embeddable_text inside MiniLM's 256 tokens.
# Dense text (code, IDs) can still overflow; then only the end of the long part
# is lost, because the hints come first.
GMAIL_BODY_CHARS = 800
CHROME_SNIPPET_CHARS = 500
YOUTUBE_DESCRIPTION_CHARS = 600
YOUTUBE_TRANSCRIPT_CHARS = 400
FALLBACK_BODY_CHARS = 800

YOUTUBE_CATEGORY_HINTS = {
    1: "film animation",
    2: "autos vehicles",
    10: "music",
    15: "pets animals",
    17: "sports",
    19: "travel events",
    20: "gaming",
    22: "people blogs",
    23: "comedy",
    24: "entertainment",
    25: "news politics",
    26: "howto style",
    27: "education learning tutorial",
    28: "science technology",
}


def _join_parts(parts: list[str]) -> str:
    return " ".join(part.strip() for part in parts if part and part.strip())


def _coerce_text(value: Any) -> str:
    """Text from a str, or from a list / dict of them (message_history)."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return " ".join(_coerce_text(item) for item in value)
    if isinstance(value, dict):
        return " ".join(_coerce_text(item) for item in value.values())
    return str(value)


def extract_keywords(text: str, limit: int = KEYWORD_LIMIT) -> list[str]:
    """Most frequent words of 3+ letters that are not stopwords or numbers.

    ASCII words only: "c++" and non-Latin scripts yield no keywords.
    """
    tokens = []
    for token in TOKEN_RE.findall((text or "").lower()):
        normalized = NON_WORD_RE.sub("", token)
        if len(normalized) < 3 or normalized in STOPWORDS or normalized.isdigit():
            continue
        tokens.append(normalized)

    if not tokens:
        return []

    counts = Counter(tokens)
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [keyword for keyword, _ in ranked[:limit]]


def sender_domain_hint(sender: str) -> str:
    """'Priya <priya@Acme.io>' -> 'acme.io'; '' when there is no address."""
    if not sender:
        return ""
    match = re.search(r"<([^>]+)>", sender)
    email = match.group(1) if match else sender
    if "@" not in email:
        return ""
    return email.split("@", 1)[1].strip().lower()


def domain_hint(domain_or_url: str) -> str:
    if not domain_or_url:
        return ""
    if "://" in domain_or_url:
        return urlparse(domain_or_url).netloc.lower()
    return domain_or_url.strip().lower()


def gmail_keywords(subject: str, clean_body: str, message_history: Any = None) -> list[str]:
    """The email's own keywords, then up to 4 new ones from earlier thread
    messages (Thread Context Enrichment, doc §5.3).

    Counted separately: in one shared count a long reply outvotes its thread,
    and the thread's topic never makes it in. With them apart, a bare "Thanks,
    looks good" reply still carries [os, scheduling, project] from its thread.
    """
    own = extract_keywords(_join_parts([subject, clean_body]), limit=GMAIL_OWN_KEYWORD_LIMIT)
    thread = [
        keyword
        for keyword in extract_keywords(_coerce_text(message_history), limit=GMAIL_OWN_KEYWORD_LIMIT + THREAD_KEYWORD_LIMIT)
        if keyword not in own
    ][:THREAD_KEYWORD_LIMIT]
    return own + thread


def youtube_category_hint(category_id: int | str | None) -> str:
    if category_id is None or category_id == "":
        return ""
    try:
        return YOUTUBE_CATEGORY_HINTS.get(int(category_id), "")
    except (TypeError, ValueError):
        return ""


def build_embeddable_text(item: dict[str, Any], cleaned_text: str, headings: list[str] | None = None) -> tuple[str, list[str]]:
    """Return (embeddable_text, auto_keywords) for one item, by source."""
    source = (item.get("source") or item.get("source_type") or "").lower()
    headings = headings or []

    if source == "gmail":
        subject = item.get("subject") or item.get("title") or ""
        keywords = gmail_keywords(subject, cleaned_text, item.get("message_history"))
        embeddable = _join_parts([
            subject,
            " ".join(keywords),
            sender_domain_hint(item.get("sender", "")),
            truncate(cleaned_text, GMAIL_BODY_CHARS),
        ])
        return embeddable, keywords

    if source == "chrome":
        title = item.get("title", "")
        heading_text = " ".join(headings)
        embeddable = _join_parts([
            title,
            heading_text,
            domain_hint(item.get("domain") or item.get("canonical_url") or item.get("url") or ""),
            truncate(cleaned_text, CHROME_SNIPPET_CHARS),
        ])
        return embeddable, extract_keywords(_join_parts([title, heading_text, cleaned_text]))

    if source == "youtube":
        title = item.get("title", "")
        description = item.get("description", "")
        transcript = item.get("transcript_text") or ""
        embeddable = _join_parts([
            title,
            item.get("channel_name", ""),
            youtube_category_hint(item.get("youtube_category_id")),
            truncate(description, YOUTUBE_DESCRIPTION_CHARS),
            truncate(transcript, YOUTUBE_TRANSCRIPT_CHARS),
        ])
        return embeddable, extract_keywords(_join_parts([title, description, transcript]))

    title = item.get("title", "")
    fallback = _join_parts([title, truncate(cleaned_text, FALLBACK_BODY_CHARS)])
    return fallback, extract_keywords(_join_parts([title, cleaned_text]))
