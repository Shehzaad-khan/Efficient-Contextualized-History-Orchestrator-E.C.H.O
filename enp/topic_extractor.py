"""
topic_extractor.py — embeddable text + auto_keywords (architecture §5.2, §5.3, §6.4, §7, §9.1; ENP module)
Echo Personal Memory System

Caller: enrichment_pipeline.prepare_item_for_embedding ->
build_embeddable_text(item, clean_text, headings) -> (parts, auto_keywords), then
embedding_generator.fit_to_window(parts) -> the embeddable_text.

parts = ordered TextPart(text, max_tokens) list. This file stays pure (no model):
it only says what goes in which order and how many tokens a long part may take
while later parts still need room. The cutting is done with the model's own
tokenizer in fit_to_window, and the text it returns is what is stored in
embedding_index for auditing, exactly as embedded.
Same parts as the doc, short hints first, long text last (A16):
  gmail    subject + keywords (own + thread, §5.3) + sender domain + filenames
           + body (up to 300 tokens) + attachments (fill the rest)        (doc §5.2)
  chrome   title + headings + domain + page text (fills the rest)         (doc §6.4)
  youtube  title + channel + category + description (up to 200) + transcript

Why this order: the model reads only its window (512 tokens for arctic-embed-xs)
and silently drops the rest. In the doc's order a long body pushed the hints after
it (thread keywords, sender, channel, category) out of that window. Order does
not otherwise matter: the model pools over all tokens.

auto_keywords (memory_items.auto_keywords; RSE matches queries against them):
the most frequent non-stopword words of the FULL text (not the cut one), ties
broken alphabetically. A heuristic, not authoritative (DB doc).

Pure functions, no shared state: safe from any thread. No logger (nothing here
can fail on valid input; the pipeline logs per item).
"""

from __future__ import annotations

from collections import Counter
import re
from typing import Any, NamedTuple
from urllib.parse import urlparse

from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

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


class TextPart(NamedTuple):
    """One piece of an item's embeddable text, in order.

    max_tokens None = keep whole (a short hint). A number = this long part may
    use at most that many tokens while later parts still need room; unused room
    flows back to it (embedding_generator.fit_to_window).
    """

    text: str
    max_tokens: int | None

# Token shares (A16), counted with the model's tokenizer in fit_to_window. A cap
# applies only while later parts need room; room left over flows back.
GMAIL_BODY_MAX_TOKENS = 300
# All attachments of a mail share this pool, split evenly, so each attachment's
# start gets in (A4). 210 = 510-token window - GMAIL_BODY_MAX_TOKENS.
GMAIL_ATTACHMENTS_POOL_TOKENS = 210
YOUTUBE_DESCRIPTION_MAX_TOKENS = 200

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


_FILENAME_SEPARATORS_RE = re.compile(r"[_\-.]+")


def _filename_words(filenames: list[str]) -> str:
    """Words of attachment filenames for keyword extraction, without the extension:
    "Offer_Letter_2026.pdf" -> "Offer Letter 2026". Left raw, the underscores glue
    the name into one useless word and the extension becomes a keyword like "pdf"."""
    words = []
    for name in filenames:
        stem = name.rsplit(".", 1)[0] if "." in name else name
        words.append(_FILENAME_SEPARATORS_RE.sub(" ", stem))
    return " ".join(words)


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


def gmail_keywords(
    subject: str,
    clean_body: str,
    message_history: Any = None,
    attachment_filenames: list[str] | None = None,
) -> list[str]:
    """The email's own keywords, then up to 4 new ones from earlier thread
    messages (Thread Context Enrichment, doc §5.3).

    Counted separately: in one shared count a long reply outvotes its thread,
    and the thread's topic never makes it in. With them apart, a bare "Thanks,
    looks good" reply still carries [os, scheduling, project] from its thread.
    """
    attachment_text = _filename_words(attachment_filenames or [])
    own = extract_keywords(_join_parts([subject, clean_body, attachment_text]), limit=GMAIL_OWN_KEYWORD_LIMIT)
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


def _attachment_parts(item: dict[str, Any]) -> list[TextPart]:
    """One part per attachment that has text, each with an even share of the pool."""
    texts = [str(entry.get("text") or "") for entry in item.get("attachment_texts") or []]
    texts = [text for text in texts if text.strip()]
    if not texts:
        return []
    share = max(1, GMAIL_ATTACHMENTS_POOL_TOKENS // len(texts))
    return [TextPart(text, share) for text in texts]


def build_embeddable_text(
    item: dict[str, Any], cleaned_text: str, headings: list[str] | None = None
) -> tuple[list[TextPart], list[str]]:
    """Return (ordered parts of the embeddable text, auto_keywords) for one item, by source.

    The parts still have to go through embedding_generator.fit_to_window.
    """
    source = (item.get("source") or item.get("source_type") or "").lower()
    headings = headings or []

    if source == "gmail":
        subject = item.get("subject") or item.get("title") or ""
        attachment_filenames = item.get("attachment_filenames") or []
        keywords = gmail_keywords(
            subject,
            cleaned_text,
            item.get("message_history"),
            attachment_filenames=attachment_filenames,
        )
        parts = [
            TextPart(subject, None),
            TextPart(" ".join(keywords), None),
            TextPart(sender_domain_hint(item.get("sender", "")), None),
            TextPart(" ".join(attachment_filenames), None),
            TextPart(cleaned_text, GMAIL_BODY_MAX_TOKENS),
            *_attachment_parts(item),
        ]
        return parts, keywords

    if source == "chrome":
        title = item.get("title", "")
        heading_text = " ".join(headings)
        parts = [
            TextPart(title, None),
            TextPart(heading_text, None),
            TextPart(domain_hint(item.get("domain") or item.get("canonical_url") or item.get("url") or ""), None),
            TextPart(cleaned_text, None),
        ]
        return parts, extract_keywords(_join_parts([title, heading_text, cleaned_text]))

    if source == "youtube":
        title = item.get("title", "")
        description = item.get("description", "")
        transcript = item.get("transcript_text") or ""
        parts = [
            TextPart(title, None),
            TextPart(item.get("channel_name", ""), None),
            TextPart(youtube_category_hint(item.get("youtube_category_id")), None),
            TextPart(description, YOUTUBE_DESCRIPTION_MAX_TOKENS),
            TextPart(transcript, None),
        ]
        return parts, extract_keywords(_join_parts([title, description, transcript]))

    title = item.get("title", "")
    return [TextPart(title, None), TextPart(cleaned_text, None)], extract_keywords(_join_parts([title, cleaned_text]))
