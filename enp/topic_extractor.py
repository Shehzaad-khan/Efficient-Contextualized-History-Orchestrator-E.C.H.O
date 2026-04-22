from __future__ import annotations

from collections import Counter
from html import unescape
import json
import re
from typing import Any
from urllib.parse import urlparse


TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_+-]{2,}")
NON_WORD_RE = re.compile(r"[^a-z0-9]+")

STOPWORDS = {
    "about", "after", "again", "been", "before", "being", "below", "between",
    "could", "does", "doing", "from", "have", "having", "into", "itself",
    "just", "more", "most", "other", "ours", "same", "should", "some", "such",
    "than", "that", "their", "theirs", "them", "then", "there", "these", "they",
    "this", "those", "through", "under", "until", "very", "what", "when", "where",
    "which", "while", "with", "would", "your", "yours", "subject", "http", "https",
    "www", "com", "mail", "reply", "forward", "video", "page",
}

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
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return " ".join(_coerce_text(item) for item in value)
    if isinstance(value, dict):
        return " ".join(_coerce_text(item) for item in value.values())
    return str(value)


def extract_keywords(text: str, limit: int = 8) -> list[str]:
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


def extract_thread_keywords(subject: str, clean_body: str, message_history: Any = None, limit: int = 6) -> list[str]:
    history_text = _coerce_text(message_history)
    combined = _join_parts([subject, clean_body, history_text])
    return extract_keywords(combined, limit=limit)


def youtube_category_hint(category_id: int | str | None) -> str:
    if category_id is None or category_id == "":
        return ""
    try:
        return YOUTUBE_CATEGORY_HINTS.get(int(category_id), "")
    except (TypeError, ValueError):
        return ""


def build_embeddable_text(item: dict[str, Any], cleaned_text: str, headings: list[str] | None = None) -> tuple[str, list[str]]:
    source = (item.get("source") or item.get("source_type") or "").lower()
    headings = headings or []

    if source == "gmail":
        keywords = extract_thread_keywords(
            item.get("subject") or item.get("title") or "",
            cleaned_text,
            item.get("message_history"),
        )
        sender_hint = sender_domain_hint(item.get("sender", ""))
        embeddable = _join_parts([
            item.get("subject") or item.get("title") or "",
            cleaned_text,
            " ".join(keywords),
            sender_hint,
        ])
        return embeddable, keywords

    if source == "chrome":
        domain = domain_hint(item.get("domain") or item.get("canonical_url") or item.get("url") or "")
        snippet = cleaned_text[:500]
        base = _join_parts([
            item.get("title", ""),
            " ".join(headings),
            snippet,
            domain,
        ])
        return base, extract_keywords(base)

    if source == "youtube":
        transcript = (item.get("transcript_text") or "")[:500]
        category = youtube_category_hint(item.get("youtube_category_id"))
        embeddable = _join_parts([
            item.get("title", ""),
            item.get("description", ""),
            item.get("channel_name", ""),
            category,
            transcript,
        ])
        return embeddable, extract_keywords(embeddable)

    fallback = _join_parts([item.get("title", ""), cleaned_text])
    return fallback, extract_keywords(fallback)


def parse_message_history(value: Any) -> Any:
    if not value:
        return None
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(unescape(value))
        except json.JSONDecodeError:
            return value
    return value
