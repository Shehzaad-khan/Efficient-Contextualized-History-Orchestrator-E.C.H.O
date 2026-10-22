"""
text_cleaner.py — text cleaning before embedding (architecture §5.2, §6.4, §9.1; ENP module)
Echo Personal Memory System

Caller: enrichment_pipeline.prepare_item_for_embedding -> clean_item_text(item).
Returns CleanedContent(clean_text, headings); topic_extractor builds the
embeddable text from it (and does its own length capping with truncate()).

What arrives here (raw_text as stored by each connector):
  gmail    plain text. ingestion/gmail/gmail_api.py already turned HTML into text
           (tags, <head>, tracking pixels and invisible characters gone, line
           breaks kept). Rows stored before that change may still hold HTML,
           so HTML is detected and converted here too.
  chrome   plain text. The extension sends the innerText of <article>/<main>/
           <body> with all whitespace collapsed, so no tags and no headings
           reach the backend. The Readability branch runs only if a row really
           holds HTML (decision A13).
  youtube  title + description (+ transcript, empty today — decision A11).

Steps per source:
  gmail    [HTML -> text] -> drop quoted replies -> normalize
  chrome   [HTML -> Readability -> text + headings] or normalize
  youtube  join title, description, transcript -> normalize

Tracking pixels (doc §5.2) need no step of their own: an <img> holds no text,
and stripping tags removes it (here and in gmail_api).

Pure functions, no shared state: safe from any thread. Logs at DEBUG only, and
never logs content.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from html import unescape
import logging
import re
from typing import Any

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - dependency-driven
    BeautifulSoup = None

try:
    from readability import Document
except ImportError:  # pragma: no cover - dependency-driven
    Document = None

logger = logging.getLogger(__name__)

SNIPPET_LIMIT = 500

# Real HTML markup, not just a "<" in prose ("if a < b and b > c").
HTML_SNIFF_RE = re.compile(
    r"<(?:html|head|body|div|p|br|span|table|tr|td|a|img|h[1-6]|ul|ol|li|article|main|section)\b[^>]*>",
    flags=re.IGNORECASE,
)
HTML_TAG_RE = re.compile(r"<[^>]+>")
# \b so <header> is not taken for <head>.
DROP_BLOCK_RE = re.compile(r"<(script|style|noscript|head|title)\b[^>]*>.*?</\1\s*>", flags=re.IGNORECASE | re.DOTALL)
COMMENT_RE = re.compile(r"<!--.*?-->", flags=re.DOTALL)
# Tags that end a visual line. Same set as gmail_api, so both produce the
# line structure the quote-removal step relies on.
LINE_BREAK_TAG_RE = re.compile(
    r"<br\s*/?>|</(?:p|div|tr|li|h[1-6]|table|blockquote|ul|ol)\s*>|<(?:hr|li)\b[^>]*>",
    flags=re.IGNORECASE,
)
# Zero-width and other invisible characters (email preheader padding, soft
# hyphens). Same set as gmail_api._INVISIBLE_RE.
INVISIBLE_RE = re.compile("[­͏​-‏⁠﻿]")
# Any whitespace except a newline: spaces, tabs, non-breaking spaces (&nbsp;).
INLINE_SPACE_RE = re.compile(r"[^\S\n]+")
MULTINEWLINE_RE = re.compile(r"\n{3,}")

# "On <date>, <name> <address> wrote:" on one line (Apple Mail, Gmail when short) ...
REPLY_HEADER_RE = re.compile(r"^on .+wrote:$", flags=re.IGNORECASE)
# ... or wrapped by Gmail's ~78-char plain-text line limit, with "wrote:" alone
# on the next line.
REPLY_HEADER_START_RE = re.compile(r"^on .+", flags=re.IGNORECASE)
REPLY_HEADER_END_RE = re.compile(r"^(?:.+ )?wrote:$", flags=re.IGNORECASE)


@dataclass(slots=True)
class CleanedContent:
    clean_text: str
    headings: list[str] = field(default_factory=list)


def normalize_text(text: str) -> str:
    """Unify line endings, drop invisible characters, collapse runs of spaces
    (incl. &nbsp;) and of blank lines. Line breaks themselves are kept."""
    # "\r\n" first: replacing a lone "\r" before it would turn every Windows
    # line ending into a blank line.
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = INVISIBLE_RE.sub("", text)
    lines = [INLINE_SPACE_RE.sub(" ", line).strip() for line in text.split("\n")]
    return MULTINEWLINE_RE.sub("\n\n", "\n".join(lines)).strip()


def truncate(text: str, limit: int = SNIPPET_LIMIT) -> str:
    """Normalized text cut to `limit` chars at the last word boundary."""
    value = normalize_text(text)
    if len(value) <= limit:
        return value
    return value[:limit].rsplit(" ", 1)[0].strip()


def looks_like_html(text: str) -> bool:
    return bool(text) and HTML_SNIFF_RE.search(text) is not None


def html_to_text(html: str) -> str:
    """HTML -> readable text with its line structure kept."""
    if not html:
        return ""
    stripped = COMMENT_RE.sub(" ", html)  # includes Outlook's <!--[if mso]> blocks
    stripped = DROP_BLOCK_RE.sub(" ", stripped)
    stripped = LINE_BREAK_TAG_RE.sub("\n", stripped)
    stripped = HTML_TAG_RE.sub(" ", stripped)  # also drops <img> tracking pixels
    return normalize_text(unescape(stripped))


def _reply_header_end(lines: list[str], index: int) -> int | None:
    """Index just after the "On ... wrote:" header that starts at `index`, or None."""
    stripped = lines[index].strip()
    if REPLY_HEADER_RE.match(stripped):
        return index + 1
    if (
        REPLY_HEADER_START_RE.match(stripped)
        and index + 1 < len(lines)
        and REPLY_HEADER_END_RE.match(lines[index + 1].strip())
    ):
        return index + 2
    return None


def _next_reply_header(lines: list[str], start: int) -> int:
    """Index of the next reply header at or after `start`, or len(lines) if none."""
    for index in range(start, len(lines)):
        if _reply_header_end(lines, index) is not None:
            return index
    return len(lines)


def remove_quoted_replies(text: str) -> str:
    """Drop quoted lines and the reply header when its block has quote markers.

    Keep unquoted inline answers below an "On ... wrote:" header. A header's
    block runs to the next reply header (or the end). If no quoted lines are in
    that block, treat the remainder as an unmarked old thread and cut it as before.

    A "Forwarded message" block is kept: its content exists nowhere else.
    """
    if not text:
        return ""

    lines = text.splitlines()
    cleaned_lines: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if line.strip().startswith(">"):
            index += 1
            continue

        header_end = _reply_header_end(lines, index)
        if header_end is not None:
            block_end = _next_reply_header(lines, header_end)
            has_quoted_lines = any(candidate.strip().startswith(">") for candidate in lines[header_end:block_end])
            if not has_quoted_lines:
                break
            index = header_end
            continue

        cleaned_lines.append(line)
        index += 1
    return "\n".join(cleaned_lines)


def clean_gmail_text(raw_text: str) -> CleanedContent:
    text = raw_text or ""
    if looks_like_html(text):
        logger.debug("Gmail body still holds HTML (pre-connector-fix row); converting")
        text = html_to_text(text)
    else:
        # Before the quote step: a zero-width character in front of ">" or
        # "On ... wrote:" would otherwise hide the marker from remove_quoted_replies.
        text = normalize_text(text)
    no_quotes = remove_quoted_replies(text)
    cleaned = normalize_text(no_quotes)
    return CleanedContent(clean_text=cleaned)


def _extract_headings_from_soup(soup: Any) -> list[str]:
    headings: list[str] = []
    for tag in soup.find_all(re.compile("^h[1-6]$")):
        heading = normalize_text(tag.get_text(" ", strip=True))
        if heading:
            headings.append(heading)
    return headings


def _fallback_extract_article_html(raw_html: str) -> str:
    """Readability stand-in: drop page chrome, keep the node with the most text."""
    if not raw_html:
        return ""

    if BeautifulSoup is None:
        return raw_html

    soup = BeautifulSoup(raw_html, "html.parser")
    for tag in soup(["script", "style", "noscript", "nav", "aside", "footer", "header", "form", "button", "svg"]):
        tag.decompose()

    best_node = None
    best_score = -1
    candidates = soup.find_all(["article", "main", "section", "div", "body"])
    for node in candidates:
        text = normalize_text(node.get_text(" ", strip=True))
        if not text:
            continue
        score = len(text) + (len(node.find_all("p")) * 50) + (len(node.find_all(re.compile("^h[1-6]$"))) * 25)
        if score > best_score:
            best_node = node
            best_score = score

    return str(best_node or soup)


def clean_chrome_text(raw_text: str) -> CleanedContent:
    if not raw_text:
        return CleanedContent(clean_text="")

    if not looks_like_html(raw_text):
        # The normal case: the extension already sent the main text.
        return CleanedContent(clean_text=normalize_text(raw_text))

    if Document is not None:
        try:
            article_html = Document(raw_text).summary(html_partial=True)
        except Exception as exc:
            logger.debug("Readability failed (%s); using the fallback extractor", type(exc).__name__)
            article_html = _fallback_extract_article_html(raw_text)
    else:
        article_html = _fallback_extract_article_html(raw_text)

    if BeautifulSoup is None:
        return CleanedContent(clean_text=html_to_text(article_html))

    soup = BeautifulSoup(article_html, "html.parser")
    for tag in soup(["script", "style", "noscript", "form", "button", "iframe", "svg"]):
        tag.decompose()

    headings = _extract_headings_from_soup(soup)
    clean_text = normalize_text(soup.get_text("\n", strip=True))
    return CleanedContent(clean_text=clean_text, headings=headings)


def clean_youtube_text(title: str = "", description: str = "", transcript: str = "") -> CleanedContent:
    combined = " ".join(part.strip() for part in (title, description, transcript) if part and part.strip())
    return CleanedContent(clean_text=normalize_text(combined))


def clean_item_text(item: dict[str, Any]) -> CleanedContent:
    """Clean one memory item by its source. Unknown sources: normalize only."""
    source = (item.get("source") or item.get("source_type") or "").lower()

    if source == "gmail":
        raw_body = item.get("content_primary_text") or item.get("raw_text") or ""
        return clean_gmail_text(raw_body)

    if source == "chrome":
        raw_text = item.get("raw_html") or item.get("raw_text") or ""
        return clean_chrome_text(raw_text)

    if source == "youtube":
        return clean_youtube_text(
            title=item.get("title", ""),
            description=item.get("description", ""),
            transcript=item.get("transcript_text", ""),
        )

    return CleanedContent(clean_text=normalize_text(str(item.get("raw_text") or item.get("title") or "")))
