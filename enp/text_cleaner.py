from __future__ import annotations

from dataclasses import dataclass, field
from html import unescape
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


HTML_TAG_RE = re.compile(r"<[^>]+>")
TRACKING_PIXEL_RE = re.compile(
    r"<img\b[^>]*(?:width=['\"]?1['\"]?|height=['\"]?1['\"]?|display\s*:\s*none|visibility\s*:\s*hidden)[^>]*>",
    flags=re.IGNORECASE,
)
SCRIPT_STYLE_RE = re.compile(r"<(script|style|noscript)[^>]*>.*?</\1>", flags=re.IGNORECASE | re.DOTALL)
COMMENT_RE = re.compile(r"<!--.*?-->", flags=re.DOTALL)
MULTISPACE_RE = re.compile(r"[ \t]+")
MULTINEWLINE_RE = re.compile(r"\n{3,}")

SIGNATURE_MARKERS = (
    "best regards",
    "thanks",
    "thank you",
    "regards",
    "warm regards",
    "kind regards",
    "cheers",
    "sincerely",
    "sent from my",
)
REPLY_HEADER_RE = re.compile(r"^on .+wrote:$", flags=re.IGNORECASE)
EMAIL_ONLY_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", flags=re.IGNORECASE)
NAME_ONLY_RE = re.compile(r"^[A-Za-z][A-Za-z .'-]{1,50}$")


@dataclass(slots=True)
class CleanedContent:
    clean_text: str
    headings: list[str] = field(default_factory=list)
    snippet: str = ""


def normalize_text(text: str) -> str:
    text = text.replace("\r", "\n")
    text = MULTISPACE_RE.sub(" ", text)
    text = MULTINEWLINE_RE.sub("\n\n", text)
    return text.strip()


def truncate(text: str, limit: int = 500) -> str:
    value = normalize_text(text)
    if len(value) <= limit:
        return value
    return value[:limit].rsplit(" ", 1)[0].strip()


def html_to_text(html: str) -> str:
    if not html:
        return ""
    stripped = COMMENT_RE.sub(" ", html)
    stripped = SCRIPT_STYLE_RE.sub(" ", stripped)
    stripped = re.sub(r"(?i)<br\s*/?>", "\n", stripped)
    stripped = re.sub(r"(?i)</p>", "\n", stripped)
    stripped = re.sub(r"(?i)</div>", "\n", stripped)
    stripped = HTML_TAG_RE.sub(" ", stripped)
    return normalize_text(unescape(stripped))


def remove_tracking_pixels(html: str) -> str:
    if not html:
        return ""

    if BeautifulSoup is None:
        return TRACKING_PIXEL_RE.sub(" ", html)

    soup = BeautifulSoup(html, "html.parser")
    for img in soup.find_all("img"):
        width = (img.get("width") or "").strip()
        height = (img.get("height") or "").strip()
        style = (img.get("style") or "").lower()
        hidden = "display:none" in style.replace(" ", "") or "visibility:hidden" in style.replace(" ", "")
        if width == "1" or height == "1" or hidden:
            img.decompose()
    return str(soup)


def remove_quoted_replies(text: str) -> str:
    if not text:
        return ""

    cleaned_lines: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith(">"):
            continue
        if REPLY_HEADER_RE.match(stripped):
            break
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines)


def _looks_like_signature_line(line: str) -> bool:
    lowered = line.strip().lower()
    if not lowered:
        return False
    if lowered in SIGNATURE_MARKERS:
        return True
    if lowered.startswith(SIGNATURE_MARKERS):
        return True
    if EMAIL_ONLY_RE.match(line.strip()):
        return True
    if NAME_ONLY_RE.match(line.strip()) and len(line.split()) <= 4:
        return True
    return False


def remove_email_signature(text: str) -> str:
    if not text:
        return ""

    lines = [line.rstrip() for line in text.splitlines()]
    signature_start: int | None = None

    for index, line in enumerate(lines):
        if _looks_like_signature_line(line):
            # Only treat it as a signature if it appears in the tail of the mail.
            if index >= max(1, len(lines) - 6):
                signature_start = index
                break

    if signature_start is None:
        return "\n".join(lines)

    return "\n".join(lines[:signature_start]).rstrip()


def clean_gmail_text(raw_text: str) -> CleanedContent:
    without_pixels = remove_tracking_pixels(raw_text or "")
    plain_text = html_to_text(without_pixels)
    no_quotes = remove_quoted_replies(plain_text)
    no_signature = remove_email_signature(no_quotes)
    cleaned = normalize_text(no_signature)
    return CleanedContent(clean_text=cleaned, snippet=truncate(cleaned))


def _extract_headings_from_soup(soup: Any) -> list[str]:
    headings: list[str] = []
    for tag in soup.find_all(re.compile("^h[1-6]$")):
        heading = normalize_text(tag.get_text(" ", strip=True))
        if heading:
            headings.append(heading)
    return headings


def _fallback_extract_article_html(raw_html: str) -> str:
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


def clean_chrome_text(raw_html: str) -> CleanedContent:
    if not raw_html:
        return CleanedContent(clean_text="")

    article_html = raw_html
    if Document is not None and "<" in raw_html and ">" in raw_html:
        try:
            article_html = Document(raw_html).summary(html_partial=True)
        except Exception:
            article_html = _fallback_extract_article_html(raw_html)
    else:
        article_html = _fallback_extract_article_html(raw_html)

    if BeautifulSoup is None:
        clean_text = html_to_text(article_html)
        return CleanedContent(clean_text=clean_text, snippet=truncate(clean_text))

    soup = BeautifulSoup(article_html, "html.parser")
    for tag in soup(["script", "style", "noscript", "form", "button", "iframe", "svg"]):
        tag.decompose()

    headings = _extract_headings_from_soup(soup)
    clean_text = normalize_text(soup.get_text("\n", strip=True))
    return CleanedContent(
        clean_text=clean_text,
        headings=headings,
        snippet=truncate(clean_text),
    )


def clean_youtube_text(title: str = "", description: str = "", transcript: str = "") -> CleanedContent:
    combined = " ".join(part.strip() for part in (title, description, transcript) if part and part.strip())
    return CleanedContent(clean_text=normalize_text(combined), snippet=truncate(transcript or combined))


def clean_item_text(item: dict[str, Any]) -> CleanedContent:
    source = (item.get("source") or item.get("source_type") or "").lower()

    if source == "gmail":
        raw_body = item.get("content_primary_text") or item.get("raw_text") or ""
        return clean_gmail_text(raw_body)

    if source == "chrome":
        raw_html = item.get("raw_html") or item.get("raw_text") or ""
        return clean_chrome_text(raw_html)

    if source == "youtube":
        return clean_youtube_text(
            title=item.get("title", ""),
            description=item.get("description", ""),
            transcript=item.get("transcript_text", ""),
        )

    fallback = normalize_text(str(item.get("raw_text") or item.get("title") or ""))
    return CleanedContent(clean_text=fallback, snippet=truncate(fallback))
