"""
attachment_processor.py — ENP Module
Echo Personal Memory System

Tier-2 (heavyweight, on-demand) attachment extraction — architecture §8.3 / §5.4.

Triggered by the LangGraph RSE fetch_attachment node when a top-3 search result
is a Gmail item with attachments and the parsed intent requested file content.

Flow per attachment:
    1. Redis cache check (1-hour TTL) — instant on repeat queries
    2. Cache miss → fetch full message from Gmail API, locate the attachment
       part by filename, download the binary via attachments.get
    3. Extract text by mime type OR file extension (Gmail often sends
       application/octet-stream, so the extension decides then)
    4. Cache extracted text in Redis (TTL 1 hour); binary is never stored
    5. Mark gmail_attachments.full_extract_cached + timestamps for visibility

Supported (doc §5.4: "PDF, DOC, DOCX, TXT, and image files (via Tesseract OCR)"):
    PDF                pypdf, page by page, stops at the char cap (fallback pdfplumber);
                       an encrypted PDF opens only with the empty password
    DOCX               python-docx: paragraphs AND tables in document order, + headers/footers
    TXT / CSV / MD /   decoded by BOM (UTF-8 / UTF-16), else UTF-8, else Latin-1
    ICS / other text/*
    HTML               tags stripped (enp.text_cleaner.html_to_text)
    images             Tesseract OCR — OPTIONAL and OFF today: pytesseract and the
                       Tesseract binary are not installed (requirements.txt)
Not supported yet (decision A22): legacy .doc, scanned PDFs (no text layer),
XLSX, PPTX, ODT, RTF, .eml, ZIP, HEIC. Audio/video are out of scope (doc §5.4).
Unsupported or failed -> the Tier-1 lightweight_extract (filename|mime|size).

Selection rules (architecture §5.4 Tier 2): an attachment qualifies when its
filename matches the query OR its file size is under 500 KB.

Threading: runs inside an RSE request (FastAPI threadpool). Builds its own
Gmail service per call (googleapiclient objects are not thread-safe); no
shared state. Logs attachment ids, extensions and sizes — never filenames or
text (a filename can be as personal as the content: "salary_slip_oct.pdf").

All extraction libraries are optional imports — a missing library degrades to
the Tier-1 lightweight_extract instead of crashing the retrieval pipeline.
"""

from __future__ import annotations

import base64
import codecs
import io
import logging
from pathlib import PurePath
import time
from typing import Any, Optional

from enp.text_cleaner import html_to_text
from enp.topic_extractor import STOPWORDS
from ste import postgresql_manager, redis_manager

logger = logging.getLogger(__name__)

# Tier-2 auto-qualification threshold (bytes) when filename doesn't match query
MAX_AUTO_FETCH_SIZE_BYTES = 500 * 1024

# Caps keep the synthesis prompt bounded — the LLM receives snippets, never
# unbounded documents (privacy + token budget, architecture §13.2).
MAX_CHARS_PER_ATTACHMENT = 6000
MAX_CHARS_TOTAL = 12000

# Query words that never count as a filename match: shorter than 3 letters,
# English stopwords ("the" would match "theory_notes.pdf"), or words naming a
# file type, which match nearly every attachment.
MIN_QUERY_TERM_LENGTH = 3
NON_MATCHING_QUERY_TERMS = STOPWORDS | {"pdf", "doc", "docx", "file", "files", "attachment", "attachments", "document"}

PDF_EXTENSIONS = (".pdf",)
DOCX_EXTENSIONS = (".docx",)
TEXT_EXTENSIONS = (".txt", ".md", ".csv", ".tsv", ".log", ".json", ".xml", ".ics")
HTML_EXTENSIONS = (".html", ".htm")
IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tif", ".tiff", ".webp")


def _extension(filename: str) -> str:
    return PurePath(filename or "").suffix.lower()


# ── Text extraction by type ───────────────────────────────────────────────────

def _extract_pdf(data: bytes) -> Optional[str]:
    try:
        from pypdf import PdfReader
    except ImportError:
        logger.warning("attachment_processor: pypdf not installed — trying pdfplumber")
        return _extract_pdf_plumber(data)
    try:
        reader = PdfReader(io.BytesIO(data))
        if reader.is_encrypted and not reader.decrypt(""):
            logger.info("attachment_processor: PDF is password-protected — skipped")
            return None
        pages: list[str] = []
        total = 0
        for page in reader.pages:  # page by page: a 300-page PDF stops once the cap is reached
            page_text = page.extract_text() or ""
            pages.append(page_text)
            total += len(page_text)
            if total >= MAX_CHARS_PER_ATTACHMENT:
                break
        text = "\n".join(pages).strip()
        if not text:
            logger.info("attachment_processor: PDF has no text layer (scanned?) — trying pdfplumber")
        return text or _extract_pdf_plumber(data)
    except Exception as exc:
        logger.warning("attachment_processor: pypdf failed (%s) — trying pdfplumber", type(exc).__name__)
        return _extract_pdf_plumber(data)


def _extract_pdf_plumber(data: bytes) -> Optional[str]:
    try:
        import pdfplumber
    except ImportError:
        return None
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            pages: list[str] = []
            total = 0
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                pages.append(page_text)
                total += len(page_text)
                if total >= MAX_CHARS_PER_ATTACHMENT:
                    break
            return "\n".join(pages).strip() or None
    except Exception as exc:
        logger.warning("attachment_processor: pdfplumber failed (%s)", type(exc).__name__)
        return None


def _docx_table_lines(table: Any) -> list[str]:
    """One line per table row, cells joined with ' | ' (merged cells appear once)."""
    lines: list[str] = []
    for row in table.rows:
        cells: list[str] = []
        for cell in row.cells:
            text = cell.text.strip()
            if text and (not cells or cells[-1] != text):
                cells.append(text)
        if cells:
            lines.append(" | ".join(cells))
    return lines


def _extract_docx(data: bytes) -> Optional[str]:
    try:
        import docx  # python-docx
        from docx.table import Table
    except ImportError:
        logger.warning("attachment_processor: python-docx not installed — skipping DOCX")
        return None
    try:
        document = docx.Document(io.BytesIO(data))
        lines: list[str] = []
        for section in document.sections:  # letterheads, reference numbers
            lines.extend(p.text for p in section.header.paragraphs if p.text.strip())
        # Body in document order: paragraphs and tables interleaved. Tables matter
        # (offer letters, invoices and marksheets keep their numbers in tables).
        for block in document.iter_inner_content():
            if isinstance(block, Table):
                lines.extend(_docx_table_lines(block))
            elif block.text.strip():
                lines.append(block.text)
        for section in document.sections:
            lines.extend(p.text for p in section.footer.paragraphs if p.text.strip())
        return "\n".join(lines).strip() or None
    except Exception as exc:
        logger.warning("attachment_processor: docx extraction failed (%s)", type(exc).__name__)
        return None


def _decode_text(data: bytes) -> str:
    """Bytes -> str: by BOM (UTF-8 / UTF-16), else UTF-8, else Latin-1 (never fails)."""
    if data.startswith(codecs.BOM_UTF8):
        return data[len(codecs.BOM_UTF8):].decode("utf-8", errors="replace")
    if data.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        return data.decode("utf-16", errors="replace")
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("latin-1")


def _extract_plain_text(data: bytes) -> Optional[str]:
    return _decode_text(data).replace("\r\n", "\n").strip() or None


def _extract_html(data: bytes) -> Optional[str]:
    return html_to_text(_decode_text(data)) or None


def _extract_image_ocr(data: bytes) -> Optional[str]:
    try:
        import pytesseract
        from PIL import Image
    except ImportError:
        logger.info("attachment_processor: pytesseract/Pillow not installed — skipping OCR")
        return None
    try:
        return pytesseract.image_to_string(Image.open(io.BytesIO(data))).strip() or None
    except Exception as exc:
        logger.warning("attachment_processor: OCR failed (%s)", type(exc).__name__)
        return None


def extract_text(data: bytes, mime_type: str, filename: str) -> Optional[str]:
    """Route binary data to the right extractor by mime type, then extension.

    Returns None for unsupported types and failed extractions (the caller then
    falls back to the Tier-1 lightweight_extract).
    """
    mime = (mime_type or "").lower()
    ext = _extension(filename)

    if "pdf" in mime or ext in PDF_EXTENSIONS:
        return _extract_pdf(data)
    if "wordprocessingml" in mime or ext in DOCX_EXTENSIONS:
        return _extract_docx(data)
    if "msword" in mime or ext == ".doc":
        # Legacy binary Word: python-docx cannot read it (decision A22).
        logger.info("attachment_processor: legacy .doc not supported yet")
        return None
    if mime == "text/html" or ext in HTML_EXTENSIONS:
        return _extract_html(data)
    if mime.startswith("text/") or ext in TEXT_EXTENSIONS:
        return _extract_plain_text(data)
    if mime.startswith("image/") or ext in IMAGE_EXTENSIONS:
        return _extract_image_ocr(data)

    logger.info("attachment_processor: unsupported type (mime %r, extension %r)", mime_type, ext)
    return None


# ── Gmail API part traversal ──────────────────────────────────────────────────

def _b64url_decode(data: str) -> bytes:
    # Padding is optional in base64url; the decoder needs it.
    return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))


def _walk_parts(payload: dict) -> list[dict]:
    """Flatten the (possibly nested) MIME part tree of a Gmail message."""
    parts: list[dict] = []
    stack = [payload]
    while stack:
        part = stack.pop()
        parts.append(part)
        stack.extend(part.get("parts", []) or [])
    return parts


def _download_attachment_data(service: Any, email_id: str, filename: str) -> Optional[bytes]:
    """
    Fetch the message, locate the part whose filename matches, and download
    its binary body. Returns None when the part or data cannot be found.

    The message is re-fetched every time on purpose: Gmail's attachmentId is
    not stable between messages.get calls, so a stored one cannot be reused.
    Two attachments with the same filename are ambiguous; the first is taken.
    """
    message = (
        service.users().messages().get(userId="me", id=email_id, format="full").execute()
    )
    for part in _walk_parts(message.get("payload", {})):
        if part.get("filename") != filename:
            continue
        body = part.get("body", {})
        if body.get("data"):  # small attachments come inline
            return _b64url_decode(body["data"])
        attachment_id = body.get("attachmentId")
        if attachment_id:
            blob = (
                service.users()
                .messages()
                .attachments()
                .get(userId="me", messageId=email_id, id=attachment_id)
                .execute()
            )
            if blob.get("data"):
                return _b64url_decode(blob["data"])
    logger.warning("attachment_processor: attachment part not found on message %s", email_id)
    return None


# ── Qualification + orchestration ─────────────────────────────────────────────

def _qualifies(attachment: dict[str, Any], query_clean: str) -> bool:
    """Architecture §5.4 Tier-2 gate: filename matches query OR size < 500 KB.

    A size of 0 means "unknown" (Tier 1 could not read it) and qualifies.
    """
    filename = (attachment.get("filename") or "").lower()
    if query_clean:
        terms = [
            term
            for term in query_clean.lower().split()
            if len(term) >= MIN_QUERY_TERM_LENGTH and term not in NON_MATCHING_QUERY_TERMS
        ]
        if any(term in filename for term in terms):
            return True
    size = attachment.get("file_size") or 0
    return 0 < size < MAX_AUTO_FETCH_SIZE_BYTES or size == 0


def _mark_extracted(attachment_id: str) -> None:
    try:
        postgresql_manager.execute(
            """
            UPDATE gmail_attachments
            SET last_extracted_at = NOW(),
                full_extract_cached = TRUE,
                full_extract_generated_at = NOW()
            WHERE attachment_id = :attachment_id
            """,
            {"attachment_id": attachment_id},
        )
    except Exception:
        # Visibility flags only — extraction result is already cached in Redis.
        logger.exception("attachment_processor: failed to mark %s extracted", attachment_id)


def fetch_attachment_text_for_memory(memory_id: str, query_clean: str = "") -> Optional[str]:
    """
    Extract full text of all qualifying attachments on a Gmail memory item.

    Returns a single labelled text block for the synthesizer, or None when
    the item has no extractable attachments. Never raises — retrieval must
    not fail because a PDF was malformed.
    """
    try:
        meta = postgresql_manager.fetchone(
            "SELECT email_id FROM gmail_metadata WHERE memory_id = :memory_id",
            {"memory_id": memory_id},
        )
        if not meta or not meta.get("email_id"):
            logger.info("attachment_processor: no gmail_metadata/email_id for %s", memory_id)
            return None
        email_id = meta["email_id"]

        attachments = postgresql_manager.fetchall(
            """
            SELECT attachment_id, filename, mime_type, file_size, lightweight_extract
            FROM gmail_attachments
            WHERE memory_id = :memory_id
            """,
            {"memory_id": memory_id},
        )
    except Exception:
        logger.exception("attachment_processor: DB lookup failed for %s", memory_id)
        return None

    if not attachments:
        return None

    service = None
    blocks: list[str] = []
    total_chars = 0
    extracted = cached = fallback = skipped = 0

    for attachment in attachments:
        if total_chars >= MAX_CHARS_TOTAL:
            break
        attachment_id = str(attachment["attachment_id"])
        filename = attachment.get("filename") or "(unnamed)"
        ext = _extension(filename) or "none"

        if not _qualifies(attachment, query_clean):
            logger.info(
                "attachment_processor: %s (%s, %s bytes) skipped — over 500 KB and no filename match",
                attachment_id, ext, attachment.get("file_size"),
            )
            skipped += 1
            continue

        # 1) Redis cache (1-hour TTL)
        text: Optional[str] = None
        try:
            text = redis_manager.get_attachment_text(memory_id, attachment_id)
            if text:
                cached += 1
        except Exception as exc:
            logger.warning("attachment_processor: Redis unavailable (%s) — fetching live", type(exc).__name__)

        # 2) Cache miss → Gmail API download + extraction
        if not text:
            started_at = time.perf_counter()
            try:
                if service is None:
                    from ingestion.gmail.gmail_api import authenticate_gmail
                    service = authenticate_gmail()
                data = _download_attachment_data(service, email_id, attachment["filename"])
            except Exception as exc:
                logger.error("attachment_processor: Gmail fetch failed for %s (%s)", attachment_id, type(exc).__name__)
                data = None

            if data:
                text = extract_text(data, attachment.get("mime_type") or "", filename)
                logger.info(
                    "attachment_processor: %s (%s, %d bytes) -> %s in %.0f ms",
                    attachment_id, ext, len(data),
                    f"{len(text)} chars" if text else "no text",
                    (time.perf_counter() - started_at) * 1000,
                )
                if text:
                    extracted += 1
                    text = text[:MAX_CHARS_PER_ATTACHMENT]
                    try:
                        redis_manager.cache_attachment_text(memory_id, attachment_id, text)
                    except Exception as exc:
                        logger.warning("attachment_processor: cache write failed (%s)", type(exc).__name__)
                    _mark_extracted(attachment_id)

        # 3) Fall back to Tier-1 lightweight extract so the answer degrades,
        #    never disappears.
        if not text and attachment.get("lightweight_extract"):
            text = f"(metadata only) {attachment['lightweight_extract']}"
            fallback += 1

        if text:
            snippet = text[: MAX_CHARS_TOTAL - total_chars]
            blocks.append(f"── {filename} ──\n{snippet}")
            total_chars += len(snippet)

    logger.info(
        "attachment_processor: memory %s — %d extracted, %d from cache, %d metadata-only, %d skipped (%d chars)",
        memory_id, extracted, cached, fallback, skipped, total_chars,
    )
    return "\n\n".join(blocks) if blocks else None
