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
    3. Extract text by mime type:
         PDF          → pypdf (fallback: pdfplumber)
         DOC/DOCX     → python-docx
         TXT / plain  → utf-8 decode
         images       → pytesseract OCR (optional — skipped if not installed)
    4. Cache extracted text in Redis (TTL 1 hour); binary is never stored
    5. Mark gmail_attachments.full_extract_cached + timestamps for visibility

Selection rules (architecture §5.4 Tier 2): an attachment qualifies when its
filename matches the query OR its file size is under 500 KB.

All extraction libraries are optional imports — a missing library degrades to
the Tier-1 lightweight_extract instead of crashing the retrieval pipeline.
"""

from __future__ import annotations

import base64
import io
import logging
from typing import Any, Optional

from ste import postgresql_manager, redis_manager

logger = logging.getLogger(__name__)

# Tier-2 auto-qualification threshold (bytes) when filename doesn't match query
MAX_AUTO_FETCH_SIZE_BYTES = 500 * 1024

# Caps keep the synthesis prompt bounded — the LLM receives snippets, never
# unbounded documents (privacy + token budget, architecture §13.2).
MAX_CHARS_PER_ATTACHMENT = 6000
MAX_CHARS_TOTAL = 12000


# ── Text extraction by mime type ──────────────────────────────────────────────

def _extract_pdf(data: bytes) -> Optional[str]:
    try:
        from pypdf import PdfReader
    except ImportError:
        try:
            from PyPDF2 import PdfReader  # legacy fallback
        except ImportError:
            logger.warning("attachment_processor: pypdf/PyPDF2 not installed — trying pdfplumber")
            return _extract_pdf_plumber(data)
    try:
        reader = PdfReader(io.BytesIO(data))
        pages = [page.extract_text() or "" for page in reader.pages]
        text = "\n".join(pages).strip()
        return text or _extract_pdf_plumber(data)
    except Exception as exc:
        logger.error("attachment_processor: pypdf extraction failed — %s", exc)
        return _extract_pdf_plumber(data)


def _extract_pdf_plumber(data: bytes) -> Optional[str]:
    try:
        import pdfplumber
    except ImportError:
        return None
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages).strip() or None
    except Exception as exc:
        logger.error("attachment_processor: pdfplumber extraction failed — %s", exc)
        return None


def _extract_docx(data: bytes) -> Optional[str]:
    try:
        import docx  # python-docx
    except ImportError:
        logger.warning("attachment_processor: python-docx not installed — skipping DOCX")
        return None
    try:
        document = docx.Document(io.BytesIO(data))
        return "\n".join(p.text for p in document.paragraphs if p.text).strip() or None
    except Exception as exc:
        logger.error("attachment_processor: docx extraction failed — %s", exc)
        return None


def _extract_plain_text(data: bytes) -> Optional[str]:
    try:
        return data.decode("utf-8", errors="replace").strip() or None
    except Exception:
        return None


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
        logger.error("attachment_processor: OCR failed — %s", exc)
        return None


def extract_text(data: bytes, mime_type: str, filename: str) -> Optional[str]:
    """Route binary data to the right extractor based on mime type / extension."""
    mime = (mime_type or "").lower()
    name = (filename or "").lower()

    if "pdf" in mime or name.endswith(".pdf"):
        return _extract_pdf(data)
    if "wordprocessingml" in mime or "msword" in mime or name.endswith((".docx", ".doc")):
        return _extract_docx(data)
    if mime.startswith("text/") or name.endswith((".txt", ".md", ".csv")):
        return _extract_plain_text(data)
    if mime.startswith("image/"):
        return _extract_image_ocr(data)

    logger.info("attachment_processor: unsupported mime %r for %r", mime_type, filename)
    return None


# ── Gmail API part traversal ──────────────────────────────────────────────────

def _walk_parts(payload: dict) -> list[dict]:
    """Flatten the (possibly nested) MIME part tree of a Gmail message."""
    parts: list[dict] = []
    stack = [payload]
    while stack:
        part = stack.pop()
        parts.append(part)
        stack.extend(part.get("parts", []))
    return parts


def _download_attachment_data(service: Any, email_id: str, filename: str) -> Optional[bytes]:
    """
    Fetch the message, locate the part whose filename matches, and download
    its binary body. Returns None when the part or data cannot be found.
    """
    message = (
        service.users().messages().get(userId="me", id=email_id, format="full").execute()
    )
    for part in _walk_parts(message.get("payload", {})):
        if part.get("filename") != filename:
            continue
        body = part.get("body", {})
        if body.get("data"):  # small attachments come inline
            return base64.urlsafe_b64decode(body["data"])
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
                return base64.urlsafe_b64decode(blob["data"])
    logger.warning("attachment_processor: part %r not found on message %s", filename, email_id)
    return None


# ── Qualification + orchestration ─────────────────────────────────────────────

def _qualifies(attachment: dict[str, Any], query_clean: str) -> bool:
    """Architecture §5.4 Tier-2 gate: filename matches query OR size < 500 KB."""
    filename = (attachment.get("filename") or "").lower()
    if query_clean:
        if any(term and term in filename for term in query_clean.lower().split()):
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
    except Exception as exc:
        # Visibility flags only — extraction result is already cached in Redis.
        logger.error("attachment_processor: failed to mark %s extracted — %s", attachment_id, exc)


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
    except Exception as exc:
        logger.error("attachment_processor: DB lookup failed for %s — %s", memory_id, exc)
        return None

    if not attachments:
        return None

    service = None
    blocks: list[str] = []
    total_chars = 0

    for attachment in attachments:
        if total_chars >= MAX_CHARS_TOTAL:
            break
        if not _qualifies(attachment, query_clean):
            logger.info(
                "attachment_processor: %r skipped (size %s, no filename match)",
                attachment.get("filename"), attachment.get("file_size"),
            )
            continue

        attachment_id = str(attachment["attachment_id"])
        filename = attachment.get("filename") or "(unnamed)"

        # 1) Redis cache (1-hour TTL)
        text: Optional[str] = None
        try:
            text = redis_manager.get_attachment_text(memory_id, attachment_id)
            if text:
                logger.info("attachment_processor: cache HIT for %r", filename)
        except Exception as exc:
            logger.warning("attachment_processor: Redis unavailable (%s) — fetching live", exc)

        # 2) Cache miss → Gmail API download + extraction
        if not text:
            try:
                if service is None:
                    from ingestion.gmail.gmail_api import authenticate_gmail
                    service = authenticate_gmail()
                data = _download_attachment_data(service, email_id, attachment["filename"])
            except Exception as exc:
                logger.error("attachment_processor: Gmail fetch failed for %r — %s", filename, exc)
                data = None

            if data:
                text = extract_text(data, attachment.get("mime_type") or "", filename)
                if text:
                    text = text[:MAX_CHARS_PER_ATTACHMENT]
                    try:
                        redis_manager.cache_attachment_text(memory_id, attachment_id, text)
                    except Exception as exc:
                        logger.warning("attachment_processor: cache write failed — %s", exc)
                    _mark_extracted(attachment_id)

        # 3) Fall back to Tier-1 lightweight extract so the answer degrades,
        #    never disappears.
        if not text and attachment.get("lightweight_extract"):
            text = f"(metadata only) {attachment['lightweight_extract']}"

        if text:
            snippet = text[: MAX_CHARS_TOTAL - total_chars]
            blocks.append(f"── {filename} ──\n{snippet}")
            total_chars += len(snippet)

    return "\n\n".join(blocks) if blocks else None
