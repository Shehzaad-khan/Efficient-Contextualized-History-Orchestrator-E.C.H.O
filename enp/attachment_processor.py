"""
attachment_processor.py — ENP Module
Echo Personal Memory System

Attachment extraction — architecture §8.3 / §5.4. Two callers share the extractors:

  * ENP worker, at ingestion (A4): extract_full_text() reads each Gmail attachment
    not read yet into gmail_attachments.extracted_text. It runs on the ENP thread
    with the batch's shared Gmail service and skips types it cannot read and files
    over MAX_INGEST_ATTACHMENT_BYTES.
  * Tier-2, on demand (below): fetch_attachment_text_for_memory().

Tier-2 (heavyweight, on-demand) is triggered by the LangGraph RSE fetch_attachment node when a top-3 search result
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
    PPTX               slide text + speaker notes, in slide order (zip + regex, no library)
    XLSX               one line per row, cells joined with ' | ', sheet name above its rows
    EML                Subject/From/To/Date + the plain (else HTML) body
    images             Tesseract OCR — needs pytesseract AND the Tesseract program;
                       without either, OCR is off and images fall back to Tier 1
    scanned PDFs       a PDF with no text layer is rendered (pypdfium2) and read by the
                       same OCR, at most OCR_PDF_MAX_PAGES pages
Not supported (decision A22): legacy .doc/.xls/.ppt, RTF, ODT/ODS/ODP, ZIP, HEIC.
Audio/video are out of scope (doc §5.4). The PPTX, XLSX and EML readers (D2) use only the standard library,
so they add no dependency.
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
from email import policy as email_policy
from email.parser import BytesParser
import functools
from html import unescape
import io
import logging
import os
from pathlib import PurePath
import re
import shutil
import time
from typing import Any, Optional
import zipfile

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

# Ingestion keeps the FULL text in gmail_attachments.extracted_text (A4), but not
# without limit: the text also goes into the stored tsvector, which PostgreSQL
# rejects above 1 MB, and a rejected vector would leave the mail unprocessed.
# 200,000 characters is about 40 pages of text.
MAX_EXTRACTED_TEXT_CHARS = 200_000

# Ingestion downloads each attachment once, on the single ENP thread, so a very large
# file would stall the whole batch. Bigger than this is skipped (no text, the mail is
# still enriched from its body). A size of 0 means "unknown" and is tried.
MAX_INGEST_ATTACHMENT_BYTES = 10 * 1024 * 1024

# Query words that never count as a filename match: shorter than 3 letters,
# English stopwords ("the" would match "theory_notes.pdf"), or words naming a
# file type, which match nearly every attachment.
MIN_QUERY_TERM_LENGTH = 3
NON_MATCHING_QUERY_TERMS = STOPWORDS | {"pdf", "doc", "docx", "file", "files", "attachment", "attachments", "document"}

PDF_EXTENSIONS = (".pdf",)
DOCX_EXTENSIONS = (".docx",)
TEXT_EXTENSIONS = (".txt", ".md", ".csv", ".tsv", ".log", ".json", ".xml", ".ics")
HTML_EXTENSIONS = (".html", ".htm")
PPTX_EXTENSIONS = (".pptx", ".pptm")
XLSX_EXTENSIONS = (".xlsx", ".xlsm")
RTF_EXTENSIONS = (".rtf",)  # skipped on purpose (A22), but text/rtf must not reach the plain-text reader
EML_EXTENSIONS = (".eml",)
IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tif", ".tiff", ".webp")

# OCR (D2). A scanned PDF is rendered at OCR_PDF_DPI and read page by page, at most
# OCR_PDF_MAX_PAGES pages; one page that takes longer than OCR_PAGE_TIMEOUT_SECONDS
# is abandoned (pytesseract raises, the extraction returns None).
OCR_PDF_DPI = 200
OCR_PDF_MAX_PAGES = 10
OCR_PAGE_TIMEOUT_SECONDS = 60
_WINDOWS_TESSERACT_PATHS = (
    r"C:\Program Files\Tesseract-OCR\tesseract.exe",
    r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
    # Unpacked without an installer (7-Zip extraction of the UB-Mannheim setup exe):
    # needs no administrator rights.
    os.path.expanduser(r"~\tools\tesseract\tesseract.exe"),
)


def _extension(filename: str) -> str:
    return PurePath(filename or "").suffix.lower()


# ── Text extraction by type ───────────────────────────────────────────────────

def _extract_pdf(data: bytes, max_chars: int) -> Optional[str]:
    try:
        from pypdf import PdfReader
    except ImportError:
        logger.warning("attachment_processor: pypdf not installed — trying pdfplumber")
        return _extract_pdf_plumber(data, max_chars)
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
            if total >= max_chars:
                break
        text = "\n".join(pages).strip()
        if not text:
            logger.info("attachment_processor: PDF has no text layer (scanned?) — trying pdfplumber")
        return text or _extract_pdf_plumber(data, max_chars)
    except Exception as exc:
        logger.warning("attachment_processor: pypdf failed (%s) — trying pdfplumber", type(exc).__name__)
        return _extract_pdf_plumber(data, max_chars)


def _extract_pdf_plumber(data: bytes, max_chars: int) -> Optional[str]:
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
                if total >= max_chars:
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


# ── PPTX / XLSX / EML (D2; standard library only) ─────────────────────────────
#
# PPTX and XLSX are zip files of XML. Their text is read with regular
# expressions instead of an XML parser, so a hostile file cannot use entity expansion,
# and only the members that hold text are opened. Mail attachments come from anyone,
# so every read is capped: one member, and all members of one file together.
OFFICE_MEMBER_MAX_BYTES = 16 * 1024 * 1024
OFFICE_TOTAL_MAX_BYTES = 64 * 1024 * 1024

_XML_FLAGS = re.IGNORECASE | re.DOTALL
_SLIDE_NAME_RE = re.compile(r"^ppt/slides/slide(\d+)\.xml$")
_NOTES_NAME_RE = re.compile(r"^ppt/notesSlides/notesSlide(\d+)\.xml$")
_SHEET_NAME_RE = re.compile(r"^xl/worksheets/sheet(\d+)\.xml$")
_DML_PARAGRAPH_RE = re.compile(r"<a:p\b[^>]*>(.*?)</a:p>", _XML_FLAGS)
_DML_TOKEN_RE = re.compile(r"<a:t(?:\s[^>]*)?>(.*?)</a:t>|(<a:br\b[^>]*>)", _XML_FLAGS)
_XLSX_SI_RE = re.compile(r"<si\b[^>]*>(.*?)</si>", _XML_FLAGS)
_XLSX_PHONETIC_RE = re.compile(r"<rPh\b[^>]*>.*?</rPh>", _XML_FLAGS)
_XLSX_T_RE = re.compile(r"<t(?:\s[^>]*)?>(.*?)</t>", _XML_FLAGS)
_XLSX_ROW_RE = re.compile(r"<row\b[^>]*>(.*?)</row>", _XML_FLAGS)
_XLSX_CELL_RE = re.compile(r"<c\b([^>]*?)(?:/>|>(.*?)</c>)", _XML_FLAGS)
_XLSX_V_RE = re.compile(r"<v(?:\s[^>]*)?>(.*?)</v>", _XML_FLAGS)
_XLSX_SHEET_TAG_RE = re.compile(r"<sheet\b[^>]*>", _XML_FLAGS)
_XLSX_REL_TAG_RE = re.compile(r"<Relationship\b[^>]*>", _XML_FLAGS)


def _xml_attr(tag: str, name: str) -> str:
    match = re.search(rf'(?<![\w:]){re.escape(name)}\s*=\s*"([^"]*)"', tag)
    return match.group(1) if match else ""


def _open_office_zip(data: bytes):
    """-> (member names, read(name) -> str | None). read returns None for a missing
    member or one over the size caps, and never reads more than the caps allow."""
    archive = zipfile.ZipFile(io.BytesIO(data))
    remaining = [OFFICE_TOTAL_MAX_BYTES]

    def read(name: str) -> Optional[str]:
        try:
            info = archive.getinfo(name)
        except KeyError:
            return None
        limit = min(OFFICE_MEMBER_MAX_BYTES, remaining[0])
        if info.file_size > limit:
            logger.info("attachment_processor: zip member over the size cap - skipped")
            return None
        with archive.open(info) as member:
            raw = member.read(limit + 1)  # the size in the header can lie
        if len(raw) > limit:
            logger.info("attachment_processor: zip member over the size cap - skipped")
            return None
        remaining[0] -= len(raw)
        return raw.decode("utf-8", errors="replace")

    return archive.namelist(), read


def _numbered(names: list[str], pattern: re.Pattern) -> list[str]:
    """Member names matching `pattern`, in the order of their number (slide2 before slide10)."""
    found = [(int(m.group(1)), name) for name in names if (m := pattern.match(name))]
    return [name for _, name in sorted(found)]


def _dml_lines(xml: str) -> list[str]:
    """Paragraph texts of a PowerPoint slide or notes part (DrawingML)."""
    lines: list[str] = []
    for paragraph in _DML_PARAGRAPH_RE.finditer(xml):
        parts = [
            "\n" if match.group(2) else unescape(match.group(1))
            for match in _DML_TOKEN_RE.finditer(paragraph.group(1))
        ]
        text = "".join(parts).strip()
        if text:
            lines.append(text)
    return lines


def _extract_pptx(data: bytes, max_chars: int) -> Optional[str]:
    try:
        names, read = _open_office_zip(data)
        blocks: list[str] = []
        total = 0
        for name in _numbered(names, _SLIDE_NAME_RE) + _numbered(names, _NOTES_NAME_RE):
            xml = read(name)
            if xml is None:
                continue
            lines = _dml_lines(xml)
            if lines:
                blocks.append("\n".join(lines))
                total += len(blocks[-1])
            if total >= max_chars:
                break
        return "\n\n".join(blocks).strip() or None
    except Exception as exc:
        logger.warning("attachment_processor: pptx extraction failed (%s)", type(exc).__name__)
        return None


def _xlsx_shared_strings(xml: str) -> list[str]:
    strings: list[str] = []
    for item in _XLSX_SI_RE.finditer(xml):
        body = _XLSX_PHONETIC_RE.sub("", item.group(1))
        strings.append(unescape("".join(_XLSX_T_RE.findall(body))))
    return strings


def _xlsx_sheet_names(names: list[str], read) -> dict[str, str]:
    """Member name -> sheet name, from workbook.xml and its relationships ({} when unreadable)."""
    workbook = read("xl/workbook.xml")
    rels = read("xl/_rels/workbook.xml.rels")
    if not workbook or not rels:
        return {}
    targets: dict[str, str] = {}
    for tag in _XLSX_REL_TAG_RE.findall(rels):
        target = _xml_attr(tag, "Target")
        if target:
            targets[_xml_attr(tag, "Id")] = target.lstrip("/") if target.startswith("/") else f"xl/{target}"
    result: dict[str, str] = {}
    for tag in _XLSX_SHEET_TAG_RE.findall(workbook):
        member = targets.get(_xml_attr(tag, "r:id"))
        if member:
            result[member] = unescape(_xml_attr(tag, "name"))
    return result


def _xlsx_sheet_lines(xml: str, shared: list[str]) -> list[str]:
    """One line per row, non-empty cells joined with ' | '."""
    lines: list[str] = []
    for row in _XLSX_ROW_RE.finditer(xml):
        cells: list[str] = []
        for cell in _XLSX_CELL_RE.finditer(row.group(1)):
            body = cell.group(2) or ""
            cell_type = _xml_attr(cell.group(1), "t")
            if cell_type == "inlineStr":
                value = unescape("".join(_XLSX_T_RE.findall(body)))
            else:
                match = _XLSX_V_RE.search(body)
                value = unescape(match.group(1)) if match else ""
                if cell_type == "s":
                    try:
                        value = shared[int(value)]
                    except (ValueError, IndexError):
                        value = ""
            value = value.strip()
            if value:
                cells.append(value)
        if cells:
            lines.append(" | ".join(cells))
    return lines


def _extract_xlsx(data: bytes, max_chars: int) -> Optional[str]:
    try:
        names, read = _open_office_zip(data)
        shared_xml = read("xl/sharedStrings.xml")
        shared = _xlsx_shared_strings(shared_xml) if shared_xml else []
        sheet_names = _xlsx_sheet_names(names, read)
        blocks: list[str] = []
        total = 0
        for name in _numbered(names, _SHEET_NAME_RE):
            xml = read(name)
            if xml is None:
                continue
            lines = _xlsx_sheet_lines(xml, shared)
            if not lines:
                continue
            title = sheet_names.get(name)
            block = "\n".join(([f"[{title}]"] if title else []) + lines)
            blocks.append(block)
            total += len(block)
            if total >= max_chars:
                break
        return "\n\n".join(blocks).strip() or None
    except Exception as exc:
        logger.warning("attachment_processor: xlsx extraction failed (%s)", type(exc).__name__)
        return None


def _extract_eml(data: bytes) -> Optional[str]:
    """A forwarded or saved e-mail: Subject/From/To/Date headers + the readable body."""
    try:
        message = BytesParser(policy=email_policy.default).parsebytes(data)
        headers = [f"{name}: {message[name]}" for name in ("Subject", "From", "To", "Date") if message[name]]
        body = message.get_body(preferencelist=("plain", "html"))
        content = body.get_content() if body is not None else ""
        if body is not None and body.get_content_type() == "text/html":
            content = html_to_text(content)
        return "\n".join(headers + ([""] if headers and content else []) + [content.strip()]).strip() or None
    except Exception as exc:
        logger.warning("attachment_processor: eml extraction failed (%s)", type(exc).__name__)
        return None


@functools.lru_cache(maxsize=1)
def _ocr_ready() -> bool:
    """True when pytesseract, Pillow and the Tesseract program are all usable.

    Checked once per process (the answer is cached), so installing Tesseract
    needs a backend restart to be noticed. The program is found, in order, from
    the TESSERACT_CMD environment variable, the PATH, then the default Windows
    install folders.
    """
    try:
        import pytesseract
        from PIL import Image  # noqa: F401  (OCR input type; checked here so a missing Pillow reads as "not ready")
    except ImportError:
        logger.info("attachment_processor: pytesseract/Pillow not installed — OCR is off")
        return False

    configured = os.getenv("TESSERACT_CMD")
    if configured:
        pytesseract.pytesseract.tesseract_cmd = configured
    elif shutil.which("tesseract") is None:
        for candidate in _WINDOWS_TESSERACT_PATHS:
            if os.path.isfile(candidate):
                pytesseract.pytesseract.tesseract_cmd = candidate
                break
    try:
        version = pytesseract.get_tesseract_version()
    except Exception as exc:
        logger.info("attachment_processor: Tesseract program not found (%s) — OCR is off", type(exc).__name__)
        return False
    logger.info("attachment_processor: OCR ready (Tesseract %s)", version)
    return True


def _ocr_image(image: Any) -> str:
    """Text read from one PIL image; "" when nothing is readable."""
    import pytesseract

    return pytesseract.image_to_string(image, timeout=OCR_PAGE_TIMEOUT_SECONDS).strip()


def _extract_image_ocr(data: bytes) -> Optional[str]:
    if not _ocr_ready():
        return None
    try:
        from PIL import Image

        return _ocr_image(Image.open(io.BytesIO(data))) or None
    except Exception as exc:
        logger.warning("attachment_processor: OCR failed (%s)", type(exc).__name__)
        return None


def _ocr_pdf(data: bytes, max_chars: int) -> Optional[str]:
    """OCR a scanned PDF (no text layer): render each page, read it, stop at the char cap.

    At most OCR_PDF_MAX_PAGES pages are read, so a 300-page scan costs the same
    as a 10-page one. Returns None when OCR is off, the PDF cannot be rendered
    (corrupt, password-protected) or no page has readable text.
    """
    if not _ocr_ready():
        return None
    try:
        import pypdfium2
    except ImportError:
        logger.info("attachment_processor: pypdfium2 not installed — scanned PDFs are not read")
        return None
    document = None
    try:
        document = pypdfium2.PdfDocument(data)
        pages: list[str] = []
        total = 0
        for index in range(min(len(document), OCR_PDF_MAX_PAGES)):
            page = document[index]
            image = page.render(scale=OCR_PDF_DPI / 72).to_pil()
            page_text = _ocr_image(image)
            pages.append(page_text)
            total += len(page_text)
            if total >= max_chars:
                break
        text = "\n".join(page for page in pages if page).strip()
        logger.info("attachment_processor: scanned PDF — OCR read %d page(s), %d chars", len(pages), len(text))
        return text or None
    except Exception as exc:
        logger.warning("attachment_processor: scanned-PDF OCR failed (%s)", type(exc).__name__)
        return None
    finally:
        if document is not None:
            document.close()


def _is_office_or_mail_type(mime: str, ext: str) -> bool:
    """PPTX, XLSX and EML."""
    return (
        "presentationml" in mime or ext in PPTX_EXTENSIONS
        or "spreadsheetml" in mime or ext in XLSX_EXTENSIONS
        or mime == "message/rfc822" or ext in EML_EXTENSIONS
    )


def can_extract_text(mime_type: str, filename: str) -> bool:
    """True when extract_text has an extractor for this type, so ingestion does not
    download files it cannot read. Mirrors the routing in extract_text; images count
    only when OCR is installed."""
    mime = (mime_type or "").lower()
    ext = _extension(filename)
    if "pdf" in mime or ext in PDF_EXTENSIONS:
        return True
    if "wordprocessingml" in mime or ext in DOCX_EXTENSIONS:
        return True
    if "msword" in mime or ext == ".doc":
        return False
    if "rtf" in mime or ext in RTF_EXTENSIONS:
        return False  # skipped (A22); text/rtf must not fall into the text/* rule
    if _is_office_or_mail_type(mime, ext):
        return True
    if mime == "text/html" or ext in HTML_EXTENSIONS:
        return True
    if mime.startswith("text/") or ext in TEXT_EXTENSIONS:
        return True
    if mime.startswith("image/") or ext in IMAGE_EXTENSIONS:
        return _ocr_ready()
    return False


def extract_text(
    data: bytes,
    mime_type: str,
    filename: str,
    max_chars: int = MAX_CHARS_PER_ATTACHMENT,
) -> Optional[str]:
    """Route binary data to the right extractor by mime type, then extension.

    max_chars only stops PDF page reading early (the cheap way to bound a 300-page
    file); the caller still trims the returned text. Returns None for unsupported
    types and failed extractions (the caller then falls back to the Tier-1
    lightweight_extract).
    """
    mime = (mime_type or "").lower()
    ext = _extension(filename)

    if "pdf" in mime or ext in PDF_EXTENSIONS:
        # No text layer (a scan) -> read the page images instead.
        return _extract_pdf(data, max_chars) or _ocr_pdf(data, max_chars)
    if "wordprocessingml" in mime or ext in DOCX_EXTENSIONS:
        return _extract_docx(data)
    if "msword" in mime or ext == ".doc":
        # Legacy binary Word: python-docx cannot read it (decision A22).
        logger.info("attachment_processor: legacy .doc not supported yet")
        return None
    if "presentationml" in mime or ext in PPTX_EXTENSIONS:
        return _extract_pptx(data, max_chars)
    if "spreadsheetml" in mime or ext in XLSX_EXTENSIONS:
        return _extract_xlsx(data, max_chars)
    if "rtf" in mime or ext in RTF_EXTENSIONS:
        # Skipped (A22). Before the text/* rule: text/rtf would be read as plain text, i.e. raw markup.
        logger.info("attachment_processor: RTF not supported (decision A22)")
        return None
    if mime == "message/rfc822" or ext in EML_EXTENSIONS:
        return _extract_eml(data)
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
    """Flatten the (possibly nested) MIME part tree of a Gmail message, in message order."""
    parts: list[dict] = []
    stack = [payload]
    while stack:
        part = stack.pop()
        parts.append(part)
        # Reversed, so the first child is popped (and listed) first.
        stack.extend(reversed(part.get("parts", []) or []))
    return parts


def _select_attachment_parts(parts: list[dict], filename: str, size: int) -> list[dict]:
    """Parts to try, best first: those named `filename` whose body size equals `size`
    (A22.3: two attachments with one name are told apart by size). A size of 0 means
    unknown, and when no same-named part has that size the same-named parts are all
    tried, in message order, as before."""
    named = [part for part in parts if part.get("filename") == filename]
    if size > 0:
        same_size = [part for part in named if int(part.get("body", {}).get("size") or 0) == size]
        if same_size:
            return same_size
        if len(named) > 1:
            logger.info("attachment_processor: no same-named part has size %d - taking the first of %d", size, len(named))
    return named


def _download_attachment_data(service: Any, email_id: str, filename: str, size: int = 0) -> Optional[bytes]:
    """
    Fetch the message, locate the part whose filename (and, when known, size) matches,
    and download its binary body. Returns None when the part or data cannot be found.

    The message is re-fetched every time on purpose: Gmail's attachmentId is
    not stable between messages.get calls, so a stored one cannot be reused.
    Two attachments with the same filename are told apart by `size`, the byte size
    stored in gmail_attachments.file_size (A22.3). Two with the same name AND size are
    taken as the same file: the first is used.
    """
    message = (
        service.users().messages().get(userId="me", id=email_id, format="full").execute()
    )
    for part in _select_attachment_parts(_walk_parts(message.get("payload", {})), filename, size):
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


# ── Ingestion-time extraction (A4) ────────────────────────────────────────────

def extract_full_text(service: Any, email_id: str, attachment: dict[str, Any]) -> Optional[str]:
    """Download one attachment and return its text, cut at MAX_EXTRACTED_TEXT_CHARS.

    Called by the ENP worker for every Gmail attachment it has not extracted yet.
    Returns None for a type with no extractor or a file over MAX_INGEST_ATTACHMENT_BYTES
    (nothing is downloaded), a file with no text (scanned PDF, password-protected) or a
    part that cannot be found. Gmail API errors are NOT caught here: the caller decides
    how to continue.
    """
    filename = attachment.get("filename") or ""
    mime_type = attachment.get("mime_type") or ""
    if not can_extract_text(mime_type, filename):
        return None
    if int(attachment.get("file_size") or 0) > MAX_INGEST_ATTACHMENT_BYTES:
        logger.info("attachment_processor: %s bytes is over the ingestion limit - skipped", attachment.get("file_size"))
        return None
    data = _download_attachment_data(service, email_id, filename, int(attachment.get("file_size") or 0))
    if not data:
        return None
    text = extract_text(data, mime_type, filename, max_chars=MAX_EXTRACTED_TEXT_CHARS)
    return text[:MAX_EXTRACTED_TEXT_CHARS] if text else None


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
                data = _download_attachment_data(
                    service, email_id, attachment["filename"], int(attachment.get("file_size") or 0)
                )
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
