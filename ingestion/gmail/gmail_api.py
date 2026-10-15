"""
gmail_api.py — Gmail API access and email parsing
Echo Personal Memory System

    authenticate_gmail()              OAuth -> Gmail API service object
    fetch_and_store_new_emails(svc)   one poll: list -> find new -> parse -> store
    extract_body(message)             readable body text from a Gmail message
    extract_attachments(message, id)  attachment metadata (no file content)

What gets stored per email (via database.store_in_postgresql ->
ste.storage_engine.store_gmail_message):
    memory_items.raw_text   the body as readable text. HTML is converted to
                            text here WITH line breaks kept, because ENP's
                            cleaner (enp/text_cleaner.clean_gmail_text) removes
                            quoted replies and signatures line by line.
                            Quotes and signatures are NOT removed here: raw_text
                            keeps the full email; ENP decides what to embed.
    gmail_metadata          sender, recipients, labels, thread id, is_sent
    gmail_attachments       filename | mime | size only (Tier 1, CLAUDE.md §3)

Skipped on purpose: drafts (unsent typed text — privacy rule) and senders the
user excluded in settings.

Callers and threads:
    ingestion/gmail/router.poll_once   via asyncio.to_thread (poll loop + POST /poll)
    enp/attachment_processor           authenticate_gmail() for Tier-2 downloads

Thread safety: a googleapiclient service object is not thread-safe (httplib2
underneath). The poll loop and POST /poll can run at the same time, and they
share one service, so `_poll_lock` lets only one poll run at a time.
"""

import base64
import codecs
import json
import logging
import re
import threading
import time
import uuid
from datetime import datetime, timezone
from email.utils import formataddr, getaddresses, parsedate_to_datetime
from html import unescape
from pathlib import Path
from typing import Any, Callable

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from ste import postgresql_manager, settings_store
from ste.security import migrate_plaintext_file, write_encrypted_text
from .config import SCOPES
from .database import store_in_postgresql

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TOKEN_PATH = PROJECT_ROOT / "token_gmail.enc"
LEGACY_TOKEN_PATH = PROJECT_ROOT / "token_gmail.json"
CREDENTIALS_PATH = PROJECT_ROOT / "credentials.json"

_LIST_PAGE_SIZE = 100          # Gmail's messages.list page size
_MAX_API_ATTEMPTS = 4          # 1 call + 3 retries: waits 2 s, 4 s, 8 s
_FIRST_RETRY_DELAY_SECONDS = 2

_poll_lock = threading.Lock()

# Emails skipped because the sender is excluded: message id -> sender. Without
# this, every 30 s poll would download each excluded email again. The sender is
# re-checked every poll, so un-excluding a sender captures its emails again.
# Only touched inside fetch_and_store_new_emails, i.e. under _poll_lock.
_excluded_ids: dict[str, str] = {}


# ── Authentication ───────────────────────────────────────────────────────────

def authenticate_gmail():
    """Return an authorised Gmail API service.

    Token source: the encrypted local file token_gmail.enc (a plaintext
    token_gmail.json is migrated into it once). An expired token is refreshed
    and saved back. With no usable token, the browser consent flow runs —
    that blocks this thread until the user finishes signing in.
    """
    creds = None
    token_json = migrate_plaintext_file(LEGACY_TOKEN_PATH, TOKEN_PATH)
    if token_json:
        try:
            creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
        except Exception as exc:
            logger.warning("Stored Gmail token is unreadable — deleting it and signing in again: %s", exc)
            TOKEN_PATH.unlink(missing_ok=True)
            creds = None

    if creds and creds.valid:
        logger.info("Gmail authenticated from the stored token")
        return build("gmail", "v1", credentials=creds)

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            logger.info("Gmail access token refreshed")
        except Exception as exc:
            logger.warning("Gmail token refresh failed — signing in again: %s", exc)
            creds = None
    else:
        # No token, or a token that can't be refreshed (no refresh_token).
        creds = None

    if creds is None:
        logger.warning("No usable Gmail token — starting the browser sign-in flow")
        flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
        creds = flow.run_local_server(port=0)
        logger.info("Gmail sign-in completed")

    write_encrypted_text(TOKEN_PATH, creds.to_json())
    return build("gmail", "v1", credentials=creds)


# ── Body extraction ──────────────────────────────────────────────────────────

# Zero-width and soft-hyphen characters. Marketing mail pads its hidden
# preview text with them; they are invisible but break word matching.
_INVISIBLE_RE = re.compile("[­͏​-‏⁠﻿]")
_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
_DROP_BLOCK_RE = re.compile(r"<(script|style|head|title)\b[^>]*>.*?</\1\s*>", re.IGNORECASE | re.DOTALL)
# Tags that end a visual line. Turning them into "\n" keeps the line structure
# ENP needs ("On <date> ... wrote:" must stay on its own line).
_LINE_BREAK_TAG_RE = re.compile(
    r"<br\s*/?>|</(p|div|tr|li|h[1-6]|table|blockquote|ul|ol)\s*>|<(hr|li)\b[^>]*>",
    re.IGNORECASE,
)
_ANY_TAG_RE = re.compile(r"<[^>]+>")
_SPACES_RE = re.compile(r"[ \t\f\v\xa0]+")
_BLANK_LINES_RE = re.compile(r"\n{3,}")
_CHARSET_RE = re.compile(r"charset\s*=\s*\"?([\w.:-]+)", re.IGNORECASE)


def _part_charset(part: dict) -> str:
    """Charset from the part's Content-Type header; utf-8 when absent or unknown."""
    for header in part.get("headers", []) or []:
        if header.get("name", "").lower() == "content-type":
            match = _CHARSET_RE.search(header.get("value", ""))
            if match:
                try:
                    return codecs.lookup(match.group(1)).name
                except LookupError:
                    break
    return "utf-8"


def _decode_part_data(part: dict) -> str:
    """Decode a MIME part's inline base64url body, or "" when it carries none."""
    data = part.get("body", {}).get("data")
    if not data:
        return ""
    try:
        # Padding is optional in base64url; the decoder needs it.
        raw = base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))
    except (ValueError, TypeError) as exc:
        logger.warning("Undecodable %s part skipped: %s", part.get("mimeType"), exc)
        return ""
    return raw.decode(_part_charset(part), errors="replace")


def _find_mime_part(part: dict, target_mime: str) -> str:
    """Depth-first search for the first `target_mime` BODY part with inline data.

    Gmail nests the real body inside multipart/alternative — and inside
    multipart/mixed once a forward or an attachment is involved. Parts with a
    filename are attachments (an attached notes.txt is text/plain too) and are
    never taken as the body.
    """
    if part.get("mimeType") == target_mime and not part.get("filename"):
        text = _decode_part_data(part)
        if text:
            return text
    for child in part.get("parts", []) or []:
        found = _find_mime_part(child, target_mime)
        if found:
            return found
    return ""


def _normalize_text(text: str) -> str:
    """Unify line endings, drop invisible characters, collapse runs of spaces
    and blank lines. Line breaks themselves are kept."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _INVISIBLE_RE.sub("", text)
    lines = [_SPACES_RE.sub(" ", line).strip() for line in text.split("\n")]
    return _BLANK_LINES_RE.sub("\n\n", "\n".join(lines)).strip()


def _html_to_text(html: str) -> str:
    """HTML mail -> readable text with its line structure kept."""
    if not html:
        return ""
    text = _COMMENT_RE.sub(" ", html)          # includes Outlook's <!--[if mso]> blocks
    text = _DROP_BLOCK_RE.sub(" ", text)
    text = _LINE_BREAK_TAG_RE.sub("\n", text)
    text = _ANY_TAG_RE.sub(" ", text)          # also drops <img> tracking pixels
    return _normalize_text(unescape(text))


def extract_body(message: dict) -> str:
    """Return the email body as text, preferring text/plain and falling back to HTML."""
    payload = message.get("payload", {})
    plain = _find_mime_part(payload, "text/plain")
    if plain:
        return _normalize_text(plain)
    return _html_to_text(_find_mime_part(payload, "text/html"))


def extract_attachments(message: dict, message_id: str) -> list[dict[str, Any]]:
    """Collect every attachment part (any part with a filename), including
    ones nested inside multipart and inline images such as signature logos."""
    attachments: list[dict[str, Any]] = []

    def walk(part: dict) -> None:
        filename = (part.get("filename") or "").strip()
        body = part.get("body", {})
        if filename:
            attachments.append(
                {
                    "filename": filename,
                    "mime_type": part.get("mimeType", "application/octet-stream"),
                    # Size lives on body, not on the part itself.
                    "size": int(body.get("size") or 0),
                    "attachment_id": body.get("attachmentId"),
                }
            )
        for child in part.get("parts", []) or []:
            walk(child)

    walk(message.get("payload", {}))
    if attachments:
        logger.debug("Email %s has %d attachment(s)", message_id, len(attachments))
    return attachments


# ── Header parsing ───────────────────────────────────────────────────────────

def _headers(message: dict) -> dict[str, str]:
    """Header name (lowercase) -> value. Gmail returns header values already decoded."""
    wanted = {"subject", "from", "to", "date"}
    found: dict[str, str] = {}
    for header in message.get("payload", {}).get("headers", []) or []:
        name = header.get("name", "").lower()
        if name in wanted and name not in found:
            found[name] = header.get("value", "")
    return found


def _split_recipients(to_header: str) -> list[str]:
    """'A <a@x.com>, b@y.com' -> ['A <a@x.com>', 'b@y.com'] (one array element each)."""
    return [formataddr((name, addr)) for name, addr in getaddresses([to_header]) if addr]


def _received_at(message: dict, date_header: str) -> str:
    """When the email arrived, as an ISO-8601 UTC string.

    Gmail's internalDate (ms since epoch) is set by Gmail, so it is always
    present and always valid. The Date header is written by the sender's mail
    client: it can be missing, malformed or in a wrong timezone, so it is only
    the fallback.
    """
    internal_ms = message.get("internalDate")
    if internal_ms:
        return datetime.fromtimestamp(int(internal_ms) / 1000, tz=timezone.utc).isoformat()
    try:
        return parsedate_to_datetime(date_header).isoformat()
    except (TypeError, ValueError):
        return ""   # storage_engine logs a WARNING and uses the current time


# ── Polling ──────────────────────────────────────────────────────────────────

def _call_with_backoff(fn: Callable[[], Any], what: str) -> Any:
    """Run one Gmail API call; retry with exponential backoff (2 s, 4 s, 8 s)."""
    delay = _FIRST_RETRY_DELAY_SECONDS
    for attempt in range(1, _MAX_API_ATTEMPTS + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt == _MAX_API_ATTEMPTS:
                raise
            logger.warning("Gmail %s failed (attempt %d/%d), retrying in %d s: %s",
                           what, attempt, _MAX_API_ATTEMPTS, delay, exc)
            time.sleep(delay)
            delay *= 2


def _list_all_message_ids(service) -> list[str]:
    """Every message id in the mailbox (newest first, spam and trash excluded)."""
    ids: list[str] = []
    page_token = None
    while True:
        results = _call_with_backoff(
            lambda: service.users().messages()
            .list(userId="me", maxResults=_LIST_PAGE_SIZE, pageToken=page_token).execute(),
            "messages.list",
        )
        ids.extend(m["id"] for m in results.get("messages", []))
        page_token = results.get("nextPageToken")
        if not page_token:
            return ids


def _already_stored(message_ids: list[str]) -> set[str]:
    """Which of these ids are already in gmail_metadata — ONE query per page of
    ids instead of one query per email (each Neon round trip is ~0.2-0.9 s)."""
    stored: set[str] = set()
    for start in range(0, len(message_ids), _LIST_PAGE_SIZE):
        chunk = message_ids[start:start + _LIST_PAGE_SIZE]
        rows = postgresql_manager.fetchall(
            "SELECT email_id FROM gmail_metadata WHERE email_id = ANY(:ids)",
            {"ids": chunk},
        )
        stored.update(row["email_id"] for row in rows)
    return stored


def _still_excluded(message_id: str) -> bool:
    """True if this id was skipped for an excluded sender who is still excluded."""
    sender = _excluded_ids.get(message_id)
    if sender is None:
        return False
    if settings_store.is_sender_excluded(sender):
        return True
    del _excluded_ids[message_id]   # sender un-excluded: fetch it again
    return False


def _build_email_data(message_id: str, msg: dict, headers: dict[str, str], received_at: str) -> dict[str, Any]:
    """The dict store_gmail_message expects (keys read by ste/storage_engine.py)."""
    attachments = extract_attachments(msg, message_id)
    labels = msg.get("labelIds", [])
    return {
        "memory_id": str(uuid.uuid4()),
        "source_type": "gmail",
        "source_item_id": message_id,
        "title": headers.get("subject", ""),
        "content": {
            "primary_text": extract_body(msg),
            "attachments": attachments,
            "summary": None,
        },
        "time": {
            "event_timestamp": received_at,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        },
        "semantic": {},
        "classification": {},
        "interaction": {},
        "analytics": {},
        "regret": {"is_regret": False},
        "source_metadata": {
            "email": {
                "from": headers.get("from", ""),
                "to": _split_recipients(headers.get("to", "")),
                "labels": labels,
                "thread_id": msg.get("threadId"),
                "has_attachments": bool(attachments),
                # Sent-mail flag (gmail_metadata.is_sent) — distinguishes
                # writing time from reading time in wellbeing analytics
                # and enables "emails I sent to X" queries.
                "is_sent": "SENT" in labels,
            }
        },
        "source_link": f"https://mail.google.com/mail/u/0/#inbox/{message_id}",
    }


def fetch_and_store_new_emails(service) -> int:
    """One poll. Returns how many emails were newly stored.

    Steps: list every message id -> drop ids already stored -> download each
    new one -> skip drafts and excluded senders -> store oldest first (so
    created_at order matches arrival order when ENP picks them up).
    Errors are logged, never raised: the poll loop must keep running.
    """
    with _poll_lock:
        started = time.perf_counter()
        try:
            all_ids = _list_all_message_ids(service)
            stored_ids = _already_stored(all_ids)
            new_ids = [i for i in all_ids if i not in stored_ids and not _still_excluded(i)]

            drafts = excluded = 0
            new_emails: list[tuple[str, dict, dict[str, str], str]] = []
            for message_id in new_ids:
                msg = _call_with_backoff(
                    lambda: service.users().messages()
                    .get(userId="me", id=message_id, format="full").execute(),
                    "messages.get",
                )
                if "DRAFT" in msg.get("labelIds", []):
                    # Unsent typed text is never captured. Not cached: once sent,
                    # Gmail may keep the same id with the SENT label instead.
                    drafts += 1
                    continue
                headers = _headers(msg)
                if settings_store.is_sender_excluded(headers.get("from", "")):
                    _excluded_ids[message_id] = headers.get("from", "")
                    excluded += 1
                    logger.debug("Skipped email %s from an excluded sender", message_id)
                    continue
                received_at = _received_at(msg, headers.get("date", ""))
                new_emails.append((message_id, msg, headers, received_at))

            # ISO-8601 UTC strings sort in time order; "" (unknown) sorts first.
            new_emails.sort(key=lambda e: e[3])

            stored = 0
            for message_id, msg, headers, received_at in new_emails:
                if store_in_postgresql(_build_email_data(message_id, msg, headers, received_at)):
                    stored += 1
        except Exception:
            logger.exception("Gmail poll failed")
            return 0

        logger.info(
            "Gmail poll: %d in mailbox, %d new, %d stored, %d drafts skipped, "
            "%d excluded-sender skipped (%.1f s)",
            len(all_ids), len(new_ids), stored, drafts, excluded, time.perf_counter() - started,
        )
        return stored
