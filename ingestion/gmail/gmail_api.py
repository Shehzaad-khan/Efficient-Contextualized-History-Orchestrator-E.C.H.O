"""
gmail_api.py — Gmail API access and email parsing
Echo Personal Memory System

    authenticate_gmail()              OAuth -> Gmail API service object
    sign_in_gmail()                   browser sign-in at backend startup if needed
    fetch_and_store_new_emails(svc)   one poll: find new ids -> parse -> store
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

Skipped on purpose: drafts (unsent typed text — privacy rule), senders the
user excluded in settings, and mail in Spam or Trash (A38).

Finding new mail (A5): Redis key echo:gmail:history_id holds the Gmail historyId the
last successful poll reached. A poll asks history.list for the messages added since
then (one cheap call when nothing arrived). With no saved id (first start, Redis lost
or down) or an id Gmail no longer knows (404), it makes ONE full listing of the mailbox
instead, after reading the mailbox's current historyId, so mail that arrives during
the listing is seen by the next poll. The id moves forward only when every new email
of the poll is stored; UNIQUE(source_type, source_id) makes any repeat harmless.

Callers and threads:
    backend.main                     sign_in_gmail() on a daemon thread at startup
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

from bs4 import BeautifulSoup
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from ste import postgresql_manager, redis_manager, settings_store
from ste.security import read_token_text, write_token_text
from .config import SCOPES
from .database import store_in_postgresql

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TOKEN_PATH = PROJECT_ROOT / "token_gmail.json"
CREDENTIALS_PATH = PROJECT_ROOT / "credentials.json"

_LIST_PAGE_SIZE = 100          # Gmail's messages.list page size
_HISTORY_PAGE_SIZE = 500       # history.list allows up to 500
_SKIPPED_LABELS = frozenset({"SPAM", "TRASH"})  # A38: Gmail's own spam marking is the filter
_MAX_API_ATTEMPTS = 4          # 1 call + 3 retries: waits 2 s, 4 s, 8 s
_FIRST_RETRY_DELAY_SECONDS = 2
_MIN_ATTACHMENT_IMAGE_BYTES = 10 * 1024

_poll_lock = threading.Lock()

# Emails skipped because the sender is excluded: message id -> sender. Without
# this, every 30 s poll would download each excluded email again. The sender is
# re-checked every poll, so un-excluding a sender captures its emails again.
# Only touched inside fetch_and_store_new_emails, i.e. under _poll_lock.
_excluded_ids: dict[str, str] = {}


# ── Authentication ───────────────────────────────────────────────────────────

class GmailAuthError(RuntimeError):
    """No usable Gmail token; finish the browser sign-in opened at startup."""


class GmailUnavailableError(RuntimeError):
    """The stored token may be fine, but Google could not be reached (network error).

    Not a GmailAuthError on purpose: sign_in_gmail() must not open a browser for it."""


# The token file is never deleted here. The poller and ENP threads call this too, and
# a deleted file would leave Gmail capture dead until a restart. A new sign-in
# overwrites the file atomically (write_token_text).
def _load_gmail_credentials() -> Credentials:
    """Load or refresh the stored Gmail token without opening a browser."""
    try:
        token_json = read_token_text(TOKEN_PATH)
    except Exception as exc:
        logger.warning("Stored Gmail token is unreadable: %s", type(exc).__name__)
        raise GmailAuthError("Stored Gmail token is unreadable; sign in again") from exc

    if token_json is None:
        raise GmailAuthError("No stored Gmail token; finish the browser sign-in opened at startup")

    try:
        creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
    except Exception as exc:
        logger.warning("Stored Gmail token is unreadable: %s", type(exc).__name__)
        raise GmailAuthError("Stored Gmail token is unreadable; sign in again") from exc

    if creds.valid:
        logger.info("Gmail authenticated from the stored token")
        return creds

    if creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except RefreshError as exc:
            # Google answered and refused: revoked or expired refresh token.
            logger.warning("Gmail token refresh was refused: %s", type(exc).__name__)
            raise GmailAuthError("Gmail token could not be refreshed; sign in again") from exc
        except Exception as exc:
            # Google was not reached (offline, DNS, timeout): the token may still be good.
            logger.warning("Gmail token refresh could not reach Google: %s", type(exc).__name__)
            raise GmailUnavailableError("Google could not be reached to refresh the Gmail token") from exc

        if not creds.valid:
            raise GmailAuthError("Gmail token refresh did not produce valid credentials")
        try:
            write_token_text(TOKEN_PATH, creds.to_json())
        except OSError as exc:
            # The refresh worked; an editor or sync client holding the file must not fail the poll.
            logger.warning("Refreshed Gmail token could not be saved (%s); using it in memory", type(exc).__name__)
        else:
            logger.info("Gmail access token refreshed")
        return creds

    raise GmailAuthError("No usable Gmail token; finish the browser sign-in opened at startup")


def authenticate_gmail():
    """Return an authorised Gmail API service or raise GmailAuthError.

    This function never opens a browser; sign_in_gmail() owns that flow.
    """
    return build("gmail", "v1", credentials=_load_gmail_credentials())


def sign_in_gmail() -> None:
    """Ensure a usable Gmail token exists, opening the browser when needed.

    Called once at backend startup on its own daemon thread. The token-file
    helpers lock only individual reads and writes, so no lock is held while the
    browser is open. Failures are logged and never raised.
    """
    try:
        _load_gmail_credentials()
        logger.info("Gmail token is already usable")
        return
    except GmailAuthError:
        pass
    except Exception as exc:
        logger.error("Gmail token check failed: %s", type(exc).__name__)
        return

    logger.warning("No usable Gmail token — starting the browser sign-in flow")
    try:
        flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
        creds = flow.run_local_server(port=0)
        write_token_text(TOKEN_PATH, creds.to_json())
    except Exception as exc:
        logger.error("Gmail sign-in failed: %s", type(exc).__name__)
        return
    logger.info("Gmail sign-in completed")


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
# Inline style properties that can hide an element (A6.1). Hiding done by a CSS class
# is not caught (accepted limitation).
_HIDING_PROPERTIES = ("display", "visibility", "max-height", "font-size", "opacity")
_IMPORTANT_RE = re.compile(r"\s*!\s*important\s*$", re.IGNORECASE)
# 0, 0.0, .0, 0px, 0em, 0% ...: a zero length with an optional unit.
_ZERO_LENGTH_RE = re.compile(r"^[+-]?(0+(\.0*)?|\.0+)(px|pt|em|rem|ex|ch|vh|vw|%)?$")
# A child of these can show its own text even when its parent sets font-size:0 or
# visibility:hidden, and its text may be styled by a class we cannot see.
_BLOCK_TAGS = frozenset({
    "p", "div", "table", "tbody", "thead", "tfoot", "tr", "td", "th", "ul", "ol", "li",
    "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "section", "article", "header",
    "footer", "center", "pre", "form",
})


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
    # Parsing with BeautifulSoup is ~30x slower than the regexes below, so a mail whose
    # text never mentions a hiding property is not parsed at all (same output).
    if _may_contain_hidden_element(html):
        soup = BeautifulSoup(html, "html.parser")
        removed_hidden_element = False
        for element in soup.find_all(style=True):
            # An element inside an already removed hidden parent is destroyed with it.
            if element.decomposed:
                continue
            if _element_is_hidden(element):
                element.decompose()
                removed_hidden_element = True
        if removed_hidden_element:
            html = str(soup)

    # Keep this regex pipeline stable: its newlines preserve reply/signature
    # boundaries for ENP's line-oriented cleaner.
    text = _COMMENT_RE.sub(" ", html)          # includes Outlook's <!--[if mso]> blocks
    text = _DROP_BLOCK_RE.sub(" ", text)
    text = _LINE_BREAK_TAG_RE.sub("\n", text)
    text = _ANY_TAG_RE.sub(" ", text)          # also drops <img> tracking pixels
    return _normalize_text(unescape(text))


def _may_contain_hidden_element(html: str) -> bool:
    """Cheap pre-check: an inline style that names one of the hiding properties exists."""
    lowered = html.lower()
    return "style" in lowered and any(name in lowered for name in _HIDING_PROPERTIES)


def _style_declarations(style: str) -> dict[str, str]:
    """'a: b; c: d !important' -> {'a': 'b', 'c': 'd'} (lower case, !important removed)."""
    declarations: dict[str, str] = {}
    for declaration in style.split(";"):
        name, separator, value = declaration.partition(":")
        if separator:
            declarations[name.strip().lower()] = _IMPORTANT_RE.sub("", value.strip().lower()).strip()
    return declarations


def _declaration_hides(name: str, value: str) -> bool:
    if name == "display":
        return value == "none"
    if name == "visibility":
        return value in {"hidden", "collapse"}
    if name in {"opacity", "max-height", "font-size"}:
        return bool(_ZERO_LENGTH_RE.match(value))
    return False


def _has_visible_descendant(element: Any, property_names: list[str]) -> bool:
    """True when something inside the element can still be seen even though the
    element itself sets a hiding value: a block-level child, or a child that
    re-declares the property with a visible value."""
    for child in element.find_all(True):
        if child.name in _BLOCK_TAGS:
            return True
        child_declarations = _style_declarations(child.get("style", ""))
        for name in property_names:
            if name in child_declarations and not _declaration_hides(name, child_declarations[name]):
                return True
    return False


def _element_is_hidden(element: Any) -> bool:
    """Whether an element's inline style hides it from the reader (A6.1).

    display:none and opacity:0 hide the whole subtree and a child cannot undo them;
    max-height:0 does so only together with overflow:hidden. visibility:hidden and
    font-size:0 are overridable by children (Outlook templates wrap real text in
    font-size:0 cells), so the element is dropped only when nothing inside it can show.
    """
    declarations = _style_declarations(element.get("style", ""))
    hiding = [name for name in _HIDING_PROPERTIES if name in declarations and _declaration_hides(name, declarations[name])]
    if not hiding:
        return False
    overflow_clipped = declarations.get("overflow") in {"hidden", "clip"} or declarations.get("overflow-y") in {"hidden", "clip"}
    if "display" in hiding or "opacity" in hiding or ("max-height" in hiding and overflow_clipped):
        return True
    overridable = [name for name in hiding if name in {"visibility", "font-size"}]
    return bool(overridable) and not _has_visible_descendant(element, overridable)


def _prefer_plain(body_parts: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """When siblings carry the same body twice (text/plain next to text/html), keep the plain one."""
    if any(kind == "text/plain" for kind, _ in body_parts):
        return [entry for entry in body_parts if entry[0] == "text/plain"]
    return body_parts


def _collect_body_parts(part: dict, include_attached_messages: bool = False) -> list[tuple[str, str]]:
    """Collect ordered body text, choosing one representation per alternative.

    An attached message (message/rfc822: a bounce's original, "forward as attachment")
    is skipped unless include_attached_messages is set; extract_body sets it only when
    the mail has no other text, so a bounce does not repeat the original it quotes.
    """
    def walk(current: dict, seen: set[int]) -> list[tuple[str, str]]:
        if not isinstance(current, dict) or id(current) in seen:
            return []
        seen.add(id(current))

        mime_type = str(current.get("mimeType") or "").lower()
        is_attached_message = mime_type == "message/rfc822"
        if is_attached_message and not include_attached_messages:
            return []

        filename = current.get("filename")
        if filename and str(filename).strip() and not is_attached_message:
            return []

        if mime_type in {"text/plain", "text/html"}:
            text = _decode_part_data(current)
            text = _normalize_text(text) if mime_type == "text/plain" else _html_to_text(text)
            return [(mime_type, text)] if text else []

        children = current.get("parts", []) or []
        if not isinstance(children, (list, tuple)):
            return []

        if mime_type == "multipart/alternative":
            first_html = None
            for child in children:
                branch_seen = seen.copy()
                branch_parts = walk(child, branch_seen)
                if not branch_parts:
                    continue
                candidate = (branch_parts, branch_seen)
                if any(kind == "text/plain" for kind, _ in branch_parts):
                    seen.update(branch_seen)
                    return branch_parts
                if first_html is None and any(kind == "text/html" for kind, _ in branch_parts):
                    first_html = candidate
            if first_html is not None:
                seen.update(first_html[1])
                return first_html[0]
            return []

        body_parts = []
        for child in children:
            body_parts.extend(walk(child, seen))
        return _prefer_plain(body_parts)

    return walk(part, set())


def extract_body(message: dict) -> str:
    """Return all ordered body text, with one representation per alternative."""
    payload = message.get("payload", {})
    body_parts = _collect_body_parts(payload)
    if not body_parts:
        # A mail that is only an attached message ("forward as attachment", no note).
        body_parts = _collect_body_parts(payload, include_attached_messages=True)
    return _normalize_text("\n\n".join(text for _, text in body_parts))


def extract_attachments(message: dict, message_id: str) -> list[dict[str, Any]]:
    """Collect filename-bearing attachments, excluding inline and small images."""
    attachments: list[dict[str, Any]] = []

    def walk(part: dict) -> None:
        filename = (part.get("filename") or "").strip()
        body = part.get("body", {})
        if filename:
            disposition = ""
            content_id_present = False
            for header in part.get("headers", []) or []:
                name = str(header.get("name") or "").lower()
                value = str(header.get("value") or "")
                if name == "content-disposition":
                    disposition = value.strip().lower()
                elif name == "content-id":
                    content_id_present = True

            mime_type = part.get("mimeType", "application/octet-stream")
            # Size lives on body, not on the part itself.
            size = int(body.get("size") or 0)
            disposition_kind = disposition.split(";", 1)[0].strip()
            is_inline = disposition.startswith("inline")
            has_non_attachment_content_id = content_id_present and disposition_kind != "attachment"
            # An unknown size (0) is not "under 10 KB": keep it rather than drop a real image.
            is_small_image = (
                str(mime_type).lower().startswith("image/")
                and 0 < size < _MIN_ATTACHMENT_IMAGE_BYTES
            )
            if not (is_inline or has_non_attachment_content_id or is_small_image):
                attachments.append(
                    {
                        "filename": filename,
                        "mime_type": mime_type,
                        "size": size,
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

def _call_with_backoff(fn: Callable[[], Any], what: str, no_retry_statuses: tuple[int, ...] = ()) -> Any:
    """Run one Gmail API call; retry with exponential backoff (2 s, 4 s, 8 s).

    An HTTP error whose status is in `no_retry_statuses` is raised at once: the answer
    will not change (history.list's 404 for a historyId Gmail has forgotten)."""
    delay = _FIRST_RETRY_DELAY_SECONDS
    for attempt in range(1, _MAX_API_ATTEMPTS + 1):
        try:
            return fn()
        except Exception as exc:
            if isinstance(exc, HttpError) and exc.resp.status in no_retry_statuses:
                raise
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


def _current_history_id(service) -> str:
    """The mailbox's current historyId (what a full listing is "as of")."""
    profile = _call_with_backoff(lambda: service.users().getProfile(userId="me").execute(), "getProfile")
    return str(profile["historyId"])


def _list_new_ids_from_history(service, start_history_id: str) -> tuple[list[str], str]:
    """Ids of messages added since `start_history_id` (oldest first, no Spam/Trash), and the
    historyId to save once they are stored. Raises HttpError 404 when Gmail no longer knows
    the start id: the caller then falls back to a full listing."""
    ids: list[str] = []
    seen: set[str] = set()
    page_token = None
    latest = start_history_id
    while True:
        response = _call_with_backoff(
            lambda: service.users().history()
            .list(userId="me", startHistoryId=start_history_id, historyTypes=["messageAdded"],
                  maxResults=_HISTORY_PAGE_SIZE, pageToken=page_token).execute(),
            "history.list",
            no_retry_statuses=(404,),
        )
        for record in response.get("history", []):
            for added in record.get("messagesAdded", []):
                message = added.get("message", {})
                message_id = message.get("id")
                if not message_id or message_id in seen:
                    continue
                seen.add(message_id)
                if _SKIPPED_LABELS.intersection(message.get("labelIds", [])):
                    continue
                ids.append(message_id)
        latest = str(response.get("historyId") or latest)
        page_token = response.get("nextPageToken")
        if not page_token:
            return ids, latest


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

    Steps: find candidate ids (history since the saved historyId, else a full listing)
    -> drop ids already stored -> download each new one -> skip drafts, Spam/Trash and
    excluded senders -> store oldest first (so created_at order matches arrival order
    when ENP picks them up) -> save the new historyId if everything was stored.
    Errors are logged, never raised: the poll loop must keep running.
    """
    with _poll_lock:
        started = time.perf_counter()
        try:
            saved_history_id = redis_manager.get_gmail_history_id()
            candidate_ids: list[str] | None = None
            new_history_id = ""
            mode = "history"
            if saved_history_id:
                try:
                    candidate_ids, new_history_id = _list_new_ids_from_history(service, saved_history_id)
                except HttpError as exc:
                    if exc.resp.status != 404:
                        raise
                    logger.warning("Gmail history id is too old; listing the whole mailbox once")
            if candidate_ids is None:
                mode = "full"
                # Read BEFORE listing: mail that arrives while the listing runs is then
                # inside the next history window instead of falling between the two.
                new_history_id = _current_history_id(service)
                candidate_ids = _list_all_message_ids(service)
            else:
                # Emails skipped for an excluded sender are re-checked, so un-excluding a
                # sender still captures them (a full listing finds them by itself).
                known = set(candidate_ids)
                candidate_ids += [i for i in _excluded_ids if i not in known]

            stored_ids = _already_stored(candidate_ids)
            new_ids = [i for i in candidate_ids if i not in stored_ids and not _still_excluded(i)]

            drafts = excluded = spam = 0
            new_emails: list[tuple[str, dict, dict[str, str], str]] = []
            for message_id in new_ids:
                msg = _call_with_backoff(
                    lambda: service.users().messages()
                    .get(userId="me", id=message_id, format="full").execute(),
                    "messages.get",
                )
                labels = msg.get("labelIds", [])
                if _SKIPPED_LABELS.intersection(labels):
                    spam += 1  # trashed or marked spam after it arrived
                    continue
                if "DRAFT" in labels:
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

            # store_in_postgresql returns False both for "already stored" and for an error,
            # so ask the database. Move the saved historyId only when nothing was lost.
            emails_saved = True
            if new_emails:
                present = _already_stored([email[0] for email in new_emails])
                emails_saved = all(email[0] in present for email in new_emails)
            if not emails_saved:
                logger.warning("Gmail poll: some emails were not stored; the next poll will retry them")
            elif new_history_id and new_history_id != saved_history_id:
                redis_manager.set_gmail_history_id(new_history_id)
        except Exception:
            logger.exception("Gmail poll failed")
            return 0

        logger.info(
            "Gmail poll (%s): %d candidates, %d new, %d stored, %d drafts skipped, "
            "%d spam/trash skipped, %d excluded-sender skipped (%.1f s)",
            mode, len(candidate_ids), len(new_ids), stored, drafts, spam, excluded, time.perf_counter() - started,
        )
        return stored
