"""
youtube_api_client.py — YouTube Data API metadata + transcript (architecture §7, YTC module)
Echo Personal Memory System

    sign_in_youtube()             once at backend startup: browser sign-in if no usable token
    get_youtube_client()          OAuth -> YouTube Data API service object
    fetch_video_metadata(id)      videos.list (snippet, contentDetails) + transcript -> dict
    fetch_video_transcript(id)    best-effort caption text, "" when none
    parse_iso8601_duration(s)     "PT1H2M3S" -> 3723
    is_valid_video_id(id)         11 chars of [A-Za-z0-9_-]

Caller: ingestion/youtube/youtube_connector._fill_metadata, after the
/ytc/video-detected response. The dict goes to ste.storage_engine.update_youtube_metadata
(raw_text = description + transcript + tags; the rest -> youtube_metadata).

Token storage — same scheme as ingestion/gmail/gmail_api.authenticate_gmail:
encrypted local file token_youtube.enc (a plaintext token_youtube.json is migrated
into it once), an expired token is refreshed and saved back.

Sign-in: like Gmail, the browser sign-in happens once, at backend startup.
backend/main.py runs sign_in_youtube() on its own daemon thread; it opens the
browser only when the stored token is missing or can't be refreshed. The
per-video path (get_youtube_client -> _load_credentials) never opens the browser:
it runs inside a background job, and run_local_server() would block that thread
until the user finishes signing in. Until the sign-in completes, video jobs get
YouTubeAuthError and skip the metadata fetch.

Threads: get_youtube_client() is blocking and runs in asyncio.to_thread worker
threads, several at once when videos arrive together. The credentials are cached
in-process under `_creds_lock`, so the token file is read and refreshed once, not
per video. Each call builds its own service object (googleapiclient services are
not thread-safe); build() uses the bundled discovery document, no network.

Transcript: best-effort (doc §7 Step 3). Known gap, tracker A11: YouTube now
returns an empty 200 for timedtext requests that lack a proof-of-origin token, so
in practice this returns "" for every video (checked 2026-10-17).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
import xml.etree.ElementTree as ET
from html import unescape
from pathlib import Path
from typing import Any, Optional

import httpx
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from ste.security import migrate_plaintext_file, write_encrypted_text

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TOKEN_PATH = PROJECT_ROOT / "token_youtube.enc"
LEGACY_TOKEN_PATH = PROJECT_ROOT / "token_youtube.json"
CREDENTIALS_PATH = PROJECT_ROOT / "credentials.json"

SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]
YOUTUBE_VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")

# ISO 8601 duration as the Data API sends it: "PT4M13S", "PT1H", "P1DT2H3M" (live
# streams longer than a day), "P0D" (a stream that is live right now).
_ISO_DURATION_RE = re.compile(r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$")

_TRANSCRIPT_TIMEOUT_SECONDS = 10.0
# json3 first (plain JSON, no escaping), then the classic XML format.
_TRANSCRIPT_URL_TEMPLATES = (
    "https://www.youtube.com/api/timedtext?v={video_id}&lang=en&fmt=json3",
    "https://www.youtube.com/api/timedtext?v={video_id}&lang=en",
)
# Caption markup such as <font color="#E5E5E5">. Requires a letter after "<" so
# caption text like "I <3 this" is not eaten.
_CAPTION_TAG_RE = re.compile(r"</?[A-Za-z][^>]*>")

_creds: Optional[Credentials] = None
_creds_lock = threading.Lock()


class YouTubeAuthError(RuntimeError):
    """No usable YouTube token. The fix is always the same: finish the browser
    sign-in that sign_in_youtube() opens at backend startup (or restart the backend)."""


def is_valid_video_id(video_id: str) -> bool:
    return bool(YOUTUBE_VIDEO_ID_RE.fullmatch(video_id or ""))


# ── Authentication ───────────────────────────────────────────────────────────

def _load_credentials() -> Credentials:
    """Return valid credentials: cached -> stored token -> refreshed token.
    Raises YouTubeAuthError when none of those works. Same steps as
    authenticate_gmail, minus the browser sign-in (see module docstring)."""
    global _creds
    with _creds_lock:
        if _creds is not None and _creds.valid:
            return _creds

        creds = _creds
        if creds is None:
            token_json = migrate_plaintext_file(LEGACY_TOKEN_PATH, TOKEN_PATH)
            if token_json:
                try:
                    creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
                except Exception as exc:
                    # Not deleted (Gmail deletes it because it signs in right after);
                    # the auth script overwrites it.
                    logger.warning("Stored YouTube token is unreadable: %s", exc)
                    creds = None
            if creds is not None and creds.valid:
                logger.info("YouTube authenticated from the stored token")
                _creds = creds
                return creds

        if creds is not None and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                logger.info("YouTube access token refreshed")
            except Exception as exc:
                logger.warning("YouTube token refresh failed: %s", exc)
                creds = None
        else:
            # No token, or a token that can't be refreshed (no refresh_token).
            creds = None

        if creds is None:
            _creds = None
            raise YouTubeAuthError(
                "No usable YouTube token — finish the browser sign-in opened at backend "
                "startup, or restart the backend to open it again."
            )

        write_encrypted_text(TOKEN_PATH, creds.to_json())
        _creds = creds
        return creds


def sign_in_youtube() -> None:
    """Make sure a usable YouTube token exists; open the browser sign-in if not.

    Called once, by backend/main.py at startup, on its own daemon thread. Blocks
    that thread until the user finishes (or abandons) the sign-in; a daemon
    thread never holds up backend shutdown. Never raises: failures are logged,
    and video jobs keep raising YouTubeAuthError until the next startup.
    _creds_lock is NOT held while the browser is open, so video jobs fail fast
    instead of queueing behind the sign-in.
    """
    global _creds
    try:
        _load_credentials()
        return
    except YouTubeAuthError:
        pass  # expected: no token, or one that can't be refreshed -> sign in below
    except Exception:
        logger.exception("YouTube token check failed; skipping the sign-in")
        return

    logger.warning("No usable YouTube token — starting the browser sign-in flow")
    try:
        flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
        # port=0: the OS picks a free port, so it can't clash with Gmail's sign-in.
        creds = flow.run_local_server(port=0)
        with _creds_lock:
            write_encrypted_text(TOKEN_PATH, creds.to_json())
            _creds = creds
    except Exception:
        logger.exception("YouTube sign-in failed; video metadata stays off until the next backend start")
        return
    logger.info("YouTube sign-in completed")


def get_youtube_client():
    """Return an authorised YouTube Data API service. Blocking — run it via
    asyncio.to_thread. Raises YouTubeAuthError when there is no usable token."""
    return build("youtube", "v3", credentials=_load_credentials())


# ── Parsing helpers ──────────────────────────────────────────────────────────

def parse_iso8601_duration(duration: str) -> int:
    """"PT1H2M3S" -> 3723 seconds. Empty or unparseable -> 0 (stored as 0;
    completion_rate is only computed when duration > 0)."""
    match = _ISO_DURATION_RE.fullmatch(duration or "")
    if not match:
        if duration:
            logger.debug("Unparseable YouTube duration %r", duration)
        return 0
    days, hours, minutes, seconds = (int(group or 0) for group in match.groups())
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _parse_json3_transcript(body: str) -> str:
    """fmt=json3: {"events": [{"segs": [{"utf8": "text"}, ...]}, ...]}.
    Auto-captions split a line into word-sized segs; "\\n" segs are line breaks."""
    payload = json.loads(body)
    segments = []
    for event in payload.get("events", []):
        for segment in event.get("segs") or []:
            text = segment.get("utf8", "").strip()
            if text:
                segments.append(text)
    return _normalize_whitespace(" ".join(segments))


def _parse_xml_transcript(body: str) -> str:
    """Classic format <transcript><text start=".." dur="..">..</text></transcript>,
    or srv3 <timedtext><body><p><s>..</s></p></body></timedtext>.

    The text inside is HTML-escaped twice ("&amp;#39;" for an apostrophe): the
    XML parser undoes one level, html.unescape the second."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        logger.debug("Transcript XML did not parse; falling back to tag stripping")
        return _normalize_whitespace(_CAPTION_TAG_RE.sub(" ", unescape(unescape(body))))

    lines = []
    for element in root.iter():
        if element.tag in ("text", "p"):
            line = _CAPTION_TAG_RE.sub(" ", unescape("".join(element.itertext())))
            line = _normalize_whitespace(line)
            if line:
                lines.append(line)
    return " ".join(lines)


def _parse_transcript(body: str) -> str:
    # Sniff the body instead of trusting Content-Type (an empty reply is still
    # served as text/html).
    stripped = body.lstrip()
    if stripped.startswith("{"):
        return _parse_json3_transcript(stripped)
    if stripped.startswith("<"):
        return _parse_xml_transcript(stripped)
    return ""


# ── Fetching ─────────────────────────────────────────────────────────────────

async def fetch_video_transcript(video_id: str) -> str:
    """Best-effort English caption text; "" when none is available (doc §7 Step 3)."""
    if not is_valid_video_id(video_id):
        logger.warning("Skipping transcript fetch for invalid YouTube video_id")
        return ""

    async with httpx.AsyncClient(timeout=_TRANSCRIPT_TIMEOUT_SECONDS) as client:
        for template in _TRANSCRIPT_URL_TEMPLATES:
            url = template.format(video_id=video_id)
            try:
                response = await client.get(url)
            except httpx.HTTPError as exc:
                logger.debug("Transcript request failed for video_id=%s: %s", video_id, type(exc).__name__)
                continue
            if response.status_code != 200 or not response.text.strip():
                continue
            try:
                transcript = _parse_transcript(response.text)
            except (ValueError, TypeError, AttributeError) as exc:
                # json.JSONDecodeError is a ValueError; TypeError/AttributeError = an
                # unexpected JSON shape (e.g. "events": 5 or a list of strings).
                logger.debug("Transcript parse failed for video_id=%s: %s", video_id, exc)
                continue
            if transcript:
                logger.debug("Transcript fetched for video_id=%s (%d chars)", video_id, len(transcript))
                return transcript

    logger.debug("No transcript available for video_id=%s", video_id)
    return ""


def _list_video_sync(video_id: str) -> dict[str, Any]:
    """Blocking YouTube Data API call — always run via asyncio.to_thread."""
    youtube = get_youtube_client()
    return youtube.videos().list(part="snippet,contentDetails", id=video_id).execute()


async def fetch_video_metadata(video_id: str) -> Optional[dict[str, Any]]:
    """Return the metadata dict for update_youtube_metadata, or None on any
    failure (logged here, so the caller only has to skip the update)."""
    if not is_valid_video_id(video_id):
        logger.warning("Skipping metadata fetch for invalid YouTube video_id")
        return None

    try:
        # googleapiclient is synchronous: build() plus execute() would block the
        # FastAPI event loop for the whole round trip if awaited inline.
        response = await asyncio.to_thread(_list_video_sync, video_id)
    except YouTubeAuthError as exc:
        logger.error("YouTube auth unavailable: %s", exc)
        return None
    except HttpError as exc:
        status = exc.resp.status
        reason = getattr(exc, "reason", "")
        if status == 403:
            logger.error("YouTube API refused (403: quota exceeded or scope missing): %s", reason)
        elif status == 404:
            logger.warning("YouTube API: video not found, video_id=%s", video_id)
        else:
            logger.error("YouTube API HTTP %s for video_id=%s: %s", status, video_id, reason)
        return None
    except Exception as exc:
        # Type name only: some google-auth/httplib2 errors echo request details.
        logger.error("YouTube metadata fetch failed for video_id=%s: %s", video_id, type(exc).__name__)
        return None

    items = response.get("items", [])
    if not items:
        # Private, deleted, or age/region-restricted for this account.
        logger.warning("No metadata returned for video_id=%s (private or deleted?)", video_id)
        return None

    item = items[0]
    snippet = item.get("snippet", {})
    content_details = item.get("contentDetails", {})
    duration_seconds = parse_iso8601_duration(content_details.get("duration", ""))
    transcript_text = await fetch_video_transcript(video_id)

    metadata = {
        "video_id": video_id,  # saves storage_engine a lookup by memory_id
        "title": snippet.get("title", ""),
        "description": snippet.get("description", ""),
        "channel_name": snippet.get("channelTitle", ""),
        "channel_id": snippet.get("channelId", ""),
        "published_at": snippet.get("publishedAt", ""),
        "duration_seconds": duration_seconds,
        "category_id": snippet.get("categoryId", ""),
        "tags": snippet.get("tags", []),
        "transcript_text": transcript_text,
        # No "is_short": the detection already stored it from the URL, and
        # storage keeps the stored value when this key is missing.
    }
    logger.info(
        "YouTube metadata fetched: video_id=%s duration=%ss category=%s tags=%d transcript=%d chars",
        video_id,
        duration_seconds,
        metadata["category_id"],
        len(metadata["tags"]),
        len(transcript_text),
    )
    return metadata
