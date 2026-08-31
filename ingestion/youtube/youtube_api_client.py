"""
youtube_api_client.py - YTC Module
"""

import asyncio
import logging
import json
import re
from html import unescape
from pathlib import Path
from typing import Optional

import httpx
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from ste.security import migrate_plaintext_file, write_encrypted_text

load_dotenv()
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent
TOKEN_PATH = PROJECT_ROOT / "token_youtube.enc"
LEGACY_TOKEN_PATH = PROJECT_ROOT / "token_youtube.json"
CREDENTIALS_PATH = PROJECT_ROOT / "credentials.json"

SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]
YOUTUBE_VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")


def is_valid_video_id(video_id: str) -> bool:
    return bool(YOUTUBE_VIDEO_ID_RE.fullmatch(video_id or ""))


def get_youtube_client(*, interactive: bool = False):
    """
    Build an authorized YouTube Data API client from the stored token.

    interactive=False (the default, and what the backend uses): never opens a
    browser consent flow. run_local_server() blocks the calling thread waiting
    for a redirect that will never arrive on a headless backend, which would
    hang video-metadata fetches forever. Missing or unrefreshable credentials
    raise instead — re-run scripts/test_youtube_auth.py to mint a new token.
    """
    creds = None
    token_json = migrate_plaintext_file(LEGACY_TOKEN_PATH, TOKEN_PATH)
    if token_json:
        creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            write_encrypted_text(TOKEN_PATH, creds.to_json())
        except Exception as exc:
            logger.error("Token refresh failed: %s", exc)
            creds = None

    if not creds or not creds.valid:
        if not CREDENTIALS_PATH.exists():
            raise FileNotFoundError(f"credentials.json not found at {CREDENTIALS_PATH}")
        if not interactive:
            raise RuntimeError(
                "No valid YouTube credentials — run scripts/test_youtube_auth.py "
                "to re-authorize (the backend never opens a consent flow itself)."
            )
        from google_auth_oauthlib.flow import InstalledAppFlow

        flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
        creds = flow.run_local_server(port=0)
        write_encrypted_text(TOKEN_PATH, creds.to_json())

    return build("youtube", "v3", credentials=creds)


def parse_iso8601_duration(duration: str) -> int:
    if not duration:
        return 0
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
    if not match:
        return 0
    return int(match.group(1) or 0) * 3600 + int(match.group(2) or 0) * 60 + int(match.group(3) or 0)


async def fetch_video_transcript(video_id: str) -> str:
    if not is_valid_video_id(video_id):
        logger.warning("Skipping transcript fetch for invalid YouTube video_id")
        return ""

    transcript_urls = [
        f"https://www.youtube.com/api/timedtext?v={video_id}&lang=en&fmt=json3",
        f"https://www.youtube.com/api/timedtext?v={video_id}&lang=en",
    ]
    for transcript_url in transcript_urls:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(transcript_url)
            if response.status_code != 200 or not response.text.strip():
                continue

            if "json" in response.headers.get("content-type", ""):
                payload = response.json()
                segments = []
                for event in payload.get("events", []):
                    for segment in event.get("segs", []):
                        text = segment.get("utf8", "").strip()
                        if text:
                            segments.append(text)
                transcript = " ".join(segments).strip()
                if transcript:
                    return transcript
            else:
                transcript = unescape(re.sub(r"<[^>]+>", " ", response.text))
                transcript = re.sub(r"\s+", " ", transcript).strip()
                if transcript:
                    return transcript
        except Exception as exc:
            logger.debug("Transcript fetch failed for %s: %s", video_id, exc)
    return ""


def _list_video_sync(video_id: str) -> dict:
    """Blocking YouTube Data API call — always run via asyncio.to_thread."""
    youtube = get_youtube_client()
    return youtube.videos().list(part="snippet,contentDetails", id=video_id).execute()


async def fetch_video_metadata(video_id: str) -> Optional[dict]:
    if not is_valid_video_id(video_id):
        logger.warning("Skipping metadata fetch for invalid YouTube video_id")
        return None

    try:
        # googleapiclient is synchronous: build() plus execute() would block the
        # FastAPI event loop for the whole round trip if awaited inline.
        response = await asyncio.to_thread(_list_video_sync, video_id)
        items = response.get("items", [])
        if not items:
            logger.warning("No metadata found for video_id=%s", video_id)
            return None

        item = items[0]
        snippet = item.get("snippet", {})
        content_details = item.get("contentDetails", {})
        duration_seconds = parse_iso8601_duration(content_details.get("duration", "PT0S"))
        transcript_text = await fetch_video_transcript(video_id)

        return {
            "title": snippet.get("title", ""),
            "description": snippet.get("description", ""),
            "channel_name": snippet.get("channelTitle", ""),
            "channel_id": snippet.get("channelId", ""),
            "published_at": snippet.get("publishedAt", ""),
            "duration_seconds": duration_seconds,
            "category_id": snippet.get("categoryId", ""),
            "tags": snippet.get("tags", []),
            "transcript_text": transcript_text,
        }

    except FileNotFoundError as exc:
        logger.error("YouTube auth setup incomplete: %s", exc)
        return None
    except RuntimeError as exc:
        # Actionable message from get_youtube_client — don't let the catch-all
        # below reduce it to a bare exception type name.
        logger.error("YouTube auth unavailable: %s", exc)
        return None
    except HttpError as exc:
        if exc.resp.status == 403:
            logger.error("YouTube API quota exceeded or OAuth scope insufficient")
        elif exc.resp.status == 404:
            logger.warning("Video not found: video_id=%s", video_id)
        else:
            logger.error("YouTube API HTTP error %s: %s", exc.resp.status, exc)
        return None
    except Exception as exc:
        logger.error("YouTube metadata fetch failed for video_id=%s: %s", video_id, type(exc).__name__)
        return None
