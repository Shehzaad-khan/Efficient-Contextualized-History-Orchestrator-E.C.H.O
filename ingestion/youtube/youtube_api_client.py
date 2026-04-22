"""
youtube_api_client.py - YTC Module
"""

import logging
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

load_dotenv()
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent
TOKEN_PATH = PROJECT_ROOT / "token_youtube.json"
CREDENTIALS_PATH = PROJECT_ROOT / "credentials.json"

SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]


def get_youtube_client():
    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")
        except Exception as exc:
            logger.error("Token refresh failed: %s", exc)
            creds = None

    if not creds or not creds.valid:
        if not CREDENTIALS_PATH.exists():
            raise FileNotFoundError(f"credentials.json not found at {CREDENTIALS_PATH}")
        from google_auth_oauthlib.flow import InstalledAppFlow

        flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
        creds = flow.run_local_server(port=0)
        TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")

    return build("youtube", "v3", credentials=creds)


def parse_iso8601_duration(duration: str) -> int:
    if not duration:
        return 0
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
    if not match:
        return 0
    return int(match.group(1) or 0) * 3600 + int(match.group(2) or 0) * 60 + int(match.group(3) or 0)


async def fetch_video_transcript(video_id: str) -> str:
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


async def fetch_video_metadata(video_id: str) -> Optional[dict]:
    try:
        youtube = get_youtube_client()
        response = youtube.videos().list(part="snippet,contentDetails", id=video_id).execute()
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
        logger.error("Auth setup incomplete: %s", exc)
        return _stub_metadata(video_id)
    except HttpError as exc:
        if exc.resp.status == 403:
            logger.error("YouTube API quota exceeded or OAuth scope insufficient")
        elif exc.resp.status == 404:
            logger.warning("Video not found: video_id=%s", video_id)
        else:
            logger.error("YouTube API HTTP error %s: %s", exc.resp.status, exc)
        return None
    except Exception as exc:
        logger.error("Unexpected error for video_id=%s: %s", video_id, exc)
        return _stub_metadata(video_id)


def _stub_metadata(video_id: str) -> dict:
    return {
        "title": f"[STUB] Video {video_id}",
        "description": "[STUB] OAuth not configured",
        "channel_name": "[STUB] Channel",
        "channel_id": "STUB_CHANNEL_ID",
        "published_at": "2026-01-01T00:00:00Z",
        "duration_seconds": 0,
        "category_id": "",
        "tags": [],
        "transcript_text": "",
    }
