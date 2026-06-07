"""
Gmail API module - Handles Gmail authentication and email extraction.
"""

import base64
import json
import time
import uuid
from datetime import datetime
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from ste import postgresql_manager
from ste.security import decrypt_text, encrypt_text, migrate_plaintext_file, write_encrypted_text
from .config import SCOPES, get_redis_client
from .database import store_in_postgresql

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TOKEN_PATH = PROJECT_ROOT / "token_gmail.enc"
LEGACY_TOKEN_PATH = PROJECT_ROOT / "token_gmail.json"
CREDENTIALS_PATH = PROJECT_ROOT / "credentials.json"
REDIS_TOKEN_KEY = "gmail_token"


def _load_stored_token_json() -> str | None:
    return migrate_plaintext_file(LEGACY_TOKEN_PATH, TOKEN_PATH)


def _save_token_json(token_json: str) -> None:
    write_encrypted_text(TOKEN_PATH, token_json)


def _cache_token_json(redis_client, token_json: str) -> None:
    if redis_client:
        redis_client.setex(REDIS_TOKEN_KEY, 3600, encrypt_text(token_json))


def authenticate_gmail():
    creds = None
    rc = get_redis_client()
    if rc:
        try:
            cached_token = rc.get(REDIS_TOKEN_KEY)
            if cached_token:
                token_json = decrypt_text(cached_token)
                creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
                if creds.valid:
                    return build("gmail", "v1", credentials=creds)
        except Exception:
            print("Failed to use cached token; falling back to stored token")

    token_json = _load_stored_token_json()
    if token_json:
        try:
            creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
            if creds.valid and rc:
                try:
                    _cache_token_json(rc, token_json)
                except Exception:
                    print("Failed to refresh encrypted token cache")
        except Exception:
            print("Invalid encrypted Gmail token; re-authenticating")
            TOKEN_PATH.unlink(missing_ok=True)
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception:
                creds = None

        if not creds:
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
            creds = flow.run_local_server(port=0)

        _save_token_json(creds.to_json())
        if rc:
            try:
                _cache_token_json(rc, creds.to_json())
            except Exception:
                print("Failed to cache token in Redis")

    return build("gmail", "v1", credentials=creds)


def extract_body(message):
    payload = message.get("payload", {})
    parts = payload.get("parts", [])

    if parts:
        for part in parts:
            if part.get("mimeType") == "text/plain":
                data = part.get("body", {}).get("data")
                if data:
                    try:
                        return base64.urlsafe_b64decode(data).decode("utf-8")
                    except Exception:
                        pass
    else:
        data = payload.get("body", {}).get("data")
        if data:
            try:
                return base64.urlsafe_b64decode(data).decode("utf-8")
            except Exception:
                pass

    return ""


def extract_attachments(message, message_id):
    attachments = []
    payload = message.get("payload", {})
    parts = payload.get("parts", [])

    for part in parts:
        filename = part.get("filename", "")
        if filename and filename.strip():
            attachments.append(
                {
                    "filename": filename,
                    "mime_type": part.get("mimeType", "application/octet-stream"),
                    "size": int(part.get("size", 0)),
                }
            )

    return attachments


def _call_with_backoff(fn, max_retries: int = 4):
    delay = 2
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception:
            if attempt == max_retries - 1:
                raise
            time.sleep(delay)
            delay *= 2


def fetch_and_store_new_emails(service):
    try:
        processed_count = 0
        all_messages = []
        page_token = None

        # Fetch all messages with pagination
        while True:
            results = _call_with_backoff(
                lambda: service.users().messages().list(userId="me", maxResults=100, pageToken=page_token).execute()
            )
            messages = results.get("messages", [])

            if not messages:
                break

            all_messages.extend(messages)
            page_token = results.get("nextPageToken")
            if not page_token:
                break

        if not all_messages:
            print("No new emails found")
            return 0

        # Fetch full message data and collect unprocessed emails with timestamps
        unprocessed_emails = []
        for message in all_messages:
            message_id = message["id"]
            existing = postgresql_manager.fetchone(
                """
                SELECT memory_id
                FROM gmail_metadata
                WHERE email_id = :email_id
                """,
                {"email_id": message_id},
            )
            if existing:
                continue

            msg = _call_with_backoff(
                lambda: service.users().messages().get(userId="me", id=message_id, format="full").execute()
            )
            headers = msg["payload"]["headers"]

            subject = ""
            sender = ""
            to = ""
            date = ""

            for header in headers:
                if header["name"] == "Subject":
                    subject = header["value"]
                elif header["name"] == "From":
                    sender = header["value"]
                elif header["name"] == "To":
                    to = header["value"]
                elif header["name"] == "Date":
                    date = header["value"]

            unprocessed_emails.append({
                "message_id": message_id,
                "msg": msg,
                "subject": subject,
                "sender": sender,
                "to": to,
                "date": date,
            })

        # Sort by date (oldest first)
        try:
            from email.utils import parsedate_to_datetime
            unprocessed_emails.sort(key=lambda e: parsedate_to_datetime(e["date"]))
        except Exception:
            print("Warning: Could not sort by date, processing in fetched order")

        # Process emails in chronological order
        for email_info in unprocessed_emails:
            message_id = email_info["message_id"]
            msg = email_info["msg"]
            subject = email_info["subject"]
            sender = email_info["sender"]
            to = email_info["to"]
            date = email_info["date"]

            attachments = extract_attachments(msg, message_id)
            email_data = {
                "memory_id": str(uuid.uuid4()),
                "source_type": "gmail",
                "source_item_id": message_id,
                "title": subject,
                "content": {
                    "primary_text": extract_body(msg),
                    "attachments": attachments,
                    "summary": None,
                },
                "time": {
                    "event_timestamp": date,
                    "ingested_at": datetime.utcnow().isoformat(),
                },
                "semantic": {},
                "classification": {},
                "interaction": {},
                "analytics": {},
                "regret": {"is_regret": False},
                "source_metadata": {
                    "email": {
                        "from": sender,
                        "to": [to] if to else [],
                        "labels": msg.get("labelIds", []),
                        "thread_id": msg.get("threadId"),
                        "has_attachments": bool(attachments),
                    }
                },
                "source_link": f"https://mail.google.com/mail/u/0/#inbox/{message_id}",
            }

            if store_in_postgresql(email_data):
                processed_count += 1

        return processed_count
    except Exception as exc:
        print(f"Email fetch error: {exc}")
        return 0
