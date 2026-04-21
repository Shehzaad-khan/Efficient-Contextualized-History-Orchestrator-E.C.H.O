"""
Gmail API module - Handles Gmail authentication and email extraction.
"""

import base64
import json
import uuid
from datetime import datetime
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from .config import SCOPES, get_redis_client
from .database import store_in_excel, store_in_postgresql

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TOKEN_PATH = PROJECT_ROOT / "token_gmail.json"
CREDENTIALS_PATH = PROJECT_ROOT / "credentials.json"


def authenticate_gmail():
    creds = None
    rc = get_redis_client()
    if rc:
        try:
            cached_token = rc.get("gmail_token")
            if cached_token:
                creds = Credentials.from_authorized_user_info(json.loads(cached_token), SCOPES)
                if creds.valid:
                    return build("gmail", "v1", credentials=creds)
        except Exception as exc:
            print(f"Failed to use cached token: {exc}")

    if TOKEN_PATH.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
        except Exception as exc:
            print(f"Invalid token_gmail.json: {exc}")
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

        TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")
        if rc:
            try:
                rc.setex("gmail_token", 3600, creds.to_json())
            except Exception as exc:
                print(f"Failed to cache token in Redis: {exc}")

    return build("gmail", "v1", credentials=creds)


def extract_body(message):
    payload = message.get("payload", {})
    parts = payload.get("parts", [])

    if parts:
        for part in parts:
            if part.get("mimeType") == "text/plain":
                data = part["body"].get("data")
                if data:
                    return base64.urlsafe_b64decode(data).decode("utf-8")
    else:
        data = payload["body"].get("data")
        if data:
            return base64.urlsafe_b64decode(data).decode("utf-8")

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


def fetch_and_store_new_emails(service, conn, cursor):
    try:
        processed_count = 0
        results = service.users().messages().list(userId="me", maxResults=10).execute()
        messages = results.get("messages", [])

        if not messages:
            print("No new emails found")
            return 0

        for message in messages:
            message_id = message["id"]
            cursor.execute(
                "SELECT memory_id FROM gmail_metadata WHERE email_id = %s",
                (message_id,),
            )
            if cursor.fetchone():
                continue

            msg = service.users().messages().get(userId="me", id=message_id, format="full").execute()
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
                store_in_excel(email_data)

        return processed_count
    except Exception as exc:
        print(f"Email fetch error: {exc}")
        return 0
