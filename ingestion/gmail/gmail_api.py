"""
Gmail API module - Handles all Gmail authentication and email extraction
"""

import os
import base64
import uuid
import json
from datetime import datetime

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from config import SCOPES, get_redis_client
from database import store_in_postgresql, store_attachments_metadata, store_in_excel

# ==============================
# AUTHENTICATE GMAIL
# ==============================

def authenticate_gmail():
    creds = None

    # Try to get cached token from Redis
    rc = get_redis_client()
    if rc:
        try:
            cached_token = rc.get('gmail_token')
            if cached_token:
                creds = Credentials.from_authorized_user_info(json.loads(cached_token), SCOPES)
                if creds.valid:
                    service = build('gmail', 'v1', credentials=creds)
                    return service
        except Exception as e:
            print(f"⚠️  Failed to use cached token: {e}")

    # Try to load from token.json
    if os.path.exists('token.json'):
        try:
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        except Exception as e:
            print(f"⚠️  Invalid/expired token.json: {e}")
            print("   Deleting old token and generating new one...")
            os.remove('token.json')
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"⚠️  Token refresh failed: {e}")
                print("   Generating fresh OAuth flow...")
                creds = None
        
        if not creds:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            except FileNotFoundError:
                print("❌ ERROR: credentials.json not found!")
                print("   Please ensure credentials.json exists in the project directory")
                raise
            except Exception as e:
                print(f"❌ OAuth authentication failed: {e}")
                raise

        # Save to file
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
        
        # Cache in Redis with 1 hour expiry
        rc = get_redis_client()
        if rc:
            try:
                rc.setex('gmail_token', 3600, creds.to_json())
            except Exception as e:
                print(f"⚠️  Failed to cache token in Redis: {e}")

    try:
        service = build('gmail', 'v1', credentials=creds)
        return service
    except Exception as e:
        print(f"❌ Failed to build Gmail service: {e}")
        raise


# ==============================
# EXTRACT EMAIL BODY
# ==============================

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


# ==============================
# EXTRACT ATTACHMENTS
# ==============================

def extract_attachments(message, message_id):
    """Extract attachment data from Gmail message and return list"""
    print(f"      🔍 extract_attachments() called with message_id: {message_id[:12]}...")
    attachments = []
    payload = message.get("payload", {})
    parts = payload.get("parts", [])
    
    print(f"      📦 Payload has {len(parts)} parts")
    
    if parts:
        for idx, part in enumerate(parts):
            filename = part.get("filename", "")
            print(f"      📦 Part [{idx}]: filename='{filename}'")
            
            # Check if this part is an attachment (has filename and attachmentId)
            if filename and filename.strip():
                # Get size from part (Gmail may return 0 for some attachment types)
                size = int(part.get("size", 0))
                
                attachment = {
                    "filename": filename,
                    "mime_type": part.get("mimeType", "application/octet-stream"),
                    "size": size
                }
                attachments.append(attachment)
                size_str = f"{size} bytes" if size > 0 else "(size not available from Gmail)"
                print(f"      📎 Attachment identified: {filename} {size_str}")
    else:
        print(f"      ℹ️  No parts found in payload")
    
    print(f"      ✅ extract_attachments() returning {len(attachments)} attachments")
    return attachments


# ==============================
# FETCH & STORE NEW EMAILS
# ==============================

def fetch_and_store_new_emails(service, conn, cursor):
    try:
        results = service.users().messages().list(
            userId='me',
            maxResults=10 # check last 10 emails
        ).execute()

        messages = results.get('messages', [])

        if not messages:
            print("No new emails found")
            return

        print(f"\n📨 Fetching up to {len(messages)} email(s)...")
        
        for message in messages:
            message_id = message['id']

            # Skip if already stored
            cursor.execute(
                "SELECT id FROM gmail_memory WHERE source_item_id = %s",
                (message_id,)
            )
            if cursor.fetchone():
                continue

            msg = service.users().messages().get(
                userId='me',
                id=message_id,
                format='full'
            ).execute()

            headers = msg['payload']['headers']

            subject = ""
            sender = ""
            to = ""
            date = ""

            for header in headers:
                if header['name'] == 'Subject':
                    subject = header['value']
                elif header['name'] == 'From':
                    sender = header['value']
                elif header['name'] == 'To':
                    to = header['value']
                elif header['name'] == 'Date':
                    date = header['value']

            body = extract_body(msg)
            attachments = extract_attachments(msg, message_id)
            has_attachments = len(attachments) > 0
            
            # Show processing info
            print(f"\n📧 Processing: {subject}")
            print(f"   From: {sender}")
            if has_attachments:
                print(f"   📎 Attachments found: {len(attachments)}")
                for att in attachments:
                    print(f"      - {att['filename']}")
            else:
                print(f"   ℹ️  No attachments")

            email_data = {
                "memory_id": str(uuid.uuid4()),
                "source_type": "email",
                "source_item_id": message_id,
                "title": subject,
                "content": {
                    "primary_text": body,
                    "attachments": attachments,  # Now contains actual attachment data
                    "summary": None
                },
                "time": {
                    "event_timestamp": date,
                    "ingested_at": datetime.utcnow().isoformat()
                },
                "semantic": {},
                "classification": {},
                "interaction": {},
                "analytics": {},
                "regret": {
                    "is_regret": False
                },
                "source_metadata": {
                    "email": {
                        "from": sender,
                        "to": [to],
                        "labels": msg.get("labelIds", []),
                        "thread_id": msg.get("threadId"),
                        "has_attachments": has_attachments  # Now reflects actual attachments
                    }
                },
                "source_link": f"https://mail.google.com/mail/u/0/#inbox/{message_id}"
            }

            stored = store_in_postgresql(email_data)

            if stored:
                # Store attachment metadata separately in gmail_attachments table
                if attachments and len(attachments) > 0:
                    print(f"   📌 === CALLING store_attachments_metadata() ===")
                    print(f"   📌 Attachments to store: {len(attachments)}")
                    for att in attachments:
                        print(f"      - {att}")
                    store_attachments_metadata(attachments, email_data["memory_id"])
                else:
                    print(f"   ✅ Stored (no attachments)")
                
                # Backup to Excel
                store_in_excel(email_data)

    except Exception as e:
        print(f"Email fetch error: {e}")
