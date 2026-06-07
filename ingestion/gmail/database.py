"""
Database module - Handles PostgreSQL and Excel storage operations.
"""

import json
import os

import pandas as pd

from backend import postgresql_manager
from backend.storage_engine import store_gmail_message
from .config import get_redis_client


def initialize_database():
    """Validate Gmail canonical tables are reachable."""
    try:
        postgresql_manager.scalar(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'gmail_metadata'
            LIMIT 1
            """
        )
        print("PostgreSQL Database initialized")
        return True
    except Exception as exc:
        print(f"PostgreSQL connection error: {exc}")
        return False


def get_thread_history(thread_id):
    try:
        rows = postgresql_manager.fetchall(
            """
            SELECT
                mi.memory_id,
                mi.title,
                gm.sender AS email_from,
                gm.recipients AS email_to,
                mi.raw_text AS content_primary_text,
                gm.gmail_labels AS email_labels,
                gm.received_at AS event_timestamp,
                mi.first_ingested_at AS ingested_at,
                gm.has_attachments AS email_has_attachments
            FROM memory_items mi
            JOIN gmail_metadata gm ON gm.memory_id = mi.memory_id
            WHERE gm.thread_id = :thread_id
            ORDER BY gm.received_at ASC
            """,
            {"thread_id": thread_id},
        )

        result = []
        for row in rows:
            converted = dict(row)
            if converted.get("event_timestamp"):
                converted["event_timestamp"] = converted["event_timestamp"].isoformat()
            if converted.get("ingested_at"):
                converted["ingested_at"] = converted["ingested_at"].isoformat()
            if isinstance(converted.get("email_to"), str):
                converted["email_to"] = json.loads(converted["email_to"])
            if isinstance(converted.get("email_labels"), str):
                converted["email_labels"] = json.loads(converted["email_labels"])
            result.append(converted)
        return result
    except Exception as exc:
        print(f"Failed to fetch thread history: {exc}")
        return []


def store_attachments_metadata(attachments, memory_id):
    if not attachments:
        return True

    try:
        for attachment in attachments:
            filename = attachment.get("filename")
            if not filename:
                continue
            postgresql_manager.execute(
                """
                INSERT INTO gmail_attachments (
                    memory_id,
                    filename,
                    mime_type,
                    file_size,
                    lightweight_extract,
                    last_extracted_at,
                    is_processed
                )
                VALUES (
                    :memory_id,
                    :filename,
                    :mime_type,
                    :file_size,
                    :lightweight_extract,
                    NOW(),
                    :is_processed
                )
                ON CONFLICT DO NOTHING
                """,
                {
                    "memory_id": memory_id,
                    "filename": filename,
                    "mime_type": attachment.get("mime_type", "application/octet-stream"),
                    "file_size": int(attachment.get("size", 0)),
                    "lightweight_extract": " | ".join(
                        part
                        for part in [
                            filename,
                            attachment.get("mime_type"),
                            str(attachment.get("size", 0) or ""),
                        ]
                        if part and part != "0"
                    ),
                    "is_processed": True,
                },
            )
        return True
    except Exception as exc:
        print(f"Attachment storage error: {exc}")
        return False


def store_in_memory_items(data, memory_id):
    return True


def store_in_gmail_metadata(data, memory_id):
    return True


def store_in_postgresql(data):
    try:
        memory_id, inserted = store_gmail_message(data)
        if not inserted:
            print("Already stored -> Skipping")
            return False

        rc = get_redis_client()
        if rc:
            try:
                rc.setex(f"email:{data['source_item_id']}", 3600, json.dumps(data))
            except Exception as exc:
                print(f"Failed to cache email in Redis: {exc}")

        print("Stored in canonical Gmail tables")
        return True
    except Exception as exc:
        print(f"PostgreSQL storage error: {exc}")
        return False


def store_in_excel(data):
    try:
        row = {
            "memory_id": data["memory_id"],
            "subject": data["title"],
            "sender": data["source_metadata"]["email"]["from"],
            "received_time": data["time"]["event_timestamp"],
            "labels": ",".join(data["source_metadata"]["email"]["labels"]),
            "body": data["content"]["primary_text"][:500],
        }

        df = pd.DataFrame([row])
        if os.path.exists("emails.xlsx"):
            existing = pd.read_excel("emails.xlsx")
            df = pd.concat([existing, df], ignore_index=True)

        df.to_excel("emails.xlsx", index=False)
    except Exception as exc:
        print(f"Excel backup error: {exc}")
