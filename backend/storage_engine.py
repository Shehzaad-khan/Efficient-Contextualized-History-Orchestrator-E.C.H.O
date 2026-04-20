from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Iterator
from uuid import uuid4

import psycopg2
from psycopg2.extras import Json

SYSTEM_GROUP_IDS = {
    "work": 1,
    "study": 2,
    "entertainment": 3,
    "personal": 4,
    "misc": 5,
}


def get_database_url() -> str:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not set")
    return database_url


@contextmanager
def get_connection() -> Iterator[psycopg2.extensions.connection]:
    connection = psycopg2.connect(get_database_url(), connect_timeout=10)
    connection.autocommit = False
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _safe_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, str) and value:
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo:
                return parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed
        except ValueError:
            try:
                parsed = parsedate_to_datetime(value)
                if parsed.tzinfo:
                    return parsed.astimezone(timezone.utc).replace(tzinfo=None)
                return parsed
            except Exception:
                pass
    return utc_now_naive()


def _fetch_memory_id(cursor, source_type: str, source_id: str) -> str | None:
    cursor.execute(
        """
        SELECT memory_id
        FROM memory_items
        WHERE source_type = %s AND source_id = %s
        LIMIT 1
        """,
        (source_type, source_id),
    )
    row = cursor.fetchone()
    return str(row[0]) if row else None


def ensure_memory_item(
    cursor,
    *,
    source_type: str,
    source_id: str,
    system_group_id: int,
    title: str,
    raw_text: str | None,
    created_at: datetime,
    classified_by: str = "pending",
) -> tuple[str, bool]:
    memory_id = str(uuid4())
    cursor.execute(
        """
        INSERT INTO memory_items (
            memory_id,
            system_group_id,
            source_type,
            source_id,
            title,
            raw_text,
            preprocessed,
            classified_by,
            created_at,
            first_ingested_at,
            last_updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, FALSE, %s, %s, NOW(), NOW())
        ON CONFLICT (source_type, source_id) DO NOTHING
        RETURNING memory_id
        """,
        (memory_id, system_group_id, source_type, source_id, title, raw_text, classified_by, created_at),
    )
    inserted = cursor.fetchone()
    if inserted:
        return str(inserted[0]), True
    existing = _fetch_memory_id(cursor, source_type, source_id)
    if existing is None:
        raise ValueError(f"Unable to resolve memory_id for {source_type}:{source_id}")
    cursor.execute(
        """
        UPDATE memory_items
        SET title = COALESCE(NULLIF(%s, ''), title),
            raw_text = COALESCE(%s, raw_text),
            last_updated_at = NOW()
        WHERE memory_id = %s
        """,
        (title, raw_text, existing),
    )
    return existing, False


def upsert_memory_engagement(
    cursor,
    *,
    memory_id: str,
    dwell_time_seconds: int = 0,
    watch_time_seconds: int = 0,
    increment_play_sessions: bool = False,
) -> None:
    cursor.execute(
        """
        INSERT INTO memory_engagement (
            memory_id,
            dwell_time_seconds,
            watch_time_seconds,
            first_opened_at,
            last_accessed_at,
            play_sessions_count
        )
        VALUES (%s, %s, %s, NOW(), NOW(), %s)
        ON CONFLICT (memory_id) DO UPDATE
        SET dwell_time_seconds = COALESCE(memory_engagement.dwell_time_seconds, 0) + EXCLUDED.dwell_time_seconds,
            watch_time_seconds = GREATEST(COALESCE(memory_engagement.watch_time_seconds, 0), EXCLUDED.watch_time_seconds),
            last_accessed_at = NOW(),
            play_sessions_count = COALESCE(memory_engagement.play_sessions_count, 0) + EXCLUDED.play_sessions_count
        """,
        (memory_id, dwell_time_seconds, watch_time_seconds, 1 if increment_play_sessions else 0),
    )


def store_gmail_message(data: dict[str, Any]) -> tuple[str, bool]:
    created_at = _safe_datetime(data.get("time", {}).get("event_timestamp"))
    ingested_at = _safe_datetime(data.get("time", {}).get("ingested_at"))
    email_meta = data.get("source_metadata", {}).get("email", {})
    body = data.get("content", {}).get("primary_text") or ""
    source_id = data["source_item_id"]
    title = data.get("title") or "(no subject)"

    with get_connection() as connection:
        with connection.cursor() as cursor:
            memory_id, inserted = ensure_memory_item(
                cursor,
                source_type="gmail",
                source_id=source_id,
                system_group_id=SYSTEM_GROUP_IDS["personal"],
                title=title,
                raw_text=body,
                created_at=created_at,
            )

            cursor.execute(
                """
                INSERT INTO gmail_metadata (
                    memory_id,
                    email_id,
                    thread_id,
                    sender,
                    recipients,
                    subject,
                    received_at,
                    has_attachments,
                    gmail_labels,
                    is_sent
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (memory_id) DO UPDATE
                SET thread_id = EXCLUDED.thread_id,
                    sender = EXCLUDED.sender,
                    recipients = EXCLUDED.recipients,
                    subject = EXCLUDED.subject,
                    received_at = EXCLUDED.received_at,
                    has_attachments = EXCLUDED.has_attachments,
                    gmail_labels = EXCLUDED.gmail_labels,
                    is_sent = EXCLUDED.is_sent
                """,
                (
                    memory_id,
                    source_id,
                    email_meta.get("thread_id"),
                    email_meta.get("from"),
                    email_meta.get("to") or [],
                    title,
                    created_at,
                    bool(email_meta.get("has_attachments")),
                    email_meta.get("labels") or [],
                    bool(email_meta.get("is_sent")),
                ),
            )

            cursor.execute(
                """
                UPDATE memory_items
                SET raw_text = %s,
                    title = %s,
                    first_ingested_at = COALESCE(first_ingested_at, %s),
                    last_updated_at = NOW()
                WHERE memory_id = %s
                """,
                (body, title, ingested_at, memory_id),
            )

            upsert_memory_engagement(cursor, memory_id=memory_id, increment_play_sessions=False)

            attachments = data.get("content", {}).get("attachments") or []
            for attachment in attachments:
                filename = attachment.get("filename")
                if not filename:
                    continue
                lightweight_extract = " | ".join(
                    part
                    for part in [filename, attachment.get("mime_type"), str(attachment.get("size") or "").strip()]
                    if part and part != "0"
                )
                cursor.execute(
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
                    VALUES (%s, %s, %s, %s, %s, NOW(), %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        memory_id,
                        filename,
                        attachment.get("mime_type"),
                        int(attachment.get("size") or 0),
                        lightweight_extract or None,
                        bool(lightweight_extract),
                    ),
                )

    return memory_id, inserted


def store_chrome_page(payload) -> dict[str, Any]:
    created_at = utc_now_naive()
    source_id = payload.canonical_url
    title = payload.title or payload.canonical_url
    raw_text = payload.content_extract if not payload.is_app_page else None

    with get_connection() as connection:
        with connection.cursor() as cursor:
            memory_id, inserted = ensure_memory_item(
                cursor,
                source_type="chrome",
                source_id=source_id,
                system_group_id=SYSTEM_GROUP_IDS["misc"],
                title=title,
                raw_text=raw_text,
                created_at=created_at,
            )

            cursor.execute(
                """
                INSERT INTO chrome_metadata (
                    memory_id,
                    url,
                    canonical_url,
                    domain,
                    referrer,
                    scroll_depth,
                    interaction_count,
                    revisit_count,
                    word_count
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (memory_id) DO UPDATE
                SET url = EXCLUDED.url,
                    canonical_url = EXCLUDED.canonical_url,
                    domain = EXCLUDED.domain,
                    referrer = COALESCE(EXCLUDED.referrer, chrome_metadata.referrer),
                    scroll_depth = GREATEST(COALESCE(chrome_metadata.scroll_depth, 0.0), EXCLUDED.scroll_depth),
                    interaction_count = GREATEST(COALESCE(chrome_metadata.interaction_count, 0), EXCLUDED.interaction_count),
                    revisit_count = GREATEST(COALESCE(chrome_metadata.revisit_count, 0), EXCLUDED.revisit_count),
                    word_count = COALESCE(EXCLUDED.word_count, chrome_metadata.word_count)
                """,
                (
                    memory_id,
                    payload.url,
                    payload.canonical_url,
                    payload.domain,
                    getattr(payload, "referrer", None),
                    payload.scroll_depth,
                    payload.interaction_count,
                    payload.revisit_count,
                    payload.word_count,
                ),
            )

            cursor.execute(
                """
                UPDATE memory_items
                SET title = %s,
                    raw_text = CASE
                        WHEN %s IS NULL OR %s = '' THEN memory_items.raw_text
                        ELSE %s
                    END,
                    last_updated_at = NOW()
                WHERE memory_id = %s
                """,
                (title, raw_text, raw_text, raw_text, memory_id),
            )

            upsert_memory_engagement(
                cursor,
                memory_id=memory_id,
                dwell_time_seconds=payload.dwell_seconds,
                increment_play_sessions=True,
            )

    return {"memory_id": memory_id, "inserted": inserted}


def store_youtube_detection(video_id: str, is_short: bool, detected_at: datetime) -> str:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            memory_id, _ = ensure_memory_item(
                cursor,
                source_type="youtube",
                source_id=video_id,
                system_group_id=SYSTEM_GROUP_IDS["entertainment"],
                title="",
                raw_text="",
                created_at=_safe_datetime(detected_at),
            )
            cursor.execute(
                """
                INSERT INTO youtube_metadata (
                    memory_id,
                    video_id,
                    is_short
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (memory_id) DO UPDATE
                SET is_short = EXCLUDED.is_short
                """,
                (memory_id, video_id, is_short),
            )
            upsert_memory_engagement(cursor, memory_id=memory_id, increment_play_sessions=True)
            return memory_id


def update_youtube_watch_time(video_id: str, watch_time_seconds: int) -> None:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE memory_engagement me
                SET watch_time_seconds = GREATEST(COALESCE(me.watch_time_seconds, 0), %s),
                    last_accessed_at = NOW(),
                    completion_rate = CASE
                        WHEN ym.duration_seconds > 0
                        THEN LEAST(1.0, %s::float / ym.duration_seconds)
                        ELSE NULL
                    END
                FROM memory_items mi
                LEFT JOIN youtube_metadata ym ON ym.memory_id = mi.memory_id
                WHERE me.memory_id = mi.memory_id
                  AND mi.source_type = 'youtube'
                  AND mi.source_id = %s
                """,
                (watch_time_seconds, watch_time_seconds, video_id),
            )


def update_youtube_metadata(memory_id: str, metadata: dict[str, Any]) -> None:
    transcript_text = metadata.get("transcript_text")
    description = metadata.get("description", "")
    tags = " ".join(metadata.get("tags", []))
    raw_text = " ".join(part for part in [description, transcript_text, tags] if part).strip()

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE youtube_metadata
                SET channel_name = %s,
                    channel_id = %s,
                    duration_seconds = %s,
                    is_short = COALESCE(%s, is_short),
                    transcript_text = COALESCE(%s, transcript_text),
                    youtube_category_id = %s
                WHERE memory_id = %s
                """,
                (
                    metadata.get("channel_name"),
                    metadata.get("channel_id"),
                    metadata.get("duration_seconds"),
                    metadata.get("is_short"),
                    transcript_text,
                    metadata.get("category_id"),
                    memory_id,
                ),
            )
            cursor.execute(
                """
                UPDATE memory_items
                SET title = %s,
                    raw_text = CASE
                        WHEN %s = '' THEN raw_text
                        ELSE %s
                    END,
                    last_updated_at = NOW()
                WHERE memory_id = %s
                """,
                (metadata.get("title", ""), raw_text, raw_text, memory_id),
            )


def fetch_retrieval_candidates(query: str, *, limit: int = 5) -> list[dict[str, Any]]:
    like_query = f"%{query}%"
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    mi.memory_id,
                    mi.source_type,
                    mi.source_id,
                    mi.title,
                    mi.raw_text,
                    COALESCE(ei.embeddable_text, mi.raw_text, '') AS embeddable_text,
                    me.dwell_time_seconds,
                    me.watch_time_seconds,
                    me.last_accessed_at
                FROM memory_items mi
                LEFT JOIN embedding_index ei ON ei.memory_id = mi.memory_id AND ei.is_active = TRUE
                LEFT JOIN memory_engagement me ON me.memory_id = mi.memory_id
                WHERE mi.is_deleted = FALSE
                  AND (
                    mi.title ILIKE %s
                    OR mi.raw_text ILIKE %s
                    OR EXISTS (
                        SELECT 1
                        FROM gmail_metadata gm
                        WHERE gm.memory_id = mi.memory_id
                          AND (gm.subject ILIKE %s OR gm.sender ILIKE %s)
                    )
                  )
                ORDER BY me.last_accessed_at DESC NULLS LAST, mi.created_at DESC
                LIMIT %s
                """,
                (like_query, like_query, like_query, like_query, limit),
            )
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def upsert_embedding_record(memory_id: str, embeddable_text: str, *, version: str = "placeholder-v1") -> None:
    vector_dimension = max(1, min(1536, len(embeddable_text.split())))
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO embedding_index (
                    memory_id,
                    embedding_version,
                    vector_dimension,
                    indexed_at,
                    is_active,
                    embeddable_text
                )
                VALUES (%s, %s, %s, NOW(), TRUE, %s)
                ON CONFLICT (memory_id) DO UPDATE
                SET embedding_version = EXCLUDED.embedding_version,
                    vector_dimension = EXCLUDED.vector_dimension,
                    indexed_at = NOW(),
                    is_active = TRUE,
                    embeddable_text = EXCLUDED.embeddable_text
                """,
                (memory_id, version, vector_dimension, embeddable_text),
            )


def append_message_store(session_id: str, role: str, content: str) -> None:
    payload = {"role": role, "content": content, "created_at": utc_now_naive().isoformat()}
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO message_store (session_id, message)
                VALUES (%s, %s)
                """,
                (session_id, Json(payload)),
            )
