from __future__ import annotations

import json
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import text

from backend import postgresql_manager

SYSTEM_GROUP_IDS = {
    "work": 1,
    "study": 2,
    "entertainment": 3,
    "personal": 4,
    "misc": 5,
}


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _safe_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
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


def _resolve_existing_memory_id(connection, source_type: str, source_id: str) -> str | None:
    row = connection.execute(
        text(
            """
            SELECT memory_id
            FROM memory_items
            WHERE source_type = :source_type AND source_id = :source_id
            LIMIT 1
            """
        ),
        {"source_type": source_type, "source_id": source_id},
    ).mappings().first()
    return str(row["memory_id"]) if row else None


def store_memory_item(
    *,
    source_type: str,
    source_id: str,
    system_group_id: int,
    title: str,
    raw_text: str | None,
    created_at: datetime,
    source_metadata: dict[str, Any],
    engagement: dict[str, Any] | None = None,
) -> tuple[str, bool]:
    memory_id = str(uuid4())
    engagement = engagement or {}

    with postgresql_manager.transaction() as connection:
        inserted = connection.execute(
            text(
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
                VALUES (
                    :memory_id,
                    :system_group_id,
                    :source_type,
                    :source_id,
                    :title,
                    :raw_text,
                    FALSE,
                    'pending',
                    :created_at,
                    NOW(),
                    NOW()
                )
                ON CONFLICT (source_type, source_id) DO NOTHING
                RETURNING memory_id
                """
            ),
            {
                "memory_id": memory_id,
                "system_group_id": system_group_id,
                "source_type": source_type,
                "source_id": source_id,
                "title": title,
                "raw_text": raw_text,
                "created_at": created_at,
            },
        ).mappings().first()

        if inserted:
            memory_id = str(inserted["memory_id"])
            created_new = True
        else:
            created_new = False
            existing = _resolve_existing_memory_id(connection, source_type, source_id)
            if existing is None:
                raise ValueError(f"Unable to resolve memory_id for {source_type}:{source_id}")
            memory_id = existing
            connection.execute(
                text(
                    """
                    UPDATE memory_items
                    SET title = COALESCE(NULLIF(:title, ''), title),
                        raw_text = COALESCE(:raw_text, raw_text),
                        last_updated_at = NOW()
                    WHERE memory_id = :memory_id
                    """
                ),
                {"title": title, "raw_text": raw_text, "memory_id": memory_id},
            )

        _store_source_metadata(connection, source_type, memory_id, source_id, title, created_at, source_metadata)
        _store_memory_engagement(connection, memory_id, engagement)
        return memory_id, created_new


def _store_source_metadata(connection, source_type: str, memory_id: str, source_id: str, title: str, created_at: datetime, source_metadata: dict[str, Any]) -> None:
    if source_type == "gmail":
        connection.execute(
            text(
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
                VALUES (
                    :memory_id,
                    :email_id,
                    :thread_id,
                    :sender,
                    :recipients,
                    :subject,
                    :received_at,
                    :has_attachments,
                    :gmail_labels,
                    :is_sent
                )
                ON CONFLICT (memory_id) DO UPDATE
                SET thread_id = EXCLUDED.thread_id,
                    sender = EXCLUDED.sender,
                    recipients = EXCLUDED.recipients,
                    subject = EXCLUDED.subject,
                    received_at = EXCLUDED.received_at,
                    has_attachments = EXCLUDED.has_attachments,
                    gmail_labels = EXCLUDED.gmail_labels,
                    is_sent = EXCLUDED.is_sent
                """
            ),
            {
                "memory_id": memory_id,
                "email_id": source_id,
                "thread_id": source_metadata.get("thread_id"),
                "sender": source_metadata.get("from"),
                "recipients": source_metadata.get("to") or [],
                "subject": title,
                "received_at": created_at,
                "has_attachments": bool(source_metadata.get("has_attachments")),
                "gmail_labels": source_metadata.get("labels") or [],
                "is_sent": bool(source_metadata.get("is_sent")),
            },
        )
        return

    if source_type == "chrome":
        connection.execute(
            text(
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
                VALUES (
                    :memory_id,
                    :url,
                    :canonical_url,
                    :domain,
                    :referrer,
                    :scroll_depth,
                    :interaction_count,
                    :revisit_count,
                    :word_count
                )
                ON CONFLICT (memory_id) DO UPDATE
                SET url = EXCLUDED.url,
                    canonical_url = EXCLUDED.canonical_url,
                    domain = EXCLUDED.domain,
                    referrer = COALESCE(EXCLUDED.referrer, chrome_metadata.referrer),
                    scroll_depth = GREATEST(COALESCE(chrome_metadata.scroll_depth, 0.0), EXCLUDED.scroll_depth),
                    interaction_count = GREATEST(COALESCE(chrome_metadata.interaction_count, 0), EXCLUDED.interaction_count),
                    revisit_count = GREATEST(COALESCE(chrome_metadata.revisit_count, 0), EXCLUDED.revisit_count),
                    word_count = COALESCE(EXCLUDED.word_count, chrome_metadata.word_count)
                """
            ),
            {
                "memory_id": memory_id,
                "url": source_metadata.get("url"),
                "canonical_url": source_metadata.get("canonical_url"),
                "domain": source_metadata.get("domain"),
                "referrer": source_metadata.get("referrer"),
                "scroll_depth": source_metadata.get("scroll_depth", 0.0),
                "interaction_count": source_metadata.get("interaction_count", 0),
                "revisit_count": source_metadata.get("revisit_count", 0),
                "word_count": source_metadata.get("word_count"),
            },
        )
        return

    if source_type == "youtube":
        category_id = source_metadata.get("youtube_category_id")
        if isinstance(category_id, str) and category_id.isdigit():
            category_id = int(category_id)
        elif not isinstance(category_id, int):
            category_id = None

        connection.execute(
            text(
                """
                INSERT INTO youtube_metadata (
                    memory_id,
                    video_id,
                    channel_name,
                    channel_id,
                    duration_seconds,
                    is_short,
                    transcript_text,
                    youtube_category_id
                )
                VALUES (
                    :memory_id,
                    :video_id,
                    :channel_name,
                    :channel_id,
                    :duration_seconds,
                    :is_short,
                    :transcript_text,
                    :youtube_category_id
                )
                ON CONFLICT (memory_id) DO UPDATE
                SET channel_name = EXCLUDED.channel_name,
                    channel_id = EXCLUDED.channel_id,
                    duration_seconds = EXCLUDED.duration_seconds,
                    is_short = EXCLUDED.is_short,
                    transcript_text = COALESCE(EXCLUDED.transcript_text, youtube_metadata.transcript_text),
                    youtube_category_id = EXCLUDED.youtube_category_id
                """
            ),
            {
                "memory_id": memory_id,
                "video_id": source_id,
                "channel_name": source_metadata.get("channel_name"),
                "channel_id": source_metadata.get("channel_id"),
                "duration_seconds": source_metadata.get("duration_seconds"),
                "is_short": bool(source_metadata.get("is_short")),
                "transcript_text": source_metadata.get("transcript_text"),
                "youtube_category_id": category_id,
            },
        )
        return

    raise ValueError(f"Unsupported source_type: {source_type}")


def _store_memory_engagement(connection, memory_id: str, engagement: dict[str, Any]) -> None:
    connection.execute(
        text(
            """
            INSERT INTO memory_engagement (
                memory_id,
                dwell_time_seconds,
                watch_time_seconds,
                first_opened_at,
                last_accessed_at,
                play_sessions_count
            )
            VALUES (
                :memory_id,
                :dwell_time_seconds,
                :watch_time_seconds,
                NOW(),
                NOW(),
                :play_sessions_count
            )
            ON CONFLICT (memory_id) DO UPDATE
            SET dwell_time_seconds = COALESCE(memory_engagement.dwell_time_seconds, 0) + EXCLUDED.dwell_time_seconds,
                watch_time_seconds = GREATEST(COALESCE(memory_engagement.watch_time_seconds, 0), EXCLUDED.watch_time_seconds),
                last_accessed_at = NOW(),
                play_sessions_count = COALESCE(memory_engagement.play_sessions_count, 0) + EXCLUDED.play_sessions_count
            """
        ),
        {
            "memory_id": memory_id,
            "dwell_time_seconds": int(engagement.get("dwell_time_seconds", 0) or 0),
            "watch_time_seconds": int(engagement.get("watch_time_seconds", 0) or 0),
            "play_sessions_count": int(engagement.get("play_sessions_count", 0) or 0),
        },
    )


def store_gmail_message(data: dict[str, Any]) -> tuple[str, bool]:
    email_meta = data.get("source_metadata", {}).get("email", {})
    body = data.get("content", {}).get("primary_text") or ""
    memory_id, inserted = store_memory_item(
        source_type="gmail",
        source_id=data["source_item_id"],
        system_group_id=SYSTEM_GROUP_IDS["personal"],
        title=data.get("title") or "(no subject)",
        raw_text=body,
        created_at=_safe_datetime(data.get("time", {}).get("event_timestamp")),
        source_metadata=email_meta,
        engagement={"play_sessions_count": 0},
    )

    attachments = data.get("content", {}).get("attachments") or []
    if attachments:
        with postgresql_manager.transaction() as connection:
            for attachment in attachments:
                filename = attachment.get("filename")
                if not filename:
                    continue
                connection.execute(
                    text(
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
                        """
                    ),
                    {
                        "memory_id": memory_id,
                        "filename": filename,
                        "mime_type": attachment.get("mime_type", "application/octet-stream"),
                        "file_size": int(attachment.get("size", 0) or 0),
                        "lightweight_extract": " | ".join(
                            str(part)
                            for part in [filename, attachment.get("mime_type"), attachment.get("size")]
                            if part not in (None, "", 0, "0")
                        )
                        or None,
                        "is_processed": True,
                    },
                )

    return memory_id, inserted


def store_chrome_page(payload) -> dict[str, Any]:
    memory_id, inserted = store_memory_item(
        source_type="chrome",
        source_id=payload.canonical_url,
        system_group_id=SYSTEM_GROUP_IDS["misc"],
        title=payload.title or payload.canonical_url,
        raw_text=None if getattr(payload, "is_app_page", False) else getattr(payload, "content_extract", None),
        created_at=utc_now_naive(),
        source_metadata={
            "url": payload.url,
            "canonical_url": payload.canonical_url,
            "domain": payload.domain,
            "referrer": getattr(payload, "referrer", None),
            "scroll_depth": payload.scroll_depth,
            "interaction_count": payload.interaction_count,
            "revisit_count": payload.revisit_count,
            "word_count": getattr(payload, "word_count", None),
        },
        engagement={
            "dwell_time_seconds": payload.dwell_seconds,
            "play_sessions_count": 1,
        },
    )
    return {"memory_id": memory_id, "inserted": inserted}


def store_youtube_detection(video_id: str, is_short: bool, detected_at: datetime) -> str:
    memory_id, _ = store_memory_item(
        source_type="youtube",
        source_id=video_id,
        system_group_id=SYSTEM_GROUP_IDS["entertainment"],
        title="",
        raw_text="",
        created_at=_safe_datetime(detected_at),
        source_metadata={"is_short": is_short},
        engagement={"play_sessions_count": 1},
    )
    return memory_id


def update_youtube_watch_time(video_id: str, watch_time_seconds: int) -> None:
    with postgresql_manager.transaction() as connection:
        connection.execute(
            text(
                """
                UPDATE memory_engagement me
                SET watch_time_seconds = GREATEST(COALESCE(me.watch_time_seconds, 0), :watch_time_seconds),
                    last_accessed_at = NOW(),
                    completion_rate = CASE
                        WHEN ym.duration_seconds > 0
                        THEN LEAST(1.0, :watch_time_seconds::float / ym.duration_seconds)
                        ELSE NULL
                    END
                FROM memory_items mi
                LEFT JOIN youtube_metadata ym ON ym.memory_id = mi.memory_id
                WHERE me.memory_id = mi.memory_id
                  AND mi.source_type = 'youtube'
                  AND mi.source_id = :video_id
                """
            ),
            {"watch_time_seconds": watch_time_seconds, "video_id": video_id},
        )


def update_youtube_metadata(memory_id: str, metadata: dict[str, Any]) -> None:
    with postgresql_manager.transaction() as connection:
        _store_source_metadata(
            connection,
            "youtube",
            memory_id,
            metadata.get("video_id") or _resolve_video_id(connection, memory_id),
            metadata.get("title", ""),
            utc_now_naive(),
            {
                "channel_name": metadata.get("channel_name"),
                "channel_id": metadata.get("channel_id"),
                "duration_seconds": metadata.get("duration_seconds"),
                "is_short": metadata.get("is_short"),
                "transcript_text": metadata.get("transcript_text"),
                "youtube_category_id": metadata.get("category_id"),
            },
        )
        connection.execute(
            text(
                """
                UPDATE memory_items
                SET title = :title,
                    raw_text = CASE
                        WHEN :raw_text = '' THEN raw_text
                        ELSE :raw_text
                    END,
                    last_updated_at = NOW()
                WHERE memory_id = :memory_id
                """
            ),
            {
                "title": metadata.get("title", ""),
                "raw_text": " ".join(
                    part
                    for part in [
                        metadata.get("description", ""),
                        metadata.get("transcript_text", ""),
                        " ".join(metadata.get("tags", [])),
                    ]
                    if part
                ).strip(),
                "memory_id": memory_id,
            },
        )


def _resolve_video_id(connection, memory_id: str) -> str:
    row = connection.execute(
        text("SELECT source_id FROM memory_items WHERE memory_id = :memory_id"),
        {"memory_id": memory_id},
    ).mappings().first()
    if not row:
        raise ValueError(f"No memory_item found for memory_id={memory_id}")
    return str(row["source_id"])


def get_connection():
    return postgresql_manager.get_engine().raw_connection()


def fetch_retrieval_candidates(query: str, *, limit: int = 5) -> list[dict[str, Any]]:
    like_query = f"%{query}%"
    return postgresql_manager.fetchall(
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
            mi.title ILIKE :like_query
            OR mi.raw_text ILIKE :like_query
            OR EXISTS (
                SELECT 1
                FROM gmail_metadata gm
                WHERE gm.memory_id = mi.memory_id
                  AND (gm.subject ILIKE :like_query OR gm.sender ILIKE :like_query)
            )
          )
        ORDER BY me.last_accessed_at DESC NULLS LAST, mi.created_at DESC
        LIMIT :limit
        """,
        {"like_query": like_query, "limit": limit},
    )


def append_message_store(session_id: str, role: str, content: str) -> None:
    postgresql_manager.execute(
        """
        INSERT INTO message_store (session_id, message)
        VALUES (:session_id, CAST(:message AS JSONB))
        """,
        {
            "session_id": session_id,
            "message": json.dumps(
                {
                    "role": role,
                    "content": content,
                    "created_at": utc_now_naive().isoformat(),
                }
            ),
        },
    )


def upsert_embedding_record(memory_id: str, embeddable_text: str, *, version: str = "placeholder-v1") -> None:
    vector_dimension = max(1, min(1536, len(embeddable_text.split())))
    postgresql_manager.execute(
        """
        INSERT INTO embedding_index (
            memory_id,
            embedding_version,
            vector_dimension,
            indexed_at,
            is_active,
            embeddable_text
        )
        VALUES (:memory_id, :version, :vector_dimension, NOW(), TRUE, :embeddable_text)
        ON CONFLICT (memory_id) DO UPDATE
        SET embedding_version = EXCLUDED.embedding_version,
            vector_dimension = EXCLUDED.vector_dimension,
            indexed_at = NOW(),
            is_active = TRUE,
            embeddable_text = EXCLUDED.embeddable_text
        """,
        {
            "memory_id": memory_id,
            "version": version,
            "vector_dimension": vector_dimension,
            "embeddable_text": embeddable_text,
        },
    )
