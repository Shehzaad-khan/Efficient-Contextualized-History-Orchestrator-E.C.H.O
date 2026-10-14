"""
storage_engine.py — STE Module
Echo Personal Memory System

The single write path from the three connectors into PostgreSQL (architecture
§8 STE, DB design v5). One captured item becomes, in ONE transaction:

    memory_items        the canonical row (dedup on UNIQUE(source_type, source_id))
    <source>_metadata   gmail_metadata / chrome_metadata / youtube_metadata
    memory_engagement   exactly one row per memory item
    gmail_attachments   Gmail only, one row per attachment (Tier-1 metadata)

New rows start with preprocessed = FALSE and a provisional system group
(Gmail -> personal, Chrome -> misc, YouTube -> entertainment); the ENP worker
picks them up, embeds them and sets the real group.

Callers (all run in a worker thread, never on the event loop):
    ingestion/gmail/database.py        store_gmail_message      (poller, asyncio.to_thread)
    ingestion/gmail/router.py          update_gmail_engagement  (asyncio.to_thread)
    ingestion/chrome/chrome_connector  store_chrome_page        (sync route, threadpool)
    ingestion/youtube/youtube_connector store_youtube_detection, update_youtube_metadata,
                                       update_youtube_watch_time (run_in_executor)

Thread safety: no shared state here — every call takes its own pooled
connection from ste.postgresql_manager. Two threads storing the same item at
once are safe: the second INSERT waits for the first to commit, hits ON
CONFLICT DO NOTHING, and then updates the existing row.

Engagement timestamps: first_opened_at / last_accessed_at mean the USER
opened the item. A Chrome visit or a YouTube watch is an open. Receiving an
email is not — Gmail rows start with both NULL and update_gmail_engagement
stamps them when the email is actually opened (wba/time_aggregator counts
only emails with first_opened_at set).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import Connection, text

from ste import postgresql_manager

logger = logging.getLogger(__name__)

SYSTEM_GROUP_IDS = {
    "work": 1,
    "study": 2,
    "entertainment": 3,
    "personal": 4,
    "misc": 5,
}

# Source types whose capture itself means the user opened the item.
_SOURCES_CAPTURED_ON_OPEN = frozenset({"chrome", "youtube"})


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _to_naive_utc(parsed: datetime) -> datetime:
    if parsed.tzinfo:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _safe_datetime(value: Any) -> datetime:
    """Parse a datetime, ISO-8601 string or RFC-2822 email date to naive UTC.
    Falls back to now (with a WARNING) when the value is missing or unparseable."""
    if isinstance(value, datetime):
        return _to_naive_utc(value)
    if isinstance(value, str) and value:
        try:
            return _to_naive_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            pass
        try:
            return _to_naive_utc(parsedate_to_datetime(value))
        except (TypeError, ValueError):
            pass
    logger.warning("Unparseable timestamp %r — using the current time instead", value)
    return utc_now_naive()


def _resolve_existing_memory_id(connection: Connection, source_type: str, source_id: str) -> str | None:
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


def _upsert_memory_item(
    connection: Connection,
    *,
    source_type: str,
    source_id: str,
    system_group_id: int,
    title: str,
    raw_text: str | None,
    created_at: datetime,
    source_metadata: dict[str, Any],
    engagement: dict[str, Any],
) -> tuple[str, bool]:
    """Insert or update one item and its metadata + engagement rows, inside the
    caller's transaction. Returns (memory_id, created_new).

    On a repeat capture, an empty title or a None raw_text keeps the stored value.
    """
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
            "memory_id": str(uuid4()),
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
    _store_memory_engagement(connection, memory_id, engagement, opened=source_type in _SOURCES_CAPTURED_ON_OPEN)
    return memory_id, created_new


def _store_source_metadata(
    connection: Connection,
    source_type: str,
    memory_id: str,
    source_id: str,
    title: str,
    created_at: datetime,
    source_metadata: dict[str, Any],
) -> None:
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
                    revisit_count = COALESCE(chrome_metadata.revisit_count, 0) + EXCLUDED.revisit_count,
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
        is_short = source_metadata.get("is_short")

        # Two writers fill this row: the detection (knows only is_short) and the
        # Data API backfill (knows everything except is_short). Every field is
        # COALESCEd so neither one wipes what the other stored — a re-watch must
        # not erase the channel/duration, and the backfill must not reset a Short
        # to is_short = FALSE.
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
                    COALESCE(:is_short, FALSE),
                    :transcript_text,
                    :youtube_category_id
                )
                ON CONFLICT (memory_id) DO UPDATE
                SET channel_name = COALESCE(EXCLUDED.channel_name, youtube_metadata.channel_name),
                    channel_id = COALESCE(EXCLUDED.channel_id, youtube_metadata.channel_id),
                    duration_seconds = COALESCE(EXCLUDED.duration_seconds, youtube_metadata.duration_seconds),
                    is_short = COALESCE(:is_short, youtube_metadata.is_short),
                    transcript_text = COALESCE(EXCLUDED.transcript_text, youtube_metadata.transcript_text),
                    youtube_category_id = COALESCE(EXCLUDED.youtube_category_id, youtube_metadata.youtube_category_id)
                """
            ),
            {
                "memory_id": memory_id,
                "video_id": source_id,
                "channel_name": source_metadata.get("channel_name"),
                "channel_id": source_metadata.get("channel_id"),
                "duration_seconds": source_metadata.get("duration_seconds"),
                "is_short": None if is_short is None else bool(is_short),
                "transcript_text": source_metadata.get("transcript_text"),
                "youtube_category_id": category_id,
            },
        )
        return

    raise ValueError(f"Unsupported source_type: {source_type}")


def _store_memory_engagement(connection: Connection, memory_id: str, engagement: dict[str, Any], *, opened: bool) -> None:
    """Create or accumulate the item's engagement row.

    `opened` is True when this capture means the user opened the item (Chrome
    visit, YouTube watch); then first_opened_at / last_accessed_at are stamped.
    A received email is not an open, so Gmail ingestion leaves them untouched.
    """
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
                CASE WHEN :opened THEN NOW() END,
                CASE WHEN :opened THEN NOW() END,
                :play_sessions_count
            )
            ON CONFLICT (memory_id) DO UPDATE
            SET dwell_time_seconds = COALESCE(memory_engagement.dwell_time_seconds, 0) + EXCLUDED.dwell_time_seconds,
                watch_time_seconds = GREATEST(COALESCE(memory_engagement.watch_time_seconds, 0), EXCLUDED.watch_time_seconds),
                first_opened_at = COALESCE(memory_engagement.first_opened_at, EXCLUDED.first_opened_at),
                last_accessed_at = COALESCE(EXCLUDED.last_accessed_at, memory_engagement.last_accessed_at),
                play_sessions_count = COALESCE(memory_engagement.play_sessions_count, 0) + EXCLUDED.play_sessions_count
            """
        ),
        {
            "memory_id": memory_id,
            "dwell_time_seconds": int(engagement.get("dwell_time_seconds", 0) or 0),
            "watch_time_seconds": int(engagement.get("watch_time_seconds", 0) or 0),
            "play_sessions_count": int(engagement.get("play_sessions_count", 0) or 0),
            "opened": opened,
        },
    )


def _attachment_lightweight_extract(attachment: dict[str, Any]) -> str:
    """Tier-1 extract = "filename | mime | size" (approved deviation: metadata,
    not content, keeps ingestion fast). Tier-2 full text is extracted on demand
    by enp/attachment_processor.py."""
    size = int(attachment.get("size", 0) or 0)
    parts = [attachment.get("filename"), attachment.get("mime_type"), str(size) if size else None]
    return " | ".join(part for part in parts if part)


def _store_gmail_attachments(connection: Connection, memory_id: str, attachments: list[dict[str, Any]]) -> int:
    """Insert one gmail_attachments row per named attachment. Returns rows written.

    Call only for a NEWLY inserted email: the table has no natural unique key
    (attachment_id is a random UUID), so writing again would duplicate rows.
    """
    written = 0
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
                    FALSE
                )
                """
            ),
            {
                "memory_id": memory_id,
                "filename": filename,
                "mime_type": attachment.get("mime_type", "application/octet-stream"),
                "file_size": int(attachment.get("size", 0) or 0),
                "lightweight_extract": _attachment_lightweight_extract(attachment),
            },
        )
        written += 1
    return written


# ── Public write API (one transaction per call) ──────────────────────────────

def store_gmail_message(data: dict[str, Any]) -> tuple[str, bool]:
    """Store one parsed Gmail message (+ its attachment rows). Returns (memory_id, inserted)."""
    email_meta = data.get("source_metadata", {}).get("email", {})
    content = data.get("content", {})
    body = content.get("primary_text") or ""
    attachments = content.get("attachments") or []
    source_id = data["source_item_id"]

    with postgresql_manager.transaction() as connection:
        memory_id, inserted = _upsert_memory_item(
            connection,
            source_type="gmail",
            source_id=source_id,
            system_group_id=SYSTEM_GROUP_IDS["personal"],
            title=data.get("title") or "(no subject)",
            raw_text=body,
            created_at=_safe_datetime(data.get("time", {}).get("event_timestamp")),
            source_metadata=email_meta,
            engagement={},
        )
        attachment_count = _store_gmail_attachments(connection, memory_id, attachments) if inserted else 0

    if inserted:
        logger.info("Gmail stored: memory_id=%s email_id=%s attachments=%d", memory_id, source_id, attachment_count)
    else:
        # DEBUG: every poll re-sees recent emails.
        logger.debug("Gmail already stored, updated: memory_id=%s email_id=%s", memory_id, source_id)
    return memory_id, inserted


def store_chrome_page(payload) -> dict[str, Any]:
    """Store one Chrome page visit (a ChromeIngestRequest). Returns {memory_id, inserted}."""
    with postgresql_manager.transaction() as connection:
        memory_id, inserted = _upsert_memory_item(
            connection,
            source_type="chrome",
            source_id=payload.canonical_url,
            system_group_id=SYSTEM_GROUP_IDS["misc"],
            title=payload.title or payload.canonical_url,
            # App pages (Slack/Notion/Jira) are never captured as text — privacy rule.
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

    logger.info(
        "Chrome %s: memory_id=%s url=%s dwell=%ss",
        "stored" if inserted else "revisit updated",
        memory_id,
        payload.canonical_url,
        payload.dwell_seconds,
    )
    return {"memory_id": memory_id, "inserted": inserted}


def store_youtube_detection(video_id: str, is_short: bool, detected_at: datetime) -> str:
    """Store a detected video before its Data API metadata arrives. Returns memory_id.

    A new row has an empty title and NULL raw_text — the ENP worker waits for the
    backfill (enrichment_pipeline._awaiting_hydration). On a re-watch, raw_text
    None keeps the stored description/transcript.
    """
    with postgresql_manager.transaction() as connection:
        memory_id, inserted = _upsert_memory_item(
            connection,
            source_type="youtube",
            source_id=video_id,
            system_group_id=SYSTEM_GROUP_IDS["entertainment"],
            title="",
            raw_text=None,
            created_at=_safe_datetime(detected_at),
            source_metadata={"is_short": is_short},
            engagement={"play_sessions_count": 1},
        )

    logger.info(
        "YouTube %s: memory_id=%s video_id=%s short=%s",
        "detected" if inserted else "re-watch recorded",
        memory_id,
        video_id,
        is_short,
    )
    return memory_id


def update_youtube_watch_time(video_id: str, watch_time_seconds: int) -> None:
    """Raise the stored watch time (never lowers it) and recompute completion_rate."""
    # completion_rate uses CAST(... AS double precision) instead of a "::float"
    # suffix cast: SQLAlchemy's text() bind parser breaks on a named parameter
    # immediately followed by "::" (and also resolves ":name" inside SQL comments,
    # so this note lives here rather than in the statement).
    with postgresql_manager.transaction() as connection:
        result = connection.execute(
            text(
                """
                UPDATE memory_engagement me
                SET watch_time_seconds = GREATEST(COALESCE(me.watch_time_seconds, 0), :watch_time_seconds),
                    last_accessed_at = NOW(),
                    completion_rate = CASE
                        WHEN ym.duration_seconds > 0
                        THEN LEAST(1.0, CAST(:watch_time_seconds AS double precision) / ym.duration_seconds)
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
        updated = result.rowcount

    if updated:
        logger.debug("YouTube watch time video_id=%s -> %ss", video_id, watch_time_seconds)
    else:
        # Heartbeat for a video that was never stored (its detection failed the intent gate or the DB write).
        logger.warning("YouTube watch time for unknown video_id=%s ignored", video_id)


def update_youtube_metadata(memory_id: str, metadata: dict[str, Any]) -> None:
    """Fill a detected video with its Data API metadata (title, channel, description,
    tags, transcript). raw_text = description + transcript + tags."""
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
        raw_text = " ".join(
            part
            for part in [
                metadata.get("description", ""),
                metadata.get("transcript_text", ""),
                " ".join(metadata.get("tags", [])),
            ]
            if part
        ).strip()
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
                "raw_text": raw_text,
                "memory_id": memory_id,
            },
        )

    logger.info(
        "YouTube metadata stored: memory_id=%s title=%r channel=%r text=%d chars transcript=%s",
        memory_id,
        metadata.get("title", ""),
        metadata.get("channel_name"),
        len(raw_text),
        bool(metadata.get("transcript_text")),
    )


def update_gmail_engagement(email_id: str, dwell_time_seconds: int) -> bool:
    """Record that the user opened a Gmail message.

    Accumulates foreground reading time, stamps first_opened_at on the first
    open only, and bumps the session counter. Returns False if no memory item
    matches the email_id (e.g. email not yet ingested).
    """
    with postgresql_manager.transaction() as connection:
        result = connection.execute(
            text(
                """
                UPDATE memory_engagement me
                SET dwell_time_seconds = COALESCE(me.dwell_time_seconds, 0) + :dwell_time_seconds,
                    first_opened_at = COALESCE(me.first_opened_at, NOW()),
                    last_accessed_at = NOW(),
                    play_sessions_count = COALESCE(me.play_sessions_count, 0) + 1
                FROM gmail_metadata gm
                WHERE me.memory_id = gm.memory_id
                  AND gm.email_id = :email_id
                """
            ),
            {
                "dwell_time_seconds": int(dwell_time_seconds or 0),
                "email_id": email_id,
            },
        )
        updated = result.rowcount > 0

    if updated:
        logger.info("Gmail opened: email_id=%s dwell=%ss", email_id, dwell_time_seconds)
    else:
        logger.info("Gmail open for email_id=%s ignored — not ingested yet", email_id)
    return updated


def _resolve_video_id(connection: Connection, memory_id: str) -> str:
    row = connection.execute(
        text("SELECT source_id FROM memory_items WHERE memory_id = :memory_id"),
        {"memory_id": memory_id},
    ).mappings().first()
    if not row:
        raise ValueError(f"No memory_item found for memory_id={memory_id}")
    return str(row["source_id"])
