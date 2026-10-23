"""
enrichment_pipeline.py — ENP worker loop (architecture §5, §9; ENP module)
Echo Personal Memory System

Every connector stores a raw memory_items row with preprocessed = FALSE. This
worker turns those rows into searchable memories:

  1. fetch_unprocessed_items   next batch, oldest first (skips 'failed' rows and
                               YouTube rows still waiting for their metadata)
  2. load_item_context         per-source metadata (gmail / chrome / youtube tables)
  3. clean_item_text           text_cleaner: HTML, quotes -> clean text
  4. build_embeddable_text     topic_extractor: the text to embed + auto_keywords
  5. generate_embeddings       embedding_generator: MiniLM, whole batch at once
  6. classify_system_group     system_group_classifier: 4-stage system group
  7. FAISSManager.add          vector + embedding_index row
  8. save_index                FAISS file written BEFORE any row is marked done
  9. mark_item_processed       system group, keywords, preprocessed = TRUE

A row that fails at any step is marked classified_by = 'failed' and is skipped
by later batches (restart the backend with ENP_RETRY_FAILED=true to try them
again).

Threading: run_pipeline is blocking. backend/main.py runs it in a worker
thread named 'enp-worker', so nothing here touches the event loop. Pass a
threading.Event as stop_event and set it to stop the loop between batches.
FAISS: this thread uses the shared get_manager(), the same instance RSE and WBA
read through its locked methods, so a new vector is searchable as soon as add()
returns (C4).

Database: psycopg2 with %s params. One connection per batch, reused for every
read and write in it. Each item's writes commit on their own.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
import sys
import threading
import time
from typing import Any

import psycopg2

if __package__ in (None, ""):
    # Run as a script (python enp/enrichment_pipeline.py): make the repo root importable.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from enp.embedding_generator import generate_embedding, generate_embeddings
from enp.system_group_classifier import (
    centroids_need_recompute,
    classify_system_group,
    initialize_centroids,
    recompute_centroids,
)
from enp.text_cleaner import clean_item_text
from enp.topic_extractor import build_embeddable_text, sender_domain_hint
from ste.faiss_manager import EMBEDDING_VERSION, VECTOR_DIMENSION, FAISSManager, get_manager
from ste.storage_engine import SYSTEM_GROUP_IDS

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 10
DEFAULT_POLL_INTERVAL = 10
FAILED_CLASSIFIED_BY = "failed"
RETRY_FAILED_ENV = "ENP_RETRY_FAILED"

# YouTube rows are inserted by /ytc/video-detected with an empty title and
# raw_text; youtube_connector fills them from the Data API in a background task
# a few seconds later. A row picked up before that has only its video_id to
# embed, and once marked preprocessed it is never embedded again, so it would
# stay findable by ID only. The fetch query therefore skips such rows for this
# long. After it, the backfill is taken as failed and the row is indexed with
# what it has (video_id + URL), so it never stays hidden from retrieval.
HYDRATION_GRACE_SECONDS = 600

# Whitespace characters BTRIM strips, matching Python's str.strip() for our data.
_SQL_WHITESPACE = " \t\r\n"


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not set")
    return database_url


def get_connection():
    return psycopg2.connect(get_database_url())


def fetch_unprocessed_items(
    conn,
    batch_size: int,
    *,
    include_failed: bool | None = None,
) -> list[dict[str, Any]]:
    """Next batch of rows to enrich, oldest first.

    YouTube rows still inside their metadata grace period are filtered out
    here, in SQL, rather than skipped after fetching: skipped rows would stay
    at the head of the queue and, once there were batch_size of them (a burst
    of Shorts while the Data API is down), block every other row.
    """
    if include_failed is None:
        include_failed = os.getenv(RETRY_FAILED_ENV, "").lower() in {"1", "true", "yes"}

    params: list[Any] = []
    failed_clause = ""
    if not include_failed:
        failed_clause = "AND COALESCE(classified_by, 'pending') <> %s"
        params.append(FAILED_CLASSIFIED_BY)
    # COALESCE(..., FALSE): a row with no first_ingested_at is processed now,
    # not hidden forever.
    params.extend([_SQL_WHITESPACE, _SQL_WHITESPACE, HYDRATION_GRACE_SECONDS, batch_size])

    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT memory_id,
                   source_type,
                   source_id,
                   title,
                   raw_text,
                   created_at
            FROM memory_items
            WHERE preprocessed = FALSE
              AND is_deleted = FALSE
              {failed_clause}
              AND NOT (
                    LOWER(source_type) = 'youtube'
                AND BTRIM(COALESCE(title, ''), %s) = ''
                AND BTRIM(COALESCE(raw_text, ''), %s) = ''
                AND COALESCE(first_ingested_at > LOCALTIMESTAMP - %s * INTERVAL '1 second', FALSE)
              )
            ORDER BY first_ingested_at ASC
            LIMIT %s
            """,
            params,
        )
        rows = cursor.fetchall()

    return [
        {
            "memory_id": str(row[0]),
            "source_type": row[1],
            "source_id": row[2],
            "title": row[3] or "",
            "raw_text": row[4] or "",
            "created_at": row[5],
        }
        for row in rows
    ]


def _fetch_thread_history(conn, item: dict[str, Any]) -> list[str]:
    """
    Thread Context Enrichment (architecture §5.3): fetch prior emails in the
    same thread so a bare "Re:" reply inherits its thread's topics in
    embeddable_text. Returns subject + keyword + first-300-chars strings of up
    to 5 earlier thread messages (empty when the item opens the thread).
    """
    thread_id = item.get("thread_id")
    if not thread_id:
        return []

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT mi.title, mi.auto_keywords, LEFT(COALESCE(mi.raw_text, ''), 300)
            FROM memory_items mi
            JOIN gmail_metadata gm ON gm.memory_id = mi.memory_id
            WHERE gm.thread_id = %s
              AND mi.memory_id != %s
              AND mi.is_deleted = FALSE
              AND mi.preprocessed = TRUE
              AND mi.created_at <= %s
            ORDER BY mi.created_at ASC
            LIMIT 5
            """,
            (thread_id, item["memory_id"], item.get("created_at")),
        )
        rows = cursor.fetchall()

    history: list[str] = []
    for title, keywords, snippet in rows:
        parts = [title or "", " ".join(keywords or []), snippet or ""]
        text = " ".join(p for p in parts if p).strip()
        if text:
            history.append(text)
    return history


def _fetch_gmail_context(conn, item: dict[str, Any]) -> dict[str, Any]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT email_id, thread_id, sender, recipients, subject, gmail_labels, has_attachments
            FROM gmail_metadata
            WHERE memory_id = %s
            """,
            (item["memory_id"],),
        )
        row = cursor.fetchone()

    if row:
        sender_str = row[2] or ""
        item.update(
            {
                "email_id": row[0],
                "thread_id": row[1],
                "sender": sender_str,
                "sender_domain": sender_domain_hint(sender_str),
                "recipients": row[3] or [],
                "subject": row[4] or item.get("title", ""),
                "gmail_labels": row[5] or [],
                "has_attachments": row[6],
            }
        )
    else:
        logger.warning("No gmail_metadata row for memory_id=%s; enriching from memory_items only", item["memory_id"])

    # The full body is memory_items.raw_text (the connector stores it there).
    item["content_primary_text"] = item.get("raw_text", "")
    item["message_history"] = _fetch_thread_history(conn, item) or None
    return item


def _fetch_chrome_context(conn, item: dict[str, Any]) -> dict[str, Any]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT url, canonical_url, domain
            FROM chrome_metadata
            WHERE memory_id = %s
            """,
            (item["memory_id"],),
        )
        row = cursor.fetchone()

    if row:
        item.update(
            {
                "url": row[0] or "",
                "canonical_url": row[1] or "",
                "domain": row[2] or "",
            }
        )
    else:
        logger.warning("No chrome_metadata row for memory_id=%s; enriching from memory_items only", item["memory_id"])
    item["raw_html"] = item.get("raw_text", "")
    return item


def _fetch_youtube_context(conn, item: dict[str, Any]) -> dict[str, Any]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT video_id, channel_name, channel_id, duration_seconds, is_short, transcript_text, youtube_category_id
            FROM youtube_metadata
            WHERE memory_id = %s
            """,
            (item["memory_id"],),
        )
        row = cursor.fetchone()

    if row:
        item.update(
            {
                "video_id": row[0],
                "channel_name": row[1] or "",
                "channel_id": row[2] or "",
                "duration_seconds": row[3],
                "is_short": bool(row[4]),
                "transcript_text": row[5] or "",
                "youtube_category_id": row[6],
            }
        )
    else:
        logger.warning("No youtube_metadata row for memory_id=%s; enriching from memory_items only", item["memory_id"])
    # The YouTube connector stores description + tags in raw_text.
    item["description"] = item.get("raw_text", "")
    return item


def load_item_context(conn, base_item: dict[str, Any]) -> dict[str, Any]:
    item = dict(base_item)
    item["source"] = item["source_type"].lower()

    if item["source"] == "gmail":
        return _fetch_gmail_context(conn, item)
    if item["source"] == "chrome":
        return _fetch_chrome_context(conn, item)
    if item["source"] == "youtube":
        return _fetch_youtube_context(conn, item)
    logger.warning("Unknown source_type=%r for memory_id=%s; enriching from memory_items only",
                   item["source_type"], item["memory_id"])
    return item


def _word_count(text: str) -> int:
    return len(text.split()) if text else 0


def prepare_item_for_embedding(conn, base_item: dict[str, Any]) -> dict[str, Any]:
    """Steps 2-4: context, cleaning, embeddable text. Raises ValueError when
    nothing at all can be embedded."""
    item = load_item_context(conn, base_item)
    cleaned = clean_item_text(item)
    embeddable_text, auto_keywords = build_embeddable_text(
        item,
        cleaned.clean_text,
        headings=cleaned.headings,
    )

    if not embeddable_text.strip():
        # Identity of last resort: title/domain-only rows (Chrome telemetry
        # without HTML, a video whose metadata never arrived) still get indexed.
        # Failing them is terminal and would hide them from retrieval and the
        # timeline for good.
        embeddable_text = " ".join(
            str(part).strip()
            for part in (
                item.get("title", ""),
                item.get("domain", ""),
                item.get("subject", ""),
                item.get("channel_name", ""),
                item.get("video_id", ""),
                item.get("url", ""),
            )
            if part and str(part).strip()
        )
        logger.debug("memory_id=%s embedded from identity fields only", item["memory_id"])
    if not embeddable_text.strip():
        raise ValueError(f"No embeddable text constructed for memory_id={item['memory_id']}")

    item["clean_text"] = cleaned.clean_text
    item["headings"] = cleaned.headings
    item["embeddable_text"] = embeddable_text
    item["auto_keywords"] = auto_keywords
    if item.get("source") == "chrome":
        item["word_count"] = _word_count(cleaned.clean_text)
    return item


def classify_item(item: dict[str, Any], embedding) -> tuple[str, str, float]:
    category, method, confidence = classify_system_group(item, embedding)
    return category, method, float(confidence)


def mark_item_processed(
    conn,
    memory_id: str,
    category: str,
    method: str,
    confidence: float,
    auto_keywords: list[str],
    *,
    source: str | None = None,
    word_count: int | None = None,
) -> None:
    """Step 9: store the classification and set preprocessed = TRUE (one commit)."""
    with conn:  # commit on success, rollback on error; does not close conn
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE memory_items
                SET system_group_id = %s,
                    classified_by = %s,
                    classification_confidence = %s,
                    auto_keywords = %s,
                    preprocessed = TRUE,
                    last_updated_at = NOW()
                WHERE memory_id = %s
                """,
                (
                    SYSTEM_GROUP_IDS.get(category, SYSTEM_GROUP_IDS["misc"]),
                    method,
                    confidence,
                    auto_keywords,
                    memory_id,
                ),
            )
            if source == "chrome" and word_count is not None:
                cursor.execute(
                    """
                    UPDATE chrome_metadata
                    SET word_count = %s
                    WHERE memory_id = %s
                    """,
                    (word_count, memory_id),
                )


def mark_item_failed(conn, memory_id: str, reason: str) -> None:
    """Mark a row classified_by='failed' so later batches skip it.

    The schema has no error column, so the reason (first 500 chars) is kept in
    auto_keywords. RSE only reads preprocessed = TRUE rows, so it is never searched.
    """
    reason_text = str(reason or "unknown failure")[:500]
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE memory_items
                SET classified_by = %s,
                    classification_confidence = NULL,
                    preprocessed = FALSE,
                    auto_keywords = ARRAY[%s],
                    last_updated_at = NOW()
                WHERE memory_id = %s
                """,
                (FAILED_CLASSIFIED_BY, reason_text, memory_id),
            )


def _fail_item(conn, memory_id: str, reason: str) -> None:
    """Log the current exception and mark the row failed.

    Rolls back first: if the failure was a SQL error, the connection's
    transaction is aborted and would reject the UPDATE.
    """
    logger.exception("ENP failed memory_id=%s at %s", memory_id, reason.split(":", 1)[0])
    conn.rollback()
    mark_item_failed(conn, memory_id, reason)


def process_batch(manager: FAISSManager, batch_size: int) -> tuple[int, int]:
    """Enrich one batch. Returns (processed, failed).

    Order matters for crash safety: every vector is added and the FAISS file
    saved BEFORE any row is marked preprocessed. A crash in between leaves rows
    unprocessed, and the retry overwrites their vectors (add() replaces by
    memory_id). The reverse order could leave a row marked done whose vector
    never reached disk, missing from semantic search for good.
    """
    conn = get_connection()
    try:
        base_items = fetch_unprocessed_items(conn, batch_size=batch_size)
        if not base_items:
            return 0, 0

        prepared_items: list[dict[str, Any]] = []
        failures = 0
        for base_item in base_items:
            try:
                prepared_items.append(prepare_item_for_embedding(conn, base_item))
            except Exception as exc:
                _fail_item(conn, base_item["memory_id"], f"prepare: {exc}")
                failures += 1
        # End the read transaction before the slow part (embedding, LLM
        # fallback), so the connection does not sit "idle in transaction".
        conn.commit()

        if not prepared_items:
            return 0, failures

        texts = [item["embeddable_text"] for item in prepared_items]
        try:
            batch_embeddings = generate_embeddings(texts)
        except Exception as exc:
            logger.warning("Batch embedding failed, falling back to item-by-item mode: %s", exc)
            batch_embeddings = None

        # Steps 5-7 per item; collect what to mark once the index is saved.
        indexed: list[tuple[dict[str, Any], str, str, float]] = []
        for index, item in enumerate(prepared_items):
            try:
                embedding = (
                    batch_embeddings[index]
                    if batch_embeddings is not None
                    else generate_embedding(item["embeddable_text"])
                )
                category, method, confidence = classify_item(item, embedding)
                manager.add(
                    item["memory_id"],
                    embedding,
                    embeddable_text=item["embeddable_text"],
                )
                indexed.append((item, category, method, confidence))
            except Exception as exc:
                _fail_item(conn, item["memory_id"], f"process: {exc}")
                failures += 1

        if not indexed:
            return 0, failures

        # Step 8. If this raises, nothing below runs: the rows stay unprocessed
        # and the next batch redoes them.
        manager.save_index()

        processed = 0
        for item, category, method, confidence in indexed:
            try:
                mark_item_processed(
                    conn,
                    item["memory_id"],
                    category,
                    method,
                    confidence,
                    item["auto_keywords"],
                    source=item.get("source"),
                    word_count=item.get("word_count"),
                )
                processed += 1
                logger.debug(
                    "ENP enriched memory_id=%s source=%s group=%s via %s (%.2f)",
                    item["memory_id"],
                    item.get("source"),
                    category,
                    method,
                    confidence,
                )
            except Exception:
                # The vector is saved; the row stays unprocessed and is redone
                # (and its vector overwritten) next batch.
                logger.exception("ENP could not mark memory_id=%s processed; will retry", item["memory_id"])
                conn.rollback()

        return processed, failures
    finally:
        conn.close()


def run_pipeline(
    batch_size: int = DEFAULT_BATCH_SIZE,
    poll_interval: int = DEFAULT_POLL_INTERVAL,
    stop_event: threading.Event | None = None,
) -> None:
    """Poll for unprocessed rows until stop_event is set (forever when None).

    Blocking; run it in its own thread. The stop is checked between batches,
    so a stop request waits for the current batch to finish.
    """
    stop_event = stop_event or threading.Event()

    manager = get_manager()

    logger.info("Initializing Stage 3 centroid seeds")
    initialize_centroids(generate_embedding)
    if centroids_need_recompute():
        try:
            updated = recompute_centroids()
            logger.info("ENP startup centroid recompute updated %d categories", len(updated))
        except Exception:
            # Centroid maintenance must not prevent the enrichment queue from running.
            logger.exception("ENP startup centroid recompute failed; continuing with available centroids")
    logger.info(
        "ENP started: batch_size=%s poll_interval=%ss embedding_version=%s dim=%s",
        batch_size,
        poll_interval,
        EMBEDDING_VERSION,
        VECTOR_DIMENSION,
    )

    iteration = 0
    while not stop_event.is_set():
        iteration += 1
        started_at = time.perf_counter()
        try:
            processed, failed = process_batch(manager, batch_size=batch_size)
            if processed == 0 and failed == 0:
                logger.debug("ENP iteration %s: nothing to process", iteration)
            else:
                logger.info(
                    "ENP iteration %s: processed=%s failed=%s (%.0f ms)",
                    iteration,
                    processed,
                    failed,
                    (time.perf_counter() - started_at) * 1000,
                )
        except Exception:
            # e.g. the database is unreachable. Keep polling; the next batch retries.
            logger.exception("ENP batch error on iteration %s", iteration)

        # Sleeps poll_interval, but wakes at once when a stop is requested.
        stop_event.wait(poll_interval)

    logger.info("ENP stopped after %s iterations", iteration)


if __name__ == "__main__":
    from dotenv import load_dotenv

    from backend.logging_config import setup_logging

    load_dotenv()
    setup_logging()
    run_pipeline(
        batch_size=int(os.getenv("ENP_BATCH_SIZE", DEFAULT_BATCH_SIZE)),
        poll_interval=int(os.getenv("ENP_POLL_INTERVAL", DEFAULT_POLL_INTERVAL)),
    )
