from __future__ import annotations

import logging
import os
from pathlib import Path
import sys
import time
from typing import Any

import psycopg2
from dotenv import load_dotenv

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from enp.embedding_generator import generate_embedding, generate_embeddings
    from enp.faiss_manager import EMBEDDING_VERSION, VECTOR_DIMENSION, FAISSManager
    from enp.system_group_classifier import classify_system_group, initialize_centroids
    from enp.text_cleaner import clean_item_text
    from enp.topic_extractor import build_embeddable_text, parse_message_history, sender_domain_hint
else:
    from .embedding_generator import generate_embedding, generate_embeddings
    from .faiss_manager import EMBEDDING_VERSION, VECTOR_DIMENSION, FAISSManager
    from .system_group_classifier import classify_system_group, initialize_centroids
    from .text_cleaner import clean_item_text
    from .topic_extractor import build_embeddable_text, parse_message_history, sender_domain_hint

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DEFAULT_BATCH_SIZE = 10
DEFAULT_POLL_INTERVAL = 10

SYSTEM_GROUP_IDS = {
    "work": 1,
    "study": 2,
    "entertainment": 3,
    "personal": 4,
    "misc": 5,
}


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not set")
    return database_url


def get_connection():
    return psycopg2.connect(get_database_url())


def fetch_unprocessed_items(conn, batch_size: int) -> list[dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT memory_id, source_type, source_id, title, raw_text, created_at
            FROM memory_items
            WHERE preprocessed = FALSE
            ORDER BY first_ingested_at ASC
            LIMIT %s
            """,
            (batch_size,),
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

    # Legacy fallback for thread context and full body until Gmail is fully unified.
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT content_primary_text, message_history
                FROM gmail_memory
                WHERE source_item_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (item["source_id"],),
            )
            legacy = cursor.fetchone()
        if legacy:
            item["content_primary_text"] = legacy[0] or item.get("raw_text", "")
            item["message_history"] = parse_message_history(legacy[1])
    except psycopg2.Error:
        item["content_primary_text"] = item.get("raw_text", "")
        item["message_history"] = None

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
    # Current YouTube connector stores description + tags in raw_text.
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
    return item


def prepare_item_for_embedding(conn, base_item: dict[str, Any]) -> dict[str, Any]:
    item = load_item_context(conn, base_item)
    cleaned = clean_item_text(item)
    embeddable_text, auto_keywords = build_embeddable_text(
        item,
        cleaned.clean_text,
        headings=cleaned.headings,
    )

    if not embeddable_text.strip():
        raise ValueError(f"No embeddable text constructed for memory_id={item['memory_id']}")

    item["clean_text"] = cleaned.clean_text
    item["headings"] = cleaned.headings
    item["content_snippet"] = cleaned.snippet
    item["embeddable_text"] = embeddable_text
    item["auto_keywords"] = auto_keywords
    return item


def classify_item(item: dict[str, Any], embedding) -> tuple[str, str, float]:
    category, method, confidence = classify_system_group(item, embedding)
    return category, method, float(confidence)


def mark_item_processed(
    memory_id: str,
    category: str,
    method: str,
    confidence: float,
    auto_keywords: list[str],
) -> None:
    conn = get_connection()
    try:
        with conn:
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
    finally:
        conn.close()


def process_batch(manager: FAISSManager, batch_size: int) -> tuple[int, int]:
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
                logger.exception("Preparation failed for memory_id=%s: %s", base_item["memory_id"], exc)
                failures += 1

        if not prepared_items:
            return 0, failures

        texts = [item["embeddable_text"] for item in prepared_items]
        try:
            batch_embeddings = generate_embeddings(texts)
        except Exception as exc:
            logger.warning("Batch embedding failed, falling back to item-by-item mode: %s", exc)
            batch_embeddings = None

        processed = 0
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
                manager.save_index()
                mark_item_processed(
                    item["memory_id"],
                    category,
                    method,
                    confidence,
                    item["auto_keywords"],
                )
                processed += 1
            except Exception as exc:
                logger.exception("Failed processing memory_id=%s: %s", item["memory_id"], exc)
                failures += 1

        return processed, failures
    finally:
        conn.close()


def run_pipeline(batch_size: int = DEFAULT_BATCH_SIZE, poll_interval: int = DEFAULT_POLL_INTERVAL) -> None:
    manager = FAISSManager()
    manager.load_index()

    logger.info("Initializing Stage 3 centroid seeds with generate_embedding()")
    initialize_centroids(generate_embedding)
    logger.info(
        "ENP started: batch_size=%s poll_interval=%ss embedding_version=%s dim=%s",
        batch_size,
        poll_interval,
        EMBEDDING_VERSION,
        VECTOR_DIMENSION,
    )

    iteration = 0
    while True:
        iteration += 1
        try:
            processed, failed = process_batch(manager, batch_size=batch_size)
            if processed == 0 and failed == 0:
                logger.info("Iteration %s: no unprocessed items found", iteration)
            else:
                logger.info(
                    "Iteration %s complete: processed=%s failed=%s",
                    iteration,
                    processed,
                    failed,
                )
        except Exception as exc:
            logger.exception("Fatal batch error on iteration %s: %s", iteration, exc)

        time.sleep(poll_interval)


if __name__ == "__main__":
    run_pipeline(
        batch_size=int(os.getenv("ENP_BATCH_SIZE", DEFAULT_BATCH_SIZE)),
        poll_interval=int(os.getenv("ENP_POLL_INTERVAL", DEFAULT_POLL_INTERVAL)),
    )
