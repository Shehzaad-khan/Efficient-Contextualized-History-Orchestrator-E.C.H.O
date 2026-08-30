from __future__ import annotations

import logging
import time
from typing import Any

from backend.storage_engine import get_connection, upsert_embedding_record

logger = logging.getLogger(__name__)


def _build_embeddable_text(row: dict[str, Any]) -> str:
    parts = [row.get("title") or "", row.get("raw_text") or "", row.get("gmail_subject") or ""]
    if row.get("sender"):
        parts.append(f"from {row['sender']}")
    if row.get("domain"):
        parts.append(f"domain {row['domain']}")
    if row.get("transcript_text"):
        parts.append(row["transcript_text"])
    return "\n".join(part.strip() for part in parts if part and part.strip()).strip()


def process_pending_items(limit: int = 25) -> int:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    mi.memory_id,
                    mi.title,
                    mi.raw_text,
                    gm.subject AS gmail_subject,
                    gm.sender,
                    cm.domain,
                    ym.transcript_text
                FROM memory_items mi
                LEFT JOIN gmail_metadata gm ON gm.memory_id = mi.memory_id
                LEFT JOIN chrome_metadata cm ON cm.memory_id = mi.memory_id
                LEFT JOIN youtube_metadata ym ON ym.memory_id = mi.memory_id
                WHERE mi.preprocessed = FALSE
                  AND mi.is_deleted = FALSE
                ORDER BY mi.created_at ASC
                LIMIT %s
                """,
                (limit,),
            )
            columns = [desc[0] for desc in cursor.description]
            rows = [dict(zip(columns, record)) for record in cursor.fetchall()]

            for row in rows:
                embeddable_text = _build_embeddable_text(row)
                if embeddable_text:
                    upsert_embedding_record(row["memory_id"], embeddable_text)
                cursor.execute(
                    """
                    UPDATE memory_items
                    SET preprocessed = TRUE,
                        last_updated_at = NOW()
                    WHERE memory_id = %s
                    """,
                    (row["memory_id"],),
                )

            return len(rows)


def run_forever(poll_interval_seconds: int = 15) -> None:
    logger.info("Enrichment worker started")
    while True:
        try:
            processed = process_pending_items()
            if processed:
                logger.info("Enrichment processed %s item(s)", processed)
        except Exception as exc:
            logger.exception("Enrichment worker failed: %s", exc)
        time.sleep(poll_interval_seconds)
