"""
export_legacy_gmail_threads.py — archive thread history from the legacy gmail_memory table.

The canonical 16-table schema has no home for per-email thread history:
gmail_metadata carries only headers, message_store is LangChain chat sessions,
and memory_items has no JSONB column. Since the schema is locked, this script
archives the remaining unique data to a local JSON file so gmail_memory can be
dropped without losing it.

Run once before dropping the table:

    python scripts/export_legacy_gmail_threads.py

Writes legacy_gmail_threads.json in the project root (gitignored).
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from ste.postgresql_manager import fetchall, scalar

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_PATH = PROJECT_ROOT / "legacy_gmail_threads.json"


def _json_default(value: object) -> str:
    """Serialise types psycopg/SQLAlchemy return that json cannot encode."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Unserialisable type: {type(value).__name__}")


def table_exists() -> bool:
    return bool(
        scalar(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'gmail_memory'
            )
            """
        )
    )


def export_threads() -> int:
    """Write every legacy row carrying thread history to OUTPUT_PATH.

    Returns the number of rows exported.
    """
    rows = fetchall(
        """
        SELECT
            source_item_id,
            memory_id,
            email_thread_id,
            email_from,
            title,
            event_timestamp,
            message_history
        FROM gmail_memory
        WHERE jsonb_array_length(COALESCE(message_history, '[]'::jsonb)) > 0
        ORDER BY event_timestamp
        """
    )

    payload = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "source_table": "gmail_memory",
        "reason": "legacy table dropped; no canonical column exists for thread history",
        "row_count": len(rows),
        "threads": rows,
    }

    OUTPUT_PATH.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=_json_default),
        encoding="utf-8",
    )
    return len(rows)


def main() -> int:
    if not table_exists():
        logger.info("gmail_memory does not exist — nothing to export.")
        return 0

    count = export_threads()
    if count == 0:
        logger.info("No rows carried thread history — nothing archived.")
    else:
        logger.info("Archived %d thread(s) to %s", count, OUTPUT_PATH)
        logger.info("gmail_memory can now be dropped safely.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
