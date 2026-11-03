"""
One-off clean-up before dropping memory_items.is_deleted (migration 2026-11-03_drop_is_deleted.sql).

Items the old DELETE /items/{id} only flagged are hard-deleted here from every store
(Postgres with all child rows, the FAISS vector, Redis keys), through the same function
the app uses now (ste.retention.delete_items).

Usage (repo root, venv active, backend stopped so the FAISS file is not written twice):
  python scripts/purge_soft_deleted.py          # only counts and reports
  python scripts/purge_soft_deleted.py --yes    # deletes

Take a pg_dump -Fc of echo_db first (CLAUDE.md section 2). Output is counts only.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from sqlalchemy.exc import ProgrammingError  # noqa: E402

from ste import postgresql_manager  # noqa: E402
from ste.retention import delete_items  # noqa: E402

BATCH = 200


def flagged_ids() -> list[str]:
    """memory_ids still flagged is_deleted; empty when the column is already gone."""
    try:
        rows = postgresql_manager.fetchall("SELECT memory_id::text AS memory_id FROM memory_items WHERE is_deleted = TRUE")
    except ProgrammingError:
        print("memory_items.is_deleted does not exist (already migrated): nothing to purge.")
        return []
    return [row["memory_id"] for row in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description="Hard-delete rows the old soft delete only flagged")
    parser.add_argument("--yes", action="store_true", help="really delete (default: only count)")
    args = parser.parse_args()

    ids = flagged_ids()
    print(f"Soft-deleted rows: {len(ids)}")
    if not ids:
        return
    if not args.yes:
        print("Dry run. Take a pg_dump -Fc first, stop the backend, then re-run with --yes.")
        return

    items = vectors = redis_keys = 0
    for start in range(0, len(ids), BATCH):
        result = delete_items(ids[start:start + BATCH])
        items += result.items_deleted
        vectors += result.vectors_removed
        redis_keys += result.redis_keys_deleted
    print(f"Deleted {items} items, {vectors} vectors, {redis_keys} Redis keys.")
    print(f"Soft-deleted rows left: {len(flagged_ids())}")


if __name__ == "__main__":
    main()
