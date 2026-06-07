"""
Reset preprocessing flags so ENP will re-index selected rows.

Usage:
  python scripts/enp_reprocess.py --all-pending   # no-op helper message
  python scripts/enp_reprocess.py --source youtube
  python scripts/enp_reprocess.py --memory-id <uuid>
  python scripts/enp_reprocess.py --drift-only     # only rows missing embedding_index
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def _connect():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL is not set")
    return psycopg2.connect(url)


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset ENP preprocessed flags for re-indexing")
    parser.add_argument("--source", choices=["gmail", "chrome", "youtube"])
    parser.add_argument("--memory-id")
    parser.add_argument(
        "--drift-only",
        action="store_true",
        help="Only rows with preprocessed=TRUE but no active embedding_index row",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not any([args.source, args.memory_id, args.drift_only]):
        raise SystemExit("Specify --source, --memory-id, or --drift-only")

    clauses = ["is_deleted = FALSE"]
    params: list = []

    if args.memory_id:
        clauses.append("memory_id = %s")
        params.append(args.memory_id)
    if args.source:
        clauses.append("source_type = %s")
        params.append(args.source)
    if args.drift_only:
        clauses.append(
            """
            memory_id IN (
                SELECT mi.memory_id
                FROM memory_items mi
                LEFT JOIN embedding_index ei
                  ON ei.memory_id = mi.memory_id AND ei.is_active = TRUE
                WHERE mi.preprocessed = TRUE
                  AND mi.is_deleted = FALSE
                  AND ei.memory_id IS NULL
            )
            """
        )

    sql = f"""
        UPDATE memory_items
        SET preprocessed = FALSE,
            classified_by = 'pending',
            classification_confidence = NULL,
            last_updated_at = NOW()
        WHERE {' AND '.join(clauses)}
    """

    conn = _connect()
    try:
        with conn.cursor() as cur:
            if args.dry_run:
                cur.execute(f"SELECT COUNT(*) FROM memory_items WHERE {' AND '.join(clauses)}", params)
                count = cur.fetchone()[0]
                print(f"Dry run: would reset {count} row(s)")
                return
            cur.execute(sql, params)
            print(f"Reset preprocessed flag on {cur.rowcount} row(s)")
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
