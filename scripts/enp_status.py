"""
ENP pipeline health report — counts processed/unprocessed rows and index drift.

Usage (from repo root, venv active):
  python scripts/enp_status.py
  python scripts/enp_status.py --sample 5
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

DEFAULT_INDEX = ROOT / "enp" / "echo_faiss.index"


def _connect():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL is not set")
    return psycopg2.connect(url)


def _faiss_meta_count() -> int | None:
    meta_path = DEFAULT_INDEX.with_suffix(f"{DEFAULT_INDEX.suffix}.meta.json")
    if not meta_path.exists():
        return 0
    data = json.loads(meta_path.read_text(encoding="utf-8"))
    return len(data.get("memory_ids", []))


def main() -> None:
    parser = argparse.ArgumentParser(description="ENP indexing status report")
    parser.add_argument("--sample", type=int, default=0, help="Show N oldest unprocessed memory_ids")
    args = parser.parse_args()

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT source_type,
                       COUNT(*) FILTER (
                           WHERE preprocessed = FALSE
                             AND is_deleted = FALSE
                             AND COALESCE(classified_by, 'pending') <> 'failed'
                       ),
                       COUNT(*) FILTER (
                           WHERE preprocessed = FALSE
                             AND is_deleted = FALSE
                             AND classified_by = 'failed'
                       ),
                       COUNT(*) FILTER (WHERE preprocessed = TRUE AND is_deleted = FALSE),
                       COUNT(*) FILTER (WHERE is_deleted = TRUE)
                FROM memory_items
                GROUP BY source_type
                ORDER BY source_type
                """
            )
            by_source = cur.fetchall()

            cur.execute(
                """
                SELECT COUNT(*) FROM memory_items
                WHERE preprocessed = FALSE
                  AND is_deleted = FALSE
                  AND COALESCE(classified_by, 'pending') <> 'failed'
                """
            )
            pending = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*) FROM memory_items
                WHERE preprocessed = FALSE
                  AND is_deleted = FALSE
                  AND classified_by = 'failed'
                """
            )
            failed = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*) FROM embedding_index WHERE is_active = TRUE
                """
            )
            embedding_rows = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM memory_items mi
                LEFT JOIN embedding_index ei
                  ON ei.memory_id = mi.memory_id AND ei.is_active = TRUE
                WHERE mi.preprocessed = TRUE
                  AND mi.is_deleted = FALSE
                  AND ei.memory_id IS NULL
                """
            )
            processed_no_embedding = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM embedding_index ei
                JOIN memory_items mi ON mi.memory_id = ei.memory_id
                WHERE ei.is_active = TRUE
                  AND mi.preprocessed = FALSE
                  AND mi.is_deleted = FALSE
                """
            )
            embedding_not_preprocessed = cur.fetchone()[0]

            if args.sample > 0:
                cur.execute(
                    """
                    SELECT memory_id, source_type, title, first_ingested_at
                    FROM memory_items
                    WHERE preprocessed = FALSE
                      AND is_deleted = FALSE
                      AND COALESCE(classified_by, 'pending') <> 'failed'
                    ORDER BY first_ingested_at ASC
                    LIMIT %s
                    """,
                    (args.sample,),
                )
                samples = cur.fetchall()
            else:
                samples = []
    finally:
        conn.close()

    faiss_count = _faiss_meta_count()

    print("=== ENP status ===")
    print(f"Pending (preprocessed=FALSE): {pending}")
    print(f"Failed and skipped:           {failed}")
    print(f"embedding_index active rows:  {embedding_rows}")
    print(f"FAISS meta memory_ids:        {faiss_count if faiss_count is not None else 'n/a'}")
    print()
    print("By source (pending | failed | processed | deleted):")
    for source, pend, failed_count, done, deleted in by_source:
        print(f"  {source:8}  {pend:5}  {failed_count:5}  {done:5}  {deleted:5}")
    print()
    print("Drift checks:")
    print(f"  preprocessed=TRUE but no embedding_index: {processed_no_embedding}")
    print(f"  embedding_index active but not preprocessed: {embedding_not_preprocessed}")
    if processed_no_embedding or embedding_not_preprocessed:
        print("  -> run enp_reprocess.py or re-run pipeline for affected rows")
    if failed:
        print("  failed rows are skipped by default; rerun with ENP_RETRY_FAILED=true or enp_run_once.py --retry-failed")

    if samples:
        print()
        print(f"Oldest unprocessed (up to {args.sample}):")
        for memory_id, source_type, title, ingested in samples:
            title_preview = (title or "")[:60]
            print(f"  {memory_id}  {source_type}  {ingested}  {title_preview}")


if __name__ == "__main__":
    main()
