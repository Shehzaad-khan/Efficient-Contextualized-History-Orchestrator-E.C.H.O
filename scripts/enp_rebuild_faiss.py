"""
Rebuild enp/echo_faiss.index from embedding_index rows (fixes FAISS/DB drift).

Usage:
  python scripts/enp_rebuild_faiss.py
  python scripts/enp_rebuild_faiss.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from enp.embedding_generator import generate_embeddings  # noqa: E402
from ste.faiss_manager import FAISSManager  # noqa: E402


def _connect():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL is not set")
    return psycopg2.connect(url)


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild FAISS index from embedding_index")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch-size", type=int, default=32)
    args = parser.parse_args()

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT memory_id, embeddable_text
                FROM embedding_index
                WHERE is_active = TRUE
                  AND embeddable_text IS NOT NULL
                  AND TRIM(embeddable_text) <> ''
                ORDER BY indexed_at ASC
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        print("No active embedding_index rows with embeddable_text.")
        return

    print(f"Found {len(rows)} active embedding rows")
    if args.dry_run:
        return

    import faiss

    manager = FAISSManager()
    manager.index = faiss.IndexFlatL2(manager.dimension)
    manager.memory_ids = []
    manager.memory_id_to_offset = {}
    manager.vectors = manager.vectors[:0]

    memory_ids = [str(row[0]) for row in rows]
    texts = [row[1] for row in rows]

    added = 0
    for start in range(0, len(texts), args.batch_size):
        chunk_ids = memory_ids[start : start + args.batch_size]
        chunk_texts = texts[start : start + args.batch_size]
        vectors = generate_embeddings(chunk_texts, batch_size=args.batch_size)
        for memory_id, vector, text in zip(chunk_ids, vectors, chunk_texts):
            if manager.add(memory_id, vector, embeddable_text=text):
                added += 1

    manager.save_index()
    print(f"Rebuilt index: {added} vectors written to {manager.index_path}")


if __name__ == "__main__":
    main()
