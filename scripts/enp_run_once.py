"""
Process one ENP batch then exit (useful for smoke tests).

Usage:
  python scripts/enp_run_once.py
  python scripts/enp_run_once.py --batch-size 20
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from enp.enrichment_pipeline import DEFAULT_BATCH_SIZE, process_batch  # noqa: E402
from enp.embedding_generator import generate_embedding  # noqa: E402
from enp.system_group_classifier import initialize_centroids  # noqa: E402
from ste.faiss_manager import EMBEDDING_VERSION, VECTOR_DIMENSION, FAISSManager  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a single ENP batch")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("ENP_BATCH_SIZE", DEFAULT_BATCH_SIZE)))
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Include rows previously marked classified_by='failed'",
    )
    args = parser.parse_args()

    if args.retry_failed:
        os.environ["ENP_RETRY_FAILED"] = "true"

    manager = FAISSManager()
    manager.load_index()
    initialize_centroids(generate_embedding)

    processed, failed = process_batch(manager, batch_size=args.batch_size)
    print(
        f"Done: processed={processed} failed={failed} "
        f"(embedding={EMBEDDING_VERSION} dim={VECTOR_DIMENSION} faiss_vectors={manager.index.ntotal})"
    )
    if processed == 0 and failed == 0:
        print("No unprocessed items in memory_items.")


if __name__ == "__main__":
    main()
