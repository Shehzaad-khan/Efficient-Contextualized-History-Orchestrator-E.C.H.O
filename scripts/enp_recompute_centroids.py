"""
Recompute Stage-3 classifier centroids from confirmed items (architecture §9.3).

Run monthly (manually or via OS scheduler). Each category with 10+ confirmed
items gets its centroid replaced by the mean embedding of those items — the
'study' centroid drifts from generic seed text toward this user's actual study
content. Passive learning; no retraining.

Usage:
  python scripts/enp_recompute_centroids.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from enp.embedding_generator import generate_embedding  # noqa: E402
from enp.faiss_manager import get_manager  # noqa: E402
from enp.system_group_classifier import initialize_centroids, recompute_centroids  # noqa: E402


def main() -> None:
    get_manager().load_index()
    # Seed centroids first so categories without enough confirmed items
    # keep a sensible starting point.
    initialize_centroids(generate_embedding)

    updated = recompute_centroids()
    if not updated:
        print("No category reached the 10-confirmed-item threshold — centroids unchanged.")
        return
    for category, count in sorted(updated.items()):
        print(f"  {category:<15} recomputed from {count} confirmed items")


if __name__ == "__main__":
    main()
