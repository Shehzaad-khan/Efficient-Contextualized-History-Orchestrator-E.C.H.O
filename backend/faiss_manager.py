from __future__ import annotations

from typing import Iterable


class FaissManager:
    """Interface stub for ENP/RSE integration. Mir will fill the implementation."""

    def add(self, memory_id: str, vector) -> None:
        return None

    def search(self, query_vector, candidate_ids: Iterable[str], k: int = 20):
        return []

    def save_index(self, path: str) -> None:
        return None

    def load_index(self, path: str) -> None:
        return None


faiss_manager = FaissManager()
