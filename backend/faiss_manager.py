from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Iterable


class FaissManager:
    """Lightweight backend implementation that works in demo/local mode.

    If the full FAISS stack is present, this wrapper can still delegate to the
    richer implementation in ste/faiss_manager.py. When not installed, it keeps a
    simple in-memory vector store so the app remains bootable.
    """

    def __init__(self, index_path: str | Path | None = None) -> None:
        self.index_path = Path(index_path) if index_path else Path.cwd() / "echo_backend_faiss.json"
        self._memory_ids: list[str] = []
        self._vectors: dict[str, list[float]] = {}
        self.load_index(self.index_path)

    @property
    def memory_ids(self) -> list[str]:
        return list(self._memory_ids)

    def _cosine_similarity(self, a: list[float], b: list[float]) -> float:
        if len(a) != len(b):
            raise ValueError("Vector dimensions do not match")
        dot = sum(x * y for x, y in zip(a, b))
        mag_a = math.sqrt(sum(x * x for x in a))
        mag_b = math.sqrt(sum(x * x for x in b))
        if mag_a == 0 or mag_b == 0:
            return 0.0
        return dot / (mag_a * mag_b)

    def add(self, memory_id: str, vector) -> None:
        self._memory_ids = [item for item in self._memory_ids if item != str(memory_id)]
        self._memory_ids.append(str(memory_id))
        arr = [float(v) for v in vector]
        self._vectors[str(memory_id)] = arr
        self.save_index(self.index_path)

    def search(self, query_vector, candidate_ids: Iterable[str], k: int = 20):
        ids = [str(item) for item in candidate_ids]
        if not ids:
            return []
        query = [float(v) for v in query_vector]
        scored = []
        for memory_id in ids:
            if memory_id in self._vectors:
                score = self._cosine_similarity(query, self._vectors[memory_id])
                scored.append((memory_id, float(score)))
        scored.sort(key=lambda item: item[1], reverse=True)
        return scored[:k]

    def save_index(self, path: str | Path | None = None) -> None:
        target = Path(path) if path is not None else self.index_path
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = {"memory_ids": self._memory_ids, "vectors": self._vectors}
        target.write_text(json.dumps(payload), encoding="utf-8")

    def load_index(self, path: str | Path | None = None) -> None:
        target = Path(path) if path is not None else self.index_path
        if not target.exists():
            return
        try:
            payload = json.loads(target.read_text(encoding="utf-8"))
            self._memory_ids = [str(item) for item in payload.get("memory_ids", [])]
            self._vectors = {str(k): [float(v) for v in values] for k, values in payload.get("vectors", {}).items()}
        except Exception:
            self._memory_ids = []
            self._vectors = {}


faiss_manager = FaissManager()
