from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Iterable

import numpy as np
import psycopg2
from dotenv import load_dotenv

load_dotenv()

try:
    import faiss
except ImportError:  # pragma: no cover - dependency-driven
    faiss = None

EMBEDDING_VERSION = "all-MiniLM-L6-v2-v1"
VECTOR_DIMENSION = 384
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INDEX_PATH = PROJECT_ROOT / "enp" / "echo_faiss.index"

# Rows preallocated the first time a vector is added. The vector cache grows by
# doubling from here, so N appends cost O(N) row copies in total instead of the
# O(N^2) a per-add np.vstack would cost.
INITIAL_CAPACITY = 256

logger = logging.getLogger(__name__)


class FAISSManager:
    """
    IndexFlatL2 over UNNORMALIZED MiniLM vectors, mirrored by a numpy cache.

    The numpy cache (``vectors``) is the source of truth; ``index`` is derived
    from it and kept in sync incrementally:

      * appending a new memory_id   -> index.add() (O(1)) plus one buffer row
      * re-embedding an existing id -> buffer row overwritten in place and the
        index marked stale. IndexFlatL2 exposes no single-row update, so the
        index is rebuilt lazily on the next read of ``.index`` — once per batch
        rather than once per changed item.
    """

    def __init__(
        self,
        index_path: str | Path | None = None,
        *,
        dimension: int = VECTOR_DIMENSION,
        embedding_version: str = EMBEDDING_VERSION,
        db_url: str | None = None,
    ) -> None:
        if faiss is None:  # pragma: no cover - dependency-driven
            raise RuntimeError(
                "faiss is required for Echo semantic indexing. "
                "Install ENP dependencies before running the pipeline."
            )

        self.dimension = dimension
        self.embedding_version = embedding_version
        self.db_url = db_url or os.getenv("DATABASE_URL")
        self.index_path = Path(index_path or os.getenv("ECHO_FAISS_INDEX_PATH", DEFAULT_INDEX_PATH))
        self.memory_ids: list[str] = []
        self.memory_id_to_offset: dict[str, int] = {}
        self._index = faiss.IndexFlatL2(self.dimension)
        self._index_stale = False
        self._buffer = np.empty((0, self.dimension), dtype=np.float32)
        self._count = 0

    # -- Derived views --------------------------------------------------------

    @property
    def index(self):
        """The FAISS index, rebuilt first if in-place updates left it stale."""
        if self._index_stale:
            self._materialize_index()
        return self._index

    @property
    def vectors(self) -> np.ndarray:
        """Live (n, dim) view of the cached vectors — not a copy."""
        return self._buffer[: self._count]

    @property
    def metadata_path(self) -> Path:
        return self.index_path.with_suffix(f"{self.index_path.suffix}.meta.json")

    # -- Internals ------------------------------------------------------------

    def _ensure_vector(self, vector: np.ndarray) -> np.ndarray:
        arr = np.asarray(vector, dtype=np.float32).reshape(1, -1)
        if arr.shape[1] != self.dimension:
            raise ValueError(f"Expected vector dimension {self.dimension}, got {arr.shape[1]}")
        return arr

    def _rebuild_mappings(self) -> None:
        self.memory_id_to_offset = {memory_id: idx for idx, memory_id in enumerate(self.memory_ids)}

    def _reserve(self, needed: int) -> None:
        capacity = self._buffer.shape[0]
        if needed <= capacity:
            return
        new_capacity = max(needed, capacity * 2, INITIAL_CAPACITY)
        grown = np.empty((new_capacity, self.dimension), dtype=np.float32)
        if self._count:
            grown[: self._count] = self._buffer[: self._count]
        self._buffer = grown

    def _materialize_index(self) -> None:
        index = faiss.IndexFlatL2(self.dimension)
        if self._count:
            index.add(np.ascontiguousarray(self.vectors))
        self._index = index
        self._index_stale = False

    def _load_vector_cache(self, index) -> None:
        """Populate the numpy cache from a freshly read index."""
        total = index.ntotal
        self._buffer = np.empty((max(total, INITIAL_CAPACITY), self.dimension), dtype=np.float32)
        self._count = total
        if not total:
            return
        try:
            self._buffer[:total] = index.reconstruct_n(0, total)
        except (AttributeError, RuntimeError):  # pragma: no cover - older faiss builds
            for offset in range(total):
                self._buffer[offset] = index.reconstruct(offset)

    def _upsert_embedding_row(self, memory_id: str, embeddable_text: str | None) -> None:
        if not self.db_url:
            logger.warning("DATABASE_URL not set; skipping embedding_index upsert for %s", memory_id)
            return

        conn = psycopg2.connect(self.db_url)
        try:
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO embedding_index (
                            memory_id,
                            embedding_version,
                            vector_dimension,
                            is_active,
                            embeddable_text
                        )
                        VALUES (%s, %s, %s, TRUE, %s)
                        ON CONFLICT (memory_id) DO UPDATE
                        SET embedding_version = EXCLUDED.embedding_version,
                            vector_dimension = EXCLUDED.vector_dimension,
                            is_active = TRUE,
                            indexed_at = NOW(),
                            embeddable_text = EXCLUDED.embeddable_text
                        """,
                        (
                            memory_id,
                            self.embedding_version,
                            self.dimension,
                            embeddable_text,
                        ),
                    )
        finally:
            conn.close()

    # -- Public API -----------------------------------------------------------

    def reset(self) -> None:
        """Drop every vector and mapping, keeping paths and configuration."""
        self.memory_ids = []
        self.memory_id_to_offset = {}
        self._index = faiss.IndexFlatL2(self.dimension)
        self._index_stale = False
        self._buffer = np.empty((0, self.dimension), dtype=np.float32)
        self._count = 0

    def add(self, memory_id: str, vector: np.ndarray, embeddable_text: str | None = None) -> bool:
        memory_id = str(memory_id)
        arr = self._ensure_vector(vector)

        offset = self.memory_id_to_offset.get(memory_id)
        if offset is not None:
            # Re-embedding a known item: overwrite its row and defer the index
            # rebuild to the next read of `.index`.
            self._buffer[offset] = arr[0]
            self._index_stale = True
            self._upsert_embedding_row(memory_id, embeddable_text)
            return False

        self._reserve(self._count + 1)
        self._buffer[self._count] = arr[0]
        self._count += 1
        self.memory_ids.append(memory_id)
        self.memory_id_to_offset[memory_id] = self._count - 1
        if not self._index_stale:
            # A stale index gets rebuilt wholesale on its next read; appending
            # to it now would only be thrown away.
            self._index.add(arr)
        self._upsert_embedding_row(memory_id, embeddable_text)
        return True

    def search(
        self,
        query_vector: np.ndarray,
        candidate_ids: Iterable[str],
        k: int = 20,
    ) -> list[tuple[str, float]]:
        ordered_ids = [memory_id for memory_id in candidate_ids if memory_id in self.memory_id_to_offset]
        if not ordered_ids:
            return []

        query = self._ensure_vector(query_vector)
        candidate_offsets = [self.memory_id_to_offset[memory_id] for memory_id in ordered_ids]
        candidate_vectors = np.ascontiguousarray(self.vectors[candidate_offsets])

        temp_index = faiss.IndexFlatL2(self.dimension)
        temp_index.add(candidate_vectors)
        limit = min(max(k, 1), len(ordered_ids))
        distances, indices = temp_index.search(query, limit)

        results: list[tuple[str, float]] = []
        for distance, idx in zip(distances[0], indices[0]):
            if idx < 0:
                continue
            results.append((ordered_ids[idx], float(distance)))
        return results

    def save_index(self, path: str | Path | None = None) -> Path:
        target = Path(path or self.index_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        faiss.write_index(self.index, str(target))

        metadata = {
            "dimension": self.dimension,
            "embedding_version": self.embedding_version,
            "memory_ids": self.memory_ids,
        }
        metadata_path = target.with_suffix(f"{target.suffix}.meta.json")
        metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        self.index_path = target
        return target

    def load_index(self, path: str | Path | None = None) -> None:
        target = Path(path or self.index_path)
        metadata_path = target.with_suffix(f"{target.suffix}.meta.json")

        if not target.exists() or not metadata_path.exists():
            self.reset()
            self.index_path = target
            return

        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        dimension = int(metadata.get("dimension", self.dimension))
        if dimension != self.dimension:
            raise ValueError(f"Expected FAISS dimension {self.dimension}, found {dimension}")

        index = faiss.read_index(str(target))
        memory_ids = [str(memory_id) for memory_id in metadata.get("memory_ids", [])]
        if index.ntotal != len(memory_ids):
            raise ValueError(
                f"FAISS index/vector metadata mismatch: index has {index.ntotal} vectors, "
                f"metadata has {len(memory_ids)} memory_ids"
            )

        self._index = index
        self._index_stale = False
        self.memory_ids = memory_ids
        self._rebuild_mappings()
        self._load_vector_cache(index)
        self.index_path = target


_default_manager: FAISSManager | None = None


def get_manager(index_path: str | Path | None = None) -> FAISSManager:
    global _default_manager
    if _default_manager is None:
        _default_manager = FAISSManager(index_path=index_path)
        _default_manager.load_index()
    elif index_path is not None and Path(index_path) != _default_manager.index_path:
        _default_manager = FAISSManager(index_path=index_path)
        _default_manager.load_index()
    return _default_manager


def add(memory_id: str, vector: np.ndarray, embeddable_text: str | None = None) -> bool:
    return get_manager().add(memory_id, vector, embeddable_text=embeddable_text)


def search(query_vector: np.ndarray, candidate_ids: Iterable[str], k: int = 20) -> list[tuple[str, float]]:
    return get_manager().search(query_vector, candidate_ids, k=k)


def save_index(path: str | Path | None = None) -> Path:
    return get_manager(path).save_index(path)


def load_index(path: str | Path | None = None) -> FAISSManager:
    manager = get_manager(path)
    manager.load_index(path)
    return manager
