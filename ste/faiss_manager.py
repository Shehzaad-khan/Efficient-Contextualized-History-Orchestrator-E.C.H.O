"""
faiss_manager.py — STE Module
Echo Personal Memory System

The semantic index (architecture §8 STE, DB design `embedding_index`):
a FAISS IndexFlatL2 over unit-length all-MiniLM-L6-v2 vectors (384-d),
mirrored by a numpy cache, plus one `embedding_index` row per vector in
PostgreSQL. L2 distance is not cosine similarity — anything that needs a
similarity score must compute true cosine from the vectors (cosine()).

On disk: data_faiss/echo_faiss.index (the vectors) +
data_faiss/echo_faiss.index.meta.json (the memory_id of each row, in order).
Row offset N in the index belongs to the Nth memory id in that list. The same folder holds the
classifier's centroids.npz. data_faiss/ is git-ignored (personal data); set
ECHO_FAISS_INDEX_PATH to use another location.

Callers:
    Writes   enp/enrichment_pipeline.py   get_manager() (ENP thread):
                                          add() per item, save_index() per batch
    Reads    get_manager() — the same shared process-wide instance:
             rse/search_coordinator.py    full-index search (request threads)
             rse/time_anchor.py           nearest item for an anchor phrase
             wba/group_manager.py         item vectors for KNN group scoring
             enp/system_group_classifier.py  centroid recomputation

ENP and the readers share one instance (C4), so an item is semantically
searchable as soon as add() returns; the files on disk are only read once, at
startup.

Thread safety:
    - `self._lock` (RLock) guards every field. All methods take it, so one
      instance can be shared by the ENP thread and request threads. FAISS
      itself is NOT safe for add() running during search() on the same index.
    - search_all() and get_vector() are the locked read paths for outside
      callers. The state behind them (_index, _buffer, _memory_ids,
      _memory_id_to_offset) is private; nothing outside this file touches it.
    - The PostgreSQL upsert in add() runs outside the lock, so a slow Neon
      round trip never blocks searches.
    - `_manager_lock` guards creation of the shared instance in get_manager().
    - save_index() writes temp files and renames them over the real ones, so
      a reader (another process, or a load at startup) never reads a
      half-written file.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
import numpy as np

from ste import postgresql_manager

try:
    import faiss
except ImportError:  # pragma: no cover - dependency-driven
    faiss = None

EMBEDDING_VERSION = "all-MiniLM-L6-v2-v1"
VECTOR_DIMENSION = 384
PROJECT_ROOT = Path(__file__).resolve().parents[1]
# Runtime data, kept out of the source folders.
DEFAULT_INDEX_PATH = PROJECT_ROOT / "data_faiss" / "echo_faiss.index"

# Rows preallocated the first time a vector is added. The vector cache grows by
# doubling from here, so N appends cost O(N) row copies in total instead of the
# O(N^2) a per-add np.vstack would cost.
INITIAL_CAPACITY = 256

# Temp files are named "<stem>.tmp<suffix>" (echo_faiss.tmp.index and
# echo_faiss.tmp.index.meta.json), so the existing *.index / *.index.meta.json
# .gitignore rules already cover a temp file left behind by a crash.
_TEMP_MARKER = ".tmp"

# Shown wherever the index cannot be trusted. There is no rebuild script: with
# the files gone, load_index() starts empty and ENP re-embeds every row it sees
# as unprocessed.
_REBUILD_HINT = (
    "To rebuild: stop the backend, delete the files in data_faiss/, run "
    "UPDATE memory_items SET preprocessed = FALSE, then restart the backend "
    "(ENP re-embeds every row; rows marked 'failed' also need ENP_RETRY_FAILED=true)."
)

_UPSERT_EMBEDDING_ROW_SQL = """
    INSERT INTO embedding_index (
        memory_id,
        embedding_version,
        vector_dimension,
        is_active,
        embeddable_text
    )
    VALUES (:memory_id, :embedding_version, :vector_dimension, TRUE, :embeddable_text)
    ON CONFLICT (memory_id) DO UPDATE
    SET embedding_version = EXCLUDED.embedding_version,
        vector_dimension = EXCLUDED.vector_dimension,
        is_active = TRUE,
        indexed_at = NOW(),
        embeddable_text = EXCLUDED.embeddable_text
"""

logger = logging.getLogger(__name__)


def _metadata_path_for(index_path: Path) -> Path:
    return index_path.with_suffix(f"{index_path.suffix}.meta.json")


def _temp_path_for(index_path: Path) -> Path:
    return index_path.with_name(f"{index_path.stem}{_TEMP_MARKER}{index_path.suffix}")


def cosine(a: np.ndarray, b: np.ndarray) -> float:
    """True cosine similarity of two vectors (0.0 if either is all zeros)."""
    denominator = float(np.linalg.norm(a) * np.linalg.norm(b))
    if denominator == 0.0:
        return 0.0
    return float(np.dot(a, b) / denominator)


class FAISSManager:
    """
    IndexFlatL2 over unit-length MiniLM vectors, mirrored by a numpy cache.

    The numpy cache (``_buffer``) is the source of truth; ``_index`` is derived
    from it and kept in sync incrementally:

      * appending a new memory_id   -> index.add() (O(1)) plus one buffer row
      * re-embedding an existing id -> buffer row overwritten in place and the
        index marked stale. IndexFlatL2 exposes no single-row update, so the
        index is rebuilt lazily on the next read of ``_fresh_index`` — once per batch
        rather than once per changed item.
    """

    def __init__(
        self,
        index_path: str | Path | None = None,
        *,
        dimension: int = VECTOR_DIMENSION,
        embedding_version: str = EMBEDDING_VERSION,
    ) -> None:
        if faiss is None:  # pragma: no cover - dependency-driven
            raise RuntimeError(
                "faiss is required for Echo semantic indexing. "
                "Install ENP dependencies before running the pipeline."
            )

        self.dimension = dimension
        self.embedding_version = embedding_version
        self.index_path = Path(index_path or os.getenv("ECHO_FAISS_INDEX_PATH", DEFAULT_INDEX_PATH))
        # RLock: public methods read `self._fresh_index`, whose property takes the lock again.
        self._lock = threading.RLock()
        self._memory_ids: list[str] = []
        self._memory_id_to_offset: dict[str, int] = {}
        self._index = faiss.IndexFlatL2(self.dimension)
        self._index_stale = False
        self._buffer = np.empty((0, self.dimension), dtype=np.float32)
        self._count = 0

    # -- Derived views --------------------------------------------------------

    @property
    def _fresh_index(self):
        """The FAISS index, rebuilt first if in-place updates left it stale."""
        with self._lock:
            if self._index_stale:
                self._materialize_index()
            return self._index

    @property
    def metadata_path(self) -> Path:
        return _metadata_path_for(self.index_path)

    # -- Internals (caller holds self._lock) -----------------------------------

    def _ensure_vector(self, vector: np.ndarray) -> np.ndarray:
        arr = np.asarray(vector, dtype=np.float32).reshape(1, -1)
        if arr.shape[1] != self.dimension:
            raise ValueError(f"Expected vector dimension {self.dimension}, got {arr.shape[1]}")
        return arr

    def _rebuild_mappings(self) -> None:
        self._memory_id_to_offset = {memory_id: idx for idx, memory_id in enumerate(self._memory_ids)}

    def _reserve(self, needed: int) -> None:
        capacity = self._buffer.shape[0]
        if needed <= capacity:
            return
        new_capacity = max(needed, capacity * 2, INITIAL_CAPACITY)
        grown = np.empty((new_capacity, self.dimension), dtype=np.float32)
        if self._count:
            grown[: self._count] = self._buffer[: self._count]
        self._buffer = grown
        logger.debug("FAISS vector cache grown to %d rows", new_capacity)

    def _materialize_index(self) -> None:
        started_at = time.perf_counter()
        index = faiss.IndexFlatL2(self.dimension)
        if self._count:
            index.add(np.ascontiguousarray(self._buffer[: self._count]))
        self._index = index
        self._index_stale = False
        # DEBUG, not INFO: with a shared instance this can run once per query
        # while ENP is re-embedding (~3 ms per 2,500 vectors).
        logger.debug(
            "FAISS index rebuilt after in-place updates: %d vectors (%.0f ms)",
            self._count,
            (time.perf_counter() - started_at) * 1000,
        )

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
        """Record the vector in embedding_index (pooled connection; raises on DB error)."""
        postgresql_manager.execute(
            _UPSERT_EMBEDDING_ROW_SQL,
            {
                "memory_id": memory_id,
                "embedding_version": self.embedding_version,
                "vector_dimension": self.dimension,
                "embeddable_text": embeddable_text,
            },
        )

    def _clear(self) -> None:
        self._memory_ids = []
        self._memory_id_to_offset = {}
        self._index = faiss.IndexFlatL2(self.dimension)
        self._index_stale = False
        self._buffer = np.empty((0, self.dimension), dtype=np.float32)
        self._count = 0

    # -- Public API -----------------------------------------------------------

    def reset(self) -> None:
        """Drop every vector and mapping, keeping paths and configuration."""
        with self._lock:
            self._clear()
        logger.info("FAISS index reset to empty (%s)", self.index_path.name)

    def add(self, memory_id: str, vector: np.ndarray, embeddable_text: str | None = None) -> bool:
        """Add or replace the vector for memory_id, then upsert its embedding_index row.

        Returns True for a new memory_id, False when an existing one was re-embedded.
        Raises if the database write fails; the in-memory vector stays, and a retry
        of the same item overwrites it and upserts again.
        """
        memory_id = str(memory_id)
        arr = self._ensure_vector(vector)

        with self._lock:
            offset = self._memory_id_to_offset.get(memory_id)
            if offset is not None:
                # Re-embedding a known item: overwrite its row and defer the index
                # rebuild to the next read of `_fresh_index`.
                self._buffer[offset] = arr[0]
                self._index_stale = True
                is_new = False
            else:
                self._reserve(self._count + 1)
                self._buffer[self._count] = arr[0]
                self._count += 1
                self._memory_ids.append(memory_id)
                self._memory_id_to_offset[memory_id] = self._count - 1
                if not self._index_stale:
                    # A stale index gets rebuilt wholesale on its next read; appending
                    # to it now would only be thrown away.
                    self._index.add(arr)
                is_new = True
            total = self._count

        # Outside the lock: a Neon round trip must not block searches.
        self._upsert_embedding_row(memory_id, embeddable_text)
        logger.debug(
            "FAISS %s memory_id=%s (total %d vectors)",
            "added" if is_new else "re-embedded",
            memory_id,
            total,
        )
        return is_new

    def search_all(self, query_vector: np.ndarray, k: int) -> list[tuple[str, float]]:
        """Nearest k items over the WHOLE index, as (memory_id, cosine), best first.

        FAISS ranks by L2 distance; the returned score is true cosine similarity
        (vectors are unit-length), and the list is re-sorted by it.
        """
        query = self._ensure_vector(query_vector)
        with self._lock:
            index = self._fresh_index
            total = index.ntotal
            if total == 0:
                return []
            _, offsets = index.search(query, min(max(k, 1), total))
            # Copy the rows while locked: a re-embed may overwrite them in place.
            hits = [
                (self._memory_ids[offset], self._buffer[offset].copy())
                for offset in offsets[0]
                if offset >= 0
            ]

        results = [(memory_id, cosine(query[0], vector)) for memory_id, vector in hits]
        results.sort(key=lambda pair: pair[1], reverse=True)
        return results

    def get_vector(self, memory_id: str) -> np.ndarray | None:
        """A copy of the item's vector, or None when it is not indexed yet."""
        with self._lock:
            offset = self._memory_id_to_offset.get(str(memory_id))
            if offset is None:
                return None
            return self._buffer[offset].copy()

    def save_index(self, path: str | Path | None = None) -> Path:
        """Write the index and its metadata atomically (temp file + os.replace each).

        The index file is replaced first, then the metadata. A crash between the
        two leaves a count mismatch, which load_index() reports loudly instead of
        loading vectors under the wrong memory_ids.
        """
        started_at = time.perf_counter()
        with self._lock:
            target = Path(path or self.index_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            temp_index_path = _temp_path_for(target)
            temp_metadata_path = _metadata_path_for(temp_index_path)

            index = self._fresh_index
            total = index.ntotal
            metadata = {
                "dimension": self.dimension,
                "embedding_version": self.embedding_version,
                "memory_ids": self._memory_ids,
            }
            faiss.write_index(index, str(temp_index_path))
            temp_metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
            os.replace(temp_index_path, target)
            os.replace(temp_metadata_path, _metadata_path_for(target))
            self.index_path = target

        logger.info(
            "FAISS index saved: %d vectors to %s (%.0f ms)",
            total,
            target.name,
            (time.perf_counter() - started_at) * 1000,
        )
        return target

    def load_index(self, path: str | Path | None = None) -> None:
        """Replace this instance's contents with the index on disk.

        Neither file exists -> start empty (first run).
        Only one exists, or they disagree -> raise ValueError. Starting empty
        there would let the next save_index() overwrite the surviving vectors.
        The error message carries the rebuild steps (_REBUILD_HINT).
        """
        started_at = time.perf_counter()
        target = Path(path or self.index_path)
        metadata_path = _metadata_path_for(target)

        with self._lock:
            index_exists = target.exists()
            metadata_exists = metadata_path.exists()
            if not index_exists and not metadata_exists:
                self._clear()
                self.index_path = target
                logger.info("No FAISS index at %s — starting with an empty index", target)
                return
            if index_exists != metadata_exists:
                raise ValueError(
                    f"FAISS files incomplete: {target.name} exists={index_exists}, "
                    f"{metadata_path.name} exists={metadata_exists}. "
                    + _REBUILD_HINT
                )

            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            dimension = int(metadata.get("dimension", self.dimension))
            if dimension != self.dimension:
                raise ValueError(f"Expected FAISS dimension {self.dimension}, found {dimension}")

            stored_version = metadata.get("embedding_version")
            if stored_version != self.embedding_version:
                # Vectors from another model are not comparable with new ones.
                logger.warning(
                    "FAISS index was built with embedding_version=%s, current is %s. %s",
                    stored_version,
                    self.embedding_version,
                    _REBUILD_HINT,
                )

            index = faiss.read_index(str(target))
            if index.d != self.dimension:
                raise ValueError(f"Expected FAISS dimension {self.dimension}, index file has {index.d}")
            memory_ids = [str(memory_id) for memory_id in metadata.get("memory_ids", [])]
            if index.ntotal != len(memory_ids):
                raise ValueError(
                    f"FAISS index/vector metadata mismatch: index has {index.ntotal} vectors, "
                    f"metadata has {len(memory_ids)} memory_ids. "
                    + _REBUILD_HINT
                )

            self._index = index
            self._index_stale = False
            self._memory_ids = memory_ids
            self._rebuild_mappings()
            self._load_vector_cache(index)
            self.index_path = target

        logger.info(
            "FAISS index loaded: %d vectors from %s (%.0f ms)",
            len(memory_ids),
            target.name,
            (time.perf_counter() - started_at) * 1000,
        )


_default_manager: FAISSManager | None = None
_manager_lock = threading.Lock()


def get_manager(index_path: str | Path | None = None) -> FAISSManager:
    """Return the shared process-wide manager, loading it from disk on first use.

    A different index_path replaces the shared manager. If loading fails, the
    error is raised and the previous manager (or none) stays in place — a broken
    load is never cached as an empty index.
    """
    global _default_manager
    wanted_path = Path(index_path) if index_path is not None else None

    manager = _default_manager
    if manager is not None and (wanted_path is None or wanted_path == manager.index_path):
        return manager

    with _manager_lock:
        manager = _default_manager
        if manager is None or (wanted_path is not None and wanted_path != manager.index_path):
            fresh = FAISSManager(index_path=index_path)
            fresh.load_index()
            _default_manager = fresh
            logger.info("Shared FAISS manager ready (%s)", fresh.index_path.name)
        return _default_manager


