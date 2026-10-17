"""
embedding_generator.py — text -> 384-d MiniLM vectors (architecture §5, §9; ENP module)
Echo Personal Memory System

Callers:
  enp/enrichment_pipeline.py      item embeddings (ENP thread), centroid seeds
  rse/search_coordinator.py       query variants  (FastAPI threadpool, per request)
  rse/time_anchor.py              anchor query    (FastAPI threadpool, per request)
  scripts/enp_rebuild_faiss.py, enp_recompute_centroids.py, enp_run_once.py

One shared SentenceTransformer per process, loaded on first use (~1-3 s).

Threading (checked by simulation, 2026-10-17):
  - Loading is NOT thread-safe: 8 threads loading at once each built their own
    model and all 8 crashed ("Cannot copy out of meta tensor"). In the backend
    that is the ENP thread's centroid seeding racing the first /query. So the
    load runs under _model_lock, once.
  - Encoding with the loaded model IS safe in parallel (320 encodes from 8
    threads: no errors, vectors identical to single-threaded ones), so encode()
    is not locked and a query never waits for an ENP batch.

Vectors come out unit-length (norm 1.0): all-MiniLM-L6-v2 ends with a Normalize
layer, so normalize_embeddings=False changes nothing. Checked 2026-10-17: the
loaded model is Transformer -> Pooling -> Normalize, and all 110 vectors in the
saved FAISS index have norm 1.0. For unit vectors, L2 order equals cosine order;
the cosine that ste/faiss_manager.py computes is therefore correct, just no
longer necessary (CLAUDE.md's "UNNORMALIZED" note is out of date, tracker C14).
Changing the model or normalization requires a new
ste.faiss_manager.EMBEDDING_VERSION and a full re-embed.

Offline model loading: by default the Hugging Face library contacts
huggingface.co on every load to check for a newer model version. Measured
2026-10-17: 6.1 s per load online vs 0.5 s with HF_HUB_OFFLINE=1 (and
local_files_only=True still went online, 2.4 s). So at import, if every model
this app uses (this one + RSE's cross-encoder) is already in the local cache,
this module sets HF_HUB_OFFLINE=1 for the process. A missing model keeps the
hub on so the first run can download it. An HF_HUB_OFFLINE the user set
themselves is never overridden. Side benefit: the model can never change
under an existing FAISS index through a silent upstream update.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
import threading
import time
from typing import Any, Sequence

import numpy as np

from rse.config import CROSS_ENCODER_MODEL
from ste.faiss_manager import VECTOR_DIMENSION

MODEL_NAME = "all-MiniLM-L6-v2"
MODEL_REPO_ID = f"sentence-transformers/{MODEL_NAME}"
DEFAULT_BATCH_SIZE = 32
HF_OFFLINE_ENV = "HF_HUB_OFFLINE"

logger = logging.getLogger(__name__)

_model: Any = None
_model_lock = threading.Lock()
_offline_set_here = False


def _hub_cache_dir() -> Path:
    """The Hugging Face hub cache, resolved the way huggingface_hub does."""
    if os.getenv("HF_HUB_CACHE"):
        return Path(os.environ["HF_HUB_CACHE"])
    hf_home = os.getenv("HF_HOME") or Path.home() / ".cache" / "huggingface"
    return Path(hf_home) / "hub"


def _is_cached(repo_id: str) -> bool:
    """True when the cache holds at least one downloaded snapshot of repo_id."""
    snapshots = _hub_cache_dir() / f"models--{repo_id.replace('/', '--')}" / "snapshots"
    return snapshots.is_dir() and any(child.is_dir() for child in snapshots.iterdir())


def _prefer_local_models() -> None:
    """Set HF_HUB_OFFLINE=1 when every model the app needs is cached.

    Must run before any Hugging Face library is imported (they read the
    variable once, at import). Both imports in this app are lazy (here and in
    rse/reranker.py), so module import time is early enough. If they were
    imported first, the only cost is the old online check, never a failure.
    """
    global _offline_set_here
    if os.getenv(HF_OFFLINE_ENV) is not None:
        logger.debug("%s already set (%s); leaving it", HF_OFFLINE_ENV, os.environ[HF_OFFLINE_ENV])
        return
    missing = [repo for repo in (MODEL_REPO_ID, CROSS_ENCODER_MODEL) if not _is_cached(repo)]
    if missing:
        logger.info("Models not cached yet (%s); Hugging Face hub stays online to download them", ", ".join(missing))
        return
    os.environ[HF_OFFLINE_ENV] = "1"
    _offline_set_here = True
    logger.info("All models cached; loading them offline (%s=1)", HF_OFFLINE_ENV)


_prefer_local_models()


def get_model():
    """The shared SentenceTransformer, loaded once (thread-safe).

    A failed load is not cached: the next call tries again.
    """
    global _model
    model = _model
    if model is not None:  # fast path, no lock once loaded
        return model

    with _model_lock:
        if _model is not None:  # another thread loaded it while we waited
            return _model
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:  # pragma: no cover - dependency-driven
            raise RuntimeError(
                "sentence-transformers is required for Echo embeddings. "
                "Install ENP dependencies before running the pipeline."
            ) from exc

        started_at = time.perf_counter()
        logger.info("Loading embedding model %s", MODEL_NAME)
        try:
            loaded = SentenceTransformer(MODEL_NAME)
        except Exception:
            if _offline_set_here:
                # e.g. a half-finished earlier download left an incomplete snapshot.
                logger.error(
                    "Offline load of %s failed. Delete its folder in %s or set %s=0, then restart",
                    MODEL_NAME,
                    _hub_cache_dir(),
                    HF_OFFLINE_ENV,
                )
            raise
        logger.info(
            "Embedding model %s loaded on %s in %.0f ms",
            MODEL_NAME,
            loaded.device,
            (time.perf_counter() - started_at) * 1000,
        )
        _model = loaded
        return loaded


def _validate_embeddings(embeddings: np.ndarray) -> np.ndarray:
    matrix = np.asarray(embeddings, dtype=np.float32)
    if matrix.ndim == 1:
        matrix = matrix.reshape(1, -1)
    if matrix.shape[1] != VECTOR_DIMENSION:
        raise ValueError(
            f"Expected embedding dimension {VECTOR_DIMENSION}, got {matrix.shape[1]}"
        )
    return matrix


def generate_embeddings(texts: Sequence[str], batch_size: int = DEFAULT_BATCH_SIZE) -> np.ndarray:
    """Embed many texts at once -> float32 array of shape (len(texts), 384).

    None becomes "". An empty text still gets a vector (of the bare special
    tokens), which means nothing; callers make sure there is text to embed.
    """
    clean_texts = [str(text or "").strip() for text in texts]
    if not clean_texts:
        return np.empty((0, VECTOR_DIMENSION), dtype=np.float32)

    empty_count = sum(1 for text in clean_texts if not text)
    if empty_count:
        logger.debug("Embedding %d empty text(s) out of %d", empty_count, len(clean_texts))

    model = get_model()
    started_at = time.perf_counter()
    embeddings = model.encode(
        clean_texts,
        batch_size=batch_size,
        show_progress_bar=False,
        convert_to_numpy=True,
        normalize_embeddings=False,
    )
    logger.debug(
        "Embedded %d text(s) in %.0f ms",
        len(clean_texts),
        (time.perf_counter() - started_at) * 1000,
    )
    return _validate_embeddings(embeddings)


def generate_embedding(text: str) -> np.ndarray:
    """Embed one text -> float32 vector of shape (384,)."""
    vector = generate_embeddings([text], batch_size=1)
    return vector[0]
