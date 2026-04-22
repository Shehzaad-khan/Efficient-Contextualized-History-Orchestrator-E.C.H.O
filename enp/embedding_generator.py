from __future__ import annotations

from functools import lru_cache
import logging
from typing import Sequence

import numpy as np

MODEL_NAME = "all-MiniLM-L6-v2"
VECTOR_DIMENSION = 384

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_model():
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:  # pragma: no cover - dependency-driven
        raise RuntimeError(
            "sentence-transformers is required for Echo embeddings. "
            "Install ENP dependencies before running the pipeline."
        ) from exc

    logger.info("Loading embedding model %s", MODEL_NAME)
    return SentenceTransformer(MODEL_NAME)


def _validate_embeddings(embeddings: np.ndarray) -> np.ndarray:
    matrix = np.asarray(embeddings, dtype=np.float32)
    if matrix.ndim == 1:
        matrix = matrix.reshape(1, -1)
    if matrix.shape[1] != VECTOR_DIMENSION:
        raise ValueError(
            f"Expected embedding dimension {VECTOR_DIMENSION}, got {matrix.shape[1]}"
        )
    return matrix


def generate_embeddings(texts: Sequence[str], batch_size: int = 32) -> np.ndarray:
    clean_texts = [str(text or "").strip() for text in texts]
    if not clean_texts:
        return np.empty((0, VECTOR_DIMENSION), dtype=np.float32)

    model = get_model()
    embeddings = model.encode(
        clean_texts,
        batch_size=batch_size,
        show_progress_bar=False,
        convert_to_numpy=True,
        normalize_embeddings=False,
    )
    return _validate_embeddings(embeddings)


def generate_embedding(text: str) -> np.ndarray:
    vector = generate_embeddings([text], batch_size=1)
    return vector[0]
