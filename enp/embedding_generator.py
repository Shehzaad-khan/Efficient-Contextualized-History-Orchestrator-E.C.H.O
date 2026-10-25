"""
embedding_generator.py — text -> 384-d arctic-embed-xs vectors (architecture §5, §9; ENP module)
Echo Personal Memory System

Callers:
  enp/enrichment_pipeline.py      item embeddings (ENP thread), centroid seeds -> document path
  rse/search_coordinator.py       query variants  (FastAPI threadpool, per request) -> query path
  rse/time_anchor.py              anchor query    (FastAPI threadpool, per request) -> query path

Two paths, one model. snowflake-arctic-embed-xs is trained for asymmetric search: a
question is embedded with the prompt "Represent this sentence for searching relevant
passages: " in front, a document with no prompt. The prompts come from the model's own
config (model.prompts), not from a string here. Stored items and centroid seeds always
take the document path; only text typed as a question takes the query path.

One shared SentenceTransformer per process, loaded on first use (~1-3 s).

Threading (checked by simulation, 2026-10-17):
  - Loading is NOT thread-safe: 8 threads loading at once each built their own
    model and all 8 crashed ("Cannot copy out of meta tensor"). In the backend
    that is the ENP thread's centroid seeding racing the first /query. So the
    load runs under _model_lock, once.
  - Encoding with the loaded model IS safe in parallel (320 encodes from 8
    threads: no errors, vectors identical to single-threaded ones), so encode()
    is not locked and a query never waits for an ENP batch.

Vectors come out unit-length (norm 1.0): the model ends with a Normalize layer
(Transformer -> Pooling(cls) -> Normalize, checked 2026-10-25), so
normalize_embeddings=False changes nothing. _validate_embeddings enforces it:
a vector whose norm is not 1.0 +- 0.001 raises instead of reaching the index. For
unit vectors, L2 order equals cosine order; the cosine that ste/faiss_manager.py
computes is therefore correct, just no longer necessary for ranking.
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

from bisect import bisect_right
import logging
import os
from pathlib import Path
import threading
import time
from typing import Any, Sequence

import numpy as np

from enp.topic_extractor import TextPart
from rse.config import CROSS_ENCODER_MODEL
from ste.faiss_manager import VECTOR_DIMENSION

MODEL_REPO_ID = "Snowflake/snowflake-arctic-embed-xs"
DEFAULT_BATCH_SIZE = 32
HF_OFFLINE_ENV = "HF_HUB_OFFLINE"
# Names of the two prompts in the model's config (model.prompts).
DOCUMENT_PROMPT_NAME = "document"
QUERY_PROMPT_NAME = "query"
UNIT_NORM_TOLERANCE = 0.001
# fit_to_window tokenizes at most this many characters of one part (see _window_prefix).
PREFIX_CHARS = 40_000

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
        logger.info("Loading embedding model %s", MODEL_REPO_ID)
        try:
            loaded = SentenceTransformer(MODEL_REPO_ID)
        except Exception:
            if _offline_set_here:
                # e.g. a half-finished earlier download left an incomplete snapshot.
                logger.error(
                    "Offline load of %s failed. Delete its folder in %s or set %s=0, then restart",
                    MODEL_REPO_ID,
                    _hub_cache_dir(),
                    HF_OFFLINE_ENV,
                )
            raise
        missing_prompts = [
            name for name in (DOCUMENT_PROMPT_NAME, QUERY_PROMPT_NAME) if name not in loaded.prompts
        ]
        if missing_prompts:
            # A model without its query prompt would embed questions as documents: no error, worse search.
            raise RuntimeError(f"{MODEL_REPO_ID} has no prompt named {missing_prompts}; found {sorted(loaded.prompts)}")
        logger.info(
            "Embedding model %s loaded on %s in %.0f ms",
            MODEL_REPO_ID,
            loaded.device,
            (time.perf_counter() - started_at) * 1000,
        )
        _model = loaded
        return loaded


def _content_token_budget() -> int:
    """Tokens of text the model reads: its window minus the special tokens ([CLS], [SEP])."""
    model = get_model()
    return int(model.max_seq_length) - model.tokenizer.num_special_tokens_to_add(pair=False)


def _token_end_offsets(text: str) -> list[int]:
    """End character of each content token of text (no special tokens), in order."""
    encoded = get_model().tokenizer(
        text, add_special_tokens=False, return_offsets_mapping=True, verbose=False
    )
    return [end for _, end in encoded["offset_mapping"]]


def _window_prefix(text: str, budget: int) -> str:
    """text, or its first PREFIX_CHARS (cut at a space) when that already holds more than a window of tokens.

    The output never uses more than `budget` tokens of one part, so a multi-megabyte
    attachment need not be tokenized whole. Cutting at whitespace keeps the tokens of
    the prefix identical to the first tokens of the full text.
    """
    if len(text) <= PREFIX_CHARS:
        return text
    cut = PREFIX_CHARS
    while cut > 0 and not text[cut - 1].isspace() and not text[cut].isspace():
        cut -= 1
    prefix = text[:cut].rstrip() if cut > 0 else text[:PREFIX_CHARS]
    return prefix if len(_token_end_offsets(prefix)) > budget else text


def _cut_to_tokens(text: str, token_ends: list[int], max_tokens: int) -> str:
    """The longest start of text that has at most max_tokens tokens and ends on a whole word.

    A word is a run without whitespace. A text that is one single word has no
    boundary to back off to, so it is cut at the token instead.
    """
    if max_tokens >= len(token_ends):
        return text
    if max_tokens <= 0:
        return ""
    cut = token_ends[max_tokens - 1]
    if cut < len(text) and not text[cut].isspace():
        word_start = cut
        while word_start > 0 and not text[word_start - 1].isspace():
            word_start -= 1
        if word_start > 0:
            cut = word_start
    return text[:cut].rstrip() or text[: token_ends[max_tokens - 1]]


def fit_to_window(parts: Sequence[TextPart]) -> str:
    """Join the parts into the text the model will embed, cut to its window with its own tokenizer.

    The window is read from the model (max_seq_length minus special tokens), not
    hard-coded. Steps:
      1. In order, each part takes min(its tokens, its max_tokens, what is left).
         A part with max_tokens None is kept whole unless the window runs out.
      2. Room still left (a short last part, a word-boundary backoff) goes back to
         the parts that were cut, in order: no body is cut while room is unused.
      3. A final re-tokenization of the joined text is the safety net.
    Cuts fall on whole words. The result is what must be stored as embeddable_text.
    """
    budget = _content_token_budget()
    texts = [_window_prefix((part.text or "").strip(), budget) for part in parts]
    kept = [index for index, text in enumerate(texts) if text]
    if not kept:
        return ""

    token_ends = {index: _token_end_offsets(texts[index]) for index in kept}
    cuts: dict[int, str] = {}
    used: dict[int, int] = {}
    remaining = budget
    for index in kept:
        cap = parts[index].max_tokens
        allowed = min(len(token_ends[index]), remaining, cap if cap is not None else remaining)
        cuts[index] = _cut_to_tokens(texts[index], token_ends[index], allowed)
        used[index] = bisect_right(token_ends[index], len(cuts[index]))
        remaining -= used[index]

    for index in kept:
        if remaining <= 0:
            break
        if used[index] >= len(token_ends[index]):
            continue
        cuts[index] = _cut_to_tokens(texts[index], token_ends[index], used[index] + remaining)
        new_used = bisect_right(token_ends[index], len(cuts[index]))
        remaining -= new_used - used[index]
        used[index] = new_used

    joined = " ".join(cuts[index] for index in kept if cuts[index])
    joined_ends = _token_end_offsets(joined)
    if len(joined_ends) > budget:
        logger.warning("fit_to_window: joined text had %d tokens after cutting; cutting again to %d", len(joined_ends), budget)
        joined = _cut_to_tokens(joined, joined_ends, budget)
    return joined


def _validate_embeddings(embeddings: np.ndarray) -> np.ndarray:
    matrix = np.asarray(embeddings, dtype=np.float32)
    if matrix.ndim == 1:
        matrix = matrix.reshape(1, -1)
    if matrix.shape[1] != VECTOR_DIMENSION:
        raise ValueError(
            f"Expected embedding dimension {VECTOR_DIMENSION}, got {matrix.shape[1]}"
        )
    norms = np.linalg.norm(matrix, axis=1)
    if not np.all(np.abs(norms - 1.0) <= UNIT_NORM_TOLERANCE):
        raise ValueError(
            f"Embeddings must have unit length (1.0 +- {UNIT_NORM_TOLERANCE}); "
            f"got norms from {norms.min():.4f} to {norms.max():.4f}"
        )
    return matrix


def _encode(texts: Sequence[str], prompt_name: str, batch_size: int) -> np.ndarray:
    """Embed many texts with one of the model's prompts -> float32 array (len(texts), 384).

    None becomes "". An empty text still gets a vector (of the bare special
    tokens, plus the prompt on the query path), which means nothing; callers
    make sure there is text to embed.
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
        prompt_name=prompt_name,
        batch_size=batch_size,
        show_progress_bar=False,
        convert_to_numpy=True,
        normalize_embeddings=False,
    )
    logger.debug(
        "Embedded %d text(s) on the %s path in %.0f ms",
        len(clean_texts),
        prompt_name,
        (time.perf_counter() - started_at) * 1000,
    )
    return _validate_embeddings(embeddings)


def generate_embeddings(texts: Sequence[str], batch_size: int = DEFAULT_BATCH_SIZE) -> np.ndarray:
    """Document path: embed many stored texts -> float32 array of shape (len(texts), 384)."""
    return _encode(texts, DOCUMENT_PROMPT_NAME, batch_size)


def generate_embedding(text: str) -> np.ndarray:
    """Document path: embed one stored text (or centroid seed) -> float32 vector of shape (384,)."""
    return generate_embeddings([text], batch_size=1)[0]


def generate_query_embeddings(texts: Sequence[str], batch_size: int = DEFAULT_BATCH_SIZE) -> np.ndarray:
    """Query path: embed many questions (with the model's query prompt) -> shape (len(texts), 384)."""
    return _encode(texts, QUERY_PROMPT_NAME, batch_size)


def generate_query_embedding(text: str) -> np.ndarray:
    """Query path: embed one question -> float32 vector of shape (384,)."""
    return generate_query_embeddings([text], batch_size=1)[0]
