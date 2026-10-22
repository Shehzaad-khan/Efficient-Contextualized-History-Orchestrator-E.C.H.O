"""
RSE module configuration — every tunable number and the LLM choice live here.

Nothing in this file runs logic; it only defines constants. Other modules import
what they need (`from rse.config import RRF_K`). Outside rse/, two modules read it:
`enp/embedding_generator.py` (CROSS_ENCODER_MODEL) and the LLM callers
`enp/system_group_classifier.py` + `wba/insight_generator.py` (LLM_CONFIG).

LLM rule (decision A25, 2026-10-18): plug-in for every provider, specialised for
OpenAI. All LLM calls go through LangChain's `init_chat_model`, so switching
provider means changing only LLM_CONFIG below. When provider == "openai" the
OpenAI-only extras (Responses API, reasoning effort) are used; other providers
take the generic path and ignore those keys.
Providers with an installed LangChain package: 'openai' | 'anthropic' |
'google_genai'. ('ollama' needs `pip install langchain-ollama` first.)

The .env file is loaded here so `python -m rse.retrieval_engine` works on its own;
when the backend has already loaded it, this is a no-op (existing values win).
"""
import os
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# ── LLM — the single change point to swap provider or model ──────────────────
# Read-only mapping: it is shared by every request thread, so nothing may
# change it at runtime (assigning a key raises TypeError).
LLM_CONFIG: Mapping[str, Any] = MappingProxyType({
    # History: google_genai → openai gpt-4.1-mini on 2026-07-09 (Google free tier
    # allowed only 20 requests/day per model). gpt-4.1-mini → gpt-5.6-luna on
    # 2026-10-18 (A25): cheaper per token ($0.20 / $1.20 per 1M in/out vs
    # $0.40 / $1.60) and much stronger on reasoning benchmarks.
    # GOOGLE_API_KEY stays in .env — switch back by changing these strings.
    "provider": "openai",
    "parser_model": "gpt-5.6-luna",
    "synthesizer_model": "gpt-5.6-luna",
    # OpenAI-only extras (A25). Luna is a reasoning model: "medium" effort for
    # both calls (better answers over faster ones), and the Responses API so the
    # model's reasoning carries across tool rounds. Wired into the callers as
    # each one is reviewed (query_parser, llm_synthesizer, ENP Stage 4, WBA).
    "reasoning_effort": "medium",
    "use_responses_api": True,
    # Temperatures apply to non-reasoning models only (other providers, or
    # gpt-4.1-mini). For gpt-5.x with any effort other than "none", LangChain
    # drops them before the request (langchain_openai ChatOpenAI temperature check).
    "parser_temperature": 0.0,
    "synthesizer_temperature": 0.3,
    # Fail fast: quota errors (429 on a DAILY limit) are not transient, and
    # LangChain's default ~6 backoff retries burned ~70 s per call. The graph
    # has deterministic fallbacks for both calls.
    "max_retries": 2,
    # Seconds per attempt (C13). Without it the OpenAI client waits up to ~10 min,
    # which would stall the ENP thread or a /query on one hung request. Worst case
    # per call: (max_retries + 1) x timeout = 180 s, then the node's fallback runs.
    "timeout": 60,
})
# ─────────────────────────────────────────────────────────────────────────────

# Read by rse/search_coordinator.py (psycopg2 pool) and rse/conversation_memory.py.
DATABASE_URL: str = os.getenv("DATABASE_URL", "")

# ── Hybrid retrieval ──────────────────────────────────────────────────────────
# Keyword branch: candidates returned by postgres_keyword_search, FTS-rank ordered.
PG_KEYWORD_LIMIT = 200
# Semantic branch: FAISS hits fetched per query variant before pooling.
FAISS_TOP_K_PER_VARIANT = 30
# Multiplier applied to FAISS k when widen_scope sets full_faiss_scan (attempt 3).
FULL_SCAN_K_MULTIPLIER = 3
# Reciprocal Rank Fusion constant: score = Σ 1 / (RRF_K + rank).
RRF_K = 60
# Fused candidates kept after merge_and_rrf — this pool feeds the cross-encoder.
RRF_POOL_SIZE = 50
# widen_scope retries before giving up with no_results_found.
MAX_WIDEN_ATTEMPTS = 3

# ── Cross-encoder re-ranking ──────────────────────────────────────────────────
CROSS_ENCODER_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"
# final_score = CROSS_ENCODER_WEIGHT * sigmoid(ce_logit) + EFFORT_WEIGHT * effort_score
CROSS_ENCODER_WEIGHT = 0.8
EFFORT_WEIGHT = 0.2
# Characters of item text (title + snippet) fed to the cross-encoder per pair.
RERANK_DOC_CHAR_LIMIT = 512

# ── Quality evaluation ────────────────────────────────────────────────────────
# Top candidate cosine similarity below this → result_quality 'weak'.
MIN_TOP_SIMILARITY = 0.35
MIN_RESULT_COUNT = 2

# ── Neighborhood extension ────────────────────────────────────────────────────
NEIGHBOR_TOP_ITEMS = 3        # ranked items that get neighborhood context
NEIGHBOR_WINDOW_MINUTES = 5   # ± window for Chrome/YouTube temporal neighbors
NEIGHBORS_PER_ITEM = 5        # max neighbors fetched per top item

# ── Time anchor resolution ────────────────────────────────────────────────────
ANCHOR_CANDIDATE_LIMIT = 5    # keyword candidates inspected when resolving anchor

# ── Synthesis ─────────────────────────────────────────────────────────────────
SYNTHESIS_TOP_RESULTS = 10    # ranked results included in LLM Call 2 context
SNIPPET_CHAR_LIMIT = 500      # raw-text excerpt length per result (plain truncation)

# ── Conversation memory ───────────────────────────────────────────────────────
CONVERSATION_HISTORY_DAYS = 30  # cleanup deletes message_store rows older than this
