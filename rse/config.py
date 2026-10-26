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
    # $0.40 / $1.60) and much stronger on reasoning benchmarks. gpt-5.6-luna →
    # gpt-6-luna on 2026-10-24: one model for every OpenAI call (chat and Decisions).
    # GOOGLE_API_KEY stays in .env — switch back by changing these strings.
    "provider": "openai",
    "parser_model": "gpt-6-luna",
    "synthesizer_model": "gpt-6-luna",
    # OpenAI Decisions API model for ENP Stage 4 (A21), provider "openai" only.
    # The Decisions endpoint accepts only "gpt-6-luna", so keep it at that value
    # even if the two chat models above change. Empty string = skip the Decisions
    # path (one-word text call).
    "decisions_model": "gpt-6-luna",
    # OpenAI-only extras (A25). Luna is a reasoning model: "medium" effort for
    # both calls (better answers over faster ones), and the Responses API so the
    # model's reasoning carries across tool rounds. Wired into the callers as
    # each one is reviewed (query_parser, llm_synthesizer, ENP Stage 4, WBA).
    "reasoning_effort": "medium",
    "use_responses_api": True,
    # Temperatures apply to non-reasoning models only (other providers, or
    # gpt-4.1-mini). llm_factory sends no temperature while reasoning_effort is set,
    # so keep it set for gpt-6-luna: LangChain's own temperature drop only matches
    # "gpt-5*" model names, not "gpt-6*".
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
# 0.55 is the design's START value for arctic-embed-xs (A40; was 0.35 on MiniLM). Unrelated text scores
# ~0.37-0.60 with arctic-xs, so 0.35 never fired. Measured 2026-10-26 (scripts/calibrate_thresholds.py): best cosine of
# the 38 answerable questions from 0.558 (median 0.734) vs 12 unanswerable ones up to 0.573. At 0.55 none of the 38 good
# queries is flagged weak and 2 of 12 bad ones pass; 0.58 would flag 1 good and pass 0 bad (one query: noise). Kept.
MIN_TOP_SIMILARITY = 0.55
MIN_RESULT_COUNT = 2

# ── Neighborhood extension ────────────────────────────────────────────────────
NEIGHBOR_TOP_ITEMS = 3        # ranked items that get neighborhood context
NEIGHBOR_WINDOW_MINUTES = 5   # ± window for Chrome/YouTube temporal neighbors
NEIGHBORS_PER_ITEM = 5        # max neighbors fetched per top item

# ── Time anchor resolution ────────────────────────────────────────────────────
ANCHOR_CANDIDATE_LIMIT = 5    # keyword candidates inspected when resolving anchor

# ── Synthesis ─────────────────────────────────────────────────────────────────
SYNTHESIS_TOP_RESULTS = 10    # ranked results included in LLM Call 2 context
# A24 (simplified 2026-10-26): no query-matched windows. Each result gets a plain leading excerpt of its
# text. The three best results get the long one (where the answer usually is); the rest a short one, so
# ten results stay near 10k tokens. gpt-6-luna has a 1.05M-token window, so this is a quality and cost
# choice, not a fit: past ~272K input tokens the request is billed at the double long-context rate.
SYNTHESIS_FULL_EXCERPT_RESULTS = 3     # best-ranked results that get the long excerpt
SYNTHESIS_EXCERPT_CHARS_TOP = 8000     # leading chars of text for those (~2k tokens each)
SYNTHESIS_EXCERPT_CHARS_REST = 2000    # leading chars for the other results (~500 tokens each)

# ── Agent loop (A26, built 2026-10-26) ────────────────────────────────────────
# Luna calls the search_memories tool; the cap is enforced in rse/graph_nodes.py, not only in the
# prompt. Calls 1..MAX_SEARCHES carry the tool, so the next call (call MAX_SEARCHES + 1 = 6) has none and
# must answer: usually 2 LLM calls per query, at most 6 (hard constraint 5).
MAX_SEARCHES = 5
# Parallel tool calls are allowed, but one round runs at most this many searches (latency bound).
MAX_SEARCHES_PER_ROUND = 3
# A new chat's first question forces a search so Luna cannot answer from nothing. Follow-ups stay on
# "auto" ("shorten that" needs no search). Set False if the provider rejects a forced tool choice.
FORCE_FIRST_SEARCH = True

# ── Conversation memory ───────────────────────────────────────────────────────
CONVERSATION_HISTORY_DAYS = 30  # cleanup deletes message_store rows older than this
# History the agent reads (one shared reader since A26; the parser and the synthesizer used to have 10 / 6
# messages cut to 500 chars each). The agent needs earlier questions to turn "and videos about that?" into a
# standalone search, and the later part of earlier answers for "when was that deadline?". Worst case
# 6 x 1,000 + 6 x 3,000 chars, about 6k tokens.
HISTORY_MESSAGES = 12          # the last 6 question-and-answer pairs
HISTORY_CHARS_QUESTION = 1000  # per user message
HISTORY_CHARS_ANSWER = 3000    # per Echo answer
