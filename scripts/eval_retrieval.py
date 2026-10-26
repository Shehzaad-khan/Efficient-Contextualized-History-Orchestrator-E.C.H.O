"""
Retrieval evaluation for the paper and the deck (Section "Evaluation Protocol", slide 16).

    python scripts/eval_retrieval.py labels                 # show which items each question's gold selectors match
    python scripts/eval_retrieval.py run [--only q01,q02]   # run everything, write docs/review/eval/results_<date>.json

What it measures (read-only on the memory data; it only writes its own chat sessions, removed at the end):

  Configs 1-4 replay the arguments Luna chose on its FIRST search_memories call, once, with no LLM in the stages:
      1 keyword only      Postgres full-text branch alone
      2 semantic only     FAISS branch alone
      3 fusion            both branches through merge_and_rrf (RRF k=60), no cross-encoder
      4 full pipeline     run_search: quality check, widening, cross-encoder, neighbourhood
  Config 5 is the whole agent loop (run_query): Luna's rounds, the cap, the answer.

  Recall@k = gold items in the top k / min(|gold|, k).   MRR = mean of 1 / rank of the first gold item (0 beyond k).
  Per turn: LLM calls, searches, wall time, tokens, request sizes. Privacy probe: for every shown item longer than
  the top-result excerpt cap, the text just past the cap must be absent from every request.

Memory: this process loads the embedding model, the cross-encoder and torch (about 2.5 GB). The laptop has 7.3 GB
visible, so the script refuses to start while the backend (port 8000) is running.
Scripts keep print(); no logging setup (owner's rule for scripts/).
"""
import argparse
import json
import math
import socket
import statistics
import sys
import time
import uuid
from contextvars import ContextVar
from datetime import date
from pathlib import Path
from typing import Any, Optional, TypedDict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2  # noqa: E402
from langchain_core.callbacks import BaseCallbackHandler  # noqa: E402
from langchain_core.tracers.context import register_configure_hook  # noqa: E402

from rse.config import DATABASE_URL, SYNTHESIS_EXCERPT_CHARS_TOP  # noqa: E402

EVAL_DIR = Path(__file__).resolve().parent.parent / "docs" / "review" / "eval"
BACKEND_PORT = 8000
TOP_K = 10
CAP_ROUNDS_CALLS = 6  # MAX_SEARCHES (5) + the answer call: reaching it means the five-search cap was used up

# ASSUMPTION, not measured: price per 1M tokens, copied from the gpt-5.6-luna comment in rse/config.py
# (2026-10-18). gpt-6-luna's own price was not checked. If wrong, only the cost lines change.
INPUT_USD_PER_MILLION = 0.20
OUTPUT_USD_PER_MILLION = 1.20

CONFIG_NAMES: tuple[str, ...] = ("keyword", "semantic", "fusion", "full_pipeline", "agent_loop")


class GoldSelector(TypedDict, total=False):
    source: str
    title: str
    date: str


class Question(TypedDict, total=False):
    id: str
    kind: str
    turns: list[str]
    gold: list[GoldSelector]
    answer_any: list[str]


# ── Pure metric functions ────────────────────────────────────────────────────

def recall_at_k(ranked_ids: list[str], gold_ids: set[str], k: int) -> float:
    """Gold items found in the top k, divided by min(|gold|, k) (so a 20-item gold set can still score 1.0)."""
    if not gold_ids:
        raise ValueError("recall_at_k needs at least one gold item")
    found = len(gold_ids.intersection(ranked_ids[:k]))
    return found / min(len(gold_ids), k)


def reciprocal_rank(ranked_ids: list[str], gold_ids: set[str], k: int) -> float:
    """1 / rank of the first gold item within the top k; 0.0 when none is there."""
    for rank, memory_id in enumerate(ranked_ids[:k], start=1):
        if memory_id in gold_ids:
            return 1.0 / rank
    return 0.0


def percentile_nearest_rank(values: list[float], percent: float) -> float:
    """Nearest-rank percentile: the value at index ceil(p/100 * n) of the sorted list (1-based)."""
    if not values:
        raise ValueError("percentile of an empty list")
    ordered = sorted(values)
    index = max(1, math.ceil(percent / 100.0 * len(ordered)))
    return ordered[index - 1]


def cost_usd(input_tokens: int, output_tokens: int) -> float:
    return input_tokens * INPUT_USD_PER_MILLION / 1e6 + output_tokens * OUTPUT_USD_PER_MILLION / 1e6


# ── Recording the LLM requests without touching rse/ ─────────────────────────

def _content_chars(message: Any) -> int:
    content = message.content
    return len(content) if isinstance(content, str) else len(json.dumps(content, default=str))


def _content_text(message: Any) -> str:
    content = message.content
    return content if isinstance(content, str) else json.dumps(content, default=str)


class LlmRecorder(BaseCallbackHandler):
    """Collects, for every chat-model call: request size, tokens, and the tool-call arguments Luna sent back."""

    raise_error = True  # a recorder bug must stop the run, not be swallowed by LangChain

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.request_texts: list[str] = []

    def reset(self) -> None:
        self.calls.clear()
        self.request_texts.clear()

    def on_chat_model_start(self, serialized: Any, messages: Any, **kwargs: Any) -> None:
        flat = [message for batch in messages for message in batch]
        sizes = [_content_chars(message) for message in flat]
        self.calls.append({
            "request_chars": sum(sizes),
            "largest_message_chars": max(sizes) if sizes else 0,
            "input_tokens": 0, "output_tokens": 0, "tool_args": [],
        })
        self.request_texts.append("\n".join(_content_text(message) for message in flat))

    def on_llm_end(self, response: Any, **kwargs: Any) -> None:
        message = response.generations[0][0].message
        usage = getattr(message, "usage_metadata", None) or {}
        call = self.calls[-1]
        call["input_tokens"] = int(usage.get("input_tokens", 0))
        call["output_tokens"] = int(usage.get("output_tokens", 0))
        call["tool_args"] = [tool_call.get("args") for tool_call in (getattr(message, "tool_calls", None) or [])]


_RECORDER: ContextVar[Optional[LlmRecorder]] = ContextVar("echo_eval_recorder", default=None)
register_configure_hook(_RECORDER, True)  # every LangChain call in this context also reports to the recorder


# ── Database helpers ─────────────────────────────────────────────────────────

def connect_readonly() -> Any:
    connection = psycopg2.connect(DATABASE_URL, connect_timeout=5)
    connection.set_session(readonly=True)
    return connection


def resolve_selector(cursor: Any, selector: GoldSelector) -> list[tuple[str, str, str]]:
    """Items matching one selector: (memory_id, source, title). Raises LookupError when nothing matches."""
    clauses = ["is_deleted = FALSE"]
    params: list[Any] = []
    if "source" in selector:
        clauses.append("source_type = %s")
        params.append(selector["source"])
    if "title" in selector:
        clauses.append("title ILIKE %s")
        params.append(f"%{selector['title']}%")
    if "date" in selector:
        clauses.append("to_char(created_at AT TIME ZONE 'Asia/Kolkata', 'MM-DD') = %s")
        params.append(selector["date"])
    cursor.execute(f"SELECT memory_id::text, source_type, title FROM memory_items WHERE {' AND '.join(clauses)}", params)
    rows = cursor.fetchall()
    if not rows:
        raise LookupError(f"gold selector matches no item: {selector}")
    return rows


def resolve_gold(cursor: Any, question: Question) -> list[tuple[str, str, str]]:
    seen: dict[str, tuple[str, str, str]] = {}
    for selector in question["gold"]:
        for row in resolve_selector(cursor, selector):
            seen[row[0]] = row
    return list(seen.values())


def item_counts(cursor: Any) -> dict[str, int]:
    cursor.execute("SELECT source_type, COUNT(*) FROM memory_items WHERE is_deleted = FALSE GROUP BY 1 ORDER BY 1")
    return {source: int(count) for source, count in cursor.fetchall()}


def full_text_lengths(cursor: Any, memory_ids: list[str]) -> dict[str, tuple[str, int]]:
    """memory_id -> (text just past the top-result cap, full length). Used only for the privacy probe."""
    if not memory_ids:
        return {}
    cap = SYNTHESIS_EXCERPT_CHARS_TOP
    cursor.execute(
        "SELECT memory_id::text, SUBSTRING(COALESCE(raw_text, '') FROM %s FOR 40), LENGTH(COALESCE(raw_text, '')) "
        "FROM memory_items WHERE memory_id::text = ANY(%s)",
        (cap + 1, memory_ids),
    )
    return {row[0]: (row[1], int(row[2])) for row in cursor.fetchall()}


# ── Stage replay (configs 1-4) ───────────────────────────────────────────────

def replay_stages(first_search_args: dict[str, Any], turn_text: str) -> dict[str, list[str]]:
    """Run the four fixed-pipeline configurations on one set of search arguments; returns ranked memory_ids each."""
    from rse.graph_nodes import (
        node_faiss_semantic_search, node_merge_and_rrf, node_postgres_keyword_search, node_resolve_time_anchor,
    )
    from rse.query_parser import intent_from_tool_args
    from rse.search_tool import run_search
    from rse.state import EchoState

    intent = intent_from_tool_args(first_search_args, turn_text)
    state: EchoState = {
        "user_query": turn_text, "parsed_intent": intent, "anchor_time": None, "anchor_item": None,
        "postgres_results": [], "faiss_results": [], "merged_candidates": [], "ranked_results": [],
        "neighbor_items": {}, "attachment_content": None, "attachment_item_id": None,
        "result_quality": "empty", "attempt_count": 0,
    }
    state.update(node_resolve_time_anchor(state))
    state.update(node_postgres_keyword_search(state))
    state.update(node_faiss_semantic_search(state))

    def merged_ids(postgres: list[Any], faiss: list[Any]) -> list[str]:
        branch_state: EchoState = {**state, "postgres_results": postgres, "faiss_results": faiss}
        return [str(item["memory_id"]) for item in node_merge_and_rrf(branch_state)["merged_candidates"]]

    outcome = run_search(intent)
    return {
        "keyword": merged_ids(state["postgres_results"], []),
        "semantic": merged_ids([], state["faiss_results"]),
        "fusion": merged_ids(state["postgres_results"], state["faiss_results"]),
        "full_pipeline": [str(item["memory_id"]) for item in outcome.items],
    }


# ── One agent turn (config 5) ────────────────────────────────────────────────

def run_turn(text: str, session_id: str, recorder: LlmRecorder) -> dict[str, Any]:
    from rse.retrieval_engine import run_query

    recorder.reset()
    started = time.perf_counter()
    result = run_query(text, session_id=session_id)
    seconds = time.perf_counter() - started
    calls = list(recorder.calls)
    first_search = next((args[0] for call in calls for args in [call["tool_args"]] if args), None)
    return {
        "text": text,
        "seconds": seconds,
        "llm_calls": int(result.get("llm_calls", 0)),
        "searches": int(result.get("search_count", 0)),
        "widened": int(result.get("attempt_count", 0)),
        "input_tokens": sum(call["input_tokens"] for call in calls),
        "output_tokens": sum(call["output_tokens"] for call in calls),
        "request_chars": [call["request_chars"] for call in calls],
        "largest_message_chars": max((call["largest_message_chars"] for call in calls), default=0),
        "first_search_args": first_search,
        "found_ids": [str(item["memory_id"]) for item in (result.get("found_items") or [])],
        "answer": result.get("final_answer", ""),
        "no_results": bool(result.get("no_results")),
        "request_texts": list(recorder.request_texts),
    }


def privacy_violations(turn: dict[str, Any], past_cap: dict[str, tuple[str, int]]) -> int:
    """Shown items whose text past the excerpt cap appears in some request (must be 0)."""
    violations = 0
    for memory_id in turn["found_ids"]:
        probe = past_cap.get(memory_id, ("", 0))[0].strip()
        if len(probe) >= 20 and any(probe in request for request in turn["request_texts"]):
            violations += 1
    return violations


def answer_matches(answer: str, expected_any: list[str]) -> bool:
    lowered = answer.lower()
    return any(expected.lower() in lowered for expected in expected_any)


# ── Orchestration ────────────────────────────────────────────────────────────

def load_questions(only: Optional[set[str]]) -> list[Question]:
    data = json.loads((EVAL_DIR / "questions.json").read_text(encoding="utf-8"))
    questions: list[Question] = data["questions"]
    if only:
        questions = [question for question in questions if question["id"] in only]
        missing = only - {question["id"] for question in questions}
        if missing:
            raise ValueError(f"unknown question ids: {sorted(missing)}")
    return questions


def backend_is_running() -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(1.0)
        return probe.connect_ex(("127.0.0.1", BACKEND_PORT)) == 0


def warm_up_models() -> None:
    """Load the embedding model, the FAISS index and the cross-encoder on the main thread (a first load inside the
    parallel search threads fails with a huggingface_hub import error when run from the command line)."""
    from enp.embedding_generator import generate_query_embeddings
    from rse.reranker import get_cross_encoder
    from ste.faiss_manager import get_manager

    generate_query_embeddings(["warm up"])
    get_manager()
    get_cross_encoder()


def command_labels(arguments: argparse.Namespace) -> int:
    questions = load_questions(None)
    with connect_readonly() as connection, connection.cursor() as cursor:
        print(f"{len(questions)} questions; items: {item_counts(cursor)}")
        for question in questions:
            gold = resolve_gold(cursor, question)
            print(f"\n{question['id']} [{question['kind']}] {' -> '.join(question['turns'])}")
            for memory_id, source, title in gold:
                print(f"    gold: {source:8s} {(title or '(no title)')[:80]}")
    return 0


def summarise(per_question: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {"retrieval": {}, "by_kind": {}}
    for name in CONFIG_NAMES:
        scored = [q for q in per_question if name in q["metrics"]]
        summary["retrieval"][name] = {
            "questions": len(scored),
            "recall_at_10": statistics.fmean(q["metrics"][name]["recall10"] for q in scored) if scored else None,
            "recall_at_5": statistics.fmean(q["metrics"][name]["recall5"] for q in scored) if scored else None,
            "mrr": statistics.fmean(q["metrics"][name]["rr"] for q in scored) if scored else None,
            "gold_found_in_top10": sum(q["metrics"][name]["found10"] for q in scored),
        }
    for kind in sorted({q["kind"] for q in per_question}):
        in_kind = [q for q in per_question if q["kind"] == kind and "agent_loop" in q["metrics"]]
        summary["by_kind"][kind] = {
            "questions": len(in_kind),
            "agent_recall_at_10": statistics.fmean(q["metrics"]["agent_loop"]["recall10"] for q in in_kind) if in_kind else None,
        }
    turns = [turn for q in per_question for turn in q["turns"]]
    seconds = [turn["seconds"] for turn in turns]
    calls = [turn["llm_calls"] for turn in turns]
    costs = [cost_usd(turn["input_tokens"], turn["output_tokens"]) for turn in turns]
    summary["cost_and_speed"] = {
        "turns": len(turns),
        "mean_llm_calls": statistics.fmean(calls),
        "turns_at_cap": sum(1 for count in calls if count >= CAP_ROUNDS_CALLS),
        "median_seconds": statistics.median(seconds),
        "p95_seconds": percentile_nearest_rank(seconds, 95),
        "mean_input_tokens": statistics.fmean(turn["input_tokens"] for turn in turns),
        "mean_output_tokens": statistics.fmean(turn["output_tokens"] for turn in turns),
        "mean_cost_usd_ASSUMED_PRICE": statistics.fmean(costs),
    }
    summary["privacy"] = {
        "largest_request_chars": max(max(turn["request_chars"], default=0) for turn in turns),
        "largest_single_message_chars": max(turn["largest_message_chars"] for turn in turns),
        "past_cap_text_found_in_a_request": sum(turn["privacy_violations"] for turn in turns),
    }
    answered = [q for q in per_question if "answer_ok" in q]
    summary["answer_check"] = {"checked": len(answered), "correct": sum(1 for q in answered if q["answer_ok"])}
    return summary


def command_run(arguments: argparse.Namespace) -> int:
    if backend_is_running():
        print(f"Port {BACKEND_PORT} is in use: stop the backend first (this script loads its own models; "
              "running both exhausted the laptop's memory and stopped PostgreSQL on 2026-10-26).")
        return 2
    only = set(arguments.only.split(",")) if arguments.only else None
    questions = load_questions(only)

    warm_up_models()
    recorder = LlmRecorder()
    _RECORDER.set(recorder)

    per_question: list[dict[str, Any]] = []
    session_ids: list[str] = []
    with connect_readonly() as connection, connection.cursor() as cursor:
        counts = item_counts(cursor)
        for number, question in enumerate(questions, start=1):
            gold = {row[0] for row in resolve_gold(cursor, question)}
            session_id = str(uuid.uuid4())
            session_ids.append(session_id)
            turns = [run_turn(text, session_id, recorder) for text in question["turns"]]
            final = turns[-1]
            past_cap = full_text_lengths(cursor, list({i for turn in turns for i in turn["found_ids"]}))
            for turn in turns:
                turn["privacy_violations"] = privacy_violations(turn, past_cap)

            metrics: dict[str, dict[str, float]] = {}

            def score(name: str, ranked: list[str]) -> None:
                metrics[name] = {
                    "recall10": recall_at_k(ranked, gold, 10), "recall5": recall_at_k(ranked, gold, 5),
                    "rr": reciprocal_rank(ranked, gold, TOP_K),
                    "found10": len(gold.intersection(ranked[:10])),
                }
            score("agent_loop", final["found_ids"])
            if final["first_search_args"]:
                for name, ranked in replay_stages(final["first_search_args"], final["text"]).items():
                    score(name, ranked)

            record: dict[str, Any] = {
                "id": question["id"], "kind": question["kind"], "gold_count": len(gold), "metrics": metrics,
                "first_search_args": final["first_search_args"],
                "turns": [{k: v for k, v in turn.items() if k != "request_texts"} for turn in turns],
            }
            if "answer_any" in question:
                record["answer_ok"] = answer_matches(final["answer"], question["answer_any"])
            per_question.append(record)
            print(f"[{number}/{len(questions)}] {question['id']} {question['kind']:10s} "
                  f"agent R@10={metrics['agent_loop']['recall10']:.2f} calls={final['llm_calls']} "
                  f"searches={final['searches']} {final['seconds']:.1f}s"
                  + (f" answer_ok={record['answer_ok']}" if 'answer_ok' in record else ""))

    summary = summarise(per_question)
    out_path = EVAL_DIR / f"results_{date.today().isoformat()}.json"
    out_path.write_text(json.dumps({"item_counts": counts, "summary": summary, "questions": per_question},
                                   indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"\nWrote {out_path}")
    print(f"Chat sessions created by this run (remove with: eval_retrieval.py cleanup {out_path.name}): {len(session_ids)}")
    (EVAL_DIR / "last_run_sessions.json").write_text(json.dumps(session_ids), encoding="utf-8")
    return 0


def command_cleanup(arguments: argparse.Namespace) -> int:
    """Delete the message_store rows of the chat sessions the last run created (and nothing else)."""
    session_ids: list[str] = json.loads((EVAL_DIR / "last_run_sessions.json").read_text(encoding="utf-8"))
    connection = psycopg2.connect(DATABASE_URL, connect_timeout=5)
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM message_store WHERE session_id = ANY(%s)", (session_ids,))
            found = cursor.fetchone()[0]
            print(f"{found} message_store rows belong to {len(session_ids)} eval sessions")
            if arguments.apply:
                cursor.execute("DELETE FROM message_store WHERE session_id = ANY(%s)", (session_ids,))
                print(f"deleted {cursor.rowcount} rows")
            else:
                print("dry run; pass --apply to delete")
    finally:
        connection.close()
    return 0


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")  # titles carry emoji; the Windows console default is cp1252
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("labels").set_defaults(handler=command_labels)
    run = commands.add_parser("run")
    run.add_argument("--only", help="comma-separated question ids, e.g. q01,q02")
    run.set_defaults(handler=command_run)
    cleanup = commands.add_parser("cleanup")
    cleanup.add_argument("--apply", action="store_true")
    cleanup.set_defaults(handler=command_cleanup)
    arguments = parser.parse_args()
    return arguments.handler(arguments)


if __name__ == "__main__":
    sys.exit(main())
