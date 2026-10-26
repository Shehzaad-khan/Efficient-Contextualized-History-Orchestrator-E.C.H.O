"""
Offline calibration of the three similarity cutoffs (tracker A18 part 2, A40, A42/A32). Read-only: it writes no database row.

    python scripts/calibrate_thresholds.py            # prints the measurements, writes docs/review/eval/thresholds_<date>.json

A18 part 2  seed-centroid bar for Stage 3. Rows with classified_by = 'structural' have a known correct group. Each row's
            stored vector is scored against the NEW seed centroids only (SEED_TEXTS, ignoring centroids.npz). Sweep score x
            lead; report the lowest score with zero wrong at the portable lead 0.08, and the best pair overall.
A40         search-quality gate (rse/config.py MIN_TOP_SIMILARITY). A list is "weak" when the best cosine is below it. The best
            cosine of every answerable question (the first search Luna chose, results_<date>.json) is compared with the best
            cosine of questions that have no answer in the data. A gate between the two groups flags no good query and passes no bad one.
A42 / A32   KNN bar (wba/group_manager.py KNN_ONLY_THRESHOLD). Three provisional user groups (labels written by the assistant, NOT
            by the owner). Every member is scored leave-one-out with the app's own _knn_score; every other item is a negative.
            A wrong auto-add is a negative at or above the bar, so the bar must exceed the highest negative.

Memory: loads the embedding model (about 1.2 GB). Stop the backend first (port 8000).
"""
import json
import socket
import sys
from datetime import date
from pathlib import Path
from typing import Any, Optional

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2  # noqa: E402

from enp.embedding_generator import generate_embedding, generate_query_embeddings  # noqa: E402
from enp.system_group_classifier import SEED_TEXTS, cosine  # noqa: E402
from rse.config import DATABASE_URL  # noqa: E402
from ste.faiss_manager import get_manager  # noqa: E402
from wba.group_manager import _knn_score  # noqa: E402

EVAL_DIR = Path(__file__).resolve().parent.parent / "docs" / "review" / "eval"
GROUP_NAMES = {1: "work", 2: "study", 3: "entertainment", 4: "personal", 5: "misc"}
SYNTHETIC_MAIL_TITLES = (
    "Interview schedule", "Offer of internship", "Distributed Systems - course", "Invoice INV-2026-0417",
    "flight to Hyderabad", "Kernel Quest 2026",
)

# Questions whose answer is NOT in the data (keyword style, as Luna writes them).
UNANSWERABLE_QUERIES = [
    "dentist appointment", "bitcoin wallet statement", "butter chicken recipe", "electricity bill September",
    "wedding invitation cousin", "visa application status", "home loan EMI schedule", "gym membership renewal",
    "cricket match scorecard", "hotel booking Goa", "passport renewal", "mutual fund statement",
]

# Provisional user groups: lists of (source or None, title substring). Labels are the assistant's, for the owner to check.
USER_GROUPS: dict[str, list[tuple[Optional[str], str]]] = {
    "Placement and jobs": [
        ("gmail", "Upcoming Placement Opportunity"), ("chrome", "Komprise"), ("youtube", "Komprise"),
        ("youtube", "power of unstructured"), ("chrome", "Infosys Limited Recruitment"), ("chrome", "Campus Placements"),
        ("gmail", "Internship from flipkart"), ("gmail", "Interview schedule"), ("gmail", "Offer of internship"),
    ],
    "Operating systems study": [
        ("chrome", "Operating System"), ("chrome", "OS Scheduling"), ("chrome", "Linux kernel"),
        ("gmail", "Distributed Systems - course"),
    ],
    "Data structures reading": [
        ("chrome", "Merkle tree"), ("chrome", "Radix tree"), ("chrome", "Treap"), ("chrome", "B-tree"),
        ("chrome", "Fenwick tree"),
    ],
}


def backend_is_running() -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(1.0)
        return probe.connect_ex(("127.0.0.1", 8000)) == 0


def load_items(cursor: Any) -> list[dict[str, Any]]:
    cursor.execute(
        "SELECT memory_id::text, source_type, title, system_group_id, classified_by FROM memory_items WHERE is_deleted = FALSE"
    )
    manager = get_manager()
    items = []
    for memory_id, source, title, group_id, classified_by in cursor.fetchall():
        vector = manager.get_vector(memory_id)
        if vector is not None:
            items.append({"id": memory_id, "source": source, "title": title or "", "group": GROUP_NAMES[group_id],
                          "classified_by": classified_by, "vector": np.asarray(vector, dtype=np.float32)})
    return items


# ── A18 part 2 ────────────────────────────────────────────────────────────────

def seed_centroids() -> dict[str, np.ndarray]:
    return {
        category: np.mean(np.asarray([generate_embedding(sentence) for sentence in sentences], dtype=np.float32), axis=0)
        for category, sentences in SEED_TEXTS.items()
    }


def score_against_seeds(items: list[dict[str, Any]], centroids: dict[str, np.ndarray]) -> list[dict[str, Any]]:
    rows = []
    for item in items:
        ranked = sorted(((cosine(item["vector"], vector), name) for name, vector in centroids.items()), reverse=True)
        rows.append({"id": item["id"], "source": item["source"], "title": item["title"], "label": item["group"],
                     "best": float(ranked[0][0]), "best_cat": ranked[0][1], "lead": float(ranked[0][0] - ranked[1][0])})
    return rows


def sweep(rows: list[dict[str, Any]], margin: float) -> dict[str, Any]:
    """Lowest score (step 0.01) with zero wrong at this lead, and its fires/correct counts."""
    for score in np.arange(0.20, 0.80, 0.01):
        fired = [r for r in rows if r["best"] > score and r["lead"] > margin]
        wrong = [r for r in fired if r["best_cat"] != r["label"]]
        if not wrong:
            return {"score": round(float(score), 2), "margin": margin, "fires": len(fired), "correct": len(fired), "of": len(rows)}
    return {"score": None, "margin": margin, "fires": 0, "correct": 0, "of": len(rows)}


def calibrate_seeds(items: list[dict[str, Any]]) -> dict[str, Any]:
    centroids = seed_centroids()
    structural = [i for i in items if i["classified_by"] == "structural"]
    rows_all = score_against_seeds(structural, centroids)
    rows_old = [r for r in rows_all if not any(t in r["title"] for t in SYNTHETIC_MAIL_TITLES)]
    result: dict[str, Any] = {"structural_rows": len(rows_all), "excluding_6_synthetic_mails": len(rows_old)}
    for name, rows in (("all_structural", rows_all), ("existing_rows_only", rows_old)):
        accuracy = sum(1 for r in rows if r["best_cat"] == r["label"]) / len(rows)
        best_pair = None
        for margin in np.arange(0.0, 0.16, 0.01):
            found = sweep(rows, round(float(margin), 2))
            if found["score"] is not None and (best_pair is None or found["fires"] > best_pair["fires"]):
                best_pair = found
        by_source = {}
        for source in ("gmail", "chrome", "youtube"):
            subset = [r for r in rows if r["source"] == source]
            if subset:
                by_source[source] = {"rows": len(subset),
                                     "best_centroid_correct": sum(1 for r in subset if r["best_cat"] == r["label"])}
        result[name] = {"best_centroid_accuracy_no_threshold": round(accuracy, 3), "by_source": by_source,
                        "at_portable_lead_0.08": sweep(rows, 0.08), "best_pair_overall": best_pair,
                        "score_range_best": [round(min(r["best"] for r in rows), 3), round(max(r["best"] for r in rows), 3)]}
    return result


# ── A40 ───────────────────────────────────────────────────────────────────────

def top_cosine(query: str) -> float:
    vector = generate_query_embeddings([query])[0]
    return float(get_manager().search_all(vector, 1)[0][1])


def calibrate_gate() -> dict[str, Any]:
    results_path = sorted(EVAL_DIR.glob("results_*.json"))[-1]
    data = json.loads(results_path.read_text(encoding="utf-8"))
    answerable = [top_cosine(q["first_search_args"]["query"]) for q in data["questions"] if q["first_search_args"]]
    unanswerable = [top_cosine(query) for query in UNANSWERABLE_QUERIES]
    low_good, high_bad = min(answerable), max(unanswerable)
    return {
        "answerable_queries": len(answerable), "answerable_min": round(low_good, 3),
        "answerable_p10": round(float(np.percentile(answerable, 10)), 3), "answerable_median": round(float(np.median(answerable)), 3),
        "unanswerable_queries": len(unanswerable), "unanswerable_max": round(high_bad, 3),
        "unanswerable_median": round(float(np.median(unanswerable)), 3),
        "gate_table": {f"{g:.2f}": {"good_flagged_weak": sum(1 for v in answerable if v < g),
                                    "bad_passed": sum(1 for v in unanswerable if v >= g)}
                       for g in (0.50, 0.55, 0.56, 0.57, 0.58, 0.60)},
        "separable": bool(high_bad < low_good),
        "midpoint": round((low_good + high_bad) / 2, 3),
        "good_queries_flagged_weak_at_0.55": sum(1 for v in answerable if v < 0.55),
        "bad_queries_passed_at_0.55": sum(1 for v in unanswerable if v >= 0.55),
    }


# ── A42 / A32 ─────────────────────────────────────────────────────────────────

def resolve_group(cursor: Any, selectors: list[tuple[Optional[str], str]]) -> set[str]:
    ids: set[str] = set()
    for source, title in selectors:
        sql, params = "SELECT memory_id::text FROM memory_items WHERE is_deleted = FALSE AND title ILIKE %s", [f"%{title}%"]
        if source:
            sql, params = sql + " AND source_type = %s", params + [source]
        cursor.execute(sql, params)
        ids.update(row[0] for row in cursor.fetchall())
    return ids


def calibrate_knn(cursor: Any, items: list[dict[str, Any]]) -> dict[str, Any]:
    vectors = {item["id"]: item["vector"] for item in items}
    groups = {name: resolve_group(cursor, selectors) & set(vectors) for name, selectors in USER_GROUPS.items()}
    positives: list[float] = []
    negatives: list[float] = []
    per_group = {}
    for name, members in groups.items():
        member_scores = [_knn_score(vectors[m], [vectors[o] for o in members if o != m]) for m in members]
        outsiders = [i for i in items if i["id"] not in members]
        outsider_scores = [_knn_score(i["vector"], [vectors[m] for m in members]) for i in outsiders]
        top_outsider = max(zip(outsider_scores, outsiders), key=lambda pair: pair[0])
        per_group[name] = {"members": len(members), "member_min": round(min(member_scores), 3),
                           "member_median": round(float(np.median(member_scores)), 3),
                           "highest_outsider": round(top_outsider[0], 3), "highest_outsider_title": top_outsider[1]["title"][:60]}
        positives += member_scores
        negatives += outsider_scores
    bar = float(np.floor(max(negatives) * 100) / 100 + 0.01)  # lowest 0.01 step above every negative
    frontier = {f"{b:.2f}": {"members_at_or_above": sum(1 for p in positives if p >= b),
                             "non_members_at_or_above": sum(1 for n in negatives if n >= b)}
                for b in (0.65, 0.70, 0.75, 0.78, 0.80, 0.82, 0.85)}
    return {"groups": per_group, "frontier": frontier, "highest_negative_overall": round(max(negatives), 3), "suggested_bar": round(bar, 2),
            "members_scored": len(positives), "members_at_or_above_bar": sum(1 for p in positives if p >= bar),
            "members_at_or_above_0.65": sum(1 for p in positives if p >= 0.65),
            "negatives_at_or_above_0.65": sum(1 for n in negatives if n >= 0.65), "negatives_scored": len(negatives)}


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    if backend_is_running():
        print("Port 8000 is in use: stop the backend first (memory).")
        return 2
    connection = psycopg2.connect(DATABASE_URL, connect_timeout=5)
    connection.set_session(readonly=True)
    with connection, connection.cursor() as cursor:
        items = load_items(cursor)
        print(f"{len(items)} items with a stored vector")
        report = {"items": len(items), "a18_part2_seed_bar": calibrate_seeds(items), "a40_search_gate": calibrate_gate(),
                  "a42_knn_bar": calibrate_knn(cursor, items)}
    out = EVAL_DIR / f"thresholds_{date.today().isoformat()}.json"
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
