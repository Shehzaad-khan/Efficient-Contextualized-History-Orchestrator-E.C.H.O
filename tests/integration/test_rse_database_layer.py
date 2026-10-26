"""Integration tests for the RSE modules that talk to PostgreSQL through psycopg2 (search, anchor, neighbours,
chat history, pool) and for the reranker and LLM factory. The database is a scripted fake connection."""
from datetime import datetime

import numpy as np
import pytest

import psycopg2

from rse import conversation_memory as cm
from rse import llm_factory, neighborhood, reranker, search_coordinator as sc, time_anchor


class Leased:
    """What get_connection() hands out: the scripted connection, with close() recorded."""

    def __init__(self, conn):
        self.conn = conn

    def __getattr__(self, name):
        return getattr(self.conn, name)

    def __enter__(self):
        return self.conn.__enter__()

    def __exit__(self, *exc):
        return self.conn.__exit__(*exc)


@pytest.fixture
def use_db(monkeypatch, fake_pg):
    """use_db(handler) installs a scripted connection for every module that leases one."""
    def install(handler):
        conn = fake_pg(handler)
        for module in (sc, cm, neighborhood, time_anchor):
            monkeypatch.setattr(module, "get_connection", lambda c=conn: Leased(c), raising=False)
        import backend.retrieval as backend_retrieval

        monkeypatch.setattr(backend_retrieval, "get_connection", lambda c=conn: Leased(c))
        return conn

    return install


ROW = {"memory_id": "00000000-0000-0000-0000-000000000001", "source_type": "gmail", "title": "T", "created_at": datetime(2026, 10, 5)}


# ── Keyword search ────────────────────────────────────────────────────────────

def test_keyword_search_builds_filters_and_returns_normalised_rows(use_db):
    conn = use_db(lambda sql, params: [dict(ROW)])
    state = {"parsed_intent": {"query_clean": "interview", "sources": ["gmail"], "time_filter": "2026-10-01", "time_relation": "after"},
             "anchor_time": "2026-10-03T00:00:00"}
    results = sc.postgres_keyword_search(state)["postgres_results"]
    assert results[0]["memory_id"] == ROW["memory_id"]
    sql, params = conn.calls[0]
    assert "websearch_to_tsquery" in sql and "m.source_type IN" in sql and "m.created_at > %s::timestamp" in sql
    assert "m.is_deleted = FALSE" in sql and "m.preprocessed = TRUE" in sql      # deleted / unprocessed items are never searched
    assert params[0] == "interview" and params[-1] == 200 and "%interview%" in params


def test_keyword_search_without_a_keyword_returns_nothing_not_recent_items(use_db):
    """Shows (CLAUDE.md §2.3): no keyword means no keyword results, never a 'most recent' list (that would be recency boosting)."""
    conn = use_db(lambda sql, params: [dict(ROW)])
    assert sc.postgres_keyword_search({"parsed_intent": {"query_clean": "  "}}) == {"postgres_results": []}
    assert conn.calls == []


def test_widen_attempt_three_drops_the_sql_filters(use_db):
    conn = use_db(lambda sql, params: [])
    sc.postgres_keyword_search({"parsed_intent": {"query_clean": "x", "skip_postgres_filter": True, "sources": ["gmail"], "time_filter": "2026-10-01"}})
    assert "m.source_type IN" not in conn.calls[0][0] and "EXISTS (SELECT 1 FROM visits" not in conn.calls[0][0]


def test_a_database_error_gives_an_empty_result_instead_of_a_crash(use_db):
    use_db(lambda sql, params: psycopg2.OperationalError("down"))
    assert sc.postgres_keyword_search({"parsed_intent": {"query_clean": "x"}}) == {"postgres_results": []}
    assert sc.hydrate_memory_items(["a"]) == []
    assert sc.fetch_visit_times(["a"], datetime(2026, 10, 1)) == {}
    assert sc.fetch_recent_items(["gmail"]) == []


def test_hydrate_visits_and_recent_items(use_db):
    use_db(lambda sql, params:
           [("a", datetime(2026, 10, 2)), ("a", datetime(2026, 10, 4)), ("b", datetime(2026, 10, 3))] if "ORDER BY memory_id, visited_at" in sql
           else [dict(ROW)])
    assert sc.hydrate_memory_items(["a"])[0]["title"] == "T"
    assert sc.hydrate_memory_items([]) == []
    assert sc.fetch_visit_times(["a", "b"], datetime(2026, 10, 1)) == {"a": [datetime(2026, 10, 2), datetime(2026, 10, 4)], "b": [datetime(2026, 10, 3)]}
    assert sc.fetch_visit_times([], datetime(2026, 10, 1)) == {}
    assert sc.fetch_recent_items(["gmail", "tiktok"], limit=3)[0]["title"] == "T"


# ── Semantic search ───────────────────────────────────────────────────────────

def test_semantic_search_pools_the_best_cosine_per_item_across_variants(monkeypatch):
    class Manager:
        def search_all(self, vector, k):
            return {0: [("a", 0.6), ("b", 0.5)], 1: [("a", 0.9), ("c", 0.4)]}[int(vector[0])]

    monkeypatch.setattr("enp.embedding_generator.generate_query_embeddings", lambda variants: [np.array([i]) for i in range(len(variants))])
    monkeypatch.setattr("ste.faiss_manager.get_manager", lambda: Manager())
    result = sc.faiss_semantic_search({"parsed_intent": {"query_variants": ["one", "two", " "]}})["faiss_results"]
    assert result == [("a", 0.9), ("b", 0.5), ("c", 0.4)]


def test_semantic_search_falls_back_to_query_clean_and_survives_failures(monkeypatch):
    seen = []
    monkeypatch.setattr("enp.embedding_generator.generate_query_embeddings", lambda variants: seen.append(variants) or [np.array([0])])
    monkeypatch.setattr("ste.faiss_manager.get_manager", lambda: type("M", (), {"search_all": lambda self, v, k: seen.append(k) or []})())
    sc.faiss_semantic_search({"parsed_intent": {"query_clean": "interview", "full_faiss_scan": True}})
    assert seen[0] == ["interview"] and seen[1] == 30 * 3                      # full scan widens k
    assert sc.faiss_semantic_search({"parsed_intent": {}}) == {"faiss_results": []}
    monkeypatch.setattr("ste.faiss_manager.get_manager", lambda: (_ for _ in ()).throw(ValueError("index version mismatch")))
    assert sc.faiss_semantic_search({"parsed_intent": {"query_clean": "x"}}) == {"faiss_results": []}


# ── Time anchor ───────────────────────────────────────────────────────────────

def test_anchor_phrase_cleaning():
    assert time_anchor._clean_anchor_phrase("interview email") == ("interview", "gmail")
    assert time_anchor._clean_anchor_phrase("email video") == ("email video", None)       # two sources: no hint
    assert time_anchor._clean_anchor_phrase("emails") == ("emails", "gmail")             # only source words: keep the phrase
    assert time_anchor._clean_anchor_phrase("50% off sale page")[0] == "50% off sale"


def test_no_anchor_means_no_constraint():
    assert time_anchor.resolve_time_anchor({"parsed_intent": {}}) == {"anchor_time": None, "anchor_item": None}
    assert time_anchor.resolve_time_anchor({"parsed_intent": {"time_anchor_query": "x", "time_relation": "during"}})["anchor_time"] is None


def test_anchor_is_found_by_keyword_first(use_db):
    use_db(lambda sql, params: [{**ROW, "subject": "S", "thread_id": "t"}])
    result = time_anchor.resolve_time_anchor({"parsed_intent": {"time_anchor_query": "interview email", "time_relation": "after"}})
    assert result["anchor_time"] == "2026-10-05T00:00:00" and result["anchor_item"]["memory_id"] == ROW["memory_id"]


def test_anchor_falls_back_to_a_semantic_match_above_the_floor(use_db, monkeypatch):
    use_db(lambda sql, params: [] if "anchor_rank" in sql else [dict(ROW, memory_id="near"), dict(ROW, memory_id="far", source_type="chrome")])
    monkeypatch.setattr("enp.embedding_generator.generate_query_embedding", lambda text: np.array([1.0]))
    monkeypatch.setattr("ste.faiss_manager.get_manager",
                        lambda: type("M", (), {"search_all": lambda self, v, k: [("far", 0.9), ("near", 0.7), ("low", 0.1)]})())
    found = time_anchor._semantic_anchor_lookup("interview", "gmail")
    assert found["memory_id"] == "near" and found["anchor_similarity"] == 0.7        # the hinted source wins over a closer hit


def test_anchor_semantic_fallback_returns_none_when_nothing_is_close_or_things_break(use_db, monkeypatch):
    use_db(lambda sql, params: [])
    monkeypatch.setattr("enp.embedding_generator.generate_query_embedding", lambda text: np.array([1.0]))
    monkeypatch.setattr("ste.faiss_manager.get_manager", lambda: type("M", (), {"search_all": lambda self, v, k: [("x", 0.1)]})())
    assert time_anchor._semantic_anchor_lookup("t", None) is None
    monkeypatch.setattr("ste.faiss_manager.get_manager", lambda: type("M", (), {"search_all": lambda self, v, k: [("x", 0.9)]})())
    assert time_anchor._semantic_anchor_lookup("t", None) is None                    # close hit was deleted
    monkeypatch.setattr("enp.embedding_generator.generate_query_embedding", lambda text: (_ for _ in ()).throw(RuntimeError("no model")))
    assert time_anchor._semantic_anchor_lookup("t", None) is None


def test_unresolved_anchor_runs_the_search_unanchored(use_db, monkeypatch):
    use_db(lambda sql, params: [])
    monkeypatch.setattr(time_anchor, "_semantic_anchor_lookup", lambda terms, hint: None)
    assert time_anchor.resolve_time_anchor({"parsed_intent": {"time_anchor_query": "x", "time_relation": "before"}}) == {"anchor_time": None, "anchor_item": None}
    use_db(lambda sql, params: psycopg2.OperationalError("down"))
    assert time_anchor._keyword_anchor_lookup("x", None) is None


# ── Neighbourhood ─────────────────────────────────────────────────────────────

def test_mail_gets_thread_neighbours_and_pages_get_nearby_activity(use_db):
    def handler(sql, params):
        if "thread_id" in params:
            return [{"memory_id": "mail2", "source_type": "gmail", "title": "Re: Interview"}]
        return [{"memory_id": "p1", "priority": 0, "distance": 1}, {"memory_id": "p1", "priority": 1, "distance": 2}, {"memory_id": "p2", "priority": 1, "distance": 3}]

    use_db(handler)
    ranked = [
        {"memory_id": "mail1", "source_type": "gmail", "created_at": datetime(2026, 10, 5), "thread_id": "t1"},
        {"memory_id": "page1", "source_type": "chrome", "created_at": datetime(2026, 10, 5)},
        {"memory_id": "nothread", "source_type": "gmail", "created_at": datetime(2026, 10, 5)},
        {"memory_id": "nodate", "source_type": "chrome"},
        {"memory_id": "fourth", "source_type": "chrome", "created_at": datetime(2026, 10, 5)},
    ]
    result = neighborhood.extend_neighborhood({"ranked_results": ranked})["neighbor_items"]
    assert result["mail1"][0]["memory_id"] == "mail2"
    assert [n["memory_id"] for n in result["page1"]] == ["p1", "p2"] and "priority" not in result["page1"][0]
    assert "nothread" not in result and "fourth" not in result       # only the top 3 get neighbours
    assert neighborhood.extend_neighborhood({}) == {"neighbor_items": {}}


def test_neighbour_failures_are_skipped(use_db):
    conn = use_db(lambda sql, params: psycopg2.OperationalError("aborted"))
    ranked = [{"memory_id": "p1", "source_type": "chrome", "created_at": datetime(2026, 10, 5)}]
    assert neighborhood.extend_neighborhood({"ranked_results": ranked}) == {"neighbor_items": {}}
    assert conn.rollbacks >= 1


# ── Chat history ──────────────────────────────────────────────────────────────

def test_a_turn_is_saved_and_loaded_back(use_db):
    from langchain_core.messages import AIMessage, HumanMessage, message_to_dict

    stored = []

    def handler(sql, params):
        if sql.startswith("INSERT"):
            stored.extend([params[1].adapted, params[3].adapted])
            return 2
        return [(message,) for message in stored]

    use_db(handler)
    cm.save_turn("s1", "find my email", "Here it is")
    history = cm.load_conversation_history("s1")
    assert [type(m) for m in history] == [HumanMessage, AIMessage] and history[0].content == "find my email"
    assert stored[0] == message_to_dict(HumanMessage(content="find my email"))


def test_history_functions_never_raise(use_db):
    use_db(lambda sql, params: psycopg2.OperationalError("down"))
    assert cm.load_conversation_history("s") == []
    cm.save_turn("s", "q", "a")
    assert cm.cleanup_old_sessions() == -1
    use_db(lambda sql, params: 7)
    assert cm.cleanup_old_sessions() == 7


# ── Pool ──────────────────────────────────────────────────────────────────────

class RawConn:
    def __init__(self, closed=False, alive=True):
        self.closed, self.alive, self.rolled_back = closed, alive, False

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql):
        if not self.alive:
            raise psycopg2.OperationalError("dead")

    def rollback(self):
        self.rolled_back = True


class FakePool:
    def __init__(self, conns):
        self.conns, self.returned = list(conns), []

    def getconn(self):
        return self.conns.pop(0)

    def putconn(self, conn, close=False):
        self.returned.append((conn, close))


def test_pool_hands_out_live_connections_and_takes_them_back(monkeypatch):
    live = RawConn()
    pool = FakePool([live])
    monkeypatch.setattr(sc, "_get_pool", lambda: pool)
    leased = sc.get_connection()
    leased.close()
    leased.close()                                                  # closing twice is harmless
    assert pool.returned == [(live, False)]


def test_pool_discards_dead_and_stale_connections(monkeypatch):
    dead, closed, healthy = RawConn(alive=False), RawConn(closed=True), RawConn()
    pool = FakePool([closed, dead, healthy])
    monkeypatch.setattr(sc, "_get_pool", lambda: pool)
    sc._returned_at[id(dead)] = 0.0                                 # idle for ages: pinged, found dead
    leased = sc.get_connection()
    assert (closed, True) in pool.returned and (dead, True) in pool.returned
    leased.close()
    broken = RawConn(closed=True)
    sc._PooledConnection(pool, broken).close()
    assert pool.returned[-1] == (broken, True)


def test_pool_gives_up_when_nothing_is_alive(monkeypatch):
    pool = FakePool([RawConn(closed=True) for _ in range(sc._POOL_MAX + 1)])
    monkeypatch.setattr(sc, "_get_pool", lambda: pool)
    with pytest.raises(psycopg2.OperationalError):
        sc.get_connection()


def test_pool_needs_a_database_url(monkeypatch):
    monkeypatch.setattr(sc, "_POOL", None)
    monkeypatch.setattr(sc, "DATABASE_URL", "")
    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        sc._get_pool()


def test_pool_is_created_once(monkeypatch):
    made = []
    monkeypatch.setattr(sc, "_POOL", None)
    monkeypatch.setattr(sc, "DATABASE_URL", "postgresql://u:p@h/db")
    monkeypatch.setattr(sc.psycopg2.pool, "ThreadedConnectionPool", lambda *a, **k: made.append(k) or object())
    first = sc._get_pool()
    assert sc._get_pool() is first and len(made) == 1 and made[0]["options"] == "-c timezone=UTC"


# ── Reranker with a fake cross-encoder ────────────────────────────────────────

class FakeCrossEncoder:
    def __init__(self, logits=None, error=None):
        self.logits, self.error = logits, error

    def predict(self, pairs):
        if self.error:
            raise self.error
        return self.logits if self.logits is not None else [float(len(text)) for _, text in pairs]


def candidates():
    return [{"memory_id": "a", "title": "Interview", "raw_snippet": "x" * 600, "subject": "Interview schedule", "source_type": "chrome"},
            {"memory_id": "b", "title": "Cooking", "raw_snippet": "pasta", "source_type": "chrome", "dwell_time_seconds": 600}]


def test_cross_encoder_decides_most_of_the_order_and_effort_the_rest(monkeypatch):
    monkeypatch.setattr(reranker, "get_cross_encoder", lambda: FakeCrossEncoder([5.0, -5.0]))
    ranked = reranker.rerank_cross_encoder({"merged_candidates": candidates(), "user_query": "interview"})["ranked_results"]
    assert [r["memory_id"] for r in ranked] == ["a", "b"]
    assert ranked[0]["ce_score"] > 0.99 and 0 <= ranked[1]["final_score"] <= 1
    assert ranked[1]["effort_score"] > ranked[0]["effort_score"]


def test_effort_can_lift_an_item_but_never_override_a_clear_relevance_gap(monkeypatch):
    items = candidates()
    monkeypatch.setattr(reranker, "get_cross_encoder", lambda: FakeCrossEncoder([0.0, 0.0]))
    ranked = reranker.rerank_cross_encoder({"merged_candidates": items, "user_query": "q"})["ranked_results"]
    assert ranked[0]["memory_id"] == "b"                           # equal relevance: the page he read for 10 minutes wins


def test_reranking_falls_back_to_the_fusion_order_when_the_model_breaks(monkeypatch):
    monkeypatch.setattr(reranker, "get_cross_encoder", lambda: FakeCrossEncoder(error=RuntimeError("no model")))
    ranked = reranker.rerank_cross_encoder({"merged_candidates": candidates(), "user_query": "q"})["ranked_results"]
    assert [r["ce_score"] for r in ranked] == [None, None] and ranked[0]["memory_id"] == "a"
    monkeypatch.setattr(reranker, "get_cross_encoder", lambda: FakeCrossEncoder([1.0]))       # wrong number of scores
    assert reranker.rerank_cross_encoder({"merged_candidates": candidates(), "user_query": "q"})["ranked_results"][0]["ce_score"] is None
    assert reranker.rerank_cross_encoder({}) == {"ranked_results": []}


def test_document_text_adds_the_subject_only_when_different_and_is_bounded():
    text = reranker._document_text({"title": "Same", "subject": "same", "raw_snippet": "body"})
    assert text == "Same body"
    assert len(reranker._document_text({"title": "T", "raw_snippet": "x" * 5000})) == 512
    assert reranker._document_text({}) == "(untitled item)"


def test_cross_encoder_is_loaded_once(monkeypatch):
    monkeypatch.setattr(reranker, "_cross_encoder", None)
    loads = []

    class FakeST:
        class CrossEncoder:
            def __init__(self, name):
                loads.append(name)

    import sys

    monkeypatch.setitem(sys.modules, "sentence_transformers", FakeST)
    first = reranker.get_cross_encoder()
    assert reranker.get_cross_encoder() is first and loads == [reranker.CROSS_ENCODER_MODEL]


# ── LLM factory ───────────────────────────────────────────────────────────────

def test_llm_clients_are_built_once_per_model_and_temperature(monkeypatch):
    built = []
    monkeypatch.setattr("langchain.chat_models.init_chat_model", lambda **kw: built.append(kw) or object())
    llm_factory.build_chat_model.cache_clear()
    first = llm_factory.build_chat_model("synthesizer_model", "synthesizer_temperature")
    assert llm_factory.build_chat_model("synthesizer_model", "synthesizer_temperature") is first and len(built) == 1
    assert built[0]["model_provider"] == llm_factory.LLM_CONFIG["provider"] and "model" in built[0]
    with pytest.raises(KeyError):
        llm_factory.build_chat_model("no_such_model", "synthesizer_temperature")
    llm_factory.build_chat_model.cache_clear()


@pytest.mark.parametrize("config, expects", [
    ({"provider": "openai", "reasoning_effort": "low", "use_responses_api": True}, {"use_responses_api": True, "reasoning": {"effort": "low"}}),
    ({"provider": "openai", "reasoning_effort": "low", "use_responses_api": False}, {"use_responses_api": False, "reasoning_effort": "low"}),
    ({"provider": "anthropic", "reasoning_effort": "low", "synthesizer_temperature": 0.3}, {"temperature": 0.3}),
])
def test_provider_specific_options(monkeypatch, config, expects):
    built = []
    monkeypatch.setattr("langchain.chat_models.init_chat_model", lambda **kw: built.append(kw) or object())
    monkeypatch.setattr(llm_factory, "LLM_CONFIG", {"synthesizer_model": "m", **config})
    llm_factory.build_chat_model.cache_clear()
    llm_factory.build_chat_model("synthesizer_model", "synthesizer_temperature")
    assert expects.items() <= built[0].items()
    assert "temperature" not in built[0] or config["provider"] != "openai"           # reasoning models take no temperature
    llm_factory.build_chat_model.cache_clear()
