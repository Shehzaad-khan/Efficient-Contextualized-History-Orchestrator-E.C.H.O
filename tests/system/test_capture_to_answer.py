"""System tests: one whole user journey through the running app, from the browser extension to a cited answer.

    capture (HTTP)  ->  enrichment worker  ->  FAISS index  ->  ask a question (HTTP)  ->  agent loop  ->  answer

Real: the FastAPI routes, the Chrome privacy gates, the storage SQL (in-memory SQLite), the ENP worker code
(cleaning, keywords, embedding window, classifier), a real FAISS index, RRF fusion, the quality check,
re-ranking, the agent loop and the citation checker.
Fake (only what lives outside the machine): PostgreSQL full-text search and psycopg2 (replaced by SQLite
look-ups), Redis, the embedding model (word hashing), the cross-encoder and the LLM (a scripted reply).
"""
from datetime import datetime

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from backend.items import router as items_router
from backend.retrieval import router as retrieval_router
from enp import enrichment_pipeline as ep
from ingestion.chrome import chrome_connector
from ingestion.chrome.chrome_connector import router as chrome_router
from rse import agent_nodes, fusion, reranker, retrieval_engine, search_coordinator, search_tool
from ste import faiss_manager as fm
from ste import postgresql_manager as db

pytest.importorskip("faiss")


class ScriptedLuna:
    """The agent LLM: asks for the searches it is told to, then answers; records every prompt it receives."""

    def __init__(self, searches, answer):
        self.searches, self.answer, self.calls = list(searches), answer, []

    def bind_tools(self, tools, **options):
        outer = self

        class Bound:
            def invoke(self, messages):
                return outer._next(messages, tools_bound=True)

        return Bound()

    def invoke(self, messages):
        return self._next(messages, tools_bound=False)

    def _next(self, messages, tools_bound):
        self.calls.append({"tools_bound": tools_bound, "messages": list(messages)})
        if self.searches and tools_bound:
            query = self.searches.pop(0)
            return AIMessage(content="", tool_calls=[{"name": "search_memories", "args": {"query": query}, "id": f"c{len(self.calls)}"}])
        return AIMessage(content=self.answer(messages) if callable(self.answer) else self.answer)


def pg_view_of_sqlite(sql, params):
    """Answers the ENP worker's psycopg2 queries from the SQLite database."""
    if "UPDATE memory_items" in sql and "preprocessed = TRUE" in sql:
        db.execute("UPDATE memory_items SET system_group_id = :g, classified_by = :c, preprocessed = 1 WHERE memory_id = :m",
                   {"g": params[0], "c": params[1], "m": params[-1]})
        return 1
    if "FROM memory_items WHERE preprocessed = FALSE" in sql:
        rows = db.fetchall("SELECT memory_id, source_type, source_id, title, raw_text, created_at FROM memory_items "
                           "WHERE preprocessed = 0 AND is_deleted = 0 ORDER BY first_ingested_at")
        return [(r["memory_id"], r["source_type"], r["source_id"], r["title"], r["raw_text"], datetime(2026, 10, 5)) for r in rows]
    if "FROM chrome_metadata" in sql and not sql.startswith("UPDATE"):
        rows = db.fetchall("SELECT url, canonical_url, domain FROM chrome_metadata WHERE memory_id = :m", {"m": params[0]})
        return [(r["url"], r["canonical_url"], r["domain"]) for r in rows]
    return 1 if sql.startswith("UPDATE") else None


def sqlite_keyword_search(state):
    """Stands in for PostgreSQL full text search: every query word must appear in title or text."""
    words = [w for w in state["parsed_intent"].get("query_clean", "").lower().split() if w]
    rows = db.fetchall("SELECT * FROM memory_items WHERE preprocessed = 1 AND is_deleted = 0")
    hits = [r for r in rows if words and all(w in f"{r['title']} {r['raw_text']}".lower() for w in words)]
    return {"postgres_results": [row_for_search(r) for r in hits]}


def row_for_search(r):
    domain = db.fetchall("SELECT domain, url FROM chrome_metadata WHERE memory_id = :m", {"m": r["memory_id"]})
    return {"memory_id": r["memory_id"], "source_type": r["source_type"], "title": r["title"],
            "raw_snippet": r["raw_text"] or "", "created_at": datetime(2026, 10, 5, 10, 0),
            "domain": domain[0]["domain"] if domain else None, "url": domain[0]["url"] if domain else None,
            "dwell_time_seconds": 120}


@pytest.fixture
def world(sqlite_db, isolated_settings, fake_embedding_model, fake_pg, tmp_path, monkeypatch):
    # Index and ENP database
    manager = fm.FAISSManager(tmp_path / "echo.index")
    monkeypatch.setattr(manager, "_upsert_embedding_row", lambda memory_id, text: None)
    monkeypatch.setattr(fm, "_default_manager", manager)
    pg = fake_pg(pg_view_of_sqlite)
    monkeypatch.setattr(ep, "get_connection", lambda: pg)
    monkeypatch.setattr(ep, "clear_enp_retry_count", lambda memory_id: None)
    monkeypatch.setattr("enp.system_group_classifier._get_stage4_llm", lambda: None)
    monkeypatch.setattr(chrome_connector, "record_visit", lambda url: None)

    # Retrieval: SQLite look-ups instead of PostgreSQL, a fake cross-encoder, no neighbours
    monkeypatch.setattr(search_tool, "node_postgres_keyword_search", sqlite_keyword_search)
    monkeypatch.setattr(fusion, "hydrate_memory_items", lambda ids: [
        row_for_search(r) for r in db.fetchall("SELECT * FROM memory_items WHERE is_deleted = 0 AND preprocessed = 1") if r["memory_id"] in ids])
    monkeypatch.setattr(search_coordinator, "hydrate_memory_items", fusion.hydrate_memory_items)
    monkeypatch.setattr(reranker, "get_cross_encoder", lambda: type("CE", (), {"predict": lambda self, pairs: [3.0 if "python" in t.lower() else -3.0 for _, t in pairs]})())
    monkeypatch.setattr(search_tool, "node_extend_neighborhood", lambda state: {"neighbor_items": {}})
    monkeypatch.setattr(search_tool, "fetch_visit_times", lambda ids, since: {})
    # The keyword stage is imported into search_tool under its graph_nodes name; make the semantic one use our model.
    history = {}
    monkeypatch.setattr(retrieval_engine, "load_conversation_history", lambda sid: history.get(sid, []))
    monkeypatch.setattr(retrieval_engine, "save_turn",
                        lambda sid, q, a: history.setdefault(sid, []).extend([HumanMessage(content=q), AIMessage(content=a)]))

    app = FastAPI()
    app.include_router(chrome_router)
    app.include_router(items_router)
    app.include_router(retrieval_router)
    client = TestClient(app, raise_server_exceptions=False)

    class World:
        pass

    w = World()
    w.client, w.manager, w.history = client, manager, history
    w.monkeypatch = monkeypatch

    def visit(url, title, text, **extra):
        body = {"url": url, "title": title, "domain": url.split("/")[2], "dwell_seconds": 40, "scroll_depth": 0.6,
                "interaction_count": 2, "content_extract": text, "word_count": len(text.split())}
        body.update(extra)
        return client.post("/chrome/ingest", json=body).json()

    def enrich():
        return ep.process_batch(manager, batch_size=10)

    def ask(question, luna, session_id=None):
        monkeypatch.setattr(agent_nodes, "build_agent_llm", lambda: luna)
        body = {"query": question}
        if session_id:
            body["session_id"] = session_id
        return client.post("/retrieval/query", json=body)

    w.visit, w.enrich, w.ask = visit, enrich, ask
    return w


PYTHON_TEXT = "## Python tutorial\nLearn python programming: variables, loops and functions explained for beginners"
COOKING_TEXT = "## Pasta recipe\nBoil the pasta, make a tomato sauce with garlic and basil, serve warm"


def test_a_page_is_captured_enriched_found_and_cited(world):
    """Shows: the whole product works end to end. A visited page becomes a cited answer to a natural question."""
    assert world.visit("https://docs.example.org/python-tutorial", "Python tutorial", PYTHON_TEXT)["status"] == "saved"
    assert world.visit("https://food.example.org/pasta", "Pasta recipe", COOKING_TEXT)["status"] == "saved"
    assert world.enrich() == (2, 0)
    assert world.manager.get_vector(db.fetchall("SELECT memory_id FROM memory_items")[0]["memory_id"]) is not None

    luna = ScriptedLuna(["python tutorial"], "You read the **Python tutorial** on docs.example.org ⟦1⟧.")
    response = world.ask("what python tutorial did I read?", luna)
    body = response.json()
    assert response.status_code == 200
    assert body["no_results"] is False and body["result_count"] >= 1
    assert body["answer_sources"][0]["title"] == "Python tutorial"
    assert "Python tutorial" in body["final_answer"]
    assert len(luna.calls) == 2                                       # usually 2 LLM calls per query (A26)


def test_what_the_llm_is_allowed_to_see(world):
    """Shows (CLAUDE.md §2.6): the model gets the question and bounded text, never database rows or the whole store."""
    long_text = "## Python deep dive\n" + "python " * 6000
    world.visit("https://docs.example.org/deep", "Python deep dive", long_text)
    world.visit("https://food.example.org/pasta", "Pasta recipe", COOKING_TEXT)
    world.enrich()
    luna = ScriptedLuna(["python"], "Found it ⟦1⟧.")
    world.ask("find my python page", luna)
    prompt = "\n".join(str(m.content) for call in luna.calls for m in call["messages"])
    assert "find my python page" in prompt
    assert "python " * 8001 not in prompt                             # the excerpt is bounded (8,000 characters for the top results)
    assert "python " * 1000 in prompt                                 # ... but real text is sent, not only a title
    for private_column in ("system_group_id", "classified_by", "dwell_time_seconds", "auto_keywords", "first_ingested_at", "search_vector"):
        assert private_column not in prompt                           # database columns are never sent, only text


def test_private_pages_never_enter_the_memory(world):
    """Shows (CLAUDE.md §2.6): login pages, mail and GitHub issues are discarded at the door, so they can never be found."""
    assert world.visit("https://bank.example.com/login", "Bank login", "account number 1234")["reason"] == "sensitive_page_excluded"
    assert world.visit("https://mail.google.com/mail/u/0/", "Inbox", "private mail")["reason"] == "gmail_web_excluded"
    assert world.visit("https://github.com/issues/3", "Issue", "bug")["reason"] == "application_path_excluded"
    assert world.visit("https://docs.example.org/python-tutorial", "Python tutorial", PYTHON_TEXT)["status"] == "saved"
    world.enrich()
    luna = ScriptedLuna(["bank account"], lambda messages: "Nothing found.")
    world.ask("my bank login", luna)
    prompt = "\n".join(str(m.content) for call in luna.calls for m in call["messages"])
    assert "account number 1234" not in prompt and "private mail" not in prompt
    assert db.fetchall("SELECT COUNT(*) AS n FROM memory_items")[0]["n"] == 1


def test_a_deleted_item_disappears_from_answers(world):
    """Shows: 'delete' really removes a memory from retrieval (soft-deleted rows are never searched)."""
    world.visit("https://docs.example.org/python-tutorial", "Python tutorial", PYTHON_TEXT)
    world.enrich()
    memory_id = db.fetchall("SELECT memory_id FROM memory_items")[0]["memory_id"]
    assert world.ask("python", ScriptedLuna(["python tutorial"], "Found ⟦1⟧.")).json()["result_count"] == 1
    assert world.client.delete(f"/items/{memory_id}").status_code == 200
    again = world.ask("python", ScriptedLuna(["python tutorial"], "Nothing.")).json()
    assert again["result_count"] == 0 and again["no_results"] is True


def test_a_follow_up_question_uses_the_chat_history(world):
    """Shows: multi-turn chat. The second question carries the first one's history and may skip the search."""
    world.visit("https://docs.example.org/python-tutorial", "Python tutorial", PYTHON_TEXT)
    world.enrich()
    first = world.ask("what python page did I read?", ScriptedLuna(["python tutorial"], "The Python tutorial ⟦1⟧.")).json()
    session = first["session_id"]
    luna = ScriptedLuna([], "It was about variables and loops.")
    second = world.ask("what was it about?", luna, session_id=session).json()
    prompt = "\n".join(str(m.content) for m in luna.calls[0]["messages"])
    assert "what python page did I read?" in prompt                   # the history reached the model
    assert len(luna.calls) == 1 and second["session_id"] == session


def test_the_agent_can_never_exceed_six_llm_calls_through_the_api(world):
    """Shows (CLAUDE.md §2.5): even a model that searches forever is stopped after 6 calls, over HTTP."""
    world.visit("https://docs.example.org/python-tutorial", "Python tutorial", PYTHON_TEXT)
    world.enrich()
    luna = ScriptedLuna([f"python {n}" for n in range(20)], "Here is what I found ⟦1⟧.")
    body = world.ask("keep searching", luna).json()
    searches_run = sum(isinstance(m, ToolMessage) for m in luna.calls[-1]["messages"])
    assert len(luna.calls) == 6 and searches_run == 5
    assert luna.calls[-1]["tools_bound"] is False and body["final_answer"]


def test_settings_stop_capture_immediately(world):
    """Shows: the user's capture switch works for the very next page, and pausing never touches stored memories."""
    world.visit("https://docs.example.org/python-tutorial", "Python tutorial", PYTHON_TEXT)
    assert world.client.post("/settings", json={"chrome_enabled": False}).status_code == 200
    assert world.visit("https://docs.example.org/other", "Other", "text")["reason"] == "chrome_capture_disabled"
    assert db.fetchall("SELECT COUNT(*) AS n FROM memory_items")[0]["n"] == 1
    export = world.client.get("/export").json()
    assert export["item_count"] == 1
