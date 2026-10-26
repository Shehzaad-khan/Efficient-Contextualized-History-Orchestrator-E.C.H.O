"""Integration tests for the ENP worker (enp/enrichment_pipeline.py).

Real: text cleaning, keyword extraction, the embedding window, the 4-stage classifier, a real FAISS index.
Fake: PostgreSQL (a scripted connection), Redis (a dictionary) and the embedding model (word hashing).
"""
import threading
from datetime import datetime

import pytest

from enp import enrichment_pipeline as ep
from ste import faiss_manager as fm

faiss = pytest.importorskip("faiss")


@pytest.fixture(autouse=True)
def quiet_redis(monkeypatch):
    monkeypatch.setattr(ep, "clear_enp_retry_count", lambda memory_id: None)


def router(rules):
    """A handler that answers by the first rule whose text appears in the SQL: rules = [(text, answer), ...]."""
    def handler(sql, params):
        for needle, answer in rules:
            if needle in sql:
                return answer(params) if callable(answer) else answer
        return None
    return handler


BASE = {"memory_id": "m1", "source_type": "chrome", "source_id": "https://docs.python.org/3/", "title": "Python docs",
        "raw_text": "## Tutorial\nPython is easy to learn and powerful programming language", "created_at": datetime(2026, 10, 5)}


# ── Fetching ──────────────────────────────────────────────────────────────────

def test_fetch_unprocessed_items_maps_rows_and_skips_failed_by_default(fake_pg, monkeypatch):
    monkeypatch.delenv(ep.RETRY_FAILED_ENV, raising=False)
    conn = fake_pg(router([("FROM memory_items", [("id1", "gmail", "sid", None, None, datetime(2026, 1, 1))])]))
    items = ep.fetch_unprocessed_items(conn, batch_size=5)
    assert items == [{"memory_id": "id1", "source_type": "gmail", "source_id": "sid", "title": "", "raw_text": "",
                      "created_at": datetime(2026, 1, 1)}]
    sql, params = conn.calls[0]
    assert "<> %s" in sql and params[0] == "failed" and params[-1] == 5


def test_fetch_can_include_failed_rows_for_a_retry(fake_pg, monkeypatch):
    conn = fake_pg(router([("FROM memory_items", [])]))
    ep.fetch_unprocessed_items(conn, 3, include_failed=True)
    assert "<> %s" not in conn.calls[0][0]
    monkeypatch.setenv(ep.RETRY_FAILED_ENV, "true")
    conn = fake_pg(router([("FROM memory_items", [])]))
    ep.fetch_unprocessed_items(conn, 3)
    assert "<> %s" not in conn.calls[0][0]


def test_thread_history_gives_a_bare_reply_its_threads_topics(fake_pg):
    conn = fake_pg(router([("gm.thread_id", [("Offer", ["salary"], "Please confirm"), (None, None, None)])]))
    assert ep._fetch_thread_history(conn, {"thread_id": "t1", "memory_id": "m", "created_at": None}) == ["Offer salary Please confirm"]
    assert ep._fetch_thread_history(conn, {"memory_id": "m"}) == []


# ── Context per source ────────────────────────────────────────────────────────

def test_gmail_context_includes_metadata_thread_and_attachment_text(fake_pg, monkeypatch):
    conn = fake_pg(router([
        ("FROM gmail_metadata", [("email-1", "t1", "Priya <p@techcorp.com>", ["me@x.com"], "Interview", ["INBOX"], True)]),
        ("SELECT filename FROM gmail_attachments", [("offer.pdf",)]),
        ("SELECT attachment_id", [("a1", "offer.pdf", "application/pdf", 100, "Stored offer text")]),
        ("gm.thread_id", []),
    ]))
    item = ep.load_item_context(conn, {**BASE, "source_type": "gmail", "raw_text": "See you Friday"}, ep.LazyGmailService())
    assert item["sender_domain"] == "techcorp.com" and item["attachment_filenames"] == ["offer.pdf"]
    assert item["attachment_texts"] == [{"filename": "offer.pdf", "text": "Stored offer text"}]
    assert item["content_primary_text"] == "See you Friday" and item["message_history"] is None


def test_missing_metadata_rows_do_not_stop_enrichment(fake_pg):
    conn = fake_pg(lambda sql, params: [])
    assert ep.load_item_context(conn, BASE)["raw_html"] == BASE["raw_text"]
    assert ep.load_item_context(conn, {**BASE, "source_type": "youtube"})["description"] == BASE["raw_text"]
    assert ep.load_item_context(conn, {**BASE, "source_type": "gmail"})["attachment_texts"] == []
    assert ep.load_item_context(conn, {**BASE, "source_type": "tiktok"})["source"] == "tiktok"


def test_chrome_and_youtube_context(fake_pg):
    conn = fake_pg(router([
        ("FROM chrome_metadata", [("https://x.com/a", "https://x.com/a", "x.com")]),
        ("FROM youtube_metadata", [("vid", "CodeCh", "chan1", 100, 1, "transcript", 27)]),
    ]))
    chrome = ep.load_item_context(conn, BASE)
    assert chrome["domain"] == "x.com"
    youtube = ep.load_item_context(conn, {**BASE, "source_type": "youtube"})
    assert youtube["channel_name"] == "CodeCh" and youtube["is_short"] is True and youtube["youtube_category_id"] == 27


def test_lazy_gmail_service_signs_in_once_and_remembers_failure(monkeypatch):
    calls = []
    monkeypatch.setattr("ingestion.gmail.gmail_api.authenticate_gmail", lambda: calls.append(1) or "service")
    lazy = ep.LazyGmailService()
    assert lazy.get() == "service" and lazy.get() == "service" and len(calls) == 1

    def fail():
        raise RuntimeError("no token")

    monkeypatch.setattr("ingestion.gmail.gmail_api.authenticate_gmail", fail)
    broken = ep.LazyGmailService()
    assert broken.get() is None and broken.get() is None


def test_attachment_text_is_downloaded_once_and_failures_are_skipped(fake_pg, monkeypatch):
    rows = [("a1", "new.pdf", "application/pdf", 10, None), ("a2", "bad.pdf", "application/pdf", 10, None),
            ("a3", "empty.pdf", "application/pdf", 10, None)]
    conn = fake_pg(router([("SELECT attachment_id", rows), ("UPDATE gmail_attachments", 1)]))

    def fake_extract(service, email_id, meta):
        if meta["filename"] == "bad.pdf":
            raise RuntimeError("unreadable")
        return "" if meta["filename"] == "empty.pdf" else "extracted words"

    monkeypatch.setattr(ep, "extract_full_text", fake_extract)

    class Service:
        def get(self):
            return "service"

    texts = ep._load_attachment_texts(conn, {"memory_id": "m", "email_id": "e"}, Service())
    assert texts == [{"filename": "new.pdf", "text": "extracted words"}]
    assert conn.rollbacks >= 1
    assert any("UPDATE gmail_attachments" in sql for sql in conn.statements())


# ── Preparing the text ────────────────────────────────────────────────────────

def test_prepare_item_builds_clean_text_keywords_and_embeddable_text(fake_pg, fake_embedding_model):
    conn = fake_pg(router([("FROM chrome_metadata", [("https://docs.python.org/3/", "https://docs.python.org/3/", "docs.python.org")])]))
    item = ep.prepare_item_for_embedding(conn, BASE)
    assert item["headings"] == ["Tutorial"] and item["clean_text"].startswith("Python is easy")
    assert item["embeddable_text"].startswith("Python docs Tutorial docs.python.org")
    assert "python" in item["auto_keywords"] and item["word_count"] == len(item["clean_text"].split())


def test_title_only_rows_are_still_embedded_from_their_identity(fake_pg, fake_embedding_model):
    conn = fake_pg(router([("FROM chrome_metadata", [("https://x.com/a", "https://x.com/a", "x.com")])]))
    item = ep.prepare_item_for_embedding(conn, {**BASE, "raw_text": "", "title": ""})
    assert item["embeddable_text"] == "x.com"
    # a video whose metadata never arrived is indexed from its video id alone
    video = fake_pg(router([("FROM youtube_metadata", [("vid42", None, None, None, 0, None, None)])]))
    bare = ep.prepare_item_for_embedding(video, {**BASE, "source_type": "youtube", "raw_text": "", "title": ""})
    assert bare["embeddable_text"] == "vid42"
    # a row with nothing at all (no title, no domain, no text) cannot be embedded
    with pytest.raises(ValueError, match="No embeddable text"):
        ep.prepare_item_for_embedding(fake_pg(lambda sql, params: []), {**BASE, "raw_text": "", "title": ""})


# ── Marking rows ──────────────────────────────────────────────────────────────

def test_mark_item_processed_stores_the_group_keywords_and_search_vector(fake_pg, monkeypatch):
    cleared = []
    conn = fake_pg(lambda sql, params: 1)
    monkeypatch.setattr(ep, "clear_enp_retry_count", cleared.append)
    ep.mark_item_processed(conn, "m1", "study", "domain", ["python"], source="chrome", word_count=42)
    first_sql, first_params = conn.calls[0]
    assert "preprocessed = TRUE" in first_sql and "search_vector" in first_sql and "to_tsvector" in first_sql
    assert first_params[0] == 2 and first_params[1] == "domain" and first_params[-1] == "m1"    # study -> system_group_id 2
    assert any("UPDATE chrome_metadata" in sql for sql in conn.statements())
    assert cleared == ["m1"] and conn.commits == 1


def test_an_unknown_category_is_stored_as_misc_never_null(fake_pg):
    """Shows (CLAUDE.md §2.2): system_group_id falls back to 'misc' (5)."""
    conn = fake_pg(lambda sql, params: 1)
    ep.mark_item_processed(conn, "m1", "banana", "llm", [], source="gmail")
    assert conn.calls[0][1][0] == 5
    assert any("is_processed = TRUE" in sql for sql in conn.statements())


def test_failed_rows_are_marked_failed_with_a_trimmed_reason(fake_pg):
    conn = fake_pg(lambda sql, params: 1)
    ep.mark_item_failed(conn, "m1", "x" * 900)
    sql, params = conn.calls[0]
    assert params[0] == "failed" and len(params[1]) == 500


# ── A whole batch ─────────────────────────────────────────────────────────────

@pytest.fixture
def real_index(tmp_path, monkeypatch):
    manager = fm.FAISSManager(tmp_path / "echo.index")
    monkeypatch.setattr(manager, "_upsert_embedding_row", lambda memory_id, text: None)
    return manager


def batch_connection(fake_pg, items):
    state = {"marked": []}

    def handler(sql, params):
        if "UPDATE memory_items" in sql and "preprocessed = TRUE" in sql:
            state["marked"].append(params[-1])
            return 1
        if "FROM memory_items WHERE preprocessed = FALSE" in sql:
            return [(i["memory_id"], i["source_type"], i["source_id"], i["title"], i["raw_text"], i["created_at"]) for i in items]
        if "FROM chrome_metadata" in sql:
            return [("https://docs.python.org/3/", "https://docs.python.org/3/", "docs.python.org")]
        return None

    conn = fake_pg(handler)
    conn.state = state
    return conn


def test_a_batch_embeds_classifies_indexes_and_marks_rows(fake_pg, fake_embedding_model, real_index, monkeypatch):
    """Shows: raw rows become searchable. The vector is in FAISS and the file is saved BEFORE the row is marked done."""
    conn = batch_connection(fake_pg, [BASE])
    order = []
    monkeypatch.setattr(ep, "get_connection", lambda: conn)
    original_save = real_index.save_index
    monkeypatch.setattr(real_index, "save_index", lambda *a, **k: order.append("saved") or original_save(*a, **k))
    original_mark = ep.mark_item_processed
    monkeypatch.setattr(ep, "mark_item_processed", lambda *a, **k: order.append("marked") or original_mark(*a, **k))
    assert ep.process_batch(real_index, batch_size=10) == (1, 0)
    assert order == ["saved", "marked"]
    assert conn.state["marked"] == ["m1"] and conn.closed
    assert real_index.get_vector("m1") is not None
    query = fake_embedding_model.encode(["python tutorial"], prompt_name="query")[0]
    assert real_index.search_all(query, k=1)[0][0] == "m1"


def test_an_empty_queue_does_nothing(fake_pg, monkeypatch, real_index):
    monkeypatch.setattr(ep, "get_connection", lambda: batch_connection(fake_pg, []))
    assert ep.process_batch(real_index, 10) == (0, 0)


def test_a_row_that_cannot_be_prepared_is_marked_failed_and_the_rest_continue(fake_pg, fake_embedding_model, real_index, monkeypatch):
    bad = {**BASE, "memory_id": "bad", "source_type": "other", "title": "", "raw_text": ""}
    conn = batch_connection(fake_pg, [bad, BASE])
    monkeypatch.setattr(ep, "get_connection", lambda: conn)
    assert ep.process_batch(real_index, 10) == (1, 1)
    assert any("classified_by = %s" in sql and "ARRAY" in sql for sql in conn.statements())      # mark_item_failed ran


def test_batch_embedding_failure_falls_back_to_one_by_one(fake_pg, fake_embedding_model, real_index, monkeypatch):
    conn = batch_connection(fake_pg, [BASE])
    monkeypatch.setattr(ep, "get_connection", lambda: conn)
    monkeypatch.setattr(ep, "generate_embeddings", lambda texts: (_ for _ in ()).throw(RuntimeError("model busy")))
    assert ep.process_batch(real_index, 10) == (1, 0)


def test_an_item_whose_embedding_fails_is_failed_not_lost(fake_pg, fake_embedding_model, real_index, monkeypatch):
    conn = batch_connection(fake_pg, [BASE])
    monkeypatch.setattr(ep, "get_connection", lambda: conn)
    monkeypatch.setattr(ep, "generate_embeddings", lambda texts: (_ for _ in ()).throw(RuntimeError("busy")))
    monkeypatch.setattr(ep, "generate_embedding", lambda text: (_ for _ in ()).throw(RuntimeError("still busy")))
    assert ep.process_batch(real_index, 10) == (0, 1)


def test_if_the_index_cannot_be_saved_no_row_is_marked_done(fake_pg, fake_embedding_model, real_index, monkeypatch):
    """Shows: crash safety. A row is never 'done' unless its vector reached the disk."""
    conn = batch_connection(fake_pg, [BASE])
    monkeypatch.setattr(ep, "get_connection", lambda: conn)
    monkeypatch.setattr(real_index, "save_index", lambda *a, **k: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError):
        ep.process_batch(real_index, 10)
    assert conn.state["marked"] == [] and conn.closed


def test_a_failed_mark_leaves_the_row_for_the_next_batch(fake_pg, fake_embedding_model, real_index, monkeypatch):
    conn = batch_connection(fake_pg, [BASE])
    monkeypatch.setattr(ep, "get_connection", lambda: conn)
    monkeypatch.setattr(ep, "mark_item_processed", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("deadlock")))
    assert ep.process_batch(real_index, 10) == (0, 0)


# ── Retrying failed rows ──────────────────────────────────────────────────────

def test_failed_rows_are_retried_at_most_three_times(fake_pg, monkeypatch, fake_redis):
    monkeypatch.setattr(ep, "get_enp_retry_count", lambda mid: {"fresh": 0, "tired": 3}[mid])
    attempts = []
    monkeypatch.setattr(ep, "increment_enp_retry_count", lambda mid: attempts.append(mid) or 1)
    conn = fake_pg(router([("classified_by = %s", [("fresh",), ("tired",)]), ("SET classified_by = NULL", 1)]))
    assert ep.retry_failed_items(conn) == 1
    assert attempts == ["fresh"]
    assert ep.retry_failed_items(fake_pg(lambda sql, params: [])) == 0


def test_a_retry_that_cannot_be_counted_is_not_retried(fake_pg, monkeypatch):
    monkeypatch.setattr(ep, "get_enp_retry_count", lambda mid: 0)
    monkeypatch.setattr(ep, "increment_enp_retry_count", lambda mid: (_ for _ in ()).throw(ConnectionError("redis down")))
    conn = fake_pg(router([("classified_by = %s", [("m1",)])]))
    assert ep.retry_failed_items(conn) == 0


# ── The worker loop ───────────────────────────────────────────────────────────

def test_run_pipeline_polls_until_stopped(monkeypatch, fake_pg):
    stop = threading.Event()
    calls = {"batches": 0, "centroids": 0}

    def fake_batch(manager, batch_size):
        calls["batches"] += 1
        if calls["batches"] == 2:
            stop.set()
        return (1, 0) if calls["batches"] == 1 else (0, 0)

    monkeypatch.setattr(ep, "get_manager", lambda: object())
    monkeypatch.setattr(ep, "initialize_centroids", lambda fn: calls.__setitem__("centroids", calls["centroids"] + 1))
    monkeypatch.setattr(ep, "centroids_need_recompute", lambda: True)
    monkeypatch.setattr(ep, "recompute_centroids", lambda: {"study": 10})
    monkeypatch.setattr(ep, "get_connection", lambda: fake_pg(lambda sql, params: []))
    monkeypatch.setattr(ep, "retry_failed_items", lambda conn: 2)
    monkeypatch.setattr(ep, "process_batch", fake_batch)
    ep.run_pipeline(batch_size=5, poll_interval=0, stop_event=stop)
    assert calls == {"batches": 2, "centroids": 1}


def test_run_pipeline_survives_errors_everywhere(monkeypatch):
    stop = threading.Event()

    def broken_batch(manager, batch_size):
        stop.set()
        raise RuntimeError("database unreachable")

    monkeypatch.setattr(ep, "get_manager", lambda: object())
    monkeypatch.setattr(ep, "initialize_centroids", lambda fn: None)
    monkeypatch.setattr(ep, "centroids_need_recompute", lambda: True)
    monkeypatch.setattr(ep, "recompute_centroids", lambda: (_ for _ in ()).throw(RuntimeError("no vectors")))
    monkeypatch.setattr(ep, "get_connection", lambda: (_ for _ in ()).throw(RuntimeError("no db")))
    monkeypatch.setattr(ep, "process_batch", broken_batch)
    ep.run_pipeline(poll_interval=0, stop_event=stop)        # must return normally


def test_database_url_is_required(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ValueError):
        ep.get_database_url()
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h/db")
    assert ep.get_database_url().startswith("postgresql://")
