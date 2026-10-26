"""Integration tests for the HTTP API of the user-data routes and the Chrome connector.

The real FastAPI routers, the real settings store, the real storage SQL (in-memory SQLite) and the real
privacy rules run together; only Redis is a dictionary and the browser is a test client.
"""
import csv
import io
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.items import router as items_router
from ingestion.chrome import chrome_connector
from ingestion.chrome.chrome_connector import router as chrome_router
from ste import postgresql_manager as db


@pytest.fixture
def client(sqlite_db, isolated_settings, monkeypatch):
    monkeypatch.setattr(chrome_connector, "record_visit", lambda url: None)
    monkeypatch.setattr(chrome_connector, "check_and_record_visit", lambda url: False)
    app = FastAPI()
    app.include_router(items_router)
    app.include_router(chrome_router)
    return TestClient(app, raise_server_exceptions=False)


def page(**overrides):
    body = {"url": "https://blog.example.com/post?utm_source=mail", "title": "A good post", "domain": "blog.example.com",
            "dwell_seconds": 30, "scroll_depth": 0.5, "interaction_count": 1, "content_extract": "Post text", "word_count": 2}
    body.update(overrides)
    return body


# ── Chrome ingest: every privacy rule, over HTTP ──────────────────────────────

def test_a_good_page_is_saved_once_even_if_sent_twice(client):
    """Shows: capture works end to end and tracking parameters do not create duplicates."""
    first = client.post("/chrome/ingest", json=page()).json()
    second = client.post("/chrome/ingest", json=page(url="https://blog.example.com/post?utm_source=ad")).json()
    assert first["status"] == "saved" and first["mode"] == "content_and_engagement"
    assert second["memory_id"] == first["memory_id"]
    assert db.fetchall("SELECT COUNT(*) AS n FROM memory_items")[0]["n"] == 1
    assert db.fetchall("SELECT source_id FROM memory_items")[0]["source_id"] == "https://blog.example.com/post"


@pytest.mark.parametrize(
    "overrides, reason",
    [
        ({"dwell_seconds": 2}, "intent_not_confirmed"),
        ({"dwell_seconds": 6, "scroll_depth": 0.0, "interaction_count": 0}, "intent_not_confirmed"),
        ({"url": "https://bank.example.com/login", "domain": "bank.example.com"}, "sensitive_page_excluded"),
        ({"url": "https://shop.example.com/checkout?x=1", "domain": "shop.example.com"}, "sensitive_page_excluded"),
        ({"url": "https://github.com/issues/4", "domain": "github.com"}, "application_path_excluded"),
        ({"url": "https://mail.google.com/mail/u/0/", "domain": "mail.google.com"}, "gmail_web_excluded"),
    ],
)
def test_pages_that_must_not_be_stored_are_discarded_with_a_reason(client, overrides, reason):
    result = client.post("/chrome/ingest", json=page(**overrides)).json()
    assert result == {"status": "discarded", "reason": reason}
    assert db.fetchall("SELECT COUNT(*) AS n FROM memory_items")[0]["n"] == 0


def test_secret_query_parameters_never_reach_the_database(client):
    """Shows: a token in the URL is removed before the URL becomes the item's id."""
    client.post("/chrome/ingest", json=page(url="https://docs.example.com/doc?page=2&token=SECRET123", domain="docs.example.com"))
    stored = db.fetchall("SELECT m.source_id, c.url FROM memory_items m JOIN chrome_metadata c USING (memory_id)")[0]
    assert "SECRET123" not in stored["source_id"] and "SECRET123" not in stored["url"]


def test_slack_pages_are_stored_by_title_only(client):
    """Shows (CLAUDE.md §2.6): app-page text is never captured, even if the extension sent it."""
    result = client.post("/chrome/ingest", json=page(
        url="https://acme.slack.com/client/T1/C1", domain="acme.slack.com", title="#general - Acme Slack",
        content_extract="private chat", dwell_seconds=6, scroll_depth=0.0, interaction_count=0)).json()
    assert result["mode"] == "engagement_only"
    stored = db.fetchall("SELECT title, raw_text FROM memory_items")[0]
    assert stored == {"title": "#general - Acme Slack", "raw_text": None}


def test_app_pages_with_no_engagement_are_discarded(client):
    result = client.post("/chrome/ingest", json=page(url="https://acme.slack.com/x", domain="acme.slack.com",
                                                    dwell_seconds=1, interaction_count=0)).json()
    assert result["reason"] == "app_page_not_engaged"


def test_public_pages_of_app_domains_are_ordinary_pages(client):
    result = client.post("/chrome/ingest", json=page(url="https://help.slack.com/articles/1", domain="help.slack.com")).json()
    assert result["mode"] == "content_and_engagement"


def test_a_database_failure_gives_the_extension_a_500(client, monkeypatch):
    monkeypatch.setattr(chrome_connector, "store_chrome_page", lambda payload: (_ for _ in ()).throw(RuntimeError("db down")))
    assert client.post("/chrome/ingest", json=page()).status_code == 500


def test_a_failing_revisit_flag_does_not_stop_the_page_being_saved(client, monkeypatch):
    monkeypatch.setattr(chrome_connector, "record_visit", lambda url: (_ for _ in ()).throw(ConnectionError("redis down")))
    assert client.post("/chrome/ingest", json=page()).json()["status"] == "saved"


def test_invalid_payloads_are_rejected(client):
    assert client.post("/chrome/ingest", json=page(dwell_seconds=-1)).status_code == 422
    assert client.post("/chrome/ingest", json=page(scroll_depth=2)).status_code == 422
    assert client.post("/chrome/ingest", json={"url": "x"}).status_code == 422


def test_revisit_check_answers_and_survives_a_redis_outage(client, monkeypatch):
    assert client.post("/chrome/revisit-check", json={"canonical_url": "https://a.com/x?token=1"}).json() == {"is_revisit": False}
    monkeypatch.setattr(chrome_connector, "check_and_record_visit", lambda url: True)
    assert client.post("/chrome/revisit-check", json={"canonical_url": "https://a.com/x"}).json() == {"is_revisit": True}
    monkeypatch.setattr(chrome_connector, "check_and_record_visit", lambda url: (_ for _ in ()).throw(ConnectionError()))
    assert client.post("/chrome/revisit-check", json={"canonical_url": "https://a.com/x"}).json() == {"is_revisit": False}


def test_rules_endpoint_serves_the_one_copy_of_the_lists(client):
    rules = client.get("/chrome/rules").json()
    assert "slack.com" in rules["app_domains"] and "login" in rules["sensitive_terms"] and "token" in rules["secret_query_params"]
    assert rules["app_domains"] == sorted(rules["app_domains"])


# ── Capture settings gate the connector ───────────────────────────────────────

def test_settings_endpoint_changes_what_is_captured(client):
    """Shows: the user's capture settings (Redis) take effect on the very next page."""
    assert client.get("/settings").json()["chrome_enabled"] is True
    assert client.post("/settings", json={"excluded_domains": ["blog.example.com"]}).status_code == 200
    assert client.post("/chrome/ingest", json=page()).json()["reason"] == "domain_excluded"
    client.post("/settings", json={"excluded_domains": [], "chrome_enabled": False})
    assert client.post("/chrome/ingest", json=page()).json()["reason"] == "chrome_capture_disabled"
    client.post("/settings", json={"chrome_enabled": True})
    assert client.post("/chrome/ingest", json=page()).json()["status"] == "saved"


def test_settings_validation(client):
    assert client.post("/settings", json={}).status_code == 400
    assert client.post("/settings", json={"retention": "never"}).status_code == 422
    assert client.post("/settings", json={"excluded_domains": ["x" * 300]}).status_code == 422


def test_settings_save_reports_503_when_redis_is_down(client, monkeypatch):
    from ste import settings_store

    monkeypatch.setattr(settings_store, "get_sync_client", lambda: (_ for _ in ()).throw(ConnectionError("down")))
    assert client.post("/settings", json={"chrome_enabled": False}).status_code == 503
    assert client.get("/settings").status_code == 200       # reading never fails


# ── Delete and export ─────────────────────────────────────────────────────────

def seed(title, source_id=None):
    memory_id = str(uuid.uuid4())
    db.execute("INSERT INTO memory_items (memory_id, system_group_id, source_type, source_id, title, created_at) "
               "VALUES (:id, 5, 'chrome', :sid, :title, '2026-10-05 10:00:00')",
               {"id": memory_id, "sid": source_id or memory_id, "title": title})
    return memory_id


def test_delete_is_a_soft_delete_and_only_works_once(client):
    memory_id = seed("to delete")
    assert client.delete(f"/items/{memory_id}").json() == {"status": "deleted", "memory_id": memory_id}
    assert db.fetchall("SELECT is_deleted FROM memory_items")[0]["is_deleted"] == 1      # the row stays
    assert client.delete(f"/items/{memory_id}").status_code == 404
    assert client.delete(f"/items/{uuid.uuid4()}").status_code == 404
    assert client.delete("/items/not-a-uuid").status_code == 422


def test_delete_database_error_is_a_plain_500(client, monkeypatch):
    monkeypatch.setattr(db, "execute", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("secret detail")))
    response = client.delete(f"/items/{uuid.uuid4()}")
    assert response.status_code == 500 and "secret detail" not in response.text


def test_export_json_leaves_out_deleted_items(client):
    keep, gone = seed("kept"), seed("deleted one")
    client.delete(f"/items/{gone}")
    body = client.get("/export").json()
    assert body["item_count"] == 1 and body["items"][0]["memory_id"] == keep
    assert "echo_export.json" in client.get("/export").headers["content-disposition"]


def test_export_csv_defuses_spreadsheet_formulas(client):
    """Shows: a page titled '=HYPERLINK(...)' cannot run as a formula when the CSV is opened in Excel."""
    seed('=HYPERLINK("http://evil.example","click")')
    response = client.get("/export?format=csv")
    assert response.headers["content-type"].startswith("text/csv")
    rows = list(csv.DictReader(io.StringIO(response.text)))
    assert rows[0]["title"].startswith("'=")
    assert client.get("/export?format=xml").status_code == 422


def test_export_of_an_empty_store_and_a_database_error(client, monkeypatch):
    assert client.get("/export").json() == {"item_count": 0, "items": []}
    assert client.get("/export?format=csv").text == ""
    monkeypatch.setattr(db, "fetchall", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("down")))
    assert client.get("/export").status_code == 500


def test_csv_cell_helper():
    from backend.items import _csv_cell

    assert _csv_cell(None) == "" and _csv_cell(["a", "b"]) == "a; b"
    assert _csv_cell("+1+1") == "'+1+1" and _csv_cell("plain") == "plain" and _csv_cell(5) == 5
