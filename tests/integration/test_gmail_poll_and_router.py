"""Integration tests for the Gmail poller (history vs. full listing), the router and the Gmail sign-in logic.

The Gmail service is a small fake; the database is the in-memory SQLite stand-in; Redis is a dictionary.
"""
import asyncio
import base64
import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from googleapiclient.errors import HttpError

from ingestion.gmail import database as gmail_db
from ingestion.gmail import gmail_api as ga
from ingestion.gmail import router as gmail_router
from ste import postgresql_manager as db
from ste import redis_manager


def b64(text):
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def gmail_message(message_id, subject="Hello", sender="Priya <p@x.com>", labels=("INBOX",), internal=1760000000000, body="Body text"):
    return {"id": message_id, "threadId": f"t-{message_id}", "labelIds": list(labels), "internalDate": str(internal),
            "payload": {"mimeType": "text/plain", "body": {"data": b64(body)},
                        "headers": [{"name": "Subject", "value": subject}, {"name": "From", "value": sender},
                                    {"name": "To", "value": "me@x.com"}]}}


class FakeService:
    """Answers users().messages().list/get, users().getProfile and users().history().list from dictionaries."""

    def __init__(self, messages, history=None, history_id="500"):
        self.mail, self.history_response, self.history_id = messages, history, history_id
        self.gets, self.lists, self._call = [], 0, None

    def users(self):
        return self

    def messages(self):  # noqa: D401 - mimics the Google client chain
        self._kind = "messages"
        return self

    def history(self):
        self._kind = "history"
        return self

    def getProfile(self, userId):
        self._call = ("profile", None)
        return self

    def list(self, **kwargs):
        self._call = (self._kind, kwargs)
        return self

    def get(self, **kwargs):
        self._call = ("get", kwargs)
        return self

    def execute(self):
        kind, kwargs = self._call
        if kind == "profile":
            return {"historyId": self.history_id}
        if kind == "get":
            self.gets.append(kwargs["id"])
            return self.mail[kwargs["id"]]
        if kind == "history":
            if isinstance(self.history_response, Exception):
                raise self.history_response
            return self.history_response
        self.lists += 1
        return {"messages": [{"id": mid} for mid in self.mail]}


@pytest.fixture
def mailbox(sqlite_db, isolated_settings, fake_redis, monkeypatch):
    monkeypatch.setattr(redis_manager, "get_sync_client", lambda: fake_redis)
    monkeypatch.setattr(ga, "_excluded_ids", {})
    monkeypatch.setattr(ga.time, "sleep", lambda s: None)
    monkeypatch.setattr(ga, "_already_stored", lambda ids: {r["email_id"] for r in db.fetchall("SELECT email_id FROM gmail_metadata")} & set(ids))
    return fake_redis


def stored_titles():
    return sorted(r["title"] for r in db.fetchall("SELECT title FROM memory_items"))


def test_first_poll_lists_the_whole_mailbox_and_saves_the_history_id(mailbox):
    service = FakeService({"a": gmail_message("a", "First", internal=1760000000000), "b": gmail_message("b", "Second", internal=1760000100000)})
    assert ga.fetch_and_store_new_emails(service) == 2
    assert stored_titles() == ["First", "Second"]
    assert redis_manager.get_gmail_history_id() == "500"


def test_next_poll_asks_only_for_what_is_new_since_the_saved_history_id(mailbox):
    redis_manager.set_gmail_history_id("400")
    history = {"history": [{"messagesAdded": [{"message": {"id": "new1", "labelIds": ["INBOX"]}},
                                              {"message": {"id": "spam1", "labelIds": ["SPAM"]}}]}], "historyId": "450"}
    service = FakeService({"new1": gmail_message("new1", "Fresh")}, history=history)
    assert ga.fetch_and_store_new_emails(service) == 1
    assert service.lists == 0 and service.gets == ["new1"]
    assert redis_manager.get_gmail_history_id() == "450"


def test_a_history_id_gmail_has_forgotten_falls_back_to_one_full_listing(mailbox):
    class Resp:
        status = 404
        reason = "not found"

    redis_manager.set_gmail_history_id("1")
    service = FakeService({"a": gmail_message("a")}, history=HttpError(Resp(), b""), history_id="900")
    assert ga.fetch_and_store_new_emails(service) == 1 and service.lists == 1
    assert redis_manager.get_gmail_history_id() == "900"


def test_re_polling_stores_nothing_twice(mailbox):
    service = FakeService({"a": gmail_message("a")})
    assert ga.fetch_and_store_new_emails(service) == 1
    redis_manager.set_gmail_history_id("")        # force another full listing
    assert ga.fetch_and_store_new_emails(service) == 0
    assert len(db.fetchall("SELECT 1 FROM memory_items")) == 1


def test_drafts_spam_and_excluded_senders_are_never_stored(mailbox):
    """Shows (privacy): unsent drafts, spam/trash and senders the user excluded are not captured."""
    from ste import settings_store

    settings_store.save_settings({"excluded_senders": ["noreply@"]})
    service = FakeService({
        "ok": gmail_message("ok", "Keep me"),
        "draft": gmail_message("draft", "Unsent", labels=("DRAFT",)),
        "spam": gmail_message("spam", "Buy now", labels=("SPAM",)),
        "bot": gmail_message("bot", "Receipt", sender="Shop <noreply@shop.com>"),
    })
    assert ga.fetch_and_store_new_emails(service) == 1
    assert stored_titles() == ["Keep me"]
    assert ga._still_excluded("bot") is True
    settings_store.save_settings({"excluded_senders": []})
    assert ga._still_excluded("bot") is False                # un-excluding captures the mail again next poll


def test_emails_are_stored_oldest_first(mailbox):
    service = FakeService({"late": gmail_message("late", "Late", internal=1760000900000), "early": gmail_message("early", "Early", internal=1760000000000)})
    ga.fetch_and_store_new_emails(service)
    order = [r["title"] for r in db.fetchall("SELECT title FROM memory_items ORDER BY rowid")]
    assert order == ["Early", "Late"]


def test_the_history_id_does_not_move_when_something_was_not_stored(mailbox, monkeypatch):
    monkeypatch.setattr(ga, "store_in_postgresql", lambda data: False)
    service = FakeService({"a": gmail_message("a")})
    assert ga.fetch_and_store_new_emails(service) == 0
    assert redis_manager.get_gmail_history_id() is None      # the next poll will try again


def test_a_broken_gmail_service_never_crashes_the_poller(mailbox):
    class Broken:
        def users(self):
            raise RuntimeError("network down")

    assert ga.fetch_and_store_new_emails(Broken()) == 0


# ── database.py ───────────────────────────────────────────────────────────────

def test_storage_layer_reports_new_vs_known_and_survives_errors(sqlite_db, monkeypatch):
    data = {"source_item_id": "x1", "title": "T", "time": {"event_timestamp": "2026-10-05T10:00:00+00:00"},
            "source_metadata": {"email": {}}, "content": {"primary_text": "b"}}
    assert gmail_db.store_in_postgresql(data) is True
    assert gmail_db.store_in_postgresql(data) is False
    monkeypatch.setattr(gmail_db, "store_gmail_message", lambda d: (_ for _ in ()).throw(RuntimeError("db")))
    assert gmail_db.store_in_postgresql(data) is False


def test_database_readiness_check(monkeypatch):
    monkeypatch.setattr(gmail_db, "_db_ready", None)
    monkeypatch.setattr(gmail_db.postgresql_manager, "scalar", lambda q, p=None: 1)
    assert gmail_db.initialize_database() is True
    monkeypatch.setattr(gmail_db.postgresql_manager, "scalar", lambda q, p=None: None)
    assert gmail_db.initialize_database() is False
    monkeypatch.setattr(gmail_db.postgresql_manager, "scalar", lambda q, p=None: (_ for _ in ()).throw(ConnectionError("down")))
    assert gmail_db.initialize_database() is False


# ── Router ────────────────────────────────────────────────────────────────────

@pytest.fixture
def client(sqlite_db, isolated_settings):
    app = FastAPI()
    app.include_router(gmail_router.router)
    return TestClient(app)


def test_health_and_engagement_routes(client):
    assert client.get("/gmail/health").json() == {"status": "ok", "module": "gmail"}
    assert client.post("/gmail/engagement", json={"email_id": "unknown", "dwell_seconds": 5}).status_code == 404
    assert client.post("/gmail/engagement", json={"email_id": "", "dwell_seconds": 5}).status_code == 422
    assert client.post("/gmail/engagement", json={"email_id": "x", "dwell_seconds": -1}).status_code == 422


def test_engagement_for_a_stored_mail_adds_reading_time(client):
    from ste import storage_engine

    storage_engine.store_gmail_message({"source_item_id": "m1", "title": "T", "time": {}, "source_metadata": {"email": {}}, "content": {}})
    assert client.post("/gmail/engagement", json={"email_id": "m1", "dwell_seconds": 40}).json() == {"status": "ok", "email_id": "m1"}
    assert db.fetchall("SELECT dwell_time_seconds FROM memory_engagement")[0]["dwell_time_seconds"] == 40


def test_poll_is_skipped_when_capture_is_off_the_database_is_down_or_not_signed_in(client, monkeypatch):
    from ste import settings_store

    monkeypatch.setattr(gmail_router, "_service", None)
    monkeypatch.setattr(gmail_router, "initialize_database", lambda: True)
    monkeypatch.setattr(gmail_router, "fetch_and_store_new_emails", lambda service: 3)
    monkeypatch.setattr(gmail_router, "authenticate_gmail", lambda: "service")
    assert client.post("/gmail/poll").json() == {"status": "ok", "processed": 3}
    monkeypatch.setattr(gmail_router, "initialize_database", lambda: False)
    assert client.post("/gmail/poll").json()["reason"] == "database_unavailable"
    monkeypatch.setattr(gmail_router, "initialize_database", lambda: True)
    monkeypatch.setattr(gmail_router, "_service", None)
    monkeypatch.setattr(gmail_router, "authenticate_gmail", lambda: (_ for _ in ()).throw(ga.GmailAuthError("sign in")))
    assert client.post("/gmail/poll").json()["reason"] == "gmail_not_signed_in"
    assert client.post("/gmail/poll").json()["reason"] == "gmail_not_signed_in"       # second time logs quietly
    settings_store.save_settings({"gmail_enabled": False})
    assert client.post("/gmail/poll").json()["reason"] == "gmail_capture_disabled"


def test_the_poll_loop_keeps_going_after_an_error_and_stops_when_cancelled(monkeypatch):
    calls = []

    def poll():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("boom")
        return {"status": "ok"}

    monkeypatch.setattr(gmail_router, "poll_once", poll)
    monkeypatch.setattr(gmail_router, "CHECK_INTERVAL", 0)

    async def run():
        task = asyncio.create_task(gmail_router.poll_forever())
        while len(calls) < 3:
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert len(calls) >= 3


# ── Sign-in ───────────────────────────────────────────────────────────────────

class FakeCreds:
    def __init__(self, valid=True, expired=False, refresh_token="r", refresh_ok=True, after_refresh_valid=True):
        self.valid, self.expired, self.refresh_token = valid, expired, refresh_token
        self.refresh_ok, self.after_refresh_valid = refresh_ok, after_refresh_valid

    def refresh(self, request):
        if isinstance(self.refresh_ok, Exception):
            raise self.refresh_ok
        self.valid = self.after_refresh_valid

    def to_json(self):
        return json.dumps({"token": "new"})


def patch_creds(monkeypatch, tmp_path, creds, token_text='{"token": "t"}'):
    token = tmp_path / "token_gmail.json"
    if token_text is not None:
        token.write_text(token_text)
    monkeypatch.setattr(ga, "TOKEN_PATH", token)
    monkeypatch.setattr(ga.Credentials, "from_authorized_user_info", lambda info, scopes: creds)
    return token


def test_a_valid_token_is_used_as_it_is(monkeypatch, tmp_path):
    patch_creds(monkeypatch, tmp_path, FakeCreds())
    assert ga._load_gmail_credentials() is not None


def test_an_expired_token_is_refreshed_and_saved(monkeypatch, tmp_path):
    token = patch_creds(monkeypatch, tmp_path, FakeCreds(valid=False, expired=True))
    ga._load_gmail_credentials()
    assert json.loads(token.read_text()) == {"token": "new"}


def test_token_problems_are_told_apart(monkeypatch, tmp_path):
    from google.auth.exceptions import RefreshError

    patch_creds(monkeypatch, tmp_path, None, token_text=None)
    with pytest.raises(ga.GmailAuthError):                      # no token file
        ga._load_gmail_credentials()
    patch_creds(monkeypatch, tmp_path, FakeCreds(valid=False, expired=True, refresh_ok=RefreshError("revoked")))
    with pytest.raises(ga.GmailAuthError):                      # Google refused: sign in again
        ga._load_gmail_credentials()
    patch_creds(monkeypatch, tmp_path, FakeCreds(valid=False, expired=True, refresh_ok=ConnectionError("offline")))
    with pytest.raises(ga.GmailUnavailableError):               # offline: not a reason to open a browser
        ga._load_gmail_credentials()
    patch_creds(monkeypatch, tmp_path, FakeCreds(valid=False, expired=True, after_refresh_valid=False))
    with pytest.raises(ga.GmailAuthError):
        ga._load_gmail_credentials()
    patch_creds(monkeypatch, tmp_path, FakeCreds(valid=False, expired=False, refresh_token=None))
    with pytest.raises(ga.GmailAuthError):
        ga._load_gmail_credentials()
    monkeypatch.setattr(ga.Credentials, "from_authorized_user_info", lambda info, scopes: (_ for _ in ()).throw(ValueError("bad")))
    with pytest.raises(ga.GmailAuthError):
        ga._load_gmail_credentials()


def test_authenticate_builds_the_gmail_service(monkeypatch, tmp_path):
    patch_creds(monkeypatch, tmp_path, FakeCreds())
    monkeypatch.setattr(ga, "build", lambda name, version, credentials: ("service", name, version))
    assert ga.authenticate_gmail() == ("service", "gmail", "v1")


def test_sign_in_only_opens_the_browser_when_needed(monkeypatch, tmp_path):
    opened = []

    class Flow:
        def run_local_server(self, port):
            opened.append(port)
            return FakeCreds()

    monkeypatch.setattr(ga.InstalledAppFlow, "from_client_secrets_file", lambda path, scopes: Flow())
    token = patch_creds(monkeypatch, tmp_path, FakeCreds())
    ga.sign_in_gmail()
    assert opened == []                                          # a usable token: no browser
    token.unlink()
    ga.sign_in_gmail()
    assert opened == [0] and token.exists()
    monkeypatch.setattr(ga, "_load_gmail_credentials", lambda: (_ for _ in ()).throw(ga.GmailUnavailableError("offline")))
    ga.sign_in_gmail()
    assert opened == [0]                                         # offline: no browser either
    monkeypatch.setattr(ga, "_load_gmail_credentials", lambda: (_ for _ in ()).throw(ga.GmailAuthError("no")))
    monkeypatch.setattr(ga.InstalledAppFlow, "from_client_secrets_file", lambda path, scopes: (_ for _ in ()).throw(FileNotFoundError()))
    ga.sign_in_gmail()                                           # a failed sign-in is logged, never raised
