"""Integration tests for the YouTube connector routes and the Data API client (Google is a fake)."""
import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from googleapiclient.errors import HttpError

from ingestion.youtube import youtube_api_client as yt
from ingestion.youtube import youtube_connector as yc
from ste import postgresql_manager as db

VIDEO = "dQw4w9WgXcQ"
SHORT = "abcdefghijk"


@pytest.fixture
def client(sqlite_db, isolated_settings, monkeypatch, fake_redis):

    seen = set()

    async def revisit(namespace, raw_key, ttl_seconds=0):
        was = raw_key in seen
        seen.add(raw_key)
        return was

    monkeypatch.setattr(yc, "check_and_record_revisit_async", revisit)
    monkeypatch.setattr(yc, "_confirmed_revisits", {})

    async def metadata(video_id):
        return {"video_id": video_id, "title": "Learn Python", "description": "A course", "channel_name": "CodeCh",
                "duration_seconds": 100, "category_id": "27", "tags": ["python"], "transcript_text": ""}

    monkeypatch.setattr(yc, "fetch_video_metadata", metadata)
    app = FastAPI()
    app.include_router(yc.router)
    return TestClient(app, raise_server_exceptions=False)


def detected(video_id=VIDEO, watch=25, **extra):
    body = {"url": f"https://www.youtube.com/watch?v={video_id}", "video_id": video_id, "is_short": False,
            "watch_time_seconds": watch, "triggered_by": "watch_time", "timestamp": "2026-10-05T10:00:00Z"}
    body.update(extra)
    return body


def test_a_watched_video_is_saved_and_its_metadata_filled_in(client):
    """Shows: detection stores the row, then the background job fills title/channel from the Data API."""
    response = client.post("/ytc/video-detected", json=detected()).json()
    assert response["status"] == "saved" and response["is_short"] is False
    item = db.fetchall("SELECT title, source_id FROM memory_items")[0]
    assert item == {"title": "Learn Python", "source_id": VIDEO}
    assert db.fetchall("SELECT channel_name FROM youtube_metadata")[0]["channel_name"] == "CodeCh"
    assert db.fetchall("SELECT system_group_id FROM memory_items")[0]["system_group_id"] == 3        # entertainment until ENP decides


def test_shorts_are_recognised_from_the_url_not_from_what_the_client_claims(client):
    body = detected(SHORT, watch=12, url=f"https://www.youtube.com/shorts/{SHORT}", is_short=False)
    assert client.post("/ytc/video-detected", json=body).json()["is_short"] is True
    liar = detected(VIDEO, watch=12, is_short=True)                   # claims Short, URL says long: 12 s is too little
    assert client.post("/ytc/video-detected", json=liar).json()["reason"] == "intent_gate_failed"


@pytest.mark.parametrize("body, reason", [
    (detected(watch=2), "intent_gate_failed"),
    (detected("bad id", watch=60), "invalid_video_id"),
])
def test_discarded_videos(client, body, reason):
    assert client.post("/ytc/video-detected", json=body).json() == {"status": "discarded", "reason": reason}
    assert db.fetchall("SELECT COUNT(*) AS n FROM memory_items")[0]["n"] == 0


def test_capture_switched_off_discards_everything(client):
    from ste import settings_store

    settings_store.save_settings({"youtube_enabled": False})
    assert client.post("/ytc/video-detected", json=detected()).json()["reason"] == "youtube_capture_disabled"
    assert client.post("/ytc/heartbeat", json={"video_id": VIDEO, "watch_time_seconds": 5, "timestamp": "2026-10-05T10:00:00Z"}).json()["status"] == "discarded"
    assert client.post("/ytc/video-closed", json={"video_id": VIDEO, "final_watch_time_seconds": 5, "timestamp": "2026-10-05T10:00:00Z"}).json()["status"] == "discarded"
    assert client.post("/ytc/revisit-check", json={"video_id": VIDEO}).json() == {"is_revisit": False}


def test_heartbeats_raise_the_watch_time_and_closing_records_the_final_value(client):
    client.post("/ytc/video-detected", json=detected())
    client.post("/ytc/heartbeat", json={"video_id": VIDEO, "watch_time_seconds": 40, "timestamp": "2026-10-05T10:01:00Z"})
    client.post("/ytc/video-closed", json={"video_id": VIDEO, "final_watch_time_seconds": 90, "timestamp": "2026-10-05T10:02:00Z"})
    assert db.fetchall("SELECT watch_time_seconds FROM memory_engagement")[0]["watch_time_seconds"] == 90
    assert client.post("/ytc/heartbeat", json={"video_id": "bad", "watch_time_seconds": 1, "timestamp": "2026-10-05T10:00:00Z"}).json()["reason"] == "invalid_video_id"
    assert client.post("/ytc/video-closed", json={"video_id": "bad", "final_watch_time_seconds": 1, "timestamp": "2026-10-05T10:00:00Z"}).json()["reason"] == "invalid_video_id"
    assert client.post("/ytc/heartbeat", json={"video_id": VIDEO, "watch_time_seconds": -1, "timestamp": "2026-10-05T10:00:00Z"}).status_code == 422


def test_a_revisit_only_counts_after_the_server_confirmed_it(client):
    """Shows: triggered_by='revisit' cannot be faked; the server must have seen the video before."""
    fake = detected(watch=1, triggered_by="revisit")
    assert client.post("/ytc/video-detected", json=fake).json()["reason"] == "intent_gate_failed"
    assert client.post("/ytc/revisit-check", json={"video_id": VIDEO}).json() == {"is_revisit": False}      # first visit
    assert client.post("/ytc/revisit-check", json={"video_id": VIDEO}).json() == {"is_revisit": True}       # second visit
    assert client.post("/ytc/video-detected", json=fake).json()["status"] == "saved"
    assert client.post("/ytc/video-detected", json=fake).json()["reason"] == "intent_gate_failed"   # the confirmation works once only
    assert client.post("/ytc/revisit-check", json={"video_id": "bad"}).json() == {"is_revisit": False}


def test_storage_failure_is_a_500_and_a_metadata_failure_is_harmless(client, monkeypatch):
    async def broken(video_id):
        raise RuntimeError("api exploded")

    monkeypatch.setattr(yc, "fetch_video_metadata", broken)
    assert client.post("/ytc/video-detected", json=detected()).json()["status"] == "saved"
    monkeypatch.setattr(yc, "store_youtube_detection", lambda *a: (_ for _ in ()).throw(RuntimeError("db down")))
    assert client.post("/ytc/video-detected", json=detected("zzzzzzzzzzz")).status_code == 500


def test_a_redis_outage_counts_as_a_first_visit(monkeypatch):
    import asyncio

    async def broken(*a, **k):
        raise ConnectionError("down")

    monkeypatch.setattr(yc, "check_and_record_revisit_async", broken)
    assert asyncio.run(yc.check_revisit(VIDEO)) is False


# ── Data API client ───────────────────────────────────────────────────────────

class FakeYouTube:
    def __init__(self, response=None, error=None):
        self.response, self.error = response, error

    def videos(self):
        return self

    def list(self, **kwargs):
        self.kwargs = kwargs
        return self

    def execute(self):
        if self.error:
            raise self.error
        return self.response


class Resp:
    def __init__(self, status):
        self.status, self.reason = status, "reason"


API_ITEM = {"items": [{"snippet": {"title": "T", "description": "D", "channelTitle": "C", "channelId": "c1", "categoryId": "27",
                                   "tags": ["a"], "publishedAt": "2026-01-01"}, "contentDetails": {"duration": "PT2M"}}]}


def test_metadata_is_shaped_for_the_storage_engine(monkeypatch):
    import asyncio

    monkeypatch.setattr(yt, "get_youtube_client", lambda: FakeYouTube(API_ITEM))

    async def no_transcript(video_id):
        return "words"

    monkeypatch.setattr(yt, "fetch_video_transcript", no_transcript)
    metadata = asyncio.run(yt.fetch_video_metadata(VIDEO))
    assert metadata["duration_seconds"] == 120 and metadata["category_id"] == "27" and metadata["transcript_text"] == "words"
    assert metadata["channel_name"] == "C" and "is_short" not in metadata


@pytest.mark.parametrize("failure", [
    HttpError(Resp(403), b"quota"), HttpError(Resp(404), b""), HttpError(Resp(500), b""),
    yt.YouTubeAuthError("no token"), RuntimeError("boom"),
])
def test_every_kind_of_api_failure_gives_none(monkeypatch, failure):
    import asyncio

    monkeypatch.setattr(yt, "get_youtube_client", lambda: FakeYouTube(error=failure))
    assert asyncio.run(yt.fetch_video_metadata(VIDEO)) is None


def test_missing_items_and_invalid_ids_give_none(monkeypatch):
    import asyncio

    monkeypatch.setattr(yt, "get_youtube_client", lambda: FakeYouTube({"items": []}))
    assert asyncio.run(yt.fetch_video_metadata(VIDEO)) is None
    assert asyncio.run(yt.fetch_video_metadata("bad")) is None
    assert asyncio.run(yt.fetch_video_transcript("bad")) == ""


def test_transcript_fetch_tries_both_formats(monkeypatch):
    import asyncio

    import httpx

    calls = []

    def handler(request: httpx.Request):
        calls.append(str(request.url))
        if "fmt=json3" in str(request.url):
            return httpx.Response(200, text="")
        return httpx.Response(200, text="<transcript><text>Hello world</text></transcript>")

    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient
    monkeypatch.setattr(yt.httpx, "AsyncClient", lambda **kw: real_client(transport=transport, **kw))
    assert asyncio.run(yt.fetch_video_transcript(VIDEO)) == "Hello world"
    assert len(calls) == 2

    monkeypatch.setattr(yt.httpx, "AsyncClient", lambda **kw: real_client(transport=httpx.MockTransport(lambda r: httpx.Response(500)), **kw))
    assert asyncio.run(yt.fetch_video_transcript(VIDEO)) == ""

    def raising(request):
        raise httpx.ConnectError("offline")

    monkeypatch.setattr(yt.httpx, "AsyncClient", lambda **kw: real_client(transport=httpx.MockTransport(raising), **kw))
    assert asyncio.run(yt.fetch_video_transcript(VIDEO)) == ""
    monkeypatch.setattr(yt.httpx, "AsyncClient", lambda **kw: real_client(transport=httpx.MockTransport(lambda r: httpx.Response(200, text='{"events": 5}')), **kw))
    assert asyncio.run(yt.fetch_video_transcript(VIDEO)) == ""


# ── Sign-in ───────────────────────────────────────────────────────────────────

class Creds:
    def __init__(self, valid=True, expired=False, refresh_token="r", refresh_error=None):
        self.valid, self.expired, self.refresh_token, self.refresh_error = valid, expired, refresh_token, refresh_error

    def refresh(self, request):
        if self.refresh_error:
            raise self.refresh_error
        self.valid = True

    def to_json(self):
        return json.dumps({"token": "t"})


@pytest.fixture
def token(tmp_path, monkeypatch):
    path = tmp_path / "token_youtube.json"
    monkeypatch.setattr(yt, "TOKEN_PATH", path)
    monkeypatch.setattr(yt, "_creds", None)
    return path


def test_stored_valid_token_is_cached(token, monkeypatch):
    token.write_text("{}")
    monkeypatch.setattr(yt.Credentials, "from_authorized_user_info", lambda info, scopes: Creds())
    first = yt._load_credentials()
    assert yt._load_credentials() is first


def test_expired_token_is_refreshed_and_failures_raise_auth_error(token, monkeypatch):
    token.write_text("{}")
    monkeypatch.setattr(yt.Credentials, "from_authorized_user_info", lambda info, scopes: Creds(valid=False, expired=True))
    yt._load_credentials()
    assert json.loads(token.read_text()) == {"token": "t"}
    monkeypatch.setattr(yt, "_creds", None)
    monkeypatch.setattr(yt.Credentials, "from_authorized_user_info", lambda info, scopes: Creds(valid=False, expired=True, refresh_error=RuntimeError("revoked")))
    with pytest.raises(yt.YouTubeAuthError):
        yt._load_credentials()
    monkeypatch.setattr(yt.Credentials, "from_authorized_user_info", lambda info, scopes: Creds(valid=False, expired=False, refresh_token=None))
    with pytest.raises(yt.YouTubeAuthError):
        yt._load_credentials()
    monkeypatch.setattr(yt.Credentials, "from_authorized_user_info", lambda info, scopes: (_ for _ in ()).throw(ValueError("corrupt")))
    with pytest.raises(yt.YouTubeAuthError):
        yt._load_credentials()
    token.unlink()
    with pytest.raises(yt.YouTubeAuthError):
        yt._load_credentials()


def test_client_is_built_from_the_credentials_and_sign_in_is_safe(token, monkeypatch):
    monkeypatch.setattr(yt, "_load_credentials", lambda: Creds())
    monkeypatch.setattr(yt, "build", lambda name, version, credentials: (name, version))
    assert yt.get_youtube_client() == ("youtube", "v3")

    opened = []

    class Flow:
        def run_local_server(self, port):
            opened.append(port)
            return Creds()

    monkeypatch.setattr(yt.InstalledAppFlow, "from_client_secrets_file", lambda path, scopes: Flow())
    monkeypatch.setattr(yt, "_load_credentials", lambda: Creds())
    yt.sign_in_youtube()
    assert opened == []
    monkeypatch.setattr(yt, "_load_credentials", lambda: (_ for _ in ()).throw(yt.YouTubeAuthError("none")))
    yt.sign_in_youtube()
    assert opened == [0] and token.exists()
    monkeypatch.setattr(yt.InstalledAppFlow, "from_client_secrets_file", lambda path, scopes: (_ for _ in ()).throw(FileNotFoundError()))
    yt.sign_in_youtube()
    monkeypatch.setattr(yt, "_load_credentials", lambda: (_ for _ in ()).throw(RuntimeError("weird")))
    yt.sign_in_youtube()
