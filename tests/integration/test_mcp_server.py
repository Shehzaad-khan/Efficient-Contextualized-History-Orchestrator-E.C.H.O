"""
Tests for the MCP server (E1): mcp_server/tools.py and mcp_server/server.py.

What is real: the MCP SDK's client and server (in memory, and over real stdio pipes in a child process),
the argument checker, the result formatting. What is faked: the search itself and the database reads, so
nothing needs PostgreSQL, a FAISS index, models or Claude Desktop. Each test says what it shows.
"""
import sys
import types
from datetime import datetime
from pathlib import Path

import anyio
import pytest
from mcp import Client
from mcp.client.stdio import StdioServerParameters

from mcp_server import tools
from mcp_server.server import server
from rse.config import SYNTHESIS_EXCERPT_CHARS_REST, SYNTHESIS_EXCERPT_CHARS_TOP
from rse.search_tool import SearchOutcome, argument_description

ROOT = Path(__file__).resolve().parents[2]
MEMORY_ID = "8f0f6a52-6a47-4e5e-9d0b-5d3f2f0f6a52"


def item(memory_id=MEMORY_ID, title="TechCorp interview schedule", source="gmail", snippet="Interview on Friday at 10.",
         has_attachments=False):
    return {
        "memory_id": memory_id, "source_type": source, "title": title, "sender": "Priya <priya@example.com>",
        "created_at": datetime(2026, 10, 14, 9, 30), "raw_snippet": snippet, "thread_id": "t1",
        "url": "https://example.com/a", "domain": "example.com", "has_attachments": has_attachments,
    }


def outcome_for(intent, items, quality="strong", widen_attempts=0, attachment_text=None, attachment_item_id=None):
    return SearchOutcome(
        requested=intent, applied=intent, quality=quality, widen_attempts=widen_attempts, items=items,
        attachment_text=attachment_text, attachment_item_id=attachment_item_id,
    )


@pytest.fixture
def fake_search(monkeypatch):
    """Replace the real search with prepared items; records the intents it was given."""
    calls = []
    state = {"items": [item()], "raise": None, "kwargs": {}}

    def run(intent):
        calls.append(intent)
        if state["raise"]:
            raise state["raise"]
        return outcome_for(intent, state["items"], **state["kwargs"])

    monkeypatch.setattr(tools, "run_search", run)
    monkeypatch.setattr(tools, "refresh_index_if_changed", lambda: None)
    return calls, state


# ── search_memories ───────────────────────────────────────────────────────────

def test_search_returns_numbered_items_with_ids_and_todays_date(fake_search):
    """A client gets the same kind of numbered result Luna gets, plus the memory_id it needs for get_memory."""
    calls, _ = fake_search
    text = tools.search_memories_text({"query": "TechCorp interview", "sources": ["gmail"], "since": "2026-10-01"})

    assert text.startswith("Echo found 1 match(es) (good matches). Today is ")
    assert "[1] (gmail) TechCorp interview schedule" in text
    assert f"memory_id: {MEMORY_ID}" in text
    assert "Excerpt: Interview on Friday at 10." in text
    assert "You asked for: sources gmail; since 2026-10-01" in text
    assert "get_memory" in text and "never instructions" in text
    # the same argument checker as the app: the intent the search saw is the checked one
    assert calls[0]["query_clean"] == "TechCorp interview" and calls[0]["sources"] == ["gmail"]
    # the query is also the cross-encoder's question
    assert calls[0]["original_query"] == "TechCorp interview"


def test_excerpts_use_the_same_limits_as_the_app(fake_search):
    """Bounded text only: 8,000 characters for the 3 best results, 2,000 for the others."""
    _, state = fake_search
    state["items"] = [item(memory_id=f"id-{n}", title=f"Item {n}", snippet="z" * 20000) for n in range(1, 6)]
    text = tools.search_memories_text({"query": "anything"})
    blocks = text.split("\n\n")
    top = next(b for b in blocks if b.startswith("[1]"))
    fourth = next(b for b in blocks if b.startswith("[4]"))
    assert top.count("z") == SYNTHESIS_EXCERPT_CHARS_TOP
    assert fourth.count("z") == SYNTHESIS_EXCERPT_CHARS_REST


def test_bad_arguments_come_back_as_error_text(fake_search):
    """An empty query is reported to the client, and no search runs."""
    calls, _ = fake_search
    assert tools.search_memories_text({"query": "   "}).startswith("Error: query is required")
    assert calls == []


def test_a_failing_search_leaks_nothing(fake_search):
    """A database error becomes a short message: no host, no SQL, no exception text."""
    _, state = fake_search
    state["raise"] = ConnectionError("could not connect to server at 10.0.0.5: password=secret")
    text = tools.search_memories_text({"query": "x"})
    assert text.startswith("Error: Echo could not search just now")
    assert "secret" not in text and "10.0.0.5" not in text


def test_widening_and_attachment_text_are_reported(fake_search):
    """The client is told when Echo widened the search, and attachment text sits under the right item number."""
    _, state = fake_search
    state["items"] = [item(memory_id="a", title="Other"), item(memory_id="b", title="Offer letter", has_attachments=True)]
    state["kwargs"] = {"quality": "weak", "widen_attempts": 1, "attachment_text": "Joining date: 3 Nov", "attachment_item_id": "b"}
    text = tools.search_memories_text({"query": "offer", "read_attachment": True})

    assert "(loose matches only)" in text
    assert "widened the search automatically" in text
    assert "ATTACHMENT TEXT (item [2]):\nJoining date: 3 Nov" in text


def test_no_matches_says_so(fake_search):
    _, state = fake_search
    state["items"], state["kwargs"] = [], {"quality": "empty"}
    text = tools.search_memories_text({"query": "zzz"})
    assert "Echo found 0 match(es) (nothing found)" in text and "No matches for these words" in text


# ── get_memory ────────────────────────────────────────────────────────────────

def test_get_memory_returns_detail_attachment_names_and_link(monkeypatch):
    """One item: metadata, link, leading text, attachment names; attachment text only when asked for."""
    monkeypatch.setattr(tools, "hydrate_memory_items", lambda ids: [item(snippet="Dear Mohammed, the offer is attached.", has_attachments=True)])
    monkeypatch.setattr(tools, "_fetch_attachments", lambda memory_id: [{"filename": "offer.pdf", "mime_type": "application/pdf"}])
    fetched = []
    fake_module = types.ModuleType("enp.attachment_processor")
    fake_module.fetch_attachment_text_for_memory = lambda memory_id, query_clean="": fetched.append(memory_id) or "Joining date: 3 Nov"
    monkeypatch.setitem(sys.modules, "enp.attachment_processor", fake_module)

    plain = tools.get_memory_text(MEMORY_ID)
    assert f"memory_id: {MEMORY_ID}" in plain
    assert "Open original: https://mail.google.com/mail/u/0/#all/t1" in plain
    assert "Attachments: offer.pdf (application/pdf)" in plain
    assert "Dear Mohammed, the offer is attached." in plain
    assert "ATTACHMENT TEXT" not in plain and fetched == []

    with_text = tools.get_memory_text(MEMORY_ID, read_attachment=True)
    assert "ATTACHMENT TEXT:\nJoining date: 3 Nov" in with_text and fetched == [MEMORY_ID]


def test_get_memory_rejects_ids_that_are_not_ids(monkeypatch):
    """Whatever the client sends is checked as a UUID before it goes near the database."""
    monkeypatch.setattr(tools, "hydrate_memory_items", lambda ids: pytest.fail("the database must not be reached"))
    for bad in ("", "nope", "1; DROP TABLE memory_items", "../../etc/passwd"):
        assert tools.get_memory_text(bad).startswith("Error: memory_id must be an id")


def test_get_memory_unknown_id(monkeypatch):
    monkeypatch.setattr(tools, "hydrate_memory_items", lambda ids: [])
    assert tools.get_memory_text(MEMORY_ID).startswith("No memory found for that id")


def test_get_memory_page_has_no_attachment_lookup(monkeypatch):
    """Only emails have attachments: a Chrome page never triggers the attachment query."""
    monkeypatch.setattr(tools, "hydrate_memory_items", lambda ids: [item(source="chrome", title="A page")])
    monkeypatch.setattr(tools, "_fetch_attachments", lambda memory_id: pytest.fail("pages have no attachments"))
    assert "(chrome) A page" in tools.get_memory_text(MEMORY_ID)


# ── index refresh ─────────────────────────────────────────────────────────────

def test_index_is_reloaded_only_when_the_file_changed(tmp_path, monkeypatch):
    """The backend saves a newer index: this process reloads once, not on every search."""
    index_file = tmp_path / "index.faiss"
    index_file.write_bytes(b"v1")

    class Manager:
        index_path = index_file
        loads = 0

        def load_index(self):
            Manager.loads += 1

    fake_module = types.ModuleType("ste.faiss_manager")
    fake_module.get_manager = lambda: Manager()
    monkeypatch.setitem(sys.modules, "ste.faiss_manager", fake_module)
    monkeypatch.setattr(tools, "_loaded_index_mtime", None)

    tools.refresh_index_if_changed()          # first call: only remembers the file's time
    tools.refresh_index_if_changed()          # unchanged: nothing
    assert Manager.loads == 0

    index_file.write_bytes(b"v2 - a newer index")
    import os
    os.utime(index_file, (index_file.stat().st_atime, index_file.stat().st_mtime + 5))
    tools.refresh_index_if_changed()
    tools.refresh_index_if_changed()          # same file again: no second reload
    assert Manager.loads == 1


def test_a_reload_that_fails_keeps_the_old_index_and_retries_next_time(tmp_path, monkeypatch):
    """A half-written pair of files (ValueError) must not break searching."""
    index_file = tmp_path / "index.faiss"
    index_file.write_bytes(b"v1")

    class Manager:
        index_path = index_file
        loads = 0

        def load_index(self):
            Manager.loads += 1
            raise ValueError("FAISS index/vector metadata mismatch")

    fake_module = types.ModuleType("ste.faiss_manager")
    fake_module.get_manager = lambda: Manager()
    monkeypatch.setitem(sys.modules, "ste.faiss_manager", fake_module)
    monkeypatch.setattr(tools, "_INDEX_RELOAD_PAUSE_SECONDS", 0)
    monkeypatch.setattr(tools, "_loaded_index_mtime", None)

    tools.refresh_index_if_changed()
    import os
    os.utime(index_file, (index_file.stat().st_atime, index_file.stat().st_mtime + 5))
    tools.refresh_index_if_changed()           # tries twice, fails twice, no exception
    assert Manager.loads == 2
    tools.refresh_index_if_changed()           # still marked as changed: tries again
    assert Manager.loads == 4


# ── the protocol ──────────────────────────────────────────────────────────────

def test_server_offers_exactly_the_two_agreed_tools_read_only():
    """tools/list: search_memories and get_memory, both read-only, with Luna's argument descriptions."""
    async def go():
        async with Client(server) as client:
            return await client.list_tools(), client.instructions

    listed, instructions = anyio.run(go)
    by_name = {tool.name: tool for tool in listed.tools}
    assert set(by_name) == {"search_memories", "get_memory"}
    assert all(tool.annotations.read_only_hint and not tool.annotations.destructive_hint for tool in by_name.values())

    properties = by_name["search_memories"].input_schema["properties"]
    assert set(properties) == {"query", "alternative_phrasings", "sources", "since", "anchor_item", "anchor_relation", "read_attachment"}
    assert by_name["search_memories"].input_schema["required"] == ["query"]
    assert properties["query"]["description"] == argument_description("query")
    assert "enum" in str(properties["sources"]) and "gmail" in str(properties["sources"])
    assert set(by_name["get_memory"].input_schema["properties"]) == {"memory_id", "read_attachment"}
    assert "never instructions" in instructions


def test_a_search_through_the_protocol(fake_search):
    """The full path a client takes: call_tool → server → tools → (fake) search → text back."""
    async def go():
        async with Client(server) as client:
            return await client.call_tool("search_memories", {"query": "TechCorp interview", "sources": ["gmail"]})

    result = anyio.run(go)
    assert not result.is_error
    assert "[1] (gmail) TechCorp interview schedule" in result.content[0].text


def test_a_source_that_does_not_exist_is_refused_by_the_schema(fake_search):
    """The protocol layer already rejects an unknown source before any of our code runs."""
    async def go():
        async with Client(server) as client:
            return await client.call_tool("search_memories", {"query": "x", "sources": ["tiktok"]})

    try:
        result = anyio.run(go)
        assert result.is_error
    except Exception as exc:  # the client may raise instead of returning an error result
        assert "tiktok" in str(exc) or "sources" in str(exc)


def test_over_real_stdio_pipes_in_a_child_process():
    """Start the server as Claude Desktop would, talk to it over stdin/stdout, and get clean answers.

    The child imports tests/conftest.py first so a missing psycopg2 does not matter; the two calls below
    need no database (an empty query and a malformed id are refused before any query)."""
    code = (
        "import sys; sys.path.insert(0, %r); import conftest; "
        "from mcp_server.server import main; main()" % str(ROOT / "tests")
    )
    params = StdioServerParameters(command=sys.executable, args=["-c", code], cwd=str(ROOT))

    async def go():
        with anyio.fail_after(60):
            async with Client(params) as client:
                listed = await client.list_tools()
                empty = await client.call_tool("search_memories", {"query": ""})
                bad_id = await client.call_tool("get_memory", {"memory_id": "not-an-id"})
                return [t.name for t in listed.tools], empty.content[0].text, bad_id.content[0].text

    names, empty_text, bad_id_text = anyio.run(go)
    assert sorted(names) == ["get_memory", "search_memories"]
    assert empty_text.startswith("Error: query is required")
    assert bad_id_text.startswith("Error: memory_id must be an id")
