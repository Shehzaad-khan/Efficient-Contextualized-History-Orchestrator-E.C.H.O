from backend.local_store import append_record
from backend.retrieval import local_query_fallback


def test_local_query_fallback_returns_records(tmp_path):
    append_record("chrome", {"url": "https://example.com", "title": "Example page"}, tmp_path / "local.db")
    append_record("chrome", {"url": "https://example.org", "title": "Another page"}, tmp_path / "local.db")

    result = local_query_fallback("example", db_path=tmp_path / "local.db")

    assert result["result_count"] >= 1
    assert "example" in result["final_answer"].lower()
