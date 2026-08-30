import json

from backend.local_store import append_record


def test_append_record_persists_payload(tmp_path):
    target = tmp_path / "echo_ingestion.json"

    record = append_record("chrome", {"url": "https://example.com", "title": "Example"}, target)

    assert record["source"] == "chrome"
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["records"][-1]["source"] == "chrome"
    assert payload["records"][-1]["payload"]["url"] == "https://example.com"
