from backend.intake_api import _save_source_record


def test_save_source_record_returns_structured_record():
    result = _save_source_record(
        "chrome",
        {"url": "https://example.com/echo", "title": "Echo page", "text": "Example content"},
    )

    assert result["status"] == "ok"
    assert result["source"] == "chrome"
    assert "record" in result
    assert result["record"]["source"] == "chrome"
