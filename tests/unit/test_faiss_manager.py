"""Unit tests for ste/faiss_manager.py with a real FAISS index (small vectors, temp files, DB write faked)."""
import json

import numpy as np
import pytest

faiss = pytest.importorskip("faiss")

from ste import faiss_manager as fm  # noqa: E402


def unit(seed, dim=8):
    vec = np.random.default_rng(seed).normal(size=dim).astype(np.float32)
    return vec / np.linalg.norm(vec)


@pytest.fixture
def manager(tmp_path, monkeypatch):
    mgr = fm.FAISSManager(tmp_path / "echo.index", dimension=8, embedding_version="test-v1")
    rows = []
    monkeypatch.setattr(mgr, "_upsert_embedding_row", lambda memory_id, text: rows.append(memory_id))
    mgr.rows = rows
    return mgr


def test_cosine_handles_zero_vectors():
    assert fm.cosine(np.zeros(3), np.ones(3)) == 0.0
    assert fm.cosine(np.array([1.0, 0]), np.array([1.0, 0])) == pytest.approx(1.0)


def test_add_search_returns_the_nearest_first_with_cosine_scores(manager):
    for i in range(5):
        assert manager.add(f"m{i}", unit(i)) is True
    results = manager.search_all(unit(3), k=3)
    assert results[0][0] == "m3"
    assert results[0][1] == pytest.approx(1.0, abs=1e-5)
    assert [score for _, score in results] == sorted([score for _, score in results], reverse=True)
    assert manager.rows == ["m0", "m1", "m2", "m3", "m4"]


def test_re_adding_an_item_replaces_its_vector(manager):
    manager.add("a", unit(1))
    assert manager.add("a", unit(2)) is False
    assert manager.search_all(unit(2), k=1)[0] == ("a", pytest.approx(1.0, abs=1e-5))
    assert manager.get_vector("a") == pytest.approx(unit(2))
    assert manager.get_vector("missing") is None


def test_wrong_dimension_is_refused(manager):
    with pytest.raises(ValueError, match="dimension"):
        manager.add("a", np.ones(5, dtype=np.float32))


def test_empty_index_returns_no_hits(manager):
    assert manager.search_all(unit(1), k=5) == []


def test_remove_many_drops_vectors_and_ignores_unknown_ids(manager):
    for i in range(3):
        manager.add(f"m{i}", unit(i))
    assert manager.remove_many(["m1", "nope"]) == 1
    assert manager.remove_many(["nope"]) == 0
    ids = [memory_id for memory_id, _ in manager.search_all(unit(0), k=10)]
    assert sorted(ids) == ["m0", "m2"]


def test_save_and_load_round_trip(manager, tmp_path):
    for i in range(4):
        manager.add(f"m{i}", unit(i))
    path = manager.save_index()
    assert path.exists() and (tmp_path / "echo.index.meta.json").exists()
    fresh = fm.FAISSManager(path, dimension=8, embedding_version="test-v1")
    fresh.load_index()
    assert fresh.search_all(unit(2), k=1)[0][0] == "m2"
    assert fresh.get_vector("m1") == pytest.approx(unit(1), abs=1e-6)


def test_load_with_no_files_starts_empty(tmp_path):
    mgr = fm.FAISSManager(tmp_path / "none.index", dimension=8, embedding_version="test-v1")
    mgr.load_index()
    assert mgr.search_all(unit(1), 3) == []


def test_load_refuses_an_index_built_with_another_model(manager, tmp_path):
    """Shows (CLAUDE.md §3): FAISSManager.load_index refuses an index of another embedding version."""
    manager.add("a", unit(1))
    path = manager.save_index()
    other = fm.FAISSManager(path, dimension=8, embedding_version="other-model")
    with pytest.raises(ValueError, match="embedding_version"):
        other.load_index()


def test_load_refuses_incomplete_or_mismatched_files(manager, tmp_path):
    manager.add("a", unit(1))
    path = manager.save_index()
    meta = tmp_path / "echo.index.meta.json"
    data = json.loads(meta.read_text())
    data["memory_ids"] = ["a", "b"]
    meta.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="mismatch"):
        fm.FAISSManager(path, dimension=8, embedding_version="test-v1").load_index()
    meta.unlink()
    with pytest.raises(ValueError, match="incomplete"):
        fm.FAISSManager(path, dimension=8, embedding_version="test-v1").load_index()


def test_reset_empties_the_index(manager):
    manager.add("a", unit(1))
    manager.reset()
    assert manager.get_vector("a") is None


def test_get_manager_loads_once_and_returns_the_shared_instance(tmp_path, monkeypatch):
    monkeypatch.setattr(fm, "_default_manager", None)
    first = fm.get_manager(tmp_path / "shared.index")
    assert fm.get_manager() is first
    assert fm.get_manager(tmp_path / "shared.index") is first
