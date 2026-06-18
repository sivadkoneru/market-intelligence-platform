"""Tests for libs.common.es — InMemorySearchStore, cosine kNN, and factory."""

import math

import pytest

from libs.common.es import InMemorySearchStore, _cosine_similarity, get_search_store

# ---------------------------------------------------------------------------
# Cosine similarity utility
# ---------------------------------------------------------------------------


def test_cosine_same_vector():
    v = [1.0, 2.0, 3.0]
    assert math.isclose(_cosine_similarity(v, v), 1.0, abs_tol=1e-9)


def test_cosine_orthogonal_vectors():
    a = [1.0, 0.0]
    b = [0.0, 1.0]
    assert math.isclose(_cosine_similarity(a, b), 0.0, abs_tol=1e-9)


def test_cosine_opposite_vectors():
    a = [1.0, 0.0]
    b = [-1.0, 0.0]
    assert math.isclose(_cosine_similarity(a, b), -1.0, abs_tol=1e-9)


def test_cosine_zero_vector():
    a = [0.0, 0.0]
    b = [1.0, 2.0]
    assert _cosine_similarity(a, b) == 0.0


# ---------------------------------------------------------------------------
# InMemorySearchStore — index and basic search
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_index_and_search_returns_document():
    store = InMemorySearchStore()
    await store.index_document("idx", "doc1", {"title": "Hello world"})
    results = await store.search("idx", {})
    assert len(results) == 1
    assert results[0]["title"] == "Hello world"
    assert results[0]["_id"] == "doc1"


@pytest.mark.asyncio
async def test_search_empty_index():
    store = InMemorySearchStore()
    results = await store.search("empty", {})
    assert results == []


@pytest.mark.asyncio
async def test_index_log():
    store = InMemorySearchStore()
    await store.index_log("logs", {"level": "info", "msg": "started"})
    assert len(store._logs["logs"]) == 1
    assert store._logs["logs"][0]["msg"] == "started"


# ---------------------------------------------------------------------------
# kNN search — cosine ordering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_knn_returns_nearest_by_cosine():
    """
    Documents at 0°, 45°, 90° from query [1,0].
    Expected order: d0 (score=1.0) > d45 (≈0.707) > d90 (score=0.0).
    """
    store = InMemorySearchStore()
    await store.index_document("vec", "d0",  {"label": "d0"},  vector=[1.0, 0.0])
    await store.index_document("vec", "d45", {"label": "d45"}, vector=[1.0, 1.0])
    await store.index_document("vec", "d90", {"label": "d90"}, vector=[0.0, 1.0])

    query = [1.0, 0.0]
    results = await store.knn_search("vec", query, k=3)

    assert len(results) == 3
    ids = [r["_id"] for r in results]
    assert ids[0] == "d0"   # closest
    assert ids[2] == "d90"  # furthest


@pytest.mark.asyncio
async def test_knn_respects_k():
    store = InMemorySearchStore()
    for i in range(10):
        await store.index_document("v", f"d{i}", {"i": i}, vector=[float(i), 1.0])

    results = await store.knn_search("v", [5.0, 1.0], k=3)
    assert len(results) == 3


@pytest.mark.asyncio
async def test_knn_excludes_docs_without_vectors():
    store = InMemorySearchStore()
    await store.index_document("v", "no_vec", {"t": "text"})  # no vector
    await store.index_document("v", "with_vec", {"t": "vec"}, vector=[1.0, 0.0])

    results = await store.knn_search("v", [1.0, 0.0], k=10)
    ids = [r["_id"] for r in results]
    assert "no_vec" not in ids
    assert "with_vec" in ids


@pytest.mark.asyncio
async def test_knn_score_included_in_result():
    store = InMemorySearchStore()
    await store.index_document("v", "d1", {"x": 1}, vector=[1.0, 0.0])
    results = await store.knn_search("v", [1.0, 0.0], k=1)
    assert "_score" in results[0]
    assert math.isclose(results[0]["_score"], 1.0, abs_tol=1e-6)


@pytest.mark.asyncio
async def test_knn_ordering_hand_crafted_vectors():
    """
    Hand-built example where ranking is non-trivial:
    query  = [3, 4]  (unit direction ≈ 0.6, 0.8)
    doc A  = [1, 0]  → cosine ≈ 0.6
    doc B  = [3, 4]  → cosine = 1.0  (identical direction)
    doc C  = [0, 1]  → cosine = 0.8
    Expected ranking: B > C > A
    """
    store = InMemorySearchStore()
    await store.index_document("v", "A", {"name": "A"}, vector=[1.0, 0.0])
    await store.index_document("v", "B", {"name": "B"}, vector=[3.0, 4.0])
    await store.index_document("v", "C", {"name": "C"}, vector=[0.0, 1.0])

    results = await store.knn_search("v", [3.0, 4.0], k=3)
    ids = [r["_id"] for r in results]
    assert ids[0] == "B"
    assert ids[1] == "C"
    assert ids[2] == "A"


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def test_get_search_store_returns_in_memory_by_default():
    store = get_search_store()
    assert isinstance(store, InMemorySearchStore)
