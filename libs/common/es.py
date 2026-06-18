"""
Search / vector store port: Elasticsearch for logs and RAG kNN vector search.

Public API
----------
SearchStore         — Protocol (interface)
InMemorySearchStore — In-memory fake with real cosine-similarity kNN ranking.
ElasticsearchStore  — Thin wrapper over ``elasticsearch.AsyncElasticsearch``.
get_search_store()  — Factory.
"""

from __future__ import annotations

import math
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "SearchStore",
    "InMemorySearchStore",
    "ElasticsearchStore",
    "get_search_store",
]


# ---------------------------------------------------------------------------
# Cosine similarity (pure Python fallback; numpy used when available)
# ---------------------------------------------------------------------------


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Return cosine similarity in [-1, 1]."""
    try:
        import numpy as np  # type: ignore

        va = np.array(a, dtype=float)
        vb = np.array(b, dtype=float)
        norm_a = np.linalg.norm(va)
        norm_b = np.linalg.norm(vb)
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return float(np.dot(va, vb) / (norm_a * norm_b))
    except ImportError:
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(y * y for y in b))
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class SearchStore(Protocol):
    async def index_document(
        self,
        index: str,
        doc_id: str,
        doc: dict[str, Any],
        vector: list[float] | None = None,
    ) -> None:
        ...

    async def knn_search(
        self,
        index: str,
        query_vector: list[float],
        k: int = 10,
    ) -> list[dict[str, Any]]:
        """Return up to *k* documents ranked by COSINE similarity (highest first).
        Each result dict includes the original document fields plus ``_score``."""
        ...

    async def index_log(self, index: str, log: dict[str, Any]) -> None:
        ...

    async def search(
        self, index: str, query: dict[str, Any]
    ) -> list[dict[str, Any]]:
        ...


# ---------------------------------------------------------------------------
# InMemorySearchStore
# ---------------------------------------------------------------------------


class InMemorySearchStore:
    """
    In-memory search store for unit tests.

    - Documents are stored per-index by doc_id.
    - kNN search uses real cosine similarity (via numpy if available, else pure Python).
    - Logs are stored in a separate flat list per index.
    """

    def __init__(self) -> None:
        # index → doc_id → {"_doc": dict, "_vector": list[float] | None}
        self._indices: dict[str, dict[str, dict[str, Any]]] = {}
        # index → list[dict]  (append-only log store)
        self._logs: dict[str, list[dict[str, Any]]] = {}

    async def index_document(
        self,
        index: str,
        doc_id: str,
        doc: dict[str, Any],
        vector: list[float] | None = None,
    ) -> None:
        self._indices.setdefault(index, {})[doc_id] = {
            "_doc": dict(doc),
            "_vector": vector,
        }

    async def knn_search(
        self,
        index: str,
        query_vector: list[float],
        k: int = 10,
    ) -> list[dict[str, Any]]:
        """
        Return up to *k* documents ranked by cosine similarity to *query_vector*.
        Documents without a stored vector are excluded.
        Result dicts contain all original fields plus ``_score`` and ``_id``.
        """
        store = self._indices.get(index, {})
        scored: list[tuple[float, str, dict[str, Any]]] = []
        for doc_id, entry in store.items():
            vec = entry.get("_vector")
            if vec is None:
                continue
            score = _cosine_similarity(query_vector, vec)
            scored.append((score, doc_id, entry["_doc"]))

        scored.sort(key=lambda t: t[0], reverse=True)
        results = []
        for score, doc_id, doc in scored[:k]:
            row = dict(doc)
            row["_score"] = score
            row["_id"] = doc_id
            results.append(row)
        return results

    async def index_log(self, index: str, log: dict[str, Any]) -> None:
        self._logs.setdefault(index, []).append(dict(log))

    async def search(
        self, index: str, query: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Minimal query support: returns all documents from *index*.
        The ``query`` dict is ignored in the fake (all docs returned).
        """
        store = self._indices.get(index, {})
        return [
            {**entry["_doc"], "_id": doc_id}
            for doc_id, entry in store.items()
        ]


# ---------------------------------------------------------------------------
# ElasticsearchStore (real — import-guarded)
# ---------------------------------------------------------------------------


class ElasticsearchStore:
    """
    Elasticsearch-backed store using ``elasticsearch.AsyncElasticsearch``.

    Integration tests only — skip without live Elasticsearch.
    """

    def __init__(self, url: str) -> None:
        from elasticsearch import AsyncElasticsearch  # type: ignore

        self._es = AsyncElasticsearch([url])

    async def index_document(
        self,
        index: str,
        doc_id: str,
        doc: dict[str, Any],
        vector: list[float] | None = None,
    ) -> None:
        body = dict(doc)
        if vector is not None:
            body["embedding"] = vector
        await self._es.index(index=index, id=doc_id, document=body)

    async def knn_search(
        self,
        index: str,
        query_vector: list[float],
        k: int = 10,
    ) -> list[dict[str, Any]]:
        resp = await self._es.search(
            index=index,
            knn={
                "field": "embedding",
                "query_vector": query_vector,
                "k": k,
                "num_candidates": k * 10,
            },
            size=k,
        )
        results = []
        for hit in resp["hits"]["hits"]:
            row = dict(hit["_source"])
            row["_score"] = hit["_score"]
            row["_id"] = hit["_id"]
            results.append(row)
        return results

    async def index_log(self, index: str, log: dict[str, Any]) -> None:
        await self._es.index(index=index, document=log)

    async def search(
        self, index: str, query: dict[str, Any]
    ) -> list[dict[str, Any]]:
        resp = await self._es.search(index=index, body=query)
        return [
            {**hit["_source"], "_id": hit["_id"], "_score": hit["_score"]}
            for hit in resp["hits"]["hits"]
        ]

    async def close(self) -> None:
        await self._es.close()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def get_search_store(settings: Any = None) -> SearchStore:
    """
    Return InMemorySearchStore when ELASTICSEARCH_URL is the default placeholder,
    else return ElasticsearchStore.
    """
    if settings is None:
        from libs.common.config import get_settings

        settings = get_settings()

    es_url: str = settings.elasticsearch_url or ""
    if not es_url or es_url == "http://localhost:9200":
        return InMemorySearchStore()
    return ElasticsearchStore(es_url)
