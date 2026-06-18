"""
Time-series store port: Apache Druid for tick data and indicator storage.

Public API
----------
TimeSeriesStore         — Protocol (interface)
InMemoryTimeSeriesStore — In-memory fake for unit tests.
DruidClient             — Thin wrapper over Druid's HTTP SQL endpoint (real).
get_timeseries_store()  — Factory.
"""

from __future__ import annotations

import copy
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "TimeSeriesStore",
    "InMemoryTimeSeriesStore",
    "DruidClient",
    "get_timeseries_store",
]


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class TimeSeriesStore(Protocol):
    async def ingest(self, rows: list[dict[str, Any]]) -> None:
        ...

    async def query_sql(self, sql: str) -> list[dict[str, Any]]:
        ...

    async def latest(self, symbol: str) -> dict[str, Any] | None:
        ...

    async def history(
        self,
        symbol: str,
        frm: datetime,
        to: datetime,
    ) -> list[dict[str, Any]]:
        ...

    async def count(self, table: str = "ticks") -> int:
        ...


# ---------------------------------------------------------------------------
# InMemoryTimeSeriesStore
# ---------------------------------------------------------------------------


class InMemoryTimeSeriesStore:
    """
    In-memory time-series store for unit tests.

    Rows are stored per-table (default: "ticks").
    ``symbol`` and ``ts`` are the primary filter fields.
    ``ts`` may be a datetime, an ISO-8601 string, or a Unix-epoch float.
    """

    def __init__(self) -> None:
        # table_name → list of row dicts
        self._tables: dict[str, list[dict[str, Any]]] = {}

    # ------------------------------------------------------------------

    def _default_table(self, row: dict[str, Any]) -> str:
        return row.get("_table", "ticks")

    @staticmethod
    def _parse_ts(ts: Any) -> datetime | None:
        if ts is None:
            return None
        if isinstance(ts, datetime):
            return ts
        if isinstance(ts, (int, float)):
            return datetime.utcfromtimestamp(ts)
        if isinstance(ts, str):
            try:
                from dateutil.parser import parse as dt_parse

                return dt_parse(ts)
            except Exception:
                pass
        return None

    async def ingest(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            table = self._default_table(row)
            stored = copy.deepcopy(row)
            stored.pop("_table", None)
            self._tables.setdefault(table, []).append(stored)

    async def query_sql(self, sql: str) -> list[dict[str, Any]]:
        """
        Minimal SQL execution for tests.
        Only handles ``SELECT COUNT(*) FROM <table>`` and full-table SELECT.
        Real SQL is executed by DruidClient against the live endpoint.
        """
        sql_lower = sql.strip().lower()
        if "count(*)" in sql_lower:
            # Extract table name
            try:
                table = sql_lower.split("from")[1].strip().split()[0].strip('"')
            except IndexError:
                table = "ticks"
            n = len(self._tables.get(table, []))
            return [{"EXPR$0": n}]
        # Fall-through: return all rows from first matching table
        for table, rows in self._tables.items():
            if table in sql_lower:
                return list(rows)
        return []

    async def latest(self, symbol: str) -> dict[str, Any] | None:
        rows = self._tables.get("ticks", [])
        matching = [r for r in rows if r.get("symbol") == symbol]
        if not matching:
            return None
        # Return the row with the most-recent ts
        def ts_key(r: dict[str, Any]) -> Any:
            parsed = self._parse_ts(r.get("ts"))
            return parsed or datetime.min

        return max(matching, key=ts_key)

    async def history(
        self,
        symbol: str,
        frm: datetime,
        to: datetime,
    ) -> list[dict[str, Any]]:
        rows = self._tables.get("ticks", [])
        result = []
        for r in rows:
            if r.get("symbol") != symbol:
                continue
            ts = self._parse_ts(r.get("ts"))
            if ts is None:
                continue
            # Strip timezone for naive comparison
            ts_naive = ts.replace(tzinfo=None) if ts.tzinfo else ts
            frm_naive = frm.replace(tzinfo=None) if frm.tzinfo else frm
            to_naive = to.replace(tzinfo=None) if to.tzinfo else to
            if frm_naive <= ts_naive <= to_naive:
                result.append(r)
        return result

    async def count(self, table: str = "ticks") -> int:
        return len(self._tables.get(table, []))


# ---------------------------------------------------------------------------
# DruidClient (real — import-guarded)
# ---------------------------------------------------------------------------


class DruidClient:
    """
    Thin HTTP client for Apache Druid's SQL endpoint.

    POST /druid/v2/sql — query
    POST /druid/v2/indexer/v1/task — native ingest spec (simplified)

    Integration tests only — skip without live Druid.
    """

    def __init__(self, url: str) -> None:
        self._base_url = url.rstrip("/")

    async def ingest(self, rows: list[dict[str, Any]]) -> None:
        """
        Submit rows to Druid via an inline-data ingestion spec.
        For a real deployment this would POST a native batch spec.
        """
        import json

        import httpx

        spec = {
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "ticks",
                    "timestampSpec": {"column": "ts", "format": "auto"},
                    "dimensionsSpec": {"dimensions": list((rows[0] if rows else {}).keys())},
                    "granularitySpec": {"rollup": False},
                },
                "ioConfig": {
                    "type": "index_parallel",
                    "inputSource": {
                        "type": "inline",
                        "data": "\n".join(json.dumps(r) for r in rows),
                    },
                    "inputFormat": {"type": "json"},
                },
            },
        }
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self._base_url}/druid/indexer/v1/task",
                json=spec,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()

    async def query_sql(self, sql: str) -> list[dict[str, Any]]:
        import httpx

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self._base_url}/druid/v2/sql",
                json={"query": sql},
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            return resp.json()

    async def latest(self, symbol: str) -> dict[str, Any] | None:
        sql = (
            f"SELECT * FROM ticks WHERE symbol = '{symbol}' "
            "ORDER BY ts DESC LIMIT 1"
        )
        rows = await self.query_sql(sql)
        return rows[0] if rows else None

    async def history(
        self,
        symbol: str,
        frm: datetime,
        to: datetime,
    ) -> list[dict[str, Any]]:
        sql = (
            f"SELECT * FROM ticks WHERE symbol = '{symbol}' "
            f"AND ts >= '{frm.isoformat()}' AND ts <= '{to.isoformat()}' "
            "ORDER BY ts ASC"
        )
        return await self.query_sql(sql)

    async def count(self, table: str = "ticks") -> int:
        sql = f'SELECT COUNT(*) FROM "{table}"'
        rows = await self.query_sql(sql)
        if rows:
            first = rows[0]
            return next(iter(first.values()), 0)
        return 0


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def get_timeseries_store(settings: Any = None) -> TimeSeriesStore:
    """
    Return InMemoryTimeSeriesStore when DRUID_URL is the default placeholder,
    else return DruidClient.
    """
    if settings is None:
        from libs.common.config import get_settings

        settings = get_settings()

    druid_url: str = settings.druid_url or ""
    if not druid_url or druid_url == "http://localhost:8888":
        return InMemoryTimeSeriesStore()
    return DruidClient(druid_url)
