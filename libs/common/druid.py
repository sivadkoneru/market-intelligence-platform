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
import json
from collections import defaultdict
from datetime import UTC, datetime
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
    async def ingest(self, rows: list[dict[str, Any]]) -> None: ...

    async def query_sql(self, sql: str) -> list[dict[str, Any]]: ...

    async def latest(self, symbol: str) -> dict[str, Any] | None: ...

    async def history(
        self,
        symbol: str,
        frm: datetime,
        to: datetime,
        limit: int | None = None,
    ) -> list[dict[str, Any]]: ...

    async def latest_indicator(self, symbol: str) -> dict[str, Any] | None:
        """Return the most recent ``indicators`` row for *symbol*, or None."""
        ...

    async def count(self, table: str = "ticks") -> int: ...


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

    # Aware sentinel for rows whose ts is missing or unparsable, so they sort
    # last without raising against offset-aware timestamps.
    _OLDEST = datetime.min.replace(tzinfo=UTC)

    def __init__(self) -> None:
        # table_name → list of row dicts
        self._tables: dict[str, list[dict[str, Any]]] = {}

    # ------------------------------------------------------------------

    def _default_table(self, row: dict[str, Any]) -> str:
        return row.get("_table", "ticks")

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        """Normalise to timezone-aware UTC, treating naive values as UTC."""
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @classmethod
    def _parse_ts(cls, ts: Any) -> datetime | None:
        """
        Parse a row timestamp into timezone-aware UTC.

        Always returning an aware datetime keeps comparisons total: rows whose
        ``ts`` is missing sort against rows whose ``ts`` carries an offset
        without raising "can't compare offset-naive and offset-aware".
        """
        if ts is None:
            return None
        if isinstance(ts, datetime):
            return cls._as_utc(ts)
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(ts, tz=UTC)
        if isinstance(ts, str):
            try:
                from dateutil.parser import parse as dt_parse

                return cls._as_utc(dt_parse(ts))
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
        Handles table metadata, distinct symbols, ``SELECT COUNT(*) FROM <table>``,
        and full-table SELECT.
        Real SQL is executed by DruidClient against the live endpoint.
        """
        sql_clean = " ".join(sql.strip().split())
        sql_lower = sql_clean.lower()
        if "information_schema.tables" in sql_lower:
            has_table_name_filter = '"table_name" in' in sql_lower or " table_name in " in sql_lower
            return [
                {"TABLE_NAME": table}
                for table in sorted(self._tables)
                if f"'{table}'" in sql_lower or not has_table_name_filter
            ]
        if "select distinct" in sql_lower and "symbol" in sql_lower:
            try:
                table = sql_lower.split("from")[1].strip().split()[0].strip('"')
            except IndexError:
                return []
            symbols = {
                str(row["symbol"]) for row in self._tables.get(table, []) if row.get("symbol")
            }
            return [{"symbol": symbol} for symbol in sorted(symbols)]
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

        # Rows without a parsable ts sort last via an aware sentinel, so a
        # mixed batch cannot raise.
        return max(matching, key=lambda r: self._parse_ts(r.get("ts")) or self._OLDEST)

    async def history(
        self,
        symbol: str,
        frm: datetime,
        to: datetime,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        rows = self._tables.get("ticks", [])
        frm_utc = self._as_utc(frm)
        to_utc = self._as_utc(to)
        result = []
        for r in rows:
            if r.get("symbol") != symbol:
                continue
            ts = self._parse_ts(r.get("ts"))
            if ts is None:
                continue
            if frm_utc <= ts <= to_utc:
                result.append(r)
        if limit is not None:
            # Newest rows win when the window is truncated, matching the
            # ORDER BY __time DESC ... LIMIT that DruidClient pushes down.
            result.sort(key=lambda r: self._parse_ts(r.get("ts")) or self._OLDEST)
            result = result[-limit:]
        return result

    async def latest_indicator(self, symbol: str) -> dict[str, Any] | None:
        matching = [
            row for row in self._tables.get("indicators", []) if row.get("symbol") == symbol
        ]
        if not matching:
            return None
        return max(matching, key=lambda r: self._parse_ts(r.get("ts")) or self._OLDEST)

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

    Connection lifecycle
    --------------------
    One ``httpx.AsyncClient`` is created lazily and reused for the client's
    lifetime, mirroring how ``ServiceBusBus`` caches its senders and receivers.
    A client per call opened and tore down a TCP (and, against a TLS endpoint,
    a full handshake) connection for every tick ingested and every query
    served — the hottest paths in the platform. Call ``await client.close()``
    to release it; the API and stream services do this via ``close_backends``.

    Integration tests only — skip without live Druid.
    """

    DEFAULT_TIMEOUT_SECONDS = 30

    def __init__(self, url: str, *, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> None:
        self._base_url = url.rstrip("/")
        self._timeout = timeout
        self._client: Any = None

    def _get_client(self) -> Any:
        """Return the shared HTTP client, creating it on first use."""
        if self._client is None:
            import httpx

            self._client = httpx.AsyncClient(timeout=self._timeout)
        return self._client

    async def close(self) -> None:
        """Release the shared HTTP client. Safe to call when never used."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @staticmethod
    def _sql_literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    @staticmethod
    def _build_ingest_specs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []

        grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            table = row.get("_table", "ticks")
            clean_row = copy.deepcopy(row)
            clean_row.pop("_table", None)
            grouped_rows[table].append(clean_row)

        specs: list[dict[str, Any]] = []
        for table, table_rows in grouped_rows.items():
            dimensions = list((table_rows[0] if table_rows else {}).keys())
            specs.append(
                {
                    "type": "index_parallel",
                    "spec": {
                        "dataSchema": {
                            "dataSource": table,
                            "timestampSpec": {"column": "ts", "format": "auto"},
                            "dimensionsSpec": {"dimensions": dimensions},
                            "granularitySpec": {"rollup": False},
                        },
                        "ioConfig": {
                            "type": "index_parallel",
                            "appendToExisting": True,
                            "inputSource": {
                                "type": "inline",
                                "data": "\n".join(json.dumps(r) for r in table_rows),
                            },
                            "inputFormat": {"type": "json"},
                        },
                    },
                }
            )
        return specs

    async def ingest(self, rows: list[dict[str, Any]]) -> None:
        """
        Submit rows to Druid via an inline-data ingestion spec.
        For a real deployment this would POST a native batch spec.
        """
        specs = self._build_ingest_specs(rows)
        if not specs:
            return

        client = self._get_client()
        for spec in specs:
            resp = await client.post(
                f"{self._base_url}/druid/indexer/v1/task",
                json=spec,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()

    async def query_sql(self, sql: str) -> list[dict[str, Any]]:
        resp = await self._get_client().post(
            f"{self._base_url}/druid/v2/sql",
            json={"query": sql},
            headers={"Content-Type": "application/json"},
        )
        # Druid creates a datasource lazily on first ingest. Querying one
        # that does not exist yet returns 400 "Object '<table>' not found".
        # Treat that as an empty result — mirroring InMemoryTimeSeriesStore
        # on an unknown table — instead of surfacing it as a 500. Other 400s
        # (genuine SQL errors, e.g. a missing column) still raise.
        if resp.status_code == 400:
            try:
                message = str(resp.json().get("errorMessage", ""))
            except Exception:
                message = resp.text
            lowered = message.lower()
            if "object '" in lowered and "not found" in lowered:
                return []
        resp.raise_for_status()
        return resp.json()

    async def latest(self, symbol: str) -> dict[str, Any] | None:
        sql = (
            'SELECT * FROM "ticks" '
            f'WHERE "symbol" = {self._sql_literal(symbol)} '
            'ORDER BY "__time" DESC LIMIT 1'
        )
        rows = await self.query_sql(sql)
        return rows[0] if rows else None

    async def history(
        self,
        symbol: str,
        frm: datetime,
        to: datetime,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        where = (
            'SELECT * FROM "ticks" '
            f'WHERE "symbol" = {self._sql_literal(symbol)} '
            f'AND "__time" >= {self._sql_literal(frm.isoformat())} '
            f'AND "__time" <= {self._sql_literal(to.isoformat())} '
        )
        if limit is None:
            return await self.query_sql(where + 'ORDER BY "__time" ASC')

        # Bound the transfer at the source: an unbounded window otherwise
        # streams every tick for the symbol into API memory. Taking the newest
        # rows needs DESC, so the result is flipped back to ascending time.
        rows = await self.query_sql(where + f'ORDER BY "__time" DESC LIMIT {int(limit)}')
        return list(reversed(rows))

    async def latest_indicator(self, symbol: str) -> dict[str, Any] | None:
        """
        Return the newest ``indicators`` row for *symbol*.

        Pushed down rather than scanned: the API previously ran an unfiltered
        ``SELECT *`` over the whole indicators datasource and filtered in
        Python, so every cache miss pulled the entire table over HTTP.
        """
        sql = (
            'SELECT * FROM "indicators" '
            f'WHERE "symbol" = {self._sql_literal(symbol)} '
            'ORDER BY "__time" DESC LIMIT 1'
        )
        rows = await self.query_sql(sql)
        return rows[0] if rows else None

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
    from libs.common.config import is_default, resolve_settings

    druid_url: str = resolve_settings(settings).druid_url or ""
    if not druid_url or is_default("druid_url", druid_url):
        return InMemoryTimeSeriesStore()
    return DruidClient(druid_url)
