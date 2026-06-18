"""
Shared helpers for news/social collectors.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Protocol, runtime_checkable

from libs.common import NewsEvent

DEFAULT_NEWS_SYMBOLS: tuple[str, ...] = ("BTC", "ETH", "BTCUSDT", "ETHUSD")

NewsFetcher = Callable[[str], Awaitable[Any]]


@runtime_checkable
class NewsCollector(Protocol):
    """Protocol implemented by all offline-safe news collectors."""

    name: str

    async def poll_once(self) -> list[NewsEvent]:
        ...


def coerce_datetime(value: Any) -> datetime:
    """Convert common RSS/REST timestamp shapes into a UTC datetime."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(float(value), tz=UTC)
    elif isinstance(value, str):
        normalized = value.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            dt = parsedate_to_datetime(value)
    else:
        raise ValueError(f"Unsupported timestamp value: {value!r}")

    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def normalize_symbols(symbols: Sequence[str]) -> list[str]:
    """Return uppercase symbols with duplicates removed, preserving order."""
    seen: set[str] = set()
    normalized: list[str] = []
    for symbol in symbols:
        candidate = symbol.upper()
        if candidate not in seen:
            seen.add(candidate)
            normalized.append(candidate)
    return normalized


def extract_symbols(
    *texts: str,
    symbol_list: Sequence[str] = DEFAULT_NEWS_SYMBOLS,
    symbol_map: Mapping[str, str] | None = None,
) -> list[str]:
    """
    Extract a small symbol list from free text.

    ``symbol_list`` provides the canonical symbols to look for, and
    ``symbol_map`` lets callers map aliases like ``BITCOIN`` to ``BTC``.
    """
    combined = " ".join(text for text in texts if text)
    if not combined:
        return []

    upper_text = combined.upper()
    candidates: list[tuple[str, str]] = []
    seen_candidates: set[str] = set()

    for symbol in symbol_list:
        candidate = symbol.upper()
        if candidate not in seen_candidates:
            seen_candidates.add(candidate)
            candidates.append((candidate, candidate))

    if symbol_map:
        for alias, canonical in symbol_map.items():
            alias_upper = alias.upper()
            canonical_upper = canonical.upper()
            if alias_upper not in seen_candidates:
                seen_candidates.add(alias_upper)
                candidates.append((alias_upper, canonical_upper))
            if canonical_upper not in seen_candidates:
                seen_candidates.add(canonical_upper)
                candidates.append((canonical_upper, canonical_upper))

    matches: list[str] = []
    for candidate, canonical in candidates:
        if re.search(rf"(?<![A-Z0-9]){re.escape(candidate)}(?![A-Z0-9])", upper_text):
            if canonical not in matches:
                matches.append(canonical)
    return matches


def hash_news_event(event: NewsEvent) -> str:
    """
    Produce a deterministic content hash for duplicate detection.

    The hash intentionally ignores ``event_id`` and logging context fields so
    that repeated polls of the same payload collapse to the same message ID.
    """
    payload = event.model_dump(
        mode="json",
        exclude={"event_id", "ts", "correlation_id", "trace_id"},
    )
    payload["symbols"] = normalize_symbols(payload.get("symbols", []))
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
