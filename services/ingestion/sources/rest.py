"""
REST/social collector for HN and Reddit-like feeds.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urljoin

import aiohttp

from libs.common import NewsEvent, get_logger
from services.ingestion.sources.base import (
    DEFAULT_NEWS_SYMBOLS,
    NewsFetcher,
    coerce_datetime,
    extract_symbols,
    normalize_symbols,
)


@dataclass(frozen=True)
class SocialFeed:
    url: str
    source: str
    symbol_list: Sequence[str] = DEFAULT_NEWS_SYMBOLS
    symbol_map: Mapping[str, str] = field(default_factory=dict)
    base_url: str | None = None


def _first_value(payload: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


def _extract_posts(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, Mapping)]

    if not isinstance(payload, Mapping):
        return []

    if isinstance(payload.get("hits"), list):
        return [item for item in payload["hits"] if isinstance(item, Mapping)]

    if isinstance(payload.get("items"), list):
        return [item for item in payload["items"] if isinstance(item, Mapping)]

    data = payload.get("data")
    if isinstance(data, Mapping) and isinstance(data.get("children"), list):
        children: list[Mapping[str, Any]] = []
        for child in data["children"]:
            if not isinstance(child, Mapping):
                continue
            nested = child.get("data")
            if isinstance(nested, Mapping):
                children.append(nested)
        return children

    if isinstance(payload.get("children"), list):
        children = []
        for child in payload["children"]:
            if isinstance(child, Mapping):
                nested = child.get("data")
                children.append(nested if isinstance(nested, Mapping) else child)
        return children

    return [payload]


def _normalize_post(
    post: Mapping[str, Any],
    *,
    source: str,
    symbol_list: Sequence[str],
    symbol_map: Mapping[str, str],
    base_url: str | None,
) -> NewsEvent | None:
    title = _first_value(post, ("title", "story_title", "name", "headline"))
    body = _first_value(post, ("selftext", "story_text", "text", "body", "summary", "description"))
    if title is None and body is None:
        return None

    url_value = _first_value(post, ("url", "link", "story_url", "permalink"))
    if isinstance(url_value, str) and base_url and url_value.startswith("/"):
        url_value = urljoin(base_url, url_value)

    author = _first_value(post, ("author", "username", "user", "display_name"))
    ts_value = _first_value(
        post,
        ("created_utc", "created_at", "published_at", "published", "timestamp", "time"),
    )
    ts: datetime | None = None
    if ts_value is not None:
        ts = coerce_datetime(ts_value)
    symbols = normalize_symbols(
        extract_symbols(title or "", body or "", symbol_list=symbol_list, symbol_map=symbol_map)
    )

    return NewsEvent(
        source=source,
        title=title or body or source,
        body=body or title or source,
        url=url_value if isinstance(url_value, str) else None,
        symbols=symbols,
        author=author if isinstance(author, str) else None,
        ts=ts or datetime.now(tz=UTC),
    )


async def _fetch_rest_json(url: str) -> Any:
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.json()


class RestSocialCollector:
    """Poll a HN/Reddit-style JSON endpoint and normalize it into news events."""

    def __init__(
        self,
        feeds: Sequence[SocialFeed],
        *,
        fetcher: NewsFetcher | None = None,
    ) -> None:
        self.feeds = list(feeds)
        self._fetcher = fetcher or _fetch_rest_json
        self._log = get_logger(__name__)
        self.name = "rest"

    async def poll_once(self) -> list[NewsEvent]:
        events: list[NewsEvent] = []
        for feed in self.feeds:
            payload = await self._fetcher(feed.url)
            posts = _extract_posts(payload)
            self._log.info("ingestion.rest_polled", feed=feed.url, entries=len(posts))
            for post in posts:
                event = _normalize_post(
                    post,
                    source=feed.source,
                    symbol_list=feed.symbol_list,
                    symbol_map=feed.symbol_map,
                    base_url=feed.base_url,
                )
                if event is not None:
                    events.append(event)
        return events
