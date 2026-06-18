"""
RSS feed collector for the ingestion service.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from xml.etree import ElementTree as ET

import aiohttp

from libs.common import NewsEvent, get_logger
from services.ingestion.sources.base import (
    DEFAULT_NEWS_SYMBOLS,
    NewsFetcher,
    coerce_datetime,
    extract_symbols,
    normalize_symbols,
)

_RSS_ITEM_FIELDS = ("title", "description", "link", "author", "pubDate", "guid")
_ATOM_ENTRY_FIELDS = ("title", "summary", "content", "link", "author", "updated", "published")


@dataclass(frozen=True)
class RssFeed:
    url: str
    source: str
    symbol_list: Sequence[str] = DEFAULT_NEWS_SYMBOLS
    symbol_map: Mapping[str, str] = field(default_factory=dict)


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def _child_text(element: ET.Element, names: Sequence[str]) -> str | None:
    for child in element:
        if _local_name(child.tag) in names and child.text and child.text.strip():
            return child.text.strip()
    return None


def _descendant_text(element: ET.Element, names: Sequence[str]) -> str | None:
    for descendant in element.iter():
        if descendant is element:
            continue
        if _local_name(descendant.tag) in names and descendant.text and descendant.text.strip():
            return descendant.text.strip()
    return None


def _child_link(element: ET.Element) -> str | None:
    for child in element:
        if _local_name(child.tag) != "link":
            continue
        href = child.attrib.get("href")
        if href:
            return href
        if child.text and child.text.strip():
            return child.text.strip()
    return None


def _first_text(*values: str | None) -> str | None:
    for value in values:
        if value and value.strip():
            return value.strip()
    return None


def _parse_feed_entries(xml_payload: str | bytes) -> list[ET.Element]:
    root = ET.fromstring(xml_payload)
    root_name = _local_name(root.tag)
    if root_name == "feed":
        return [entry for entry in root if _local_name(entry.tag) == "entry"]
    return [item for item in root.iter() if _local_name(item.tag) == "item"]


def _normalize_rss_entry(
    entry: ET.Element,
    *,
    source: str,
    symbol_list: Sequence[str],
    symbol_map: Mapping[str, str],
) -> NewsEvent | None:
    title = _first_text(
        _child_text(entry, ("title",)),
        _descendant_text(entry, ("title",)),
    )
    body = _first_text(
        _child_text(entry, ("description", "summary")),
        _descendant_text(entry, ("encoded", "content")),
        title,
    )
    if title is None and body is None:
        return None

    link = _first_text(_child_link(entry), _descendant_text(entry, ("link",)))
    author = _first_text(
        _child_text(entry, ("author", "creator")),
        _descendant_text(entry, ("author", "creator")),
    )
    ts_value = _first_text(
        _child_text(entry, ("pubdate", "updated", "published", "created")),
        _descendant_text(entry, ("pubdate", "updated", "published", "created")),
    )
    ts = coerce_datetime(ts_value) if ts_value is not None else None
    symbols = normalize_symbols(
        extract_symbols(title or "", body or "", symbol_list=symbol_list, symbol_map=symbol_map)
    )

    return NewsEvent(
        source=source,
        title=title or body or source,
        body=body or title or source,
        url=link,
        symbols=symbols,
        author=author,
        ts=ts or datetime.now(tz=UTC),
    )


async def _fetch_rss_text(url: str) -> str:
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.text()


class RssCollector:
    """Poll one or more RSS feeds and normalize them into ``NewsEvent``."""

    def __init__(
        self,
        feeds: Sequence[RssFeed],
        *,
        fetcher: NewsFetcher | None = None,
    ) -> None:
        self.feeds = list(feeds)
        self._fetcher = fetcher or _fetch_rss_text
        self._log = get_logger(__name__)
        self.name = "rss"

    async def poll_once(self) -> list[NewsEvent]:
        events: list[NewsEvent] = []
        for feed in self.feeds:
            payload = await self._fetcher(feed.url)
            entries = _parse_feed_entries(payload)
            self._log.info("ingestion.rss_polled", feed=feed.url, entries=len(entries))
            for entry in entries:
                event = _normalize_rss_entry(
                    entry,
                    source=feed.source,
                    symbol_list=feed.symbol_list,
                    symbol_map=feed.symbol_map,
                )
                if event is not None:
                    events.append(event)
        return events
