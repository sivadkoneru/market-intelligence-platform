"""
RSS feed collector for the ingestion service.
"""

from __future__ import annotations

import re
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

# Feed bodies are untrusted remote input.
MAX_FEED_BYTES = 5 * 1024 * 1024
MAX_ENTRIES_PER_FEED = 500

# ``xml.etree`` expands internal entities, so a DOCTYPE with nested entity
# declarations is a billion-laughs amplifier (four levels turns 5 lines into
# 100 MB). ``defusedxml`` is off the approved stack and the C ``XMLParser``
# does not expose its expat handle, so the declaration itself is refused
# instead — no RSS or Atom feed needs a DTD.
_DOCTYPE = re.compile(r"<!DOCTYPE", re.IGNORECASE)
_ROOT_ELEMENT_START = re.compile(r"<[A-Za-z_]")
_PROLOG_SCAN_LIMIT = 64 * 1024
# Processing instructions (``<?...?>``) and comments (``<!--...-->``) are the
# only things XML allows before the root element besides the DOCTYPE itself.
# Both can contain a ``<letter`` sequence (e.g. inside a comment's text), which
# would otherwise fool ``_ROOT_ELEMENT_START`` into treating the comment's
# interior as the root element and truncating the DOCTYPE scan before it.
_LEADING_MISC = re.compile(r"\A\s*(?:<\?.*?\?>|<!--.*?-->)", re.DOTALL)


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


def _strip_leading_misc(head: str) -> str:
    """Strip leading processing instructions and comments from *head*."""
    while True:
        match = _LEADING_MISC.match(head)
        if not match:
            return head
        head = head[match.end() :]


def _reject_doctype(xml_payload: str | bytes) -> None:
    """
    Raise if the document declares a DTD.

    Only the prolog — everything before the root element's start tag — is
    scanned, so a feed whose article text happens to mention ``<!DOCTYPE`` is
    still accepted. Leading processing instructions and comments are skipped
    before looking for the root element, so a ``<letter`` sequence inside a
    comment cannot be mistaken for the root element start and truncate the
    scan before a DOCTYPE that follows the comment.
    """
    head = xml_payload[:_PROLOG_SCAN_LIMIT]
    if isinstance(head, bytes):
        head = head.decode("utf-8", "replace")

    stripped = _strip_leading_misc(head)
    consumed = len(head) - len(stripped)
    root_start = _ROOT_ELEMENT_START.search(stripped)
    prolog = head[: consumed + root_start.start()] if root_start else head
    if _DOCTYPE.search(prolog):
        raise ValueError("feed declares a DOCTYPE; refusing to parse untrusted DTD")


def _payload_bytes(xml_payload: str | bytes) -> int:
    """Size of *xml_payload* in bytes — ``len()`` of a ``str`` counts characters."""
    if isinstance(xml_payload, str):
        return len(xml_payload.encode("utf-8"))
    return len(xml_payload)


def _parse_feed_entries(xml_payload: str | bytes) -> list[ET.Element]:
    if _payload_bytes(xml_payload) > MAX_FEED_BYTES:
        raise ValueError(f"feed payload exceeds {MAX_FEED_BYTES} bytes")
    _reject_doctype(xml_payload)

    root = ET.fromstring(xml_payload)
    root_name = _local_name(root.tag)
    if root_name == "feed":
        entries = [entry for entry in root if _local_name(entry.tag) == "entry"]
    else:
        entries = [item for item in root.iter() if _local_name(item.tag) == "item"]
    return entries[:MAX_ENTRIES_PER_FEED]


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


FEED_CHUNK_BYTES = 64 * 1024


async def _fetch_rss_text(url: str) -> str:
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session, session.get(url) as response:
        response.raise_for_status()
        # Read with a hard cap rather than response.text(): a hostile or
        # broken feed must not be able to stream unbounded bytes into memory.
        #
        # Chunked rather than a single ``content.read(n)``: that call returns
        # whatever is already buffered, so any feed arriving in more than one
        # chunk was silently truncated mid-document and then failed to parse.
        # The cap is re-checked per chunk, so at most one chunk overshoots it.
        buffer = bytearray()
        async for chunk in response.content.iter_chunked(FEED_CHUNK_BYTES):
            buffer.extend(chunk)
            if len(buffer) > MAX_FEED_BYTES:
                raise ValueError(f"feed at {url} exceeds {MAX_FEED_BYTES} bytes")
        return bytes(buffer).decode(response.charset or "utf-8", "replace")


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
