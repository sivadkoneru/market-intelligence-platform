from datetime import UTC, datetime

import pytest

from libs.common import TOPIC_NEWS_RAW, InMemoryBus
from services.ingestion import (
    NewsPollingService,
    RestSocialCollector,
    RssCollector,
    RssFeed,
    SocialFeed,
    extract_symbols,
)

RSS_XML = """\
<rss version="2.0">
  <channel>
    <title>Market Updates</title>
    <item>
      <title>BTC and ETH rally on ETF optimism</title>
      <description>BTCUSDT and ETHUSD lead the move.</description>
      <link>https://example.com/rss/btc-eth</link>
      <author>alice</author>
      <pubDate>Mon, 01 Jan 2024 00:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

RSS_XML_WITHOUT_DATE = """\
<rss version="2.0">
  <channel>
    <item>
      <title>BTC no-date item</title>
      <description>ETH follows the move.</description>
      <link>https://example.com/rss/no-date</link>
    </item>
  </channel>
</rss>
"""


@pytest.mark.asyncio
async def test_rss_collector_parses_symbols_and_news_fields() -> None:
    calls: list[str] = []

    async def fetcher(url: str) -> str:
        calls.append(url)
        return RSS_XML

    collector = RssCollector(
        [
            RssFeed(
                url="https://example.com/feed.xml",
                source="example-rss",
                symbol_list=("BTC", "ETH", "BTCUSDT", "ETHUSD"),
            )
        ],
        fetcher=fetcher,
    )

    events = await collector.poll_once()

    assert calls == ["https://example.com/feed.xml"]
    assert len(events) == 1
    event = events[0]
    assert event.source == "example-rss"
    assert event.title == "BTC and ETH rally on ETF optimism"
    assert event.body == "BTCUSDT and ETHUSD lead the move."
    assert event.url == "https://example.com/rss/btc-eth"
    assert event.author == "alice"
    assert event.ts == datetime(2024, 1, 1, tzinfo=UTC)
    assert event.symbols == ["BTC", "ETH", "BTCUSDT", "ETHUSD"]


@pytest.mark.asyncio
async def test_rest_collector_parses_hn_and_reddit_like_payloads() -> None:
    payloads: dict[str, dict[str, object]] = {
        "https://example.com/hn": {
            "hits": [
                {
                    "title": "Bitcoin chatter picks up",
                    "story_text": "ETH and BTCUSD names show up in the thread.",
                    "url": "https://news.ycombinator.com/item?id=1",
                    "author": "hn-user",
                    "created_at": "2024-01-02T03:04:05Z",
                }
            ]
        },
        "https://example.com/reddit": {
            "data": {
                "children": [
                    {
                        "data": {
                            "title": "ETHUSD watch",
                            "selftext": "Market sees ETH and BTCUSDT again.",
                            "permalink": "/r/crypto/comments/2",
                            "author": "reddit-user",
                            "created_utc": 1704164645,
                        }
                    }
                ]
            }
        },
    }
    calls: list[str] = []

    async def fetcher(url: str) -> dict[str, object]:
        calls.append(url)
        return payloads[url]

    collector = RestSocialCollector(
        [
            SocialFeed(
                url="https://example.com/hn",
                source="hackernews",
                symbol_map={"BITCOIN": "BTC", "BTCUSD": "BTC"},
            ),
            SocialFeed(
                url="https://example.com/reddit",
                source="reddit",
                base_url="https://www.reddit.com",
            ),
        ],
        fetcher=fetcher,
    )

    events = await collector.poll_once()

    assert calls == ["https://example.com/hn", "https://example.com/reddit"]
    assert [event.source for event in events] == ["hackernews", "reddit"]
    assert events[0].author == "hn-user"
    assert set(events[0].symbols) == {"BTC", "ETH"}
    assert events[1].url == "https://www.reddit.com/r/crypto/comments/2"
    assert events[1].author == "reddit-user"
    assert set(events[1].symbols) == {"ETH", "ETHUSD", "BTCUSDT"}


@pytest.mark.asyncio
async def test_news_service_publishes_to_news_raw_with_stable_ids() -> None:
    bus = InMemoryBus()
    await bus.receive(TOPIC_NEWS_RAW, "analysis", max_messages=1)

    async def rss_fetcher(_: str) -> str:
        return RSS_XML

    collector = RssCollector(
        [RssFeed(url="https://example.com/feed.xml", source="example-rss")],
        fetcher=rss_fetcher,
    )
    service = NewsPollingService(bus=bus, collectors=[collector])

    await service.run_once()
    first_messages = await bus.peek(TOPIC_NEWS_RAW, "analysis", n=10)
    second_metrics = await service.run_once()
    second_messages = await bus.peek(TOPIC_NEWS_RAW, "analysis", n=10)

    assert len(first_messages) == 1
    assert len(second_messages) == 1
    assert second_metrics.duplicate_events == 1
    assert set(second_messages[0].body) == {
        "event_id",
        "ts",
        "correlation_id",
        "trace_id",
        "source",
        "title",
        "body",
        "url",
        "symbols",
        "author",
    }
    assert second_messages[0].body["source"] == "example-rss"
    assert second_messages[0].body["title"] == "BTC and ETH rally on ETF optimism"
    assert second_messages[0].body["body"] == "BTCUSDT and ETHUSD lead the move."
    assert second_messages[0].body["symbols"] == ["BTC", "ETH", "BTCUSDT", "ETHUSD"]
    assert second_messages[0].body["ts"].startswith("2024-01-01T00:00:00")
    assert second_metrics.unique_publishes == 1
    assert second_metrics.events_seen == 2


@pytest.mark.asyncio
async def test_news_hash_suppresses_duplicates_without_source_timestamp() -> None:
    bus = InMemoryBus()
    await bus.receive(TOPIC_NEWS_RAW, "analysis", max_messages=1)

    async def rss_fetcher(_: str) -> str:
        return RSS_XML_WITHOUT_DATE

    collector = RssCollector(
        [RssFeed(url="https://example.com/no-date.xml", source="example-rss")],
        fetcher=rss_fetcher,
    )
    service = NewsPollingService(bus=bus, collectors=[collector])

    await service.run_once()
    await service.run_once()
    messages = await bus.peek(TOPIC_NEWS_RAW, "analysis", n=10)

    assert len(messages) == 1
    assert service.metrics.duplicate_events == 1


def test_extract_symbols_is_configurable() -> None:
    symbols = extract_symbols(
        "Solana update: SOL and BTC",
        "bitcoin also appears",
        symbol_list=("BTC", "SOL"),
        symbol_map={"BITCOIN": "BTC"},
    )

    assert symbols == ["BTC", "SOL"]


@pytest.mark.asyncio
async def test_collectors_use_injected_fetchers_without_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_client_session(*args: object, **kwargs: object) -> None:
        raise AssertionError("network session should not be opened in tests")

    monkeypatch.setattr(
        "services.ingestion.sources.rss.aiohttp.ClientSession",
        fail_client_session,
    )
    monkeypatch.setattr(
        "services.ingestion.sources.rest.aiohttp.ClientSession",
        fail_client_session,
    )

    async def rss_fetcher(_: str) -> str:
        return RSS_XML

    async def rest_fetcher(_: str) -> dict[str, object]:
        return {
            "hits": [
                {
                    "title": "BTC is active",
                    "body": "ETH remains in view.",
                    "url": "https://example.com/post",
                    "created_at": "2024-01-03T00:00:00Z",
                }
            ]
        }

    rss_events = await RssCollector(
        [RssFeed(url="https://example.com/rss", source="rss")],
        fetcher=rss_fetcher,
    ).poll_once()
    rest_events = await RestSocialCollector(
        [SocialFeed(url="https://example.com/rest", source="rest")],
        fetcher=rest_fetcher,
    ).poll_once()

    assert len(rss_events) == 1
    assert len(rest_events) == 1


ENTITY_BOMB_FEED = """<?xml version="1.0"?>
<!DOCTYPE rss [
<!ENTITY a "AAAAAAAAAA">
<!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;">
<!ENTITY c "&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;">
]>
<rss version="2.0"><channel><item><title>&c;</title></item></channel></rss>"""


@pytest.mark.asyncio
async def test_rss_collector_refuses_a_feed_that_declares_a_dtd() -> None:
    """xml.etree expands internal entities, so a DTD is a billion-laughs vector."""

    async def bomb_fetcher(_: str) -> str:
        return ENTITY_BOMB_FEED

    collector = RssCollector(
        [RssFeed(url="https://example.com/bomb.xml", source="example-rss")],
        fetcher=bomb_fetcher,
    )

    with pytest.raises(ValueError, match="DOCTYPE"):
        await collector.poll_once()


@pytest.mark.asyncio
async def test_rss_collector_accepts_a_feed_mentioning_doctype_in_its_body() -> None:
    """The guard scans the prolog only, so article text is not collateral damage."""
    feed = (
        '<?xml version="1.0"?><rss version="2.0"><channel><item>'
        "<title>How to write a BTC page header</title>"
        "<description><![CDATA[A guide mentioning <!DOCTYPE html> inline.]]></description>"
        "</item></channel></rss>"
    )

    async def fetcher(_: str) -> str:
        return feed

    collector = RssCollector(
        [RssFeed(url="https://example.com/feed.xml", source="example-rss")],
        fetcher=fetcher,
    )

    events = await collector.poll_once()
    assert len(events) == 1


@pytest.mark.asyncio
async def test_rss_collector_rejects_an_oversized_payload() -> None:
    from services.ingestion.sources.rss import MAX_FEED_BYTES

    async def huge_fetcher(_: str) -> str:
        return "<rss>" + ("x" * (MAX_FEED_BYTES + 1)) + "</rss>"

    collector = RssCollector(
        [RssFeed(url="https://example.com/huge.xml", source="example-rss")],
        fetcher=huge_fetcher,
    )

    with pytest.raises(ValueError, match="exceeds"):
        await collector.poll_once()


@pytest.mark.asyncio
async def test_rss_collector_caps_entries_per_poll() -> None:
    from services.ingestion.sources.rss import MAX_ENTRIES_PER_FEED

    items = "".join(
        f"<item><title>BTC item {i}</title></item>" for i in range(MAX_ENTRIES_PER_FEED + 25)
    )

    async def fetcher(_: str) -> str:
        return f'<?xml version="1.0"?><rss version="2.0"><channel>{items}</channel></rss>'

    collector = RssCollector(
        [RssFeed(url="https://example.com/feed.xml", source="example-rss")],
        fetcher=fetcher,
    )

    events = await collector.poll_once()
    assert len(events) == MAX_ENTRIES_PER_FEED


class _FakeStreamContent:
    """Stand-in for aiohttp's StreamReader, delivering the body in chunks."""

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    async def iter_chunked(self, size: int):
        for chunk in self._chunks:
            yield chunk

    async def read(self, n: int = -1) -> bytes:
        # aiohttp returns whatever is buffered rather than n bytes; modelling
        # that is what makes these tests fail against a single read() call.
        return self._chunks[0] if self._chunks else b""


class _FakeResponse:
    def __init__(self, chunks: list[bytes], charset: str | None = "utf-8") -> None:
        self.content = _FakeStreamContent(chunks)
        self.charset = charset

    def raise_for_status(self) -> None:
        return None

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


class _FakeSession:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    def get(self, url: str) -> _FakeResponse:
        return self._response

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


def _patch_session(monkeypatch: pytest.MonkeyPatch, response: _FakeResponse) -> None:
    monkeypatch.setattr(
        "services.ingestion.sources.rss.aiohttp.ClientSession",
        lambda *args, **kwargs: _FakeSession(response),
    )


@pytest.mark.asyncio
async def test_fetch_rss_text_reads_a_feed_delivered_in_several_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    ``StreamReader.read(n)`` returns what is buffered, not n bytes, so a single
    read truncated every multi-chunk feed mid-document.
    """
    from services.ingestion.sources.rss import _fetch_rss_text, _parse_feed_entries

    body = RSS_XML.encode("utf-8")
    chunks = [body[i : i + 16] for i in range(0, len(body), 16)]
    assert len(chunks) > 1, "the fixture must arrive in more than one chunk"
    _patch_session(monkeypatch, _FakeResponse(chunks))

    payload = await _fetch_rss_text("https://example.com/feed.xml")

    assert payload == RSS_XML
    assert len(_parse_feed_entries(payload)) == 1


@pytest.mark.asyncio
async def test_fetch_rss_text_still_caps_an_oversized_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingestion.sources.rss import MAX_FEED_BYTES, _fetch_rss_text

    chunk = b"x" * (1024 * 1024)
    _patch_session(
        monkeypatch,
        _FakeResponse([chunk] * (MAX_FEED_BYTES // len(chunk) + 2)),
    )

    with pytest.raises(ValueError, match="exceeds"):
        await _fetch_rss_text("https://example.com/huge.xml")


def test_payload_size_is_measured_in_bytes_not_characters() -> None:
    """``len()`` of a str counts characters, so multi-byte feeds slipped the cap."""
    from services.ingestion.sources.rss import MAX_FEED_BYTES, _parse_feed_entries

    # Just under the cap in characters, comfortably over it in UTF-8 bytes.
    oversized = "<rss>" + ("é" * (MAX_FEED_BYTES - 100)) + "</rss>"

    with pytest.raises(ValueError, match="exceeds"):
        _parse_feed_entries(oversized)
