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
    payloads = {
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
