# Ingestion News Sources

Offline-safe RSS and REST/social collectors for the ingestion service. These
collectors normalize third-party news payloads into the shared
`libs.common.NewsEvent` schema and the news polling runner publishes them to
`news.raw`.

## Disclaimer

No financial advice. No real trades. This repository is a portfolio project.

## Modules

- `base.py` — timestamp coercion, symbol extraction, content hashing, and the
  collector protocol
- `rss.py` — RSS/Atom parsing with injected fetchers and symbol extraction
- `rest.py` — HN/Reddit-style JSON polling and normalization
- `service.py` — one-pass polling runner that publishes to `news.raw`

## Offline-safe design

- Tests inject fake fetchers; no live network is required.
- Duplicate suppression uses a deterministic content hash as the message ID.
- Symbol extraction is configurable through the `symbol_list` and
  `symbol_map` inputs on each feed configuration.

## Example

```python
from libs.common import InMemoryBus
from services.ingestion.sources import NewsPollingService, RssCollector, RssFeed

async def fetch_rss(_: str) -> str:
    return "<rss><channel><item><title>BTC update</title><description>ETH follows</description><pubDate>Mon, 01 Jan 2024 00:00:00 GMT</pubDate></item></channel></rss>"

bus = InMemoryBus()
collector = RssCollector(
    [RssFeed(url="https://example.invalid/feed.xml", source="example-rss")],
    fetcher=fetch_rss,
)
service = NewsPollingService(bus=bus, collectors=[collector])
```

## Dependencies

Uses the pinned `aiohttp` runtime dependency for real RSS/REST polling.
Tests inject plain async callables and do not require network access.
