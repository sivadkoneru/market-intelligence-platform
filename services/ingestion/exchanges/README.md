# Exchange WebSocket Clients

Portfolio-only WebSocket clients for live market ingestion. These clients wrap
Binance and Coinbase public feeds behind the same offline-testable publish path:
decode exchange JSON, normalize into `libs.common.MarketEvent`, and publish to
`market.raw` with deterministic message IDs.

## Disclaimer

No financial advice. No real trades. This repository is a portfolio project.

## What lives here

- `base.py` — shared reconnect/backoff loop, circuit breaker integration,
  heartbeat tracking, stale-stream reconnects, and message-bus publishing
- `binance.py` — Binance trade/ticker client
- `coinbase.py` — Coinbase ticker client with subscribe message support
- `__init__.py` — public package exports

## Offline-safe design

- Pass an injected `connect_factory` in tests instead of opening real sockets.
- Fake WebSocket objects only need async context manager, async iteration, and
  `send()` support.
- Publishing uses the shared `MessageBus` port from `libs.common`.
- `heartbeat_timeout_seconds` actively reconnects sockets that stop yielding
  messages; set it to `None` to disable that watchdog in a specialized test.

## Example

```python
from libs.common import InMemoryBus
from services.ingestion.exchanges import CoinbaseWebSocketClient

bus = InMemoryBus()
client = CoinbaseWebSocketClient(
    bus=bus,
    product_ids=["BTC-USD"],
)
```

## Dependencies

Uses the existing pinned `websockets` dependency plus shared helpers from
`libs.common`. No extra external packages are required.
