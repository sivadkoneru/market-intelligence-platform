# API

This platform is a portfolio project only. No financial advice. No real trades.

## Base URLs

- Direct service: `http://localhost:8005`
- Compose-exposed service: `http://localhost:8000`
- WebSocket smoke URL: `ws://localhost:8000/ws/stream`
- Direct WebSocket URL: `ws://localhost:8005/ws/stream`
- Ingestion service for local mock inputs: `http://localhost:8001`

## Common Notes

- All HTTP services expose `GET /health` and `GET /metrics`.
- Requests and event processing honor `X-Correlation-ID` and `X-Trace-ID` when present.
- REST timestamps are ISO-8601 strings.
- The API service resolves to in-memory ports by default when live infra is not configured.

## Local Mock Inputs

### `POST http://localhost:8001/mock/news`

Publishes a valid `NewsEvent` to `news.raw` so the `ai-analysis` service can
generate an `Insight`.

Minimal request:

```bash
curl -X POST http://localhost:8001/mock/news
```

Custom request:

```bash
curl -X POST http://localhost:8001/mock/news \
  -H 'Content-Type: application/json' \
  -d '{"symbols":["BTCUSDT"],"title":"BTC ETF inflows improve","body":"BTC sentiment is constructive in this local mock item."}'
```

Then read the generated insight from the API service:

```bash
curl http://localhost:8000/insights/BTCUSDT
```

## REST Surface

### `GET /`

Returns the API service banner and the implemented route list.

Response shape:

```json
{
  "service": "api",
  "message": "Portfolio project only. No financial advice. No real trades.",
  "routes": ["..."]
}
```

### `GET /health`

Returns backend status for the API service.

Response shape includes:

- `status`
- `service`
- `subscription`
- `backends.timeseries`
- `backends.cache`
- `backends.bus`
- `structured_logging`

### `GET /metrics`

Plain-text counters and gauges using `# TYPE` metadata for scraper-friendly local diagnostics.

### `GET /symbols`

Returns the set of symbols discovered from Druid rows and latest stream
snapshots cached in Redis. Fresh local runs can therefore show symbols as soon
as the stream service processes `market.raw`, even before Druid indexing catches
up.

Response shape:

```json
{ "symbols": ["BTCUSDT"], "count": 1 }
```

### `GET /market/{symbol}/latest`

Returns the latest cached market snapshot or latest Druid row for the symbol, or
`404` if none exists.

### `GET /market/{symbol}/history?from=...&to=...`

Query params:

- `from` required, ISO-8601 datetime, mapped to the `from` alias in code
- `to` required, ISO-8601 datetime

Response shape:

```json
{
  "symbol": "BTCUSDT",
  "from": "2026-01-01T00:00:00+00:00",
  "to": "2026-01-01T00:02:00+00:00",
  "rows": []
}
```

Rows are read from Druid and merged with the recent `history:{symbol}` stream
cache so newly seeded local data is available before Druid batch ingestion
settles.

Notes:

- Returns `400` when `from > to`.
- Rows are sorted by timestamp ascending.

### `GET /indicators/{symbol}`

Returns the latest indicator snapshot for a symbol.

Lookup order:

1. Redis snapshot from `Cache.get_snapshot(symbol)`
2. Latest `indicators` row from Druid
3. `404` if neither exists

Response shape includes:

- `symbol`
- `ts`
- `source`
- `price`
- `anomaly`
- `indicators.sma`
- `indicators.ema`
- `indicators.rsi`
- `indicators.volatility`
- `indicators.trend`
- `flags.trend`
- `flags.zscore_anomaly`
- `flags.ewma_anomaly`

### `GET /signals?limit=20`

Returns recent `Signal` payloads from the `signals` topic.

Query params:

- `limit` optional, default `20`, range `1..100`

Response shape:

```json
{ "signals": [], "count": 0 }
```

### `GET /alerts?limit=20`

Returns recent `Alert` payloads from the `alerts` topic.

Query params:

- `limit` optional, default `20`, range `1..100`

Response shape:

```json
{ "alerts": [], "count": 0 }
```

### `GET /insights/{symbol}`

Returns the latest `Insight` for the symbol.

Lookup order:

1. Redis cache at `insight:{symbol}`
2. Recent `insights` messages on the bus
3. `404` if nothing matches

## WebSocket Surface

### `WS /ws/stream`

The socket accepts commands of the form:

```json
{ "action": "subscribe", "symbols": ["BTCUSDT"] }
```

Valid subscribe requests return:

```json
{ "type": "subscribed", "symbols": ["BTCUSDT"] }
```

Invalid commands return:

```json
{ "type": "error", "detail": "..." }
```

Live messages use this shape:

```json
{
  "type": "market",
  "topic": "market.raw",
  "symbol": "BTCUSDT",
  "payload": {}
}
```

The same stream carries `signal`, `alert`, and `insight` message types. Fanout is filtered by the subscribed symbol list, and the API service uses the `api-ws` subscription under the hood.
