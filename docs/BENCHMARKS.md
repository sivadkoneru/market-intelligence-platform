# Benchmark Harness

`scripts/bench.py` runs a deterministic offline benchmark against the in-memory stream, AI, and alerting pipeline. It measures end-to-end per-event latency from `market.raw` publish through alert processing, then reports throughput plus `p50` / `p95` / `p99` latency in JSON.

## Run From Repo Root

```bash
.venv/bin/python scripts/bench.py --events 100 --output /tmp/mip-bench.json
cat /tmp/mip-bench.json
```

Add `--verbose` when you want per-event structured logs during the run. The default
mode keeps stdout quiet and writes the report to the requested output file.

The harness is offline-safe by default:

- `InMemoryBus`
- `InMemoryCache`
- `InMemoryTimeSeriesStore`
- `InMemorySearchStore`
- `MockLLMProvider`

It uses the existing service implementations instead of Docker containers or live network calls.

## What It Measures

- Throughput in events per second across the deterministic single-process pipeline
- End-to-end latency in milliseconds for each market event
- Tail latency summary: `p50`, `p95`, `p99`
- Processed message counts for stream, AI, and alerting stages

Latency starts immediately before publishing a synthetic `market.raw` event and stops after the event has flowed through:

1. `StreamService`
2. `AIAnalysisService`
3. `AlertingService`

## Latest Local Deterministic Result

The following sample was generated locally on June 18, 2026 with:

```bash
.venv/bin/python scripts/bench.py --events 100 --output /tmp/mip-bench.json
```

Update this section when the harness changes materially.

```json
{
  "completed_at": "2026-06-18T19:43:21.296933+00:00",
  "elapsed_seconds": 0.04897,
  "events": 100,
  "latency_ms": {
    "max_ms": 0.859709,
    "mean_ms": 0.485201,
    "min_ms": 0.276292,
    "p50_ms": 0.48598,
    "p95_ms": 0.606269,
    "p99_ms": 0.65152
  },
  "limitations": [
    "This harness uses the in-memory bus, cache, search store, and time-series store only.",
    "Latency reflects a deterministic single-process pipeline, not Docker, network, or Azure infra.",
    "A live full-stack benchmark is a follow-up once Docker daemon access is available."
  ],
  "mode": "offline-in-memory",
  "pipeline_counts": {
    "ai_insights_published": 100,
    "ai_messages_processed": 100,
    "alerting_alerts_published": 48,
    "alerting_messages_processed": 200,
    "stream_messages_processed": 100,
    "stream_signals_published": 100
  },
  "started_at": "2026-06-18T19:43:21.247831+00:00",
  "symbols": [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT"
  ],
  "throughput_events_per_second": 2042.066571
}
```

## Limitations

- This is not a live infrastructure benchmark.
- Results exclude Docker, Azure Service Bus, Redis, Druid, Elasticsearch, and HTTP/WebSocket transport overhead.
- The harness is designed for repeatable local regression checks, not production capacity planning.

## Follow-Up

When Docker daemon access is available, add a full-stack benchmark that exercises the compose environment and compares those results with this deterministic baseline.
