"""Deterministic offline benchmark harness for the in-memory service pipeline."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import fmean
from time import perf_counter_ns
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from libs.common import (  # noqa: E402
    TOPIC_ALERTS,
    TOPIC_INSIGHTS,
    TOPIC_MARKET_RAW,
    TOPIC_SIGNALS,
    InMemoryBus,
    InMemoryCache,
    InMemorySearchStore,
    InMemoryTimeSeriesStore,
    configure_logging,
    get_logger,
)
from services.ai import (  # noqa: E402
    AI_SUBSCRIPTION,
    AIAnalysisService,
    MockLLMProvider,
    RAGPipeline,
)
from services.alerting import ALERTING_SUBSCRIPTION, AlertingService  # noqa: E402
from services.stream.service import STREAM_SUBSCRIPTION, StreamService  # noqa: E402

DEFAULT_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
DEFAULT_LIMITATIONS = (
    "This harness uses the in-memory bus, cache, search store, and time-series store only.",
    "Latency reflects a deterministic single-process pipeline, not Docker, network, "
    "or Azure infra.",
    "A live full-stack benchmark is a follow-up once Docker daemon access is available.",
)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")
    return parsed


@dataclass(frozen=True)
class BenchmarkConfig:
    events: int
    output: Path
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS
    log_level: str = "WARNING"


@dataclass(frozen=True)
class LatencySummary:
    min_ms: float
    mean_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float


@dataclass(frozen=True)
class BenchmarkResult:
    mode: str
    started_at: str
    completed_at: str
    events: int
    symbols: tuple[str, ...]
    elapsed_seconds: float
    throughput_events_per_second: float
    latency_ms: LatencySummary
    pipeline_counts: dict[str, int]
    limitations: tuple[str, ...]

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["symbols"] = list(self.symbols)
        payload["limitations"] = list(self.limitations)
        return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a deterministic offline benchmark of the in-memory service pipeline.",
    )
    parser.add_argument(
        "--events",
        type=_positive_int,
        default=100,
        help="Number of market events to process through stream, AI, and alerting.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to the JSON report file to write.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Emit per-event structured logs while the benchmark runs.",
    )
    return parser


def percentile(values: Sequence[float], pct: float) -> float:
    """Return a percentile using linear interpolation between ranked samples."""
    if not values:
        raise ValueError("percentile requires at least one value")
    if not 0 <= pct <= 100:
        raise ValueError("percentile must be between 0 and 100")

    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 6)

    rank = (len(ordered) - 1) * (pct / 100.0)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    value = ordered[lower] + ((ordered[upper] - ordered[lower]) * weight)
    return round(value, 6)


def summarize_latencies(latencies_ms: Sequence[float]) -> LatencySummary:
    if not latencies_ms:
        raise ValueError("latencies_ms must not be empty")

    return LatencySummary(
        min_ms=round(min(latencies_ms), 6),
        mean_ms=round(fmean(latencies_ms), 6),
        p50_ms=percentile(latencies_ms, 50),
        p95_ms=percentile(latencies_ms, 95),
        p99_ms=percentile(latencies_ms, 99),
        max_ms=round(max(latencies_ms), 6),
    )


async def _create_services() -> tuple[
    InMemoryBus,
    StreamService,
    AIAnalysisService,
    AlertingService,
]:
    bus = InMemoryBus()
    cache = InMemoryCache()
    search_store = InMemorySearchStore()
    timeseries_store = InMemoryTimeSeriesStore()
    llm_provider = MockLLMProvider()

    await bus.receive(TOPIC_MARKET_RAW, STREAM_SUBSCRIPTION, max_messages=0)
    await bus.receive(TOPIC_SIGNALS, AI_SUBSCRIPTION, max_messages=0)
    await bus.receive(TOPIC_SIGNALS, ALERTING_SUBSCRIPTION, max_messages=0)
    await bus.receive(TOPIC_INSIGHTS, ALERTING_SUBSCRIPTION, max_messages=0)
    await bus.receive(TOPIC_ALERTS, "observer", max_messages=0)

    stream_service = StreamService(
        bus=bus,
        cache=cache,
        store=timeseries_store,
    )
    ai_service = AIAnalysisService(
        bus=bus,
        cache=cache,
        search_store=search_store,
        rag_pipeline=RAGPipeline(
            search_store=search_store,
            embedding_provider=llm_provider,
        ),
        llm_provider=llm_provider,
    )
    alerting_service = AlertingService(
        bus=bus,
        cache=cache,
    )
    return bus, stream_service, ai_service, alerting_service


def build_market_event_payload(index: int, symbols: Sequence[str]) -> tuple[dict[str, Any], str]:
    symbol = symbols[index % len(symbols)]
    cycle = index // len(symbols)
    base_prices = {
        "BTCUSDT": 42000.0,
        "ETHUSDT": 2250.0,
        "SOLUSDT": 110.0,
    }
    pattern = (0.0, 1.5, 3.0, 6.0, 12.0, 24.0, 8.0, 28.0)
    price = base_prices[symbol] + (cycle * 2.25) + pattern[index % len(pattern)]
    ts = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=index)
    message_id = f"market-{index:05d}"
    payload = {
        "event_id": f"ev-{message_id}",
        "ts": ts.isoformat(),
        "symbol": symbol,
        "source": "bench.replay",
        "event_type": "trade",
        "price": round(price, 4),
        "volume": round(1.0 + ((index % 5) * 0.1), 4),
        "bid": round(price - 0.25, 4),
        "ask": round(price + 0.25, 4),
        "correlation_id": f"corr-{message_id}",
        "trace_id": f"trace-{message_id}",
    }
    return payload, message_id


async def run_benchmark(config: BenchmarkConfig) -> BenchmarkResult:
    configure_logging(level=config.log_level, service_name="benchmark")
    log = get_logger(__name__)
    bus, stream_service, ai_service, alerting_service = await _create_services()

    latencies_ms: list[float] = []
    started_at = datetime.now(tz=UTC)
    started_ns = perf_counter_ns()
    log.info(
        "bench.started",
        events=config.events,
        symbols=list(config.symbols),
        mode="offline-in-memory",
    )

    for index in range(config.events):
        payload, message_id = build_market_event_payload(index, config.symbols)
        event_started_ns = perf_counter_ns()
        await bus.publish(TOPIC_MARKET_RAW, payload, message_id=message_id)
        await stream_service.poll_once(max_messages=1)
        await ai_service.poll_once(max_messages=1)
        await alerting_service.poll_once(max_messages=2)
        latencies_ms.append((perf_counter_ns() - event_started_ns) / 1_000_000.0)

    elapsed_seconds = (perf_counter_ns() - started_ns) / 1_000_000_000.0
    completed_at = datetime.now(tz=UTC)

    result = BenchmarkResult(
        mode="offline-in-memory",
        started_at=started_at.isoformat(),
        completed_at=completed_at.isoformat(),
        events=config.events,
        symbols=config.symbols,
        elapsed_seconds=round(elapsed_seconds, 6),
        throughput_events_per_second=round(config.events / elapsed_seconds, 6),
        latency_ms=summarize_latencies(latencies_ms),
        pipeline_counts={
            "stream_messages_processed": stream_service.metrics.messages_processed,
            "stream_signals_published": stream_service.metrics.signals_published,
            "ai_messages_processed": ai_service.metrics.messages_processed,
            "ai_insights_published": ai_service.metrics.insights_published,
            "alerting_messages_processed": alerting_service.metrics.messages_processed,
            "alerting_alerts_published": alerting_service.metrics.alerts_published,
        },
        limitations=DEFAULT_LIMITATIONS,
    )
    log.info(
        "bench.completed",
        events=result.events,
        throughput_events_per_second=result.throughput_events_per_second,
        p95_ms=result.latency_ms.p95_ms,
        alerts_published=result.pipeline_counts["alerting_alerts_published"],
    )
    return result


def write_result(output_path: Path, result: BenchmarkResult) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(result.to_payload(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = asyncio.run(
        run_benchmark(
            BenchmarkConfig(
                events=args.events,
                output=args.output,
                log_level="INFO" if args.verbose else "WARNING",
            )
        )
    )
    write_result(args.output, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
