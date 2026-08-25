"""
Core AI-analysis worker for news and signal insight generation.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError
from tenacity import RetryError

from libs.common import (
    IDEMPOTENCY_TTL_SECONDS,
    INSIGHT_CACHE_PREFIX,
    TOPIC_INSIGHTS,
    TOPIC_NEWS_RAW,
    TOPIC_SIGNALS,
    Cache,
    Insight,
    MessageBus,
    NewsEvent,
    ReceivedMessage,
    SearchStore,
    Signal,
    WorkerMetrics,
    close_backends,
    dead_letter_message,
    get_logger,
    render_counters,
    retry_async,
    run_poll_loop,
    validation_reason,
)
from services.ai.llm import (
    GenerationRequest,
    GenerationResult,
    LLMProvider,
    apply_guardrails,
)
from services.ai.llm.models import ContextDocument
from services.ai.rag import RAGPipeline, SourceDocument

AI_SUBSCRIPTION = "ai"
PROCESSED_PREFIX = "ai-analysis:processed"
LLM_CACHE_PREFIX = "ai-analysis:llm"


def _stable_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class EventDetectionResult:
    category: str
    label: str
    rationale: str
    framework: str


class DeterministicEventDetector:
    """Offline-safe event detection fallback compatible with the AutoGen requirement."""

    async def detect_news(
        self,
        event: NewsEvent,
        *,
        symbol: str,
    ) -> EventDetectionResult:
        text = f"{event.title} {event.body}".lower()
        if any(term in text for term in ("hack", "exploit", "breach", "lawsuit")):
            return EventDetectionResult(
                category="risk_event",
                label="Risk event",
                rationale=f"{symbol} coverage highlights a risk-heavy news catalyst.",
                framework="deterministic",
            )
        if any(term in text for term in ("earnings", "revenue", "margin", "profit", "beat")):
            return EventDetectionResult(
                category="fundamental_update",
                label="Fundamental update",
                rationale=f"{symbol} coverage points to a fundamentals-driven repricing event.",
                framework="deterministic",
            )
        if any(term in text for term in ("etf", "flows", "inflow", "outflow", "adoption")):
            return EventDetectionResult(
                category="flow_event",
                label="Flow event",
                rationale=f"{symbol} coverage emphasizes allocation and demand-flow signals.",
                framework="deterministic",
            )
        return EventDetectionResult(
            category="market_update",
            label="Market update",
            rationale=f"{symbol} coverage looks like a general market-moving update.",
            framework="deterministic",
        )

    async def detect_signal(self, signal: Signal) -> EventDetectionResult:
        trend = signal.indicators.get("trend")
        rsi = signal.indicators.get("rsi")
        if signal.anomaly:
            return EventDetectionResult(
                category="anomaly_event",
                label="Anomaly event",
                rationale=f"{signal.symbol} triggered an anomalous technical move.",
                framework="deterministic",
            )
        if trend is not None and trend > 0:
            return EventDetectionResult(
                category="bullish_technical_shift",
                label="Bullish technical shift",
                rationale=f"{signal.symbol} indicators lean constructive with an upward trend.",
                framework="deterministic",
            )
        if trend is not None and trend < 0:
            return EventDetectionResult(
                category="bearish_technical_shift",
                label="Bearish technical shift",
                rationale=f"{signal.symbol} indicators lean defensive with a downward trend.",
                framework="deterministic",
            )
        if rsi is not None and rsi >= 70:
            return EventDetectionResult(
                category="overbought_signal",
                label="Overbought signal",
                rationale=f"{signal.symbol} momentum is elevated enough to look stretched.",
                framework="deterministic",
            )
        return EventDetectionResult(
            category="technical_update",
            label="Technical update",
            rationale=f"{signal.symbol} posted a routine indicator refresh.",
            framework="deterministic",
        )


class AutoGenCompatibleEventDetector:
    """
    Import-guarded adapter.

    When AutoGen is unavailable, the deterministic detector remains the tested path.
    """

    def __init__(self) -> None:
        self._fallback = DeterministicEventDetector()
        try:
            import autogen  # noqa: F401

            self._mode = "autogen"
        except ImportError:
            self._mode = "deterministic"

    async def detect_news(
        self,
        event: NewsEvent,
        *,
        symbol: str,
    ) -> EventDetectionResult:
        result = await self._fallback.detect_news(event, symbol=symbol)
        if self._mode == "autogen":
            return EventDetectionResult(
                category=result.category,
                label=result.label,
                rationale=result.rationale,
                framework="autogen-compatible",
            )
        return result

    async def detect_signal(self, signal: Signal) -> EventDetectionResult:
        result = await self._fallback.detect_signal(signal)
        if self._mode == "autogen":
            return EventDetectionResult(
                category=result.category,
                label=result.label,
                rationale=result.rationale,
                framework="autogen-compatible",
            )
        return result


@dataclass
class AIMetrics(WorkerMetrics):
    messages_seen: int = 0
    messages_processed: int = 0
    duplicates_suppressed: int = 0
    insights_published: int = 0
    llm_cache_hits: int = 0
    processing_retries: int = 0
    news_indexed: int = 0
    signal_contexts_indexed: int = 0
    dead_lettered: int = 0

    def render(self) -> str:
        lines = render_counters(
            "ai",
            {
                "messages_seen": self.messages_seen,
                "messages_processed": self.messages_processed,
                "duplicates_suppressed": self.duplicates_suppressed,
                "insights_published": self.insights_published,
                "llm_cache_hits": self.llm_cache_hits,
                "processing_retries": self.processing_retries,
                "news_indexed": self.news_indexed,
                "signal_contexts_indexed": self.signal_contexts_indexed,
                "dead_lettered": self.dead_lettered,
            },
        )
        lines.extend(self.http.render("ai"))
        return "\n".join(lines) + "\n"


class AIAnalysisService:
    def __init__(
        self,
        *,
        bus: MessageBus,
        cache: Cache,
        search_store: SearchStore,
        rag_pipeline: RAGPipeline,
        llm_provider: LLMProvider,
        event_detector: AutoGenCompatibleEventDetector | None = None,
        news_topic: str = TOPIC_NEWS_RAW,
        signal_topic: str = TOPIC_SIGNALS,
        insight_topic: str = TOPIC_INSIGHTS,
        subscription: str = AI_SUBSCRIPTION,
        max_processing_attempts: int = 3,
        retry_wait_min_seconds: float = 0.0,
        retry_wait_max_seconds: float = 0.0,
    ) -> None:
        self._bus = bus
        self._cache = cache
        self._search_store = search_store
        self._rag = rag_pipeline
        self._llm = llm_provider
        self._event_detector = event_detector or AutoGenCompatibleEventDetector()
        self._news_topic = news_topic
        self._signal_topic = signal_topic
        self._insight_topic = insight_topic
        self._subscription = subscription
        self._max_processing_attempts = max(1, max_processing_attempts)
        self._retry_wait_min_seconds = retry_wait_min_seconds
        self._retry_wait_max_seconds = retry_wait_max_seconds
        self.metrics = AIMetrics()
        self._log = get_logger(__name__)

    async def poll_once(self, *, max_messages: int = 10) -> int:
        total = 0
        for topic in (self._news_topic, self._signal_topic):
            messages = await self._bus.receive(
                topic,
                self._subscription,
                max_messages=max_messages,
            )
            total += len(messages)
            for message in messages:
                self.metrics.messages_seen += 1
                await self._handle_message(message)
        return total

    async def run_forever(
        self,
        *,
        poll_interval_seconds: float = 0.25,
        max_messages: int = 10,
    ) -> None:
        await run_poll_loop(
            self.poll_once,
            service_name="ai",
            log=self._log,
            metrics=self.metrics,
            poll_interval_seconds=poll_interval_seconds,
            max_messages=max_messages,
        )

    async def close(self) -> None:
        """
        Release every backend that owns a connection.

        This service was the only one that never closed its clients, so each
        restart leaked the Elasticsearch, Redis, and Service Bus sockets built
        by ``build_default_service``.
        """
        await close_backends(
            (self._search_store, self._cache, self._bus, self._llm),
            log=self._log,
            service_name="ai",
        )

    async def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "service": "ai-analysis",
            "topics": [self._news_topic, self._signal_topic],
            "subscription": self._subscription,
            "messages_processed": self.metrics.messages_processed,
            "duplicates_suppressed": self.metrics.duplicates_suppressed,
            "dead_lettered": self.metrics.dead_lettered,
            "event_detection_mode": type(self._event_detector).__name__,
        }

    async def _handle_message(self, message: ReceivedMessage) -> None:
        if message.topic == self._news_topic:
            await self._handle_news_message(message)
        elif message.topic == self._signal_topic:
            await self._handle_signal_message(message)
        else:
            await self._dead_letter(message, f"unsupported topic: {message.topic}")

    async def _handle_news_message(self, message: ReceivedMessage) -> None:
        try:
            event = NewsEvent.model_validate(message.body)
        except ValidationError as exc:
            await self._dead_letter(message, validation_reason("invalid news event payload", exc))
            return

        await self._process_with_retries(
            message,
            lambda: self._process_news_event(message, event),
        )

    async def _handle_signal_message(self, message: ReceivedMessage) -> None:
        try:
            signal = Signal.model_validate(message.body)
        except ValidationError as exc:
            await self._dead_letter(message, validation_reason("invalid signal payload", exc))
            return

        await self._process_with_retries(
            message,
            lambda: self._process_signal_event(message, signal),
        )

    async def _process_with_retries(
        self,
        message: ReceivedMessage,
        operation: Callable[[], Awaitable[None]],
    ) -> None:
        attempts = 0

        async def _attempt() -> None:
            nonlocal attempts
            attempts += 1
            if attempts > 1:
                self.metrics.processing_retries += 1
            await operation()

        try:
            await retry_async(
                _attempt,
                max_attempts=self._max_processing_attempts,
                wait_min=self._retry_wait_min_seconds,
                wait_max=self._retry_wait_max_seconds,
            )
        except RetryError as exc:
            last_exc = exc.last_attempt.exception()
            self.metrics.last_error = f"ai analysis failed: {type(last_exc).__name__}: {last_exc}"
            await self._dead_letter(message, self.metrics.last_error)

    async def _process_news_event(
        self,
        message: ReceivedMessage,
        event: NewsEvent,
    ) -> None:
        await self._index_news_event(event)
        for symbol in event.symbols:
            await self._publish_news_insight(event, symbol=symbol)
        self.metrics.messages_processed += 1
        await self._bus.complete(message)

    async def _process_signal_event(
        self,
        message: ReceivedMessage,
        signal: Signal,
    ) -> None:
        await self._index_signal_context(signal)
        await self._publish_signal_insight(signal)
        self.metrics.messages_processed += 1
        await self._bus.complete(message)

    async def _index_news_event(self, event: NewsEvent) -> None:
        await self._rag.index_source_document(
            SourceDocument(
                doc_id=event.event_id,
                title=event.title,
                text=f"{event.title}\n\n{event.body}",
                url=event.url,
                metadata={
                    "source": event.source,
                    "symbols": list(event.symbols),
                    "author": event.author,
                    "correlation_id": event.correlation_id,
                    "trace_id": event.trace_id,
                    "citation": event.url or event.event_id,
                },
            )
        )
        self.metrics.news_indexed += 1

    async def _index_signal_context(self, signal: Signal) -> None:
        await self._rag.index_source_document(
            SourceDocument(
                doc_id=f"signal:{signal.event_id}",
                title=f"{signal.symbol} technical signal",
                text=_signal_context_text(signal),
                metadata={
                    "source": signal.source,
                    "symbol": signal.symbol,
                    "anomaly": signal.anomaly,
                    "indicators": dict(signal.indicators),
                    "citation": f"signal:{signal.event_id}",
                    "correlation_id": signal.correlation_id,
                    "trace_id": signal.trace_id,
                },
            )
        )
        self.metrics.signal_contexts_indexed += 1

    async def _publish_news_insight(self, event: NewsEvent, *, symbol: str) -> None:
        content_hash = news_content_hash(event, symbol=symbol)
        processed_key = news_processed_key(event, symbol=symbol)
        if await self._cache.get(processed_key):
            self.metrics.duplicates_suppressed += 1
            return

        detection = await self._event_detector.detect_news(event, symbol=symbol)
        prompt = _news_prompt(event, symbol=symbol, detection=detection)
        await self._publish_insight_for(
            symbol=symbol,
            content_hash=content_hash,
            processed_key=processed_key,
            prompt=prompt,
            query=f"{symbol} {event.title} {event.body}",
            metadata={
                "symbol": symbol,
                "event_category": detection.category,
                "event_framework": detection.framework,
                "source_event_id": event.event_id,
            },
            event_ts=event.ts,
            correlation_id=event.correlation_id,
            trace_id=event.trace_id,
            detection=detection,
        )

    async def _publish_signal_insight(self, signal: Signal) -> None:
        content_hash = signal_content_hash(signal)
        processed_key = signal_processed_key(signal)
        if await self._cache.get(processed_key):
            self.metrics.duplicates_suppressed += 1
            return

        detection = await self._event_detector.detect_signal(signal)
        prompt = _signal_prompt(signal, detection=detection)
        await self._publish_insight_for(
            symbol=signal.symbol,
            content_hash=content_hash,
            processed_key=processed_key,
            prompt=prompt,
            query=_signal_query(signal),
            metadata={
                "symbol": signal.symbol,
                "event_category": detection.category,
                "event_framework": detection.framework,
                "source_event_id": signal.event_id,
            },
            event_ts=signal.ts,
            correlation_id=signal.correlation_id,
            trace_id=signal.trace_id,
            detection=detection,
        )

    async def _publish_insight_for(
        self,
        *,
        symbol: str,
        content_hash: str,
        processed_key: str,
        prompt: str,
        query: str,
        metadata: dict[str, Any],
        event_ts: Any,
        correlation_id: str | None,
        trace_id: str | None,
        detection: EventDetectionResult,
    ) -> None:
        """
        Shared tail for ``_publish_news_insight``/``_publish_signal_insight``:
        generate (with cache) → build the insight → publish → cache it →
        mark the source event processed. Callers own the kind-specific
        dedupe check, detection call, and prompt/query construction.
        """
        result = await self._generate_with_cache(
            content_hash=content_hash,
            prompt=prompt,
            query=query,
            metadata=metadata,
        )
        insight = _insight_from_result(
            symbol=symbol,
            event_ts=event_ts,
            correlation_id=correlation_id,
            trace_id=trace_id,
            result=_with_event_detection(result, detection),
        )
        await self._publish_insight(insight, message_id=processed_key)
        await self._cache.set(
            f"{INSIGHT_CACHE_PREFIX}:{symbol}",
            insight.model_dump(mode="json"),
        )
        await self._cache.set(processed_key, True, ttl=IDEMPOTENCY_TTL_SECONDS)

    async def _generate_with_cache(
        self,
        *,
        content_hash: str,
        prompt: str,
        query: str,
        metadata: dict[str, Any],
    ) -> GenerationResult:
        context = await self._rag.build_context_documents(query, top_k=4, candidate_k=6)
        cache_key = _llm_cache_key(
            content_hash=content_hash,
            prompt=prompt,
            query=query,
            metadata=metadata,
            context=context,
            provider_name=type(self._llm).__qualname__,
        )
        request = GenerationRequest(
            prompt=prompt,
            context=context,
            metadata=metadata,
        )
        cached = await self._cache.get(cache_key)
        if cached is not None:
            self.metrics.llm_cache_hits += 1
            guarded, _ = apply_guardrails(request, GenerationResult(**cached))
            return guarded

        result = await self._llm.generate(request)
        guarded, _ = apply_guardrails(request, result)
        # Bounded for the same reason as the processed markers: one key per
        # unique generation, retained forever, is unbounded store growth.
        await self._cache.set(
            cache_key, _generation_result_payload(guarded), ttl=IDEMPOTENCY_TTL_SECONDS
        )
        return guarded

    async def _publish_insight(self, insight: Insight, *, message_id: str) -> None:
        await self._bus.publish(
            self._insight_topic,
            insight.model_dump(mode="json"),
            message_id=message_id,
            correlation_id=insight.correlation_id,
        )
        self.metrics.insights_published += 1
        self._log.info(
            "ai.insight_published",
            symbol=insight.symbol,
            message_id=message_id,
            grounded=insight.grounded,
            confidence=insight.confidence,
        )

    async def _dead_letter(self, message: ReceivedMessage, reason: str) -> None:
        await dead_letter_message(
            message,
            reason,
            bus=self._bus,
            log=self._log,
            metrics=self.metrics,
            service_name="ai",
        )


def news_content_hash(event: NewsEvent, *, symbol: str) -> str:
    return _stable_hash(
        {
            "kind": "news",
            "symbol": symbol,
            "source": event.source,
            "title": event.title,
            "body": event.body,
            "url": event.url,
            "author": event.author,
        }
    )


def news_processed_key(event: NewsEvent, *, symbol: str) -> str:
    return f"{PROCESSED_PREFIX}:news:{event.event_id}:{symbol}"


def signal_content_hash(signal: Signal) -> str:
    return _stable_hash(
        {
            "kind": "signal",
            "symbol": signal.symbol,
            "source": signal.source,
            "ts": signal.ts.isoformat(),
            "anomaly": signal.anomaly,
            "indicators": signal.indicators,
        }
    )


def signal_processed_key(signal: Signal) -> str:
    return f"{PROCESSED_PREFIX}:signal:{signal.event_id}"


def _llm_cache_key(
    *,
    content_hash: str,
    prompt: str,
    query: str,
    metadata: dict[str, Any],
    context: tuple[ContextDocument, ...],
    provider_name: str,
) -> str:
    generation_metadata = {
        key: value for key, value in metadata.items() if key != "source_event_id"
    }
    context_payload = sorted(
        {
            (
                document.citation,
                hashlib.sha256(document.text.encode("utf-8")).hexdigest(),
            )
            for document in context
        }
    )
    cache_payload = {
        "content_hash": content_hash,
        "prompt": prompt,
        "query": query,
        "metadata": generation_metadata,
        "context": context_payload,
        "provider": provider_name,
    }
    return f"{LLM_CACHE_PREFIX}:{_stable_hash(cache_payload)}"


def _signal_context_text(signal: Signal) -> str:
    parts = [f"Signal for {signal.symbol} from {signal.source}."]
    parts.append(f"Anomaly flag: {'yes' if signal.anomaly else 'no'}.")
    if signal.indicators:
        indicator_bits = []
        for name, value in sorted(signal.indicators.items()):
            indicator_bits.append(f"{name}={value}")
        parts.append("Indicators: " + ", ".join(indicator_bits) + ".")
    return " ".join(parts)


def _signal_query(signal: Signal) -> str:
    trend = signal.indicators.get("trend")
    rsi = signal.indicators.get("rsi")
    return f"{signal.symbol} technical signal anomaly {signal.anomaly} " f"trend {trend} rsi {rsi}"


def _news_prompt(
    event: NewsEvent,
    *,
    symbol: str,
    detection: EventDetectionResult,
) -> str:
    return (
        f"Analyze the market impact of this news for {symbol}. "
        f"Detected event category: {detection.label}. "
        f"Event rationale: {detection.rationale} "
        f"Headline: {event.title}. "
        f"Explain the sentiment, summarize the catalyst, and ground the explanation in citations."
    )


def _signal_prompt(signal: Signal, *, detection: EventDetectionResult) -> str:
    return (
        f"Explain the latest technical picture for {signal.symbol}. "
        f"Detected event category: {detection.label}. "
        f"Event rationale: {detection.rationale} "
        f"Signal anomaly: {signal.anomaly}. "
        f"Indicators: {json.dumps(signal.indicators, sort_keys=True)}. "
        "Summarize the sentiment and explain the likely market interpretation with citations."
    )


def _generation_result_payload(result: GenerationResult) -> dict[str, Any]:
    return {
        "summary": result.summary,
        "explanation": result.explanation,
        "sentiment_score": result.sentiment_score,
        "sentiment_label": result.sentiment_label,
        "citations": list(result.citations),
        "confidence": result.confidence,
        "grounded": result.grounded,
        "provider": result.provider,
        "model": result.model,
        "raw_text": result.raw_text,
        "metadata": dict(result.metadata),
    }


def _with_event_detection(
    result: GenerationResult,
    detection: EventDetectionResult,
) -> GenerationResult:
    explanation = (
        f"Detected {detection.label.lower()} via {detection.framework}: "
        f"{detection.rationale} {result.explanation}"
    ).strip()
    metadata = dict(result.metadata)
    metadata.update(
        {
            "event_category": detection.category,
            "event_label": detection.label,
            "event_framework": detection.framework,
        }
    )
    return GenerationResult(
        summary=result.summary,
        explanation=explanation,
        sentiment_score=result.sentiment_score,
        sentiment_label=result.sentiment_label,
        citations=result.citations,
        confidence=result.confidence,
        grounded=result.grounded,
        provider=result.provider,
        model=result.model,
        raw_text=result.raw_text,
        metadata=metadata,
    )


def _insight_from_result(
    *,
    symbol: str,
    event_ts: Any,
    correlation_id: str | None,
    trace_id: str | None,
    result: GenerationResult,
) -> Insight:
    return Insight(
        symbol=symbol,
        ts=event_ts,
        correlation_id=correlation_id,
        trace_id=trace_id,
        sentiment_score=result.sentiment_score,
        sentiment_label=result.sentiment_label,
        summary=result.summary,
        explanation=result.explanation,
        citations=list(result.citations),
        confidence=result.confidence,
        grounded=result.grounded,
        model=result.model,
    )


def indexed_symbols(search_store: SearchStore, index_name: str) -> Sequence[str]:
    """Test helper exposing indexed symbols from the in-memory store."""
    indices = getattr(search_store, "_indices", {})
    store = indices.get(index_name, {})
    symbols: list[str] = []
    for entry in store.values():
        metadata = entry.get("_doc", {}).get("metadata", {})
        symbol = metadata.get("symbol")
        if symbol is not None:
            symbols.append(str(symbol))
    return tuple(symbols)
