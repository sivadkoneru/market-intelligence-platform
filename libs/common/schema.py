"""
Common Pydantic v2 event models and topic constants for the market intelligence platform.

All services import from this module — do NOT define per-service duplicate models.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Topic constants
# ---------------------------------------------------------------------------

TOPIC_MARKET_RAW: str = "market.raw"
TOPIC_NEWS_RAW: str = "news.raw"
TOPIC_SIGNALS: str = "signals"
TOPIC_INSIGHTS: str = "insights"
TOPIC_ALERTS: str = "alerts"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _uuid4_str() -> str:
    return str(uuid4())


# ---------------------------------------------------------------------------
# Base mixin — every event carries these fields
# ---------------------------------------------------------------------------


class EventBase(BaseModel):
    """Mixin that adds observability fields to every event model."""

    event_id: str = Field(default_factory=_uuid4_str)
    ts: datetime = Field(default_factory=_utcnow)
    correlation_id: Optional[str] = None
    trace_id: Optional[str] = None

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# Market events
# ---------------------------------------------------------------------------


class MarketEvent(EventBase):
    """A single trade or ticker event from an exchange feed."""

    symbol: str
    source: str
    event_type: str  # "trade" | "ticker"
    price: float
    volume: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None


# ---------------------------------------------------------------------------
# News events
# ---------------------------------------------------------------------------


class NewsEvent(EventBase):
    """A news or social-media item linked to one or more symbols."""

    source: str
    title: str
    body: str
    url: Optional[str] = None
    symbols: list[str]
    author: Optional[str] = None


# ---------------------------------------------------------------------------
# Signal (computed by stream service)
# ---------------------------------------------------------------------------


class Signal(EventBase):
    """Technical-indicator snapshot for a symbol, output of the stream service."""

    symbol: str
    source: str = "stream"
    indicators: dict[str, Optional[float]]
    anomaly: bool = False


# ---------------------------------------------------------------------------
# Insight (output of AI-analysis service)
# ---------------------------------------------------------------------------


class Insight(EventBase):
    """RAG-driven LLM insight for a symbol."""

    symbol: str
    sentiment_score: float
    sentiment_label: str
    summary: str
    explanation: str
    citations: list[str] = Field(default_factory=list)
    confidence: float
    grounded: bool
    model: str


# ---------------------------------------------------------------------------
# Alert (output of alerting service)
# ---------------------------------------------------------------------------


class Alert(EventBase):
    """A rule-triggered alert for a symbol."""

    symbol: str
    rule: str
    severity: str
    message: str
    dedupe_key: str


# ---------------------------------------------------------------------------
# Idempotency key helper
# ---------------------------------------------------------------------------


def market_event_key(symbol: str, ts: datetime, source: str) -> str:
    """
    Return a stable, deterministic idempotency key for a market event.

    Keyed on (symbol, ts.isoformat(), source) — used for Azure Service Bus
    duplicate-detection and Redis idempotency checks.
    """
    raw = f"{symbol}:{ts.isoformat()}:{source}"
    return hashlib.sha256(raw.encode()).hexdigest()
