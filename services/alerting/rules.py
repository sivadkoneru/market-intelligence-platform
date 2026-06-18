"""Deterministic alerting rules for signals and insights."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from libs.common import Alert, Insight, Signal


@dataclass(frozen=True)
class AlertRuleConfig:
    rsi_overbought_threshold: float = 70.0
    rsi_oversold_threshold: float = 30.0
    volatility_threshold: float = 0.05
    sentiment_spike_threshold: float = 0.75
    high_severity_sentiment_threshold: float = 0.9


class RuleEngine:
    """Evaluate signal and insight events into zero or more alerts."""

    def __init__(self, config: AlertRuleConfig | None = None) -> None:
        self._config = config or AlertRuleConfig()

    def evaluate_signal(self, signal: Signal) -> list[Alert]:
        alerts: list[Alert] = []
        indicators = signal.indicators

        if signal.anomaly:
            alerts.append(
                self._build_alert(
                    symbol=signal.symbol,
                    rule="anomaly_flag",
                    severity="critical",
                    message=f"{signal.symbol} posted an anomalous technical move.",
                    source_kind="signal",
                    source_event_id=signal.event_id,
                    event_ts=signal.ts,
                    correlation_id=signal.correlation_id,
                    trace_id=signal.trace_id,
                )
            )

        rsi = _float_or_none(indicators.get("rsi"))
        if rsi is not None and rsi >= self._config.rsi_overbought_threshold:
            alerts.append(
                self._build_alert(
                    symbol=signal.symbol,
                    rule="rsi_overbought",
                    severity="high",
                    message=(
                        f"{signal.symbol} RSI reached {rsi:.2f}, above "
                        f"{self._config.rsi_overbought_threshold:.0f}."
                    ),
                    source_kind="signal",
                    source_event_id=signal.event_id,
                    event_ts=signal.ts,
                    correlation_id=signal.correlation_id,
                    trace_id=signal.trace_id,
                )
            )
        elif rsi is not None and rsi <= self._config.rsi_oversold_threshold:
            alerts.append(
                self._build_alert(
                    symbol=signal.symbol,
                    rule="rsi_oversold",
                    severity="medium",
                    message=(
                        f"{signal.symbol} RSI dropped to {rsi:.2f}, below "
                        f"{self._config.rsi_oversold_threshold:.0f}."
                    ),
                    source_kind="signal",
                    source_event_id=signal.event_id,
                    event_ts=signal.ts,
                    correlation_id=signal.correlation_id,
                    trace_id=signal.trace_id,
                )
            )

        volatility = _float_or_none(indicators.get("volatility"))
        if volatility is not None and volatility >= self._config.volatility_threshold:
            alerts.append(
                self._build_alert(
                    symbol=signal.symbol,
                    rule="volatility_threshold_breach",
                    severity="high",
                    message=(
                        f"{signal.symbol} volatility reached {volatility:.4f}, above "
                        f"{self._config.volatility_threshold:.4f}."
                    ),
                    source_kind="signal",
                    source_event_id=signal.event_id,
                    event_ts=signal.ts,
                    correlation_id=signal.correlation_id,
                    trace_id=signal.trace_id,
                )
            )

        return alerts

    def evaluate_insight(self, insight: Insight) -> list[Alert]:
        sentiment = float(insight.sentiment_score)
        if abs(sentiment) < self._config.sentiment_spike_threshold:
            return []

        severity = (
            "critical"
            if abs(sentiment) >= self._config.high_severity_sentiment_threshold
            else "high"
        )
        direction = "positive" if sentiment > 0 else "negative"
        return [
            self._build_alert(
                symbol=insight.symbol,
                rule="sentiment_spike",
                severity=severity,
                message=(
                    f"{insight.symbol} sentiment spiked {direction} at "
                    f"{sentiment:.2f} ({insight.sentiment_label})."
                ),
                source_kind="insight",
                source_event_id=insight.event_id,
                event_ts=insight.ts,
                correlation_id=insight.correlation_id,
                trace_id=insight.trace_id,
            )
        ]

    def _build_alert(
        self,
        *,
        symbol: str,
        rule: str,
        severity: str,
        message: str,
        source_kind: str,
        source_event_id: str,
        event_ts,
        correlation_id: str | None,
        trace_id: str | None,
    ) -> Alert:
        dedupe_key = _dedupe_key(
            source_kind=source_kind,
            source_event_id=source_event_id,
            symbol=symbol,
            rule=rule,
            event_ts=event_ts.isoformat(),
        )
        return Alert(
            symbol=symbol,
            rule=rule,
            severity=severity,
            message=message,
            dedupe_key=dedupe_key,
            ts=event_ts,
            correlation_id=correlation_id,
            trace_id=trace_id,
        )


def _dedupe_key(
    *,
    source_kind: str,
    source_event_id: str,
    symbol: str,
    rule: str,
    event_ts: str,
) -> str:
    raw = f"{source_kind}:{source_event_id}:{symbol}:{rule}:{event_ts}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    return float(value)
