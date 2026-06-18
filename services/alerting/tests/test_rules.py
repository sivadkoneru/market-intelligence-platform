from datetime import datetime, timezone

from libs.common import Insight, Signal
from services.alerting.rules import RuleEngine


def test_signal_rules_emit_threshold_and_anomaly_alerts() -> None:
    signal = Signal(
        event_id="sig-1",
        ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
        symbol="BTCUSDT",
        indicators={
            "rsi": 78.5,
            "volatility": 0.08,
            "trend": 1.0,
        },
        anomaly=True,
    )

    alerts = RuleEngine().evaluate_signal(signal)

    assert [alert.rule for alert in alerts] == [
        "anomaly_flag",
        "rsi_overbought",
        "volatility_threshold_breach",
    ]
    assert all(alert.symbol == "BTCUSDT" for alert in alerts)
    assert len({alert.dedupe_key for alert in alerts}) == 3


def test_signal_rules_emit_oversold_alert() -> None:
    signal = Signal(
        event_id="sig-2",
        ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
        symbol="ETHUSDT",
        indicators={"rsi": 24.0, "volatility": 0.01},
        anomaly=False,
    )

    alerts = RuleEngine().evaluate_signal(signal)

    assert len(alerts) == 1
    assert alerts[0].rule == "rsi_oversold"
    assert alerts[0].severity == "medium"


def test_insight_rules_emit_sentiment_spike_alert() -> None:
    insight = Insight(
        event_id="ins-1",
        ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
        symbol="SOLUSDT",
        sentiment_score=-0.92,
        sentiment_label="very_negative",
        summary="Momentum reversed.",
        explanation="A sharp negative catalyst hit sentiment.",
        confidence=0.88,
        grounded=True,
        model="mock-llm",
    )

    alerts = RuleEngine().evaluate_insight(insight)

    assert len(alerts) == 1
    assert alerts[0].rule == "sentiment_spike"
    assert alerts[0].severity == "critical"
    assert "negative" in alerts[0].message


def test_insight_rules_ignore_small_sentiment_moves() -> None:
    insight = Insight(
        event_id="ins-2",
        ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
        symbol="ADAUSDT",
        sentiment_score=0.2,
        sentiment_label="neutral",
        summary="Routine update.",
        explanation="Nothing unusual changed.",
        confidence=0.6,
        grounded=True,
        model="mock-llm",
    )

    assert RuleEngine().evaluate_insight(insight) == []
