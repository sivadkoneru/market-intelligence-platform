"""Pure technical indicator helpers for the stream service."""

from __future__ import annotations

from collections.abc import Sequence
from math import sqrt

import numpy as np

TrendLabel = str


def simple_moving_average(values: Sequence[float], window: int) -> float | None:
    """Return the latest simple moving average over the requested window."""
    series = _as_array(values)
    _validate_window(window)
    if len(series) < window:
        return None
    return float(np.mean(series[-window:]))


def exponential_moving_average(values: Sequence[float], window: int) -> float | None:
    """Return the latest EMA seeded from the first full SMA window."""
    series = _as_array(values)
    _validate_window(window)
    if len(series) < window:
        return None

    alpha = 2.0 / (window + 1)
    ema = float(np.mean(series[:window]))
    for value in series[window:]:
        ema = (alpha * float(value)) + ((1.0 - alpha) * ema)
    return ema


def relative_strength_index(values: Sequence[float], period: int = 14) -> float | None:
    """Return the latest RSI using Wilder smoothing."""
    series = _as_array(values)
    _validate_window(period)
    if len(series) < period + 1:
        return None

    deltas = np.diff(series)
    gains = np.clip(deltas, a_min=0.0, a_max=None)
    losses = np.clip(-deltas, a_min=0.0, a_max=None)

    avg_gain = float(np.mean(gains[:period]))
    avg_loss = float(np.mean(losses[:period]))

    for gain, loss in zip(gains[period:], losses[period:]):
        avg_gain = ((avg_gain * (period - 1)) + float(gain)) / period
        avg_loss = ((avg_loss * (period - 1)) + float(loss)) / period

    if avg_gain == 0.0 and avg_loss == 0.0:
        return 50.0
    if avg_loss == 0.0:
        return 100.0
    if avg_gain == 0.0:
        return 0.0

    relative_strength = avg_gain / avg_loss
    return float(100.0 - (100.0 / (1.0 + relative_strength)))


def rolling_volatility(values: Sequence[float], window: int) -> float | None:
    """Return the population standard deviation of simple returns over the window."""
    series = _as_array(values)
    _validate_window(window)
    if window < 2:
        raise ValueError("window must be at least 2 for volatility")
    if len(series) < window:
        return None

    window_values = series[-window:]
    if np.any(window_values[:-1] <= 0.0):
        raise ValueError("volatility requires strictly positive baseline prices")
    returns = np.diff(window_values) / window_values[:-1]
    return float(np.std(returns, ddof=0))


def detect_trend(
    values: Sequence[float],
    window: int,
    flat_tolerance: float = 1e-9,
) -> TrendLabel | None:
    """Classify the latest window as uptrend, downtrend, or flat."""
    series = _as_array(values)
    _validate_window(window)
    if window < 2:
        raise ValueError("window must be at least 2 for trend detection")
    if len(series) < window:
        return None

    window_values = series[-window:]
    slope, _ = np.polyfit(np.arange(window, dtype=float), window_values, deg=1)
    if abs(float(slope)) <= flat_tolerance:
        return "flat"
    if slope > 0:
        return "uptrend"
    return "downtrend"


def z_score_anomaly(values: Sequence[float], window: int, threshold: float = 2.0) -> bool | None:
    """Flag whether the latest value is a z-score anomaly vs the prior window.

    When the baseline variance is zero, the threshold is treated as an absolute
    delta from the baseline mean.
    """
    series = _as_array(values)
    _validate_window(window)
    if threshold <= 0:
        raise ValueError("threshold must be positive")
    if len(series) < window + 1:
        return None

    baseline = series[-(window + 1) : -1]
    latest = float(series[-1])
    mean = float(np.mean(baseline))
    std_dev = float(np.std(baseline, ddof=0))

    if std_dev == 0.0:
        return bool(abs(latest - mean) >= threshold)

    z_score = abs((latest - mean) / std_dev)
    return bool(z_score >= threshold)


def ewma_anomaly(values: Sequence[float], span: int, threshold: float = 3.0) -> bool | None:
    """Flag whether the latest value deviates from an EWMA baseline.

    When the EWMA variance is zero, the threshold is treated as an absolute
    delta from the EWMA mean.
    """
    series = _as_array(values)
    _validate_window(span)
    if threshold <= 0:
        raise ValueError("threshold must be positive")
    if len(series) < span + 1:
        return None

    history = series[:-1]
    latest = float(series[-1])
    alpha = 2.0 / (span + 1)

    ewma_mean = float(history[0])
    ewma_variance = 0.0
    for value in history[1:]:
        previous_mean = ewma_mean
        ewma_mean = (alpha * float(value)) + ((1.0 - alpha) * ewma_mean)
        residual = float(value) - previous_mean
        ewma_variance = (alpha * (residual**2)) + ((1.0 - alpha) * ewma_variance)

    ewma_std = sqrt(ewma_variance)
    if ewma_std == 0.0:
        return bool(abs(latest - ewma_mean) >= threshold)

    normalized_residual = abs(latest - ewma_mean) / ewma_std
    return bool(normalized_residual >= threshold)


def _as_array(values: Sequence[float]) -> np.ndarray:
    if len(values) == 0:
        return np.array([], dtype=float)
    return np.asarray(values, dtype=float)


def _validate_window(window: int) -> None:
    if window <= 0:
        raise ValueError("window must be positive")
