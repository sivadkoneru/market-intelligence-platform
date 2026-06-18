"""Behavior tests for stream technical indicators."""

import math

import pytest

from services.stream.indicators import (
    detect_trend,
    ewma_anomaly,
    exponential_moving_average,
    relative_strength_index,
    rolling_volatility,
    simple_moving_average,
    z_score_anomaly,
)


def test_simple_moving_average_returns_latest_window_average():
    assert simple_moving_average([1, 2, 3, 4, 5], window=3) == 4.0


def test_simple_moving_average_returns_none_for_insufficient_data():
    assert simple_moving_average([1, 2], window=3) is None


def test_exponential_moving_average_is_deterministic():
    first = exponential_moving_average([1, 2, 3, 4, 5], window=3)
    second = exponential_moving_average([1, 2, 3, 4, 5], window=3)

    assert first == pytest.approx(4.0)
    assert second == pytest.approx(first)


def test_relative_strength_index_returns_none_without_enough_prices():
    assert relative_strength_index([100, 101, 102], period=3) is None


def test_relative_strength_index_returns_50_for_flat_series():
    prices = [100.0] * 15
    assert relative_strength_index(prices, period=14) == pytest.approx(50.0)


def test_relative_strength_index_returns_100_for_all_gains():
    prices = list(range(1, 17))
    assert relative_strength_index(prices, period=14) == pytest.approx(100.0)


def test_relative_strength_index_returns_0_for_all_losses():
    prices = list(range(16, 0, -1))
    assert relative_strength_index(prices, period=14) == pytest.approx(0.0)


def test_rolling_volatility_uses_returns_and_is_zero_for_flat_series():
    prices = [50.0, 50.0, 50.0, 50.0]
    assert rolling_volatility(prices, window=4) == pytest.approx(0.0)


def test_rolling_volatility_matches_expected_population_std():
    prices = [100.0, 102.0, 101.0, 103.0]
    returns = [0.02, -0.00980392156862745, 0.019801980198019802]
    mean_return = sum(returns) / len(returns)
    expected = math.sqrt(sum((value - mean_return) ** 2 for value in returns) / len(returns))
    assert rolling_volatility(prices, window=4) == pytest.approx(expected)


def test_detect_trend_distinguishes_uptrend_downtrend_and_flat():
    assert detect_trend([1, 2, 3, 4, 5], window=5) == "uptrend"
    assert detect_trend([5, 4, 3, 2, 1], window=5) == "downtrend"
    assert detect_trend([2, 2, 2, 2, 2], window=5) == "flat"


def test_detect_trend_returns_none_for_insufficient_data():
    assert detect_trend([1, 2, 3], window=5) is None


def test_detect_trend_requires_window_of_at_least_two():
    with pytest.raises(ValueError, match="window must be at least 2 for trend detection"):
        detect_trend([5.0], window=1)


def test_z_score_anomaly_returns_none_for_insufficient_history():
    assert z_score_anomaly([10, 11, 12], window=3) is None


def test_z_score_anomaly_detects_spike_and_ignores_normal_move():
    assert z_score_anomaly([10, 10, 10, 10, 14], window=4) is True
    assert z_score_anomaly([10, 10, 10, 10, 10], window=4) is False


def test_z_score_anomaly_uses_absolute_threshold_when_baseline_variance_is_zero():
    assert z_score_anomaly([10, 10, 10, 10, 10.1], window=4, threshold=0.5) is False
    assert z_score_anomaly([10, 10, 10, 10, 11.0], window=4, threshold=0.5) is True


def test_ewma_anomaly_returns_none_for_insufficient_history():
    assert ewma_anomaly([10, 10, 10], span=3) is None


def test_ewma_anomaly_detects_large_jump_and_ignores_stable_series():
    assert ewma_anomaly([10, 10, 10, 10, 20], span=4) is True
    assert ewma_anomaly([10, 10, 10, 10, 10], span=4) is False


def test_ewma_anomaly_uses_absolute_threshold_when_variance_is_zero():
    assert ewma_anomaly([10, 10, 10, 10, 10.1], span=4, threshold=0.5) is False
    assert ewma_anomaly([10, 10, 10, 10, 11.0], span=4, threshold=0.5) is True


@pytest.mark.parametrize(
    ("func", "kwargs"),
    [
        (simple_moving_average, {"window": 0}),
        (exponential_moving_average, {"window": 0}),
        (relative_strength_index, {"period": 0}),
        (rolling_volatility, {"window": 0}),
        (detect_trend, {"window": 0}),
        (z_score_anomaly, {"window": 0}),
        (ewma_anomaly, {"span": 0}),
    ],
)
def test_indicator_functions_reject_non_positive_windows(func, kwargs):
    with pytest.raises(ValueError, match="window must be positive"):
        func([1, 2, 3, 4], **kwargs)


def test_threshold_based_functions_require_positive_thresholds():
    with pytest.raises(ValueError, match="threshold must be positive"):
        z_score_anomaly([1, 2, 3, 4, 5], window=4, threshold=0)

    with pytest.raises(ValueError, match="threshold must be positive"):
        ewma_anomaly([1, 2, 3, 4, 5], span=4, threshold=0)


def test_rolling_volatility_requires_window_of_at_least_two():
    with pytest.raises(ValueError, match="window must be at least 2"):
        rolling_volatility([1, 2, 3], window=1)


def test_rolling_volatility_requires_strictly_positive_baseline_prices():
    with pytest.raises(ValueError, match="volatility requires strictly positive baseline prices"):
        rolling_volatility([0.0, 1.0, 2.0], window=3)
