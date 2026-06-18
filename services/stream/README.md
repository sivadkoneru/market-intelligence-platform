# Stream Service

Computes deterministic technical indicators for market price streams. This module is pure and offline-friendly so later stream processors can reuse the same math in tests and in message handling code.

## Indicators

- SMA
- EMA
- RSI
- Rolling volatility
- Trend detection
- Z-score anomaly detection
- EWMA anomaly detection

## Inputs

- Ordered price series as Python sequences of numeric values
- Integer window or period arguments for rolling calculations
- Positive anomaly thresholds for z-score and EWMA checks

For `rolling_volatility`, baseline prices used as return denominators must be strictly positive.

## Outputs

- `float` for SMA, EMA, RSI, and rolling volatility when enough data is available
- `str` trend labels: `uptrend`, `downtrend`, or `flat`
- `bool` anomaly flags for z-score and EWMA detection
- `None` when the input series does not yet contain enough data for the requested calculation

For anomaly detection, if the baseline variance is zero, the threshold is interpreted as an absolute delta from the baseline mean rather than a normalized score.

## Dependencies

- Python standard library
- `numpy`

No network, storage, clocks, randomness, or live infrastructure are involved.

## Usage

```python
from services.stream.indicators import (
    detect_trend,
    exponential_moving_average,
    relative_strength_index,
    rolling_volatility,
    simple_moving_average,
)

prices = [100.0, 101.0, 102.5, 104.0, 103.5]

sma = simple_moving_average(prices, window=3)
ema = exponential_moving_average(prices, window=3)
rsi = relative_strength_index(prices, period=4)
volatility = rolling_volatility(prices, window=5)
trend = detect_trend(prices, window=5)
```

All functions are deterministic, perform no I/O, and return `None` when there is not enough data yet for the requested calculation.

Portfolio project only. No financial advice and no real trades.
