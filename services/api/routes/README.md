# API Routes

Route modules for the FastAPI API service.

Portfolio project only. No financial advice and no real trades.

## Purpose

- Keep the REST surface grouped by feature area
- Reuse the shared `APIService` dependency instead of embedding storage logic in endpoints
- Make the future `WS /ws/stream` addition straightforward without reshaping the app

## Modules

- `market.py` - symbols, latest market data, history, and indicators
- `signals.py` - latest signal feed snapshots
- `alerts.py` - latest alerts feed snapshots
- `insights.py` - latest insight per symbol

## Dependencies

- `services.api.dependencies.get_api_service`
- `services.api.service.APIService`
- `libs.common` shared ports and schema models
