# scripts/

Operator and benchmarking entry points. These are standalone CLIs, not an
importable package — each is run by path with the repo root on `PYTHONPATH`
(the `Taskfile.yml` sets that for you).

**Portfolio project only — no financial advice, no real trades.**

## Scripts

| Script | Purpose | Task target |
|---|---|---|
| `bench.py` | Deterministic offline benchmark of the in-memory pipeline (ingestion → stream → alerting). Emits JSON latency/throughput stats. | — |
| `sb_peek.py` | Peek queued messages on a Service Bus topic without consuming them. | `task smoke:sb` |
| `seed_market_data.py` | Publish deterministic sample `market.raw` events so the API has data to serve locally. | `task seed:market` |
| `ws_smoke.py` | Subscribe to the API websocket, send a subscribe frame, and log live payloads. | `task smoke:ws` |

## Usage

```bash
# Via task (preferred — sets PYTHONPATH and uses the venv interpreter)
task smoke:sb
task seed:market
task smoke:ws

# Directly
PYTHONPATH=. .venv/bin/python scripts/sb_peek.py market.raw --messages 3
PYTHONPATH=. .venv/bin/python scripts/seed_market_data.py --events 15
PYTHONPATH=. .venv/bin/python scripts/ws_smoke.py --messages 1
PYTHONPATH=. .venv/bin/python scripts/bench.py --help
```

Every script takes `--help`.

## Inputs / outputs

- **Inputs**: CLI flags plus the shared `libs.common` settings (env vars / `.env`).
  With no configuration they resolve to the offline fakes, so nothing here needs
  live infrastructure to run.
- **Outputs**: structlog JSON on stdout. No bare `print` — see the logging rule in
  the root `CLAUDE.md`.

## Dependencies

- `libs.common` — settings, structured logging, and the bus/cache/store ports.
- `websockets` (`ws_smoke.py` only).

Because these import the top-level `libs` package, they need the repo root on
`sys.path`. `bench.py` inserts it itself; the others rely on `PYTHONPATH=.`.

## Tests

Covered by `tests/test_bench.py`, `tests/test_sb_peek.py`,
`tests/test_seed_market_data.py`, and `tests/test_ws_smoke.py`.
