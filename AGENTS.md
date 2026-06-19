# AGENTS.md - Codex Guide

This file is the quick-start contract for Codex and other coding agents working in this repo. `CLAUDE.md` remains the full guide; this file keeps the highest-signal instructions close to the root for agent discovery.

## Project State

- Branch: `feat/market-intel-platform`.
- T0-T21 are complete. Do not rebuild completed tasks.
- Final-review fixes are already included at the current tip: Service Bus sectioned body decoding, New Relic runtime pins, and Druid extension cleanup.
- This is a portfolio project only: no financial advice, no real trades, no real capital at risk.

## Start Here

1. Read `CLAUDE.md` before non-trivial edits.
2. Check `git status --short` before changing files. The worktree may contain user edits; do not revert unrelated changes.
3. Use `rg` / `rg --files` for search.
4. Use `apply_patch` for manual edits.
5. Keep changes small and aligned with existing service patterns.

## Verification

Canonical commands:

```bash
task lint
task test
docker compose config -q
```

If `task` is unavailable in the shell, use the underlying commands:

```bash
.venv/bin/ruff check .
.venv/bin/python -m pytest -q
docker compose config -q
```

`task test` must stay offline: no live infra, secrets, or network calls. Use in-memory fakes and `MOCK_LLM` for tests.

## Coding Rules

- No bare `print`; use `libs.common.get_logger`.
- Every new module or directory needs a `README.md`.
- Add tests for created or modified behavior.
- Keep optional heavy SDKs import-guarded.
- Keep `ServiceBusBus` compatible with raw bytes, strings, and iterable AMQP body sections.
- Keep Druid local config limited to the approved extension set.
- Preserve the no-financial-advice disclaimer in user-facing docs and API root behavior.

## Runtime Notes

- API host port is `8000`; API container port is `8005`.
- App services run on host/container ports `8001` through `8004`.
- Smoke helpers are `task smoke:sb` and `task smoke:ws`.
- A full `docker compose up -d --build` requires a working local Docker daemon; `docker compose config -q` is the offline structural gate.

## Files Agents Should Avoid

- Do not stage or commit `.venv/`, caches, build artifacts, or untracked planning files unless explicitly asked.
- Do not edit `.env` or add secrets.
