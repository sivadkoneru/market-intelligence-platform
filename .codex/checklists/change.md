# Change Checklist

Before editing:

- Read `AGENTS.md` and the relevant section of `CLAUDE.md`.
- Run `git status --short` and identify unrelated user changes.
- Locate existing patterns with `rg` before adding new abstractions.

While editing:

- Use `apply_patch` for manual file edits.
- Keep new code ASCII unless the file already uses Unicode for a clear reason.
- Add or update focused tests for changed behavior.
- Keep optional integrations import-guarded and offline-safe.

Before finishing:

- Run focused tests for touched behavior.
- Run `.venv/bin/ruff check .` or `task lint`.
- Run `.venv/bin/python -m pytest -q` or `task test` when the worktree is in a state where existing unrelated changes will not fail the suite.
- Run `docker compose config -q` after compose, env, Dockerfile, or runtime dependency changes.
- Report any verification you could not run and why.

