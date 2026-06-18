.PHONY: setup test lint format up down ps clean

setup:
	python3.11 -m venv .venv
	.venv/bin/python -m pip install -U pip
	.venv/bin/python -m pip install -r requirements-dev.txt

test:
	@if [ ! -d ".venv" ]; then $(MAKE) setup; fi
	.venv/bin/python -m pytest -q

lint:
	.venv/bin/ruff check .

format:
	.venv/bin/black . && .venv/bin/ruff check --fix .

up:
	docker compose up -d

down:
	docker compose down -v

ps:
	docker compose ps

clean:
	rm -rf .venv .pytest_cache .ruff_cache .mypy_cache __pycache__ *.egg-info dist build
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
