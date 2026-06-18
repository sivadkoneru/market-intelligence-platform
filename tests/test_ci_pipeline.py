"""Focused checks for the Azure DevOps pipeline and top-level README."""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).parent.parent
PIPELINE_FILE = REPO_ROOT / "azure-pipelines.yml"
README_FILE = REPO_ROOT / "README.md"


def load_pipeline() -> dict:
    assert PIPELINE_FILE.exists(), "azure-pipelines.yml does not exist"
    return yaml.safe_load(PIPELINE_FILE.read_text(encoding="utf-8"))


def test_azure_pipeline_covers_push_pr_python_311_lint_test_and_compose_validation() -> None:
    pipeline = load_pipeline()
    steps = pipeline["steps"]
    script_text = "\n".join(
        step.get("script", "") for step in steps if isinstance(step, dict) and "script" in step
    )

    assert pipeline["trigger"]
    assert pipeline["pr"]
    assert pipeline["pool"]["vmImage"] == "ubuntu-latest"
    assert pipeline["variables"]["PYTHON_VERSION"] == "3.11"

    python_version_step = next(
        step
        for step in steps
        if isinstance(step, dict) and step.get("task") == "UsePythonVersion@0"
    )
    assert python_version_step["inputs"]["versionSpec"] == "$(PYTHON_VERSION)"

    assert "docker compose config -q" in script_text
    assert "ruff check" in script_text
    assert "pytest -q" in script_text
    assert "print(" not in script_text


def test_readme_finalization_mentions_quickstart_docs_ports_smoke_and_disclaimer() -> None:
    content = README_FILE.read_text(encoding="utf-8")

    for snippet in (
        "cp .env.example .env",
        "task up",
        "task setup",
        "task lint",
        "task test",
        "task smoke:sb",
        "task smoke:ws",
        "docs/ARCHITECTURE.md",
        "docs/API.md",
        "docs/BENCHMARKS.md",
        "Current Ports",
        "http://localhost:8000",
        "8001",
        "8005",
        "no financial advice",
    ):
        assert snippet in content
