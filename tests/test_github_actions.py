from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


def _load_workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_github_actions_ci_targets_main_prs_and_pushes() -> None:
    workflow = _load_workflow()
    triggers = workflow.get("on", workflow.get(True))

    assert triggers["pull_request"]["branches"] == ["main"]
    assert triggers["push"]["branches"] == ["main"]


def test_github_actions_ci_runs_repo_quality_gates() -> None:
    workflow = _load_workflow()
    jobs = workflow["jobs"]
    python_steps = "\n".join(
        step.get("run", "") for step in jobs["python-checks"]["steps"]
    )
    docker_steps = "\n".join(
        step.get("run", "") for step in jobs["docker-build"]["steps"]
    )

    assert ".venv/bin/python -m pip install -r requirements-dev.txt" in python_steps
    assert "docker compose config -q" in python_steps
    assert ".venv/bin/ruff check ." in python_steps
    assert ".venv/bin/python -m pytest -q" in python_steps
    assert "docker compose config -q" in docker_steps
    assert "docker compose build" in docker_steps


def test_github_actions_directories_are_documented() -> None:
    assert (ROOT / ".github" / "README.md").exists()
    assert (ROOT / ".github" / "workflows" / "README.md").exists()

