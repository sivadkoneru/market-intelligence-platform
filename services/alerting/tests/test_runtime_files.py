from pathlib import Path

SERVICE_ROOT = Path(__file__).parents[1]


def test_alerting_requirements_are_pinned() -> None:
    requirements = SERVICE_ROOT / "requirements.txt"
    lines = [
        line.strip()
        for line in requirements.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert lines
    assert all("==" in line for line in lines)
    assert "fastapi==0.115.6" in lines
    assert "azure-servicebus==7.12.3" in lines
    assert "tenacity==9.0.0" in lines


def test_alerting_dockerfile_uses_runtime_pattern() -> None:
    dockerfile = (SERVICE_ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "FROM python:3.12-slim" in dockerfile
    assert "USER appuser" in dockerfile
    assert "services.alerting.app:app" in dockerfile
    assert "EXPOSE 8004" in dockerfile


def test_alerting_readme_documents_contract() -> None:
    readme = (SERVICE_ROOT / "README.md").read_text(encoding="utf-8")

    assert "signals" in readme
    assert "insights" in readme
    assert "alerts" in readme
    assert "No financial advice" in readme
    assert "python -m services.alerting.replay" in readme
