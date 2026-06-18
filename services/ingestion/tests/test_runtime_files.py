from pathlib import Path

SERVICE_ROOT = Path(__file__).parents[1]


def test_ingestion_requirements_are_pinned() -> None:
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


def test_ingestion_dockerfile_uses_runtime_pattern() -> None:
    dockerfile = (SERVICE_ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "FROM python:3.12-slim" in dockerfile
    assert "USER appuser" in dockerfile
    assert "services.ingestion.app:app" in dockerfile
    assert "EXPOSE 8001" in dockerfile
