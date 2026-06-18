from pathlib import Path

SERVICE_ROOT = Path(__file__).parents[1]


def test_stream_requirements_are_pinned() -> None:
    requirements = SERVICE_ROOT / "requirements.txt"
    lines = [
        line.strip()
        for line in requirements.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert lines
    assert all("==" in line for line in lines)
    assert "fastapi==0.115.6" in lines
    assert "newrelic==13.1.1" in lines
    assert "azure-servicebus==7.12.3" in lines
    assert "redis==5.2.1" in lines
    assert "tenacity==9.0.0" in lines
    assert "elasticsearch==8.17.0" in lines


def test_stream_dockerfile_uses_runtime_pattern() -> None:
    dockerfile = (SERVICE_ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "FROM python:3.12-slim" in dockerfile
    assert "USER appuser" in dockerfile
    assert "services.stream.app:app" in dockerfile
    assert "EXPOSE 8002" in dockerfile


def test_stream_readme_documents_contract() -> None:
    readme = (SERVICE_ROOT / "README.md").read_text(encoding="utf-8")

    assert "market.raw" in readme
    assert "signals" in readme
    assert "No financial advice" in readme
