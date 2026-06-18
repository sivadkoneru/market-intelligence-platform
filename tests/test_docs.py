"""Smoke tests for required project documentation."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DOCS_ROOT = REPO_ROOT / "docs"
ADRS_ROOT = DOCS_ROOT / "ADRs"
REQUIRED_DOCS = [
    DOCS_ROOT / "ARCHITECTURE.md",
    DOCS_ROOT / "SEQUENCE.md",
    DOCS_ROOT / "API.md",
    DOCS_ROOT / "AZURE_PRODUCTION.md",
    ADRS_ROOT / "README.md",
    ADRS_ROOT / "0001-service-bus-emulator-topology.md",
    ADRS_ROOT / "0002-druid-micro-quickstart.md",
    ADRS_ROOT / "0003-shared-schema-common-clients.md",
    ADRS_ROOT / "0004-mock-first-llm-rag-guardrails.md",
    ADRS_ROOT / "0005-observability-grafana.md",
]
FORBIDDEN_TECH_TERMS = ("Prometheus", "OpenTelemetry", "Kafka", "Timescale", "Qdrant")


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_required_docs_exist() -> None:
    for path in REQUIRED_DOCS:
        assert path.exists(), f"{path.relative_to(REPO_ROOT)} is missing"


def test_architecture_doc_names_core_services_and_disclaimer() -> None:
    content = _read(DOCS_ROOT / "ARCHITECTURE.md")

    for term in ("ingestion", "stream", "ai-analysis", "alerting", "api"):
        assert term in content
    for term in ("market.raw", "news.raw", "signals", "insights", "alerts"):
        assert term in content
    assert "No financial advice" in content


def test_sequence_doc_contains_mermaid_paths() -> None:
    content = _read(DOCS_ROOT / "SEQUENCE.md")

    assert content.count("```mermaid") >= 2
    assert "publish market.raw" in content
    assert "publish news.raw" in content
    assert "publish alerts" in content
    assert "api-ws subscription" in content


def test_api_doc_matches_current_route_surface() -> None:
    content = _read(DOCS_ROOT / "API.md")

    for route in (
        "GET /symbols",
        "GET /market/{symbol}/latest",
        "GET /market/{symbol}/history",
        "GET /indicators/{symbol}",
        "GET /signals",
        "GET /alerts",
        "GET /insights/{symbol}",
        "WS /ws/stream",
    ):
        assert route in content
    assert "ws://localhost:8000/ws/stream" in content


def test_adr_directory_has_readme_and_records() -> None:
    records = sorted(ADRS_ROOT.glob("[0-9][0-9][0-9][0-9]-*.md"))

    assert (ADRS_ROOT / "README.md").exists()
    expected_records = sorted(
        path for path in REQUIRED_DOCS if path.parent == ADRS_ROOT and path.name[0].isdigit()
    )
    assert records == expected_records
    for path in records:
        content = _read(path)
        assert "Status: Accepted" in content
        assert "## Context" in content
        assert "## Decision" in content
        assert "## Consequences" in content


def test_azure_production_mapping_is_not_iac() -> None:
    content = _read(DOCS_ROOT / "AZURE_PRODUCTION.md")

    assert "Azure Service Bus namespace" in content
    assert "Azure Cache for Redis" in content
    assert "It is not an IaC plan" in content


def test_docs_do_not_introduce_banned_stack_terms() -> None:
    for path in REQUIRED_DOCS:
        content = _read(path)
        for term in FORBIDDEN_TECH_TERMS:
            assert term not in content, f"{term} appears in {path.relative_to(REPO_ROOT)}"
