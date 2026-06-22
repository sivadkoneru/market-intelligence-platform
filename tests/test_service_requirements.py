"""
test_service_requirements.py — guard service requirements against the async-ES gap.

``libs/common/es.py`` builds ``elasticsearch.AsyncElasticsearch`` (the real
``ElasticsearchStore``, selected by ``get_search_store()`` whenever
``ELASTICSEARCH_URL`` is set). Its async transport node (``AiohttpHttpNode``)
requires ``aiohttp``, which the ``elasticsearch`` package does NOT pull in by
default. ``task test`` uses the in-memory fake, so a service that pins
``elasticsearch`` without ``aiohttp`` builds fine but dies at container startup
with:

    ValueError: You must have 'aiohttp' installed to use AiohttpHttpNode

These tests fail offline if any service reintroduces that gap.
"""

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent
SERVICES_DIR = REPO_ROOT / "services"

REQUIREMENTS_FILES = sorted(SERVICES_DIR.glob("*/requirements.txt"))


def parse_pins(path: Path) -> dict[str, str]:
    """Return {package: version} from a requirements file (ignores comments/blanks)."""
    pins: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        name, sep, version = line.partition("==")
        pins[name.strip().lower()] = version.strip() if sep else ""
    return pins


def test_requirements_files_discovered() -> None:
    assert REQUIREMENTS_FILES, "No services/*/requirements.txt files found"


@pytest.mark.parametrize("req_file", REQUIREMENTS_FILES, ids=lambda p: p.parent.name)
def test_elasticsearch_services_pin_aiohttp(req_file: Path) -> None:
    """Any service using the elasticsearch client must also ship aiohttp."""
    pins = parse_pins(req_file)
    if "elasticsearch" not in pins:
        pytest.skip(f"{req_file.parent.name} does not use elasticsearch")
    assert "aiohttp" in pins, (
        f"{req_file.parent.name}/requirements.txt pins 'elasticsearch' but not 'aiohttp'; "
        "the async ElasticsearchStore (AsyncElasticsearch) fails at startup without it"
    )
    assert pins["aiohttp"], f"{req_file.parent.name}: aiohttp must be version-pinned (==)"
