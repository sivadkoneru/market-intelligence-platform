"""
Structural invariants for the source tree.

These encode project rules that were previously only prose in ``CLAUDE.md``, so
a drifting layout fails the gate instead of being noticed by eye at review time.
"""

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOTS = (REPO_ROOT / "libs", REPO_ROOT / "services")
SERVICE_NAMES = ("ingestion", "stream", "ai", "alerting", "api")

EXCLUDED_DIR_NAMES = {"__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache"}


def _source_package_dirs() -> list[Path]:
    """Every directory under libs/ and services/ that holds Python modules."""
    packages: list[Path] = []
    for root in SOURCE_ROOTS:
        packages.append(root)
        for path in sorted(root.rglob("*")):
            if not path.is_dir():
                continue
            if any(part in EXCLUDED_DIR_NAMES for part in path.parts):
                continue
            if path.name == "tests":
                continue
            if not any(path.glob("*.py")):
                continue
            packages.append(path)
    return packages


def _test_package_dirs() -> list[Path]:
    return [
        path
        for root in SOURCE_ROOTS
        for path in sorted(root.rglob("tests"))
        if path.is_dir() and "__pycache__" not in path.parts
    ]


def _relative(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT))


@pytest.mark.parametrize("package", _source_package_dirs(), ids=_relative)
def test_every_source_package_ships_a_readme(package):
    """Project rule 1: every module/dir ships a README.md."""
    assert (package / "README.md").exists(), f"{_relative(package)}/README.md is missing"


@pytest.mark.parametrize("package", _source_package_dirs(), ids=_relative)
def test_every_source_package_is_importable(package):
    """A missing __init__.py silently changes how pytest and mypy resolve modules."""
    assert (package / "__init__.py").exists(), f"{_relative(package)}/__init__.py is missing"


@pytest.mark.parametrize("package", _test_package_dirs(), ids=_relative)
def test_every_test_package_declares_itself(package):
    """
    Test directories are packages too.

    Without ``__init__.py`` pytest's prepend import mode names a module by its
    bare filename, so two same-named test files in different services collide
    at collection time. Four of the five services had one; ingestion did not.
    """
    assert (package / "__init__.py").exists(), f"{_relative(package)}/__init__.py is missing"


@pytest.mark.parametrize("service", SERVICE_NAMES)
def test_every_service_ships_its_runtime_files(service):
    root = REPO_ROOT / "services" / service
    for required in ("Dockerfile", "requirements.txt", "README.md", "app.py"):
        assert (root / required).exists(), f"services/{service}/{required} is missing"


@pytest.mark.parametrize("service", SERVICE_NAMES)
def test_every_service_readme_carries_the_disclaimer(service):
    """Project rule 5 — the disclaimer is not optional on any surface."""
    readme = (REPO_ROOT / "services" / service / "README.md").read_text(encoding="utf-8")

    assert "no financial advice" in readme.lower()


def test_services_do_not_import_each_other():
    """Services communicate over the bus, never by importing a sibling."""
    offenders: list[str] = []
    for service in SERVICE_NAMES:
        root = REPO_ROOT / "services" / service
        for module in root.rglob("*.py"):
            if "tests" in module.parts or "__pycache__" in module.parts:
                continue
            source = module.read_text(encoding="utf-8")
            for other in SERVICE_NAMES:
                if other == service:
                    continue
                if f"from services.{other}" in source or f"import services.{other}" in source:
                    offenders.append(f"{_relative(module)} imports services.{other}")

    assert not offenders, "cross-service imports found: " + "; ".join(offenders)


def test_libs_common_does_not_import_services():
    """The dependency arrow points one way: services → libs, never back."""
    offenders: list[str] = []
    for module in (REPO_ROOT / "libs").rglob("*.py"):
        if "__pycache__" in module.parts:
            continue
        source = module.read_text(encoding="utf-8")
        if "from services." in source or "import services." in source:
            offenders.append(_relative(module))

    assert not offenders, f"libs must not import services: {offenders}"
