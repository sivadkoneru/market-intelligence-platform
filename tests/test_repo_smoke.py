"""Smoke test: repo structure, README, and tooling exist."""
from pathlib import Path


def test_key_directories_exist():
    """Assert that key directories exist."""
    repo_root = Path(__file__).parent.parent
    required_dirs = [
        "libs",
        "libs/common",
        "services",
        "services/ingestion",
        "services/stream",
        "services/ai",
        "services/alerting",
        "services/api",
        "infra",
        "docs",
        "scripts",
        "tests",
    ]
    for dir_name in required_dirs:
        dir_path = repo_root / dir_name
        assert dir_path.exists() and dir_path.is_dir(), f"Directory {dir_name} does not exist"


def test_readme_has_disclaimer():
    """Assert README.md contains disclaimer language."""
    repo_root = Path(__file__).parent.parent
    readme_path = repo_root / "README.md"
    assert readme_path.exists(), "README.md does not exist"

    content = readme_path.read_text(encoding="utf-8").lower()
    assert "disclaimer" in content, "README.md missing 'Disclaimer' section"
    assert "no financial advice" in content, "README.md missing 'no financial advice' phrase"


def test_makefile_exists():
    """Assert Makefile exists."""
    repo_root = Path(__file__).parent.parent
    makefile_path = repo_root / "Makefile"
    assert makefile_path.exists(), "Makefile does not exist"


def test_gitignore_exists():
    """Assert .gitignore exists."""
    repo_root = Path(__file__).parent.parent
    gitignore_path = repo_root / ".gitignore"
    assert gitignore_path.exists(), ".gitignore does not exist"
