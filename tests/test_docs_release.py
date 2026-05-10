"""Documentation layout and v1.1.0 release structure."""

from __future__ import annotations

import re

from wildfire_smoke import __version__
from wildfire_smoke.settings import repo_root


def test_version_is_1_1_0() -> None:
    assert __version__ == "1.1.0"


def test_mkdocs_and_site_layout_exists() -> None:
    root = repo_root()
    mk = root / "mkdocs.yml"
    assert mk.is_file()
    # Avoid yaml.safe_load: mkdocs.yml uses !!python/name tags (superfences/mermaid).
    text = mk.read_text(encoding="utf-8")
    for needle in (
        "getting-started.md",
        "user-guide/calibration.md",
        "operations/db-doctor.md",
        "reference/make-targets.md",
        "release/v1.1.0.md",
        "development/ci.md",
        "models/plume-dispersion.md",
        "stylesheets/extra.css",
    ):
        assert needle in text
    assert "site_url: https://" in text and "github.io" in text
    assert "pymdownx.superfences" in text
    assert "name: mermaid" in text
    assert "mermaid.min.js" in text
    assert "pymdownx.highlight" in text
    # Task-oriented nav must not use phase rollout labels
    assert "Phase " not in text


def test_nav_pages_exist_on_disk() -> None:
    root = repo_root()
    expected = [
        "docs/index.md",
        "docs/stylesheets/extra.css",
        "docs/getting-started.md",
        "docs/development/ci.md",
        "docs/models/plume-dispersion.md",
        "docs/architecture/overview.md",
        "docs/architecture/dataflow.md",
        "docs/architecture/data-model.md",
        "docs/architecture/operational-model.md",
        "docs/user-guide/no-secrets-demo.md",
        "docs/user-guide/live-ingest.md",
        "docs/user-guide/dashboards.md",
        "docs/user-guide/calibration.md",
        "docs/user-guide/alerting.md",
        "docs/user-guide/dlq-and-replay.md",
        "docs/user-guide/exports.md",
        "docs/operations/db-bootstrap.md",
        "docs/operations/db-doctor.md",
        "docs/operations/release-check.md",
        "docs/operations/troubleshooting.md",
        "docs/reference/make-targets.md",
        "docs/reference/environment.md",
        "docs/reference/data-sources.md",
        "docs/reference/tables-and-views.md",
        "docs/reference/risk-models.md",
        "docs/reference/limitations.md",
        "docs/release/v1.1.0.md",
        "docs/release/v1.0.0-checklist.md",
    ]
    for rel in expected:
        assert (root / rel).is_file(), rel


def test_docs_workflow_exists() -> None:
    p = repo_root() / ".github/workflows/docs.yml"
    assert p.is_file()
    txt = p.read_text(encoding="utf-8")
    assert "docs-check" in txt or "mkdocs" in txt.lower()
    assert "pages: write" in txt
    assert "id-token: write" in txt
    assert "workflow_dispatch" in txt
    assert "actions/deploy-pages" in txt or "deploy-pages@" in txt


def test_makefile_has_docs_targets() -> None:
    mf = (repo_root() / "Makefile").read_text(encoding="utf-8")
    assert "docs-check:" in mf
    assert "mkdocs build --strict" in mf


def test_release_check_invokes_docs_check() -> None:
    txt = (repo_root() / "scripts/release_check.sh").read_text(encoding="utf-8")
    assert "make docs-check" in txt
    assert "docs/stylesheets/extra.css" in txt


def test_changelog_has_v1_1_0() -> None:
    cl = (repo_root() / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "[1.1.0]" in cl


def test_readme_avoids_phase_chronicle_headings() -> None:
    """README front door should not use 'Phase N' rollout headings."""
    readme = (repo_root() / "README.md").read_text(encoding="utf-8")
    assert not re.search(r"\*\*Phase\s+[0-9]", readme)


def test_pyproject_has_docs_extra() -> None:
    txt = (repo_root() / "pyproject.toml").read_text(encoding="utf-8")
    assert "docs = [" in txt or 'docs = [' in txt
    assert "mkdocs" in txt
