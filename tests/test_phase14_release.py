from __future__ import annotations

import json
import subprocess

import pytest

from wildfire_smoke import __version__
from wildfire_smoke.db_doctor import EXPECTED_FN_PARAM_COUNT
from wildfire_smoke.release_manifest import build_manifest, redact_env_snapshot
from wildfire_smoke.settings import repo_root


def test_version_is_1_1_0() -> None:
    assert __version__ == "1.1.0"


def test_migration_013_drops_fn_overloads_then_defines_canonical() -> None:
    txt = (repo_root() / "sql/migrations/013_phase14_canonical_alert_function.sql").read_text(encoding="utf-8")
    assert "DO $$" in txt
    assert "DROP FUNCTION IF EXISTS analytics.fn_alert_candidates" in txt
    assert "pg_proc" in txt and "proname = 'fn_alert_candidates'" in txt
    assert "CREATE OR REPLACE FUNCTION analytics.fn_alert_candidates" in txt
    assert "CREATE OR REPLACE VIEW analytics.v_alert_candidates" in txt


def test_phase9_view_stub_points_at_migration() -> None:
    stub = (repo_root() / "sql/views/zzz_phase9_fn_alert_candidates.sql").read_text(encoding="utf-8")
    assert "013_phase14_canonical_alert_function.sql" in stub


def test_initdb_phase14_stub_exists() -> None:
    p = repo_root() / "docker/postgres/initdb/78_phase14_canonical_alert_drop_stub.sql"
    assert p.is_file()
    assert "DROP FUNCTION IF EXISTS analytics.fn_alert_candidates" in p.read_text(encoding="utf-8")


def test_github_issue_template_exists() -> None:
    root = repo_root()
    assert (root / ".github/ISSUE_TEMPLATE/ops_feedback.yml").is_file()
    assert (root / ".github/ISSUE_TEMPLATE/config.yml").is_file()


def test_release_docs_and_checklist_exist() -> None:
    root = repo_root()
    assert (root / "docs/release/v1.0.0.md").is_file()
    assert (root / "docs/release/v1.0.1.md").is_file()
    assert (root / "docs/release/v1.1.0.md").is_file()
    assert (root / "docs/release/v1.0.0-checklist.md").is_file()


def test_changelog_documents_v1_releases() -> None:
    cl = (repo_root() / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "[1.0.0]" in cl
    assert "[1.0.1]" in cl
    assert "[1.1.0]" in cl


def test_pyproject_declares_parquet_extra() -> None:
    txt = (repo_root() / "pyproject.toml").read_text(encoding="utf-8")
    assert 'parquet = [' in txt or "parquet = [" in txt
    assert "pyarrow" in txt


def test_release_manifest_redacts_sensitive_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "secret-openai")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret-pg")
    snap = redact_env_snapshot()
    dumped = json.dumps(snap)
    assert "secret-openai" not in dumped
    assert "secret-pg" not in dumped


def test_release_manifest_build_has_non_claims() -> None:
    m = build_manifest()
    assert m["version"] == "1.1.0"
    assert "known_non_claims" in m
    assert any("public health" in x.lower() for x in m["known_non_claims"])


def test_fn_alert_expected_param_count_matches_python_call() -> None:
    # alerts.fetch_candidates passes 23 Python parameters into SQL call
    assert EXPECTED_FN_PARAM_COUNT == 23


def test_release_check_script_references_v1_0_0() -> None:
    txt = (repo_root() / "scripts/release_check.sh").read_text(encoding="utf-8")
    assert "v1.0.0.md" in txt


def test_compose_repair_and_fresh_volume_scripts_shell_syntax() -> None:
    root = repo_root()
    for name in ("repair_alert_function.sh", "release_fresh_volume_test.sh", "db_doctor.sh", "write_release_manifest.sh"):
        subprocess.run(["bash", "-n", str(root / "scripts" / name)], check=True)


def test_bootstrap_skips_phase14_until_last() -> None:
    txt = (repo_root() / "scripts/bootstrap_db.sh").read_text(encoding="utf-8")
    assert "PHASE14_MIGRATION_BASENAME" in txt
    assert "Applying ${PHASE14_MIGRATION_BASENAME} last" in txt
