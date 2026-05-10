from __future__ import annotations

import builtins
import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from wildfire_smoke import __version__
from wildfire_smoke.export_calibration import (
    EXPORTS,
    build_metadata,
    csv_to_parquet,
    export_run,
    redact_db_host,
)
from wildfire_smoke.settings import repo_root


def test_redact_db_host_nonlocal() -> None:
    assert redact_db_host("db.internal.example") == "[redacted]"
    assert redact_db_host("localhost") == "localhost"
    assert redact_db_host("") == "localhost"


def test_exports_cover_named_calibration_views() -> None:
    names = {fname for fname, _sql in EXPORTS}
    assert "dispersion_aq_evidence_summary.csv" in names
    assert "risk_observation_coverage.csv" in names
    assert len(EXPORTS) == 8


def test_build_metadata_redacts_remote_host_and_row_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "wildfire_smoke.export_calibration.fetch_model_versions",
        lambda _s: ["v5"],
    )
    settings = MagicMock()
    settings.postgres_host = "private.rds.amazonaws.com"
    settings.postgres_port = 5432
    settings.postgres_db = "smoke"

    meta = build_metadata(settings=settings, row_counts={"a.csv": 3}, include_parquet=False)
    dumped = json.dumps(meta)
    assert "private.rds.amazonaws.com" not in dumped
    assert meta["database_host_redacted"] == "[redacted]"
    assert meta["row_counts"]["a.csv"] == 3
    assert "postgres_password" not in dumped


def test_calibration_export_dry_run_writes_stub_metadata(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CALIBRATION_EXPORT_DRY_RUN", "1")
    monkeypatch.setenv("CALIBRATION_EXPORT_DIR", str(tmp_path / "cal"))
    out = export_run(include_parquet=False)
    assert out is not None
    meta = json.loads((out / "metadata.json").read_text(encoding="utf-8"))
    assert meta.get("dry_run") is True


def test_csv_to_parquet_writes_when_pyarrow_present(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    csv_path = tmp_path / "t.csv"
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
    pq_path = tmp_path / "t.parquet"
    csv_to_parquet(csv_path, pq_path)
    assert pq_path.is_file()


def test_csv_to_parquet_requires_pyarrow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    csv_path = tmp_path / "t.csv"
    csv_path.write_text("col\n1\n", encoding="utf-8")
    pq_path = tmp_path / "t.parquet"

    real_import = builtins.__import__

    def fake_import(
        name: str,
        globals_arg=None,
        locals_arg=None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ):
        if name == "pyarrow" or name.startswith("pyarrow."):
            raise ImportError("pyarrow unavailable in test double")
        return real_import(name, globals_arg, locals_arg, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(RuntimeError, match="pyarrow"):
        csv_to_parquet(csv_path, pq_path)


@pytest.mark.parametrize("workflow", ("ci.yml", "integration.yml"))
def test_github_workflows_are_valid_yaml(workflow: str) -> None:
    path = repo_root() / ".github/workflows" / workflow
    assert path.is_file()
    yaml.safe_load(path.read_text(encoding="utf-8"))


def test_release_check_script_shell_syntax() -> None:
    subprocess.run(["bash", "-n", str(repo_root() / "scripts/release_check.sh")], check=True)


def test_load_minimal_census_script_shell_syntax() -> None:
    subprocess.run(["bash", "-n", str(repo_root() / "scripts/load_minimal_census_fixtures.sh")], check=True)


def test_export_calibration_script_shell_syntax() -> None:
    subprocess.run(["bash", "-n", str(repo_root() / "scripts/export_calibration.sh")], check=True)


def test_minimal_census_geojson_fixtures_parse() -> None:
    root = repo_root()
    for fname in ("census_minimal_counties.geojson", "census_minimal_tracts.geojson"):
        data = json.loads((root / "tests/fixtures" / fname).read_text(encoding="utf-8"))
        assert data.get("type") == "FeatureCollection"
        assert len(data.get("features") or []) >= 1


def test_package_version_exported() -> None:
    assert isinstance(__version__, str)
    assert __version__.startswith("1.0.")
    assert __version__ >= "1.0.1"
