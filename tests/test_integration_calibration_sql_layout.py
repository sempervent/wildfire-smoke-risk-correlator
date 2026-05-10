from __future__ import annotations

from wildfire_smoke.settings import repo_root


def test_integration_calibration_migration_and_views_exist() -> None:
    root = repo_root()
    m10 = root / "sql/migrations/010_phase10_calibration.sql"
    i75 = root / "docker/postgres/initdb/75_phase10_calibration.sql"
    v10 = root / "sql/views/zzz_phase10_10_integration_and_calibration_views.sql"
    for p in (m10, i75, v10):
        assert p.is_file(), f"missing {p}"

    ddl = m10.read_text()
    assert "analytics.risk_observations" in ddl
    assert "analytics.risk_model_evaluations" in ddl

    views = v10.read_text()
    assert "v_integration_pipeline_counts" in views
    assert "v_fire_weather_unmatched" in views
    assert "v_risk_calibration_summary" in views
