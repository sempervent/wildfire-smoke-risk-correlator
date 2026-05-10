from __future__ import annotations

from wildfire_smoke.settings import repo_root


def test_grid_weather_migrations_initdb_views_and_alert_fn_exist() -> None:
    root = repo_root()
    m9 = root / "sql/migrations/009_phase9_gridded_weather.sql"
    i74 = root / "docker/postgres/initdb/74_phase9_gridded_weather.sql"
    vgrid = root / "sql/views/zzz_phase9_gridded_weather_views.sql"
    val = root / "sql/views/zzz_phase9_fn_alert_candidates.sql"
    m13 = root / "sql/migrations/013_phase14_canonical_alert_function.sql"
    for p in (m9, i74, vgrid, val, m13):
        assert p.is_file(), f"missing {p}"

    ddl = m9.read_text()
    assert "raw.gridded_weather" in ddl
    assert "normalized.weather_grid_cells" in ddl
    assert "analytics.fire_weather_matches" in ddl

    views_txt = vgrid.read_text()
    assert "v_latest_weather_grid_cells" in views_txt
    assert "v_fire_weather_matches" in views_txt
    assert "v_latest_smoke_plume_exposures_v2" in views_txt
    assert "v_latest_smoke_risk_v4" in views_txt

    stub_txt = val.read_text()
    assert "migration 013" in stub_txt.lower() or "013_phase14" in stub_txt

    canon = m13.read_text()
    assert "DROP FUNCTION IF EXISTS analytics.fn_alert_candidates" in canon
    assert "CREATE OR REPLACE FUNCTION analytics.fn_alert_candidates" in canon
    assert "p_grid_weather_stale_hours" in canon
    assert "grid_weather_stale" in canon
    assert "no_recent_grid_weather" in canon
    assert "fire_weather_unmatched_high" in canon
    assert "grid_weather_parse_errors_high" in canon
    assert "integration_pipeline_incomplete" in canon
    assert "v4_risk_missing" in canon
    assert "fire_weather_match_missing" in canon
    assert "high_dispersion_exposure" in canon
    assert "dispersion_no_wind_matches" in canon
