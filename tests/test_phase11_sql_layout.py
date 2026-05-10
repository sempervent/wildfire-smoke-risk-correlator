from __future__ import annotations

from wildfire_smoke.settings import repo_root


def test_phase11_migration_initdb_views_exist() -> None:
    root = repo_root()
    m11 = root / "sql/migrations/011_phase11_dispersion.sql"
    i76 = root / "docker/postgres/initdb/76_phase11_dispersion.sql"
    v11 = root / "sql/views/zzz_phase11_dispersion_views.sql"
    for p in (m11, i76, v11):
        assert p.is_file(), f"missing {p}"

    ddl = m11.read_text()
    assert "analytics.smoke_dispersion_exposures" in ddl
    assert "analytics.dispersion_aq_comparisons" in ddl

    views_txt = v11.read_text()
    assert "v_latest_smoke_dispersion_exposures" in views_txt
    assert "v_dispersion_operational_summary" in views_txt
    assert "v_latest_smoke_risk_v5" in views_txt


def test_phase9_fn_lists_dispersion_alert_types() -> None:
    fn_path = repo_root() / "sql/migrations/013_phase14_canonical_alert_function.sql"
    txt = fn_path.read_text()
    assert "p_high_dispersion_exposure_min" in txt
    assert "high_dispersion_exposure" in txt
    assert "dispersion_no_wind_matches" in txt
    assert "dispersion_no_targets" in txt
    assert "dispersion_aq_mismatch_high" in txt
