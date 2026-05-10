from __future__ import annotations

from wildfire_smoke.settings import repo_root


def test_smoke_transport_migrations_and_initdb_exist() -> None:
    root = repo_root()
    m5 = root / "sql/migrations/005_phase6_wind_observations.sql"
    m6 = root / "sql/migrations/006_phase6_smoke_plume_exposures.sql"
    i70 = root / "docker/postgres/initdb/70_phase6_wind.sql"
    i71 = root / "docker/postgres/initdb/71_phase6_plume.sql"
    vtr = root / "sql/views/zzz_phase6_smoke_transport_views.sql"
    val = root / "sql/views/zzz_phase6_fn_alert_candidates.sql"
    for p in (m5, m6, i70, i71, vtr, val):
        assert p.is_file(), f"missing {p}"

    wind_txt = m5.read_text()
    assert "raw.wind_observations" in wind_txt
    assert "normalized.wind_observations" in wind_txt

    plume_txt = m6.read_text()
    assert "analytics.smoke_plume_exposures" in plume_txt

    views_txt = vtr.read_text()
    assert "v_latest_wind_observations" in views_txt
    assert "v_smoke_transport_summary" in views_txt

    alerts_txt = val.read_text()
    assert "p_high_plume_exposure_min" in alerts_txt
    assert "high_plume_exposure" in alerts_txt
    assert "wind_data_stale" in alerts_txt
