from __future__ import annotations

from wildfire_smoke.settings import repo_root


def test_maps_and_slis_sql_files_exist_and_reference_views() -> None:
    root = repo_root()
    alerts = root / "sql/views/zzz_phase3_alerts_sli.sql"
    geo = root / "sql/views/zzz_phase3_geojson_views.sql"
    mat = root / "sql/views/zzz_phase3_materialized.sql"
    for p in (alerts, geo, mat):
        assert p.is_file(), f"missing {p}"

    txt_alerts = alerts.read_text()
    assert "fn_alert_candidates" in txt_alerts
    assert "v_alert_candidates" in txt_alerts

    txt_geo = geo.read_text()
    assert "v_latest_smoke_risk_county_geojson" in txt_geo
    assert "v_latest_fire_detections_geojson" in txt_geo

    txt_mat = mat.read_text()
    assert "mv_latest_smoke_risk_county_geojson" in txt_mat
