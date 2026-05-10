from __future__ import annotations

from wildfire_smoke.settings import repo_root


def test_calibration_migration_initdb_views_exist() -> None:
    root = repo_root()
    m12 = root / "sql/migrations/012_phase12_calibration_metrics.sql"
    i77 = root / "docker/postgres/initdb/77_phase12_calibration_metrics.sql"
    v12 = root / "sql/views/zzz_phase12_calibration_views.sql"
    for p in (m12, i77, v12):
        assert p.is_file(), f"missing {p}"

    ddl = m12.read_text()
    assert "analytics.risk_observations" in ddl
    assert "analytics.risk_model_evaluations" in ddl
    assert "analytics.dispersion_aq_comparisons" in ddl
    assert "analytics.risk_observation_features" in ddl

    views_txt = v12.read_text()
    for name in (
        "v_dispersion_aq_evidence_summary",
        "v_dispersion_aq_lag_summary",
        "v_risk_model_evaluation_latest",
        "v_risk_model_evaluation_history",
        "v_model_overprediction_candidates",
        "v_model_underprediction_candidates",
        "v_calibration_confidence_summary",
        "v_risk_observation_coverage",
    ):
        assert name in views_txt


def test_canonical_alert_fn_lists_calibration_alert_types() -> None:
    fn_path = repo_root() / "sql/migrations/013_phase14_canonical_alert_function.sql"
    txt = fn_path.read_text()
    assert "p_model_mismatch_min_count" in txt
    assert "model_overprediction_possible" in txt
    assert "model_underprediction_possible" in txt
    assert "calibration_insufficient_data" in txt
    assert "aq_observation_coverage_low" in txt
