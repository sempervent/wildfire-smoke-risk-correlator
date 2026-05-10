from __future__ import annotations

from wildfire_smoke.runbooks import load_runbook_mappings, runbook_slug_for_alert_type


def test_runbooks_yaml_maps_known_alert_types() -> None:
    m = load_runbook_mappings()
    assert "stale_firms_normalized" in m
    assert "no_recent_air_quality" in m
    assert "kafka_lag_high" in m
    assert "dlq_depth_high" in m
    assert "replay_failures_recent" in m
    assert "grid_weather_stale" in m
    assert "no_recent_grid_weather" in m
    assert "fire_weather_unmatched_high" in m
    assert "grid_weather_parse_errors_high" in m
    assert "integration_pipeline_incomplete" in m
    assert "v4_risk_missing" in m
    assert "fire_weather_match_missing" in m
    slug = runbook_slug_for_alert_type("high_smoke_risk", m)
    assert slug == "high-smoke-risk"


def test_runbook_markdown_files_exist() -> None:
    for mapping in load_runbook_mappings().values():
        assert mapping.path.is_file(), f"missing runbook markdown {mapping.path}"
