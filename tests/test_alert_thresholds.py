from __future__ import annotations

import pytest

from wildfire_smoke.alert_thresholds import alert_thresholds_from_env


def test_alert_threshold_defaults(monkeypatch) -> None:
    monkeypatch.delenv("ALERT_FRESHNESS_WARN_HOURS", raising=False)
    monkeypatch.delenv("ALERT_FRESHNESS_CRITICAL_HOURS", raising=False)
    monkeypatch.delenv("ALERT_HIGH_RISK_MIN_SCORE", raising=False)
    monkeypatch.delenv("ALERT_LOOKBACK_HOURS", raising=False)
    monkeypatch.delenv("ALERT_HIGH_PLUME_EXPOSURE_MIN_SCORE", raising=False)
    monkeypatch.delenv("ALERT_PARSE_ERRORS_WARN_COUNT", raising=False)
    monkeypatch.delenv("ALERT_PARSE_ERRORS_CRITICAL_COUNT", raising=False)
    monkeypatch.delenv("ALERT_CONSUMER_OFFSET_STALE_HOURS", raising=False)
    monkeypatch.delenv("ALERT_PARSER_SPIKE_WARN_COUNT", raising=False)
    monkeypatch.delenv("ALERT_PARSER_SPIKE_CRITICAL_COUNT", raising=False)
    monkeypatch.delenv("ALERT_KAFKA_LAG_WARN_MESSAGES", raising=False)
    monkeypatch.delenv("ALERT_KAFKA_LAG_CRITICAL_MESSAGES", raising=False)
    monkeypatch.delenv("ALERT_DLQ_DEPTH_WARN_MESSAGES", raising=False)
    monkeypatch.delenv("ALERT_DLQ_DEPTH_CRITICAL_MESSAGES", raising=False)
    monkeypatch.delenv("ALERT_GRID_WEATHER_STALE_HOURS", raising=False)
    monkeypatch.delenv("ALERT_FIRE_WEATHER_UNMATCHED_WARN_COUNT", raising=False)
    monkeypatch.delenv("ALERT_FIRE_WEATHER_UNMATCHED_CRITICAL_COUNT", raising=False)
    monkeypatch.delenv("ALERT_HIGH_DISPERSION_EXPOSURE_MIN_SCORE", raising=False)
    monkeypatch.delenv("ALERT_DISPERSION_NO_WIND_MATCHES_HOURS", raising=False)
    monkeypatch.delenv("ALERT_DISPERSION_AQ_MISMATCH_MIN_SCORE", raising=False)

    t = alert_thresholds_from_env()
    assert t.freshness_warn_hours == 6
    assert t.freshness_critical_hours == 24
    assert t.high_risk_min_score == 75.0
    assert t.lookback_hours == 24
    assert t.high_plume_exposure_min_score == 70.0
    assert t.parse_errors_warn_count == 1
    assert t.parse_errors_critical_count == 25
    assert t.consumer_offset_stale_hours == 6
    assert t.grid_weather_stale_hours == 6
    assert t.fire_weather_unmatched_warn_count == 5
    assert t.fire_weather_unmatched_critical_count == 25
    assert t.high_dispersion_exposure_min_score == 70.0
    assert t.dispersion_no_wind_matches_hours == 24
    assert t.dispersion_aq_mismatch_min_score == 50.0


def test_alert_threshold_overrides(monkeypatch) -> None:
    monkeypatch.setenv("ALERT_FRESHNESS_WARN_HOURS", "8")
    monkeypatch.setenv("ALERT_HIGH_RISK_MIN_SCORE", "70")
    t = alert_thresholds_from_env()
    assert t.freshness_warn_hours == 8
    assert t.high_risk_min_score == 70.0


def test_alert_threshold_phase8_overrides(monkeypatch) -> None:
    monkeypatch.setenv("ALERT_PARSER_SPIKE_WARN_COUNT", "9")
    monkeypatch.setenv("ALERT_PARSER_SPIKE_CRITICAL_COUNT", "33")
    monkeypatch.setenv("ALERT_KAFKA_LAG_WARN_MESSAGES", "50")
    monkeypatch.setenv("ALERT_KAFKA_LAG_CRITICAL_MESSAGES", "500")
    monkeypatch.setenv("ALERT_DLQ_DEPTH_WARN_MESSAGES", "2")
    monkeypatch.setenv("ALERT_DLQ_DEPTH_CRITICAL_MESSAGES", "80")
    t = alert_thresholds_from_env()
    assert t.parser_spike_warn_count == 9
    assert t.parser_spike_critical_count == 33
    assert t.kafka_lag_warn_messages == 50
    assert t.kafka_lag_critical_messages == 500
    assert t.dlq_depth_warn_messages == 2
    assert t.dlq_depth_critical_messages == 80


def test_alert_threshold_phase9_overrides(monkeypatch) -> None:
    monkeypatch.setenv("ALERT_GRID_WEATHER_STALE_HOURS", "12")
    monkeypatch.setenv("ALERT_FIRE_WEATHER_UNMATCHED_WARN_COUNT", "7")
    monkeypatch.setenv("ALERT_FIRE_WEATHER_UNMATCHED_CRITICAL_COUNT", "40")
    t = alert_thresholds_from_env()
    assert t.grid_weather_stale_hours == 12
    assert t.fire_weather_unmatched_warn_count == 7
    assert t.fire_weather_unmatched_critical_count == 40


def test_alert_threshold_parse_dlq_overrides(monkeypatch) -> None:
    monkeypatch.setenv("ALERT_PARSE_ERRORS_WARN_COUNT", "5")
    monkeypatch.setenv("ALERT_PARSE_ERRORS_CRITICAL_COUNT", "100")
    monkeypatch.setenv("ALERT_CONSUMER_OFFSET_STALE_HOURS", "12")
    t = alert_thresholds_from_env()
    assert t.parse_errors_warn_count == 5
    assert t.parse_errors_critical_count == 100
    assert t.consumer_offset_stale_hours == 12


def test_alert_threshold_rejects_negative(monkeypatch) -> None:
    monkeypatch.setenv("ALERT_FRESHNESS_WARN_HOURS", "-1")
    with pytest.raises(ValueError):
        alert_thresholds_from_env()
