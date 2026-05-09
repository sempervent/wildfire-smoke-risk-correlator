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

    t = alert_thresholds_from_env()
    assert t.freshness_warn_hours == 6
    assert t.freshness_critical_hours == 24
    assert t.high_risk_min_score == 75.0
    assert t.lookback_hours == 24
    assert t.high_plume_exposure_min_score == 70.0
    assert t.parse_errors_warn_count == 1
    assert t.parse_errors_critical_count == 25
    assert t.consumer_offset_stale_hours == 6


def test_alert_threshold_overrides(monkeypatch) -> None:
    monkeypatch.setenv("ALERT_FRESHNESS_WARN_HOURS", "8")
    monkeypatch.setenv("ALERT_HIGH_RISK_MIN_SCORE", "70")
    t = alert_thresholds_from_env()
    assert t.freshness_warn_hours == 8
    assert t.high_risk_min_score == 70.0


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
