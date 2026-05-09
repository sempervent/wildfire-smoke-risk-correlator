from __future__ import annotations

import pytest

from wildfire_smoke.alert_thresholds import alert_thresholds_from_env


def test_alert_threshold_defaults(monkeypatch) -> None:
    monkeypatch.delenv("ALERT_FRESHNESS_WARN_HOURS", raising=False)
    monkeypatch.delenv("ALERT_FRESHNESS_CRITICAL_HOURS", raising=False)
    monkeypatch.delenv("ALERT_HIGH_RISK_MIN_SCORE", raising=False)
    monkeypatch.delenv("ALERT_LOOKBACK_HOURS", raising=False)

    t = alert_thresholds_from_env()
    assert t.freshness_warn_hours == 6
    assert t.freshness_critical_hours == 24
    assert t.high_risk_min_score == 75.0
    assert t.lookback_hours == 24


def test_alert_threshold_overrides(monkeypatch) -> None:
    monkeypatch.setenv("ALERT_FRESHNESS_WARN_HOURS", "8")
    monkeypatch.setenv("ALERT_HIGH_RISK_MIN_SCORE", "70")
    t = alert_thresholds_from_env()
    assert t.freshness_warn_hours == 8
    assert t.high_risk_min_score == 70.0


def test_alert_threshold_rejects_negative(monkeypatch) -> None:
    monkeypatch.setenv("ALERT_FRESHNESS_WARN_HOURS", "-1")
    with pytest.raises(ValueError):
        alert_thresholds_from_env()
