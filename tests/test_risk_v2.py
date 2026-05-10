from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from wildfire_smoke.risk import compute_risk_score_v2_fields, recency_component_from_hours


def test_recency_component_piecewise() -> None:
    assert recency_component_from_hours(None) == 0.0
    assert recency_component_from_hours(-1.0) == 1.0
    assert recency_component_from_hours(0.0) == 1.0
    assert recency_component_from_hours(3.0) == 1.0
    assert recency_component_from_hours(3.5) == 0.75
    assert recency_component_from_hours(6.0) == 0.75
    assert recency_component_from_hours(10.0) == 0.50
    assert recency_component_from_hours(24.0) == 0.25
    assert recency_component_from_hours(25.0) == 0.0


def test_risk_v2_explanation_shape() -> None:
    window_end = datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc)
    newest = window_end - timedelta(hours=2)

    score, band, explanation = compute_risk_score_v2_fields(
        fire_inside_count=10,
        nearby_fire_count=25,
        max_frp=250.0,
        avg_pm25=30.0,
        avg_pm10=50.0,
        newest_fire_observed_at=newest,
        window_end=window_end,
    )

    assert isinstance(score, float)
    assert band in {"low", "moderate", "high", "severe"}
    assert explanation["model_version"] == "v2"
    assert set(explanation["components"].keys()) == {
        "fire_inside",
        "nearby_fire",
        "frp",
        "pm25",
        "pm10",
        "recency",
    }
    assert set(explanation["weights"].keys()) == {
        "fire_inside",
        "nearby_fire",
        "frp",
        "pm25",
        "pm10",
        "recency",
    }
    assert explanation["hours_since_newest_fire"] == pytest.approx(2.0)
    assert explanation["inputs"]["fire_inside_count"] == 10


def test_risk_v2_saturated_components_hit_severe_band() -> None:
    window_end = datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc)
    newest = window_end - timedelta(minutes=30)

    score, band, _explanation = compute_risk_score_v2_fields(
        fire_inside_count=100,
        nearby_fire_count=200,
        max_frp=800.0,
        avg_pm25=100.0,
        avg_pm10=200.0,
        newest_fire_observed_at=newest,
        window_end=window_end,
    )

    assert score == pytest.approx(100.0)
    assert band == "severe"
