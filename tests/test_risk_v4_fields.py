from __future__ import annotations

from datetime import datetime, timezone

import pytest

from wildfire_smoke.risk import compute_risk_score_v4_fields


def _window_end() -> datetime:
    return datetime(2026, 5, 9, 20, 0, tzinfo=timezone.utc)


def test_v4_humidity_dampening_high_rh_reduces_score() -> None:
    end = _window_end()
    low_rh, _, expl1 = compute_risk_score_v4_fields(
        fire_inside_count=2,
        nearby_fire_count=0,
        max_frp=100.0,
        avg_pm25=10.0,
        avg_pm10=None,
        newest_fire_observed_at=end,
        window_end=end,
        max_plume_exposure_score=80.0,
        plume_detection_count=1,
        plume_model_version="wind_grid_v2",
        avg_relative_humidity_percent=40.0,
        grid_weather_observation_count=3,
        plume_fallback_used=False,
    )
    high_rh, _, expl2 = compute_risk_score_v4_fields(
        fire_inside_count=2,
        nearby_fire_count=0,
        max_frp=100.0,
        avg_pm25=10.0,
        avg_pm10=None,
        newest_fire_observed_at=end,
        window_end=end,
        max_plume_exposure_score=80.0,
        plume_detection_count=1,
        plume_model_version="wind_grid_v2",
        avg_relative_humidity_percent=90.0,
        grid_weather_observation_count=3,
        plume_fallback_used=False,
    )
    assert expl1["humidity_dampening"] == 1.0
    assert expl2["humidity_dampening"] == pytest.approx(0.75)
    assert high_rh < low_rh


def test_v4_explanation_shape() -> None:
    end = _window_end()
    score, band, expl = compute_risk_score_v4_fields(
        fire_inside_count=1,
        nearby_fire_count=0,
        max_frp=None,
        avg_pm25=None,
        avg_pm10=None,
        newest_fire_observed_at=None,
        window_end=end,
        max_plume_exposure_score=0.0,
        plume_detection_count=0,
        plume_model_version="wind_grid_v2",
        avg_relative_humidity_percent=None,
        grid_weather_observation_count=0,
        plume_fallback_used=True,
    )
    assert expl["risk_model_version"] == "v4"
    assert expl["model_version"] == "v4"
    assert expl["plume_model_version"] == "wind_grid_v2"
    assert expl["fallback_used"] is True
    assert expl["grid_weather_observation_count"] == 0
    assert "max_grid_plume_exposure_score" in expl
    assert score >= 0 and band in {"low", "moderate", "high", "severe"}
