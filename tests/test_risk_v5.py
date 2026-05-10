from __future__ import annotations

from datetime import datetime, timezone

import pytest

from wildfire_smoke.risk import compute_risk_score_v5_fields


def test_v5_formula_blends_components() -> None:
    window_end = datetime(2026, 5, 9, 18, 0, tzinfo=timezone.utc)
    score, band, expl = compute_risk_score_v5_fields(
        fire_inside_count=2,
        nearby_fire_count=5,
        max_frp=120.0,
        avg_pm25=15.0,
        avg_pm10=25.0,
        newest_fire_observed_at=window_end,
        window_end=window_end,
        max_plume_exposure_score=40.0,
        plume_detection_count=1,
        plume_model_version="wind_grid_v2",
        max_dispersion_score=30.0,
        dispersion_detection_count=2,
        dispersion_model_version="gaussian_v0",
        avg_relative_humidity_percent=None,
        grid_weather_observation_count=0,
        plume_fallback_used=False,
    )
    assert 0.0 <= score <= 100.0
    assert band in {"low", "moderate", "high", "severe"}
    assert expl["risk_model_version"] == "v5"
    assert expl["max_dispersion_score"] == pytest.approx(30.0)
    assert expl["max_plume_exposure_score"] == pytest.approx(40.0)
    assert "dispersion" in expl["components"]


def test_v5_humidity_dampening_caps() -> None:
    window_end = datetime(2026, 5, 9, 18, 0, tzinfo=timezone.utc)
    dry_score, _, _ = compute_risk_score_v5_fields(
        fire_inside_count=2,
        nearby_fire_count=5,
        max_frp=120.0,
        avg_pm25=15.0,
        avg_pm10=25.0,
        newest_fire_observed_at=window_end,
        window_end=window_end,
        max_plume_exposure_score=40.0,
        plume_detection_count=1,
        plume_model_version="wind_grid_v2",
        max_dispersion_score=30.0,
        dispersion_detection_count=2,
        dispersion_model_version="gaussian_v0",
        avg_relative_humidity_percent=None,
        grid_weather_observation_count=1,
        plume_fallback_used=False,
    )
    humid_score, _, expl_h = compute_risk_score_v5_fields(
        fire_inside_count=2,
        nearby_fire_count=5,
        max_frp=120.0,
        avg_pm25=15.0,
        avg_pm10=25.0,
        newest_fire_observed_at=window_end,
        window_end=window_end,
        max_plume_exposure_score=40.0,
        plume_detection_count=1,
        plume_model_version="wind_grid_v2",
        max_dispersion_score=30.0,
        dispersion_detection_count=2,
        dispersion_model_version="gaussian_v0",
        avg_relative_humidity_percent=95.0,
        grid_weather_observation_count=1,
        plume_fallback_used=False,
    )
    assert humid_score <= dry_score
    assert expl_h["humidity_dampening"] <= 1.0
