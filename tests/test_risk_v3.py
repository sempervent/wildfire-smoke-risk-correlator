from __future__ import annotations

import math
from datetime import datetime, timezone

from wildfire_smoke.risk import compute_risk_score_v3_fields


def test_v3_blends_plume_into_base_v2() -> None:
    window_end = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)
    score, band, expl = compute_risk_score_v3_fields(
        fire_inside_count=0,
        nearby_fire_count=0,
        max_frp=None,
        avg_pm25=None,
        avg_pm10=None,
        newest_fire_observed_at=None,
        window_end=window_end,
        max_plume_exposure_score=100.0,
        plume_detection_count=3,
        wind_model_version="wind_v1",
    )
    base_v2 = float(expl["base_v2_score"])
    assert math.isclose(float(expl["plume_component"]), 1.0)
    assert math.isclose(score, min(100.0, 0.75 * base_v2 + 25.0))
    assert expl["wind_model_version"] == "wind_v1"
    assert expl["plume_detection_count"] == 3
    assert band in {"low", "moderate", "high", "severe"}
