from __future__ import annotations

import math

from wildfire_smoke.plume_scoring import fire_component_from_frp, wind_v1_exposure_components


def test_fire_component_null_frp_surrogate() -> None:
    assert math.isclose(fire_component_from_frp(None), 0.25)


def test_wind_v1_exposure_perfect_alignment() -> None:
    score, expl = wind_v1_exposure_components(
        distance_km=0.0,
        plume_max_distance_km=150.0,
        angular_error_degrees=0.0,
        plume_half_angle_degrees=30.0,
        wind_speed_mps=10.0,
        frp=500.0,
    )
    assert score > 95.0
    assert expl["alignment_component"] == 1.0
    assert expl["distance_component"] == 1.0
    assert expl["wind_component"] == 1.0
    assert expl["fire_component"] == 1.0
