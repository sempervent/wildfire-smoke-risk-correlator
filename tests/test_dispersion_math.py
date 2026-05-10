from __future__ import annotations

import math

import pytest

from wildfire_smoke.dispersion import (
    dispersion_concentration_proxy,
    dispersion_score_from_proxy,
    gaussian_weight,
    source_strength_from_fire,
    wind_aligned_components,
)


def test_gaussian_weight_peak_and_decay() -> None:
    assert gaussian_weight(0.0, 10.0) == pytest.approx(1.0)
    assert gaussian_weight(10.0, 10.0) == pytest.approx(math.exp(-0.5))
    assert gaussian_weight(1.0, 0.0) == 0.0


def test_source_strength_modes() -> None:
    assert source_strength_from_fire(12.0, 300.0, "frp") == pytest.approx(12.0)
    assert source_strength_from_fire(None, 300.0, "brightness") == pytest.approx(300.0)
    assert source_strength_from_fire(None, None, "unit") == pytest.approx(1.0)
    with pytest.raises(ValueError, match="DISPERSION_SOURCE_STRENGTH_MODE"):
        source_strength_from_fire(1.0, 1.0, "nope")


def test_wind_aligned_downwind_crosswind() -> None:
    # Fire at origin; target lies east (90°). Downwind axis east (90°) → aligned.
    dk, ck = wind_aligned_components(10.0, 90.0, 90.0)
    assert dk == pytest.approx(10.0)
    assert ck == pytest.approx(0.0)
    # Target north (0°), downwind east (90°) → predominantly crosswind.
    dk2, ck2 = wind_aligned_components(10.0, 0.0, 90.0)
    assert dk2 == pytest.approx(0.0)
    assert ck2 == pytest.approx(10.0)


def test_concentration_proxy_upwind_zero() -> None:
    proxy = dispersion_concentration_proxy(
        source_strength=50.0,
        downwind_km=-5.0,
        crosswind_km=2.0,
        wind_speed_mps=5.0,
        sigma_downwind_km=75.0,
        sigma_crosswind_km=15.0,
        min_wind_speed_mps=0.5,
    )
    assert proxy == 0.0


def test_dispersion_score_scaling_bounds() -> None:
    assert dispersion_score_from_proxy(0.0) == 0.0
    s = dispersion_score_from_proxy(500.0)
    assert 0.0 < s < 100.0

