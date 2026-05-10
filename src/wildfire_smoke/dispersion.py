"""
Gaussian-ish dispersion **proxy** for engineering correlation (not regulatory modeling).

This is **not** HYSPLIT, regulatory dispersion, or public-health-grade modeling.
It complements corridor plumes (``wind_v1`` / ``wind_grid_v2``) with a separate
heuristic so dashboards can compare scores — not forecast concentrations.

Assumptions (meteorological wind FROM convention):
    * ``wind_from_degrees`` is where wind originates.
    * Smoke advection is modeled toward ``downwind_bearing = wind_from + 180°``.
    * Targets **behind** the source relative to downwind (negative along-wind
      component) are treated as **upwind** and scored ~0.

Transforms:
    * Gaussian radial weights use ``exp(-0.5 * (x/sigma)^2)``.
    * ``dispersion_score_from_proxy`` maps proxy ≥ 0 into ``[0, 100]`` via
      ``100 * (1 - exp(-proxy / REF))`` with ``REF = 250`` (tunable scalar).
"""

from __future__ import annotations

import math

from wildfire_smoke.wind import angular_difference_degrees

# Scale for proxy → score saturating transform (engineering tuning only).
_PROXY_SCORE_REF = 250.0


def gaussian_weight(x: float, sigma: float) -> float:
    """Bell-shaped weight; ``sigma`` is effectively the characteristic scale (km)."""

    if sigma <= 0 or not math.isfinite(sigma):
        return 0.0
    if not math.isfinite(x):
        return 0.0
    z = x / sigma
    return math.exp(-0.5 * z * z)


def source_strength_from_fire(
    frp: float | None,
    brightness: float | None,
    mode: str,
) -> float:
    """
    Scalar emission proxy (unitless). Minimum floor avoids zeros for stable ratios.

    Modes: ``frp`` (MW proxy), ``brightness`` (instrument native), ``unit`` (1.0).
    """

    m = mode.strip().lower()
    if m == "frp":
        return max(float(frp or 0.0), 1.0)
    if m == "brightness":
        return max(float(brightness or 0.0), 1.0)
    if m == "unit":
        return 1.0
    raise ValueError(f"unsupported DISPERSION_SOURCE_STRENGTH_MODE: {mode!r}")


def wind_aligned_components(
    distance_km: float,
    bearing_from_fire_to_target_degrees: float,
    downwind_bearing_degrees: float,
) -> tuple[float, float]:
    """
    Project great-circle ``distance_km`` onto downwind / crosswind axes.

    ``bearing_from_fire_to_target_degrees``: compass bearing fire → target centroid.
    Returns ``(downwind_km, crosswind_km)`` with crosswind ≥ 0.
    """

    if distance_km <= 0 or not math.isfinite(distance_km):
        return 0.0, 0.0
    angle = angular_difference_degrees(bearing_from_fire_to_target_degrees, downwind_bearing_degrees)
    rad = math.radians(angle)
    downwind_km = distance_km * math.cos(rad)
    crosswind_km = abs(distance_km * math.sin(rad))
    return downwind_km, crosswind_km


def dispersion_concentration_proxy(
    source_strength: float,
    downwind_km: float,
    crosswind_km: float,
    wind_speed_mps: float,
    sigma_downwind_km: float,
    sigma_crosswind_km: float,
    min_wind_speed_mps: float,
) -> float:
    """
    Unscaled concentration proxy (``downwind`` peak shifted per spec).

    ``downwind_component`` uses ``gaussian_weight(downwind_km - sigma_downwind_km/2, sigma_downwind_km)``.
    Upwind rows should pass ``downwind_km <= 0`` from caller and get ~0 proxy.
    """

    if downwind_km <= 0:
        return 0.0

    eff_wind = max(float(wind_speed_mps or 0.0), float(min_wind_speed_mps))
    if eff_wind <= 0:
        return 0.0

    downwind_component = gaussian_weight(downwind_km - sigma_downwind_km / 2.0, sigma_downwind_km)
    crosswind_component = gaussian_weight(crosswind_km, sigma_crosswind_km)
    wind_component = min(1.0, eff_wind / 10.0)

    return (
        float(source_strength)
        * downwind_component
        * crosswind_component
        * wind_component
        / eff_wind
    )


def dispersion_score_from_proxy(proxy: float, *, ref: float = _PROXY_SCORE_REF) -> float:
    """Map nonnegative proxy into ``[0, 100)``, asymptoting below 100."""

    if proxy <= 0 or not math.isfinite(proxy):
        return 0.0
    if ref <= 0:
        return 0.0
    return min(100.0, 100.0 * (1.0 - math.exp(-proxy / ref)))
