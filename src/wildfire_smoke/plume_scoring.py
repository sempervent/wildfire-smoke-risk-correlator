"""Deterministic wind_v1 corridor exposure scoring (engineering approximation only)."""

from __future__ import annotations


def fire_component_from_frp(frp: float | None) -> float:
    """``min(1, frp/500)`` with ``frp is None`` mapped to the specified 0.25 surrogate."""

    if frp is None:
        return 0.25
    return min(1.0, max(float(frp), 0.0) / 500.0)


def wind_v1_exposure_components(
    *,
    distance_km: float,
    plume_max_distance_km: float,
    angular_error_degrees: float,
    plume_half_angle_degrees: float,
    wind_speed_mps: float | None,
    frp: float | None,
) -> tuple[float, dict[str, float]]:
    """
    Corridor alignment score on ``[0, 100]`` plus explanation components.

    Caller should only invoke when ``angular_error_degrees <= plume_half_angle_degrees``
    (inside the modeled corridor).
    """

    distance_component = max(0.0, 1.0 - distance_km / plume_max_distance_km)
    alignment_component = max(0.0, 1.0 - angular_error_degrees / plume_half_angle_degrees)
    ws = 0.0 if wind_speed_mps is None else float(wind_speed_mps)
    wind_component = min(1.0, ws / 10.0)
    fire_c = fire_component_from_frp(frp)

    exposure_score = 100.0 * (
        0.35 * alignment_component
        + 0.25 * distance_component
        + 0.20 * wind_component
        + 0.20 * fire_c
    )

    explanation = {
        "alignment_component": alignment_component,
        "distance_component": distance_component,
        "wind_component": wind_component,
        "fire_component": fire_c,
        "weights": {
            "alignment": 0.35,
            "distance": 0.25,
            "wind": 0.20,
            "fire": 0.20,
        },
    }
    return exposure_score, explanation
