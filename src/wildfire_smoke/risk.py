from __future__ import annotations

from datetime import datetime


def risk_band(score: float) -> str:
    if score < 0:
        raise ValueError("risk score must be non-negative")
    if score < 25:
        return "low"
    if score < 50:
        return "moderate"
    if score < 75:
        return "high"
    return "severe"


def compute_risk_score_fields(
    fire_count: int,
    max_frp: float | None,
    avg_pm25: float | None,
    avg_pm10: float | None,
) -> tuple[float, str]:
    """
    Engineering smoke-risk index (NOT a health advisory).

    Components are intentionally simple for observability and correlation experiments.
    """

    fire_component = min(1.0, max(fire_count, 0) / 20.0)
    frp_component = min(1.0, max(max_frp or 0.0, 0.0) / 500.0)
    pm25_component = min(1.0, max((avg_pm25 or 0.0) - 5.0, 0.0) / 50.0)
    pm10_component = min(1.0, max((avg_pm10 or 0.0) - 10.0, 0.0) / 100.0)

    score = 100.0 * (
        0.35 * fire_component
        + 0.25 * frp_component
        + 0.30 * pm25_component
        + 0.10 * pm10_component
    )
    return score, risk_band(score)


def recency_component_from_hours(hours_since_newest_fire: float | None) -> float:
    """
    Maps lag from the newest fire observation to the scoring reference time (hours)
    into [0, 1] for the v2 index.
    """

    if hours_since_newest_fire is None:
        return 0.0
    if hours_since_newest_fire < 0:
        return 1.0
    if hours_since_newest_fire <= 3:
        return 1.0
    if hours_since_newest_fire <= 6:
        return 0.75
    if hours_since_newest_fire <= 12:
        return 0.50
    if hours_since_newest_fire <= 24:
        return 0.25
    return 0.0


def compute_risk_score_v2_fields(
    *,
    fire_inside_count: int,
    nearby_fire_count: int,
    max_frp: float | None,
    avg_pm25: float | None,
    avg_pm10: float | None,
    newest_fire_observed_at: datetime | None,
    window_end: datetime,
) -> tuple[float, str, dict]:
    """
    Engineering smoke-risk index v2 (NOT a health advisory).

    Uses spatial inside vs nearby fire counts, pollutant baselines, and fire recency.
    """

    fire_inside_component = min(1.0, max(fire_inside_count, 0) / 20.0)
    nearby_fire_component = min(1.0, max(nearby_fire_count, 0) / 50.0)
    frp_component = min(1.0, max(max_frp or 0.0, 0.0) / 500.0)
    pm25_component = min(1.0, max((avg_pm25 or 0.0) - 5.0, 0.0) / 50.0)
    pm10_component = min(1.0, max((avg_pm10 or 0.0) - 10.0, 0.0) / 100.0)

    hours_since_fire: float | None
    if newest_fire_observed_at is None:
        hours_since_fire = None
    else:
        hours_since_fire = (window_end - newest_fire_observed_at).total_seconds() / 3600.0

    recency_component = recency_component_from_hours(hours_since_fire)

    score = 100.0 * (
        0.25 * fire_inside_component
        + 0.20 * nearby_fire_component
        + 0.15 * frp_component
        + 0.25 * pm25_component
        + 0.05 * pm10_component
        + 0.10 * recency_component
    )

    weights = {
        "fire_inside": 0.25,
        "nearby_fire": 0.20,
        "frp": 0.15,
        "pm25": 0.25,
        "pm10": 0.05,
        "recency": 0.10,
    }

    explanation = {
        "model_version": "v2",
        "inputs": {
            "fire_inside_count": fire_inside_count,
            "nearby_fire_count": nearby_fire_count,
            "max_frp": max_frp,
            "avg_pm25": avg_pm25,
            "avg_pm10": avg_pm10,
            "newest_fire_observed_at": newest_fire_observed_at.isoformat()
            if newest_fire_observed_at is not None
            else None,
            "window_end": window_end.isoformat(),
        },
        "components": {
            "fire_inside": fire_inside_component,
            "nearby_fire": nearby_fire_component,
            "frp": frp_component,
            "pm25": pm25_component,
            "pm10": pm10_component,
            "recency": recency_component,
        },
        "weights": weights,
        "hours_since_newest_fire": hours_since_fire,
    }

    return score, risk_band(score), explanation
