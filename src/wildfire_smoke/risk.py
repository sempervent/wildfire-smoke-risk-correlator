from __future__ import annotations


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
