"""Evidence classification for dispersion vs AQ calibration (Phase 12 — not scientific validation)."""

from __future__ import annotations


def parse_lag_windows_hours(raw: str) -> tuple[tuple[str, float, float], ...]:
    """Parse CALIBRATION_LAG_WINDOWS_HOURS like ``0-3,3-6,6-12,12-24`` into (label, lo_h, hi_h).

    Labels are normalized to ``{lo}-{hi}h`` for stable lag_bucket keys.
    """
    out: list[tuple[str, float, float]] = []
    for part in raw.split(","):
        chunk = part.strip()
        if not chunk or "-" not in chunk:
            continue
        left, right = chunk.split("-", 1)
        lo = float(left.strip())
        hi = float(right.strip())
        if hi <= lo:
            continue
        label = f"{int(lo) if lo == int(lo) else lo}-{int(hi) if hi == int(hi) else hi}h"
        out.append((label, lo, hi))
    return tuple(out)


def classify_dispersion_aq_evidence(
    *,
    aq_observation_count: int,
    min_aq_observations: int,
    dispersion_exposure_count: int,
    max_dispersion_score: float,
    avg_pm25: float | None,
    high_pm25: float,
    low_pm25: float,
    high_dispersion_score: float,
    low_dispersion_score: float,
) -> str:
    """Assign a qualitative evidence label (engineering heuristics only)."""
    if aq_observation_count == 0:
        return "no_aq_data"
    if aq_observation_count < min_aq_observations:
        return "insufficient_aq_data"
    if dispersion_exposure_count <= 0:
        return "insufficient_dispersion_data"

    pm = avg_pm25 if avg_pm25 is not None else 0.0
    hi_d = max_dispersion_score >= high_dispersion_score
    lo_d = max_dispersion_score <= low_dispersion_score
    hi_p = pm >= high_pm25
    lo_p = pm <= low_pm25

    if hi_d and lo_p:
        return "possible_overprediction"
    if lo_d and hi_p:
        return "possible_underprediction"
    if (hi_d and hi_p) or (lo_d and lo_p):
        return "plausible_alignment"
    return "comparable"
