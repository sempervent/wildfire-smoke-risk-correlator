"""Normalize SQL alert severities to an ordered scale for filtering and notifications."""

from __future__ import annotations

ORDER: tuple[str, ...] = ("info", "warning", "high", "critical")

_RANK: dict[str, int] = {name: idx for idx, name in enumerate(ORDER)}


def normalize_db_severity(alert_type: str, raw: str) -> str:
    """Map fn_alert_candidates.severity (warn/critical) into ORDER labels."""
    r = (raw or "").strip().lower()
    if r == "critical":
        return "critical"
    if r == "warn":
        if alert_type in {"high_smoke_risk", "high_plume_exposure", "high_dispersion_exposure"}:
            return "high"
        return "warning"
    if r in _RANK:
        return r
    return "warning"


def severity_rank(severity: str) -> int:
    key = severity.strip().lower()
    if key not in _RANK:
        raise ValueError(f"unknown severity: {severity!r}")
    return _RANK[key]


def passes_min_severity(observed: str, minimum: str) -> bool:
    return severity_rank(observed) >= severity_rank(minimum)
