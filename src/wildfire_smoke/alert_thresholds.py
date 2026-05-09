"""Alert threshold parsing from environment (used by scripts/check_alerts.sh and tests)."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _positive_int(raw: str | None, default: int) -> int:
    if raw is None or not str(raw).strip():
        return default
    v = int(str(raw).strip())
    if v < 0:
        raise ValueError("expected non-negative integer")
    return v


def _positive_float(raw: str | None, default: float) -> float:
    if raw is None or not str(raw).strip():
        return default
    v = float(str(raw).strip())
    if v < 0:
        raise ValueError("expected non-negative float")
    return v


@dataclass(frozen=True)
class AlertThresholds:
    freshness_warn_hours: int
    freshness_critical_hours: int
    high_risk_min_score: float
    lookback_hours: int


def alert_thresholds_from_env() -> AlertThresholds:
    return AlertThresholds(
        freshness_warn_hours=_positive_int(os.environ.get("ALERT_FRESHNESS_WARN_HOURS"), 6),
        freshness_critical_hours=_positive_int(os.environ.get("ALERT_FRESHNESS_CRITICAL_HOURS"), 24),
        high_risk_min_score=_positive_float(os.environ.get("ALERT_HIGH_RISK_MIN_SCORE"), 75.0),
        lookback_hours=_positive_int(os.environ.get("ALERT_LOOKBACK_HOURS"), 24),
    )
