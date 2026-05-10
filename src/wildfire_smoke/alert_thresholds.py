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
    high_plume_exposure_min_score: float
    parse_errors_warn_count: int
    parse_errors_critical_count: int
    consumer_offset_stale_hours: int
    parser_spike_warn_count: int
    parser_spike_critical_count: int
    kafka_lag_warn_messages: int
    kafka_lag_critical_messages: int
    dlq_depth_warn_messages: int
    dlq_depth_critical_messages: int
    grid_weather_stale_hours: int
    fire_weather_unmatched_warn_count: int
    fire_weather_unmatched_critical_count: int
    high_dispersion_exposure_min_score: float
    dispersion_no_wind_matches_hours: int
    dispersion_aq_mismatch_min_score: float
    model_mismatch_min_count: int
    aq_observation_coverage_min_count: int
    calibration_warn_only: bool


def alert_thresholds_from_env() -> AlertThresholds:
    return AlertThresholds(
        freshness_warn_hours=_positive_int(os.environ.get("ALERT_FRESHNESS_WARN_HOURS"), 6),
        freshness_critical_hours=_positive_int(os.environ.get("ALERT_FRESHNESS_CRITICAL_HOURS"), 24),
        high_risk_min_score=_positive_float(os.environ.get("ALERT_HIGH_RISK_MIN_SCORE"), 75.0),
        lookback_hours=_positive_int(os.environ.get("ALERT_LOOKBACK_HOURS"), 24),
        high_plume_exposure_min_score=_positive_float(os.environ.get("ALERT_HIGH_PLUME_EXPOSURE_MIN_SCORE"), 70.0),
        parse_errors_warn_count=_positive_int(os.environ.get("ALERT_PARSE_ERRORS_WARN_COUNT"), 1),
        parse_errors_critical_count=_positive_int(os.environ.get("ALERT_PARSE_ERRORS_CRITICAL_COUNT"), 25),
        consumer_offset_stale_hours=_positive_int(os.environ.get("ALERT_CONSUMER_OFFSET_STALE_HOURS"), 6),
        parser_spike_warn_count=_positive_int(os.environ.get("ALERT_PARSER_SPIKE_WARN_COUNT"), 15),
        parser_spike_critical_count=_positive_int(os.environ.get("ALERT_PARSER_SPIKE_CRITICAL_COUNT"), 40),
        kafka_lag_warn_messages=_positive_int(os.environ.get("ALERT_KAFKA_LAG_WARN_MESSAGES"), 100),
        kafka_lag_critical_messages=_positive_int(os.environ.get("ALERT_KAFKA_LAG_CRITICAL_MESSAGES"), 1000),
        dlq_depth_warn_messages=_positive_int(os.environ.get("ALERT_DLQ_DEPTH_WARN_MESSAGES"), 1),
        dlq_depth_critical_messages=_positive_int(os.environ.get("ALERT_DLQ_DEPTH_CRITICAL_MESSAGES"), 100),
        grid_weather_stale_hours=_positive_int(os.environ.get("ALERT_GRID_WEATHER_STALE_HOURS"), 6),
        fire_weather_unmatched_warn_count=_positive_int(
            os.environ.get("ALERT_FIRE_WEATHER_UNMATCHED_WARN_COUNT"), 5
        ),
        fire_weather_unmatched_critical_count=_positive_int(
            os.environ.get("ALERT_FIRE_WEATHER_UNMATCHED_CRITICAL_COUNT"), 25
        ),
        high_dispersion_exposure_min_score=_positive_float(
            os.environ.get("ALERT_HIGH_DISPERSION_EXPOSURE_MIN_SCORE"), 70.0
        ),
        dispersion_no_wind_matches_hours=_positive_int(
            os.environ.get("ALERT_DISPERSION_NO_WIND_MATCHES_HOURS"), 24
        ),
        dispersion_aq_mismatch_min_score=_positive_float(
            os.environ.get("ALERT_DISPERSION_AQ_MISMATCH_MIN_SCORE"), 50.0
        ),
        model_mismatch_min_count=_positive_int(os.environ.get("ALERT_MODEL_MISMATCH_MIN_COUNT"), 3),
        aq_observation_coverage_min_count=_positive_int(
            os.environ.get("ALERT_AQ_OBSERVATION_COVERAGE_MIN_COUNT"), 3
        ),
        calibration_warn_only=os.environ.get("ALERT_CALIBRATION_WARN_ONLY", "1").strip().lower()
        in {"1", "true", "yes"},
    )
