from __future__ import annotations

from wildfire_smoke.calibration_evidence import classify_dispersion_aq_evidence, parse_lag_windows_hours


def test_parse_lag_windows_hours_default_shape() -> None:
    buckets = parse_lag_windows_hours("0-3,3-6,6-12,12-24")
    assert len(buckets) == 4
    assert buckets[0][1] == 0.0 and buckets[0][2] == 3.0
    assert buckets[-1][0] == "12-24h"


def test_parse_lag_windows_hours_skips_invalid() -> None:
    assert parse_lag_windows_hours("6-3") == ()
    assert parse_lag_windows_hours("") == ()


def test_classify_no_aq_data() -> None:
    assert (
        classify_dispersion_aq_evidence(
            aq_observation_count=0,
            min_aq_observations=3,
            dispersion_exposure_count=3,
            max_dispersion_score=80.0,
            avg_pm25=None,
            high_pm25=35.0,
            low_pm25=12.0,
            high_dispersion_score=70.0,
            low_dispersion_score=25.0,
        )
        == "no_aq_data"
    )


def test_classify_insufficient_aq() -> None:
    assert (
        classify_dispersion_aq_evidence(
            aq_observation_count=2,
            min_aq_observations=3,
            dispersion_exposure_count=3,
            max_dispersion_score=80.0,
            avg_pm25=20.0,
            high_pm25=35.0,
            low_pm25=12.0,
            high_dispersion_score=70.0,
            low_dispersion_score=25.0,
        )
        == "insufficient_aq_data"
    )


def test_classify_possible_overprediction() -> None:
    assert (
        classify_dispersion_aq_evidence(
            aq_observation_count=5,
            min_aq_observations=3,
            dispersion_exposure_count=3,
            max_dispersion_score=80.0,
            avg_pm25=10.0,
            high_pm25=35.0,
            low_pm25=12.0,
            high_dispersion_score=70.0,
            low_dispersion_score=25.0,
        )
        == "possible_overprediction"
    )


def test_classify_possible_underprediction() -> None:
    assert (
        classify_dispersion_aq_evidence(
            aq_observation_count=5,
            min_aq_observations=3,
            dispersion_exposure_count=3,
            max_dispersion_score=20.0,
            avg_pm25=40.0,
            high_pm25=35.0,
            low_pm25=12.0,
            high_dispersion_score=70.0,
            low_dispersion_score=25.0,
        )
        == "possible_underprediction"
    )


def test_classify_plausible_alignment_high_high() -> None:
    assert (
        classify_dispersion_aq_evidence(
            aq_observation_count=5,
            min_aq_observations=3,
            dispersion_exposure_count=3,
            max_dispersion_score=80.0,
            avg_pm25=40.0,
            high_pm25=35.0,
            low_pm25=12.0,
            high_dispersion_score=70.0,
            low_dispersion_score=25.0,
        )
        == "plausible_alignment"
    )


def test_classify_comparable_middle() -> None:
    lab = classify_dispersion_aq_evidence(
        aq_observation_count=5,
        min_aq_observations=3,
        dispersion_exposure_count=3,
        max_dispersion_score=50.0,
        avg_pm25=20.0,
        high_pm25=35.0,
        low_pm25=12.0,
        high_dispersion_score=70.0,
        low_dispersion_score=25.0,
    )
    assert lab == "comparable"
