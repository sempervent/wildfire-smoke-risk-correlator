from __future__ import annotations

from wildfire_smoke.alerts import fingerprint_for_candidate


def test_fingerprint_stable_for_same_logical_alert() -> None:
    fp1 = fingerprint_for_candidate(
        alert_type="stale_firms_normalized",
        raw_severity="critical",
        geography_type=None,
        geoid=None,
        title="Stale FIRMS-derived fire timestamps",
        details={"max_acq_datetime": "2024-01-01T00:00:00Z"},
    )
    fp2 = fingerprint_for_candidate(
        alert_type="stale_firms_normalized",
        raw_severity="critical",
        geography_type=None,
        geoid=None,
        title="Stale FIRMS-derived fire timestamps",
        details={"max_acq_datetime": "2099-01-01T00:00:00Z"},
    )
    assert fp1 == fp2


def test_high_smoke_risk_splits_by_geography_and_model() -> None:
    base = dict(raw_severity="warn", geography_type="county", geoid="47001", title="ignored")
    fp_a = fingerprint_for_candidate(
        alert_type="high_smoke_risk",
        details={"model_version": "v2", "risk_band": "high"},
        **base,
    )
    fp_b = fingerprint_for_candidate(
        alert_type="high_smoke_risk",
        details={"model_version": "v3", "risk_band": "high"},
        **base,
    )
    assert fp_a != fp_b


def test_ingestion_failed_splits_by_source() -> None:
    fp_a = fingerprint_for_candidate(
        alert_type="ingestion_failed",
        raw_severity="critical",
        geography_type=None,
        geoid=None,
        title="Ingestion failed: firms",
        details={"source": "firms"},
    )
    fp_b = fingerprint_for_candidate(
        alert_type="ingestion_failed",
        raw_severity="critical",
        geography_type=None,
        geoid=None,
        title="Ingestion failed: openaq",
        details={"source": "openaq"},
    )
    assert fp_a != fp_b
