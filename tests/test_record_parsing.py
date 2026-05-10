from __future__ import annotations

from datetime import datetime, timezone

import pytest

from wildfire_smoke import firms_csv
from wildfire_smoke.openaq_records import measurement_id, normalized_measurement_fields, parse_openaq_datetime
from wildfire_smoke.risk import compute_risk_score_fields, risk_band


def test_firms_detection_id_stability() -> None:
    source = "VIIRS_SNPP_NRT"
    lat = 36.162439
    lon = -86.781234
    acq = datetime(2024, 5, 9, 16, 30, tzinfo=timezone.utc)

    first = firms_csv.detection_id(source, lat, lon, acq)
    second = firms_csv.detection_id(source, lat, lon, acq)
    assert first == second
    assert first == "1ac5c57923d512b0a73541b26134715bbc19940dac1767e411313640aaa90d61"


def test_firms_csv_normalization_matches_fixture_row() -> None:
    rows = firms_csv.parse_firms_csv_text(
        "latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,instrument,confidence,version,bright_t31,frp,daynight\n"
        "36.162439,-86.781234,325.2,1.0,1.0,20240509,1630,N,VIIRS,n,2.0,305.4,18.7,D\n"
    )
    fields = firms_csv.normalized_fire_fields("VIIRS_SNPP_NRT", rows[0])
    assert fields["detection_id"] == "1ac5c57923d512b0a73541b26134715bbc19940dac1767e411313640aaa90d61"
    assert fields["latitude"] == 36.162439
    assert fields["longitude"] == -86.781234
    assert fields["frp"] == 18.7


def test_openaq_measurement_id_stability() -> None:
    dt = datetime(2026, 5, 9, 11, 0, tzinfo=timezone.utc)
    first = measurement_id("fixture-sensor-1", dt, "pm25")
    second = measurement_id("fixture-sensor-1", dt, "pm25")
    assert first == second
    assert first == "d4576f1d05a27e8b76a839e439ba02f05927166f219fa2ff594f0171b0c3f338"


def test_openaq_fixture_jsonl_normalized_roundtrip() -> None:
    raw = {
        "source": "openaq",
        "parameter": "pm25",
        "record": {
            "normalized": {
                "measurement_id": "d4576f1d05a27e8b76a839e439ba02f05927166f219fa2ff594f0171b0c3f338",
                "provider": "openaq",
                "location_id": "fixture-loc-1",
                "sensor_id": "fixture-sensor-1",
                "parameter": "pm25",
                "value": 22.4,
                "unit": "µg/m³",
                "measured_at": "2026-05-09T11:00:00+00:00",
                "latitude": 36.162439,
                "longitude": -86.781234,
            }
        },
    }
    normalized = raw["record"]["normalized"]
    dt = parse_openaq_datetime(normalized["measured_at"])
    rebuilt = normalized_measurement_fields(
        provider=normalized["provider"],
        location_id=normalized["location_id"],
        sensor_id=normalized["sensor_id"],
        parameter=normalized["parameter"],
        value=normalized["value"],
        unit=normalized["unit"],
        measured_at=dt,
        latitude=normalized["latitude"],
        longitude=normalized["longitude"],
    )
    assert rebuilt["measurement_id"] == normalized["measurement_id"]


def test_risk_score_and_bands() -> None:
    score, band = compute_risk_score_fields(
        fire_count=0,
        max_frp=None,
        avg_pm25=None,
        avg_pm10=None,
    )
    assert score == 0.0
    assert band == "low"

    score2, band2 = compute_risk_score_fields(
        fire_count=20,
        max_frp=500.0,
        avg_pm25=55.0,
        avg_pm10=110.0,
    )
    assert score2 == pytest.approx(100.0)
    assert band2 == "severe"

    assert risk_band(24.9) == "low"
    assert risk_band(25.0) == "moderate"
    assert risk_band(50.0) == "high"
    assert risk_band(75.0) == "severe"
