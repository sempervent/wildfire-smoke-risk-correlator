from __future__ import annotations

import pytest

from wildfire_smoke.wind_records import normalized_wind_from_dict, parse_wind_envelope_record


def test_normalized_wind_from_fixture_line() -> None:
    raw = {
        "wind_observation_id": "wo-x",
        "source": "fixture",
        "station_id": "KX",
        "observed_at": "2024-06-01T18:00:00+00:00",
        "latitude": 36.0,
        "longitude": -86.7,
        "wind_speed_mps": 5.0,
        "wind_direction_degrees": 225.0,
        "wind_gust_mps": None,
    }
    n = normalized_wind_from_dict(raw)
    assert n["wind_observation_id"] == "wo-x"
    assert n["wind_gust_mps"] is None


def test_parse_envelope_roundtrip() -> None:
    env = {
        "source": "fixture",
        "fetched_at": "2024-01-01T00:00:00Z",
        "record": {
            "normalized": {
                "wind_observation_id": "wo-y",
                "source": "fixture",
                "observed_at": "2024-06-01T18:00:00+00:00",
                "latitude": 36.0,
                "longitude": -86.7,
                "wind_speed_mps": None,
                "wind_direction_degrees": None,
                "wind_gust_mps": None,
                "station_id": None,
            }
        },
    }
    out = parse_wind_envelope_record(env)
    assert out["wind_observation_id"] == "wo-y"


def test_envelope_missing_normalized_raises() -> None:
    with pytest.raises(ValueError, match="record.normalized"):
        parse_wind_envelope_record({"record": {}})
