from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from wildfire_smoke.dlq import (
    build_dlq_envelope,
    classify_parse_exception,
    payload_hash,
    sanitize_payload_sample,
)
from wildfire_smoke.firms_csv import normalized_fire_fields
from wildfire_smoke.openaq_records import parse_openaq_datetime
from wildfire_smoke.wind_records import parse_wind_envelope_record


def test_payload_hash_stable() -> None:
    b = b'{"a":1,"b":"x"}'
    assert payload_hash(b) == payload_hash(b)
    assert len(payload_hash(b)) == 64


def test_sanitize_redacts_and_truncates() -> None:
    big = {"api_key": "secret", "nested": {"Authorization": "bearer x"}, "ok": 1}
    s = sanitize_payload_sample(big, max_bytes=200)
    assert s["api_key"] == "[REDACTED]"
    assert s["nested"]["Authorization"] == "[REDACTED]"
    assert s["ok"] == 1

    huge = {"x": "y" * 50000}
    t = sanitize_payload_sample(huge, max_bytes=256)
    assert t["_truncated"] is True
    assert "preview" in t


def test_classify_parse_exception() -> None:
    assert classify_parse_exception(KeyError("x")) == "KeyError"
    assert classify_parse_exception(json.JSONDecodeError("msg", "", 0)) == "JSONDecodeError"
    assert classify_parse_exception(ValueError("bad")) == "ValueError"
    assert classify_parse_exception(RuntimeError()) == "RuntimeError"


def test_build_dlq_envelope_shape() -> None:
    ts = datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc)
    env = build_dlq_envelope(
        source_topic="t.raw",
        target_dataset="normalized.x",
        consumer_group="g",
        original_key="k",
        original_partition=0,
        original_offset=99,
        payload_hash_hex="abc",
        error_class="ValueError",
        error_message="oops",
        error_context={"n": 1},
        original_payload={"z": 1},
        failed_at=ts,
    )
    assert env["source_topic"] == "t.raw"
    assert env["target_dataset"] == "normalized.x"
    assert env["consumer_group"] == "g"
    assert env["original_key"] == "k"
    assert env["original_partition"] == 0
    assert env["original_offset"] == 99
    assert env["payload_hash"] == "abc"
    assert env["error_class"] == "ValueError"
    assert env["error_message"] == "oops"
    assert env["error_context"] == {"n": 1}
    assert env["original_payload"] == {"z": 1}
    assert env["failed_at"] == ts.isoformat()


def test_replay_from_postgres_dry_run_no_send(monkeypatch: pytest.MonkeyPatch) -> None:
    from wildfire_smoke import replay_dlq as rd

    rows = [
        (
            "00000000-0000-0000-0000-000000000001",
            "firms.hotspots.raw",
            {"record": 1},
            {},
        )
    ]
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = rows

    class CM:
        def __enter__(self):  # noqa: ANN204
            return cur

        def __exit__(self, *exc):  # noqa: ANN002
            return False

    conn.cursor.return_value = CM()

    class ConnCM:
        def __enter__(self):  # noqa: ANN204
            return conn

        def __exit__(self, *exc):  # noqa: ANN002
            return False

    def fake_connect(_settings):  # noqa: ANN001
        return ConnCM()

    monkeypatch.setattr(rd, "connect", fake_connect)

    producer = MagicMock()
    settings = MagicMock()
    n = rd.replay_from_postgres(
        settings=settings,
        producer=producer,
        dry_run=True,
        limit=5,
        source_topic_filter=None,
        target_dataset_filter=None,
        status_filter="open",
    )
    assert n == 1
    producer.send.assert_not_called()


def test_bad_firms_fixture_rows_raise() -> None:
    with pytest.raises(ValueError, match="acq_date"):
        normalized_fire_fields("SRC", {"latitude": "36.1", "longitude": "-86.7"})
    with pytest.raises(ValueError):
        normalized_fire_fields(
            "SRC",
            {
                "latitude": "not-a-lat",
                "longitude": "-86.7",
                "acq_date": "20240509",
                "acq_time": "1630",
            },
        )


def test_bad_openaq_fixture_missing_value() -> None:
    normalized = {
        "measurement_id": "bad-no-value",
        "parameter": "pm25",
        "unit": "µg/m³",
        "measured_at": "2026-05-09T11:00:00+00:00",
        "latitude": 36.0,
        "longitude": -86.0,
    }
    with pytest.raises(KeyError):
        float(normalized["value"])


def test_bad_openaq_non_numeric_lat() -> None:
    normalized = {
        "measurement_id": "bad-lat",
        "provider": "x",
        "location_id": "l",
        "sensor_id": "s",
        "parameter": "pm25",
        "value": 1,
        "unit": "µg/m³",
        "measured_at": "2026-05-09T11:00:00+00:00",
        "latitude": "oops",
        "longitude": -86.0,
    }
    with pytest.raises(ValueError):
        float(normalized["latitude"])


def test_bad_wind_envelope_shape_and_non_numeric_lat() -> None:
    with pytest.raises(ValueError):
        parse_wind_envelope_record({"wind_observation_id": "x", "source": "fixture"})
    env = {
        "source": "fixture",
        "record": {
            "normalized": {
                "wind_observation_id": "wo-bad-lat",
                "source": "fixture",
                "observed_at": "2026-06-01T18:00:00+00:00",
                "latitude": "not-a-number",
                "longitude": -86.7,
                "wind_speed_mps": 1,
                "wind_direction_degrees": 180,
                "wind_gust_mps": None,
            }
        },
    }
    rec = env["record"]["normalized"]
    with pytest.raises(ValueError):
        float(rec["latitude"])


def test_parse_openaq_datetime_accepts_z() -> None:
    dt = parse_openaq_datetime("2026-05-09T11:00:00Z")
    assert dt.tzinfo is not None
