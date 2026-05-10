from __future__ import annotations

from datetime import datetime, timedelta, timezone

from wildfire_smoke.fixture_time import (
    compute_shift_to_anchor,
    rewrite_firms_rows,
    rewrite_grid_weather_dict,
    rewrite_openaq_envelope,
    rewrite_wind_json_object,
)


def test_compute_shift_maps_latest_to_target() -> None:
    now = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
    anchors = [
        datetime(2026, 5, 9, 10, 0, tzinfo=timezone.utc),
        datetime(2026, 5, 9, 18, 0, tzinfo=timezone.utc),
    ]
    shift = compute_shift_to_anchor(anchors, base_hours_ago=1.0, now=now)
    latest = datetime(2026, 5, 9, 18, 0, tzinfo=timezone.utc)
    assert latest + shift == now - timedelta(hours=1)


def test_rewrite_firms_rows_preserves_relative_spacing() -> None:
    rows = [
        {"latitude": "36", "longitude": "-86", "acq_date": "20260509", "acq_time": "1200"},
        {"latitude": "36", "longitude": "-86", "acq_date": "20260509", "acq_time": "1300"},
    ]
    shift = timedelta(hours=2)
    originals = rewrite_firms_rows(rows, shift)
    assert len(originals) == 2
    assert rows[0]["acq_time"] == "1400"
    assert rows[1]["acq_time"] == "1500"


def test_rewrite_openaq_and_wind_and_grid() -> None:
    env = {
        "record": {
            "normalized": {"measured_at": "2026-05-09T12:00:00+00:00"},
            "measurement": {
                "period": {
                    "datetimeFrom": {"utc": "2026-05-09T12:00:00Z"},
                    "datetimeTo": {"utc": "2026-05-09T13:00:00Z"},
                }
            },
        }
    }
    shift = timedelta(hours=1)
    orig = rewrite_openaq_envelope(env, shift)
    assert orig is not None
    assert "13:00:00" in env["record"]["normalized"]["measured_at"]

    wind = {"observed_at": "2026-05-09T12:00:00+00:00"}
    rewrite_wind_json_object(wind, shift)
    assert wind["observed_at"].startswith("2026-05-09T13:")

    grid = {"valid_time": "2026-05-09T12:00:00+00:00", "cells": [{}]}
    rewrite_grid_weather_dict(grid, shift)
    assert "13:00:00" in grid["valid_time"]
