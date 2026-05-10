from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from wildfire_smoke.grid_weather_records import (
    cells_from_envelope_record,
    mph_to_mps,
    normalized_cell_from_dict,
    parse_iso_datetime,
    parse_wind_speed_to_mps,
)
from wildfire_smoke.settings import repo_root


def test_parse_wind_speed_units() -> None:
    assert parse_wind_speed_to_mps("10 mph") == pytest.approx(mph_to_mps(10.0))
    assert parse_wind_speed_to_mps("18 km/h") == pytest.approx(18.0 / 3.6)


def test_cells_from_fixture_sample() -> None:
    path = repo_root() / "tests/fixtures/nws_gridpoint_sample.json"
    env = json.loads(path.read_text())
    vt = parse_iso_datetime(env["valid_time"])
    assert vt is not None
    cells = cells_from_envelope_record(env, source="nws_gridpoint", grid_id=env.get("grid_id"), valid_time=vt)
    assert len(cells) == 2
    assert cells[0]["wind_speed_mps"] == pytest.approx(4.2)
    assert cells[1]["wind_speed_mps"] == pytest.approx(15.0 / 3.6)
    assert cells[1]["temperature_c"] == pytest.approx((66.0 - 32.0) * 5.0 / 9.0)


def test_cells_from_envelope_requires_list() -> None:
    with pytest.raises(ValueError, match="cells must be a list"):
        cells_from_envelope_record({"cells": None}, source="x", grid_id=None, valid_time=datetime.now(timezone.utc))


def test_normalized_cell_requires_coordinates() -> None:
    vt = datetime(2026, 5, 9, 18, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="latitude"):
        normalized_cell_from_dict({}, source="x", grid_id=None, valid_time=vt)
