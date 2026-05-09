from __future__ import annotations

import math

from wildfire_smoke import wind


def test_normalize_degrees() -> None:
    assert math.isclose(wind.normalize_degrees(-10), 350.0)
    assert math.isclose(wind.normalize_degrees(370), 10.0)


def test_downwind_from_west_is_east() -> None:
    assert math.isclose(wind.downwind_bearing(270.0) or 0.0, 90.0)


def test_angular_difference_symmetric() -> None:
    assert math.isclose(wind.angular_difference_degrees(10, 350), 20.0)
    assert math.isclose(wind.angular_difference_degrees(0, 180), 180.0)


def test_bearing_cardinal_ns() -> None:
    assert math.isclose(wind.bearing_degrees(0, 0, 0, 1), 0.0)
    assert math.isclose(wind.bearing_degrees(0, 1, 0, 0), 180.0)


def test_approximate_distance_small() -> None:
    d = wind.approximate_distance_km(0, 0, 0, 1)
    assert 110 < d < 112


def test_is_downwind_smoke_travels_opposite_wind_from() -> None:
    # Wind FROM west (270°): modeled downwind bearing ~90° (east).
    assert wind.is_downwind(-86.7, 36.1, -86.0, 36.1, 270.0, 45.0) is True


def test_degrees_to_cardinal() -> None:
    assert wind.degrees_to_cardinal(0) == "N"
    assert wind.degrees_to_cardinal(270) == "W"
