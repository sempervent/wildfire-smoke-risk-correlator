"""
Wind direction helpers for smoke transport approximations.

Meteorological convention (critical — bugs hide here):
    ``wind_direction_degrees`` is the compass bearing **wind flows FROM**
    (where the air mass originates). Smoke advection is modeled as moving
    **toward** the opposite bearing (“downwind”), i.e. roughly::

        downwind_bearing = wind_from + 180° (normalized)

Example: wind_from_degrees = 270° means wind from the west; modeled downwind
bearing is toward the east (90°).
"""

from __future__ import annotations

import math

_CARDINALS_16: tuple[str, ...] = (
    "N",
    "NNE",
    "NE",
    "ENE",
    "E",
    "ESE",
    "SE",
    "SSE",
    "S",
    "SSW",
    "SW",
    "WSW",
    "W",
    "WNW",
    "NW",
    "NNW",
)


def normalize_degrees(degrees: float) -> float:
    """Return ``degrees`` mapped into ``[0, 360)``."""

    x = degrees % 360.0
    return x + 360.0 if x < 0 else x


def angular_difference_degrees(a: float, b: float) -> float:
    """Smallest absolute difference between two compass headings, in ``[0, 180]``."""

    da = normalize_degrees(a)
    db = normalize_degrees(b)
    diff = abs(da - db)
    return min(diff, 360.0 - diff)


def downwind_bearing(wind_from_degrees: float | None) -> float | None:
    """Bearing (degrees) smoke tends toward for meteorological *wind from* input."""

    if wind_from_degrees is None:
        return None
    return normalize_degrees(float(wind_from_degrees) + 180.0)


def bearing_degrees(source_lon: float, source_lat: float, target_lon: float, target_lat: float) -> float:
    """Initial compass bearing from source point to target point, ``[0, 360)``."""

    phi1 = math.radians(source_lat)
    phi2 = math.radians(target_lat)
    dlon = math.radians(target_lon - source_lon)
    y = math.sin(dlon) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlon)
    brng = math.degrees(math.atan2(y, x))
    return normalize_degrees(brng)


def approximate_distance_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Great-circle distance (Haversine) in kilometers."""

    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    h = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(h)))


def degrees_to_cardinal(degrees: float) -> str:
    """16-point compass label for a normalized meteorological *wind from* direction."""

    idx = int((normalize_degrees(degrees) + 11.25) // 22.5) % 16
    return _CARDINALS_16[idx]


def is_downwind(
    source_lon: float,
    source_lat: float,
    target_lon: float,
    target_lat: float,
    wind_from_degrees: float,
    tolerance_degrees: float,
) -> bool:
    """Return True if target lies within ``tolerance_degrees`` of modeled downwind from source."""

    dw = downwind_bearing(wind_from_degrees)
    if dw is None:
        return False
    bear = bearing_degrees(source_lon, source_lat, target_lon, target_lat)
    return angular_difference_degrees(bear, dw) <= tolerance_degrees
