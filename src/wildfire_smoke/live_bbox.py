"""Bounded bbox parsing and sanity checks for live ingestion."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class BBox:
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float

    @property
    def lon_span(self) -> float:
        return abs(self.max_lon - self.min_lon)

    @property
    def lat_span(self) -> float:
        return abs(self.max_lat - self.min_lat)


def parse_bbox(raw: str) -> BBox:
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox must be min_lon,min_lat,max_lon,max_lat")
    min_lon, min_lat, max_lon, max_lat = (float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3]))
    if min_lon >= max_lon or min_lat >= max_lat:
        raise ValueError("bbox min/max ordering invalid")
    if not (-180.0 <= min_lon <= 180.0 and -180.0 <= max_lon <= 180.0):
        raise ValueError("longitude out of range")
    if not (-90.0 <= min_lat <= 90.0 and -90.0 <= max_lat <= 90.0):
        raise ValueError("latitude out of range")
    return BBox(min_lon=min_lon, min_lat=min_lat, max_lon=max_lon, max_lat=max_lat)


def max_span_degrees_from_env() -> float:
    raw = os.environ.get("LIVE_INGEST_MAX_SPAN_DEG")
    if raw is None or not str(raw).strip():
        return 14.0
    return float(str(raw).strip())


def bbox_allowed_for_live_ingest(bbox: BBox, *, max_span_degrees: float | None = None) -> bool:
    limit = max_span_degrees if max_span_degrees is not None else max_span_degrees_from_env()
    return max(bbox.lon_span, bbox.lat_span) <= limit


def assert_bbox_allowed_for_live_ingest(raw: str | None = None) -> BBox:
    """Parse LIVE_INGEST_BBOX (or argument), refuse oversized areas unless LIVE_INGEST_ALLOW_LARGE_BBOX=1."""
    combined = raw if raw is not None else os.environ.get("LIVE_INGEST_BBOX", "")
    if not str(combined).strip():
        raise ValueError("LIVE_INGEST_BBOX is required for live ingest bounding checks")
    bbox = parse_bbox(combined)
    allow = os.environ.get("LIVE_INGEST_ALLOW_LARGE_BBOX", "0").strip().lower() in {"1", "true", "yes"}
    if allow:
        return bbox
    if not bbox_allowed_for_live_ingest(bbox):
        lim = max_span_degrees_from_env()
        raise ValueError(
            f"bbox span too large (max of lon/lat span must be <= {lim} degrees); "
            "narrow LIVE_INGEST_BBOX or set LIVE_INGEST_ALLOW_LARGE_BBOX=1 with intent."
        )
    return bbox
