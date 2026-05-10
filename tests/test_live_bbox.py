from __future__ import annotations

import pytest

from wildfire_smoke.live_bbox import (
    assert_bbox_allowed_for_live_ingest,
    bbox_allowed_for_live_ingest,
    parse_bbox,
)


def test_parse_bbox_ok() -> None:
    b = parse_bbox("-88.2,34.9,-81.6,36.7")
    assert b.lon_span > 0 and b.lat_span > 0


def test_parse_bbox_invalid_order() -> None:
    with pytest.raises(ValueError):
        parse_bbox("10,0,5,1")


def test_bbox_allowed_small_region() -> None:
    assert bbox_allowed_for_live_ingest(parse_bbox("-88.2,34.9,-81.6,36.7"), max_span_degrees=14.0) is True


def test_bbox_rejects_continental_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LIVE_INGEST_ALLOW_LARGE_BBOX", raising=False)
    huge = parse_bbox("-125,24,-66,50")
    assert bbox_allowed_for_live_ingest(huge, max_span_degrees=14.0) is False


def test_assert_bbox_allowed_respects_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LIVE_INGEST_ALLOW_LARGE_BBOX", "1")
    monkeypatch.setenv("LIVE_INGEST_BBOX", "-125,24,-66,50")
    assert_bbox_allowed_for_live_ingest()
