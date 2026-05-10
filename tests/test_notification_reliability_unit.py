from __future__ import annotations

from wildfire_smoke.notification_reliability import (
    compute_retry_after,
    destination_hash,
    normalize_url_for_hash,
    payload_hash_json,
    retry_delay_seconds,
    safe_truncate,
    utcnow,
)


def test_destination_hash_console_stable() -> None:
    h1 = destination_hash("console")
    h2 = destination_hash("console")
    assert h1 == h2
    assert len(h1) == 64


def test_normalize_url_strips_query() -> None:
    a = normalize_url_for_hash("https://hooks.example.com/path?token=secret")
    b = normalize_url_for_hash("https://hooks.example.com/path")
    assert a == b


def test_retry_delay_sequence() -> None:
    assert retry_delay_seconds(1) == 5 * 60
    assert retry_delay_seconds(2) == 15 * 60
    assert retry_delay_seconds(3) == 60 * 60


def test_safe_truncate() -> None:
    long = "x" * 500
    out = safe_truncate(long, limit=10)
    assert out is not None
    assert len(out) <= 10


def test_payload_hash_json_stable() -> None:
    assert payload_hash_json({"b": 1, "a": 2}) == payload_hash_json({"a": 2, "b": 1})


def test_compute_retry_after_moves_forward() -> None:
    ra = compute_retry_after(1)
    assert ra > utcnow()
