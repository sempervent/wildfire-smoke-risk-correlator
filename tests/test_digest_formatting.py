from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from wildfire_smoke.digest import (
    build_digest_summary,
    build_slack_digest_payload,
    build_webhook_digest_payload,
    format_console_digest,
    format_smtp_digest,
)
from wildfire_smoke.notifiers.base import AlertEventRow


def _row(sev: str, title: str, geo: tuple[str | None, str | None] = (None, None)) -> AlertEventRow:
    now = datetime(2026, 5, 9, tzinfo=timezone.utc)
    gt, geoid = geo
    return AlertEventRow(
        alert_event_id=uuid4(),
        fingerprint="fp",
        alert_type="high_smoke_risk",
        severity=sev,
        geography_type=gt,
        geoid=geoid,
        title=title,
        description="d",
        observed_at=now,
        first_seen_at=now,
        last_seen_at=now,
        details={},
        runbook_slug="high-smoke-risk",
    )


def test_console_digest_includes_counts_and_disclaimer() -> None:
    s = build_digest_summary(window_hours=24, alerts=[_row("critical", "t1"), _row("high", "t2")])
    txt = format_console_digest(s)
    assert "critical" in txt or "severities=" in txt
    assert "fn_alert_candidates" in txt


def test_webhook_digest_payload_shape() -> None:
    s = build_digest_summary(window_hours=12, alerts=[_row("high", "x", ("county", "47001"))])
    payload = build_webhook_digest_payload(s)
    assert payload["kind"] == "wildfire_smoke_alert_digest"
    assert payload["count"] == 1
    assert "47001" in str(payload.get("top_geographies"))


def test_slack_digest_payload_has_text() -> None:
    s = build_digest_summary(window_hours=6, alerts=[_row("high", "hello")])
    payload = build_slack_digest_payload(s)
    assert "text" in payload
    assert "hello" in payload["text"]


def test_smtp_digest_matches_console_body() -> None:
    s = build_digest_summary(window_hours=24, alerts=[_row("critical", "boom")])
    assert format_smtp_digest(s) == format_console_digest(s)
