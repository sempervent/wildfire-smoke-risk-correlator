from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from wildfire_smoke.notifiers.base import AlertEventRow
from wildfire_smoke.notifiers.slack import build_slack_payload
from wildfire_smoke.notifiers.webhook import WebhookNotifier, _safe_alert_payload


def _sample_row() -> AlertEventRow:
    now = datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc)
    return AlertEventRow(
        alert_event_id=uuid4(),
        fingerprint="abc",
        alert_type="stale_firms_normalized",
        severity="critical",
        geography_type=None,
        geoid=None,
        title="Stale FIRMS-derived fire timestamps",
        description="MAX(...) = ...",
        observed_at=now,
        first_seen_at=now,
        last_seen_at=now,
        details={"max_acq_datetime": "2024-01-01"},
    )


def test_safe_alert_payload_has_no_secrets() -> None:
    payload = _safe_alert_payload(_sample_row())
    assert payload["alert_type"] == "stale_firms_normalized"
    assert "password" not in payload


def test_slack_payload_structure() -> None:
    slack = build_slack_payload([_sample_row()])
    assert "text" in slack
    assert slack["attachments"][0]["text"]


def test_webhook_requires_url() -> None:
    with pytest.raises(RuntimeError):
        WebhookNotifier("")
