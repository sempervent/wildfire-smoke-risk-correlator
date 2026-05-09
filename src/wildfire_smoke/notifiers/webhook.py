from __future__ import annotations

import json
import os
from typing import Any

import httpx

from wildfire_smoke.notifiers.base import AlertEventRow, Notifier


def _safe_alert_payload(row: AlertEventRow) -> dict[str, Any]:
    return {
        "alert_event_id": str(row.alert_event_id),
        "fingerprint": row.fingerprint,
        "alert_type": row.alert_type,
        "severity": row.severity,
        "geography_type": row.geography_type,
        "geoid": row.geoid,
        "title": row.title,
        "description": row.description,
        "observed_at": row.observed_at.isoformat(),
        "first_seen_at": row.first_seen_at.isoformat(),
        "last_seen_at": row.last_seen_at.isoformat(),
        "details": row.details,
    }


class WebhookNotifier(Notifier):
    key = "webhook"

    def __init__(self, url: str) -> None:
        if not url or not str(url).strip():
            raise RuntimeError("ALERT_WEBHOOK_URL is required for webhook notifier")
        self._url = str(url).strip()

    def send(self, alerts: list[AlertEventRow]) -> None:
        payload = {"alerts": [_safe_alert_payload(a) for a in alerts]}
        body = json.dumps(payload, default=str).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        extra = os.environ.get("ALERT_WEBHOOK_HEADERS_JSON")
        if extra and str(extra).strip():
            try:
                headers.update(json.loads(extra))
            except json.JSONDecodeError as exc:
                raise RuntimeError("ALERT_WEBHOOK_HEADERS_JSON must be valid JSON object") from exc
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(self._url, content=body, headers=headers)
            resp.raise_for_status()
