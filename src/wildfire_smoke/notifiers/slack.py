from __future__ import annotations

import json
from typing import Any

import httpx

from wildfire_smoke.notifiers.base import AlertEventRow, Notifier


def build_slack_payload(alerts: list[AlertEventRow]) -> dict[str, Any]:
    lines: list[str] = []
    for a in alerts:
        geo = ""
        if a.geography_type or a.geoid:
            geo = f" `{a.geography_type}:{a.geoid}`"
        lines.append(f"*{a.severity.upper()}* `{a.alert_type}`{geo} — {a.title}")
        lines.append(f"> {a.description}")
    text = "\n".join(lines)
    return {
        "text": f"Smoke correlator alerts ({len(alerts)})",
        "attachments": [{"color": "danger", "text": text}],
    }


class SlackWebhookNotifier(Notifier):
    key = "slack"

    def __init__(self, url: str) -> None:
        if not url or not str(url).strip():
            raise RuntimeError("SLACK_WEBHOOK_URL is required for slack notifier")
        self._url = str(url).strip()

    @property
    def url(self) -> str:
        return self._url

    def send(self, alerts: list[AlertEventRow]) -> None:
        # Include machine-readable blocks inside attachment footer for debugging without secrets.
        payload = build_slack_payload(alerts)
        payload["attachments"][0]["footer"] = json.dumps(
            {"fingerprints": [a.fingerprint for a in alerts]},
            separators=(",", ":"),
        )
        body = json.dumps(payload, default=str).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(self._url, content=body, headers=headers)
            resp.raise_for_status()
