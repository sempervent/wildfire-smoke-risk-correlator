from __future__ import annotations

from wildfire_smoke.notifiers.base import AlertEventRow, Notifier


class ConsoleNotifier(Notifier):
    key = "console"

    def send(self, alerts: list[AlertEventRow]) -> None:
        for a in alerts:
            geo = ""
            if a.geography_type or a.geoid:
                geo = f" [{a.geography_type or ''}:{a.geoid or ''}]".strip()
            print(
                f"[{a.severity.upper()}] {a.alert_type}{geo}\n"
                f"  title: {a.title}\n"
                f"  observed_at: {a.observed_at.isoformat()}\n"
                f"  last_seen_at: {a.last_seen_at.isoformat()}\n"
                f"  description: {a.description}\n"
                f"  fingerprint: {a.fingerprint}\n"
            )
