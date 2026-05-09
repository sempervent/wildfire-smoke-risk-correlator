from __future__ import annotations

import os

from wildfire_smoke.notifiers.base import Notifier
from wildfire_smoke.notifiers.console import ConsoleNotifier
from wildfire_smoke.notifiers.slack import SlackWebhookNotifier
from wildfire_smoke.notifiers.smtp import SmtpNotifier
from wildfire_smoke.notifiers.webhook import WebhookNotifier


def notifier_from_env() -> Notifier:
    name = os.environ.get("ALERT_NOTIFIER", "console").strip().lower()
    if name in {"", "console"}:
        return ConsoleNotifier()
    if name == "webhook":
        return WebhookNotifier(os.environ.get("ALERT_WEBHOOK_URL", ""))
    if name == "slack":
        return SlackWebhookNotifier(os.environ.get("SLACK_WEBHOOK_URL", ""))
    if name in {"smtp", "email"}:
        return SmtpNotifier(
            host=os.environ.get("SMTP_HOST", ""),
            port=int(os.environ.get("SMTP_PORT", "587")),
            user=os.environ.get("SMTP_USER"),
            password=os.environ.get("SMTP_PASSWORD"),
            mail_from=os.environ.get("ALERT_EMAIL_FROM", ""),
            mail_to=os.environ.get("ALERT_EMAIL_TO", ""),
        )
    raise RuntimeError(f"Unsupported ALERT_NOTIFIER={name!r} (try console, webhook, slack, smtp)")
