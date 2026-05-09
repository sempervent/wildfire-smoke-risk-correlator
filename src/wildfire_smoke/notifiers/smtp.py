from __future__ import annotations

import smtplib
import ssl
from email.message import EmailMessage

from wildfire_smoke.notifiers.base import AlertEventRow, Notifier


class SmtpNotifier(Notifier):
    key = "smtp"

    def __init__(
        self,
        *,
        host: str,
        port: int,
        user: str | None,
        password: str | None,
        mail_from: str,
        mail_to: str,
    ) -> None:
        if not host.strip():
            raise RuntimeError("SMTP_HOST is required for smtp notifier")
        if not mail_from.strip() or not mail_to.strip():
            raise RuntimeError("ALERT_EMAIL_FROM and ALERT_EMAIL_TO are required for smtp notifier")
        self._host = host.strip()
        self._port = int(port)
        self._user = user.strip() if user else None
        self._password = password
        self._mail_from = mail_from.strip()
        self._mail_to = mail_to.strip()

    def send_plain(self, subject: str, body: str) -> None:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self._mail_from
        msg["To"] = self._mail_to
        msg.set_content(body)
        context = ssl.create_default_context()
        if self._port == 465:
            with smtplib.SMTP_SSL(self._host, self._port, context=context, timeout=30) as smtp:
                if self._user and self._password is not None:
                    smtp.login(self._user, self._password)
                smtp.send_message(msg)
            return
        with smtplib.SMTP(self._host, self._port, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls(context=context)
            smtp.ehlo()
            if self._user and self._password is not None:
                smtp.login(self._user, self._password)
            smtp.send_message(msg)

    def send(self, alerts: list[AlertEventRow]) -> None:
        msg = EmailMessage()
        msg["Subject"] = f"Smoke correlator alerts ({len(alerts)})"
        msg["From"] = self._mail_from
        msg["To"] = self._mail_to
        text_lines: list[str] = []
        for a in alerts:
            text_lines.append(f"[{a.severity}] {a.alert_type} — {a.title}")
            text_lines.append(a.description)
            text_lines.append(f"fingerprint={a.fingerprint}")
            text_lines.append("")
        msg.set_content("\n".join(text_lines))

        context = ssl.create_default_context()
        if self._port == 465:
            with smtplib.SMTP_SSL(self._host, self._port, context=context, timeout=30) as smtp:
                if self._user and self._password is not None:
                    smtp.login(self._user, self._password)
                smtp.send_message(msg)
            return

        with smtplib.SMTP(self._host, self._port, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls(context=context)
            smtp.ehlo()
            if self._user and self._password is not None:
                smtp.login(self._user, self._password)
            smtp.send_message(msg)
