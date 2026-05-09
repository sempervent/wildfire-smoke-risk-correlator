"""Notification attempt audit helpers: hashing, backoff, and safe error text."""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse, urlunparse


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def normalize_url_for_hash(url: str) -> str:
    """Strip secrets-in-query best-effort: drop query + fragment, trim whitespace."""
    raw = (url or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    normalized = urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path or "", "", "", ""))
    return normalized


def destination_material(notifier_key: str) -> str:
    key = notifier_key.strip().lower()
    if key in {"", "console"}:
        return "console"
    if key == "webhook":
        return normalize_url_for_hash(os.environ.get("ALERT_WEBHOOK_URL", "")) or "webhook:missing"
    if key == "slack":
        return normalize_url_for_hash(os.environ.get("SLACK_WEBHOOK_URL", "")) or "slack:missing"
    if key in {"smtp", "email"}:
        host = (os.environ.get("SMTP_HOST", "") or "").strip().lower()
        port = (os.environ.get("SMTP_PORT", "587") or "").strip()
        to_addr = (os.environ.get("ALERT_EMAIL_TO", "") or "").strip().lower()
        return f"smtp:{host}:{port}:{to_addr}"
    return f"unknown:{key}"


def destination_hash(notifier_key: str) -> str | None:
    mat = destination_material(notifier_key)
    if not mat:
        return None
    return sha256_hex(mat)


def safe_truncate(message: str | None, limit: int = 400) -> str | None:
    if message is None:
        return None
    msg = str(message).replace("\n", " ").strip()
    if len(msg) <= limit:
        return msg
    return msg[: limit - 3] + "..."


def classify_exception(exc: BaseException) -> str:
    mod = type(exc).__module__
    name = type(exc).__name__
    return f"{mod}.{name}"


def retry_delay_seconds(after_failure_number: int) -> int:
    """failure_number is 1-indexed count of failures including the one just recorded."""
    if after_failure_number <= 1:
        return 5 * 60
    if after_failure_number == 2:
        return 15 * 60
    return 60 * 60


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def retry_disabled_from_env() -> bool:
    return os.environ.get("ALERT_RETRY_DISABLED", "0").strip().lower() in {"1", "true", "yes"}


def max_attempts_from_env() -> int:
    raw = os.environ.get("ALERT_MAX_ATTEMPTS", "5")
    v = int(str(raw).strip())
    return max(1, v)


def retry_queue_only_from_env() -> bool:
    return os.environ.get("ALERT_RETRY_QUEUE", "0").strip().lower() in {"1", "true", "yes"}


def digest_enabled_from_env() -> bool:
    return os.environ.get("ALERT_DIGEST", "0").strip().lower() in {"1", "true", "yes"}


def digest_window_hours_from_env() -> int:
    return max(1, int(os.environ.get("ALERT_DIGEST_WINDOW_HOURS", "24")))


def digest_max_items_from_env() -> int:
    return max(1, int(os.environ.get("ALERT_DIGEST_MAX_ITEMS", "50")))


def payload_hash_bytes(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


def payload_hash_json(data: Any) -> str:
    raw = __import__("json").dumps(data, sort_keys=True, separators=(",", ":"), default=str)
    return payload_hash_bytes(raw.encode("utf-8"))


def compute_retry_after(failure_count: int) -> datetime:
    delay = retry_delay_seconds(failure_count)
    return utcnow() + timedelta(seconds=delay)
