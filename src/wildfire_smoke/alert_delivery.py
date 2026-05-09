"""Alert notification dispatch with retries, digest mode, and durable audit rows."""

from __future__ import annotations

import json
import logging
import os
from datetime import timedelta
from typing import Any
from uuid import UUID

import httpx
import psycopg
from psycopg.types.json import Json

from wildfire_smoke.db.connection import connect
from wildfire_smoke.digest import (
    build_digest_summary,
    build_slack_digest_payload,
    build_webhook_digest_payload,
    format_console_digest,
    format_smtp_digest,
)
from wildfire_smoke.notification_reliability import (
    classify_exception,
    compute_retry_after,
    destination_hash,
    digest_enabled_from_env,
    digest_max_items_from_env,
    digest_window_hours_from_env,
    max_attempts_from_env,
    payload_hash_json,
    retry_disabled_from_env,
    retry_queue_only_from_env,
    safe_truncate,
    utcnow,
)
from wildfire_smoke.notifiers import notifier_from_env
from wildfire_smoke.notifiers.base import AlertEventRow
from wildfire_smoke.notifiers.smtp import SmtpNotifier
from wildfire_smoke.notifiers.webhook import WebhookNotifier
from wildfire_smoke.severity import passes_min_severity
from wildfire_smoke.settings import Settings

log = logging.getLogger(__name__)


def send_cooldown_seconds_from_env() -> int:
    raw = os.environ.get("ALERT_SEND_COOLDOWN_SECONDS", "0")
    v = int(str(raw).strip())
    return max(0, v)


def non_skipped_attempt_count(cur: psycopg.Cursor, alert_event_id: UUID, notifier: str) -> int:
    cur.execute(
        """
        SELECT COUNT(*)::bigint
        FROM analytics.notification_attempts
        WHERE alert_event_id = %s
          AND notifier = %s
          AND status <> 'skipped'
        """,
        (alert_event_id, notifier),
    )
    row = cur.fetchone()
    return int(row[0] if row else 0)


def failure_count(cur: psycopg.Cursor, alert_event_id: UUID, notifier: str) -> int:
    cur.execute(
        """
        SELECT COUNT(*)::bigint
        FROM analytics.notification_attempts
        WHERE alert_event_id = %s
          AND notifier = %s
          AND status = 'failed'
        """,
        (alert_event_id, notifier),
    )
    row = cur.fetchone()
    return int(row[0] if row else 0)


def last_attempt_row(cur: psycopg.Cursor, alert_event_id: UUID, notifier: str) -> tuple[Any, ...] | None:
    cur.execute(
        """
        SELECT status, retry_after, attempted_at
        FROM analytics.notification_attempts
        WHERE alert_event_id = %s
          AND notifier = %s
        ORDER BY attempted_at DESC
        LIMIT 1
        """,
        (alert_event_id, notifier),
    )
    hit = cur.fetchone()
    return hit


def insert_attempt(
    cur: psycopg.Cursor,
    *,
    alert_event_id: UUID,
    notifier_key: str,
    dest_hash: str | None,
    status: str,
    error_class: str | None = None,
    error_message: str | None = None,
    response_code: int | None = None,
    retry_after: Any = None,
    payload_hash: str | None = None,
) -> None:
    cur.execute(
        """
        INSERT INTO analytics.notification_attempts (
            alert_event_id,
            notifier,
            destination_hash,
            status,
            attempted_at,
            completed_at,
            error_class,
            error_message,
            response_code,
            retry_after,
            payload_hash
        )
        VALUES (%s, %s, %s, %s, now(), now(), %s, %s, %s, %s, %s)
        """,
        (
            alert_event_id,
            notifier_key,
            dest_hash,
            status,
            error_class,
            error_message,
            response_code,
            retry_after,
            payload_hash,
        ),
    )


def maybe_cooldown_block(cur: psycopg.Cursor, notifier_key: str, cooldown_s: int) -> bool:
    if cooldown_s <= 0:
        return False
    cur.execute(
        """
        SELECT MAX(attempted_at)
        FROM analytics.notification_attempts
        WHERE notifier = %s
        """,
        (notifier_key,),
    )
    row = cur.fetchone()
    last = row[0] if row else None
    if last is None:
        return False
    return last + timedelta(seconds=cooldown_s) > utcnow()


def send_digest_external(notifier_key: str, summary: Any) -> None:
    key = notifier_key.strip().lower()
    if key in {"", "console"}:
        print(format_console_digest(summary))
        return
    if key == "webhook":
        wh = WebhookNotifier(os.environ.get("ALERT_WEBHOOK_URL", ""))
        payload = build_webhook_digest_payload(summary)
        body = json.dumps(payload, default=str).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        extra = os.environ.get("ALERT_WEBHOOK_HEADERS_JSON")
        if extra and str(extra).strip():
            try:
                headers.update(json.loads(extra))
            except json.JSONDecodeError as exc:
                raise RuntimeError("ALERT_WEBHOOK_HEADERS_JSON must be valid JSON object") from exc
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(wh.url, content=body, headers=headers)
            resp.raise_for_status()
        return
    if key == "slack":
        from wildfire_smoke.notifiers.slack import SlackWebhookNotifier

        slack = SlackWebhookNotifier(os.environ.get("SLACK_WEBHOOK_URL", ""))
        payload = build_slack_digest_payload(summary)
        body = json.dumps(payload, default=str).encode("utf-8")
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(slack.url, content=body, headers={"Content-Type": "application/json"})
            resp.raise_for_status()
        return
    if key in {"smtp", "email"}:
        mail = SmtpNotifier(
            host=os.environ.get("SMTP_HOST", ""),
            port=int(os.environ.get("SMTP_PORT", "587")),
            user=os.environ.get("SMTP_USER"),
            password=os.environ.get("SMTP_PASSWORD"),
            mail_from=os.environ.get("ALERT_EMAIL_FROM", ""),
            mail_to=os.environ.get("ALERT_EMAIL_TO", ""),
        )
        mail.send_plain(
            subject=f"Smoke correlator digest ({len(summary.items)} alerts)",
            body=format_smtp_digest(summary),
        )
        return
    raise RuntimeError(f"unsupported notifier for digest: {notifier_key}")


def persist_digest_success(
    conn: psycopg.Connection,
    *,
    notifier_key: str,
    alerts: list[AlertEventRow],
    states_by_id: dict[Any, dict[str, Any]],
    payload_hash: str,
) -> None:
    dest = destination_hash(notifier_key)
    with conn.cursor() as cur:
        for ev in alerts:
            insert_attempt(
                cur,
                alert_event_id=ev.alert_event_id,
                notifier_key=notifier_key,
                dest_hash=dest,
                status="succeeded",
                payload_hash=payload_hash,
            )
            state_dict = dict(states_by_id.get(ev.alert_event_id) or {})
            block = dict(state_dict.get(notifier_key) or {})
            block["last_digest_at"] = utcnow().isoformat()
            block["last_digest_payload_hash"] = payload_hash
            state_dict[notifier_key] = block
            cur.execute(
                """
                UPDATE analytics.alert_events
                SET notification_state = %s::jsonb,
                    updated_at = now()
                WHERE alert_event_id = %s
                """,
                (Json(state_dict), ev.alert_event_id),
            )


def persist_digest_failures(
    conn: psycopg.Connection,
    *,
    notifier_key: str,
    alerts: list[AlertEventRow],
    payload_hash: str,
    err_cls: str,
    err_msg: str | None,
) -> None:
    dest = destination_hash(notifier_key)
    with conn.cursor() as cur:
        for ev in alerts:
            fails = failure_count(cur, ev.alert_event_id, notifier_key)
            ra = compute_retry_after(fails + 1)
            insert_attempt(
                cur,
                alert_event_id=ev.alert_event_id,
                notifier_key=notifier_key,
                dest_hash=dest,
                status="failed",
                error_class=err_cls,
                error_message=err_msg,
                retry_after=ra,
                payload_hash=payload_hash,
            )


def send_notifications_impl(
    *,
    digest: bool,
    retry_queue: bool,
) -> dict[str, Any]:
    force = os.environ.get("FORCE_NOTIFY", "0").strip().lower() in {"1", "true", "yes"}
    min_sev = os.environ.get("ALERT_SEVERITY_MIN", "high").strip().lower()
    limit = int(os.environ.get("ALERT_LIMIT", "20"))
    notifier = notifier_from_env()
    key = notifier.key
    retry_disabled = retry_disabled_from_env()
    max_attempts = max_attempts_from_env()
    cooldown_s = send_cooldown_seconds_from_env()
    retry_only = retry_queue or retry_queue_only_from_env()

    use_digest = digest or digest_enabled_from_env()
    window_h = digest_window_hours_from_env()
    digest_limit = digest_max_items_from_env()

    with connect(Settings.from_env()) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        with conn.cursor() as cur:
            if maybe_cooldown_block(cur, key, cooldown_s):
                log.info("alert_send_cooldown_active notifier=%s seconds=%s", key, cooldown_s)
                return {"sent": 0, "cooldown": True}

        cutoff = utcnow() - timedelta(hours=window_h)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    alert_event_id,
                    fingerprint,
                    alert_type,
                    severity,
                    geography_type,
                    geoid,
                    title,
                    description,
                    observed_at,
                    first_seen_at,
                    last_seen_at,
                    details,
                    runbook_slug,
                    notification_state
                FROM analytics.alert_events
                WHERE status = 'open'
                ORDER BY
                    CASE severity
                        WHEN 'critical' THEN 4
                        WHEN 'high' THEN 3
                        WHEN 'warning' THEN 2
                        WHEN 'info' THEN 1
                        ELSE 0
                    END DESC,
                    last_seen_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            fetched = cur.fetchall()

        rows: list[tuple[AlertEventRow, dict[str, Any]]] = []
        for row in fetched:
            rec = row[:-1]
            state = dict(row[-1] or {})
            ev = AlertEventRow.from_record(rec)
            if use_digest and ev.observed_at < cutoff:
                continue
            if not passes_min_severity(ev.severity, min_sev):
                continue
            rows.append((ev, state))

        if use_digest:
            capped = rows[:digest_limit]
            alerts = [e for e, _ in capped]
            states_by_id = {e.alert_event_id: s for e, s in capped}
            if not alerts:
                return {"sent": 0, "digest": True, "items": 0}
            summary = build_digest_summary(window_hours=window_h, alerts=alerts)
            payload_hash = payload_hash_json(
                {
                    "kind": "digest",
                    "window_hours": window_h,
                    "fingerprints": [a.fingerprint for a in alerts],
                    "counts": summary.counts_by_severity,
                }
            )
            try:
                send_digest_external(key, summary)
                persist_digest_success(
                    conn,
                    notifier_key=key,
                    alerts=alerts,
                    states_by_id=states_by_id,
                    payload_hash=payload_hash,
                )
                conn.commit()
                return {"sent": len(alerts), "digest": True}
            except BaseException as exc:
                err_cls = classify_exception(exc)
                err_msg = safe_truncate(str(exc))
                persist_digest_failures(
                    conn,
                    notifier_key=key,
                    alerts=alerts,
                    payload_hash=payload_hash,
                    err_cls=err_cls,
                    err_msg=err_msg,
                )
                conn.commit()
                log.exception("digest_send_failed notifier=%s", key)
                raise

        sent = 0
        with conn.cursor() as cur:
            for ev, state in rows:
                dest = destination_hash(key)

                attempts = non_skipped_attempt_count(cur, ev.alert_event_id, key)
                if attempts >= max_attempts:
                    insert_attempt(
                        cur,
                        alert_event_id=ev.alert_event_id,
                        notifier_key=key,
                        dest_hash=dest,
                        status="skipped",
                        error_class="max_attempts",
                        error_message=safe_truncate(f"max_attempts={max_attempts}"),
                        payload_hash=None,
                    )
                    continue

                last = last_attempt_row(cur, ev.alert_event_id, key)
                if retry_only:
                    if last is None or last[0] != "failed":
                        insert_attempt(
                            cur,
                            alert_event_id=ev.alert_event_id,
                            notifier_key=key,
                            dest_hash=dest,
                            status="skipped",
                            error_class="retry_queue_filter",
                            error_message="not_retry_eligible",
                            payload_hash=None,
                        )
                        continue

                if last and last[0] == "failed" and last[1] is not None and not retry_disabled:
                    if last[1] > utcnow():
                        insert_attempt(
                            cur,
                            alert_event_id=ev.alert_event_id,
                            notifier_key=key,
                            dest_hash=dest,
                            status="skipped",
                            error_class="retry_backoff",
                            error_message=safe_truncate(f"retry_after={last[1].isoformat()}"),
                            retry_after=last[1],
                            payload_hash=None,
                        )
                        continue

                block = dict(state.get(key) or {})
                prev = block.get("last_sent_last_seen_at")
                if not force and prev == ev.last_seen_at.isoformat():
                    insert_attempt(
                        cur,
                        alert_event_id=ev.alert_event_id,
                        notifier_key=key,
                        dest_hash=dest,
                        status="skipped",
                        error_class="already_current",
                        error_message="notification_state_suppressed",
                        payload_hash=None,
                    )
                    continue

                payload_hash = payload_hash_json(
                    {
                        "alert_event_id": str(ev.alert_event_id),
                        "fingerprint": ev.fingerprint,
                        "severity": ev.severity,
                        "last_seen_at": ev.last_seen_at.isoformat(),
                    }
                )

                cur.execute("SAVEPOINT alert_send")
                try:
                    notifier.send([ev])
                    insert_attempt(
                        cur,
                        alert_event_id=ev.alert_event_id,
                        notifier_key=key,
                        dest_hash=dest,
                        status="succeeded",
                        payload_hash=payload_hash,
                    )
                    state_dict = dict(state)
                    inner = dict(state_dict.get(key) or {})
                    inner["last_sent_last_seen_at"] = ev.last_seen_at.isoformat()
                    state_dict[key] = inner
                    cur.execute(
                        """
                        UPDATE analytics.alert_events
                        SET notification_state = %s::jsonb,
                            updated_at = now()
                        WHERE alert_event_id = %s
                        """,
                        (Json(state_dict), ev.alert_event_id),
                    )
                    cur.execute("RELEASE SAVEPOINT alert_send")
                    sent += 1
                except BaseException as exc:
                    cur.execute("ROLLBACK TO SAVEPOINT alert_send")
                    fails = failure_count(cur, ev.alert_event_id, key)
                    ra = compute_retry_after(fails + 1)
                    insert_attempt(
                        cur,
                        alert_event_id=ev.alert_event_id,
                        notifier_key=key,
                        dest_hash=dest,
                        status="failed",
                        error_class=classify_exception(exc),
                        error_message=safe_truncate(str(exc)),
                        retry_after=ra,
                        payload_hash=payload_hash,
                    )
                    log.exception("notifier_send_failed alert_event_id=%s notifier=%s", ev.alert_event_id, key)

        conn.commit()

    return {"sent": sent, "digest": False}


def send_notifications() -> dict[str, Any]:
    return send_notifications_impl(digest=False, retry_queue=False)
