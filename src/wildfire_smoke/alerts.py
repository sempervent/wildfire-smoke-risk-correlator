"""Materialize analytics.fn_alert_candidates into analytics.alert_events and dispatch notifications."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.types.json import Json

from wildfire_smoke.alert_thresholds import alert_thresholds_from_env
from wildfire_smoke.alert_delivery import send_notifications_impl
from wildfire_smoke.db.connection import connect
from wildfire_smoke.runbooks import load_runbook_mappings, runbook_slug_for_alert_type
from wildfire_smoke.severity import normalize_db_severity
from wildfire_smoke.settings import Settings

log = logging.getLogger(__name__)


def stable_fingerprint_details(alert_type: str, details: dict[str, Any]) -> dict[str, Any]:
    if alert_type == "ingestion_failed":
        return {"source": details.get("source")}
    if alert_type == "high_smoke_risk":
        return {
            "model_version": details.get("model_version"),
            "risk_band": details.get("risk_band"),
        }
    if alert_type in {"no_recent_fire_detections", "no_recent_air_quality"}:
        return {}
    if alert_type in {"stale_firms_normalized", "stale_openaq_normalized", "wind_data_stale"}:
        return {}
    if alert_type == "high_plume_exposure":
        return {
            "model_version": details.get("model_version"),
            "max_exposure_score": details.get("max_exposure_score"),
        }
    if alert_type in {"no_recent_wind_data"}:
        return {}
    if alert_type in {
        "parse_errors_high",
        "parser_failure_spike",
        "dlq_records_present",
        "consumer_offset_stale",
    }:
        return {}
    return {}


def fingerprint_for_candidate(
    *,
    alert_type: str,
    raw_severity: str,
    geography_type: str | None,
    geoid: str | None,
    title: str,
    details: dict[str, Any],
) -> str:
    severity_norm = normalize_db_severity(alert_type, raw_severity)
    title_key = (
        ""
        if alert_type
        in {
            "high_smoke_risk",
            "high_plume_exposure",
            "parse_errors_high",
            "parser_failure_spike",
            "dlq_records_present",
            "consumer_offset_stale",
        }
        else title
    )
    payload = {
        "alert_type": alert_type,
        "severity": severity_norm,
        "geography_type": geography_type or "",
        "geoid": geoid or "",
        "title": title_key,
        "detail": stable_fingerprint_details(alert_type, details),
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def fetch_candidates(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
    thr = alert_thresholds_from_env()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT alert_type, severity, geography_type, geoid, title, description, observed_at, details
            FROM analytics.fn_alert_candidates(
              %s, %s, %s::double precision, %s, %s::double precision, %s, %s, %s
            )
            """,
            (
                thr.freshness_warn_hours,
                thr.freshness_critical_hours,
                thr.high_risk_min_score,
                thr.lookback_hours,
                thr.high_plume_exposure_min_score,
                thr.parse_errors_warn_count,
                thr.parse_errors_critical_count,
                thr.consumer_offset_stale_hours,
            ),
        )
        return list(cur.fetchall())


def materialize_alerts(*, dry_run: bool, resolve_missing: bool) -> dict[str, int]:
    maps = load_runbook_mappings()
    thresholds = alert_thresholds_from_env()
    stats = {"candidates": 0, "upserts": 0, "resolved": 0}

    with connect(Settings.from_env()) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        rows = fetch_candidates(conn)
        stats["candidates"] = len(rows)

        fingerprints: set[str] = set()
        planned: list[tuple[str, str, str, Any, Any, str, str, Any, dict[str, Any], str | None]] = []

        for alert_type, raw_sev, geography_type, geoid, title, description, observed_at, details in rows:
            details_dict = dict(details or {})
            observed_at_eff = observed_at if observed_at is not None else datetime.now(timezone.utc)
            fp = fingerprint_for_candidate(
                alert_type=str(alert_type),
                raw_severity=str(raw_sev),
                geography_type=geography_type,
                geoid=geoid,
                title=str(title),
                details=details_dict,
            )
            fingerprints.add(fp)
            sev_norm = normalize_db_severity(str(alert_type), str(raw_sev))
            slug = runbook_slug_for_alert_type(str(alert_type), maps)
            planned.append(
                (
                    fp,
                    str(alert_type),
                    sev_norm,
                    geography_type,
                    geoid,
                    str(title),
                    str(description),
                    observed_at_eff,
                    details_dict,
                    slug,
                )
            )

        if dry_run:
            log.info("alerts_materialize_dry_run candidates=%s fingerprints=%s", len(rows), len(fingerprints))
            for p in planned:
                log.info("would upsert fingerprint=%s type=%s severity=%s", p[0], p[1], p[2])
            if resolve_missing:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT fingerprint
                        FROM analytics.alert_events
                        WHERE status = 'open'
                          AND fingerprint <> ALL(%s::text[])
                        """,
                        (list(fingerprints),),
                    )
                    missing_fps = [r[0] for r in cur.fetchall()]
                stats["resolved"] = len(missing_fps)
                for fp in missing_fps:
                    log.info("would resolve fingerprint=%s", fp)
            return stats

        with conn.cursor() as cur:
            for (
                fp,
                alert_type,
                sev_norm,
                geography_type,
                geoid,
                title,
                description,
                observed_at,
                details_dict,
                slug,
            ) in planned:
                cur.execute(
                    """
                    INSERT INTO analytics.alert_events (
                        fingerprint,
                        alert_type,
                        severity,
                        geography_type,
                        geoid,
                        title,
                        description,
                        observed_at,
                        details,
                        runbook_slug,
                        status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'open')
                    ON CONFLICT (fingerprint) WHERE (status IN ('open', 'acknowledged'))
                    DO UPDATE SET
                        severity = EXCLUDED.severity,
                        geography_type = EXCLUDED.geography_type,
                        geoid = EXCLUDED.geoid,
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        observed_at = EXCLUDED.observed_at,
                        details = EXCLUDED.details,
                        runbook_slug = EXCLUDED.runbook_slug,
                        last_seen_at = now(),
                        updated_at = now()
                    """,
                    (
                        fp,
                        alert_type,
                        sev_norm,
                        geography_type,
                        geoid,
                        title,
                        description,
                        observed_at,
                        Json(details_dict),
                        slug,
                    ),
                )
                stats["upserts"] += 1

            if resolve_missing:
                if fingerprints:
                    cur.execute(
                        """
                        UPDATE analytics.alert_events
                        SET status = 'resolved',
                            updated_at = now()
                        WHERE status = 'open'
                          AND fingerprint <> ALL(%s::text[])
                        """,
                        (list(fingerprints),),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE analytics.alert_events
                        SET status = 'resolved',
                            updated_at = now()
                        WHERE status = 'open'
                        """
                    )
                stats["resolved"] = cur.rowcount or 0

        conn.commit()

    _ = thresholds  # thresholds drive candidate fetch; kept for future metrics
    return stats


def cmd_materialize(args: argparse.Namespace) -> int:
    dry = os.environ.get("ALERTS_DRY_RUN", "0").strip().lower() in {"1", "true", "yes"}
    if getattr(args, "dry_run", False):
        dry = True
    resolve = os.environ.get("ALERTS_RESOLVE_MISSING", "0").strip().lower() in {"1", "true", "yes"}
    if getattr(args, "resolve_missing", False):
        resolve = True
    stats = materialize_alerts(dry_run=dry, resolve_missing=resolve)
    print(json.dumps({"dry_run": dry, "resolve_missing": resolve, **stats}, default=str))
    return 0


def cmd_send(args: argparse.Namespace) -> int:
    stats = send_notifications_impl(digest=args.digest, retry_queue=args.retry_queue)
    print(json.dumps(stats, default=str))
    return 0


def build_parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="wildfire_smoke.alerts")
    sub = root.add_subparsers(dest="cmd", required=True)

    p_mat = sub.add_parser("materialize", help="Upsert analytics.alert_events from v_alert_candidates logic")
    p_mat.add_argument("--dry-run", action="store_true", help="Print actions without writing (also ALERTS_DRY_RUN=1)")
    p_mat.add_argument(
        "--resolve-missing",
        action="store_true",
        help="Resolve open alerts absent from current candidates (also ALERTS_RESOLVE_MISSING=1)",
    )
    p_mat.set_defaults(func=cmd_materialize)

    p_send = sub.add_parser("send", help="Dispatch notifications for open alert_events")
    p_send.add_argument("--digest", action="store_true", help="Send one digest (also ALERT_DIGEST=1)")
    p_send.add_argument(
        "--retry-queue",
        action="store_true",
        help="Only retry alerts whose last notifier attempt failed (also ALERT_RETRY_QUEUE=1)",
    )
    p_send.set_defaults(func=cmd_send)

    return root


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(args.func(args))


if __name__ == "__main__":
    main()
