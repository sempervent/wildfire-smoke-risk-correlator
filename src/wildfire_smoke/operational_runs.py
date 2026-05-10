"""Lightweight operational cycle bookkeeping in analytics.operational_runs."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from uuid import UUID

from wildfire_smoke.db.connection import connect
from wildfire_smoke.notification_reliability import safe_truncate
from wildfire_smoke.settings import Settings

log = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def start_run(*, mode: str) -> UUID:
    if mode not in {"fixture", "live"}:
        raise ValueError("mode must be fixture or live")
    with connect(Settings.from_env()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analytics.operational_runs (mode, status, steps)
                VALUES (%s, 'running', %s::jsonb)
                RETURNING operational_run_id
                """,
                (mode, json.dumps([], default=str)),
            )
            rid = cur.fetchone()[0]
        conn.commit()
    log.info("operational_run_started run_id=%s mode=%s", rid, mode)
    return rid


def append_step(*, run_id: UUID, name: str, status: str) -> None:
    payload = [{"name": name, "status": status, "ts": _utc_now_iso()}]
    with connect(Settings.from_env()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE analytics.operational_runs
                SET steps = COALESCE(steps, '[]'::jsonb) || %s::jsonb
                WHERE operational_run_id = %s
                """,
                (json.dumps(payload, default=str), run_id),
            )
        conn.commit()


def finish_run(*, run_id: UUID, status: str, error_message: str | None = None) -> None:
    if status not in {"succeeded", "failed"}:
        raise ValueError("status must be succeeded or failed")
    with connect(Settings.from_env()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE analytics.operational_runs
                SET finished_at = now(),
                    status = %s,
                    error_message = %s
                WHERE operational_run_id = %s
                """,
                (status, safe_truncate(error_message, 800), run_id),
            )
        conn.commit()
    log.info("operational_run_finished run_id=%s status=%s", run_id, status)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    parser = argparse.ArgumentParser(prog="wildfire_smoke.operational_runs")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_start = sub.add_parser("start", help="Insert a running operational cycle row")
    p_start.add_argument("--mode", required=True, choices=["fixture", "live"])
    p_start.set_defaults(func=_cmd_start)

    p_step = sub.add_parser("step", help="Append a JSON step entry")
    p_step.add_argument("--run-id", required=True)
    p_step.add_argument("--name", required=True)
    p_step.add_argument("--status", required=True)
    p_step.set_defaults(func=_cmd_step)

    p_finish = sub.add_parser("finish", help="Finalize an operational cycle row")
    p_finish.add_argument("--run-id", required=True)
    p_finish.add_argument("--status", required=True, choices=["succeeded", "failed"])
    p_finish.add_argument("--error", default=None)
    p_finish.set_defaults(func=_cmd_finish)

    args = parser.parse_args()
    raise SystemExit(args.func(args))


def _cmd_start(args: argparse.Namespace) -> int:
    rid = start_run(mode=str(args.mode))
    print(str(rid))
    return 0


def _cmd_step(args: argparse.Namespace) -> int:
    append_step(run_id=UUID(str(args.run_id)), name=str(args.name), status=str(args.status))
    return 0


def _cmd_finish(args: argparse.Namespace) -> int:
    finish_run(run_id=UUID(str(args.run_id)), status=str(args.status), error_message=args.error)
    return 0


if __name__ == "__main__":
    main()
