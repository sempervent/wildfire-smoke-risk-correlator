"""Report or archive aged parse_errors rows (no deletes by default)."""

from __future__ import annotations

import argparse
import json
import logging
import os
from typing import Any

from wildfire_smoke.db.connection import connect
from wildfire_smoke.settings import Settings

log = logging.getLogger(__name__)


def _dry_run_default() -> bool:
    return os.environ.get("DRY_RUN", "1").strip().lower() in {"1", "true", "yes"}


def summarize_candidates(conn: Any, *, older_than_days: int, status: str) -> list[tuple[Any, ...]]:
    q = """
        SELECT source_topic, target_dataset, error_class, COUNT(*)::bigint AS cnt
        FROM analytics.parse_errors
        WHERE status = %s::text
          AND COALESCE(updated_at, last_seen_at) < (now() - (%s * interval '1 day'))
        GROUP BY source_topic, target_dataset, error_class
        ORDER BY cnt DESC
        """
    with conn.cursor() as cur:
        cur.execute(q, (status, older_than_days))
        return list(cur.fetchall())


def archive_candidates(conn: Any, *, older_than_days: int, status: str) -> int:
    q = """
        UPDATE analytics.parse_errors
        SET status = 'archived',
            updated_at = now()
        WHERE status = %s::text
          AND COALESCE(updated_at, last_seen_at) < (now() - (%s * interval '1 day'))
        """
    with conn.cursor() as cur:
        cur.execute(q, (status, older_than_days))
        return cur.rowcount


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    parser = argparse.ArgumentParser(description="Compact / archive aged parse_errors")
    parser.add_argument("--no-dry-run", action="store_true", help="Mark aged rows archived (requires archived status in DDL)")
    parser.add_argument("--older-than-days", type=int, default=int(os.environ.get("PARSE_ERROR_COMPACT_OLDER_THAN_DAYS", "30")))
    parser.add_argument("--status", default=os.environ.get("PARSE_ERROR_COMPACT_STATUS", "resolved"))
    args = parser.parse_args()
    dry = not args.no_dry_run and _dry_run_default()
    settings = Settings.from_env()

    with connect(settings) as conn:
        rows = summarize_candidates(conn, older_than_days=args.older_than_days, status=args.status)
        print(json.dumps({"dry_run": dry, "status_filter": args.status, "older_than_days": args.older_than_days, "groups": rows}, default=str))
        if dry:
            log.info("compact_parse_errors_dry_run", extra={"groups": len(rows)})
            return
        n = archive_candidates(conn, older_than_days=args.older_than_days, status=args.status)
        conn.commit()
        log.info("compact_parse_errors_archived", extra={"updated_rows": n})


if __name__ == "__main__":
    main()
