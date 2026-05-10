from __future__ import annotations

import logging
import uuid
from typing import Any

from psycopg import Connection
from psycopg.types.json import Json

log = logging.getLogger(__name__)


def create_run(
    conn: Connection,
    *,
    source: str,
    mode: str,
    config: dict[str, Any],
) -> uuid.UUID:
    if mode not in {"live", "dry_run"}:
        raise ValueError(f"invalid ingestion mode: {mode!r}")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO analytics.ingestion_runs (
              source, mode, status, config
            ) VALUES (
              %s, %s, 'running', %s::jsonb
            )
            RETURNING run_id;
            """,
            (source, mode, Json(config)),
        )
        row = cur.fetchone()
    conn.commit()
    if row is None:
        raise RuntimeError("failed to create ingestion run (no row returned)")
    run_id = row[0]
    log.info("ingestion_run_started", extra={"run_id": str(run_id), "source": source, "mode": mode})
    return run_id


def finish_run(
    conn: Connection,
    run_id: uuid.UUID,
    *,
    status: str,
    records_fetched: int,
    records_published: int,
    records_failed: int,
    error_message: str | None = None,
) -> None:
    if status not in {"succeeded", "failed"}:
        raise ValueError(f"finish_run status must be succeeded or failed, got {status!r}")

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE analytics.ingestion_runs
            SET finished_at = now(),
                status = %s,
                records_fetched = %s,
                records_published = %s,
                records_failed = %s,
                error_message = %s
            WHERE run_id = %s;
            """,
            (status, records_fetched, records_published, records_failed, error_message, run_id),
        )
    conn.commit()
    log.info(
        "ingestion_run_finished",
        extra={
            "run_id": str(run_id),
            "status": status,
            "records_fetched": records_fetched,
            "records_published": records_published,
            "records_failed": records_failed,
        },
    )
