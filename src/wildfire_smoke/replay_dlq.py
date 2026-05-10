"""Replay DLQ / parse-error payloads back to raw topics (defaults to dry-run)."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from kafka import KafkaConsumer, KafkaProducer
from psycopg import Connection

from wildfire_smoke.db.connection import connect
from wildfire_smoke.settings import Settings, kafka_topics

log = logging.getLogger(__name__)


def _dry_run_default() -> bool:
    return os.environ.get("DRY_RUN", "1").strip().lower() in {"1", "true", "yes"}


def _bookkeeping_enabled() -> bool:
    return os.environ.get("DLQ_REPLAY_BOOKKEEPING", "1").strip().lower() in {"1", "true", "yes"}


def _create_replay_run(
    conn: Connection,
    *,
    source: str,
    dry_run: bool,
    limit: int,
    source_topic_filter: str | None,
    target_dataset_filter: str | None,
    status_filter: str,
    mode_extra: dict[str, Any],
) -> UUID:
    cfg = {
        "limit": limit,
        "source_topic": source_topic_filter,
        "target_dataset": target_dataset_filter,
        "status": status_filter,
        **mode_extra,
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO analytics.dlq_replay_runs (source, status, dry_run, config)
            VALUES (%s, 'running', %s, %s::jsonb)
            RETURNING dlq_replay_run_id
            """,
            (source, dry_run, json.dumps(cfg, default=str)),
        )
        rid = cur.fetchone()[0]
    conn.commit()
    return UUID(str(rid))


def _finish_replay_run(
    conn: Connection,
    run_id: UUID,
    *,
    status: str,
    scanned: int,
    replayed: int,
    resolved: int,
    error_message: str | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE analytics.dlq_replay_runs
            SET status = %s,
                finished_at = now(),
                records_scanned = %s,
                records_replayed = %s,
                records_resolved = %s,
                error_message = %s
            WHERE dlq_replay_run_id = %s
            """,
            (status, scanned, replayed, resolved, error_message, run_id),
        )
    conn.commit()


def _insert_item(
    conn: Connection,
    *,
    run_id: UUID,
    parse_error_id: UUID | None,
    source_topic: str | None,
    target_topic: str | None,
    payload_hash: str | None,
    status: str,
    error_message: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO analytics.dlq_replay_items (
              dlq_replay_run_id, parse_error_id, source_topic, target_topic,
              payload_hash, status, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (str(run_id), str(parse_error_id) if parse_error_id else None, source_topic, target_topic, payload_hash, status, error_message),
        )
    conn.commit()


def replay_from_postgres(
    *,
    settings: Settings,
    producer: KafkaProducer,
    dry_run: bool,
    limit: int,
    source_topic_filter: str | None,
    target_dataset_filter: str | None,
    status_filter: str,
    bookkeeping_conn: Connection | None,
    run_id: UUID | None,
) -> tuple[int, int, int]:
    """Returns (scanned, replayed, resolved)."""

    q = """
        SELECT parse_error_id, source_topic, payload_sample, error_context, payload_hash
        FROM analytics.parse_errors
        WHERE status = %s::text
          AND (%s::text IS NULL OR source_topic = %s::text)
          AND (%s::text IS NULL OR target_dataset = %s::text)
        ORDER BY last_seen_at DESC
        LIMIT %s
        """
    resolve = os.environ.get("DLQ_RESOLVE_ON_REPLAY", "0").strip().lower() in {"1", "true", "yes"}
    scanned = 0
    replayed = 0
    resolved = 0

    with connect(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                q,
                (
                    status_filter,
                    source_topic_filter,
                    source_topic_filter,
                    target_dataset_filter,
                    target_dataset_filter,
                    limit,
                ),
            )
            rows = cur.fetchall()

        for parse_error_id, source_topic, payload_sample, err_ctx, payload_hash in rows:
            scanned += 1
            ph = str(payload_hash) if payload_hash else None
            payload: Any = payload_sample
            if payload is None:
                log.warning("replay_dlq_missing_payload", extra={"parse_error_id": str(parse_error_id)})
                if bookkeeping_conn and run_id:
                    _insert_item(
                        bookkeeping_conn,
                        run_id=run_id,
                        parse_error_id=UUID(str(parse_error_id)),
                        source_topic=source_topic,
                        target_topic=source_topic,
                        payload_hash=ph,
                        status="skipped",
                        error_message="missing payload_sample",
                    )
                continue

            if dry_run:
                log.info(
                    "replay_dlq_dry_run",
                    extra={
                        "parse_error_id": str(parse_error_id),
                        "source_topic": source_topic,
                        "payload_preview": str(payload)[:500],
                    },
                )
                if bookkeeping_conn and run_id:
                    _insert_item(
                        bookkeeping_conn,
                        run_id=run_id,
                        parse_error_id=UUID(str(parse_error_id)),
                        source_topic=source_topic,
                        target_topic=source_topic,
                        payload_hash=ph,
                        status="skipped",
                        error_message="dry_run",
                    )
                continue

            try:
                producer.send(source_topic, value=payload)
                replayed += 1
                if bookkeeping_conn and run_id:
                    _insert_item(
                        bookkeeping_conn,
                        run_id=run_id,
                        parse_error_id=UUID(str(parse_error_id)),
                        source_topic=source_topic,
                        target_topic=source_topic,
                        payload_hash=ph,
                        status="replayed",
                    )
            except Exception as exc:  # noqa: BLE001
                log.warning("replay_dlq_publish_failed", extra={"error": str(exc)})
                if bookkeeping_conn and run_id:
                    _insert_item(
                        bookkeeping_conn,
                        run_id=run_id,
                        parse_error_id=UUID(str(parse_error_id)),
                        source_topic=source_topic,
                        target_topic=source_topic,
                        payload_hash=ph,
                        status="failed",
                        error_message=str(exc)[:800],
                    )
                continue

            if resolve:
                with conn.cursor() as cur:
                    meta = dict(err_ctx or {})
                    meta["replayed_at"] = datetime.now(timezone.utc).isoformat()
                    meta["replay_mode"] = "postgres_parse_errors"
                    cur.execute(
                        """
                        UPDATE analytics.parse_errors
                        SET status = 'resolved',
                            updated_at = now(),
                            error_context = %s::jsonb
                        WHERE parse_error_id = %s
                        """,
                        (json.dumps(meta), parse_error_id),
                    )
                conn.commit()
                resolved += 1

    if not dry_run:
        producer.flush()
    return scanned, replayed, resolved


def replay_from_kafka_dlq(
    *,
    settings: Settings,
    producer: KafkaProducer,
    dry_run: bool,
    limit: int,
    dlq_topic: str,
    target_dataset_filter: str | None,
    bookkeeping_conn: Connection | None,
    run_id: UUID | None,
) -> tuple[int, int, int]:
    bootstrap = settings.kafka_bootstrap_servers
    consumer = KafkaConsumer(
        dlq_topic,
        bootstrap_servers=[s.strip() for s in bootstrap.split(",") if s.strip()],
        consumer_group=os.environ.get("DLQ_REPLAY_CONSUMER_GROUP", "dlq-replay-tool"),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    processed = 0
    scanned = 0
    max_scan = max(limit * 200, 500)
    stop = False
    replayed = 0
    try:
        while processed < limit and not stop:
            packs = consumer.poll(timeout_ms=2000)
            if not packs:
                break
            for _tp, records in packs.items():
                for msg in records:
                    scanned += 1
                    if scanned > max_scan:
                        stop = True
                        break
                    env = msg.value
                    if not isinstance(env, dict):
                        continue
                    if target_dataset_filter and env.get("target_dataset") != target_dataset_filter:
                        continue
                    src_topic = env.get("source_topic")
                    original = env.get("original_payload")
                    ph = env.get("payload_hash")
                    if not src_topic or original is None:
                        log.warning("replay_dlq_kafka_bad_envelope", extra={"offset": msg.offset})
                        continue
                    processed += 1
                    if dry_run:
                        log.info(
                            "replay_dlq_kafka_dry_run",
                            extra={"source_topic": src_topic, "offset": msg.offset},
                        )
                        if bookkeeping_conn and run_id:
                            _insert_item(
                                bookkeeping_conn,
                                run_id=run_id,
                                parse_error_id=None,
                                source_topic=src_topic,
                                target_topic=src_topic,
                                payload_hash=str(ph) if ph else None,
                                status="skipped",
                                error_message="dry_run",
                            )
                        continue
                    try:
                        producer.send(src_topic, value=original)
                        replayed += 1
                        if bookkeeping_conn and run_id:
                            _insert_item(
                                bookkeeping_conn,
                                run_id=run_id,
                                parse_error_id=None,
                                source_topic=src_topic,
                                target_topic=src_topic,
                                payload_hash=str(ph) if ph else None,
                                status="replayed",
                            )
                    except Exception as exc:  # noqa: BLE001
                        if bookkeeping_conn and run_id:
                            _insert_item(
                                bookkeeping_conn,
                                run_id=run_id,
                                parse_error_id=None,
                                source_topic=src_topic,
                                target_topic=src_topic,
                                payload_hash=str(ph) if ph else None,
                                status="failed",
                                error_message=str(exc)[:800],
                            )
                    if processed >= limit:
                        stop = True
                        break
                if stop:
                    break
    finally:
        consumer.close()
    if not dry_run:
        producer.flush()
    return scanned, replayed, 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Replay DLQ / parse errors to raw topics")
    p.add_argument(
        "--source-mode",
        choices=("postgres", "kafka"),
        default=os.environ.get("DLQ_SOURCE_MODE", "postgres"),
    )
    p.add_argument("--limit", type=int, default=int(os.environ.get("DLQ_REPLAY_LIMIT", "50")))
    p.add_argument("--dlq-topic", default=os.environ.get("DLQ_TOPIC", ""))
    p.add_argument("--source-topic", default=os.environ.get("SOURCE_TOPIC", "") or None)
    p.add_argument("--status", default=os.environ.get("STATUS", "open"))
    p.add_argument("--no-dry-run", action="store_true", help="Publish messages / resolve rows (also set DRY_RUN=0)")
    return p


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    args = build_parser().parse_args()
    dry = not args.no_dry_run and _dry_run_default()
    settings = Settings.from_env()
    topics = kafka_topics()
    target_ds = os.environ.get("TARGET_DATASET") or None
    producer = KafkaProducer(
        bootstrap_servers=[s.strip() for s in settings.kafka_bootstrap_servers.split(",") if s.strip()],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    bk = _bookkeeping_enabled()
    run_id: UUID | None = None

    def _run_body(bk_conn: Connection | None) -> None:
        try:
            if args.source_mode == "kafka":
                dlq_topic = args.dlq_topic or topics.get("firms_dlq_topic", "firms.hotspots.dlq")
                scanned, replayed, resolved = replay_from_kafka_dlq(
                    settings=settings,
                    producer=producer,
                    dry_run=dry,
                    limit=args.limit,
                    dlq_topic=dlq_topic,
                    target_dataset_filter=target_ds,
                    bookkeeping_conn=bk_conn,
                    run_id=run_id,
                )
            else:
                st_filter = args.source_topic if args.source_topic else None
                scanned, replayed, resolved = replay_from_postgres(
                    settings=settings,
                    producer=producer,
                    dry_run=dry,
                    limit=args.limit,
                    source_topic_filter=st_filter,
                    target_dataset_filter=target_ds,
                    status_filter=args.status,
                    bookkeeping_conn=bk_conn,
                    run_id=run_id,
                )

            if bk_conn and run_id:
                _finish_replay_run(
                    bk_conn,
                    run_id,
                    status="succeeded",
                    scanned=scanned,
                    replayed=replayed,
                    resolved=resolved,
                    error_message=None,
                )
            log.info(
                "replay_dlq_finished",
                extra={
                    "scanned": scanned,
                    "replayed": replayed,
                    "resolved": resolved,
                    "dry_run": dry,
                    "mode": args.source_mode,
                    "run_id": str(run_id) if run_id else None,
                },
            )
        except Exception as exc:
            if bk_conn and run_id:
                _finish_replay_run(
                    bk_conn,
                    run_id,
                    status="failed",
                    scanned=0,
                    replayed=0,
                    resolved=0,
                    error_message=str(exc)[:800],
                )
            raise

    try:
        if bk:
            with connect(settings) as bk_conn:
                run_id = _create_replay_run(
                    bk_conn,
                    source=args.source_mode,
                    dry_run=dry,
                    limit=args.limit,
                    source_topic_filter=args.source_topic or os.environ.get("SOURCE_TOPIC"),
                    target_dataset_filter=target_ds,
                    status_filter=args.status,
                    mode_extra={"dlq_topic": args.dlq_topic or None},
                )
                _run_body(bk_conn)
        else:
            _run_body(None)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
