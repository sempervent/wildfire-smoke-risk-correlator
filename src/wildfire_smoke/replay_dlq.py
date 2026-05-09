"""Replay DLQ / parse-error payloads back to raw topics (defaults to dry-run)."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaConsumer, KafkaProducer

from wildfire_smoke.db.connection import connect
from wildfire_smoke.settings import Settings, kafka_topics

log = logging.getLogger(__name__)


def _dry_run_default() -> bool:
    return os.environ.get("DRY_RUN", "1").strip().lower() in {"1", "true", "yes"}


def replay_from_postgres(
    *,
    settings: Settings,
    producer: KafkaProducer,
    dry_run: bool,
    limit: int,
    source_topic_filter: str | None,
    target_dataset_filter: str | None,
    status_filter: str,
) -> int:
    """Republish ``payload_sample`` as raw message value (best-effort; may be truncated)."""

    q = """
        SELECT parse_error_id, source_topic, payload_sample, error_context
        FROM analytics.parse_errors
        WHERE status = %s
          AND (%s IS NULL OR source_topic = %s)
          AND (%s IS NULL OR target_dataset = %s)
        ORDER BY last_seen_at DESC
        LIMIT %s
        """
    resolve = os.environ.get("DLQ_RESOLVE_ON_REPLAY", "0").strip().lower() in {"1", "true", "yes"}
    count = 0
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
        for parse_error_id, source_topic, payload_sample, err_ctx in rows:
            count += 1
            payload: Any = payload_sample
            if payload is None:
                log.warning("replay_dlq_missing_payload", extra={"parse_error_id": str(parse_error_id)})
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
                continue
            producer.send(source_topic, value=payload)
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
    if not dry_run:
        producer.flush()
    return count


def replay_from_kafka_dlq(
    *,
    settings: Settings,
    producer: KafkaProducer,
    dry_run: bool,
    limit: int,
    dlq_topic: str,
    target_dataset_filter: str | None,
) -> int:
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
                    if not src_topic or original is None:
                        log.warning("replay_dlq_kafka_bad_envelope", extra={"offset": msg.offset})
                        continue
                    processed += 1
                    if dry_run:
                        log.info(
                            "replay_dlq_kafka_dry_run",
                            extra={"source_topic": src_topic, "offset": msg.offset},
                        )
                    else:
                        producer.send(src_topic, value=original)
                    if processed >= limit:
                        stop = True
                        break
                if stop:
                    break
    finally:
        consumer.close()
    if not dry_run:
        producer.flush()
    return processed


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
    try:
        if args.source_mode == "kafka":
            dlq_topic = args.dlq_topic or topics.get("firms_dlq_topic", "firms.hotspots.dlq")
            n = replay_from_kafka_dlq(
                settings=settings,
                producer=producer,
                dry_run=dry,
                limit=args.limit,
                dlq_topic=dlq_topic,
                target_dataset_filter=target_ds,
            )
        else:
            st_filter = args.source_topic if args.source_topic else None
            n = replay_from_postgres(
                settings=settings,
                producer=producer,
                dry_run=dry,
                limit=args.limit,
                source_topic_filter=st_filter,
                target_dataset_filter=target_ds,
                status_filter=args.status,
            )
        log.info("replay_dlq_finished", extra={"replayed": n, "dry_run": dry, "mode": args.source_mode})
    finally:
        producer.close()


if __name__ == "__main__":
    main()
