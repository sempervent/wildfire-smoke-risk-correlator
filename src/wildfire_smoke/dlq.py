"""Parse-error persistence, DLQ envelopes, and consumer offset evidence."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer
from psycopg import Connection

log = logging.getLogger(__name__)

_SENSITIVE_KEY_RE = re.compile(r"(api[_-]?key|password|token|secret|map[_-]?key|authorization)", re.I)

UPSERT_PARSE_ERROR_SQL = """
INSERT INTO analytics.parse_errors (
  source_topic, target_dataset, consumer_group, partition, offset_value,
  message_key, payload_hash, payload_sample, error_class, error_message, error_context
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s::jsonb)
ON CONFLICT (source_topic, target_dataset, consumer_group, payload_hash, error_class)
  WHERE status = 'open'
DO UPDATE SET
  occurrence_count = analytics.parse_errors.occurrence_count + 1,
  last_seen_at = now(),
  updated_at = now(),
  partition = COALESCE(EXCLUDED.partition, analytics.parse_errors.partition),
  offset_value = CASE
    WHEN EXCLUDED.offset_value IS NULL THEN analytics.parse_errors.offset_value
    ELSE GREATEST(
      COALESCE(analytics.parse_errors.offset_value, EXCLUDED.offset_value),
      EXCLUDED.offset_value
    )
  END,
  error_message = LEFT(EXCLUDED.error_message, 8000),
  payload_sample = COALESCE(EXCLUDED.payload_sample, analytics.parse_errors.payload_sample),
  error_context = analytics.parse_errors.error_context || EXCLUDED.error_context;
"""

UPSERT_OFFSET_SQL = """
INSERT INTO analytics.kafka_consumer_offsets (
  consumer_group, topic, partition, current_offset, last_processed_at,
  last_successful_offset, last_error_offset, metadata
) VALUES (%s, %s, %s, %s, now(), %s, %s, %s::jsonb)
ON CONFLICT (consumer_group, topic, partition)
DO UPDATE SET
  current_offset = GREATEST(analytics.kafka_consumer_offsets.current_offset, EXCLUDED.current_offset),
  last_processed_at = now(),
  last_successful_offset = CASE
    WHEN EXCLUDED.last_successful_offset IS NULL THEN analytics.kafka_consumer_offsets.last_successful_offset
    ELSE GREATEST(
      COALESCE(analytics.kafka_consumer_offsets.last_successful_offset, EXCLUDED.last_successful_offset),
      EXCLUDED.last_successful_offset
    )
  END,
  last_error_offset = CASE
    WHEN EXCLUDED.last_error_offset IS NULL THEN analytics.kafka_consumer_offsets.last_error_offset
    ELSE GREATEST(
      COALESCE(analytics.kafka_consumer_offsets.last_error_offset, EXCLUDED.last_error_offset),
      EXCLUDED.last_error_offset
    )
  END,
  metadata = analytics.kafka_consumer_offsets.metadata || EXCLUDED.metadata;
"""


def payload_hash(raw_bytes: bytes) -> str:
    """Stable SHA-256 hex digest of raw message bytes."""

    return hashlib.sha256(raw_bytes).hexdigest()


def sanitize_payload_sample(payload: Any, *, max_bytes: int = 8192) -> Any:
    """
    Produce a JSON-serializable sample safe for Postgres JSONB and logs.

    Truncates serialized size and redacts obvious sensitive keys (shallow dicts).
    """

    def _redact(obj: Any) -> Any:
        if isinstance(obj, dict):
            out: dict[str, Any] = {}
            for k, v in obj.items():
                ks = str(k)
                if _SENSITIVE_KEY_RE.search(ks):
                    out[ks] = "[REDACTED]"
                else:
                    out[ks] = _redact(v)
            return out
        if isinstance(obj, list):
            return [_redact(x) for x in obj[:200]]
        return obj

    cleaned = _redact(payload)
    blob = json.dumps(cleaned, default=str, separators=(",", ":")).encode("utf-8")
    if len(blob) <= max_bytes:
        return json.loads(blob.decode("utf-8"))
    preview = blob[:max_bytes].decode("utf-8", errors="replace")
    return {"_truncated": True, "preview": preview}


def classify_parse_exception(exc: BaseException) -> str:
    """Short classifier label for analytics.parse_errors.error_class."""

    if isinstance(exc, KeyError):
        return "KeyError"
    if isinstance(exc, json.JSONDecodeError):
        return "JSONDecodeError"
    if isinstance(exc, ValueError):
        return "ValueError"
    if isinstance(exc, TypeError):
        return "TypeError"
    return type(exc).__name__


def build_dlq_envelope(
    *,
    source_topic: str,
    target_dataset: str,
    consumer_group: str,
    original_key: str | None,
    original_partition: int | None,
    original_offset: int | None,
    payload_hash_hex: str,
    error_class: str,
    error_message: str,
    error_context: dict[str, Any],
    original_payload: Any,
    failed_at: datetime | None = None,
) -> dict[str, Any]:
    when = failed_at or datetime.now(timezone.utc)
    return {
        "source_topic": source_topic,
        "target_dataset": target_dataset,
        "consumer_group": consumer_group,
        "original_key": original_key,
        "original_partition": original_partition,
        "original_offset": original_offset,
        "payload_hash": payload_hash_hex,
        "error_class": error_class,
        "error_message": error_message[:8000],
        "error_context": error_context,
        "original_payload": original_payload,
        "failed_at": when.isoformat(),
    }


def publish_dlq(
    producer: KafkaProducer,
    *,
    specific_dlq_topic: str,
    normalization_errors_topic: str,
    envelope: dict[str, Any],
) -> None:
    """Publish the same logical failure to a source-specific DLQ and the shared normalization.errors topic."""

    producer.send(specific_dlq_topic, value=envelope)
    producer.send(normalization_errors_topic, value=envelope)


def record_parse_error(
    conn: Connection,
    *,
    source_topic: str,
    target_dataset: str,
    consumer_group: str,
    partition: int | None,
    offset_value: int | None,
    message_key: str | None,
    payload_hash_hex: str,
    payload_sample: Any,
    error_class: str,
    error_message: str,
    error_context: dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            UPSERT_PARSE_ERROR_SQL,
            (
                source_topic,
                target_dataset,
                consumer_group,
                partition,
                offset_value,
                message_key,
                payload_hash_hex,
                json.dumps(payload_sample, default=str),
                error_class,
                error_message[:8000],
                json.dumps(error_context, default=str),
            ),
        )


def upsert_consumer_offset(
    conn: Connection,
    *,
    consumer_group: str,
    topic: str,
    partition: int,
    current_offset: int,
    last_successful_offset: int | None,
    last_error_offset: int | None,
    metadata: dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            UPSERT_OFFSET_SQL,
            (
                consumer_group,
                topic,
                partition,
                current_offset,
                last_successful_offset,
                last_error_offset,
                json.dumps(metadata, default=str),
            ),
        )


def handle_normalize_failure(
    *,
    conn: Connection,
    producer: KafkaProducer,
    topics: dict[str, str],
    raw_bytes: bytes,
    exc: BaseException,
    source_topic: str,
    target_dataset: str,
    consumer_group: str,
    specific_dlq_topic: str,
    partition: int | None,
    offset_value: int | None,
    message_key: str | None,
    envelope_for_sample: Any | None = None,
) -> None:
    """Persist parse error + publish DLQ topics; never raises."""

    try:
        ph = payload_hash(raw_bytes)
        err_cls = classify_parse_exception(exc)
        msg = str(exc)
        sample_src: Any
        if envelope_for_sample is not None:
            sample_src = envelope_for_sample
        else:
            try:
                sample_src = json.loads(raw_bytes.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                sample_src = {"_raw_preview": raw_bytes[:512].decode("utf-8", errors="replace")}
        sample = sanitize_payload_sample(sample_src)

        ctx = {
            "normalizer": consumer_group,
            "partition": partition,
            "offset": offset_value,
        }
        record_parse_error(
            conn,
            source_topic=source_topic,
            target_dataset=target_dataset,
            consumer_group=consumer_group,
            partition=partition,
            offset_value=offset_value,
            message_key=message_key,
            payload_hash_hex=ph,
            payload_sample=sample,
            error_class=err_cls,
            error_message=msg,
            error_context=ctx,
        )

        orig_payload: Any
        try:
            orig_payload = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            orig_payload = sample_src

        orig_payload = sanitize_payload_sample(orig_payload, max_bytes=16384)

        dlq_env = build_dlq_envelope(
            source_topic=source_topic,
            target_dataset=target_dataset,
            consumer_group=consumer_group,
            original_key=message_key,
            original_partition=partition,
            original_offset=offset_value,
            payload_hash_hex=ph,
            error_class=err_cls,
            error_message=msg,
            error_context=ctx,
            original_payload=orig_payload,
        )
        publish_dlq(
            producer,
            specific_dlq_topic=specific_dlq_topic,
            normalization_errors_topic=topics["normalization_errors_topic"],
            envelope=dlq_env,
        )
    except Exception as inner:  # noqa: BLE001 — DLQ path must not raise
        log.exception("dlq_handle_failure_failed", extra={"error": str(inner)})
