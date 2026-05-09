# Replay failures recent (`replay_failures_recent`)

## Meaning

Recent DLQ / parse-error replay tooling recorded items with **`failed`** status in `analytics.dlq_replay_items`, or finished runs with **`failed`** status in `analytics.dlq_replay_runs`. This surfaces operator replay regressions (Kafka publish errors, missing payloads, bad envelopes).

## Confirm

1. Inspect `analytics.v_dlq_replay_runs` and `analytics.v_dlq_replay_items_recent`.
2. Read `error_message` / `error_message_preview` on failed rows.
3. Verify broker availability (`KAFKA_BOOTSTRAP_SERVERS`) and topic ACLs if applicable.

## Mitigate

- Fix serialization or schema mismatches before re-running with `DRY_RUN=0`.
- For Postgres replay, ensure `payload_sample` is present and valid JSON (note truncation caveats in README).
- Disable bookkeeping temporarily with `DLQ_REPLAY_BOOKKEEPING=0` only for debugging — prefer fixing root cause and retaining audit rows.

## Related

- `scripts/replay_dlq.sh` — defaults to **`DRY_RUN=1`**.
- `docs/runbooks/dlq-depth-high.md`
