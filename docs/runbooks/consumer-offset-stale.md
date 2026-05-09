# Runbook: consumer offset evidence stale or missing (`consumer_offset_stale`)

## Meaning

Either **no** rows exist in **`analytics.kafka_consumer_offsets`** for consumer groups matching **`spark-normalize%`**, or the worst **`last_processed_at`** age exceeds **`ALERT_CONSUMER_OFFSET_STALE_HOURS`**.

This table is **application evidence** written by Spark normalizers—it is **not** the same as broker-committed consumer offsets unless explicitly unified later.

## Confirm

```sql
SELECT * FROM analytics.v_consumer_offset_state;
SELECT * FROM analytics.kafka_consumer_offsets WHERE consumer_group LIKE 'spark-normalize%';
```

## Mitigate

- Run **`make normalize`** / **`make normalize-wind`** and confirm new rows or refreshed **`last_processed_at`**.
- If jobs succeed but evidence is missing, verify executor env (**`PSYCOPG_CONNINFO`**, **`KAFKA_BOOTSTRAP_SERVERS`**) in **`scripts/run_normalize*.sh`**.

## Escalate

Broker lag without Postgres updates may indicate the batch job is not reaching the offset upsert path—capture partition summary logs from **`normalize_*`** jobs.
