# Integration pipeline incomplete (`integration_pipeline_incomplete`)

## Meaning

`analytics.v_integration_pipeline_counts` shows **recent fires** but one or more companion streams are empty: AQ, wind, grid cells, or Kafka offset snapshots.

## Confirm

```sql
SELECT * FROM analytics.v_integration_pipeline_counts;
```

## Mitigate

- Run **`make integration-regression`** (no secrets) or **`make replay-fixtures`** + **`make normalize`** + grid replay chain.
- Run **`make collect-lag`** so **`analytics.kafka_topic_offsets`** is populated.

## Related

- `docs/runbooks/v4-risk-missing.md`
- `docs/runbooks/fire-weather-match-missing.md`
