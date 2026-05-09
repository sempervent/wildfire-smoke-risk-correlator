# Runbook: high corridor plume exposure

## Meaning

`high_plume_exposure` candidates come from **`analytics.smoke_plume_exposures`** (model **`wind_v1`**): a geography reached the configured minimum **corridor exposure score** inside the alert lookback window.

This score is an **engineering heuristic** (wind direction cone + distance decay). It is **not** dispersion modeling and **not** a health advisory.

## Confirm

```sql
SELECT *
FROM analytics.v_top_plume_exposures
ORDER BY exposure_rank
LIMIT 50;
```

Compare against **`analytics.v_smoke_transport_summary`** for coarse freshness.

## Mitigate

- Verify **`normalized.wind_observations`** has plausible timestamps and wind-from directions.
- Re-run **`make compute-plume`** after fixing upstream wind or fire normalization issues.
- Tune **`PLUME_*`** and **`WIND_MATCH_*`** env vars only when you understand the approximation limits.

## Escalate

If scores remain extreme with verified inputs, treat as a **model tuning** problem—not evidence of observed air quality harm.
