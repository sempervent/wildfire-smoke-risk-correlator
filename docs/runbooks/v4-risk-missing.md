# V4 risk missing (`v4_risk_missing`)

## Meaning

Recent normalized fires exist but **`analytics.smoke_risk_scores`** has **no `model_version = 'v4'`** rows (often **`make compute-risk RISK_MODEL_VERSION=v4`** not run or grid/plume prerequisites missing).

## Confirm

```sql
SELECT COUNT(*) FROM analytics.smoke_risk_scores WHERE model_version = 'v4';
SELECT * FROM analytics.v_latest_risk_v4_explanations LIMIT 20;
```

## Mitigate

- Ensure **`make compute-risk RISK_MODEL_VERSION=v4`** after fixtures + normalization.
- For grid-informed v4, run **`make match-fire-weather`** and **`make compute-plume PLUME_MODEL_VERSION=wind_grid_v2`** first.
