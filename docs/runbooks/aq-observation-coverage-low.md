# AQ observation coverage low

## Meaning

Few distinct tract geographies produced AQ measurements in the last 24h while fires were present. Calibration and lag comparisons will be skewed or empty.

## What to check

- `normalized.air_quality_measurements` recency and `tract_geoid` population.
- Bounding boxes vs operational areas.

## Actions

- Expand ingest bounds cautiously or pair with supplemental monitors if available.
- Expect calibration alerts to remain informational until coverage improves.
