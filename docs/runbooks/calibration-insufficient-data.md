# Calibration insufficient data

## Meaning

Most recent dispersion–AQ comparison rows carry `no_aq_data` or `insufficient_aq_data` labels. The pipeline cannot fairly judge alignment until monitors report in those lag windows.

## What to check

- `analytics.v_dispersion_aq_lag_summary.thin_evidence_rows` vs total rows.
- OpenAQ / ingest freshness and tract assignment coverage.
- `CALIBRATION_MIN_AQ_OBSERVATIONS` threshold realism for your deployment.

## Actions

- Do **not** interpret absence of PM2.5 as confirmation the model was correct.
- Improve ingest footprint or relax evaluation windows deliberately—not silently.
