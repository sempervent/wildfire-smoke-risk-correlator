# Model overprediction possible (calibration)

## Meaning

The heuristic dispersion-vs-AQ comparison flagged geographies where the Gaussian proxy score is high relative to lag-window PM2.5 averages. This is **not** proof of model error—meteorology, chemistry, monitor gaps, and proxy limits dominate.

## What to check

- `analytics.v_model_overprediction_candidates` for recent rows and lag buckets.
- `analytics.v_dispersion_aq_evidence_summary` for how often `possible_overprediction` appears vs thin-data labels.
- Whether AQ counts in those windows are below `CALIBRATION_MIN_AQ_OBSERVATIONS`.

## Actions

- Treat as **informational** unless triangulated with independent evidence.
- Increase AQ coverage or widen lag windows only after documenting trade-offs.
