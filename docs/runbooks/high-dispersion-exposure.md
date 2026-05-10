# High Gaussian dispersion proxy score

## Meaning

`high_dispersion_exposure` fires when **recent** `analytics.smoke_dispersion_exposures` rows show a **maximum `dispersion_score`** above `ALERT_HIGH_DISPERSION_EXPOSURE_MIN_SCORE` (default **70**).

`gaussian_v0` is an **engineering proxy** (not HYSPLIT, not regulatory, not a health model). Use it for **ops visibility and correlation experiments** only.

## What to check

- Confirm **`make compute-dispersion`** / **`DISPERSION_ENABLED=1`** runs after fires + wind/grid wind are present.
- Inspect **`analytics.v_dispersion_operational_summary`** and **`analytics.v_dispersion_model_debug`** for odd winds, fallback usage, or centroid geography mismatches.
- Compare with **corridor plumes** (`wind_v1` / `wind_grid_v2`) — divergence is expected; neither is a dispersion solver.

## Mitigations

- Temporarily raise **`ALERT_HIGH_DISPERSION_EXPOSURE_MIN_SCORE`** if thresholds are too noisy on fixtures.
- Review **`DISPERSION_MAX_DISTANCE_KM`** / **`DISPERSION_MAX_TARGET_GEOGRAPHIES`** if runtime cost or geographic fan-out is undesired.
