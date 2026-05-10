# Model underprediction possible (calibration)

## Meaning

Lag-window PM2.5 averages are elevated while the Gaussian dispersion proxy stayed low for the same geography. This pattern can reflect transport outside the centroid proxy, missing fires, mixing height, or sparse monitors—not a validated “miss.”

## What to check

- `analytics.v_model_underprediction_candidates`.
- Fire detection density and dispersion configuration caps (`DISPERSION_MAX_DISTANCE_KM`, lookback).
- Local climatology / unrelated sources affecting PM2.5.

## Actions

- Keep severity low; use as a prompt for manual review, not paging.
