# Dispersion rows without positive scores

## Meaning

`dispersion_no_targets` warns when **`analytics.smoke_dispersion_exposures`** rows exist for the recent window but **none** carry **`dispersion_score > 0`**, while **`normalized.fire_detections`** shows activity.

This usually means **all candidate census centroids fell outside the modeled downwind Gaussian support** (upwind-only geometry sampling, zero proxy inputs, or over-aggressive distance caps).

## What to check

- Inspect **`analytics.v_dispersion_model_debug`** explanations (**`downwind_component`**, **`crosswind_component`**).
- Confirm **`DISPERSION_MAX_DISTANCE_KM`** and Gaussian **`DISPERSION_*_SIGMA_KM`** settings.
- Verify tract/county geometries intersect expectations for your bbox (`geo.counties` / `geo.tracts`).

## Mitigations

- Widen **`DISPERSION_MAX_DISTANCE_KM`** modestly (still bounded) or adjust sigma knobs — document changes in operator notes.
- Ensure **`DISPERSION_ALLOW_LARGE_RUN=1`** if intentionally scoring large tract corpora.
