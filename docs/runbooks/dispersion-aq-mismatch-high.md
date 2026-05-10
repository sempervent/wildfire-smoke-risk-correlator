# Dispersion vs AQ lag comparison divergence

## Meaning

`dispersion_aq_mismatch_high` fires when **`analytics.dispersion_aq_comparisons`** contains rows with **`comparison_score`** ≥ **`ALERT_DISPERSION_AQ_MISMATCH_MIN_SCORE`** (default **50**) and **`aq_observation_count`** ≥ **2** in the last **48 hours**.

This table is **evaluation scaffolding**: a simple divergence metric between normalized dispersion intensity and lagged PM summaries — **not** validated forecast skill.

## What to check

- Confirm AQ ingest is healthy (**`normalized.air_quality_measurements`**).
- Review lag buckets (**`0-3h`**, **`3-6h`**, …) — misaligned clocks or sparse AQ geographies inflate divergence.
- Inspect per-row **`explanation`** JSON for the embedded **`disp_norm`** / **`pm_norm`** snapshots.

## Mitigations

- Raise **`ALERT_DISPERSION_AQ_MISMATCH_MIN_SCORE`** if noisy on sparse AQ fixtures.
- Treat persistent divergence as a **data/integration** signal, not automatic evidence of model error.
