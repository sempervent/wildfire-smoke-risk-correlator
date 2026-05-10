# Dataflow

## Ingest

1. Producers publish to **raw Kafka topics** (`firms.hotspots.raw`, `openaq.measurements.raw`, `weather.wind.raw`, optional `weather.grid.raw`).
2. Malformed payloads may be copied to **source DLQs** and/or summarized into **`analytics.parse_errors`** depending on normalizer behavior.

## Normalize

3. Spark jobs consume raw topics and upsert **`normalized.*`** rows (fires, AQ measurements, wind observations, optional grid cells).
4. Spatial enrichment attaches **`county_geoid`** / **`tract_geoid`** when points fall inside **`geo.*`** geometries.

## Derive

5. **Plume** jobs write **`analytics.smoke_plume_exposures`** using corridor heuristics (`wind_v1`, `wind_grid_v2`).
6. Optional **dispersion** jobs write **`analytics.smoke_dispersion_exposures`** (`gaussian_v0` proxy).
7. **Risk** jobs write **`analytics.smoke_risk_scores`** (models **v1–v5** depending on configuration).
8. Optional **dispersion vs AQ** comparisons land in **`analytics.dispersion_aq_comparisons`** with **evidence labels**.

## Observe / evaluate

9. Operators may load **`analytics.risk_observations`** (fixtures or external proxies) and run **`make evaluate-risk`** to populate **`analytics.risk_model_evaluations`** — **engineering metrics only**.

## Present / alert

10. Grafana reads stable **`analytics.v_*`** views (maps, freshness SLIs, calibration summaries).
11. **`analytics.fn_alert_candidates`** emits candidate incidents; materialization/notifiers create durable **`analytics.alert_events`** when enabled.

## Phase 13 exports

12. **`make export-calibration`** dumps selected **`analytics.v_*`** calibration surfaces to **`artifacts/calibration/<UTC-stamp>/`** as immutable CSV snapshots (Parquet optional). Metadata records row counts and **non-secret** configuration hints.
