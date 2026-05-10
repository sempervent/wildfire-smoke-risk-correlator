# Data model (high level)

Schemas are created by **`docker/postgres/initdb`** and extended by **`sql/migrations/`** + **`sql/views/`**. This page summarizes **major** relations — not every column.

## Schemas

| Schema | Purpose |
|--------|---------|
| **`raw`** | Vendor payloads (JSONB), gridded weather staging. |
| **`normalized`** | Fire detections, AQ measurements, wind observations, weather grid cells. |
| **`geo`** | Census county and tract geometries (`geoid`, `geom`). |
| **`analytics`** | Risk scores, plume/dispersion exposures, calibration, alerts, lag/DLQ bookkeeping. |

## Core normalized tables

| Table | Purpose |
|-------|---------|
| **`normalized.fire_detections`** | FIRMS-derived points with optional **`county_geoid`** / **`tract_geoid`**. |
| **`normalized.air_quality_measurements`** | OpenAQ-derived PM and related measurements. |
| **`normalized.wind_observations`** | Wind stations / observations (`wind FROM` convention). |
| **`normalized.weather_grid_cells`** | Gridded weather cells when grid ingest is enabled. |

## Analytics highlights

| Table | Purpose |
|-------|---------|
| **`analytics.smoke_risk_scores`** | County/tract risk scores and bands per model version. |
| **`analytics.smoke_plume_exposures`** | Corridor plume heuristic exposures. |
| **`analytics.smoke_dispersion_exposures`** | Gaussian proxy dispersion-style exposures (`gaussian_v0`). |
| **`analytics.dispersion_aq_comparisons`** | Lag-window AQ vs dispersion summaries + evidence labels. |
| **`analytics.risk_observations`** | Optional labeled observations for evaluation. |
| **`analytics.risk_model_evaluations`** | Batch evaluation metrics vs observations. |
| **`analytics.parse_errors`** | Quarantined normalization failures. |
| **`analytics.alert_events`** | Materialized alert incidents (when materialization is run). |

## Views

Stable **`analytics.v_*`** views power Grafana and ad hoc queries. See **[Tables and views](../reference/tables-and-views.md)** for a longer list.

DDL source of truth: **`sql/migrations/`**, **`sql/views/`**, **`docker/postgres/initdb/`** (fresh volumes).
