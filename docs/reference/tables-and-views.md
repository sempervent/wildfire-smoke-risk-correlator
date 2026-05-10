# Tables and views (major)

## Schemas

**`raw`**, **`normalized`**, **`geo`**, **`analytics`**.

## Normalized (examples)

| Relation | Purpose |
|----------|---------|
| **`normalized.fire_detections`** | FIRMS-derived detections with optional tract/county |
| **`normalized.air_quality_measurements`** | OpenAQ measurements |
| **`normalized.wind_observations`** | Wind obs (**wind FROM** convention) |
| **`normalized.weather_grid_cells`** | Gridded weather cells |

## Analytics (examples)

| Relation | Purpose |
|----------|---------|
| **`analytics.smoke_risk_scores`** | Risk scores per geography/model/window |
| **`analytics.smoke_plume_exposures`** | Corridor plume heuristic |
| **`analytics.smoke_dispersion_exposures`** | Gaussian proxy exposures |
| **`analytics.dispersion_aq_comparisons`** | Dispersion vs AQ lag scaffolding |
| **`analytics.parse_errors`** | Normalization quarantine |
| **`analytics.alert_events`** | Materialized incidents |

## Views (dashboard / ops)

| View | Purpose |
|------|---------|
| **`analytics.v_latest_smoke_risk_by_county`** / **`by_tract`** | Latest scores |
| **`analytics.v_latest_fire_detections`** / **`v_latest_air_quality_measurements`** | Point facts |
| **`analytics.v_latest_smoke_risk_*_geojson`** | Map-friendly GeoJSON |
| **`analytics.v_alert_candidates`** | Default-threshold alert union |
| **`analytics.v_integration_pipeline_counts`** | Integration regression snapshot |
| **`analytics.v_consumer_lag_latest`** | Lag visibility |
| **`analytics.v_dispersion_operational_summary`** | Dispersion ops summary |

DDL: **`sql/migrations/`**, **`sql/views/`**.
