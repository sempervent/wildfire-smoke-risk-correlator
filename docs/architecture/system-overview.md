# System overview

This project is a **bounded vertical slice** that joins wildfire hotspot detections, air-quality observations, and optional meteorology into **PostGIS census units**, then publishes **engineering smoke-risk indices** and related **diagnostic views**.

## Major components

- **Kafka** buffers raw vendor payloads (FIRMS CSV rows, OpenAQ measurements, wind/grid streams).
- **Apache Spark** batch jobs normalize Kafka topics into **`normalized.*`** tables with spatial joins to **`geo.counties`** / **`geo.tracts`**.
- **Postgres + PostGIS** holds canonical geometries, normalized facts, analytical aggregates, calibration tables, and **SQL-first alerting** primitives.
- **Python batch jobs** (often executed via the Spark app container) compute plumes, optional dispersion proxies, and smoke-risk scores into **`analytics.*`**.
- **Grafana** (optional Compose profile) renders maps/tables over Postgres views — presentation only.

## Design stance

- **Inspectability over magic:** prefer SQL views and explicit jobs over opaque scoring services.
- **Honest uncertainty:** calibration labels, confidence labels, and dashboard banners emphasize **insufficient data** vs **weak evidence** vs **exploratory** alignment — not validated performance.

## Phase 13 additions

- **Fast CI** validates static artifacts (lint, unit tests, script syntax, dashboard JSON) without live APIs or Census downloads.
- **Optional integration** exercises Compose with **minimal synthetic census** fixtures when full TIGER bootstrap is undesirable.
- **Calibration exports** produce **timestamped CSV bundles** (and optional Parquet) for offline review, with **redacted connection metadata**.
