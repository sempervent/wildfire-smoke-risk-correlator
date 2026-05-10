# Wildfire Smoke Risk Correlator

A **local-first research and engineering** stack that correlates **NASA FIRMS** wildfire hotspots and **OpenAQ** air-quality measurements with **U.S. Census** geometries using **Kafka**, **Apache Spark**, and **PostGIS**. It publishes **smoke-risk indices**, optional **plume and dispersion-style proxies**, **calibration scaffolding**, and **SQL-first alerting**.

!!! warning "Non-claims"

    This software is **not** a public-health advisory system, **not** regulatory dispersion modeling, and **not** operational emergency guidance. Outputs are **engineering correlations** for experimentation and operator inspection.

## Who this documentation is for

- **Researchers and engineers** running reproducible demos on open data.
- **Operators** inspecting Postgres views, Grafana, and alerts.

Start with **[Getting started](getting-started.md)** or the **[no-secrets demo](user-guide/no-secrets-demo.md)**.

## Quick links

| I want to… | Go to |
|------------|-------|
| Run fixtures without API keys | [No-secrets demo](user-guide/no-secrets-demo.md) |
| Bring up Docker Compose and bootstrap DB | [Getting started](getting-started.md), [Database bootstrap](operations/db-bootstrap.md) |
| Ingest live data (bounded) | [Live ingest](user-guide/live-ingest.md) |
| Inspect outputs / Grafana | [Dashboards](user-guide/dashboards.md) |
| Understand risk bands and models | [Risk models](reference/risk-models.md) |
| Fix migration drift | [Troubleshooting](operations/troubleshooting.md), [Database doctor](operations/db-doctor.md) |

Repository: [GitHub](https://github.com/sempervent/wildfire-smoke-risk-correlator).
