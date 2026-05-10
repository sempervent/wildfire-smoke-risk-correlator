<div class="wf-hero md-typeset" markdown="1">

## Wildfire Smoke Risk Correlator

<p class="wf-lead">A <strong>local-first research and engineering</strong> stack that correlates <strong>NASA FIRMS</strong> wildfire hotspots and <strong>OpenAQ</strong> air-quality measurements with <strong>U.S. Census</strong> geometries using <strong>Kafka</strong>, <strong>Apache Spark</strong>, and <strong>PostGIS</strong>. It publishes smoke-risk indices, optional plume and dispersion-style proxies, calibration scaffolding, and SQL-first alerting.</p>

!!! warning "Non-claims"

    This software is **not** a public-health advisory system, **not** regulatory dispersion modeling, and **not** operational emergency guidance. Outputs are **engineering correlations** for experimentation and operator inspection.

</div>

<div class="wf-card-grid">
  <a class="wf-card" href="getting-started/">
    <p class="wf-card-title">Quickstart</p>
    <p class="wf-card-desc">Compose, topics, database bootstrap, and first steps.</p>
  </a>
  <a class="wf-card" href="architecture/overview/">
    <p class="wf-card-title">Architecture</p>
    <p class="wf-card-desc">How sources, Kafka, Spark, and PostGIS fit together.</p>
  </a>
  <a class="wf-card" href="operations/db-bootstrap/">
    <p class="wf-card-title">Operations</p>
    <p class="wf-card-desc">Bootstrap, Kafka/DLQ, dashboards, calibration exports.</p>
  </a>
  <a class="wf-card" href="user-guide/calibration/">
    <p class="wf-card-title">Calibration</p>
    <p class="wf-card-desc">Evaluation scaffolding — honest metrics, not validation claims.</p>
  </a>
  <a class="wf-card" href="release/v1.1.0/">
    <p class="wf-card-title">Release notes</p>
    <p class="wf-card-desc">v1.1.0 documentation site, CI gates, and validation commands.</p>
  </a>
</div>

## Who this documentation is for

- **Researchers and engineers** running reproducible demos on open data.
- **Operators** inspecting Postgres views, Grafana, and alerts.

Start with **[Quickstart](getting-started.md)** or the **[No-secrets demo](user-guide/no-secrets-demo.md)**.

## Quick links

| I want to… | Go to |
|------------|-------|
| Run fixtures without API keys | [No-secrets demo](user-guide/no-secrets-demo.md) |
| Bring up Docker Compose and bootstrap DB | [Quickstart](getting-started.md), [Database bootstrap](operations/db-bootstrap.md) |
| Ingest live data (bounded) | [Live ingest](user-guide/live-ingest.md) |
| Inspect outputs / Grafana | [Dashboards](user-guide/dashboards.md) |
| Understand risk bands and models | [Risk models](reference/risk-models.md) |
| Fix migration drift | [Troubleshooting](operations/troubleshooting.md), [Database doctor](operations/db-doctor.md) |

Repository: [GitHub](https://github.com/sempervent/wildfire-smoke-risk-correlator).
