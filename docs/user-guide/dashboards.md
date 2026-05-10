# Grafana dashboards

## Start Grafana

```bash
make grafana-up
```

Uses Compose **`--profile grafana`**. Default Console/UI ports are defined in **`docker-compose.yml`** (Grafana often **`localhost:3001`** if mapped).

## Provisioned dashboard

Checked-in JSON: **`docker/grafana/dashboards/smoke-risk.json`**.

Panels include:

- County / tract risk maps (centroid markers)
- Fire and AQ point layers
- Operational summaries (freshness, lag, DLQ proxies where applicable)
- Calibration and evaluation tables when migrations are applied

Datasource UID **`smoke_pg`** must match provisioning (**`docker/grafana/provisioning/`**).

!!! note "Presentation only"

    GeoJSON presentation views are for maps — canonical geometries remain in **`geo.*`** and **`normalized.*`** point rows.
