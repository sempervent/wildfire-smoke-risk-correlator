# Getting started

## Prerequisites

- **Docker** and **Docker Compose**
- **`uv`** (recommended) or Python **3.11+**
- **`bash`**, **`curl`**, **`unzip`** (for Census bootstrap download/extract)

## Configure environment

Copy **`.env.example`** to **`.env`**. For **fixture-only** runs you do not need API keys. For **live** FIRMS ingestion set **`FIRMS_MAP_KEY`**; OpenAQ may require **`OPENAQ_API_KEY`** depending on access.

Never commit **`.env`** or secrets.

## Start the stack

```bash
make up
make topics
```

## Bootstrap PostGIS

**Full Census load (default Tennessee)** — downloads TIGER shapefiles:

```bash
make db-bootstrap
```

**Minimal synthetic geometries** (CI/testing; not real boundaries):

```bash
# Often combined with MINIMAL_CENSUS_REPLACE_ALL=1 on disposable databases
make db-bootstrap-minimal
```

See **[Database bootstrap](operations/db-bootstrap.md)** for details.

## Validate

```bash
make deps
make test
```

End-to-end checks against running Compose:

```bash
make smoke-test
```

Fast checks without Docker (**CI-style**):

```bash
SMOKE_NO_COMPOSE=1 bash scripts/smoke_test.sh
```

## Common next steps

| Goal | Command / doc |
|------|----------------|
| Publish fixtures to Kafka (no live APIs) | **[No-secrets demo](user-guide/no-secrets-demo.md)** — `make replay-fixtures`, `FIRMS_DRY_RUN=1` producers |
| Normalize raw Kafka topics | `make normalize` |
| Compute smoke risk | `make compute-risk` |
| Optional Grafana | `make grafana-up` → **[Dashboards](user-guide/dashboards.md)** |

Wind direction uses the **meteorological convention** (*wind FROM*); transport modeling uses the **opposite bearing** — see `src/wildfire_smoke/wind.py`.
