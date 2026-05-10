# Make targets (reference)

Compose **`docker compose`** unless **`COMPOSE`** is overridden. **Secrets** required only where noted.

| Target | Purpose | Compose? | Secrets? |
|--------|---------|----------|----------|
| **`make deps`** | Install Python deps (`uv sync --extra dev`) | No | No |
| **`make up`** | Start Postgres, Redpanda, Console, Spark | Yes | No |
| **`make down`** | Stop stack | Yes | No |
| **`make topics`** | Create Kafka topics | Yes | No |
| **`make db-bootstrap`** | Download/load Census + migrations/views | Yes | No |
| **`make db-bootstrap-minimal`** | Synthetic minimal geo + migrations | Yes | No |
| **`make ingest-once`** | Run producers once | Yes | Optional (live) |
| **`make normalize`** | Spark normalize FIRMS/OpenAQ/wind | Yes | No |
| **`make compute-risk`** | Smoke risk job | Yes | No |
| **`make compute-plume`** | Plume exposures | Yes | No |
| **`make replay-fixtures`** | Fixture publish + pipeline steps | Yes | No |
| **`make demo`** | Scripted no-secrets demo | Yes | No |
| **`make smoke-test`** | Full Compose smoke | Yes | No |
| **`make integration-smoke-test`** | Lightweight wiring checks | Yes | No |
| **`make integration-regression`** | Long fixture regression | Yes | No |
| **`make quality-check`** | Structural DB checks | Yes | No |
| **`make grafana-up`** | Start Grafana profile | Yes | No |
| **`make alerts-check`** | Print alert candidates | Yes | No |
| **`make export-calibration`** | CSV calibration export | Yes | No |
| **`make db-doctor`** | Structural/drift checks | Yes | No |
| **`make repair-alert-function`** | Fix alert fn overload drift | Yes | No |
| **`make release-check`** | Maintainer gate | Partial | No |
| **`make docs`** / **`make docs-check`** | MkDocs build / `--strict` | No | No |
| **`make version`** | Print package + git metadata | No | No |

See **`Makefile`** for the full list and **`README.md`** for narrative quickstart.
