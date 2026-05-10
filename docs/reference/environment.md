# Environment variables (reference)

Copy **`.env.example`** to **`.env`**. This table highlights commonly used variables — not exhaustive.

| Variable | Default (typical) | Purpose | Sensitive? |
|----------|-------------------|---------|------------|
| **`POSTGRES_*`** | `smoke` / `5432` | DB connection | Password yes |
| **`KAFKA_BOOTSTRAP_SERVERS`** | `localhost:19092` | Kafka clients | No |
| **`FIRMS_MAP_KEY`** | unset | Live FIRMS API | **Yes** |
| **`OPENAQ_API_KEY`** | unset | OpenAQ if required | Often yes |
| **`FIRMS_DRY_RUN`** | `0` | Fixture-only FIRMS | No |
| **`OPENAQ_DRY_RUN`** | `0` | Fixture-only OpenAQ | No |
| **`WIND_DRY_RUN`** | `0` | Fixture wind | No |
| **`SMOKE_RISK_MODEL_VERSION`** | `v2` | Risk model | No |
| **`DISPERSION_ENABLED`** | `0` | Dispersion jobs | No |
| **`ALERTS_WARN_ONLY`** | varies | Soften alert exit | No |
| **`FIXTURE_TIME_MODE`** | `static` | Fixture timestamp mode | No |
| **`CALIBRATION_EXPORT_DIR`** | `artifacts/calibration` | Export path | No |
| **`DB_DOCTOR_WARN_ONLY`** | `0` | Non-failing doctor | No |
| **`COMPOSE_INTEGRATION`** | `0` | Extra release-check gates | No |

Full commentary lives in **`.env.example`**.
