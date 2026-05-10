# Data sources

| Source | Role | Notes |
|--------|------|-------|
| **NASA FIRMS** | Active fire hotspots (CSV by area) | Live requires **`FIRMS_MAP_KEY`**; bbox/day range configurable |
| **OpenAQ v3** | PM and related measurements | May require API key depending on access |
| **U.S. Census TIGER/Line** | County/tract boundaries | Downloaded by **`make db-bootstrap`**; multi-state optional |
| **NWS** | Wind observations; gridpoint forecast for grid weather | Bounded discovery via **`WIND_*`** / **`GRID_WEATHER_*`** |
| **Synthetic minimal GeoJSON** | CI/integration geography only | **`make db-bootstrap-minimal`** — not operational Census |

Producer **`analytics.ingestion_runs`** rows must not store secrets in **`config`** JSONB.
