# No recent grid weather (`no_recent_grid_weather`)

## Meaning

No **`weather_grid_cells`** rows with **`valid_time`** in the last **24 hours**, while historical rows exist. Differs from **`grid_weather_stale`** (MAX age) — this flags a complete recent gap.

## Confirm

- Query **`analytics.v_latest_weather_grid_cells`**.
- Inspect **`weather.grid.raw`** depth / **`make collect-lag`**.

## Mitigate

- Replay fixtures: **`GRID_WEATHER_DRY_RUN=1 make replay-grid-weather-fixtures`**.
- Re-run Spark normalizer: **`make normalize-grid-weather`**.

## Related

- `docs/runbooks/grid-weather-stale.md`
