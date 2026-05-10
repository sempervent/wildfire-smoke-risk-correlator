# Grid weather stale (`grid_weather_stale`)

## Meaning

`normalized.weather_grid_cells` has not refreshed within **`ALERT_GRID_WEATHER_STALE_HOURS`** (default **6**). This alert only evaluates once at least one grid row exists (empty warehouse stays quiet).

## Confirm

1. `SELECT MAX(valid_time), COUNT(*) FROM normalized.weather_grid_cells;`
2. Check **`analytics.v_grid_weather_operational_summary`**.
3. Verify **`weather.grid.raw`** / **`spark-normalize-grid-weather`** consumer progress.

## Mitigate

- Run **`make replay-grid-weather-fixtures`** (fixture) or fix live NWS adapter (**`NWS_USER_AGENT`**, bbox bounds).
- Run **`make normalize-grid-weather`** after new Kafka payloads.

## Related

- `docs/runbooks/no-recent-grid-weather.md`
