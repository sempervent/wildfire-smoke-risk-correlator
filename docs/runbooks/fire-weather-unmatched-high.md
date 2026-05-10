# Fire–weather unmatched high (`fire_weather_unmatched_high`)

## Meaning

Many **recent fire detections** (24h) lack a row in **`analytics.fire_weather_matches`**. Downstream **`wind_grid_v2`** plume and **risk v4** quality suffer.

## Confirm

- `SELECT * FROM analytics.v_fire_weather_match_summary;`
- Review **`FIRE_WEATHER_MATCH_RADIUS_KM`** and **`FIRE_WEATHER_MATCH_MAX_TIME_DELTA_HOURS`**.

## Mitigate

- Ensure grid cells cover fire locations and times: **`make normalize-grid-weather`** then **`make match-fire-weather`**.
- Widen match radius/time window cautiously (bounded ops only).

## Related

- `docs/runbooks/no-recent-grid-weather.md`
