# Fire–weather match missing (`fire_weather_match_missing`)

## Meaning

Both **recent grid cells** and **recent fires** exist, but **`analytics.fire_weather_matches`** is empty — **`wind_grid_v2`** plume and **risk v4** grid signals will be weak.

## Confirm

```sql
SELECT * FROM analytics.v_fire_weather_unmatched LIMIT 50;
SELECT * FROM analytics.v_integration_pipeline_counts;
```

## Mitigate

- Run **`make match-fire-weather`** after **`make normalize-grid-weather`**.
- Widen **`FIRE_WEATHER_MATCH_RADIUS_KM`** / **`FIRE_WEATHER_MATCH_MAX_TIME_DELTA_HOURS`** cautiously.
- Use **`FIXTURE_TIME_MODE=relative`** + **`USE_ALIGNED_FIXTURES=1`** for deterministic local demos.
