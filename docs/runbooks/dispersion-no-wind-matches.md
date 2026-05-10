# Dispersion pipeline gaps (wind / matches)

## Meaning

`dispersion_no_wind_matches` indicates **recent fires with grid fire–weather matches** still lack **`analytics.smoke_dispersion_exposures`** rows inside **`ALERT_DISPERSION_NO_WIND_MATCHES_HOURS`** (parameter **`p_dispersion_no_wind_hours`** in SQL; env **`ALERT_DISPERSION_NO_WIND_MATCHES_HOURS`**, default **24**).

Related ops counters live in **`analytics.v_dispersion_operational_summary`**.

## What to check

- Run **`make compute-dispersion`** with **`DISPERSION_ENABLED=1`** after **`match-fire-weather`**.
- Confirm **`DISPERSION_USE_GRID_WEATHER`** / **`DISPERSION_FALLBACK_TO_STATION_WIND`** match your ingest path.
- Verify **`analytics.fire_weather_matches`** and **`normalized.weather_grid_cells`** are fresh.

## Mitigations

- Ensure **`make operational-cycle`** (or your scheduler) enables dispersion steps when **`DISPERSION_ENABLED=1`**.
- Inspect producer logs for skipped fires (**`skipped_no_wind`** metrics printed by the dispersion job).
