# Live bounded ingest

Live pulls require network access and, for FIRMS, **`FIRMS_MAP_KEY`**. OpenAQ may require **`OPENAQ_API_KEY`** depending on API behavior.

## One cycle

```bash
make ingest-live-once
```

Or lower-level:

```bash
make ingest-once
```

after exporting keys in **`.env`**.

## Bounding boxes

- **`FIRMS_BBOX`**, **`OPENAQ_BBOX`** — keep spans modest.
- **`LIVE_INGEST_BBOX`** / **`LIVE_INGEST_MAX_SPAN_DEG`** — **`make ingest-live-once`** rejects huge areas unless **`LIVE_INGEST_ALLOW_LARGE_BBOX=1`**.
- **Wind**: prefer **`WIND_STATION_IDS`** or bounded **`WIND_BBOX`** with **`WIND_STATION_DISCOVERY_LIMIT`** for station discovery.

## Grid weather (optional)

When **`GRID_WEATHER_*`** is configured, grid ingest stays bounded via **`GRID_WEATHER_BBOX`**, **`GRID_WEATHER_POINTS`**, **`GRID_WEATHER_MAX_POINTS`**, and **`GRID_WEATHER_REFUSE_LARGE_BBOX`** (default on).

## Ingestion bookkeeping

Producers record rows in **`analytics.ingestion_runs`** with a **`config`** JSONB that **must not** contain secrets (operators audit producers for safe fields only).
