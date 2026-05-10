# No-secrets demo (fixtures)

Run the stack **without** FIRMS or OpenAQ API keys by publishing **checked-in fixtures** to Kafka.

## One-shot producers

```bash
export FIRMS_DRY_RUN=1
export OPENAQ_DRY_RUN=1
export WIND_DRY_RUN=1
make ingest-once
```

Optional paths are documented in **`.env.example`** (`FIRMS_FIXTURE_CSV`, `OPENAQ_FIXTURE_JSONL`, `WIND_FIXTURE_JSONL`).

## Replay pipeline

```bash
make replay-fixtures
```

This publishes FIRMS/OpenAQ fixtures and can chain normalization and risk steps depending on repo defaults. Disable downstream steps with **`REPLAY_RUN_NORMALIZE=0`** / **`REPLAY_RUN_COMPUTE=0`** if you only want Kafka traffic.

## Guided demo

```bash
make demo
```

Uses **`scripts/demo_local.sh`** — a scripted walkthrough over fixture paths.

## Fixture time modes

**`FIXTURE_TIME_MODE=relative`** (with **`FIXTURE_RELATIVE_BASE_HOURS_AGO`**) rewrites timestamps **in memory only** for integration-style runs — fixture files on disk are not modified.

## Wind and grid weather

- **`WIND_DRY_RUN=1`** uses JSONL wind fixtures.
- **`GRID_WEATHER_DRY_RUN=1`** uses packaged grid weather JSON for bounded demos.

See **[Live ingest](live-ingest.md)** when you intentionally call live APIs.
