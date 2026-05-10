# Calibration exports

Immutable snapshots for offline review — **not** regulatory submissions.

## CSV (baseline)

```bash
make export-calibration
# or
make export-calibration-csv
```

Writes under **`artifacts/calibration/<YYYYMMDDTHHMMSSZ>/`**:

- Per-view CSV files
- **`metadata.json`** (redacted host, row counts, git/package hints when available — **no secrets**)

## Parquet (optional)

```bash
uv sync --extra parquet
make export-calibration-parquet
```

Requires **`pyarrow`**; otherwise the command fails with a clear message.

## Dry run

**`CALIBRATION_EXPORT_DRY_RUN=1`** writes a minimal **`metadata.json`** without querying views — useful when Postgres is unavailable.

## Environment

**`CALIBRATION_EXPORT_DIR`**, **`CALIBRATION_EXPORT_FORMATS`**, **`CALIBRATION_EXPORT_INCLUDE_PARQUET`** — see **`.env.example`**.
