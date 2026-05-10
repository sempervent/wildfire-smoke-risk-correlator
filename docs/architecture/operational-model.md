# Operational model

## Roles

- **Developers** iterate on producers, Spark jobs, SQL views, and Python jobs using **`uv`** + Compose.
- **Operators** run scripted targets (`Makefile`, `scripts/*.sh`), inspect Postgres views, and tune thresholds via env vars documented in **`.env.example`**.

## Environments

- **Local Compose** is the reference runtime for demos and integration checks.
- **CI** separates **fast static checks** (no Compose) from **optional integration** (Compose + minimal census + bounded fixtures).

## Safety defaults

- Dry-run producers avoid network secrets (`*_DRY_RUN=1`).
- Alert checks often run **`ALERTS_WARN_ONLY=1`** during fixture-heavy smoke because timestamps look stale versus freshness SLIs.
- **Minimal census bootstrap** (`make db-bootstrap-minimal`) may **`TRUNCATE`** geo tables when **`MINIMAL_CENSUS_REPLACE_ALL=1`** — **CI/dev only**, not a production migration path.

## Releases

- **`make release-check`** is the maintainer gate before tagging; it assumes **Compose is available** for **`make smoke-test`** unless you adapt the environment.
- **`CHANGELOG.md`** and **`docs/release/v1.0.0.md`** describe milestone scope — explicitly **not** scientific validation or public-health proof.

## Immutable calibration snapshots

- Exports are **point-in-time review bundles** for drift inspection and incident retrospectives.
- Treat them like logs/metrics artifacts: **no passwords**, **no raw webhook URLs**, **no vendor keys** — metadata is intentionally redacted.
