# Operational model

## Roles

- **Developers** work with producers, Spark jobs, SQL, and Python using **`uv`** and Compose.
- **Operators** run **`Makefile`** targets and **`scripts/*.sh`**, tune **`ALERT_*`** and related env vars (see **`.env.example`**).

## Environments

- **Local Compose** is the reference runtime for demos and integration checks.
- **CI** runs fast static checks without Compose; optional workflows exercise Compose with **minimal census** fixtures.

## Safety defaults

- Dry-run producers (`*_DRY_RUN=1`) avoid live vendor calls for demos.
- **`ALERTS_WARN_ONLY=1`** is common with stale fixtures so freshness SLIs do not page incorrectly.
- **`make db-bootstrap-minimal`** with **`MINIMAL_CENSUS_REPLACE_ALL=1`** can **truncate** geo tables — **disposable DBs / CI only**.

## Releases

- **`make release-check`** is the maintainer gate (lint, tests, docs strict build, smoke).
- **`CHANGELOG.md`** and **`docs/release/`** describe scope; they do **not** claim scientific validation.

## Calibration exports

Exports under **`artifacts/calibration/`** are **immutable review bundles**. Metadata must not contain passwords, raw DSNs, or tokens.
