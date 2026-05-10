# Changelog

All notable changes to this project are documented here. Releases are **engineering milestones** — they do **not** imply scientific validation of smoke-risk outputs.

## [1.1.0] — 2026-05-10

### Added

- **MkDocs** documentation site (**Material** theme) under **`docs/`** with task-oriented sections (getting started, user guide, operations, reference).
- **GitHub Pages** workflow **`.github/workflows/docs.yml`** (deploy on **`main`** + **`workflow_dispatch`**).
- **CI docs gate:** **`make docs-check`** (**`mkdocs build --strict`**) in **`release-check`** and **`.github/workflows/ci.yml`**.
- **`docs/release/v1.1.0.md`** release notes for documentation restructuring.

### Changed

- **README.md** rewritten as a concise project front door; detailed material moved into MkDocs pages.
- **Package version → 1.1.0**.

### Notes

- **No** changes to risk scoring formulas, core Spark/Python pipeline behavior, or default data sources beyond documentation and validation hooks.

## [1.0.1] — 2026-05-10

### Added

- **Integration workflow:** scheduled and **`workflow_dispatch`** runs invoke **`release_check.sh`** with **`COMPOSE_INTEGRATION=1`** after the Compose bootstrap path; optional **`upload-artifact`** for **`make export-calibration`** outputs (best-effort).
- **Documentation:** **`docs/release/v1.0.1.md`** maintenance stub (overload repair notes), **`README.md`** **`db-doctor`** troubleshooting examples, self-hosted runner pointers in workflow comments.
- **GitHub:** issue form **Ops / bug / data / calibration feedback** (`.github/ISSUE_TEMPLATE/ops_feedback.yml`).

### Notes

- No changes to risk formulas, modeling claims, or default data sources.

## [1.0.0] — 2026-05-10

### Added

- **Phase 14 — release hardening:** canonical **`analytics.fn_alert_candidates`** migration (**`013_phase14_canonical_alert_function.sql`**) applied **after** dependent views via **`scripts/bootstrap_db.sh`** to eliminate ambiguous overload drift on legacy volumes.
- **`make db-doctor`** / **`wildfire_smoke.db_doctor`** — structural checks (schemas, key tables/views, single overload + **23** parameters, selectable calibration export views).
- **`make repair-alert-function`** — reapplies Phase **10–12** alert-dependent views then migration **013** for operator repair on drifted databases.
- **`make release-fresh-volume-test`** — isolated Compose project (**`wildfire-smoke-release-test`** default) fresh-volume walkthrough (minimal census by default).
- **`make release-manifest`** — non-secret **`artifacts/release/<version>/release-manifest.json`** generator.
- **Optional `parquet` extra** (`pyproject.toml`) for Parquet calibration exports.
- **Documentation:** **`docs/release/v1.0.0.md`**, **`docs/release/v1.0.0-checklist.md`**.

### Changed

- **Package version → 1.0.0** — framed as the first **stable local/demo/research platform** release; still **not** public-health or regulatory validated.
- **`sql/views/zzz_phase9_fn_alert_candidates.sql`** is now a **stub**; canonical DDL ships in migration **013**.

## [0.1.0] — 2026-05-09

### Added

- **Phase 13 — CI / release hardening:** GitHub Actions **fast CI** (ruff, pytest, no-Compose smoke, Grafana JSON validation) and **optional Compose integration** workflow (manual, weekly, or PR label **`integration`**).
- **Minimal census fixtures** for CI/integration (`tests/fixtures/census_minimal_*.geojson`, `make db-bootstrap-minimal`) — synthetic geometries only, **not** operational TIGER data.
- **Immutable calibration exports** (`wildfire_smoke.export_calibration`, `make export-calibration*`) writing timestamped bundles under `artifacts/calibration/` with redacted metadata.
- **Release gate** (`scripts/release_check.sh`, `make release-check`) and **`make version`** (package + optional git metadata).

### Notes

- Default PR CI stays **no secrets**, **no Census download**, **no Compose**.
- **v0.1.0** documented the first engineering milestone before **v1.0.0** stabilization messaging.
