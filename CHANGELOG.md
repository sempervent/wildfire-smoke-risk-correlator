# Changelog

All notable changes to this project are documented here. Releases are **engineering milestones** — they do **not** imply scientific validation of smoke-risk outputs.

## [0.1.0] — 2026-05-09

### Added

- **Phase 13 — CI / release hardening:** GitHub Actions **fast CI** (ruff, pytest, no-Compose smoke, Grafana JSON validation) and **optional Compose integration** workflow (manual, weekly, or PR label `integration`).
- **Minimal census fixtures** for CI/integration (`tests/fixtures/census_minimal_*.geojson`, `make db-bootstrap-minimal`) — synthetic geometries only, **not** operational TIGER data.
- **Immutable calibration exports** (`wildfire_smoke.export_calibration`, `make export-calibration` / `export-calibration-csv` / `export-calibration-parquet`) writing timestamped bundles under `artifacts/calibration/` with redacted metadata.
- **Release gate** (`scripts/release_check.sh`, `make release-check`) and **`make version`** (package + optional git metadata).
- **Documentation:** `docs/release/v0.1.0.md`, architecture notes under `docs/architecture/`, Grafana **calibration confidence banner** panel.

### Notes

- Default PR CI stays **no secrets**, **no Census download**, **no Compose**.
- **v0.1.0** is a **vertical-slice correlator** with calibration **scaffolding** — not a validated public-health or forecasting product.
