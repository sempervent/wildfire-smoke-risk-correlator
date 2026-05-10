# CI and testing

Pull requests and pushes to **`main`** run **`.github/workflows/ci.yml`**:

- **Lint + tests:** `ruff` and `pytest` (no API keys, no Census download, no Compose).
- **Smoke (host-only):** `SMOKE_NO_COMPOSE=1 bash scripts/smoke_test.sh` after syncing deps.
- **Grafana JSON:** validates `docker/grafana/dashboards/smoke-risk.json`.
- **Documentation:** `make docs-check` (**`mkdocs build --strict`**) with the **`docs`** optional dependency group.

Heavy Compose workflows live in **`.github/workflows/integration.yml`** (manual, scheduled, or label-gated).

The published documentation site is built and deployed by **`.github/workflows/docs.yml`** on pushes to **`main`** and **`workflow_dispatch`** (GitHub Pages → **GitHub Actions** source).

Related: **[Release check](../operations/release-check.md)**, **[Troubleshooting](../operations/troubleshooting.md)**.
