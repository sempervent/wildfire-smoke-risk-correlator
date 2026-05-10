# Release check

Maintainer gate script:

```bash
make release-check
```

Runs **`scripts/release_check.sh`**: **ruff**, **pytest**, Grafana JSON validation, **`bash -n`** on scripts, **`SMOKE_NO_COMPOSE=1`** smoke, **`make version`**, changelog/release-doc guards, **`mkdocs build --strict`** (**`make docs-check`**), optional Compose gates when **`COMPOSE_INTEGRATION=1`**, optional **`FULL_RELEASE_TEST=1`** for isolated fresh-volume test.

Use **`COMPOSE_INTEGRATION=1`** before tagging when Compose is available locally.
