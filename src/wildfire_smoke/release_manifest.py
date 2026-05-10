"""Write a non-secret JSON manifest for release tagging."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from wildfire_smoke.settings import repo_root


def _git(fmt: str) -> str | None:
    try:
        return subprocess.check_output(
            ["git", *fmt.split()],
            cwd=repo_root(),
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def _git_dirty() -> bool | None:
    try:
        out = subprocess.check_output(
            ["git", "status", "--porcelain"],
            cwd=repo_root(),
            text=True,
            stderr=subprocess.DEVNULL,
        )
        return bool(out.strip())
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def redact_env_snapshot() -> dict[str, str | None]:
    """Include only non-sensitive toggles relevant to reproducibility."""
    keys = (
        "FIXTURE_TIME_MODE",
        "USE_ALIGNED_FIXTURES",
        "LOAD_RISK_OBSERVATION_FIXTURES",
        "DISPERSION_ENABLED",
        "GRID_WEATHER_ENABLED",
        "FIRMS_DRY_RUN",
        "OPENAQ_DRY_RUN",
        "WIND_DRY_RUN",
        "GRID_WEATHER_DRY_RUN",
        "SMOKE_RISK_MODEL_VERSION",
        "RISK_MODEL_VERSION",
        "DISPERSION_MODEL_VERSION",
        "PLUME_MODEL_VERSION",
    )
    out: dict[str, str | None] = {}
    for k in keys:
        out[k] = os.environ.get(k)
    return out


def build_manifest() -> dict:
    from wildfire_smoke import __version__

    root = repo_root()
    ver = __version__
    return {
        "version": ver,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "git_commit": _git("rev-parse HEAD"),
        "git_branch": _git("rev-parse --abbrev-ref HEAD"),
        "git_dirty": _git_dirty(),
        "repository_root": str(root),
        "validation_commands": [
            "uv run ruff check src tests",
            "uv run pytest -q",
            "python3 -m json.tool docker/grafana/dashboards/smoke-risk.json",
            "bash -n scripts/*.sh",
            "SMOKE_NO_COMPOSE=1 bash scripts/smoke_test.sh",
            "make version",
            "make db-doctor",
            "make release-check",
        ],
        "model_versions_supported": {
            "smoke_risk": ["v1", "v2", "v3", "v4", "v5"],
            "plume": ["wind_v1", "wind_grid_v2"],
            "dispersion_proxy": ["gaussian_v0"],
        },
        "major_tables_expected": [
            "normalized.fire_detections",
            "normalized.air_quality_measurements",
            "analytics.smoke_risk_scores",
            "analytics.smoke_plume_exposures",
            "analytics.smoke_dispersion_exposures",
            "analytics.dispersion_aq_comparisons",
            "analytics.risk_observations",
            "analytics.risk_model_evaluations",
        ],
        "major_views_expected": [
            "analytics.v_alert_candidates",
            "analytics.v_integration_pipeline_counts",
            "analytics.v_calibration_confidence_summary",
        ],
        "known_non_claims": [
            "Not validated for public health or emergency response.",
            "Not regulatory dispersion or air-quality compliance modeling.",
            "Risk and calibration outputs are engineering correlation aids only.",
        ],
        "data_sources": [
            "NASA FIRMS (optional live)",
            "OpenAQ v3 (optional live)",
            "NWS (optional wind/grid pathways)",
            "US Census TIGER/Line (operational bootstrap) or synthetic minimal fixtures (CI)",
        ],
        "dashboard_path": "docker/grafana/dashboards/smoke-risk.json",
        "release_docs_path": f"docs/release/v{ver}.md",
        "env_snapshot_redacted": redact_env_snapshot(),
        "note": "No API keys, passwords, or raw connection strings.",
    }


def write_manifest(*, dry_run: bool) -> Path | None:
    manifest = build_manifest()
    ver = manifest["version"]
    out_dir = Path(os.environ.get("RELEASE_MANIFEST_DIR", str(repo_root() / "artifacts" / "release"))) / str(
        ver
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "release-manifest.json"
    text = json.dumps(manifest, indent=2, default=str)
    if dry_run:
        print(text)
        print(json.dumps({"dry_run": True, "would_write": str(path)}, indent=2))
        return None
    path.write_text(text, encoding="utf-8")
    print(json.dumps({"written": str(path)}, indent=2))
    return path


def main() -> None:
    dry = os.environ.get("RELEASE_MANIFEST_DRY_RUN", "0").strip().lower() in {"1", "true", "yes"}
    if "--dry-run" in sys.argv:
        dry = True
    write_manifest(dry_run=dry)


if __name__ == "__main__":
    main()
