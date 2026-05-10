"""Export calibration views to immutable CSV/optional Parquet snapshots (no secrets)."""

from __future__ import annotations

import csv
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg.errors

from wildfire_smoke.db.connection import connect
from wildfire_smoke.logging import configure_logging
from wildfire_smoke.settings import Settings, repo_root

log = logging.getLogger(__name__)

SCHEMA_VERSION = "calibration_export_v1"

EXPORTS: tuple[tuple[str, str], ...] = (
    ("dispersion_aq_evidence_summary.csv", "SELECT * FROM analytics.v_dispersion_aq_evidence_summary"),
    ("dispersion_aq_lag_summary.csv", "SELECT * FROM analytics.v_dispersion_aq_lag_summary"),
    ("risk_model_evaluation_latest.csv", "SELECT * FROM analytics.v_risk_model_evaluation_latest"),
    ("risk_model_evaluation_history.csv", "SELECT * FROM analytics.v_risk_model_evaluation_history"),
    ("overprediction_candidates.csv", "SELECT * FROM analytics.v_model_overprediction_candidates"),
    ("underprediction_candidates.csv", "SELECT * FROM analytics.v_model_underprediction_candidates"),
    ("calibration_confidence_summary.csv", "SELECT * FROM analytics.v_calibration_confidence_summary"),
    ("risk_observation_coverage.csv", "SELECT * FROM analytics.v_risk_observation_coverage"),
)


def _git_rev() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repo_root(), text=True, stderr=subprocess.DEVNULL
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def _git_branch() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
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


def redact_db_host(host: str) -> str:
    h = (host or "").strip().lower()
    if h in {"", "localhost", "127.0.0.1", "::1"}:
        return h or "localhost"
    return "[redacted]"


def package_version() -> str:
    try:
        from importlib.metadata import version as pkg_version

        return pkg_version("wildfire-smoke-risk-correlator")
    except Exception:
        return "1.1.0"


def fetch_model_versions(settings: Settings) -> list[str]:
    versions: set[str] = set()
    try:
        with connect(settings) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT model_version::text FROM analytics.risk_model_evaluations
                    UNION
                    SELECT DISTINCT model_version::text FROM analytics.dispersion_aq_comparisons
                    """
                )
                for row in cur.fetchall():
                    if row and row[0]:
                        versions.add(str(row[0]))
    except Exception:
        pass
    return sorted(versions)


def write_csv(path: Path, sql: str, settings: Settings) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with connect(settings) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql)
            except psycopg.errors.UndefinedTable as exc:
                raise RuntimeError(
                    "Calibration export requires Phase 12 calibration views (apply "
                    "sql/views/zzz_phase12_calibration_views.sql or run make db-bootstrap)."
                ) from exc
            cols = [d[0] for d in (cur.description or ())]
            rows = cur.fetchall()
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for row in rows:
            w.writerow(row)
    return len(rows)


def csv_to_parquet(csv_path: Path, parquet_path: Path) -> None:
    try:
        import pyarrow.csv as pacsv  # noqa: PLC0415
        import pyarrow.parquet as pq  # noqa: PLC0415
    except ImportError as e:
        raise RuntimeError(
            "Parquet export requires pyarrow; install pyarrow or use CSV export only."
        ) from e

    table = pacsv.read_csv(str(csv_path))
    pq.write_table(table, str(parquet_path))


def build_metadata(
    *,
    settings: Settings,
    row_counts: dict[str, int],
    include_parquet: bool,
) -> dict[str, Any]:
    safe_env_keys = (
        "DISPERSION_ENABLED",
        "DISPERSION_MODEL_VERSION",
        "RISK_MODEL_VERSION",
        "SMOKE_RISK_MODEL_VERSION",
        "CALIBRATION_MIN_AQ_OBSERVATIONS",
        "RISK_EVAL_MIN_MATCH_COUNT",
        "RISK_EVAL_MODEL_VERSION",
    )
    env_snap = {k: os.environ.get(k) for k in safe_env_keys if os.environ.get(k) is not None}
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": SCHEMA_VERSION,
        "package_version": package_version(),
        "git_commit": _git_rev(),
        "git_branch": _git_branch(),
        "git_dirty": _git_dirty(),
        "database_host_redacted": redact_db_host(settings.postgres_host),
        "database_port": settings.postgres_port,
        "database_name": settings.postgres_db,
        "include_parquet": include_parquet,
        "row_counts": row_counts,
        "model_versions_observed": fetch_model_versions(settings),
        "env_toggles_redacted": env_snap,
        "note": "No passwords or raw connection strings; snapshots are not scientific validation.",
    }


def export_run(*, include_parquet: bool) -> Path | None:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    dry = os.environ.get("CALIBRATION_EXPORT_DRY_RUN", "0").strip().lower() in {"1", "true", "yes"}

    base = Path(os.environ.get("CALIBRATION_EXPORT_DIR", str(repo_root() / "artifacts" / "calibration")))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = base / stamp

    if dry:
        meta = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": SCHEMA_VERSION,
            "package_version": package_version(),
            "dry_run": True,
            "note": "CALIBRATION_EXPORT_DRY_RUN=1 — database export skipped.",
        }
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "metadata.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
        print(json.dumps({"output_dir": str(out_dir), "dry_run": True}, indent=2))
        return out_dir

    settings = Settings.from_env()
    row_counts: dict[str, int] = {}

    try:
        with connect(settings):
            pass
    except Exception as exc:
        print(f"Database unavailable; skipping calibration export ({exc}).", file=sys.stderr)
        return None

    out_dir.mkdir(parents=True, exist_ok=True)

    for fname, sql in EXPORTS:
        csv_path = out_dir / fname
        n = write_csv(csv_path, sql, settings)
        row_counts[fname] = n
        if include_parquet:
            pq_path = out_dir / fname.replace(".csv", ".parquet")
            csv_to_parquet(csv_path, pq_path)
            row_counts[pq_path.name] = n

    fmts = ["csv"] + (["parquet"] if include_parquet else [])
    meta = build_metadata(settings=settings, row_counts=row_counts, include_parquet=include_parquet)
    meta["export_formats"] = fmts
    (out_dir / "metadata.json").write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")

    print(json.dumps({"output_dir": str(out_dir), "row_counts": row_counts}, indent=2))
    log.info("calibration_export_complete", extra={"dir": str(out_dir)})
    return out_dir


def main() -> None:
    argv = [a.lower() for a in sys.argv[1:]]
    include_parquet = "--parquet" in argv or os.environ.get(
        "CALIBRATION_EXPORT_INCLUDE_PARQUET", "0"
    ).strip().lower() in {"1", "true", "yes"}
    fmts = os.environ.get("CALIBRATION_EXPORT_FORMATS", "").strip().lower()
    if fmts:
        parts = {p.strip() for p in fmts.replace(";", ",").split(",") if p.strip()}
        include_parquet = include_parquet or "parquet" in parts

    try:
        export_run(include_parquet=include_parquet)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
