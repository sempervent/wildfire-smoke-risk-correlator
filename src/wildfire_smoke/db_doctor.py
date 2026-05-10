"""Postgres structural checks for migration / alert-function drift (Phase 14)."""

from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from typing import Any

from wildfire_smoke.db.connection import connect
from wildfire_smoke.export_calibration import EXPORTS
from wildfire_smoke.logging import configure_logging
from wildfire_smoke.settings import Settings


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    detail: str


EXPECTED_FN_PARAM_COUNT = 23

SCHEMAS = ("raw", "normalized", "geo", "analytics")

TABLES = (
    "analytics.parse_errors",
    "analytics.kafka_consumer_offsets",
    "analytics.kafka_topic_offsets",
    "analytics.kafka_consumer_lag_observations",
    "analytics.dlq_replay_runs",
    "normalized.weather_grid_cells",
    "analytics.fire_weather_matches",
    "analytics.smoke_dispersion_exposures",
    "analytics.dispersion_aq_comparisons",
    "analytics.risk_observations",
    "analytics.risk_model_evaluations",
)

VIEWS_STRUCTURAL = (
    "analytics.v_alert_candidates",
    "analytics.v_integration_pipeline_counts",
    "analytics.v_calibration_confidence_summary",
    "analytics.v_dispersion_operational_summary",
)


def _scalar(cur, sql: str, params: tuple[Any, ...] | None = None) -> Any:
    cur.execute(sql, params or ())
    row = cur.fetchone()
    return row[0] if row else None


def run_checks(settings: Settings) -> list[CheckResult]:
    results: list[CheckResult] = []

    try:
        with connect(settings) as conn:
            results.append(CheckResult("postgres_reachable", True, "connected"))
            with conn.cursor() as cur:
                ver = _scalar(cur, "SELECT PostGIS_Version();")
                results.append(
                    CheckResult("postgis_extension", ver is not None, str(ver or "missing"))
                )

                for schema in SCHEMAS:
                    q = _scalar(
                        cur,
                        "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = %s);",
                        (schema,),
                    )
                    results.append(CheckResult(f"schema:{schema}", bool(q), "present" if q else "missing"))

                for tbl in TABLES:
                    reg = _scalar(cur, "SELECT to_regclass(%s)::text;", (tbl,))
                    ok = reg is not None and reg != ""
                    results.append(CheckResult(f"table:{tbl}", ok, reg or "null"))

                for vw in VIEWS_STRUCTURAL:
                    reg = _scalar(cur, "SELECT to_regclass(%s)::text;", (vw,))
                    ok = reg is not None and reg != ""
                    results.append(CheckResult(f"view:{vw}", ok, reg or "null"))

                cur.execute(
                    """
                    SELECT p.oid, p.pronargs,
                           pg_get_function_identity_arguments(p.oid) AS args
                    FROM pg_proc p
                    JOIN pg_namespace n ON n.oid = p.pronamespace
                    WHERE n.nspname = 'analytics'
                      AND p.proname = 'fn_alert_candidates'
                      AND p.prokind = 'f'
                    ORDER BY p.oid
                    """
                )
                fn_rows = cur.fetchall()
                cnt = len(fn_rows)
                overload_ok = cnt == 1
                params_ok = cnt == 1 and int(fn_rows[0][1]) == EXPECTED_FN_PARAM_COUNT
                results.append(
                    CheckResult(
                        "fn_alert_candidates:single_overload",
                        overload_ok,
                        f"count={cnt}",
                    )
                )
                results.append(
                    CheckResult(
                        "fn_alert_candidates:param_count",
                        params_ok,
                        f"expected={EXPECTED_FN_PARAM_COUNT} "
                        + (f"actual={fn_rows[0][1]} args={fn_rows[0][2]}" if cnt else "none"),
                    )
                )

                try:
                    cur.execute("SELECT 1 FROM analytics.v_alert_candidates LIMIT 1;")
                    results.append(CheckResult("select:v_alert_candidates", True, "ok"))
                except Exception as exc:
                    results.append(
                        CheckResult("select:v_alert_candidates", False, str(exc).split("\n")[0])
                    )

                for fname, _sql in EXPORTS:
                    m = re.search(r"FROM\s+(analytics\.\w+)\s*", _sql, flags=re.IGNORECASE)
                    if not m:
                        results.append(
                            CheckResult(f"export_sql:{fname}", False, "cannot parse view name")
                        )
                        continue
                    view_name = m.group(1)
                    try:
                        cur.execute(f"SELECT 1 FROM {view_name} LIMIT 1;")
                        results.append(CheckResult(f"select:{view_name}", True, "ok"))
                    except Exception as exc:
                        results.append(
                            CheckResult(
                                f"select:{view_name}",
                                False,
                                str(exc).split("\n")[0],
                            )
                        )
    except Exception as exc:
        results.append(CheckResult("postgres_reachable", False, str(exc).split("\n")[0]))
        return results

    return results


def main() -> None:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    warn_only = os.environ.get("DB_DOCTOR_WARN_ONLY", "0").strip().lower() in {"1", "true", "yes"}

    settings = Settings.from_env()
    rows = run_checks(settings)

    name_w = max(len(r.name) for r in rows)
    status_w = 5
    print(f"{'check'.ljust(name_w)}  {'ok'.ljust(status_w)}  detail")
    print("-" * (name_w + status_w + len("  detail") + 4))

    failed = 0
    for r in rows:
        st = "yes" if r.ok else "NO"
        print(f"{r.name.ljust(name_w)}  {st.ljust(status_w)}  {r.detail}")
        if not r.ok:
            failed += 1

    if failed:
        print(f"\ndb_doctor: {failed} check(s) failed.", file=sys.stderr)
        if not warn_only:
            sys.exit(1)
        sys.exit(0)

    print("\ndb_doctor: all checks passed.")
    sys.exit(0)


if __name__ == "__main__":
    main()
