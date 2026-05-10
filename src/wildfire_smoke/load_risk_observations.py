"""Load calibration observations from JSONL fixtures (no secrets)."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from psycopg.types.json import Json

from wildfire_smoke.db.connection import connect
from wildfire_smoke.logging import configure_logging
from wildfire_smoke.settings import Settings, repo_root

log = logging.getLogger(__name__)


def _parse_ts(raw: str) -> datetime:
    s = raw.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_fixture_file(path: Path) -> int:
    settings = Settings.from_env()
    inserted = 0
    lines_read = 0
    with path.open(encoding="utf-8") as fh:
        with connect(settings) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                      SELECT 1 FROM information_schema.tables
                      WHERE table_schema = 'analytics' AND table_name = 'risk_observations'
                    )
                    """
                )
                if not cur.fetchone()[0]:
                    print("analytics.risk_observations not installed; skip load (exit 0).")
                    return 0
                cur.execute(
                    """
                    DELETE FROM analytics.risk_observations
                    WHERE COALESCE(metadata->>'phase12_fixture','') = 'true'
                    """
                )

            for line in fh:
                line = line.strip()
                if not line:
                    continue
                lines_read += 1
                row: dict[str, Any] = json.loads(line)
                observed_at = _parse_ts(str(row["observed_at"]))
                geography_type = str(row["geography_type"]).strip().lower()
                geoid = str(row["geoid"]).strip()
                observation_type = str(row["observation_type"]).strip()
                observed_value = row.get("observed_value")
                ov = float(observed_value) if observed_value is not None else None
                source = str(row.get("source") or "risk_observation_fixture").strip()
                notes = row.get("notes")
                metadata = dict(row.get("metadata") or {})
                metadata.setdefault("phase12_fixture", True)

                units = row.get("units")
                parameter = row.get("parameter")
                lag_hours = row.get("lag_hours")
                lh = float(lag_hours) if lag_hours is not None else None
                confidence_label = row.get("confidence_label")
                quality_flags = row.get("quality_flags") or {}

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO analytics.risk_observations (
                          observed_at, geography_type, geoid, observation_type,
                          observed_value, observed_band, source, notes, metadata,
                          units, parameter, lag_hours, confidence_label, quality_flags
                        ) VALUES (
                          %s, %s, %s, %s,
                          %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s::jsonb
                        )
                        """,
                        (
                            observed_at,
                            geography_type,
                            geoid,
                            observation_type,
                            ov,
                            row.get("observed_band"),
                            source,
                            notes,
                            Json(metadata),
                            units,
                            parameter,
                            lh,
                            confidence_label,
                            Json(quality_flags),
                        ),
                    )
                inserted += 1
            conn.commit()

    log.info(
        "risk_observation_fixture_load_complete",
        extra={"path": str(path), "lines": lines_read, "inserted": inserted},
    )
    print(json.dumps({"risk_observations_inserted": inserted, "lines_read": lines_read}, indent=2))
    return inserted


def main() -> None:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    raw_path = os.environ.get(
        "RISK_OBSERVATION_FIXTURE_JSONL",
        str(repo_root() / "tests/fixtures/risk_observations_sample.jsonl"),
    )
    path = Path(raw_path)
    if not path.is_file():
        print(f"Fixture not found: {path}; exit 0.")
        return
    load_fixture_file(path)


if __name__ == "__main__":
    main()
