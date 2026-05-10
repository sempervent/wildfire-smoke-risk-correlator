from __future__ import annotations

import subprocess

from wildfire_smoke.settings import repo_root


def test_calibration_shell_scripts_parse() -> None:
    root = repo_root()
    for name in (
        "scripts/load_risk_observation_fixtures.sh",
        "scripts/calibration_summary.sh",
        "scripts/calibration_demo.sh",
        "scripts/load_minimal_census_fixtures.sh",
        "scripts/export_calibration.sh",
        "scripts/release_check.sh",
        "scripts/db_doctor.sh",
        "scripts/repair_alert_function.sh",
        "scripts/release_fresh_volume_test.sh",
        "scripts/write_release_manifest.sh",
    ):
        subprocess.run(["bash", "-n", str(root / name)], check=True)
