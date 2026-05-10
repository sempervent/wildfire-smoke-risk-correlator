"""
Census bootstrap configuration: resolve state FIPS list and county load mode from env + census.yaml.

Canonical geometries remain in geo.*; this module only drives download/load scripts.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from wildfire_smoke.settings import repo_root


def _truthy(val: str | None) -> bool:
    if val is None:
        return False
    return val.strip().lower() in {"1", "true", "yes", "on"}


def load_census_yaml(path: Path | None = None) -> dict[str, Any]:
    p = path or repo_root() / "config" / "census.yaml"
    if not p.exists():
        raise FileNotFoundError(f"Missing census config: {p}")
    return yaml.safe_load(p.read_text())


def resolved_state_fps(yaml_data: dict[str, Any]) -> list[str]:
    """Ordered unique state FIPS codes (e.g. ['47', '37'])."""

    env_fps = os.environ.get("CENSUS_STATEFPS")
    if env_fps is not None and env_fps.strip():
        out = [x.strip().zfill(2) for x in env_fps.split(",") if x.strip()]
        return list(dict.fromkeys(out))

    env_fp = os.environ.get("CENSUS_STATEFP")
    if env_fp is not None and env_fp.strip():
        return [env_fp.strip().zfill(2)]

    states = yaml_data.get("states")
    if isinstance(states, list) and states:
        out = []
        for s in states:
            if isinstance(s, dict) and s.get("statefp"):
                out.append(str(s["statefp"]).strip().zfill(2))
        if out:
            return list(dict.fromkeys(out))

    legacy = yaml_data.get("state") or {}
    fp = str(legacy.get("statefp", "")).strip().zfill(2)
    if not fp:
        raise ValueError("census.yaml must define state.statefp, states[], or use CENSUS_STATEFP(S)")
    return [fp]


def load_national_counties_full_us() -> bool:
    """Load all US counties into geo.counties (large)."""

    return _truthy(os.environ.get("CENSUS_LOAD_NATIONAL_COUNTIES"))


@dataclass(frozen=True)
class CensusValidationThresholds:
    min_total_counties: int
    min_total_tracts: int


def validation_thresholds(yaml_data: dict[str, Any], num_states: int) -> CensusValidationThresholds:
    v = yaml_data.get("validation") or {}
    base_c = int(v.get("min_counties", 90))
    base_t = int(v.get("min_tracts", 1000))

    if num_states <= 1:
        min_c, min_t = base_c, base_t
    else:
        floor_c = int(v.get("min_counties_per_state_floor", 75))
        floor_t = int(v.get("min_tracts_per_state_floor", 800))
        min_c = max(base_c, floor_c * num_states)
        min_t = max(base_t, floor_t * num_states)

    if num_states > 1:
        if v.get("min_total_counties_multi_state") is not None:
            min_c = max(min_c, int(v["min_total_counties_multi_state"]))
        if v.get("min_total_tracts_multi_state") is not None:
            min_t = max(min_t, int(v["min_total_tracts_multi_state"]))

    if load_national_counties_full_us():
        min_c = max(min_c, int(v.get("min_counties_national_us", 3100)))

    return CensusValidationThresholds(min_total_counties=min_c, min_total_tracts=min_t)


def state_fps_sql_in_clause(state_fps: list[str]) -> str:
    """SQL IN list for STATEFP filters, e.g. ''47'',''37'''."""

    return ",".join("'" + fp.replace("'", "") + "'" for fp in state_fps)
