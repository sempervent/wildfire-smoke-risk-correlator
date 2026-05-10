"""Load alert_type → runbook slug/path mapping from config/runbooks.yaml."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from wildfire_smoke.settings import repo_root


@dataclass(frozen=True)
class RunbookMapping:
    slug: str
    path: Path


def load_runbook_mappings() -> dict[str, RunbookMapping]:
    path = repo_root() / "config" / "runbooks.yaml"
    raw: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    mappings = raw.get("mappings") or {}
    out: dict[str, RunbookMapping] = {}
    for alert_type, row in mappings.items():
        if not isinstance(row, dict):
            continue
        slug = str(row.get("slug") or "").strip()
        rel = str(row.get("path") or "").strip()
        if not slug or not rel:
            continue
        out[str(alert_type)] = RunbookMapping(slug=slug, path=repo_root() / rel)
    return out


def runbook_slug_for_alert_type(alert_type: str, mappings: dict[str, RunbookMapping] | None = None) -> str | None:
    m = mappings if mappings is not None else load_runbook_mappings()
    hit = m.get(alert_type)
    return hit.slug if hit else None
