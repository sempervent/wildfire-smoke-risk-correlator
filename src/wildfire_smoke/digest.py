"""Alert digest formatting (console / structured payloads) without network I/O."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from wildfire_smoke.notifiers.base import AlertEventRow


@dataclass(frozen=True)
class DigestSummary:
    window_hours: int
    items: list[AlertEventRow]
    counts_by_severity: dict[str, int]
    newest_observed_at: datetime | None
    top_titles: list[str]
    top_geographies: list[str]
    runbook_slugs: list[str]


def build_digest_summary(*, window_hours: int, alerts: list[AlertEventRow]) -> DigestSummary:
    counts: dict[str, int] = {}
    newest: datetime | None = None
    titles: list[str] = []
    geos: list[str] = []
    slugs: list[str] = []
    for a in alerts:
        counts[a.severity] = counts.get(a.severity, 0) + 1
        if newest is None or a.observed_at > newest:
            newest = a.observed_at
        titles.append(a.title)
        if a.geography_type or a.geoid:
            geos.append(f"{a.geography_type or ''}:{a.geoid or ''}".strip(":"))
        if a.runbook_slug:
            slugs.append(a.runbook_slug)
    unique_geos = []
    seen_g = set()
    for g in geos:
        if g and g not in seen_g:
            seen_g.add(g)
            unique_geos.append(g)
    unique_titles: list[str] = []
    seen_t = set()
    for t in titles:
        if t not in seen_t:
            seen_t.add(t)
            unique_titles.append(t)
    return DigestSummary(
        window_hours=window_hours,
        items=list(alerts),
        counts_by_severity=counts,
        newest_observed_at=newest,
        top_titles=unique_titles[:15],
        top_geographies=unique_geos[:15],
        runbook_slugs=sorted(set(slugs)),
    )


def format_console_digest(summary: DigestSummary) -> str:
    lines = [
        f"Smoke correlator alert digest (last {summary.window_hours}h window)",
        f"Items={len(summary.items)} severities={summary.counts_by_severity}",
    ]
    if summary.newest_observed_at is not None:
        lines.append(f"Newest observed_at={summary.newest_observed_at.isoformat()}")
    if summary.top_titles:
        lines.append("Top titles:")
        for t in summary.top_titles:
            lines.append(f"  - {t}")
    if summary.top_geographies:
        lines.append("Affected geographies:")
        for g in summary.top_geographies:
            lines.append(f"  - {g}")
    if summary.runbook_slugs:
        lines.append("Runbooks:")
        for s in summary.runbook_slugs:
            lines.append(f"  - {s}")
    lines.append(
        "NOTE: digest summarizes included open incidents; verify critical items directly in "
        "analytics.v_open_alert_events / analytics.fn_alert_candidates."
    )
    return "\n".join(lines)


def build_webhook_digest_payload(summary: DigestSummary) -> dict[str, Any]:
    return {
        "kind": "wildfire_smoke_alert_digest",
        "window_hours": summary.window_hours,
        "count": len(summary.items),
        "counts_by_severity": summary.counts_by_severity,
        "newest_observed_at": summary.newest_observed_at.isoformat() if summary.newest_observed_at else None,
        "top_titles": summary.top_titles,
        "top_geographies": summary.top_geographies,
        "runbook_slugs": summary.runbook_slugs,
        "fingerprints": [a.fingerprint for a in summary.items[:50]],
    }


def build_slack_digest_payload(summary: DigestSummary) -> dict[str, Any]:
    text = format_console_digest(summary)
    return {"text": text[:15000]}


def format_smtp_digest(summary: DigestSummary) -> str:
    return format_console_digest(summary)


def digest_json_bytes(summary: DigestSummary, notifier_key: str) -> bytes:
    key = notifier_key.strip().lower()
    if key == "slack":
        payload = build_slack_digest_payload(summary)
    else:
        payload = build_webhook_digest_payload(summary)
    return json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
