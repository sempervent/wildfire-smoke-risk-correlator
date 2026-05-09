from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class AlertEventRow:
    alert_event_id: UUID
    fingerprint: str
    alert_type: str
    severity: str
    geography_type: str | None
    geoid: str | None
    title: str
    description: str
    observed_at: datetime
    first_seen_at: datetime
    last_seen_at: datetime
    details: dict[str, Any]

    @staticmethod
    def from_record(row: tuple[Any, ...]) -> AlertEventRow:
        (
            alert_event_id,
            fingerprint,
            alert_type,
            severity,
            geography_type,
            geoid,
            title,
            description,
            observed_at,
            first_seen_at,
            last_seen_at,
            details,
        ) = row
        return AlertEventRow(
            alert_event_id=alert_event_id,
            fingerprint=str(fingerprint),
            alert_type=str(alert_type),
            severity=str(severity),
            geography_type=geography_type,
            geoid=geoid,
            title=str(title),
            description=str(description),
            observed_at=observed_at,
            first_seen_at=first_seen_at,
            last_seen_at=last_seen_at,
            details=dict(details or {}),
        )


class Notifier(ABC):
    key: str

    @abstractmethod
    def send(self, alerts: list[AlertEventRow]) -> None:
        raise NotImplementedError
