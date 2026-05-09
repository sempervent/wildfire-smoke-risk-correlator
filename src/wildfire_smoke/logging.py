from __future__ import annotations

import json
import logging
import sys
from datetime import date, datetime
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "time": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=_json_default)


def _json_default(obj: Any) -> str:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)


def configure_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.handlers.clear()
    root.addHandler(handler)
