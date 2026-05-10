from __future__ import annotations

import json
import logging

from wildfire_smoke.logging import JsonFormatter


def test_json_formatter_uses_timezone_aware_utc() -> None:
    fmt = JsonFormatter()
    record = logging.LogRecord(name="t", level=logging.INFO, pathname=__file__, lineno=1, msg="x", args=(), exc_info=None)
    record.created = 1_700_000_000.0
    out = json.loads(fmt.format(record))
    assert out["time"].endswith("Z")
    assert "+00:00" not in out["time"]
