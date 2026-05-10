"""Wildfire / smoke risk correlator (local vertical slice)."""

from __future__ import annotations

try:
    from importlib.metadata import version as _pkg_version

    __version__ = _pkg_version("wildfire-smoke-risk-correlator")
except Exception:
    __version__ = "1.1.0"

__all__ = ["__version__"]
