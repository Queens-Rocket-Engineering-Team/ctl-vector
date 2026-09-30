"""Transport-agnostic sensor representation for VECTOR."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class CoreSensor:
    """A sensor as registered by a runtime service.

    ``source`` records which runtime produced this sensor (e.g. "qlcp", "kasa")
    so consumers can distinguish transport-specific metadata without needing to
    import transport types.
    """

    id: int
    name: str
    group: str
    unit: str
    source: str