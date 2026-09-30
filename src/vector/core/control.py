"""Transport-agnostic control representation for VECTOR."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class CoreControl:
    """A control as registered by a runtime service.

    ``type`` is stored as a string (e.g. ``"BOOL"``, ``"FLOAT32"``) rather than
    a transport-specific enum so the core package stays independent of any
    protocol layer. Adapters in the transport-specific code convert between
    this string and their native enum.
    """

    id: int
    name: str
    group: str
    type: str
    default: object
    unit: str | None
    source: str