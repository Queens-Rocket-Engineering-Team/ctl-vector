"""Core sensor and control abstractions for VECTOR.

This package defines transport-agnostic representations for sensors and controls.
Runtime services (QLCP, Kasa, etc.) register their sensors/controls through these
common types rather than each having their own representation.
"""

from vector.core.control import CoreControl
from vector.core.sensor import CoreSensor

__all__ = ["CoreSensor", "CoreControl"]