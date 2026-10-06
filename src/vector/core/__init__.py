"""Shared sensor/control API. Construct one Core and inject it into services.

Providers keep Source handles; consumers use bindings, queries, and subscriptions.
See docs/CORE.md for examples and the boundary between core and service ownership.
"""

from vector.core.core import (
    TARE_DEFAULT_SAMPLES,
    TARE_SAMPLE_CAPACITY,
    TARE_SAMPLE_MAX_AGE_S,
    ControlDispatchError,
    ControlHandler,
    ControlValidationError,
    Core,
    Source,
    TareCapture,
    TareCaptureError,
)
from vector.core.models import (
    ControlBinding,
    ControlChanged,
    ControlDefinition,
    ControlObservation,
    ControlStatus,
    ControlType,
    ControlValue,
    CoreChange,
    SensorBinding,
    SensorDefinition,
    SourceChanged,
    TareChanged,
    TelemetryBatch,
    TelemetryReading,
    TimestampSource,
)


__all__ = [
    "TARE_DEFAULT_SAMPLES",
    "TARE_SAMPLE_CAPACITY",
    "TARE_SAMPLE_MAX_AGE_S",
    "ControlBinding",
    "ControlChanged",
    "ControlDefinition",
    "ControlDispatchError",
    "ControlHandler",
    "ControlObservation",
    "ControlStatus",
    "ControlType",
    "ControlValidationError",
    "ControlValue",
    "Core",
    "CoreChange",
    "SensorBinding",
    "SensorDefinition",
    "Source",
    "SourceChanged",
    "TareCapture",
    "TareCaptureError",
    "TareChanged",
    "TelemetryBatch",
    "TelemetryReading",
    "TimestampSource",
]
