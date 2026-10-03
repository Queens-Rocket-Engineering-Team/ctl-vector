"""Shared sensor/control API. Construct one Core and inject it into services.

Providers keep Source handles; consumers use bindings, queries, and subscriptions.
See docs/CORE.md for examples and the boundary between core and service ownership.
"""

from vector.core.core import (
    TARE_DEFAULT_SAMPLES,
    TARE_SAMPLE_CAPACITY,
    TARE_SAMPLE_MAX_AGE_S,
    ControlHandler,
    ControlValidationError,
    Core,
    Source,
    TareCapture,
    TareCaptureError,
)
from vector.core.models import (
    ChangeKind,
    ControlBinding,
    ControlDefinition,
    ControlObservation,
    ControlStatus,
    ControlType,
    ControlValue,
    CoreChange,
    DispatchResult,
    SensorBinding,
    SensorDefinition,
    TelemetryBatch,
    TelemetryReading,
    TimestampSource,
)


__all__ = [
    "TARE_DEFAULT_SAMPLES",
    "TARE_SAMPLE_CAPACITY",
    "TARE_SAMPLE_MAX_AGE_S",
    "ChangeKind",
    "ControlBinding",
    "ControlDefinition",
    "ControlHandler",
    "ControlObservation",
    "ControlStatus",
    "ControlType",
    "ControlValidationError",
    "ControlValue",
    "Core",
    "CoreChange",
    "DispatchResult",
    "SensorBinding",
    "SensorDefinition",
    "Source",
    "TareCapture",
    "TareCaptureError",
    "TelemetryBatch",
    "TelemetryReading",
    "TimestampSource",
]
