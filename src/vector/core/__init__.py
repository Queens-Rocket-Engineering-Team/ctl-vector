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
    TareCaptureError,
)
from vector.core.models import (
    ControlBinding,
    ControlDefinition,
    ControlObservation,
    ControlStatus,
    ControlType,
    ControlValue,
    CoreChange,
    DispatchResult,
    LatestSample,
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
    "LatestSample",
    "SensorBinding",
    "SensorDefinition",
    "Source",
    "TareCaptureError",
    "TelemetryBatch",
    "TelemetryReading",
    "TimestampSource",
]
