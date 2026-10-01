"""Protocol-independent declarations and the values exchanged with the core."""

from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Literal


if TYPE_CHECKING:
    from vector.core.core import Source


ControlValue = bool | int | float
TimestampSource = Literal["device_synced", "server_receive"]


class ControlType(StrEnum):
    BOOL = "BOOL"
    UINT32 = "UINT32"
    INT32 = "INT32"
    FLOAT32 = "FLOAT32"


class ControlStatus(StrEnum):
    CONFIRMED = "confirmed"
    PENDING = "pending"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class SensorDefinition:
    """A measurement in physical units; providers convert wire values before publishing."""

    name: str
    group: str = ""
    unit: str = ""


@dataclass(frozen=True, slots=True)
class ControlDefinition:
    """A typed actuator declaration; ``default`` is metadata, not a startup command."""

    name: str
    group: str = ""
    type: ControlType = ControlType.BOOL
    default: ControlValue | None = None
    unit: str | None = None


@dataclass(frozen=True, slots=True)
class ControlObservation:
    """An accepted request or a device observation; dispatch alone creates neither."""

    value: ControlValue | None
    timestamp: float
    status: ControlStatus | None = None


@dataclass(frozen=True, slots=True)
class SensorBinding:
    """One source's declaration; ``id`` is its declaration ordinal, not a packet ID."""

    source: Source
    definition: SensorDefinition
    id: int

    @property
    def name(self) -> str:
        return self.definition.name

    @property
    def group(self) -> str:
        return self.definition.group

    @property
    def unit(self) -> str:
        return self.definition.unit


@dataclass(frozen=True, slots=True)
class ControlBinding:
    """An explicit command target tied to a single registration generation."""

    source: Source
    definition: ControlDefinition
    id: int

    @property
    def name(self) -> str:
        return self.definition.name

    @property
    def group(self) -> str:
        return self.definition.group

    @property
    def unit(self) -> str | None:
        return self.definition.unit

    @property
    def type(self) -> ControlType:
        return self.definition.type

    @property
    def default(self) -> ControlValue | None:
        return self.definition.default

    @property
    def accepted(self) -> ControlObservation | None:
        return self.source.accepted_control(self)

    @property
    def reported(self) -> ControlObservation | None:
        return self.source.reported_control(self)


@dataclass(frozen=True, slots=True)
class DispatchResult:
    """Submission outcome, separate from subsequent accepted/reported control state."""

    submitted: bool
    command_id: int | None = None
    error: str | None = None
    cause: Exception | None = None
    target: ControlBinding | None = None


@dataclass(frozen=True, slots=True)
class CoreChange:
    """Synchronous state notification; consumers translate it into their own events."""

    kind: str
    source: Source | None = None
    control: ControlBinding | None = None
    sensor_name: str | None = None
    offset: float | None = None


@dataclass(frozen=True, slots=True)
class TelemetryReading:
    """A reading after taring; ``value + tare`` recovers the submitted raw value.

    The ordinal and field names preserve the existing telemetry consumer format.
    """

    sensor_id: int
    sensor_name: str
    value: float
    unit_name: str
    sensor_type: str
    tare: float = 0.0


@dataclass(frozen=True, slots=True)
class TelemetryBatch:
    """A normalized batch. Legacy ``device_*`` names describe any source, not just nodes.

    Key consumers by ``(source_provider, source_key)``; ``device_name`` is a label
    and may collide or change. Include ``connection_key`` when separating reconnects.
    """

    source_provider: str
    source_key: str
    device_name: str
    device_address: str
    connection_key: str
    timestamp_s: float
    readings: tuple[TelemetryReading, ...]
    timestamp_source: TimestampSource
    timestamp_synced: bool
