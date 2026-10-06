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
    """A device's report of a control's state; dispatch alone does not create one."""

    value: ControlValue | None
    timestamp: float
    status: ControlStatus


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
    def reported(self) -> ControlObservation | None:
        return self.source._reported.get(self.id)  # noqa: SLF001


@dataclass(frozen=True, slots=True)
class SourceChanged:
    """A source was registered, or a registration was closed."""

    kind: Literal["registered", "closed"]
    source: Source


@dataclass(frozen=True, slots=True)
class ControlChanged:
    """A provider reported feedback; read the new state from ``control.reported``."""

    control: ControlBinding


@dataclass(frozen=True, slots=True)
class TareChanged:
    """A shared tare offset was set, or cleared when ``offset`` is None."""

    sensor_name: str
    offset: float | None


# A synchronous state notification; consumers translate it into their own events.
CoreChange = SourceChanged | ControlChanged | TareChanged


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
