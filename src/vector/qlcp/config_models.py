from dataclasses import dataclass

from vector.qlcp.enums import ControlState, ControlType


@dataclass(slots=True, frozen=True, kw_only=True)
class SensorConfig:
    id: int
    name: str
    group: str
    unit: str

@dataclass(slots=True, frozen=True, kw_only=True)
class ControlConfig:
    id: int
    name: str
    group: str
    default: ControlState | int | float
    type: ControlType
    unit: str | None


@dataclass(slots=True, frozen=True)
class DeviceConfig:
    name: str
    sensors_by_id: dict[int, SensorConfig]
    controls_by_id: dict[int, ControlConfig]

    def __post_init__(self) -> None:
        # VECTOR maps wire IDs to declaration ordinals for telemetry, commands, and
        # feedback. A gap or mismatch would route a command to the wrong control.
        for kind, ids in (
            ("sensor", [(key, sensor.id) for key, sensor in self.sensors_by_id.items()]),
            ("control", [(key, control.id) for key, control in self.controls_by_id.items()]),
        ):
            if sorted(ids) != [(i, i) for i in range(len(ids))]:
                message = f"CONFIG {kind} IDs must be contiguous from 0, got {sorted(ids)}."
                raise ValueError(message)
        # A control is addressed by device and name, so a repeated name has no target.
        names = [control.name.upper() for control in self.controls_by_id.values()]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            message = f"CONFIG control names must be unique within a device, ignoring case: {', '.join(duplicates)}."
            raise ValueError(message)
