from dataclasses import dataclass

from vector.qlcp.enums import ControlState, ControlType


@dataclass(slots=True, frozen=True, kw_only=True)
class SensorConfig:
    id: int
    name: str
    group: str
    unit: str

    def to_core(self) -> "CoreSensor":
        """Convert this QLCP sensor config to the transport-agnostic core representation."""
        from vector.core.sensor import CoreSensor

        return CoreSensor(
            id=self.id,
            name=self.name,
            group=self.group,
            unit=self.unit,
            source="qlcp",
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class ControlConfig:
    id: int
    name: str
    group: str
    default: ControlState | int | float
    type: ControlType
    unit: str | None

    def to_core(self) -> "CoreControl":
        """Convert this QLCP control config to the transport-agnostic core representation."""
        from vector.core.control import CoreControl

        return CoreControl(
            id=self.id,
            name=self.name,
            group=self.group,
            type=self.type.name,
            default=self.default,
            unit=self.unit,
            source="qlcp",
        )


@dataclass(slots=True, frozen=True)
class DeviceConfig:
    name: str
    sensors_by_id: dict[int, SensorConfig]
    controls_by_id: dict[int, ControlConfig]
