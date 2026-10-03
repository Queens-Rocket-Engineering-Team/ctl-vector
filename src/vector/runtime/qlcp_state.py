"""Translate QLCP sessions and tracker diagnostics into the shared core and GUI.

Only this adapter knows both wire configuration/enum types and core definitions.
It retains sessions for live transport health; SystemState receives scalar views.
"""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, overload

from vector.core import ControlDefinition, ControlStatus, ControlType, SensorDefinition
from vector.qlcp.enums import ControlConfirmStatus, ControlState, PacketType
from vector.runtime.command_tracker import CommandRecord, CommandTracker, is_operator_visible
from vector.state.system_state import StateEvent, SystemState, TransportHealth, control_state_name, source_identity


if TYPE_CHECKING:
    from vector.core import ControlBinding, ControlHandler, ControlValue, Source
    from vector.runtime.esp_connection_runtime import ESPDeviceSession


@overload
def to_core_value(value: ControlState | ControlValue) -> ControlValue: ...
@overload
def to_core_value(value: None) -> None: ...
def to_core_value(value: ControlState | ControlValue | None) -> ControlValue | None:
    """Map the QLCP wire state to the core's typed value; BOOL controls become bools."""
    if isinstance(value, ControlState):
        return value == ControlState.OPEN
    return value


# The core leaves range to the provider; these are the integers the wire types carry.
_WIRE_INT_RANGE = {
    ControlType.UINT32: (0, 2**32 - 1),
    ControlType.INT32: (-(2**31), 2**31 - 1),
}


def to_qlcp_state(control_type: ControlType, value: ControlValue) -> ControlState | int | float:
    """Inverse of ``to_core_value`` for outgoing CONTROL packets.

    Raises ValueError for an integer the wire type cannot carry, so a bad
    setpoint is refused before anything is tracked or sent.
    """
    if isinstance(value, bool):
        return ControlState.OPEN if value else ControlState.CLOSED
    bounds = _WIRE_INT_RANGE.get(control_type)
    if bounds is not None and not bounds[0] <= value <= bounds[1]:
        message = f"{value!r} does not fit {control_type.name}."
        raise ValueError(message)
    return value


class CommandProjection:
    """Serialize the existing tracker without copying its command history."""

    def __init__(self, tracker: CommandTracker) -> None:
        self.tracker = tracker

    def snapshot(self) -> dict[str, Any]:
        return {
            "pending": [
                self.command(command)
                for command in sorted(self.tracker.pending, key=lambda command: command.command_id)
                if is_operator_visible(command.packet_type)
            ],
            "recent": [
                self.command(command)
                for command in sorted(self.tracker.recent_completed, key=lambda command: command.command_id)
            ],
        }

    def pending_command_id(self, connection_key: str, control_id: int) -> int | None:
        return max(
            (
                command.command_id
                for command in self.tracker.pending
                if command.connection_key == connection_key
                and command.packet_type == PacketType.CONTROL
                and command.control_id == control_id
            ),
            default=None,
        )

    @staticmethod
    def command(command: CommandRecord) -> dict[str, Any]:
        return {
            "command_id": command.command_id,
            "source_provider": "qlcp",
            "source_key": command.device_name,
            "connection_key": command.connection_key,
            "device_address": command.device_address,
            "device_name": command.device_name,
            "packet_type": command.packet_type.name,
            "sequence": command.packet_sequence,
            "state": command.state.value,
            "sent_at": command.sent_at,
            "ack_expected": command.ack_expected,
            "acked_at": command.acked_at,
            "nacked_at": command.nacked_at,
            "timed_out_at": command.timed_out_at,
            "nack_error_code": command.nack_error_code.name if command.nack_error_code is not None else None,
            "control_id": command.control_id,
            "control_name": command.control_name,
            "requested_state": control_state_name(to_core_value(command.requested_state)),
        }


class QLCPStateAdapter:
    """Provider boundary used by the QLCP connection runtime.

    Registration, observations, and close publish through the core subscription.
    Command methods return projected events for the runtime's existing emitter.
    """

    def __init__(self, state: SystemState, tracker: CommandTracker) -> None:
        self.core = state.core
        self.state = state
        self.commands = CommandProjection(tracker)
        self._sessions: dict[str, ESPDeviceSession] = {}
        state.set_transport_views(commands=self.commands, health=self._health)

    def register_device(self, device: ESPDeviceSession, *, control_handler: ControlHandler | None = None) -> None:
        # CONFIG IDs are declaration ordinals (DeviceConfig rejects anything else).
        # Preserve that order so telemetry, dispatch, and feedback can target
        # bindings even when names repeat.
        self._sessions[device.name] = device
        device.core_source = self.core.register_source(
            "qlcp",
            device.name,
            address=device.address,
            connection_key=device.connection_key,
            sensors=tuple(
                SensorDefinition(name=sensor.name, group=sensor.group, unit=sensor.unit)
                for sensor in sorted(device.qlcp_config.sensors_by_id.values(), key=lambda sensor: sensor.id)
            ),
            controls=tuple(
                ControlDefinition(
                    name=control.name,
                    group=control.group,
                    unit=control.unit,
                    type=ControlType[control.type.name],
                    default=to_core_value(control.default),
                )
                for control in sorted(device.qlcp_config.controls_by_id.values(), key=lambda control: control.id)
            ),
            control_handler=control_handler,
        )

    def mark_disconnected(self, device: ESPDeviceSession) -> None:
        if device.core_source is not None:
            device.core_source.close()

    def record_reported_control_state(
        self,
        device: ESPDeviceSession,
        control_id: int,
        state: ControlState | ControlValue | None,
        *,
        status: ControlConfirmStatus = ControlConfirmStatus.CONFIRMED,
        now: float | None = None,
    ) -> None:
        control = self._control_for(device, control_id)
        if control is not None:
            control.source.report_control(control, to_core_value(state), status=ControlStatus[status.name], now=now)

    def record_accepted_control_state(
        self,
        device: ESPDeviceSession,
        control_id: int,
        state: ControlState | ControlValue,
        *,
        now: float | None = None,
    ) -> None:
        control = self._control_for(device, control_id)
        value = to_core_value(state)
        if control is not None and value is not None:
            control.source.accept_control(control, value, now=now)

    def _control_for(self, device: ESPDeviceSession, control_id: int) -> ControlBinding | None:
        # QLCP control IDs are the declaration ordinals preserved at registration.
        source = device.core_source
        return source.controls[control_id] if source is not None and 0 <= control_id < len(source.controls) else None

    def _health(self, source: Source) -> TransportHealth | None:
        device = self._sessions.get(source.key)
        if source.provider != "qlcp" or device is None or device.connection_key != source.connection_key:
            return None
        return TransportHealth(device.last_sync_time, device.missed_heartbeat_count)

    def record_command_sent(self, command: CommandRecord) -> StateEvent | None:
        return self._command_event("command.sent", command)

    def record_command_acked(self, command: CommandRecord) -> StateEvent | None:
        return self._command_event("command.acked", command)

    def record_command_nacked(self, command: CommandRecord) -> StateEvent | None:
        return self._command_event("command.nacked", command)

    def record_command_timed_out(self, command: CommandRecord) -> StateEvent | None:
        return self._command_event("command.timed_out", command)

    def _command_event(self, event_type: str, command: CommandRecord) -> StateEvent | None:
        if command.packet_type == PacketType.HEARTBEAT:
            source = self.core.source("qlcp", command.device_name)
            if source is None or source.connection_key != command.connection_key:
                return None
            return self.state.make_event(
                "heartbeat.updated",
                device_name=source.name,
                device_address=source.address,
                **source_identity(source),
                heartbeat=self.state.snapshot_heartbeat(source),
            )
        if not is_operator_visible(command.packet_type):
            return None
        return self.state.make_event(event_type, command=self.commands.command(command))
