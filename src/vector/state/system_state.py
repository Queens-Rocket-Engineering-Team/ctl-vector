from __future__ import annotations
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from vector.core.control import CoreControl
from vector.core.sensor import CoreSensor
from vector.qlcp.enums import ControlConfirmStatus, ControlState, PacketType
from vector.runtime.command_tracker import (
    CommandRecord,
    CommandTracker,
    is_operator_visible,
)


if TYPE_CHECKING:
    from vector.runtime.esp_connection_runtime import ESPDeviceSession


StateEvent = dict[str, object]


@dataclass(slots=True)
class _KasaState:
    """State projection for a single Kasa device."""
    host: str
    alias: str
    model: str
    active: bool
    connected: bool


@dataclass(slots=True)
class _ControlStateRecord:
    """Timestamped control state value: either device-reported or server-accepted.

    `status` is the device's confirmation status for reported records (confirmed/pending/error);
    unset (None) for accepted records, which have no such concept.
    """
    state: str | None
    timestamp: float
    status: str | None = None


@dataclass(frozen=True, slots=True)
class RecordingSchema:
    """Everything a recording's columns can be built from, as of one instant.

    Devices are never evicted from the projection, so this covers hardware that has
    disconnected as well as hardware currently reporting -- a device that drops and
    reconnects mid-session keeps its columns.
    """

    sensors: tuple[CoreSensor, ...]
    controls: tuple[CoreControl, ...]
    kasa: tuple[_KasaState, ...]


@dataclass(slots=True)
class _SessionStateRecord:
    """The recording session currently in progress, if any."""

    session_id: str
    name: str
    started_unix: float
    started_monotonic: float
    components: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class _DeviceState:
    """State projection for a single device. Control flows requested -> accepted (CONTROL ACK) -> reported (STATUS)."""
    device_name: str
    address: str
    connection_key: str
    config: DeviceConfig
    connected: bool
    device: ESPDeviceSession | None = None
    reported_controls: dict[int, _ControlStateRecord] = field(default_factory=dict)
    accepted_controls: dict[int, _ControlStateRecord] = field(default_factory=dict)


class SystemState:
    """Projection keyed by operational device identity. Heartbeat/sync/pending-command fields are sampled live and not covered by state_version.

    Device and Kasa state here is a projection of state owned elsewhere. Sensor tares are
    the exception: this class is their authoritative store, because taring is server-local
    state with no device-side counterpart and it needs the versioned event stream.
    """

    def __init__(self, *, command_tracker: CommandTracker) -> None:
        self._devices_by_name: dict[str, _DeviceState] = {}
        self._kasa_by_host: dict[str, _KasaState] = {}
        self._tares: dict[str, float] = {}
        self._session: _SessionStateRecord | None = None
        self._command_tracker = command_tracker
        self._state_version = 0

    @property
    def state_version(self) -> int:
        return self._state_version

    def register_device(self, device: ESPDeviceSession) -> StateEvent:
        """Manage the registration of a new device."""
        self._devices_by_name[device.name] = _DeviceState(
            device_name=device.name,
            address=device.address,
            connection_key=device.connection_key,
            config=device.qlcp_config,
            connected=True,
            device=device,
        )
        return self._make_event(
            "device.registered",
            device=self._snapshot_device(self._devices_by_name[device.name]),
        )

    def mark_disconnected(self, device: ESPDeviceSession) -> StateEvent | None:
        """Mark a device as disconnected and return a state event if it was previously registered."""
        device_state = self._devices_by_name.get(device.name)
        if device_state is None or device_state.connection_key != device.connection_key:
            return None

        device_state.connected = False
        device_state.device = device
        return self._make_event(
            "device.disconnected",
            device_name=device_state.device_name,
            device_address=device_state.address,
            connection_key=device_state.connection_key,
        )

    def record_reported_control_state(
        self,
        device: ESPDeviceSession,
        control_id: int,
        state: ControlState | int | float | None,
        *,
        status: ControlConfirmStatus = ControlConfirmStatus.CONFIRMED,
        now: float | None = None,
    ) -> StateEvent | None:
        """Record a control's reported state and return a state event if the device/control is known.

        On ControlConfirmStatus.ERROR, `state` is undefined per protocol and ignored; the control's
        last-known reported value is preserved and only its status is updated. Emits a distinct
        "control.error" event in that case so error onset is visible to consumers.
        """

        device_state = self._devices_by_name.get(device.name)
        if device_state is None:
            return None
        if device_state.connection_key != device.connection_key:
            return None

        control = device_state.config.controls_by_id.get(control_id)
        if control is None:
            return None

        existing = device_state.reported_controls.get(control_id)
        if status == ControlConfirmStatus.ERROR:
            state_name = existing.state if existing is not None else None
        else:
            state_name = self._control_state_name(state)

        device_state.reported_controls[control_id] = _ControlStateRecord(
            state=state_name,
            timestamp=time.monotonic() if now is None else now,
            status=status.name.lower(),
        )
        event_type = "control.error" if status == ControlConfirmStatus.ERROR else "control.updated"
        return self._make_event(
            event_type,
            device_name=device_state.device_name,
            control=self._snapshot_control(device_state, control),
        )

    def record_accepted_control_state(
        self,
        device: ESPDeviceSession,
        control_id: int,
        state: ControlState | int | float | str,
        *,
        now: float | None = None,
    ) -> StateEvent | None:
        """Record a control's accepted state and return a state event if the device/control is known."""

        device_state = self._devices_by_name.get(device.name)
        if device_state is None:
            return None
        if device_state.connection_key != device.connection_key:
            return None

        control = device_state.config.controls_by_id.get(control_id)
        if control is None:
            return None

        device_state.accepted_controls[control_id] = _ControlStateRecord(
            state=self._control_state_name(state),
            timestamp=time.monotonic() if now is None else now,
        )
        return self._make_event(
            "control.accepted",
            device_name=device_state.device_name,
            control=self._snapshot_control(device_state, control),
        )

    def register_kasa_device(self, host: str, alias: str, model: str, active: bool) -> StateEvent:
        """Register or update a Kasa device and return a state event."""
        self._kasa_by_host[host] = _KasaState(
            host=host,
            alias=alias,
            model=model,
            active=active,
            connected=True,
        )
        return self._make_event(
            "kasa.registered",
            kasa=self._snapshot_kasa(self._kasa_by_host[host]),
        )

    def record_kasa_state(self, host: str, active: bool) -> StateEvent | None:
        """Update a Kasa device's power state and return a state event, or None if not registered."""
        kasa_state = self._kasa_by_host.get(host)
        if kasa_state is None:
            return None
        kasa_state.active = active
        kasa_state.connected = True
        return self._make_event(
            "kasa.updated",
            kasa=self._snapshot_kasa(kasa_state),
        )

    def mark_kasa_unavailable(self, host: str) -> StateEvent | None:
        """Mark a Kasa device as unavailable and return a state event, or None if not registered."""
        kasa_state = self._kasa_by_host.get(host)
        if kasa_state is None:
            return None
        kasa_state.connected = False
        return self._make_event(
            "kasa.disconnected",
            kasa=self._snapshot_kasa(kasa_state),
        )

    def tares(self) -> dict[str, float]:
        """Return every applied tare offset, keyed by sensor name and sorted for determinism."""
        return dict(sorted(self._tares.items()))

    def tare_for(self, sensor_name: str) -> float:
        """Return the offset subtracted from *sensor_name* readings, or 0.0 if untared.

        Called once per reading from the UDP ingest loop, so it must stay a plain lookup.
        """
        return self._tares.get(sensor_name, 0.0)

    def set_tare(self, sensor_name: str, offset: float) -> StateEvent:
        """Set a sensor's tare offset and return a state event.

        Tares are keyed by sensor name alone, deliberately: during flight handoff the same
        sensor name is carried by more than one device and the tare must apply to all of
        them. For the same reason they are never cleared on device disconnect.
        """
        self._tares[sensor_name] = offset
        return self._make_event("tare.updated", sensor_name=sensor_name, offset=offset)

    def clear_tare(self, sensor_name: str) -> StateEvent | None:
        """Remove a sensor's tare offset, returning None if it had none."""
        if self._tares.pop(sensor_name, None) is None:
            return None
        return self._make_event("tare.cleared", sensor_name=sensor_name)

    def recording_schema(self) -> RecordingSchema:
        """Sensors, controls and Kasa outlets known to this process, for building recording columns.

        Read once when a recording starts. Because devices announce their full sensor
        and control set in their CONFIG packet, the columns are known up front and do
        not have to be discovered from the telemetry itself.
        """
        devices = [self._devices_by_name[name] for name in sorted(self._devices_by_name)]
        return RecordingSchema(
            sensors=tuple(sensor.to_core() for device in devices for sensor in device.config.sensors_by_id.values()),
            controls=tuple(control.to_core() for device in devices for control in device.config.controls_by_id.values()),
            kasa=tuple(self._kasa_by_host[host] for host in sorted(self._kasa_by_host)),
        )

    def control_states(self) -> dict[str, str | None]:
        """Every control's last device-reported state, keyed by control name.

        Flat across devices, matching how controls are named in the UI. Called once per
        telemetry batch from the ingest loop, so it stays a plain dict build.
        """
        states: dict[str, str | None] = {}
        for device_name in sorted(self._devices_by_name):
            device_state = self._devices_by_name[device_name]
            for control_id, control in device_state.config.controls_by_id.items():
                reported = device_state.reported_controls.get(control_id)
                states[control.name] = reported.state if reported is not None else None
        return states

    def kasa_active(self) -> dict[str, bool]:
        """Each Kasa outlet's power state, keyed by host."""
        return {host: self._kasa_by_host[host].active for host in sorted(self._kasa_by_host)}

    def session(self) -> dict[str, Any] | None:
        """Return the in-progress recording session, or None when nothing is being recorded."""
        if self._session is None:
            return None
        return {
            "id": self._session.session_id,
            "name": self._session.name,
            "started_unix": self._session.started_unix,
            "started_monotonic": self._session.started_monotonic,
            "components": dict(self._session.components),
        }

    def start_session(self, *, session_id: str, name: str, started_unix: float, started_monotonic: float) -> StateEvent:
        """Mark a recording session as in progress and return a state event."""
        self._session = _SessionStateRecord(
            session_id=session_id,
            name=name,
            started_unix=started_unix,
            started_monotonic=started_monotonic,
        )
        return self._make_event("session.started", session=self.session())

    def update_session_components(self, components: dict[str, str]) -> StateEvent | None:
        """Merge per-component recording statuses into the active session."""
        if self._session is None:
            return None
        self._session.components.update(components)
        return self._make_event("session.updated", session=self.session())

    def stop_session(self, *, stopped_unix: float, end_reason: str) -> StateEvent | None:
        """Clear the active session, returning None if there was none."""
        if self._session is None:
            return None
        session_id = self._session.session_id
        self._session = None
        return self._make_event("session.stopped", session_id=session_id, stopped_unix=stopped_unix, end_reason=end_reason)

    def record_session_warning(self, warning: str, detail: str | None = None) -> StateEvent | None:
        """Surface a non-fatal recording problem (a late device, an unsettled video file)."""
        if self._session is None:
            return None
        return self._make_event("session.warning", session_id=self._session.session_id, warning=warning, detail=detail)

    def record_command_sent(self, command: CommandRecord) -> StateEvent | None:
        return self._command_event("command.sent", command)

    def record_command_acked(self, command: CommandRecord) -> StateEvent | None:
        return self._command_event("command.acked", command)

    def record_command_nacked(self, command: CommandRecord) -> StateEvent | None:
        return self._command_event("command.nacked", command)

    def record_command_timed_out(self, command: CommandRecord) -> StateEvent | None:
        return self._command_event("command.timed_out", command)

    def snapshot(self) -> dict[str, Any]:
        devices = [self._snapshot_device(device_state) for device_state in sorted(self._devices_by_name.values(), key=lambda item: item.device_name)]
        kasa = [self._snapshot_kasa(kasa_state) for kasa_state in sorted(self._kasa_by_host.values(), key=lambda item: item.host)]
        commands = self._snapshot_commands()

        return {
            "state_version": self._state_version,
            "devices": devices,
            "kasa": kasa,
            "commands": commands,
            "tares": self.tares(),
            "session": self.session(),
        }

    @staticmethod
    def _snapshot_kasa(kasa_state: _KasaState) -> dict[str, Any]:
        return {
            "host": kasa_state.host,
            "alias": kasa_state.alias,
            "model": kasa_state.model,
            "active": kasa_state.active,
            "connected": kasa_state.connected,
        }

    def _snapshot_device(self, device_state: _DeviceState) -> dict[str, Any]:
        config = device_state.config
        device = device_state.device

        return {
            "name": config.name,
            "connected": device_state.connected,
            "address": device_state.address,
            "sensors": [self._snapshot_sensor(sensor.to_core()) for sensor in sorted(config.sensors_by_id.values(), key=lambda sensor: sensor.id)],
            "controls": [
                self._snapshot_control(device_state, control.to_core()) for control in sorted(config.controls_by_id.values(), key=lambda control: control.id)
            ],
            "last_sync_time": device.last_sync_time if device is not None else None,
            "heartbeat": self._snapshot_heartbeat(device_state),
        }

    @staticmethod
    def _snapshot_sensor(sensor: CoreSensor) -> dict[str, Any]:
        return {
            "id": sensor.id,
            "name": sensor.name,
            "group": sensor.group,
            "unit": sensor.unit,
        }

    def _snapshot_control(
        self,
        device_state: _DeviceState,
        control: CoreControl,
    ) -> dict[str, Any]:
        reported_state = device_state.reported_controls.get(control.id)
        accepted_state = device_state.accepted_controls.get(control.id)
        pending_command_id = self._pending_command_id(device_state, control.id)
        reported_status = reported_state.status if reported_state is not None else None

        return {
            "id": control.id,
            "name": control.name,
            "group": control.group,
            "type": control.type,
            "unit": control.unit,
            "default_state": self._control_state_name(control.default),
            "reported_state": reported_state.state if reported_state is not None else None,
            "reported_status": reported_status,
            "reported_timestamp": reported_state.timestamp if reported_state is not None else None,
            "accepted_state": accepted_state.state if accepted_state is not None else None,
            "accepted_timestamp": accepted_state.timestamp if accepted_state is not None else None,
            "pending_command_id": pending_command_id,
            # False while a CONTROL command is outstanding OR the device's last STATUS report
            # was "pending" (still actuating). One flag drives the existing pending UI treatment
            # regardless of which of those two sources is the cause.
            "settled": pending_command_id is None and reported_status != "pending",
        }

    def _pending_command_id(self, device_state: _DeviceState, control_id: int) -> int | None:
        device = device_state.device
        if device is None:
            return None

        pending_commands = [
            command
            for command in self._command_tracker.pending
            if (command.connection_key == device.connection_key and command.packet_type == PacketType.CONTROL and command.control_id == control_id)
        ]
        if not pending_commands:
            return None

        return max(command.command_id for command in pending_commands)

    def _snapshot_heartbeat(self, device_state: _DeviceState) -> dict[str, Any]:
        device = device_state.device
        consecutive_misses = device.missed_heartbeat_count if device is not None else 0

        if not device_state.connected:
            state = "disconnected"
        elif consecutive_misses > 0:
            state = "missed"
        else:
            state = "ok"

        return {
            "state": state,
            "consecutive_misses": consecutive_misses,
        }

    def _snapshot_commands(self) -> dict[str, Any]:
        pending = [
            self._snapshot_command(command)
            for command in sorted(self._command_tracker.pending, key=lambda command: command.command_id)
            if is_operator_visible(command.packet_type)
        ]
        recent = [
            self._snapshot_command(command) for command in sorted(self._command_tracker.recent_completed, key=lambda command: command.command_id)
        ]

        return {"pending": pending, "recent": recent}

    def _snapshot_command(self,command: CommandRecord) -> dict[str, Any]:
        return {
            "command_id": command.command_id,
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
            "requested_state": self._control_state_name(command.requested_state) if command.requested_state is not None else None,
        }

    @staticmethod
    def _control_state_name(state: ControlState | int | float | str) -> str:
        match state:
            case ControlState() as control_state:
                return control_state.name
            case int() | float():
                return str(state).upper()
            case _:
                return state.upper()

    def _command_event(self, event_type: str, command: CommandRecord) -> StateEvent | None:
        if command.packet_type == PacketType.HEARTBEAT:
            return self._heartbeat_event(command.connection_key)
        if not is_operator_visible(command.packet_type):
            return None

        return self._make_event(
            event_type,
            command=self._snapshot_command(command),
        )

    def _heartbeat_event(self, connection_key: str) -> StateEvent | None:
        device_state = self._device_state_for_connection(connection_key)
        if device_state is None:
            return None

        return self._make_event(
            "heartbeat.updated",
            device_name=device_state.device_name,
            device_address=device_state.address,
            connection_key=device_state.connection_key,
            heartbeat=self._snapshot_heartbeat(device_state),
        )

    def _device_state_for_connection(self, connection_key: str) -> _DeviceState | None:
        for device_state in self._devices_by_name.values():
            if device_state.connection_key == connection_key:
                return device_state
        return None

    def _make_event(self, event_type: str, **payload: object) -> StateEvent:
        self._state_version += 1
        return {
            "type": event_type,
            "state_version": self._state_version,
            **payload,
        }
