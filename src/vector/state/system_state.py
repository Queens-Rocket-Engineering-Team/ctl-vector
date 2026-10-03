"""The existing GUI and recording views of the shared resource core.

The core owns definitions, control observations, and tares. This module
only gives those objects their established REST/WebSocket and recording shapes.
Transport diagnostics are supplied as read-only scalar views by their adapters.
"""

from __future__ import annotations
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any, NamedTuple

from vector.core import ChangeKind, ControlStatus


if TYPE_CHECKING:
    from collections.abc import Callable

    from vector.core import ControlBinding, ControlDefinition, ControlValue, Core, CoreChange, SensorDefinition, Source
    from vector.runtime.qlcp_state import CommandProjection

StateEvent = dict[str, object]


class TransportHealth(NamedTuple):
    """Live connection diagnostics an adapter samples for one source."""

    last_sync_time: float | None
    consecutive_misses: int


@dataclass(frozen=True, slots=True)
class KasaState:
    host: str
    alias: str
    model: str
    active: bool
    connected: bool


@dataclass(frozen=True, slots=True)
class RecordingSchema:
    """A momentary view of declarations, including disconnected sources."""

    sensors: tuple[SensorDefinition, ...]
    controls: tuple[ControlDefinition, ...]
    kasa: tuple[KasaState, ...]


@dataclass(slots=True)
class _SessionStateRecord:
    session_id: str
    name: str
    started_unix: float
    started_monotonic: float
    components: dict[str, str] = field(default_factory=dict)


def control_state_name(value: ControlValue | None) -> str | None:
    """Preserve the established display and CSV boolean spelling."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "OPEN" if value else "CLOSED"
    return str(value).upper()


def source_identity(source: Source) -> dict[str, str]:
    # Labels may repeat. Match sources by provider/key and use connection_key to
    # distinguish a reconnect from feedback belonging to an older registration.
    return {
        "source_provider": source.provider,
        "source_key": source.key,
        "connection_key": source.connection_key,
    }


class SystemState:
    """Versioned presentation of core resources and recording-session state.

    Core changes publish automatically once ``set_publisher`` is installed.
    Session and transport command methods return events for their existing callers
    to publish. Live transport diagnostics do not advance ``state_version``.
    """

    def __init__(self, *, core: Core) -> None:
        self.core = core
        self._session: _SessionStateRecord | None = None
        self._state_version = 0
        self._publisher: Callable[[StateEvent], None] | None = None
        self._command_view: CommandProjection | None = None
        self._health: Callable[[Source], TransportHealth | None] = lambda _source: None
        core.subscribe_changes(self._on_core_change)

    @property
    def state_version(self) -> int:
        return self._state_version

    def set_publisher(self, publisher: Callable[[StateEvent], None]) -> None:
        self._publisher = publisher

    def set_transport_views(
        self,
        *,
        commands: CommandProjection,
        health: Callable[[Source], TransportHealth | None],
    ) -> None:
        self._command_view = commands
        self._health = health

    def recording_schema(self) -> RecordingSchema:
        sources = self._ordinary_sources()
        return RecordingSchema(
            sensors=tuple(sensor.definition for source in sources for sensor in source.sensors),
            controls=tuple(control.definition for source in sources for control in source.controls),
            kasa=tuple(self._kasa_state(source) for source in self._kasa_sources()),
        )

    def control_states(self) -> dict[str, str | None]:
        states: dict[str, str | None] = {}
        for source in self._ordinary_sources():
            for control in source.controls:
                reported = control.reported
                states[control.name] = control_state_name(reported.value) if reported else None
        return states

    def kasa_active(self) -> dict[str, bool]:
        return {source.key: self._kasa_active(source) for source in self._kasa_sources()}

    def session(self) -> dict[str, Any] | None:
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
        self._session = _SessionStateRecord(
            session_id=session_id,
            name=name,
            started_unix=started_unix,
            started_monotonic=started_monotonic,
        )
        return self.make_event("session.started", session=self.session())

    def update_session_components(self, components: dict[str, str]) -> StateEvent | None:
        if self._session is None:
            return None
        self._session.components.update(components)
        return self.make_event("session.updated", session=self.session())

    def stop_session(self, *, stopped_unix: float, end_reason: str) -> StateEvent | None:
        if self._session is None:
            return None
        session_id = self._session.session_id
        self._session = None
        return self.make_event("session.stopped", session_id=session_id, stopped_unix=stopped_unix, end_reason=end_reason)

    def record_session_warning(self, warning: str, detail: str | None = None) -> StateEvent | None:
        if self._session is None:
            return None
        return self.make_event("session.warning", session_id=self._session.session_id, warning=warning, detail=detail)

    def snapshot(self) -> dict[str, Any]:
        return {
            "state_version": self._state_version,
            "devices": [self._snapshot_device(source) for source in self._ordinary_sources()],
            "kasa": [self._snapshot_kasa(source) for source in self._kasa_sources()],
            "commands": self._command_view.snapshot() if self._command_view else {"pending": [], "recent": []},
            "tares": self.core.tares(),
            "session": self.session(),
        }

    def _ordinary_sources(self) -> list[Source]:
        return sorted(
            (source for source in self.core.sources() if source.provider != "kasa"),
            key=lambda source: (source.name, source.provider, source.key),
        )

    def _kasa_sources(self) -> list[Source]:
        return sorted(self.core.sources(provider="kasa"), key=lambda source: source.key)

    @staticmethod
    def _kasa_active(source: Source) -> bool:
        power = source.control("power")
        reported = power.reported if power is not None else None
        return bool(reported.value) if reported else False

    def _kasa_state(self, source: Source) -> KasaState:
        return KasaState(
            host=source.key,
            alias=str(source.metadata.get("alias", "")),
            model=str(source.metadata.get("model", "")),
            active=self._kasa_active(source),
            connected=source.connected,
        )

    def _snapshot_kasa(self, source: Source) -> dict[str, Any]:
        return {**source_identity(source), **asdict(self._kasa_state(source))}

    def _snapshot_device(self, source: Source) -> dict[str, Any]:
        health = self._health(source)
        return {
            **source_identity(source),
            "name": source.name,
            "connected": source.connected,
            "address": source.address,
            "sensors": [
                {"id": sensor.id, "name": sensor.name, "group": sensor.group, "unit": sensor.unit}
                for sensor in source.sensors
            ],
            "controls": [self._snapshot_control(control) for control in source.controls],
            "last_sync_time": health.last_sync_time if health else None,
            "heartbeat": self.snapshot_heartbeat(source),
        }

    def _snapshot_control(self, control: ControlBinding) -> dict[str, Any]:
        reported = control.reported
        accepted = control.accepted
        pending_id = (
            self._command_view.pending_command_id(control.source.connection_key, control.id)
            if self._command_view
            else None
        )
        reported_status = reported.status if reported else None
        return {
            "id": control.id,
            "name": control.name,
            "group": control.group,
            "type": control.type.name,
            "unit": control.unit,
            "default_state": control_state_name(control.default),
            "reported_state": control_state_name(reported.value) if reported else None,
            "reported_status": reported_status,
            "reported_timestamp": reported.timestamp if reported else None,
            "accepted_state": control_state_name(accepted.value) if accepted else None,
            "accepted_timestamp": accepted.timestamp if accepted else None,
            "pending_command_id": pending_id,
            "settled": pending_id is None and reported_status is not ControlStatus.PENDING,
        }

    def snapshot_heartbeat(self, source: Source) -> dict[str, Any]:
        health = self._health(source)
        misses = health.consecutive_misses if health else 0
        if not source.connected:
            state = "disconnected"
        elif health is None:
            # ponytail: no liveness signal for non-QLCP sources; add a per-source
            # report interval to the core when the first polling provider lands.
            state = "unknown"
        else:
            state = "missed" if misses else "ok"
        return {"state": state, "consecutive_misses": misses}

    def _on_core_change(self, change: CoreChange) -> None:
        source = change.source
        kind = change.kind
        if kind in (ChangeKind.TARE_UPDATED, ChangeKind.TARE_CLEARED):
            payload: dict[str, object] = {"sensor_name": change.sensor_name}
            if kind is ChangeKind.TARE_UPDATED:
                payload["offset"] = change.offset
            event = self.make_event(kind.value, **payload)
        elif source is None:
            return
        elif source.provider == "kasa":
            event_type = {
                ChangeKind.SOURCE_REGISTERED: "kasa.registered",
                ChangeKind.SOURCE_CLOSED: "kasa.disconnected",
                ChangeKind.CONTROL_REPORTED: "kasa.updated",
            }.get(kind)
            if event_type is None:
                return
            event = self.make_event(event_type, kasa=self._snapshot_kasa(source))
        elif kind is ChangeKind.SOURCE_REGISTERED:
            event = self.make_event("device.registered", device=self._snapshot_device(source))
        elif kind is ChangeKind.SOURCE_CLOSED:
            event = self.make_event(
                "device.disconnected",
                device_name=source.name,
                device_address=source.address,
                **source_identity(source),
            )
        elif change.control is not None:
            control = change.control
            if kind is ChangeKind.CONTROL_ACCEPTED:
                event_type = "control.accepted"
            else:
                event_type = "control.error" if control.reported and control.reported.status is ControlStatus.ERROR else "control.updated"
            event = self.make_event(event_type, device_name=source.name, control=self._snapshot_control(control), **source_identity(source))
        else:
            return
        if self._publisher:
            self._publisher(event)

    def make_event(self, event_type: str, **payload: object) -> StateEvent:
        """Version an already serialized session or transport event."""
        self._state_version += 1
        return {"type": event_type, "state_version": self._state_version, **payload}
