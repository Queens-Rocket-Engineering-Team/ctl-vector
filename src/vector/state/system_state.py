"""The existing GUI and recording views of the shared resource core.

The core owns definitions, control observations, and tares. This module
only gives those objects their established REST/WebSocket and recording shapes.
Transport diagnostics are supplied as read-only scalar views by their adapters.
"""

from __future__ import annotations
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any, NamedTuple

from vector.core import ControlChanged, ControlStatus, SourceChanged, TareChanged


if TYPE_CHECKING:
    from collections.abc import Callable

    from vector.core import ControlBinding, ControlValue, Core, CoreChange, SensorDefinition, Source
    from vector.runtime.qlcp_state import CommandProjection, StreamSetting

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
    # Bindings rather than definitions: a column is named for its source as well as its control.
    controls: tuple[ControlBinding, ...]
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

    Every event is published through ``publish_event`` once ``set_publisher`` is
    installed: core changes, session changes, and the transport events the QLCP
    adapter projects. Live transport diagnostics do not advance ``state_version``.
    """

    def __init__(self, *, core: Core) -> None:
        self.core = core
        self._session: _SessionStateRecord | None = None
        self._state_version = 0
        self._publisher: Callable[[StateEvent], None] | None = None
        self._command_view: CommandProjection | None = None
        # One view per provider; each answers only for its own sources.
        self._health_views: list[Callable[[Source], TransportHealth | None]] = []
        self._stream: Callable[[], StreamSetting | None] = lambda: None
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
        stream: Callable[[], StreamSetting | None],
    ) -> None:
        self._command_view = commands
        self._stream = stream

    def add_health_view(self, view: Callable[[Source], TransportHealth | None]) -> None:
        """Register a provider's liveness view; it returns None for sources it does not own."""
        self._health_views.append(view)

    def _health(self, source: Source) -> TransportHealth | None:
        return next((health for view in self._health_views if (health := view(source)) is not None), None)

    def recording_schema(self) -> RecordingSchema:
        sources = self._ordinary_sources()
        return RecordingSchema(
            sensors=tuple(sensor.definition for source in sources for sensor in source.sensors),
            controls=tuple(control for source in sources for control in source.controls),
            kasa=tuple(self._kasa_state(source) for source in self._kasa_sources()),
        )

    def control_states(self) -> dict[tuple[str, str, str], str | None]:
        """Latest reported state of every control, keyed by (provider, key, control name)."""
        states: dict[tuple[str, str, str], str | None] = {}
        for source in self._ordinary_sources():
            for control in source.controls:
                reported = control.reported
                states[source.provider, source.key, control.name] = control_state_name(reported.value) if reported else None
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

    def start_session(self, *, session_id: str, name: str, started_unix: float, started_monotonic: float) -> None:
        self._session = _SessionStateRecord(
            session_id=session_id,
            name=name,
            started_unix=started_unix,
            started_monotonic=started_monotonic,
        )
        self.publish_event("session.started", session=self.session())

    def update_session_components(self, components: dict[str, str]) -> None:
        if self._session is None:
            return
        self._session.components.update(components)
        self.publish_event("session.updated", session=self.session())

    def stop_session(self, *, stopped_unix: float, end_reason: str) -> None:
        if self._session is None:
            return
        session_id = self._session.session_id
        self._session = None
        self.publish_event("session.stopped", session_id=session_id, stopped_unix=stopped_unix, end_reason=end_reason)

    def snapshot(self) -> dict[str, Any]:
        return {
            "state_version": self._state_version,
            "devices": [self._snapshot_device(source) for source in self._ordinary_sources()],
            "kasa": [self._snapshot_kasa(source) for source in self._kasa_sources()],
            "commands": self._command_view.snapshot() if self._command_view else {"pending": [], "recent": []},
            "stream": asdict(stream) if (stream := self._stream()) is not None else None,
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
            "pending_command_id": pending_id,
            "settled": pending_id is None and reported_status is not ControlStatus.PENDING,
        }

    def snapshot_heartbeat(self, source: Source) -> dict[str, Any]:
        health = self._health(source)
        misses = health.consecutive_misses if health else 0
        if not source.connected:
            state = "disconnected"
        elif health is None:
            # The provider registered no health view, so nothing has verified this source is alive.
            state = "unknown"
        else:
            state = "missed" if misses else "ok"
        return {"state": state, "consecutive_misses": misses}

    def _on_core_change(self, change: CoreChange) -> None:
        match change:
            case TareChanged(sensor_name=sensor_name, offset=None):
                self.publish_event("tare.cleared", sensor_name=sensor_name)
            case TareChanged(sensor_name=sensor_name, offset=offset):
                self.publish_event("tare.updated", sensor_name=sensor_name, offset=offset)
            case SourceChanged(kind=kind, source=source) if source.provider == "kasa":
                event_type = "kasa.registered" if kind == "registered" else "kasa.disconnected"
                self.publish_event(event_type, kasa=self._snapshot_kasa(source))
            case ControlChanged(control=control) if control.source.provider == "kasa":
                self.publish_event("kasa.updated", kasa=self._snapshot_kasa(control.source))
            case SourceChanged(kind="registered", source=source):
                self.publish_event("device.registered", device=self._snapshot_device(source))
            case SourceChanged(kind="closed", source=source):
                self.publish_event(
                    "device.disconnected",
                    device_name=source.name,
                    device_address=source.address,
                    **source_identity(source),
                )
            case ControlChanged(control=control):
                event_type = "control.error" if control.reported and control.reported.status is ControlStatus.ERROR else "control.updated"
                source = control.source
                self.publish_event(event_type, device_name=source.name, control=self._snapshot_control(control), **source_identity(source))

    def publish_event(self, event_type: str, **payload: object) -> None:
        """Version an already serialized event and hand it to the publisher, if one is installed."""
        self._state_version += 1
        if self._publisher:
            self._publisher({"type": event_type, "state_version": self._state_version, **payload})
