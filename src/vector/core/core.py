"""One in-process catalog, measurement path, and control dispatcher.

The core uses direct calls on one asyncio loop. Services own tasks and connections;
subscriptions are synchronous so consumers explicitly own any buffering they need.
"""

# Source and Core implement one API together; their private methods stay internal.
# ruff: noqa: SLF001

from __future__ import annotations
import logging
import math
import time
from collections import deque
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import NamedTuple, TypeVar

from vector.core.models import (
    ControlBinding,
    ControlChanged,
    ControlDefinition,
    ControlObservation,
    ControlStatus,
    ControlType,
    ControlValue,
    CoreChange,
    SensorBinding,
    SensorDefinition,
    SourceChanged,
    TareChanged,
    TelemetryBatch,
    TelemetryReading,
    TimestampSource,
)


logger = logging.getLogger(__name__)

TARE_SAMPLE_CAPACITY = 256
TARE_DEFAULT_SAMPLES = 16
TARE_SAMPLE_MAX_AGE_S = 2.0

# Returns the provider's command ID, or None if it has none. Raise ControlValidationError
# to refuse a value without sending; raise anything else when sending failed.
ControlHandler = Callable[[ControlBinding, ControlValue], Awaitable[int | None]]
_Notification = TypeVar("_Notification")
_Binding = TypeVar("_Binding", SensorBinding, ControlBinding)


class TareCaptureError(Exception):
    """Missing, ambiguous, or non-finite recent samples prevented tare capture."""


class ControlValidationError(ValueError):
    """The requested value is invalid for at least one target; nothing was sent."""


class ControlDispatchError(Exception):
    """The command was not submitted: its source is unavailable or its provider failed.

    When the provider raised, that exception is chained as ``__cause__``.
    """


class TareCapture(NamedTuple):
    """A computed offset and where it came from; apply it with ``Core.set_tare``."""

    offset: float
    source_name: str
    sample_count: int


@dataclass(slots=True)
class _SampleBuffer:
    values: deque[float] = field(default_factory=lambda: deque(maxlen=TARE_SAMPLE_CAPACITY))
    last_updated_monotonic: float = 0.0


class Source:
    """A producer's registration handle. Keep this handle for every delayed update.

    ``(provider, key)`` is the stable identity; ``name`` is a display label.
    ``connection_key`` identifies this connection for transport diagnostics.
    Registering the same provider/key replaces this generation. Calls through a
    closed or replaced handle have no effect, including late hardware feedback.
    """

    def __init__(
        self,
        core: Core,
        provider: str,
        key: str,
        *,
        name: str,
        address: str,
        connection_key: str,
        sensors: Sequence[SensorDefinition],
        controls: Sequence[ControlDefinition],
        control_handler: ControlHandler | None,
        metadata: Mapping[str, object],
        initial_controls: Mapping[str, ControlObservation],
    ) -> None:
        self._core = core
        self.provider = provider
        self.key = key
        self.name = name
        self.address = address
        self.connection_key = connection_key
        self.metadata = MappingProxyType(dict(metadata))
        self.sensors = tuple(SensorBinding(self, definition, i) for i, definition in enumerate(sensors))
        self.controls = tuple(ControlBinding(self, definition, i) for i, definition in enumerate(controls))
        self._sensors = {sensor.name: sensor for sensor in self.sensors}
        self._controls = {control.name.upper(): control for control in self.controls}
        # Recent raw samples by sensor name, for tare capture while this source is current.
        self._history: dict[str, _SampleBuffer] = {}
        self._reported: dict[int, ControlObservation] = {}
        for control_name, observation in initial_controls.items():
            control = self._controls[control_name.upper()]
            value = observation.value if observation.value is None else _validated_value(control, observation.value)
            self._reported[control.id] = replace(observation, value=value)
        self._control_handler = control_handler
        self._connected = True

    @property
    def connected(self) -> bool:
        return self._connected

    def sensor(self, name: str) -> SensorBinding | None:
        """Look up an exact name; repeated names select the last declaration."""
        return self._sensors.get(name)

    def control(self, name: str) -> ControlBinding | None:
        """Look up a name ignoring case; control names are unique within a source."""
        return self._controls.get(name.upper())

    def _resolve_control(self, name: str | ControlBinding) -> ControlBinding | None:
        if isinstance(name, str):
            return self.control(name)
        return name if _owns(self.controls, name) else None

    def _resolve_sensor(self, name: str | SensorBinding) -> SensorBinding | None:
        if isinstance(name, str):
            return self.sensor(name)
        return name if _owns(self.sensors, name) else None

    def publish_samples(
        self,
        samples: Iterable[tuple[str | SensorBinding, float]],
        timestamp_s: float,
        *,
        timestamp_source: TimestampSource = "server_receive",
        timestamp_synced: bool = False,
    ) -> TelemetryBatch | None:
        """Publish raw physical-unit values with a server-monotonic timestamp.

        Do not subtract tare here; the core applies it once for every consumer.
        Use explicit sensor bindings when declaration names repeat.
        """
        return self._core._publish_samples(self, samples, timestamp_s, timestamp_source, timestamp_synced)

    def report_control(
        self,
        name: str | ControlBinding,
        value: ControlValue | None,
        *,
        status: ControlStatus = ControlStatus.CONFIRMED,
        now: float | None = None,
    ) -> None:
        """Report hardware feedback, retaining the last known value on an error.

        A reported value never raises: one that does not fit the declared type
        is recorded as an error so the operator sees the fault and the
        provider's feedback loop keeps running.
        """
        control = self._resolve_control(name)
        if control is None or not self._core._is_current(self):
            return
        if status != ControlStatus.ERROR and value is not None:
            try:
                value = _validated_value(control, value)
            except ControlValidationError:
                logger.exception("Source %r reported an invalid control value; recording an error.", self.name)
                status = ControlStatus.ERROR
        if status == ControlStatus.ERROR:
            previous = control.reported
            value = previous.value if previous is not None else None
        self._reported[control.id] = ControlObservation(
            value=value,
            timestamp=time.monotonic() if now is None else now,
            status=status,
        )
        self._core._changed(ControlChanged(control))

    def close(self) -> None:
        """Disable this source's routing while retaining declarations and last state."""
        if not self._core._is_current(self):
            return
        self._retire()
        self._core._changed(SourceChanged("closed", self))

    def _retire(self) -> None:
        self._connected = False
        self._control_handler = None
        self._history.clear()


class Core:
    """Shared resources and operations, with no dependency on a hardware protocol."""

    def __init__(self) -> None:
        self._sources: dict[tuple[str, str], Source] = {}
        self._next_generation = 0
        self._tares: dict[str, float] = {}
        self._sample_subscribers: list[Callable[[TelemetryBatch], None]] = []
        self._change_subscribers: list[Callable[[CoreChange], None]] = []

    def register_source(
        self,
        provider: str,
        key: str,
        *,
        name: str | None = None,
        address: str = "",
        connection_key: str | None = None,
        sensors: Sequence[SensorDefinition] = (),
        controls: Sequence[ControlDefinition] = (),
        control_handler: ControlHandler | None = None,
        metadata: Mapping[str, object] | None = None,
        initial_controls: Mapping[str, ControlObservation] | None = None,
    ) -> Source:
        """Declare a source, atomically replacing an older registration of its identity.

        Reuse ``(provider, key)`` on reconnect, then retain the returned handle for
        all publishing and feedback. The async handler receives the exact control
        binding and typed value; defaults describe controls but never actuate them.
        """
        names = [control.name.upper() for control in controls]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            message = f"Control names must be unique within a source, ignoring case: {', '.join(duplicates)}."
            raise ValueError(message)
        if any(name.upper() not in names for name in initial_controls or {}):
            raise ValueError("Initial control observations must refer to declared controls.")
        self._next_generation += 1
        source = Source(
            self,
            provider,
            key,
            name=key if name is None else name,
            address=address,
            connection_key=connection_key or f"{provider}:{key}:{self._next_generation}",
            sensors=sensors,
            controls=controls,
            control_handler=control_handler,
            metadata={} if metadata is None else metadata,
            initial_controls={} if initial_controls is None else initial_controls,
        )
        previous = self._sources.get((provider, key))
        if previous is not None:
            previous._retire()
        self._sources[provider, key] = source
        for control in source.controls:
            # Legal, since a command names its source; warn because recording
            # control columns are keyed by name alone and will conflate them.
            others = [
                f"{other.provider}:{other.key}"
                for other in self._sources.values()
                if other is not source and other.connected and other.control(control.name) is not None
            ]
            if others:
                logger.warning("Control %r on %s:%s is also declared by %s.", control.name, provider, key, ", ".join(others))
        self._changed(SourceChanged("registered", source))
        return source

    def source(self, provider: str, key: str) -> Source | None:
        return self._sources.get((provider, key))

    def sources(self, provider: str | None = None) -> tuple[Source, ...]:
        """Return current registrations, including descriptions of disconnected sources."""
        return tuple(source for source in self._sources.values() if provider is None or source.provider == provider)

    def sensors(self, name: str | None = None) -> tuple[SensorBinding, ...]:
        return tuple(sensor for source in self.sources() for sensor in source.sensors if name is None or sensor.name == name)

    async def set_control(self, target: ControlBinding, value: ControlValue) -> int | None:
        """Validate one explicit target and value, submit it, and return its command ID.

        Returns None when the provider has no command IDs. Raises
        ControlValidationError when the target or value is invalid, including a
        value the provider refuses; nothing was sent. Raises ControlDispatchError
        when the source is unavailable or the provider failed to send. A command
        that was sent is reported as sent even if its source closes while the
        handler awaits I/O. Providers report physical feedback separately.
        """
        source = target.source
        if source._core is not self or source._resolve_control(target) is not target:
            raise ControlValidationError("Control target does not belong to this core registration.")
        validated = _validated_value(target, value)
        handler = source._control_handler
        if not self._is_current(source) or handler is None:
            raise ControlDispatchError("Control source is unavailable.")
        try:
            return await handler(target, validated)
        except (ControlValidationError, ControlDispatchError):
            raise
        except Exception as exc:
            raise ControlDispatchError(str(exc) or type(exc).__name__) from exc

    def subscribe_samples(self, callback: Callable[[TelemetryBatch], None]) -> None:
        """Receive future batches inline for the core's lifetime; queue slow work. No history is replayed."""
        self._sample_subscribers.append(callback)

    def subscribe_changes(self, callback: Callable[[CoreChange], None]) -> None:
        """Receive future catalog/state changes inline, with the same rules as samples."""
        self._change_subscribers.append(callback)

    def tares(self) -> dict[str, float]:
        return dict(sorted(self._tares.items()))

    def set_tare(self, sensor_name: str, offset: float) -> None:
        """Share the exact-name offset across sources; apply it to subsequent readings."""
        if not math.isfinite(offset):
            raise ValueError("Tare offset must be finite.")
        self._tares[sensor_name] = offset
        self._changed(TareChanged(sensor_name, offset))

    def clear_tare(self, sensor_name: str) -> bool:
        """Remove an offset; return False if the sensor was not tared."""
        if self._tares.pop(sensor_name, None) is None:
            return False
        self._changed(TareChanged(sensor_name, None))
        return True

    def capture_tare_offset(
        self,
        sensor_name: str,
        *,
        device_name: str | None = None,
        samples: int = TARE_DEFAULT_SAMPLES,
    ) -> TareCapture:
        """Average recent raw samples from one source into a ``TareCapture``.

        ``device_name`` accepts a display name or ``provider:key`` to select a
        source. Ambiguous, stale, or non-finite captures raise TareCaptureError;
        use ``set_tare`` to apply a successful result.
        """
        if samples < 1:
            raise TareCaptureError("samples must be >= 1.")
        now = time.monotonic()
        candidates = [
            (source, buffer)
            for source in self._sources.values()
            if self._is_current(source)
            and device_name in (None, source.name, f"{source.provider}:{source.key}")
            and (buffer := source._history.get(sensor_name)) is not None
            and buffer.values
            and now - buffer.last_updated_monotonic <= TARE_SAMPLE_MAX_AGE_S
        ]
        if not candidates:
            scope = f" on {device_name}" if device_name is not None else ""
            message = f"No telemetry received for sensor {sensor_name!r}{scope} in the last {TARE_SAMPLE_MAX_AGE_S}s."
            raise TareCaptureError(message)
        if len(candidates) > 1:
            names = tuple(sorted(source.name for source, _ in candidates))
            if len(set(names)) != len(names):
                names = tuple(sorted(f"{source.provider}:{source.key}" for source, _ in candidates))
            message = f"Sensor {sensor_name!r} is reported by multiple devices ({', '.join(names)}); specify which to sample from."
            raise TareCaptureError(message)
        source, buffer = candidates[0]
        window = list(buffer.values)[-samples:]
        offset = sum(window) / len(window)
        if not math.isfinite(offset):
            message = f"Cannot capture a tare for sensor {sensor_name!r}: recent readings produced a non-finite offset."
            raise TareCaptureError(message)
        return TareCapture(offset, source.name, len(window))

    def _is_current(self, source: Source) -> bool:
        return source.connected and self._sources.get((source.provider, source.key)) is source

    def _publish_samples(
        self,
        source: Source,
        samples: Iterable[tuple[str | SensorBinding, float]],
        timestamp_s: float,
        timestamp_source: TimestampSource,
        timestamp_synced: bool,
    ) -> TelemetryBatch | None:
        if not self._is_current(source):
            return None
        # Resolve the whole batch before changing history or notifying consumers.
        resolved = []
        for name, raw_value in samples:
            binding = source._resolve_sensor(name)
            if binding is None:
                message = f"Unknown sensor {name!r} on source {source.name!r}."
                raise ValueError(message)
            resolved.append((binding, raw_value))
        now = time.monotonic()
        readings = []
        for binding, raw_value in resolved:
            definition = binding.definition
            buffer = source._history.get(definition.name)
            if buffer is None:
                buffer = _SampleBuffer()
                source._history[definition.name] = buffer
            buffer.values.append(raw_value)
            buffer.last_updated_monotonic = now
            tare = self._tares.get(definition.name, 0.0)
            readings.append(TelemetryReading(binding.id, definition.name, raw_value - tare, definition.unit, definition.group, tare))
        batch = TelemetryBatch(
            source_provider=source.provider,
            source_key=source.key,
            device_name=source.name,
            device_address=source.address,
            connection_key=source.connection_key,
            timestamp_s=timestamp_s,
            readings=tuple(readings),
            timestamp_source=timestamp_source,
            timestamp_synced=timestamp_synced,
        )
        _notify(self._sample_subscribers, batch)
        return batch

    def _changed(self, change: CoreChange) -> None:
        _notify(self._change_subscribers, change)


def _validated_value(target: ControlBinding, value: ControlValue) -> ControlValue:
    control_type = target.type
    valid = False
    if control_type == ControlType.BOOL:
        valid = isinstance(value, bool)
    elif control_type in (ControlType.UINT32, ControlType.INT32):
        # Range/encoding constraints belong to the provider, as before extraction.
        valid = type(value) is int
    elif control_type == ControlType.FLOAT32:
        valid = type(value) in (int, float)
        if valid:
            return float(value)
    if not valid:
        message = f"Invalid value {value!r} for {target.name!r} ({control_type})."
        raise ControlValidationError(message)
    return value


def _owns(bindings: tuple[_Binding, ...], binding: _Binding) -> bool:
    return 0 <= binding.id < len(bindings) and bindings[binding.id] is binding


def _notify(subscribers: list[Callable[[_Notification], None]], value: _Notification) -> None:
    # One broken consumer must not stop the remaining consumers from receiving the same update.
    for callback in subscribers:
        try:
            callback(value)
        except Exception:
            logger.exception("Core subscriber failed")
