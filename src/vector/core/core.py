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
from typing import Generic, TypeVar

from vector.core.models import (
    ControlBinding,
    ControlDefinition,
    ControlObservation,
    ControlStatus,
    ControlType,
    ControlValue,
    CoreChange,
    DispatchResult,
    SensorBinding,
    SensorDefinition,
    TelemetryBatch,
    TelemetryReading,
    TimestampSource,
)


logger = logging.getLogger(__name__)

TARE_SAMPLE_CAPACITY = 256
TARE_DEFAULT_SAMPLES = 16
TARE_SAMPLE_MAX_AGE_S = 2.0

ControlHandler = Callable[[ControlBinding, ControlValue], Awaitable[DispatchResult]]
_Notification = TypeVar("_Notification")
_Binding = TypeVar("_Binding", SensorBinding, ControlBinding)


class TareCaptureError(Exception):
    """Missing, ambiguous, or non-finite recent samples prevented tare capture."""

    def __init__(self, message: str, *, candidates: tuple[str, ...] = ()) -> None:
        super().__init__(message)
        self.candidates = candidates


class ControlValidationError(ValueError):
    """The requested value is invalid for at least one target; nothing was sent."""


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
        generation: int,
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
        self.generation = generation
        self.metadata = MappingProxyType(dict(metadata))
        self.sensors = tuple(SensorBinding(self, definition, i) for i, definition in enumerate(sensors))
        self.controls = tuple(ControlBinding(self, definition, i) for i, definition in enumerate(controls))
        self._sensors = {sensor.name: sensor for sensor in self.sensors}
        self._controls = {control.name.upper(): control for control in self.controls}
        self._accepted: dict[int, ControlObservation] = {}
        self._reported = {
            self._controls[name.upper()].id: ControlObservation(
                value=_validated_value(self._controls[name.upper()], observation.value) if observation.value is not None else None,
                timestamp=observation.timestamp,
                status=ControlStatus(observation.status) if observation.status is not None else None,
            )
            for name, observation in initial_controls.items()
        }
        self._control_handler = control_handler
        self._connected = True

    @property
    def connected(self) -> bool:
        return self._connected

    def sensor(self, name: str) -> SensorBinding | None:
        """Look up an exact name; repeated names select the last declaration."""
        return self._sensors.get(name)

    def control(self, name: str) -> ControlBinding | None:
        """Look up a name ignoring case; use ``controls`` to retain duplicate bindings."""
        return self._controls.get(name.upper())

    def accepted_control(self, control: ControlBinding) -> ControlObservation | None:
        return self._accepted.get(control.id)

    def reported_control(self, control: ControlBinding) -> ControlObservation | None:
        return self._reported.get(control.id)

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
        status: ControlStatus | str = ControlStatus.CONFIRMED,
        now: float | None = None,
    ) -> CoreChange | None:
        """Report hardware feedback, retaining the last known value on an error."""
        if not self._core._is_current(self):
            return None
        control = self._resolve_control(name)
        if control is None:
            return None
        status = ControlStatus(status)
        if status == ControlStatus.ERROR:
            previous = control.reported
            value = previous.value if previous is not None else None
        elif value is not None:
            value = _validated_value(control, value)
        self._reported[control.id] = ControlObservation(
            value=value,
            timestamp=time.monotonic() if now is None else now,
            status=status,
        )
        return self._core._changed(CoreChange("control.reported", source=self, control=control))

    def accept_control(self, name: str | ControlBinding, value: ControlValue, *, now: float | None = None) -> CoreChange | None:
        """Record provider acceptance, independently of the reported physical state."""
        if not self._core._is_current(self):
            return None
        control = self._resolve_control(name)
        if control is None:
            return None
        self._accepted[control.id] = ControlObservation(
            value=_validated_value(control, value),
            timestamp=time.monotonic() if now is None else now,
        )
        return self._core._changed(CoreChange("control.accepted", source=self, control=control))

    def close(self) -> CoreChange | None:
        """Disable this source's routing while retaining declarations and last state."""
        if not self._core._is_current(self):
            return None
        self._retire()
        return self._core._changed(CoreChange("source.closed", source=self))

    def _retire(self) -> None:
        self._connected = False
        self._control_handler = None
        self._core._discard_history(self)


class Core:
    """Shared resources and operations, with no dependency on a hardware protocol."""

    def __init__(self) -> None:
        self._sources: dict[tuple[str, str], Source] = {}
        self._next_generation = 0
        self._tares: dict[str, float] = {}
        self._history: dict[tuple[Source, str], _SampleBuffer] = {}
        self._sample_subscribers: list[_Subscription[TelemetryBatch]] = []
        self._change_subscribers: list[_Subscription[CoreChange]] = []

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
        control_names = {control.name.upper() for control in controls}
        if any(name.upper() not in control_names for name in initial_controls or {}):
            raise ValueError("Initial control observations must refer to declared controls.")
        self._next_generation += 1
        source = Source(
            self,
            provider,
            key,
            name=key if name is None else name,
            address=address,
            connection_key=connection_key or f"{provider}:{key}:{self._next_generation}",
            generation=self._next_generation,
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
        self._changed(CoreChange("source.registered", source=source))
        return source

    def source(self, provider: str, key: str) -> Source | None:
        return self._sources.get((provider, key))

    def sources(self, provider: str | None = None) -> tuple[Source, ...]:
        """Return current registrations, including descriptions of disconnected sources."""
        return tuple(source for source in self._sources.values() if provider is None or source.provider == provider)

    def sensors(self, name: str | None = None) -> tuple[SensorBinding, ...]:
        return tuple(sensor for source in self.sources() for sensor in source.sensors if name is None or sensor.name == name)

    def controls(self, name: str | None = None, *, provider: str | None = None) -> tuple[ControlBinding, ...]:
        return tuple(
            control
            for source in self.sources(provider)
            for control in source.controls
            if name is None or control.name.upper() == name.upper()
        )

    async def set_control(self, target: ControlBinding, value: ControlValue) -> DispatchResult:
        """Validate one explicit target and value, then report the provider's submission.

        An invalid target or value raises before anything is sent. An unavailable
        source or a failed handler returns a failure result. The handler's result
        stands even if its source closes while it awaits I/O: a command that was
        sent is reported as sent. Providers report acceptance and physical
        feedback separately.
        """
        source = target.source
        if source._core is not self or source._resolve_control(target) is not target:
            raise ControlValidationError("Control target does not belong to this core registration.")
        validated = _validated_value(target, value)
        handler = source._control_handler
        if not self._is_current(source) or handler is None:
            return DispatchResult(False, error="Control source is unavailable.", target=target)
        try:
            outcome = await handler(target, validated)
        except Exception as exc:
            return DispatchResult(False, error=str(exc), cause=exc, target=target)
        return replace(outcome, target=target)

    def subscribe_samples(self, callback: Callable[[TelemetryBatch], None]) -> Callable[[], None]:
        """Receive future batches inline; queue slow work and unsubscribe on teardown.

        No history is replayed. The returned unsubscribe function is idempotent.
        """
        return _subscribe(self._sample_subscribers, callback)

    def subscribe_changes(self, callback: Callable[[CoreChange], None]) -> Callable[[], None]:
        """Receive future catalog/state changes inline, with the same rules as samples."""
        return _subscribe(self._change_subscribers, callback)

    def tares(self) -> dict[str, float]:
        return dict(sorted(self._tares.items()))

    def set_tare(self, sensor_name: str, offset: float) -> CoreChange:
        """Share the exact-name offset across sources; apply it to subsequent readings."""
        if not math.isfinite(offset):
            raise ValueError("Tare offset must be finite.")
        self._tares[sensor_name] = offset
        return self._changed(CoreChange("tare.updated", sensor_name=sensor_name, offset=offset))

    def clear_tare(self, sensor_name: str) -> CoreChange | None:
        if self._tares.pop(sensor_name, None) is None:
            return None
        return self._changed(CoreChange("tare.cleared", sensor_name=sensor_name))

    def capture_tare_offset(
        self,
        sensor_name: str,
        *,
        device_name: str | None = None,
        samples: int = TARE_DEFAULT_SAMPLES,
    ) -> tuple[float, str, int]:
        """Compute ``(offset, source_name, count)`` from recent raw samples.

        ``device_name`` accepts a display name or ``provider:key`` to select a
        source. Ambiguous, stale, or non-finite captures raise TareCaptureError
        without changing the tare; use ``set_tare`` to apply a successful result.
        """
        if samples < 1:
            raise TareCaptureError("samples must be >= 1.")
        now = time.monotonic()
        candidates = [
            (source, buffer)
            for (source, name), buffer in self._history.items()
            if name == sensor_name
            and self._is_current(source)
            and device_name in (None, source.name, f"{source.provider}:{source.key}")
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
            raise TareCaptureError(message, candidates=names)
        source, buffer = candidates[0]
        window = list(buffer.values)[-samples:]
        offset = sum(window) / len(window)
        if not math.isfinite(offset):
            message = f"Cannot capture a tare for sensor {sensor_name!r}: recent readings produced a non-finite offset."
            raise TareCaptureError(message)
        return offset, source.name, len(window)

    def capture_tare(
        self,
        sensor_name: str,
        *,
        device_name: str | None = None,
        samples: int = TARE_DEFAULT_SAMPLES,
    ) -> tuple[float, str, int]:
        """Capture and apply an offset; callers needing separate events can use the two methods."""
        captured = self.capture_tare_offset(sensor_name, device_name=device_name, samples=samples)
        self.set_tare(sensor_name, captured[0])
        return captured

    def _is_current(self, source: Source) -> bool:
        return source.connected and self._sources.get((source.provider, source.key)) is source

    def _discard_history(self, source: Source) -> None:
        for binding in source.sensors:
            self._history.pop((source, binding.name), None)

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
            history_key = (source, definition.name)
            buffer = self._history.get(history_key)
            if buffer is None:
                buffer = _SampleBuffer()
                self._history[history_key] = buffer
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

    def _changed(self, change: CoreChange) -> CoreChange:
        _notify(self._change_subscribers, change)
        return change


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


@dataclass(slots=True, eq=False)
class _Subscription(Generic[_Notification]):
    # Identity equality keeps duplicate subscriptions independent, even for the same callback.
    callback: Callable[[_Notification], None]


def _subscribe(
    subscribers: list[_Subscription[_Notification]],
    callback: Callable[[_Notification], None],
) -> Callable[[], None]:
    subscription = _Subscription(callback)
    subscribers.append(subscription)

    def unsubscribe() -> None:
        if subscription in subscribers:
            subscribers.remove(subscription)

    return unsubscribe


def _notify(subscribers: list[_Subscription[_Notification]], value: _Notification) -> None:
    # Callbacks may unsubscribe during delivery; one broken consumer must not stop
    # the remaining consumers from receiving the same update.
    for subscription in tuple(subscribers):
        try:
            subscription.callback(value)
        except Exception:
            logger.exception("Core subscriber failed")
