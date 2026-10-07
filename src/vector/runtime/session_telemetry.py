"""Server-side telemetry recording: one CSV row per ingested batch.

The format follows the client recorder this replaces, so existing analysis scripts keep
working: alphabetically ordered blocks, ``NAME [unit]`` sensor headers, an empty cell for
a sensor absent from a batch. Control columns deliberately differ -- see `build_columns`.
"""

from __future__ import annotations
import csv
import io
import logging
import os
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, TextIO

from vector.core import ControlType


if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence, Set
    from pathlib import Path

    from vector.core import TelemetryBatch, TelemetryReading
    from vector.state.system_state import RecordingSchema, SystemState


class KasaEntry(Protocol):
    """The parts of a Kasa outlet's state a column key is derived from."""

    @property
    def host(self) -> str: ...

    @property
    def alias(self) -> str: ...


logger = logging.getLogger(__name__)

# A megabyte of buffering keeps almost every row write to a memcpy, which matters
# because rows are written synchronously from the UDP ingest loop.
WRITE_BUFFER_BYTES = 1 << 20

# Relays are wired normally-closed, so CLOSED is the energized state and reads as 1.
# Every other boolean, such as a solenoid valve or a plug, reads 1 when OPEN/true.
RELAY_GROUP = "relay"
SOURCE_COLUMNS = ("source_provider", "source_key")

_NON_ALNUM = re.compile(r"[^A-Za-z0-9]")
_NON_ALNUM_LOWER = re.compile(r"[^a-z0-9]+")


def _slug(text: str) -> str:
    return _NON_ALNUM_LOWER.sub("_", text.strip().lower()).strip("_")


def kasa_column_keys(kasa: Sequence[KasaEntry]) -> dict[str, str]:
    """Map Kasa host -> column key, sanitizing aliases and de-duplicating collisions.

    Two outlets sharing an alias would otherwise collapse into one column, so the
    second and later get a numeric suffix in iteration order.
    """
    keys: dict[str, str] = {}
    used: set[str] = set()
    for entry in kasa:
        base = _NON_ALNUM.sub("_", entry.alias or entry.host)
        key = base
        suffix = 2
        while key in used:
            key = f"{base}_{suffix}"
            suffix += 1
        used.add(key)
        keys[entry.host] = key
    return keys


@dataclass(frozen=True, slots=True)
class ColumnPlan:
    """The frozen column layout of one recording.

    Fixed when recording starts so that a mid-session change -- a renamed Kasa alias, a
    device reconnecting -- can never shift columns underneath rows already written.
    """

    sensor_names: tuple[str, ...]
    # (column name, control name, is_relay, is_bool), in output order.
    controls: tuple[tuple[str, str, bool, bool], ...]
    # (column name, kasa host), in output order.
    kasa: tuple[tuple[str, str], ...]
    header: str
    # Derived once here rather than per batch: every ingested batch tests its sensors
    # against this, on the UDP loop.
    sensor_name_set: frozenset[str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "sensor_name_set", frozenset(self.sensor_names))

    @property
    def column_names(self) -> tuple[str, ...]:
        return (
            "device_timestamp", "source", *self.sensor_names,
            *(name for name, _, _, _ in self.controls), *(name for name, _ in self.kasa),
            *SOURCE_COLUMNS,
        )


def build_columns(schema: RecordingSchema, *, readings: Sequence[TelemetryReading] = ()) -> ColumnPlan:
    """Derive the column layout from a device/Kasa schema.

    Sensor and Kasa columns match the client recorder exactly. Control columns do not:
    that recorder had only ``valve_``/``relay_`` buckets and sorted every non-``AV*``
    control into ``relay_``, which filed the analog heater under a boolean column that
    could only ever read 0. Here the prefix is the control's declared QLCP group and
    non-boolean controls carry their actual value, so nothing is silently discarded.
    A current batch can fill gaps in a stale schema snapshot when opening a new file.
    """
    units = {sensor.name: sensor.unit for sensor in schema.sensors}
    for reading in readings:
        units.setdefault(reading.sensor_name, reading.unit)
    sensor_names = tuple(sorted(units))

    controls = tuple(
        (
            f"{_slug(control.group)}_{control.name}",
            control.name,
            control.group.strip().lower() == RELAY_GROUP,
            control.type == ControlType.BOOL,
        )
        # Group first, then control name, so the layout is one rule end to end.
        for control in sorted(schema.controls, key=lambda item: (_slug(item.group), item.name))
    )
    # The same control name on two sources is two controls, but cells are looked up
    # by name alone, so both columns would carry whichever source was listed last.
    names = [control.name for control in schema.controls]
    shared = sorted({name for name in names if names.count(name) > 1})
    if shared:
        logger.warning("Controls declared by more than one source record one source's state: %s", ", ".join(shared))

    kasa_keys = kasa_column_keys(schema.kasa)
    kasa = tuple(sorted(((f"kasa_{key}", host) for host, key in kasa_keys.items()), key=lambda item: item[0]))

    columns = [
        "device_timestamp", "source",
        *(f"{name} [{units[name]}]" if units[name] else name for name in sensor_names),
        *(name for name, _, _, _ in controls),
        *(name for name, _ in kasa),
        # Append identity so existing sensor/control column positions stay stable.
        *SOURCE_COLUMNS,
    ]
    header = io.StringIO(newline="")
    csv.writer(header, lineterminator="\n").writerow(columns)

    return ColumnPlan(sensor_names=sensor_names, controls=controls, kasa=kasa, header=header.getvalue())


def _control_cell(state: str | None, *, is_relay: bool, is_bool: bool) -> str:
    if not is_bool:
        # Analog control: record the setpoint, not a meaningless bit.
        if state is None:
            return ""
        try:
            return f"{float(state):.4f}"
        except ValueError:
            return ""
    if is_relay:
        return "1" if state == "CLOSED" else "0"
    return "1" if state == "OPEN" else "0"


@dataclass(slots=True)
class SessionTelemetryWriter:
    """Writes one recording's telemetry CSV.

    ``write_batch`` runs inline on the event loop and buffers normal row writes;
    a newly seen source may open a late file. A caller flushes periodically to
    bound loss on an abrupt exit.
    """

    path: Path
    plan: ColumnPlan
    _state: SystemState
    _handle: TextIO = field(repr=False)
    _writerow: Callable[[Iterable[str]], object] = field(init=False, repr=False)
    rows: int = 0
    _dirty: bool = False
    # Stable source identity -> files with frozen schemas. Retain every generation
    # for flushing and metadata; a reconnect can add columns more than once.
    _late: dict[tuple[str, str], list[SessionTelemetryWriter]] = field(default_factory=dict, repr=False)
    _accepts_late: bool = True

    def __post_init__(self) -> None:
        # Labels and provider keys are arbitrary strings, so rows need CSV quoting.
        self._writerow = csv.writer(self._handle, lineterminator="\n").writerow

    @classmethod
    def open(cls, path: Path, state: SystemState, plan: ColumnPlan, *, accepts_late: bool = True) -> SessionTelemetryWriter:
        path.parent.mkdir(parents=True, exist_ok=True)
        # newline="\n" so the file is byte-identical regardless of host platform.
        handle = path.open("w", encoding="utf-8", newline="\n", buffering=WRITE_BUFFER_BYTES)
        handle.write(plan.header)
        return cls(path=path, plan=plan, _state=state, _handle=handle, _accepts_late=accepts_late)

    def write_batch(self, batch: TelemetryBatch) -> None:
        values = {reading.sensor_name: reading.value for reading in batch.readings}

        if not values.keys() <= self.plan.sensor_name_set:
            if not self._accepts_late:
                raise ValueError("Late writer does not cover the incoming sensor columns")
            if self.rows == 0:
                # Nothing has been written yet, so the header is not yet committed to.
                # Covers starting a recording before the stand is powered on, which
                # would otherwise send every row to a "late" file.
                self._rebuild_header(batch)
            else:
                self._late_writer_for(batch, values.keys()).write_batch(batch)
                return

        control_states = self._state.control_states()
        kasa_active = self._state.kasa_active()

        cells = [f"{batch.timestamp_s:.4f}", batch.source_name]
        # A sensor missing from this batch leaves an empty cell rather than a zero, so a
        # gap is distinguishable from a real reading of zero.
        cells += [f"{values[name]:.4f}" if name in values else "" for name in self.plan.sensor_names]
        cells += [
            _control_cell(control_states.get(control_name), is_relay=is_relay, is_bool=is_bool)
            for _, control_name, is_relay, is_bool in self.plan.controls
        ]
        cells += ["1" if kasa_active.get(host) else "0" for _, host in self.plan.kasa]
        cells += [batch.source_provider, batch.source_key]

        self._writerow(cells)
        self.rows += 1
        self._dirty = True

    def _rebuild_header(self, batch: TelemetryBatch) -> None:
        """Re-derive the columns from the current schema, before any row exists.

        Only safe while no data row has been written; afterwards the header is frozen
        and a newly seen device gets its own file instead.
        """
        self.plan = build_columns(self._state.recording_schema(), readings=batch.readings)
        self._handle.seek(0)
        self._handle.truncate()
        self._handle.write(self.plan.header)
        self._dirty = True

    def _late_writer_for(self, batch: TelemetryBatch, sensor_names: Set[str]) -> SessionTelemetryWriter:
        """Reuse a compatible file, or open another without rewriting recorded rows."""
        source_id = (batch.source_provider, batch.source_key)
        writers = self._late.setdefault(source_id, [])
        for existing in reversed(writers):
            if sensor_names <= existing.plan.sensor_name_set:
                return existing

        file_count = sum(len(files) for files in self._late.values())
        suffix = f"_{file_count + 1}" if file_count else ""
        path = self.path.with_name(f"{self.path.stem}_late{suffix}{self.path.suffix}")
        logger.warning("Source %r needs additional recording columns; its telemetry is going to %s", source_id, path.name)
        # Keep fallback files flat: this parent owns routing and checks coverage on
        # every batch, including reconnects with changed sensor declarations.
        plan = build_columns(self._state.recording_schema(), readings=batch.readings)
        writer = SessionTelemetryWriter.open(path, self._state, plan, accepts_late=False)
        writers.append(writer)
        return writer

    @property
    def late_files(self) -> tuple[str, ...]:
        return tuple(sorted(writer.path.name for files in self._late.values() for writer in files))

    def flush_if_dirty(self) -> None:
        for files in self._late.values():
            for writer in files:
                writer.flush_if_dirty()
        if not self._dirty:
            return
        self._handle.flush()
        self._dirty = False

    def close(self) -> None:
        for files in self._late.values():
            for writer in files:
                writer.close()
        try:
            self._handle.flush()
            os.fsync(self._handle.fileno())
        finally:
            self._handle.close()
        self._dirty = False


class TelemetrySessionPublisher:
    """Routes ingested telemetry into the active recording, if there is one.

    Attaching and detaching happen on the event loop inside the non-awaiting parts of
    session start and stop, and `publish_batch` runs on that same loop from the UDP
    listener. There is no suspension point between attach and the first write, so the
    swap is atomic with respect to ingest and needs no lock.
    """

    __slots__ = ("_writer",)

    def __init__(self) -> None:
        self._writer: SessionTelemetryWriter | None = None

    def attach(self, writer: SessionTelemetryWriter) -> None:
        self._writer = writer

    def detach(self) -> None:
        self._writer = None

    def publish_batch(self, batch: TelemetryBatch) -> None:
        writer = self._writer
        if writer is None:
            return
        writer.write_batch(batch)
