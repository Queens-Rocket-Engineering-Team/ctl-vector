from __future__ import annotations
import csv
import io
from dataclasses import dataclass
from typing import TYPE_CHECKING

from vector.core import ControlDefinition, ControlType, SensorDefinition, TelemetryBatch, TelemetryReading
from vector.runtime.session_telemetry import (
    SessionTelemetryWriter,
    TelemetrySessionPublisher,
    build_columns,
    kasa_column_keys,
)


if TYPE_CHECKING:
    from pathlib import Path

    import pytest


@dataclass
class _Kasa:
    host: str
    alias: str
    active: bool = False


@dataclass
class _Schema:
    sensors: tuple[SensorDefinition, ...] = ()
    controls: tuple[ControlDefinition, ...] = ()
    kasa: tuple[_Kasa, ...] = ()


class _FakeState:
    """Stands in for SystemState, exposing only what the writer reads."""

    def __init__(self, schema: _Schema, controls: dict[str, str | None] | None = None, kasa: dict[str, bool] | None = None) -> None:
        self.schema = schema
        self._controls = controls or {}
        self._kasa = kasa or {}

    def recording_schema(self) -> _Schema:
        return self.schema

    def control_states(self) -> dict[str, str | None]:
        return self._controls

    def kasa_active(self) -> dict[str, bool]:
        return self._kasa


def _sensor(name: str, unit: str = "PSI") -> SensorDefinition:
    return SensorDefinition(name=name, group="pressure_transducer", unit=unit)


def _control(name: str, group: str, control_type: ControlType = ControlType.BOOL) -> ControlDefinition:
    return ControlDefinition(name=name, group=group, type=control_type, default=False if control_type == ControlType.BOOL else 0)


def _batch(
    values: dict[str, float], device_name: str = "MockDevice", timestamp_s: float = 236711.7952,
    *, source_provider: str = "test", source_key: str | None = None,
) -> TelemetryBatch:
    return TelemetryBatch(
        source_provider=source_provider,
        source_key=source_key if source_key is not None else device_name,
        source_name=device_name,
        source_address="10.0.0.1",
        connection_key="10.0.0.1:1",
        timestamp_s=timestamp_s,
        timestamp_source="server_receive",
        timestamp_synced=False,
        readings=tuple(
            TelemetryReading(sensor_id=index, sensor_name=name, value=value, unit="PSI", group="pressure_transducer")
            for index, (name, value) in enumerate(values.items())
        ),
    )


def _write(tmp_path: Path, state: _FakeState, batches: list[TelemetryBatch]) -> SessionTelemetryWriter:
    plan = build_columns(state.recording_schema())  # type: ignore[arg-type]
    writer = SessionTelemetryWriter.open(tmp_path / "telemetry.csv", state, plan)  # type: ignore[arg-type]
    for batch in batches:
        writer.write_batch(batch)
    writer.close()
    return writer


# ---------------------------------------------------------------------------
# Header layout
# ---------------------------------------------------------------------------


def test_header_orders_blocks_and_annotates_units() -> None:
    schema = _Schema(
        sensors=(_sensor("TC101", "C"), _sensor("PT101", "PSI")),
        controls=(_control("AV101", "valve"), _control("SAFE24", "relay"), _control("HEATER1", "heater", ControlType.FLOAT32)),
        kasa=(_Kasa("10.0.0.5", "Pump"),),
    )

    plan = build_columns(schema)  # type: ignore[arg-type]

    assert plan.header == (
        "device_timestamp,source,PT101 [PSI],TC101 [C],heater_HEATER1,relay_SAFE24,valve_AV101,kasa_Pump,source_provider,source_key\n"
    )
    assert plan.column_names == (
        "device_timestamp", "source", "PT101", "TC101", "heater_HEATER1", "relay_SAFE24", "valve_AV101", "kasa_Pump",
        "source_provider", "source_key",
    )


def test_sensor_columns_sort_by_raw_name_not_by_the_annotated_label() -> None:
    # "PT1 [PSI]" < "PT1B [PSI]" only holds if the sort happens before the unit is
    # appended -- comparing the labels puts the space-bearing name first either way,
    # so use names where the two orderings genuinely disagree.
    schema = _Schema(sensors=(_sensor("PT1B", "PSI"), _sensor("PT1", "PSI")))

    plan = build_columns(schema)  # type: ignore[arg-type]

    assert plan.sensor_names == ("PT1", "PT1B")
    assert plan.header == "device_timestamp,source,PT1 [PSI],PT1B [PSI],source_provider,source_key\n"


def test_sensor_without_a_unit_keeps_a_bare_name() -> None:
    plan = build_columns(_Schema(sensors=(_sensor("GPSFIX", ""),)))  # type: ignore[arg-type]

    assert plan.header == "device_timestamp,source,GPSFIX,source_provider,source_key\n"


def test_empty_blocks_are_omitted_entirely() -> None:
    plan = build_columns(_Schema(controls=(_control("AV101", "valve"),)))  # type: ignore[arg-type]

    assert plan.header == "device_timestamp,source,valve_AV101,source_provider,source_key\n"


def test_header_degrades_to_the_prefix_when_nothing_is_known() -> None:
    assert build_columns(_Schema()).header == "device_timestamp,source,source_provider,source_key\n"  # type: ignore[arg-type]


def test_control_group_becomes_the_column_prefix() -> None:
    schema = _Schema(controls=(_control("HEATER1", "heater", ControlType.FLOAT32), _control("IGNRUN", "relay"), _control("AV205", "valve")))

    plan = build_columns(schema)  # type: ignore[arg-type]

    assert [name for name, _, _, _ in plan.controls] == ["heater_HEATER1", "relay_IGNRUN", "valve_AV205"]


def test_a_control_name_shared_by_two_sources_is_warned_about_once(monkeypatch: pytest.MonkeyPatch) -> None:
    warnings: list[tuple[object, ...]] = []
    monkeypatch.setattr("vector.runtime.session_telemetry.logger.warning", lambda _message, *args: warnings.append(args))
    schema = _Schema(controls=(_control("AV101", "valve"), _control("HTR", "heater"), _control("AV101", "valve")))

    plan = build_columns(schema)  # type: ignore[arg-type]

    assert [name for name, _, _, _ in plan.controls] == ["heater_HTR", "valve_AV101", "valve_AV101"]
    assert warnings == [("AV101",)]


def test_control_group_with_punctuation_is_sanitized() -> None:
    plan = build_columns(_Schema(controls=(_control("X1", "Fill / Vent"),)))  # type: ignore[arg-type]

    assert [name for name, _, _, _ in plan.controls] == ["fill_vent_X1"]


def test_kasa_keys_are_sanitized_and_deduplicated() -> None:
    keys = kasa_column_keys([_Kasa("10.0.0.5", "Pump A"), _Kasa("10.0.0.6", "Pump-A"), _Kasa("10.0.0.7", "")])  # type: ignore[arg-type]

    assert keys == {"10.0.0.5": "Pump_A", "10.0.0.6": "Pump_A_2", "10.0.0.7": "10_0_0_7"}


# ---------------------------------------------------------------------------
# Row contents
# ---------------------------------------------------------------------------


def test_row_formats_timestamp_and_values_to_four_decimals(tmp_path: Path) -> None:
    state = _FakeState(_Schema(sensors=(_sensor("PT101"), _sensor("PT201", "PSI"))))

    _write(tmp_path, state, [_batch({"PT101": 20.50749, "PT201": -4.22671})])

    rows = (tmp_path / "telemetry.csv").read_text().splitlines()
    assert rows[1] == "236711.7952,MockDevice,20.5075,-4.2267,test,MockDevice"


def test_missing_sensor_leaves_an_empty_cell(tmp_path: Path) -> None:
    state = _FakeState(_Schema(sensors=(_sensor("PT101"), _sensor("PT201", "PSI"))))

    _write(tmp_path, state, [_batch({"PT101": 1.0})])

    assert (tmp_path / "telemetry.csv").read_text().splitlines()[1] == "236711.7952,MockDevice,1.0000,,test,MockDevice"


def test_source_column_carries_the_device_name(tmp_path: Path) -> None:
    state = _FakeState(_Schema(sensors=(_sensor("PT101"),)))

    _write(tmp_path, state, [_batch({"PT101": 1.0}, device_name="Chimera")])

    assert (tmp_path / "telemetry.csv").read_text().splitlines()[1].split(",")[1] == "Chimera"


def test_only_relays_read_one_when_closed(tmp_path: Path) -> None:
    schema = _Schema(controls=(_control("AV101", "solenoid"), _control("SAFE24", "relay"), _control("power", "power")))
    state = _FakeState(schema, controls={"AV101": "OPEN", "SAFE24": "OPEN", "power": "OPEN"})

    _write(tmp_path, state, [_batch({})])

    lines = (tmp_path / "telemetry.csv").read_text().splitlines()
    # Columns are ordered by group: power, relay, solenoid.
    assert lines[0] == "device_timestamp,source,power_power,relay_SAFE24,solenoid_AV101,source_provider,source_key"
    # Same reported state, one opposite bit: relays are wired normally-closed, so
    # CLOSED is their energized state. Everything else reads 1 for OPEN/true.
    assert lines[1] == "236711.7952,MockDevice,1,0,1,test,MockDevice"


def test_relay_reads_one_when_closed(tmp_path: Path) -> None:
    state = _FakeState(_Schema(controls=(_control("SAFE24", "relay"),)), controls={"SAFE24": "CLOSED"})

    _write(tmp_path, state, [_batch({})])

    assert (tmp_path / "telemetry.csv").read_text().splitlines()[1] == "236711.7952,MockDevice,1,test,MockDevice"


def test_unreported_boolean_control_reads_zero(tmp_path: Path) -> None:
    state = _FakeState(_Schema(controls=(_control("AV101", "valve"),)), controls={"AV101": None})

    _write(tmp_path, state, [_batch({})])

    assert (tmp_path / "telemetry.csv").read_text().splitlines()[1] == "236711.7952,MockDevice,0,test,MockDevice"


def test_analog_control_records_its_setpoint_rather_than_a_bit(tmp_path: Path) -> None:
    schema = _Schema(controls=(_control("HEATER1", "heater", ControlType.FLOAT32),))
    state = _FakeState(schema, controls={"HEATER1": "50.5"})

    _write(tmp_path, state, [_batch({})])

    assert (tmp_path / "telemetry.csv").read_text().splitlines()[1] == "236711.7952,MockDevice,50.5000,test,MockDevice"


def test_unreported_analog_control_leaves_an_empty_cell(tmp_path: Path) -> None:
    schema = _Schema(controls=(_control("HEATER1", "heater", ControlType.FLOAT32),))
    state = _FakeState(schema, controls={"HEATER1": None})

    _write(tmp_path, state, [_batch({})])

    assert (tmp_path / "telemetry.csv").read_text().splitlines()[1] == "236711.7952,MockDevice,,test,MockDevice"


def test_kasa_bit_follows_the_outlet_power_state(tmp_path: Path) -> None:
    state = _FakeState(_Schema(kasa=(_Kasa("10.0.0.5", "Pump"),)), kasa={"10.0.0.5": True})

    _write(tmp_path, state, [_batch({})])

    assert (tmp_path / "telemetry.csv").read_text().splitlines()[1] == "236711.7952,MockDevice,1,test,MockDevice"


def test_a_comma_in_a_sensor_name_is_quoted() -> None:
    plan = build_columns(_Schema(sensors=(_sensor("PT,101"),)))  # type: ignore[arg-type]

    assert list(csv.reader(io.StringIO(plan.header))) == [[
        "device_timestamp", "source", "PT,101 [PSI]", "source_provider", "source_key",
    ]]


def test_source_label_and_identity_are_csv_quoted(tmp_path: Path) -> None:
    state = _FakeState(_Schema(sensors=(_sensor("PT101"),)))
    _write(tmp_path, state, [_batch(
        {"PT101": 1.0}, device_name='Pad, "north"\nantenna',
        source_provider="wireless,poller", source_key='radio,"one"\nlink',
    )])

    with (tmp_path / "telemetry.csv").open(newline="") as handle:
        assert list(csv.DictReader(handle)) == [{
            "device_timestamp": "236711.7952", "source": 'Pad, "north"\nantenna', "PT101 [PSI]": "1.0000",
            "source_provider": "wireless,poller", "source_key": 'radio,"one"\nlink',
        }]


# ---------------------------------------------------------------------------
# Devices that appear after recording started
# ---------------------------------------------------------------------------


def test_a_device_appearing_mid_recording_gets_its_own_file(tmp_path: Path) -> None:
    known = _Schema(sensors=(_sensor("PT101"),))
    state = _FakeState(known)
    plan = build_columns(known)  # type: ignore[arg-type]
    writer = SessionTelemetryWriter.open(tmp_path / "telemetry.csv", state, plan)  # type: ignore[arg-type]

    writer.write_batch(_batch({"PT101": 1.0}))
    # A device connects that was unknown when the header was written.
    state.schema = _Schema(sensors=(_sensor("PT101"), _sensor("GPSALT", "m")))
    writer.write_batch(_batch({"GPSALT": 100.0}, device_name="Chimera"))
    writer.close()

    assert writer.late_files == ("telemetry_late.csv",)
    # The primary file keeps its original columns and only its own device's rows.
    assert (tmp_path / "telemetry.csv").read_text() == (
        "device_timestamp,source,PT101 [PSI],source_provider,source_key\n236711.7952,MockDevice,1.0000,test,MockDevice\n"
    )
    late = (tmp_path / "telemetry_late.csv").read_text().splitlines()
    assert late[0] == "device_timestamp,source,GPSALT [m],PT101 [PSI],source_provider,source_key"
    assert late[1] == "236711.7952,Chimera,100.0000,,test,Chimera"


def test_a_second_late_device_gets_a_numbered_file(tmp_path: Path) -> None:
    state = _FakeState(_Schema(sensors=(_sensor("PT101"),)))
    plan = build_columns(state.recording_schema())  # type: ignore[arg-type]
    writer = SessionTelemetryWriter.open(tmp_path / "telemetry.csv", state, plan)  # type: ignore[arg-type]

    writer.write_batch(_batch({"PT101": 1.0}))  # commits the header
    state.schema = _Schema(sensors=(_sensor("PT101"), _sensor("GPSALT", "m"), _sensor("BATV", "V")))
    writer.write_batch(_batch({"GPSALT": 1.0}, device_name="Chimera"))
    writer.write_batch(_batch({"BATV": 2.0}, device_name="Beacon"))
    writer.close()

    assert writer.late_files == ("telemetry_late.csv", "telemetry_late_2.csv")


def test_columns_are_re_derived_if_a_device_appears_before_the_first_row(tmp_path: Path) -> None:
    # Starting a recording before the stand is powered on must not banish every row to a
    # "late" file; until a row exists the header is not yet committed to.
    state = _FakeState(_Schema())
    writer = SessionTelemetryWriter.open(tmp_path / "telemetry.csv", state, build_columns(state.recording_schema()))  # type: ignore[arg-type]

    state.schema = _Schema(sensors=(_sensor("PT101"),))
    writer.write_batch(_batch({"PT101": 1.0}))
    writer.close()

    assert writer.late_files == ()
    assert (tmp_path / "telemetry.csv").read_text() == (
        "device_timestamp,source,PT101 [PSI],source_provider,source_key\n236711.7952,MockDevice,1.0000,test,MockDevice\n"
    )


def test_late_columns_include_batch_readings_missing_from_the_schema_snapshot(tmp_path: Path) -> None:
    state = _FakeState(_Schema(sensors=(_sensor("PT101"),)))
    writer = _write(tmp_path, state, [
        _batch({"PT101": 1.0}),
        _batch({"FIRST": 2.0}, device_name="Late"),
        _batch({"SECOND": 3.0}, device_name="Late"),
        _batch({"FIRST": 4.0}, device_name="Late"),
    ])

    assert writer.late_files == ("telemetry_late.csv", "telemetry_late_2.csv")
    with (tmp_path / "telemetry_late.csv").open(newline="") as handle:
        assert [row["FIRST [PSI]"] for row in csv.DictReader(handle)] == ["2.0000", "4.0000"]
    with (tmp_path / "telemetry_late_2.csv").open(newline="") as handle:
        assert [row["SECOND [PSI]"] for row in csv.DictReader(handle)] == ["3.0000"]


# ---------------------------------------------------------------------------
# Publisher
# ---------------------------------------------------------------------------


def test_publisher_is_a_no_op_until_attached(tmp_path: Path) -> None:
    publisher = TelemetrySessionPublisher()

    publisher.publish_batch(_batch({"PT101": 1.0}))  # must not raise

    state = _FakeState(_Schema(sensors=(_sensor("PT101"),)))
    writer = SessionTelemetryWriter.open(tmp_path / "telemetry.csv", state, build_columns(state.recording_schema()))  # type: ignore[arg-type]
    publisher.attach(writer)
    publisher.publish_batch(_batch({"PT101": 1.0}))
    publisher.detach()
    publisher.publish_batch(_batch({"PT101": 2.0}))
    writer.close()

    assert writer.rows == 1
