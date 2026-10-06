"""Behavior of the shared core without hardware services or protocol objects."""

from __future__ import annotations
import asyncio
import subprocess
import sys

import pytest

from vector.core import (
    TARE_SAMPLE_MAX_AGE_S,
    ControlBinding,
    ControlChanged,
    ControlDefinition,
    ControlDispatchError,
    ControlObservation,
    ControlStatus,
    ControlType,
    ControlValidationError,
    ControlValue,
    Core,
    SensorDefinition,
    SourceChanged,
    TareCaptureError,
)


def test_core_runs_with_provider_and_web_imports_blocked() -> None:
    code = """
import builtins
import asyncio

original_import = builtins.__import__
blocked = ('vector.qlcp', 'vector._protocol', 'vector.drivers', 'vector.runtime', 'kasa', 'fastapi')
def guarded_import(name, *args, **kwargs):
    if name.startswith(blocked):
        raise ImportError('provider import blocked: ' + name)
    return original_import(name, *args, **kwargs)
builtins.__import__ = guarded_import

from vector.core import Core, SensorDefinition, ControlDefinition
async def write(target, value):
    return 7
core = Core()
source = core.register_source('test', 'source', sensors=[SensorDefinition('pressure')],
                              controls=[ControlDefinition('valve')], control_handler=write)
source.publish_samples([('pressure', 12.0)], 1.0)
assert core.capture_tare_offset('pressure') == (12.0, 'source', 1)
assert asyncio.run(core.set_control(source.controls[0], True)) == 7
"""
    subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)  # noqa: S603


def test_named_bindings_keep_each_sources_metadata_and_declaration_order() -> None:
    core = Core()
    ground = core.register_source(
        "test", "ground", name="GROUND",
        sensors=[SensorDefinition("PT101", "pressure", "psi"), SensorDefinition("pt101", "pressure", "bar")],
        controls=[ControlDefinition("AV101", "valve")],
    )
    flight = core.register_source(
        "test", "flight", name="FLIGHT",
        sensors=[SensorDefinition("PT101", "flight-pressure", "kPa")],
        controls=[ControlDefinition("av101", "flight-valve")],
    )

    assert core.sensors("PT101") == (ground.sensors[0], flight.sensors[0])
    assert core.sensors("pt101") == (ground.sensors[1],)
    assert [binding.unit for binding in core.sensors("PT101")] == ["psi", "kPa"]
    assert [binding.id for binding in ground.sensors] == [0, 1]
    assert ground.control("av101") is ground.controls[0]
    assert core.source("test", "ground") is ground


def test_duplicate_sensor_declarations_keep_independent_ordinal_readings() -> None:
    core = Core()
    source = core.register_source(
        "test", "source",
        sensors=[SensorDefinition("PT101", "pressure", "psi"), SensorDefinition("PT101", "backup", "kPa")],
    )
    first = source.publish_samples([(source.sensors[0], 1.0), (source.sensors[1], 2.0)], 1.0)

    assert first is not None
    assert [reading.sensor_id for reading in first.readings] == [0, 1]
    assert [reading.unit_name for reading in first.readings] == ["psi", "kPa"]
    assert core.capture_tare_offset("PT101") == (1.5, "source", 2)
    assert source.sensor("PT101") is source.sensors[1]
    batch = source.publish_samples([("PT101", 3.0)], 2.0)
    assert batch.readings[0].sensor_id == 1


def test_duplicate_control_names_within_a_source_are_rejected() -> None:
    core = Core()
    with pytest.raises(ValueError, match="AV101"):
        core.register_source("test", "source", controls=[ControlDefinition("AV101", "valve"), ControlDefinition("av101", "relay")])
    assert core.sources() == ()


def test_shared_control_names_across_sources_are_allowed_with_a_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    warnings: list[tuple[object, ...]] = []
    monkeypatch.setattr("vector.core.core.logger.warning", lambda _message, *args: warnings.append(args))
    core = Core()
    ground = core.register_source("test", "ground", controls=[ControlDefinition("AV101")])
    flight = core.register_source("test", "flight", controls=[ControlDefinition("av101")])

    assert core.sources() == (ground, flight)
    assert ground.control("AV101") is ground.controls[0]
    assert flight.control("AV101") is flight.controls[0]
    assert [args[-1] for args in warnings] == ["test:ground"]


def test_tare_uses_raw_history_and_only_changes_subsequent_samples() -> None:
    core = Core()
    source = core.register_source("wireless", "antenna", sensors=[SensorDefinition("RSSI", "radio", "dBm")])
    published = []
    core.subscribe_samples(published.append)
    for value in (-61.0, -62.0, -63.0):
        source.publish_samples([("RSSI", value)], 100.0)

    assert core.capture_tare_offset("RSSI", samples=2) == (-62.5, "antenna", 2)
    core.set_tare("RSSI", -62.5)
    batch = source.publish_samples([("RSSI", -60.0)], 101.0)
    assert batch is not None
    assert (batch.readings[0].value, batch.readings[0].tare) == (2.5, -62.5)
    assert core.capture_tare_offset("RSSI", samples=2) == (-61.5, "antenna", 2)
    assert published[-1] is batch
    assert len(published) == 4


def test_tare_is_shared_by_exact_name_and_capture_requires_a_source_if_ambiguous() -> None:
    core = Core()
    ground = core.register_source("test", "ground", name="GROUND", sensors=[SensorDefinition("PT101")])
    flight = core.register_source("test", "flight", name="FLIGHT", sensors=[SensorDefinition("PT101")])
    ground.publish_samples([("PT101", 5.0)], 1.0)
    flight.publish_samples([("PT101", 50.0)], 1.0)

    with pytest.raises(TareCaptureError, match=r"\(FLIGHT, GROUND\)"):
        core.capture_tare_offset("PT101")
    assert core.capture_tare_offset("PT101", device_name="GROUND") == (5.0, "GROUND", 1)
    core.set_tare("PT101", 5.0)
    assert core.tares().get("pt101", 0.0) == 0
    batch = flight.publish_samples([("PT101", 50.0)], 2.0)
    assert batch is not None
    assert batch.readings[0].value == 45.0
    assert core.tares() == {"PT101": 5.0}
    assert core.clear_tare("PT101") is True
    assert core.clear_tare("PT101") is False


@pytest.mark.parametrize("offset", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_tares_do_not_replace_existing_offset(offset: float) -> None:
    core = Core()
    core.set_tare("PT101", 10.0)
    with pytest.raises(ValueError, match="finite"):
        core.set_tare("PT101", offset)
    assert core.tares().get("PT101", 0.0) == 10.0


@pytest.mark.parametrize(
    "values",
    [(float("nan"),), (float("inf"),), (float("-inf"),), (sys.float_info.max, sys.float_info.max)],
    ids=["nan", "positive-infinity", "negative-infinity", "finite-sum-overflow"],
)
def test_nonfinite_capture_rejects_without_changing_existing_tare(values: tuple[float, ...]) -> None:
    core = Core()
    source = core.register_source("test", "source", sensors=[SensorDefinition("PT101")])
    core.set_tare("PT101", 10.0)
    changes = []
    core.subscribe_changes(changes.append)
    source.publish_samples([("PT101", value) for value in values], 1.0)

    with pytest.raises(TareCaptureError):
        core.capture_tare_offset("PT101")

    assert core.tares().get("PT101", 0.0) == 10.0
    assert changes == []


def test_provider_qualified_tare_selection_disambiguates_duplicate_source_names() -> None:
    core = Core()
    a = core.register_source("a", "sensor", sensors=[SensorDefinition("RSSI")])
    b = core.register_source("b", "sensor", sensors=[SensorDefinition("RSSI")])
    a.publish_samples([("RSSI", -60.0)], 1.0)
    b.publish_samples([("RSSI", -70.0)], 1.0)

    with pytest.raises(TareCaptureError, match=r"\(a:sensor, b:sensor\)"):
        core.capture_tare_offset("RSSI")
    assert core.capture_tare_offset("RSSI", device_name="b:sensor") == (-70.0, "sensor", 1)


@pytest.mark.parametrize("other_identity", [("b", "sensor"), ("a", "other")])
def test_batches_preserve_source_identity_when_labels_and_connections_collide(
    other_identity: tuple[str, str],
) -> None:
    core = Core()
    batches = []
    core.subscribe_samples(batches.append)
    for identity, value in ((("a", "sensor"), -60.0), (other_identity, -70.0)):
        source = core.register_source(
            *identity, name="Antenna", connection_key="shared",
            sensors=[SensorDefinition("RSSI")],
        )
        source.publish_samples([("RSSI", value)], 1.0)

    assert [(batch.source_provider, batch.source_key) for batch in batches] == [("a", "sensor"), other_identity]
    assert [batch.readings[0].value for batch in batches] == [-60.0, -70.0]
    assert [batch.device_name for batch in batches] == ["Antenna", "Antenna"]
    assert [batch.connection_key for batch in batches] == ["shared", "shared"]


def test_batch_identity_survives_rename_and_reconnect() -> None:
    core = Core()
    old = core.register_source("wireless", "radio-1", name="Antenna", sensors=[SensorDefinition("RSSI")])
    before = old.publish_samples([("RSSI", -60.0)], 1.0)
    new = core.register_source("wireless", "radio-1", name="Pad antenna", sensors=[SensorDefinition("RSSI")])
    after = new.publish_samples([("RSSI", -70.0)], 2.0)

    assert before is not None
    assert after is not None
    assert (before.source_provider, before.source_key) == (after.source_provider, after.source_key)
    assert (after.source_provider, after.source_key) == ("wireless", "radio-1")
    assert (before.device_name, after.device_name) == ("Antenna", "Pad antenna")
    assert before.connection_key != after.connection_key
    assert not old.connected
    assert new.connected


def test_capture_ignores_stale_and_disconnected_history(monkeypatch: pytest.MonkeyPatch) -> None:
    core = Core()
    source = core.register_source("test", "source", sensors=[SensorDefinition("PT101")])
    monkeypatch.setattr("vector.core.core.time.monotonic", lambda: 100.0)
    source.publish_samples([("PT101", 1.0)], 1.0)
    monkeypatch.setattr("vector.core.core.time.monotonic", lambda: 100.0 + TARE_SAMPLE_MAX_AGE_S + 0.1)
    with pytest.raises(TareCaptureError, match="No telemetry"):
        core.capture_tare_offset("PT101")
    source.publish_samples([("PT101", 2.0)], 2.0)
    source.close()
    with pytest.raises(TareCaptureError, match="No telemetry"):
        core.capture_tare_offset("PT101")
    assert core.sensors() == source.sensors


def test_replacing_a_source_rejects_old_samples_reports_and_disconnects() -> None:
    core = Core()
    declarations = dict(sensors=[SensorDefinition("PT101")], controls=[ControlDefinition("AV101")])
    old = core.register_source("test", "source", **declarations)
    old.publish_samples([("PT101", 1.0)], 1.0)
    old.report_control("AV101", True)
    new = core.register_source("test", "source", **declarations)
    assert new is not old
    assert not old.connected

    assert old.publish_samples([("PT101", 100.0)], 100.0) is None
    old.report_control("AV101", False)
    old.accept_control("AV101", False)
    old.close()
    assert new.connected
    assert old.controls[0].reported.value is True
    assert old.controls[0].accepted is None
    assert new.controls[0].reported is None
    assert new.controls[0].accepted is None
    with pytest.raises(TareCaptureError):
        core.capture_tare_offset("PT101")

    batch = new.publish_samples([("PT101", 2.0)], 2.0)
    assert batch is not None
    assert batch.readings[0].value == 2.0


def test_accepted_and_reported_state_are_separate_and_errors_retain_last_value() -> None:
    core = Core()
    changes = []
    core.subscribe_changes(changes.append)
    source = core.register_source("test", "source", controls=[ControlDefinition("AV101")])
    source.accept_control("av101", True, now=10.0)
    source.report_control("AV101", False, status=ControlStatus.PENDING, now=11.0)
    control = source.controls[0]
    assert control.accepted == ControlObservation(True, 10.0)
    assert control.reported == ControlObservation(False, 11.0, ControlStatus.PENDING)
    source.report_control("AV101", None, status="error", now=12.0)
    assert control.reported == ControlObservation(False, 12.0, ControlStatus.ERROR)
    assert control.accepted == ControlObservation(True, 10.0)
    assert changes == [
        SourceChanged("registered", source),
        ControlChanged("accepted", control),
        ControlChanged("reported", control),
        ControlChanged("reported", control),
    ]


def test_initial_controls_are_visible_during_registration_notification() -> None:
    core = Core()
    snapshots = []
    core.subscribe_changes(lambda change: snapshots.append(change.source.controls[0].reported))
    observation = ControlObservation(True, 1.0, ControlStatus.CONFIRMED)
    core.register_source(
        "kasa", "plug", controls=[ControlDefinition("power")], initial_controls={"power": observation},
    )
    assert snapshots == [observation]


def test_dispatch_passes_each_exact_binding_to_the_handler() -> None:
    core = Core()
    writes = []

    async def handler(target: ControlBinding, value: ControlValue) -> int:
        writes.append((target, value))
        return target.id + 100

    source = core.register_source(
        "test", "source", control_handler=handler,
        controls=[ControlDefinition("AV101", "valve"), ControlDefinition("RL101", "relay")],
    )
    targets = source.controls

    command_ids = [asyncio.run(core.set_control(target, True)) for target in targets]

    assert len(writes) == 2
    assert writes[0][0] is source.controls[0]
    assert writes[1][0] is source.controls[1]
    assert [(target.id, target.group, value) for target, value in writes] == [(0, "valve", True), (1, "relay", True)]
    assert command_ids == [100, 101]


def test_forged_and_foreign_bindings_fail_before_any_dispatch() -> None:
    core = Core()
    writes = []

    async def handler(target: ControlBinding, value: ControlValue) -> None:
        writes.append((target, value))

    source = core.register_source("test", "source", controls=[ControlDefinition("AV101")], control_handler=handler)
    target = source.controls[0]
    forged = ControlBinding(source, target.definition, target.id)
    assert forged == target
    assert forged is not target

    with pytest.raises(ControlValidationError):
        asyncio.run(core.set_control(forged, True))
    with pytest.raises(ControlValidationError):
        asyncio.run(Core().set_control(target, True))

    assert writes == []


@pytest.mark.parametrize(
    ("control_type", "invalid"),
    [(ControlType.BOOL, 1), (ControlType.UINT32, 1.5), (ControlType.UINT32, True),
     (ControlType.INT32, 1.5), (ControlType.INT32, True), (ControlType.FLOAT32, True)],
)
def test_invalid_typed_values_never_reach_the_provider(control_type: ControlType, invalid: ControlValue) -> None:
    core = Core()
    source = core.register_source("test", "source", controls=[ControlDefinition("setpoint", type=control_type)])
    with pytest.raises(ControlValidationError):
        asyncio.run(core.set_control(source.controls[0], invalid))


def test_integer_range_errors_remain_the_providers_responsibility() -> None:
    core = Core()
    values = []

    async def handler(_target: ControlBinding, value: ControlValue) -> None:
        values.append(value)
        raise ControlValidationError("-1 does not fit UINT32.")

    source = core.register_source(
        "test", "source", controls=[ControlDefinition("setpoint", type=ControlType.UINT32)], control_handler=handler,
    )
    with pytest.raises(ControlValidationError, match="UINT32"):
        asyncio.run(core.set_control(source.controls[0], -1))
    assert values == [-1]


def test_dispatch_reports_handler_failure_and_does_not_infer_acceptance() -> None:
    core = Core()
    writes = []
    failure = OSError("connection failed")

    async def handler(target: ControlBinding, value: ControlValue) -> int:
        writes.append((target.name, value))
        if target.name == "failed":
            raise failure
        return 42

    source = core.register_source(
        "test", "source", control_handler=handler,
        controls=[ControlDefinition("failed"), ControlDefinition("good")],
    )
    with pytest.raises(ControlDispatchError, match="connection failed") as raised:
        asyncio.run(core.set_control(source.controls[0], True))
    assert raised.value.__cause__ is failure
    assert asyncio.run(core.set_control(source.controls[1], True)) == 42
    assert writes == [("failed", True), ("good", True)]
    assert source.controls[1].accepted is None
    assert source.controls[1].reported is None


def test_disconnected_and_foreign_targets_cannot_dispatch() -> None:
    core = Core()
    writes = []

    async def handler(target: ControlBinding, value: ControlValue) -> None:
        writes.append((target, value))

    source = core.register_source("test", "source", controls=[ControlDefinition("AV101")], control_handler=handler)
    source.close()
    with pytest.raises(ControlDispatchError, match="unavailable"):
        asyncio.run(core.set_control(source.controls[0], True))
    with pytest.raises(ControlValidationError, match="does not belong"):
        asyncio.run(Core().set_control(source.controls[0], True))
    assert writes == []


def test_a_delayed_command_completion_cannot_change_a_replacement() -> None:
    async def scenario() -> None:
        core = Core()
        started = asyncio.Event()
        finish = asyncio.Event()

        async def handler(target: ControlBinding, value: ControlValue) -> int:
            started.set()
            await finish.wait()
            old.accept_control(target, value)
            old.report_control(target, value)
            return 42

        old = core.register_source("test", "source", controls=[ControlDefinition("AV101")], control_handler=handler)
        task = asyncio.create_task(core.set_control(old.controls[0], True))
        await started.wait()
        new = core.register_source("test", "source", controls=[ControlDefinition("AV101")])
        finish.set()
        # The handler did send; the result must say so even though its source was replaced.
        assert await task == 42
        assert new.controls[0].accepted is None
        assert new.controls[0].reported is None

    asyncio.run(scenario())


def test_subscriber_failure_is_isolated() -> None:
    core = Core()
    batches = []
    changes = []

    def fail(_value: object) -> None:
        raise RuntimeError("consumer failed")

    core.subscribe_samples(fail)
    core.subscribe_changes(fail)
    core.subscribe_samples(batches.append)
    core.subscribe_changes(changes.append)
    source = core.register_source("test", "source", sensors=[SensorDefinition("PT101")])
    first = source.publish_samples([("PT101", 1.0)], 1.0)
    assert batches == [first]
    assert changes == [SourceChanged("registered", source)]


def test_a_batch_with_an_unknown_sensor_publishes_nothing() -> None:
    core = Core()
    source = core.register_source("test", "source", sensors=[SensorDefinition("PT101")])
    batches = []
    core.subscribe_samples(batches.append)
    with pytest.raises(ValueError, match="Unknown sensor"):
        source.publish_samples([("PT101", 1.0), ("unknown", 2.0)], 1.0)
    assert batches == []
    with pytest.raises(TareCaptureError):
        core.capture_tare_offset("PT101")
