from types import SimpleNamespace
from typing import Any, cast

import pytest

from vector.core import TARE_SAMPLE_MAX_AGE_S, Core, SensorDefinition, TareCaptureError, TelemetryReading
from vector.qlcp.config_parser import parse_config
from vector.qlcp.packets import AckPacket, DataPacket, HeartbeatPacket, PacketHeader, SensorReading
from vector.runtime.esp_connection_runtime import ESPDeviceSession
from vector.runtime.metrics import Metrics
from vector.runtime.telemetry_ingest import TelemetryRuntime


def _metrics_snapshot(metrics: Metrics) -> dict[str, Any]:
    return cast("dict[str, Any]", metrics.to_dict())


def _make_config(device_name: str = "PANDA") -> dict[str, Any]:
    return {
        "device_name": device_name,
        "device_type": "Sensor Monitor",
        "sensors": {
            "thermocouple": {
                "TC1": {
                    "sensor_index": "TC1",
                    "type": "K",
                    "unit": "C",
                },
                "TC2": {
                    "sensor_index": "TC2",
                    "type": "K",
                    "unit": "C",
                },
            },
        },
        "controls": {},
    }


def _make_session(
    *,
    address: str = "10.0.0.2",
    last_sync_time: float | None = 1.0,
    device_name: str = "PANDA",
    connection_key: str = "conn-a",
) -> ESPDeviceSession:
    config = parse_config(_make_config(device_name))
    session = SimpleNamespace(
        name=config.name,
        address=address,
        connection_key=connection_key,
        qlcp_config=config,
        last_sync_time=last_sync_time,
    )
    return cast("ESPDeviceSession", session)


def _make_ingest(*sessions: ESPDeviceSession, metrics: Metrics | None = None) -> tuple[TelemetryRuntime, Core]:
    core = Core()
    for session in sessions:
        session.core_source = core.register_source(
            "qlcp", session.name, address=session.address, connection_key=session.connection_key,
            sensors=tuple(
                SensorDefinition(sensor.name, sensor.group, sensor.unit)
                for sensor in session.qlcp_config.sensors_by_id.values()
            ),
        )
    return TelemetryRuntime({session.address: session for session in sessions}.get, metrics=metrics), core


def _data_packet(*readings: tuple[int, float], sequence: int = 1) -> DataPacket:
    return DataPacket(
        header=PacketHeader(sequence=sequence, timestamp_us=12345),
        readings=[SensorReading(sensor_id=sensor_id, value=value) for sensor_id, value in readings],
    )


def test_data_packet_from_registered_session_produces_batch() -> None:
    session = _make_session()
    ingest, _core = _make_ingest(session)
    packet = DataPacket(
        header=PacketHeader(
            sequence=1,
            timestamp_us=12345,
        ),
        readings=[
            SensorReading(sensor_id=0, value=12.345),
            SensorReading(sensor_id=1, value=67.891),
        ],
    )

    batch = ingest.handle_datagram(packet.encode(), session.address)

    assert batch is not None
    assert batch.source_name == "PANDA"
    assert batch.source_address == session.address
    assert batch.connection_key == "conn-a"
    # 12345 us of device (server-base) time == 0.012345 s, on the same axis as time.monotonic().
    assert batch.timestamp_s == pytest.approx(0.012345)
    assert batch.timestamp_source == "device_synced"
    assert batch.timestamp_synced is True
    assert len(batch.readings) == 2
    assert batch.readings[0].value == pytest.approx(12.345)
    assert batch.readings[1].value == pytest.approx(67.891)
    assert batch.readings == (
        TelemetryReading(
            sensor_id=0,
            sensor_name="TC1",
            value=batch.readings[0].value,
            unit="C",
            group="thermocouple",
        ),
        TelemetryReading(
            sensor_id=1,
            sensor_name="TC2",
            value=batch.readings[1].value,
            unit="C",
            group="thermocouple",
        ),
    )


def test_unsynced_session_uses_monotonic_timestamp(monkeypatch: pytest.MonkeyPatch) -> None:
    session = _make_session(last_sync_time=None)
    ingest, _core = _make_ingest(session)
    monkeypatch.setattr("vector.runtime.telemetry_ingest.time.monotonic", lambda: 42.25)
    packet = DataPacket(
        header=PacketHeader(
            sequence=1,
            timestamp_us=12345,
        ),
        readings=[SensorReading(sensor_id=0, value=1.0)],
    )

    batch = ingest.handle_datagram(packet.encode(), session.address)

    assert batch is not None
    assert batch.timestamp_s == 42.25
    assert batch.timestamp_source == "server_receive"
    assert batch.timestamp_synced is False


def test_unknown_device_address_is_logged_and_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    errors: list[str] = []
    monkeypatch.setattr("vector.runtime.telemetry_ingest.logger.error", lambda msg, *args, **kwargs: errors.append(msg % args if args else msg))
    metrics = Metrics(time_fn=lambda: 100.0)
    ingest, _core = _make_ingest(metrics=metrics)

    batch = ingest.handle_datagram(b"not decoded", "10.0.0.99")

    assert batch is None
    assert errors == ["Received UDP packet from unknown device 10.0.0.99"]
    snapshot = _metrics_snapshot(metrics)
    assert snapshot["telemetry"]["unknown_source"]["total"]["unregistered_address"] == 1
    assert snapshot["telemetry"]["decode_errors"]["total"] == {}


def test_decode_error_records_metric(monkeypatch: pytest.MonkeyPatch) -> None:
    session = _make_session()
    metrics = Metrics(time_fn=lambda: 100.0)
    monkeypatch.setattr("vector.runtime.telemetry_ingest.logger.error", lambda *_args, **_kwargs: None)
    ingest, _core = _make_ingest(session, metrics=metrics)

    batch = ingest.handle_datagram(b"not decoded", session.address)

    assert batch is None
    snapshot = _metrics_snapshot(metrics)
    assert snapshot["telemetry"]["decode_errors"]["total"]["decode"] == 1
    assert snapshot["telemetry"]["ingest"]["by_device"]["PANDA"]["udp_bytes_total"] == len(b"not decoded")


def test_data_packets_record_throughput_without_packet_loss_estimate() -> None:
    session = _make_session()
    metrics = Metrics(time_fn=lambda: 100.0)
    ingest, _core = _make_ingest(session, metrics=metrics)
    readings = [SensorReading(sensor_id=0, value=1.0)]

    ingest.handle_packet(DataPacket(header=PacketHeader(sequence=254, timestamp_us=12345), readings=readings), session)
    ingest.handle_packet(DataPacket(header=PacketHeader(sequence=1, timestamp_us=12346), readings=readings), session)

    snapshot = _metrics_snapshot(metrics)
    assert "loss" not in snapshot["telemetry"]
    assert snapshot["telemetry"]["ingest"]["by_device"]["PANDA"]["data_packets_total"] == 2
    assert "batches_total" not in snapshot["telemetry"]["ingest"]["by_device"]["PANDA"]
    assert snapshot["telemetry"]["ingest"]["by_device"]["PANDA"]["readings_total"] == 2


def test_non_data_packet_is_logged_and_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    session = _make_session()
    errors: list[str] = []
    monkeypatch.setattr("vector.runtime.telemetry_ingest.logger.error", lambda msg, *args, **kwargs: errors.append(msg % args if args else msg))
    ingest, _core = _make_ingest(session)
    packet = AckPacket.create(HeartbeatPacket(header=PacketHeader(sequence=4, timestamp_us=0)))

    batch = ingest.handle_datagram(packet.encode(), session.address)

    assert batch is None
    assert errors == ["Received non-DATA packet over UDP from PANDA. Ignoring."]


def test_unknown_sensor_id_is_logged_and_dropped(monkeypatch: pytest.MonkeyPatch) -> None:
    session = _make_session()
    errors: list[str] = []
    monkeypatch.setattr("vector.runtime.telemetry_ingest.logger.error", lambda msg, *args, **kwargs: errors.append(msg % args if args else msg))
    ingest, _core = _make_ingest(session)
    packet = DataPacket(
        header=PacketHeader(
            sequence=1,
            timestamp_us=12345,
        ),
        readings=[SensorReading(sensor_id=99, value=1.0)],
    )

    batch = ingest.handle_datagram(packet.encode(), session.address)

    assert batch is not None
    assert batch.readings == ()
    assert errors == ["Received DATA reading for unknown sensor id 99 from PANDA. Ignoring."]


def test_tare_is_subtracted_and_reported_per_reading() -> None:
    session = _make_session()
    ingest, core = _make_ingest(session)
    core.set_tare("TC1", 10.0)

    batch = ingest.handle_packet(_data_packet((0, 12.5), (1, 30.0)), session)

    tc1, tc2 = batch.readings
    assert (tc1.value, tc1.tare) == pytest.approx((2.5, 10.0))
    # Untared sensors are unchanged, and the raw reading stays recoverable for both.
    assert (tc2.value, tc2.tare) == pytest.approx((30.0, 0.0))
    assert tc1.value + tc1.tare == pytest.approx(12.5)


def test_capture_tare_offset_averages_recent_raw_readings() -> None:
    session = _make_session()
    ingest, core = _make_ingest(session)
    for value in (1.0, 2.0, 3.0, 100.0):
        ingest.handle_packet(_data_packet((0, value)), session)

    offset, device_name, count = core.capture_tare_offset("TC1", samples=3)

    assert offset == pytest.approx(35.0)  # mean of the last three, not all four
    assert device_name == "PANDA"
    assert count == 3


def test_capture_tare_offset_samples_raw_values_not_tared_ones() -> None:
    """A second tare must recompute an absolute offset rather than compound onto the first."""
    session = _make_session()
    ingest, core = _make_ingest(session)
    core.set_tare("TC1", 10.0)
    for value in (20.0, 22.0):
        ingest.handle_packet(_data_packet((0, value)), session)

    offset, _device_name, _count = core.capture_tare_offset("TC1", samples=2)

    assert offset == pytest.approx(21.0)


def test_capture_tare_offset_uses_every_available_sample_when_fewer_than_requested() -> None:
    session = _make_session()
    ingest, core = _make_ingest(session)
    ingest.handle_packet(_data_packet((0, 4.0)), session)

    offset, _device_name, count = core.capture_tare_offset("TC1", samples=64)

    assert offset == pytest.approx(4.0)
    assert count == 1


def test_capture_tare_offset_without_samples_raises() -> None:
    session = _make_session()
    _ingest, core = _make_ingest(session)

    with pytest.raises(TareCaptureError, match="No telemetry received"):
        core.capture_tare_offset("TC1")


def test_capture_tare_offset_ignores_stale_samples(monkeypatch: pytest.MonkeyPatch) -> None:
    """A disconnected device's last readings must never be used to capture a tare."""
    session = _make_session()
    ingest, core = _make_ingest(session)
    monkeypatch.setattr("vector.runtime.telemetry_ingest.time.monotonic", lambda: 100.0)
    ingest.handle_packet(_data_packet((0, 4.0)), session)

    monkeypatch.setattr("vector.runtime.telemetry_ingest.time.monotonic", lambda: 100.0 + TARE_SAMPLE_MAX_AGE_S + 0.1)
    with pytest.raises(TareCaptureError):
        core.capture_tare_offset("TC1")


def test_capture_tare_offset_reports_candidates_when_name_is_ambiguous() -> None:
    """Two devices carrying the same sensor name is the flight handoff case."""
    ground = _make_session(address="10.0.0.2", device_name="GROUND")
    flight = _make_session(address="10.0.0.3", device_name="FLIGHT", connection_key="conn-b")
    ingest, core = _make_ingest(ground, flight)
    ingest.handle_packet(_data_packet((0, 4.0)), ground)
    ingest.handle_packet(_data_packet((0, 90.0)), flight)

    with pytest.raises(TareCaptureError, match=r"\(FLIGHT, GROUND\)"):
        core.capture_tare_offset("TC1")

    offset, device_name, _count = core.capture_tare_offset("TC1", device_name="FLIGHT")
    assert offset == pytest.approx(90.0)
    assert device_name == "FLIGHT"
