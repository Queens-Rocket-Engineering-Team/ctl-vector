"""Integration tests: MockSensorDevice ↔ real ESPConnectionRuntime over localhost TCP/UDP.

These tests compose the same runtime components the server uses (no mocking of the
protocol path) and drive them with MockSensorDevice over loopback sockets on ephemeral
ports. No fixed sleeps — tests await asyncio.Event milestones or poll with a short
tick and a generous timeout.

Async test pattern (no pytest-asyncio): each test defines an inner ``async def run()``
and calls ``asyncio.run(run())``.
"""

from __future__ import annotations
import asyncio
import contextlib
import socket
from typing import TYPE_CHECKING, Any

from tests.mock_device import MockSensorDevice
from vector.core import Core, TelemetryBatch
from vector.qlcp.enums import ControlState, PacketType
from vector.runtime.command_tracker import CommandLifecycle, CommandTracker
from vector.runtime.esp_connection_runtime import ESPConnectionRuntime, ESPDeviceSession
from vector.runtime.telemetry_ingest import TelemetryRuntime
from vector.state.system_state import SystemState


if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


# --------------------------------------------------------------------------- #
# Test harness helpers                                                          #
# --------------------------------------------------------------------------- #


class _FakeStateStream:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def publish(self, event: dict[str, object] | None) -> None:
        if event is not None:
            self.events.append(event)


class _CollectingPublisher:
    def __init__(self) -> None:
        self.batches: list[TelemetryBatch] = []

    def publish_batch(self, batch: TelemetryBatch) -> None:
        self.batches.append(batch)


def _free_tcp_port() -> int:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]
    finally:
        probe.close()


def _free_udp_port() -> int:
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]
    finally:
        probe.close()


@contextlib.asynccontextmanager
async def _runtime_harness() -> AsyncGenerator[
    tuple[ESPConnectionRuntime, CommandTracker, SystemState, _CollectingPublisher, TelemetryRuntime, int, int],
    None,
]:
    """Spin up TCP + UDP listeners on ephemeral ports and yield runtime handles.

    Yields: (runtime, tracker, system_state, telemetry_publisher, telemetry_runtime, tcp_port, udp_port)
    """
    tcp_port = _free_tcp_port()
    udp_port = _free_udp_port()

    tracker = CommandTracker()
    state = SystemState(core=Core())
    stream = _FakeStateStream()
    state.set_publisher(stream.publish)
    runtime = ESPConnectionRuntime(
        command_tracker=tracker,
        system_state=state,
        state_stream=stream,
    )

    publisher = _CollectingPublisher()
    telemetry_runtime = TelemetryRuntime(runtime.get_device_by_address)
    state.core.subscribe_samples(publisher.publish_batch)

    tasks = [
        asyncio.create_task(runtime.run_tcp_listener(port=tcp_port)),
        asyncio.create_task(telemetry_runtime.run_udp_listener(port=udp_port)),
    ]
    # Yield once to let both listener tasks bind their sockets before the mock connects.
    await asyncio.sleep(0)

    try:
        yield runtime, tracker, state, publisher, telemetry_runtime, tcp_port, udp_port
    finally:
        for task in tasks:
            task.cancel()
        runtime.close_all()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task


async def _wait_for(condition: Any, *, timeout_s: float = 2.0, tick: float = 0.02) -> bool:
    """Poll ``condition()`` every tick seconds; return True when it becomes truthy or False on timeout."""
    deadline = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < deadline:
        if condition():
            return True
        await asyncio.sleep(tick)
    return False


def _session_for(runtime: ESPConnectionRuntime, device_name: str) -> ESPDeviceSession:
    """Return the registered session for device_name, or raise AssertionError."""
    for session in runtime.devices.values():
        if session.name == device_name:
            return session
    message = f"No session found for device {device_name!r}; registered: {list(runtime.devices.snapshot_by_address())}"
    raise AssertionError(message)


# --------------------------------------------------------------------------- #
# Tests                                                                         #
# --------------------------------------------------------------------------- #


def test_device_registers_on_connect() -> None:
    """Mock device connects → CONFIG → device appears in runtime.devices."""

    async def run() -> None:
        async with (
            _runtime_harness() as (runtime, _tracker, _state, _publisher, _telemetry_runtime, tcp_port, udp_port),
            MockSensorDevice(
                server_ip="127.0.0.1",
                server_port=tcp_port,
                server_udp_port=udp_port,
            ) as dev,
        ):
            await asyncio.wait_for(dev.timesync_received.wait(), timeout=2.0)
            assert dev.device_name in {s.name for s in runtime.devices.values()}, (
                f"Device {dev.device_name!r} not found in runtime.devices: {list(runtime.devices.snapshot_by_address())}"
            )
            session = _session_for(runtime, dev.device_name)
            relays = {name: session.controls[name] for name in ("SAFE24", "IGNPRIME", "IGNRUN")}

            assert set(relays) == {"SAFE24", "IGNPRIME", "IGNRUN"}
            assert all(control.group == "relay" for control in relays.values())
            assert all(control.default == ControlState.OPEN for control in relays.values())

    asyncio.run(run())


def test_control_command_acked_and_state_updated() -> None:
    """Server sends CONTROL → mock ACKs and updates valve_states; tracker records ACKED."""

    async def run() -> None:
        async with (
            _runtime_harness() as (runtime, tracker, _state, _publisher, _telemetry_runtime, tcp_port, udp_port),
            MockSensorDevice(
                server_ip="127.0.0.1",
                server_port=tcp_port,
                server_udp_port=udp_port,
            ) as dev,
        ):
            await asyncio.wait_for(dev.timesync_received.wait(), timeout=2.0)
            session = _session_for(runtime, dev.device_name)

            # AV101 starts OPEN (from config default).
            assert dev.control_states.get("AV101") == "OPEN"
            assert dev.control_states.get("SAFE24") == "OPEN"

            # Clear the event before sending so we can reliably await it.
            dev.control_handled.clear()
            await runtime.set_control(session, "AV101", "CLOSED")

            await asyncio.wait_for(dev.control_handled.wait(), timeout=2.0)

            assert dev.control_states.get("AV101") == "CLOSED"

            dev.control_handled.clear()
            await runtime.set_control(session, "SAFE24", "CLOSED")

            await asyncio.wait_for(dev.control_handled.wait(), timeout=2.0)

            assert dev.control_states.get("SAFE24") == "CLOSED"

            # Give the ACK a tick to propagate through the server's monitor loop.
            await asyncio.sleep(0.05)

            control_records = [r for r in tracker.recent_completed if r.packet_type == from_name("CONTROL")]
            assert any(r.state == CommandLifecycle.ACKED for r in control_records), (
                f"No ACKED CONTROL record found. recent_completed: {tracker.recent_completed}"
            )

    asyncio.run(run())


def test_control_command_closed() -> None:
    """OPEN → CLOSE round-trip updates valve state correctly."""

    async def run() -> None:
        async with (
            _runtime_harness() as (runtime, _tracker, _state, _publisher, _telemetry_runtime, tcp_port, udp_port),
            MockSensorDevice(
                server_ip="127.0.0.1",
                server_port=tcp_port,
                server_udp_port=udp_port,
            ) as dev,
        ):
            await asyncio.wait_for(dev.timesync_received.wait(), timeout=2.0)
            session = _session_for(runtime, dev.device_name)

            dev.control_handled.clear()
            await runtime.set_control(session, "AV101", "OPEN")
            await asyncio.wait_for(dev.control_handled.wait(), timeout=2.0)

            dev.control_handled.clear()
            await runtime.set_control(session, "AV101", "CLOSED")
            await asyncio.wait_for(dev.control_handled.wait(), timeout=2.0)

            assert dev.control_states.get("AV101") == "CLOSED"

    asyncio.run(run())


def test_telemetry_stream_readings_match_config() -> None:
    """Streaming DATA packets carry readings that map sensor_id → name → unit per the device config."""

    async def run() -> None:
        async with (
            _runtime_harness() as (runtime, _tracker, _state, publisher, _telemetry_runtime, tcp_port, udp_port),
            MockSensorDevice(
                server_ip="127.0.0.1",
                server_port=tcp_port,
                server_udp_port=udp_port,
            ) as dev,
        ):
            await asyncio.wait_for(dev.timesync_received.wait(), timeout=2.0)
            session = _session_for(runtime, dev.device_name)

            # Start streaming at 20 Hz for fast batch accumulation.
            dev.stream_started.clear()
            await runtime.start_streaming(session, frequency_hz=20)
            await asyncio.wait_for(dev.stream_started.wait(), timeout=2.0)

            # Wait for at least 3 batches.
            reached = await _wait_for(lambda: len(publisher.batches) >= 3, timeout_s=2.0)
            assert reached, f"Expected ≥3 batches, got {len(publisher.batches)}"

            # Verify sensor-id / name / unit consistency against the device config.
            batch = publisher.batches[0]
            config_sensors = session.qlcp_config.sensors_by_id

            assert len(batch.readings) == len(config_sensors), f"Expected {len(config_sensors)} readings, got {len(batch.readings)}"

            for reading in batch.readings:
                sensor = config_sensors.get(reading.sensor_id)
                assert sensor is not None, f"Unknown sensor_id {reading.sensor_id} in batch"
                assert reading.sensor_name == sensor.name, f"sensor_id {reading.sensor_id}: name {reading.sensor_name!r} != config {sensor.name!r}"
                assert reading.unit_name == sensor.unit, f"sensor {sensor.name}: unit {reading.unit_name!r} != config {sensor.unit!r}"

            # Stop streaming and confirm the mock acknowledges it.
            dev.stream_stopped.clear()
            await runtime.stop_streaming(session)
            await asyncio.wait_for(dev.stream_stopped.wait(), timeout=2.0)
            assert not dev.streaming

    asyncio.run(run())


def test_get_single_delivers_data_over_udp() -> None:
    """GET_SINGLE causes mock to send one DATA packet over UDP; no ACK is sent over TCP."""

    async def run() -> None:
        async with (
            _runtime_harness() as (runtime, tracker, _state, publisher, _telemetry_runtime, tcp_port, udp_port),
            MockSensorDevice(
                server_ip="127.0.0.1",
                server_port=tcp_port,
                server_udp_port=udp_port,
            ) as dev,
        ):
            await asyncio.wait_for(dev.timesync_received.wait(), timeout=2.0)
            session = _session_for(runtime, dev.device_name)

            initial_count = len(publisher.batches)
            await runtime.get_single(session)

            reached = await _wait_for(
                lambda: len(publisher.batches) > initial_count,
                timeout_s=2.0,
            )
            assert reached, "GET_SINGLE did not produce a telemetry batch"

            # GET_SINGLE is not ACK-expected: the record should be immediately completed.
            get_single_records = [r for r in tracker.recent_completed if r.packet_type == from_name("GET_SINGLE")]
            assert get_single_records, "GET_SINGLE not found in recent_completed"
            # ack_expected=False → state is SENT (immediately completed, not pending ACK)
            assert all(not r.ack_expected for r in get_single_records)

    asyncio.run(run())


def test_estop_stops_streaming_and_resets_state() -> None:
    """ESTOP causes the mock to stop streaming and reset control states to defaults."""

    async def run() -> None:
        async with (
            _runtime_harness() as (runtime, _tracker, _state, _publisher, _telemetry_runtime, tcp_port, udp_port),
            MockSensorDevice(
                server_ip="127.0.0.1",
                server_port=tcp_port,
                server_udp_port=udp_port,
            ) as dev,
        ):
            await asyncio.wait_for(dev.timesync_received.wait(), timeout=2.0)
            session = _session_for(runtime, dev.device_name)

            # Close a valve so we can verify ESTOP resets it back to its OPEN default.
            dev.control_handled.clear()
            await runtime.set_control(session, "AV101", "CLOSED")
            await asyncio.wait_for(dev.control_handled.wait(), timeout=2.0)
            assert dev.control_states.get("AV101") == "CLOSED"

            # Close a relay so we can verify ESTOP resets it back to its OPEN default.
            dev.control_handled.clear()
            await runtime.set_control(session, "SAFE24", "CLOSED")
            await asyncio.wait_for(dev.control_handled.wait(), timeout=2.0)
            assert dev.control_states.get("SAFE24") == "CLOSED"

            # Start streaming.
            dev.stream_started.clear()
            await runtime.start_streaming(session, frequency_hz=10)
            await asyncio.wait_for(dev.stream_started.wait(), timeout=2.0)
            assert dev.streaming

            # Fire ESTOP.
            await runtime.emergency_stop(session)

            # Streaming should stop.
            stopped = await _wait_for(lambda: not dev.streaming, timeout_s=2.0)
            assert stopped, "Mock is still streaming after ESTOP"

            # Valve state should be reset to default (OPEN).
            reset_ok = await _wait_for(
                lambda: dev.control_states.get("AV101") == "OPEN",
                timeout_s=2.0,
            )
            assert reset_ok, f"Valve AV101 did not reset to OPEN; got {dev.control_states.get('AV101')!r}"

            # Relay state should be reset to default (OPEN).
            relay_reset_ok = await _wait_for(
                lambda: dev.control_states.get("SAFE24") == "OPEN",
                timeout_s=2.0,
            )
            assert relay_reset_ok, f"Relay SAFE24 did not reset to OPEN; got {dev.control_states.get('SAFE24')!r}"

    asyncio.run(run())


def test_custom_config_sensor_ids_match_readings() -> None:
    """A non-default device config produces readings that align with that config's sensor IDs."""

    custom_config: dict[str, Any] = {
        "device_name": "CustomDevice",
        "sensors": {
            "load_cell": {
                "LC101": {
                    "sensor_index": "LC1",
                    "unit": "N",
                    "load_rating_N": 500.0,
                    "excitation_V": 10.0,
                    "sensitivity_vV": 2.0,
                },
            },
        },
        "controls": {
            "relay": {
                "RELAY1": {
                    "control_index": "RELAY_1",
                    "type": "BOOL",
                    "default_state": "CLOSED",
                },
            },
        },
    }

    async def run() -> None:
        async with (
            _runtime_harness() as (runtime, _tracker, _state, publisher, _telemetry_runtime, tcp_port, udp_port),
            MockSensorDevice(
                server_ip="127.0.0.1",
                server_port=tcp_port,
                server_udp_port=udp_port,
                config=custom_config,
            ) as dev,
        ):
            await asyncio.wait_for(dev.timesync_received.wait(), timeout=2.0)
            session = _session_for(runtime, "CustomDevice")

            await runtime.get_single(session)
            reached = await _wait_for(lambda: len(publisher.batches) > 0, timeout_s=2.0)
            assert reached

            batch = publisher.batches[0]
            assert len(batch.readings) == 1
            reading = batch.readings[0]
            assert reading.sensor_name == "LC101"
            assert reading.unit_name == "N"

    asyncio.run(run())


def test_tare_set_mid_stream_offsets_readings_without_losing_the_raw_value() -> None:
    """Setting a tare while streaming steps `value` down by the offset; `value + tare` stays continuous."""

    def _reading_for(batch: TelemetryBatch, sensor_name: str) -> Any:
        return next(reading for reading in batch.readings if reading.sensor_name == sensor_name)

    async def run() -> None:
        async with (
            _runtime_harness() as (runtime, _tracker, state, publisher, _telemetry_runtime, tcp_port, udp_port),
            MockSensorDevice(
                server_ip="127.0.0.1",
                server_port=tcp_port,
                server_udp_port=udp_port,
            ) as dev,
        ):
            await asyncio.wait_for(dev.timesync_received.wait(), timeout=2.0)
            session = _session_for(runtime, dev.device_name)

            dev.stream_started.clear()
            await runtime.start_streaming(session, frequency_hz=20)
            await asyncio.wait_for(dev.stream_started.wait(), timeout=2.0)

            reached = await _wait_for(lambda: len(publisher.batches) >= 3, timeout_s=2.0)
            assert reached, f"Expected ≥3 batches, got {len(publisher.batches)}"

            before = publisher.batches[-1]
            assert _reading_for(before, "PT101").tare == 0.0

            offset, sampled_device, count = state.core.capture_tare_offset("PT101", samples=1)
            assert sampled_device == dev.device_name
            assert count == 1
            state.core.set_tare("PT101", offset)

            tared_at = len(publisher.batches)
            reached = await _wait_for(lambda: len(publisher.batches) > tared_at, timeout_s=2.0)
            assert reached, "No batches arrived after the tare was set"
            after = publisher.batches[tared_at]

            before_reading = _reading_for(before, "PT101")
            after_reading = _reading_for(after, "PT101")

            assert after_reading.tare == offset
            # The mock's PT101 signal is a 0.25 Hz / 20-unit sine, so it moves at most
            # 20*2*pi*0.25 units per second. Anything beyond that is the tare misapplied.
            drift = 31.5 * abs(after.timestamp_s - before.timestamp_s) + 0.01
            assert abs(after_reading.value - (before_reading.value - offset)) <= drift
            # Other sensors are untouched: tares are per sensor name.
            assert _reading_for(after, "TC101").tare == 0.0

            state.core.clear_tare("PT101")
            cleared_at = len(publisher.batches)
            reached = await _wait_for(lambda: len(publisher.batches) > cleared_at, timeout_s=2.0)
            assert reached, "No batches arrived after the tare was cleared"
            assert _reading_for(publisher.batches[cleared_at], "PT101").tare == 0.0

            await runtime.stop_streaming(session)

    asyncio.run(run())


# --------------------------------------------------------------------------- #
# Utility                                                                       #
# --------------------------------------------------------------------------- #


def from_name(name: str) -> Any:
    """Resolve a PacketType by name (avoids a direct enum import in assertions)."""
    return PacketType[name]
