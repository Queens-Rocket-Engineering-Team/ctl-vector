import asyncio
import socket
from functools import partial
from types import SimpleNamespace
from typing import Any, cast

import orjson
import pytest

from vector.core import ControlValidationError, Core
from vector.qlcp.config_parser import parse_config
from vector.qlcp.decoding import decode_packet_client
from vector.qlcp.enums import ControlConfirmStatus, ControlState, ControlType, ErrorCode, PacketType
from vector.qlcp.packets import (
    AckPacket,
    ConfigPacket,
    ControlPacket,
    ControlStatus,
    EstopPacket,
    HeartbeatPacket,
    NackPacket,
    PacketHeader,
    StatusPacket,
    StatusRequestPacket,
)
from vector.runtime.command_tracker import CommandLifecycle, CommandTracker
from vector.runtime.esp_connection_runtime import ESPConnectionRuntime, ESPDeviceSession
from vector.state.system_state import SystemState


class FakeStateStream:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def publish(self, event: dict[str, object] | None) -> None:
        if event is not None:
            self.events.append(event)


class FakeDriver:
    def __init__(self) -> None:
        self.sent_packets: list[object] = []

    async def send_packet(self, packet: object) -> None:
        self.sent_packets.append(packet)


def _make_config(name: str = "TEST-DEVICE") -> dict[str, Any]:
    return {
        "device_name": name,
        "sensors": {
            "thermocouple": {
                "TC1": {
                    "sensor_index": "TC1",
                    "type": "K",
                    "unit": "C",
                },
            },
        },
        "controls": {
            "valve": {
                "VALVE1": {
                    "control_index": "VALVE1",
                    "type": "BOOL",
                    "default_state": "CLOSED",
                },
            },
            "heater": {
                "HEATER1": {
                    "control_index": "HEATER1",
                    "type": "UINT32",
                    "default_state": "0",
                },
                "HEATER2": {
                    "control_index": "HEATER2",
                    "type": "FLOAT32",
                    "default_state": "0.0",
                },
            },
        },
    }


def _heartbeat(sequence: int) -> HeartbeatPacket:
    """Build a HEARTBEAT with a fixed sequence, for ACKs with a known ack_sequence."""
    return HeartbeatPacket(
        header=PacketHeader(
            sequence=sequence,
            timestamp_us=0,
        ),
    )


def _sent_packets(session: ESPDeviceSession) -> list[object]:
    """Packets recorded by the FakeDriver a _make_session device was built with."""
    return cast("FakeDriver", session.driver).sent_packets


def _make_runtime() -> tuple[ESPConnectionRuntime, CommandTracker, SystemState, FakeStateStream]:
    tracker = CommandTracker()
    state = SystemState(core=Core())
    stream = FakeStateStream()
    state.set_publisher(stream.publish)
    runtime = ESPConnectionRuntime(
        command_tracker=tracker,
        system_state=state,
        state_stream=stream,
    )
    return runtime, tracker, state, stream


def _make_session(
    runtime: ESPConnectionRuntime,
    *,
    address: str = "10.0.0.2",
    connection_key: str = "conn-a",
    name: str = "TEST-DEVICE",
    config_dict: dict[str, Any] | None = None,
) -> ESPDeviceSession:
    config = parse_config(_make_config(name=name) if config_dict is None else config_dict)
    session = SimpleNamespace(
        address=address,
        connection_key=connection_key,
        name=config.name,
        qlcp_config=config,
        controls={control.name.upper(): control for control in config.controls_by_id.values()},
        monitor_task=None,
        heartbeat_task=None,
        driver=FakeDriver(),
        last_sync_time=None,
        missed_heartbeat_count=0,
        HEARTBEAT_ACK_MISS_LIMIT=3,
        is_connected=True,
        core_source=None,
    )

    def control_name_for_id(control_id: int | None) -> str | None:
        if control_id is None:
            return None
        control = config.controls_by_id.get(control_id)
        return None if control is None else control.name

    def record_heartbeat_ack(command: object | None) -> None:
        if command is not None:
            session.missed_heartbeat_count = 0

    def register_missed_heartbeat() -> bool:
        session.missed_heartbeat_count += 1
        return session.missed_heartbeat_count >= session.HEARTBEAT_ACK_MISS_LIMIT

    def close() -> None:
        session.is_connected = False

    session.control_name_for_id = control_name_for_id
    session.record_heartbeat_ack = record_heartbeat_ack
    session.register_missed_heartbeat = register_missed_heartbeat
    session.close = close
    return cast(ESPDeviceSession, session)


def test_runtime_registers_valid_device() -> None:
    async def run() -> None:
        runtime, _tracker, state, stream = _make_runtime()
        server_sock, peer_sock = socket.socketpair()
        server_sock.setblocking(False)
        peer_sock.setblocking(False)

        try:
            config = _make_config()
            session = await runtime.register_configured_device(
                server_sock,
                "10.0.0.2",
                config,
                ConfigPacket(
                    header=PacketHeader(
                        sequence=12,
                        timestamp_us=0,
                    ),
                    config_json=orjson.dumps(config).decode(),
                ),
            )

            assert isinstance(session, ESPDeviceSession)
            assert session.connection_key == "esp-1"
            assert session.monitor_task is not None
            assert session.heartbeat_task is not None
            assert runtime.devices.by_address("10.0.0.2") is session
            assert state.snapshot()["devices"][0]["name"] == "TEST-DEVICE"
            assert stream.events[0]["type"] == "device.registered"
        finally:
            runtime.close_all()
            peer_sock.close()
            await asyncio.sleep(0)

    asyncio.run(run())


def test_accept_connection_registers_device_on_config() -> None:
    async def run() -> None:
        runtime, _tracker, state, stream = _make_runtime()
        server_sock, peer_sock = socket.socketpair()
        server_sock.setblocking(False)
        peer_sock.setblocking(False)

        try:
            peer_sock.sendall(ConfigPacket.create(orjson.dumps(_make_config()).decode("utf-8")).encode())

            session = await runtime.accept_connection(server_sock, "10.0.0.2")

            assert isinstance(session, ESPDeviceSession)
            assert runtime.devices.by_address("10.0.0.2") is session
            assert state.snapshot()["devices"][0]["name"] == "TEST-DEVICE"
            assert stream.events[0]["type"] == "device.registered"
        finally:
            runtime.close_all()
            peer_sock.close()
            await asyncio.sleep(0)

    asyncio.run(run())


def test_accept_connection_closes_socket_when_peer_disconnects_before_config() -> None:
    async def run() -> None:
        runtime, _tracker, _state, _stream = _make_runtime()
        server_sock, peer_sock = socket.socketpair()
        server_sock.setblocking(False)
        peer_sock.setblocking(False)
        peer_sock.close()  # peer gone before sending CONFIG

        result = await runtime.accept_connection(server_sock, "10.0.0.2")

        assert result is None
        assert runtime.devices.by_address("10.0.0.2") is None
        assert server_sock.fileno() == -1  # accept_connection closed the socket

    asyncio.run(run())


def test_accept_connection_closes_socket_on_non_config_first_packet() -> None:
    async def run() -> None:
        runtime, _tracker, _state, _stream = _make_runtime()
        server_sock, peer_sock = socket.socketpair()
        server_sock.setblocking(False)
        peer_sock.setblocking(False)

        try:
            # A device-sent, server-decodable packet that is not CONFIG.
            peer_sock.sendall(AckPacket.create(_heartbeat(sequence=1)).encode())

            result = await runtime.accept_connection(server_sock, "10.0.0.2")

            assert result is None
            assert runtime.devices.by_address("10.0.0.2") is None
            assert server_sock.fileno() == -1
        finally:
            peer_sock.close()
            await asyncio.sleep(0)

    asyncio.run(run())


def test_accept_connection_times_out_when_no_config_arrives(monkeypatch: Any) -> None:
    async def run() -> None:
        monkeypatch.setattr(
            "vector.runtime.esp_connection_runtime.CONFIG_HANDSHAKE_TIMEOUT_S",
            0.05,
        )
        runtime, _tracker, _state, _stream = _make_runtime()
        server_sock, peer_sock = socket.socketpair()
        server_sock.setblocking(False)
        peer_sock.setblocking(False)

        try:
            # Peer connects but never sends CONFIG.
            result = await runtime.accept_connection(server_sock, "10.0.0.2")

            assert result is None
            assert runtime.devices.by_address("10.0.0.2") is None
            assert server_sock.fileno() == -1
        finally:
            peer_sock.close()
            await asyncio.sleep(0)

    asyncio.run(run())


def test_heartbeat_loop_removes_device_when_expiry_raises() -> None:
    async def run() -> None:
        runtime, _tracker, state, stream = _make_runtime()
        session = _make_session(runtime)
        runtime.devices.register(session)
        runtime.state_adapter.register_device(session, control_handler=partial(runtime._write_control, session))

        async def _raise(_session: ESPDeviceSession) -> bool:
            raise BrokenPipeError

        runtime.expire_command_timeouts = _raise  # type: ignore[method-assign, assignment]

        # Must not raise: the guard removes the device and exits the loop.
        await runtime._heartbeat_session(session)

        assert runtime.devices.by_address(session.address) is None
        assert any(event["type"] == "device.disconnected" for event in stream.events)

    asyncio.run(run())


def test_first_missed_heartbeat_event_reports_miss_count() -> None:
    runtime, tracker, state, stream = _make_runtime()
    session = _make_session(runtime)
    runtime.devices.register(session)
    runtime.state_adapter.register_device(session, control_handler=partial(runtime._write_control, session))

    command = tracker.mark_sent(
        connection_key=session.connection_key,
        device_name=session.name,
        device_address=session.address,
        packet_type=PacketType.HEARTBEAT,
        packet_sequence=1,
        now=0.0,
    )

    removed = runtime._handle_missed_heartbeat(session, command)

    assert removed is False
    event = stream.events[-1]
    assert event["type"] == "heartbeat.updated"
    heartbeat = cast(dict[str, Any], event["heartbeat"])
    assert heartbeat["consecutive_misses"] == 1
    assert heartbeat["state"] == "missed"


def test_runtime_replaces_existing_device_and_fails_pending_commands() -> None:
    runtime, tracker, state, stream = _make_runtime()
    old_device = _make_session(runtime, address="10.0.0.2", connection_key="conn-old")
    other_device = _make_session(runtime, address="10.0.0.4", connection_key="conn-other", name="OTHER")
    runtime.devices.register(old_device)
    runtime.devices.register(other_device)
    runtime.state_adapter.register_device(old_device, control_handler=partial(runtime._write_control, old_device))
    pending = tracker.mark_sent(
        connection_key=old_device.connection_key,
        device_name=old_device.name,
        device_address=old_device.address,
        packet_type=PacketType.CONTROL,
        packet_sequence=7,
        now=10.0,
    )

    runtime.disconnect_registered_devices_with_name(old_device.name)

    assert runtime.devices.by_address(old_device.address) is None
    assert runtime.devices.by_address(other_device.address) is other_device
    assert pending.state == CommandLifecycle.TIMED_OUT
    assert tracker.pending == ()
    assert stream.events[-1]["type"] == "device.disconnected"


def test_runtime_removal_marks_device_disconnected() -> None:
    runtime, _tracker, state, stream = _make_runtime()
    device = _make_session(runtime)
    runtime.devices.register(device)
    runtime.state_adapter.register_device(device, control_handler=partial(runtime._write_control, device))

    runtime.remove_device(device)

    assert runtime.devices.by_address(device.address) is None
    assert state.snapshot()["devices"][0]["connected"] is False
    assert stream.events[-1]["type"] == "device.disconnected"


def test_control_ack_completes_the_command_without_recording_control_state() -> None:
    runtime, tracker, state, stream = _make_runtime()
    device = _make_session(runtime)
    runtime.devices.register(device)
    runtime.state_adapter.register_device(device, control_handler=partial(runtime._write_control, device))
    command = tracker.mark_sent(
        connection_key=device.connection_key,
        device_name=device.name,
        device_address=device.address,
        packet_type=PacketType.CONTROL,
        packet_sequence=12,
        now=10.0,
        control_id=0,
        control_name="VALVE1",
        requested_state=ControlState.OPEN,
    )

    runtime.handle_ack(
        device,
        AckPacket(
            header=PacketHeader(
                sequence=20,
                timestamp_us=0,
            ),
            ack_packet_type=PacketType.CONTROL,
            ack_sequence=12,
        ),
    )

    assert command.state == CommandLifecycle.ACKED
    # Only STATUS reports control state; an ACK says the packet arrived, not where the valve is.
    control = state.snapshot()["devices"][0]["controls"][0]
    assert control["reported_state"] is None
    assert "accepted_state" not in control
    assert stream.events[-1]["type"] == "command.acked"


def test_runtime_nack_routes_through_tracker_without_control_update() -> None:
    runtime, tracker, state, stream = _make_runtime()
    device = _make_session(runtime)
    runtime.devices.register(device)
    runtime.state_adapter.register_device(device, control_handler=partial(runtime._write_control, device))
    command = tracker.mark_sent(
        connection_key=device.connection_key,
        device_name=device.name,
        device_address=device.address,
        packet_type=PacketType.CONTROL,
        packet_sequence=12,
        now=10.0,
        control_id=0,
        control_name="VALVE1",
        requested_state=ControlState.OPEN,
    )

    runtime.handle_nack(
        device,
        NackPacket(
            header=PacketHeader(
                sequence=12,
                timestamp_us=0,
            ),
            nack_packet_type=PacketType.CONTROL,
            nack_sequence=12,
            error_code=ErrorCode.INVALID_PARAM,
        ),
    )

    assert command.state == CommandLifecycle.NACKED
    assert state.snapshot()["devices"][0]["controls"][0]["reported_state"] is None
    assert stream.events[-1]["type"] == "command.nacked"


def test_set_control_sends_integer_state_for_variable_control() -> None:
    async def run() -> None:
        runtime, _tracker, _state, _stream = _make_runtime()
        device = _make_session(runtime)
        runtime.state_adapter.register_device(device, control_handler=partial(runtime._write_control, device))

        assert await runtime.set_control(device, "HEATER1", "75") is True

        packet = _sent_packets(device)[-1]
        assert isinstance(packet, ControlPacket)
        assert packet.control_type == ControlType.UINT32
        assert packet.control_state == 75

    asyncio.run(run())


def test_set_control_sends_float_state_for_variable_control() -> None:
    async def run() -> None:
        runtime, _tracker, _state, _stream = _make_runtime()
        device = _make_session(runtime)
        runtime.state_adapter.register_device(device, control_handler=partial(runtime._write_control, device))

        assert await runtime.set_control(device, "HEATER2", "62.5") is True

        packet = _sent_packets(device)[-1]
        assert isinstance(packet, ControlPacket)
        assert packet.control_type == ControlType.FLOAT32
        assert packet.control_state == 62.5

    asyncio.run(run())


def test_set_control_rejects_non_numeric_state_for_variable_control() -> None:
    async def run() -> None:
        runtime, _tracker, _state, _stream = _make_runtime()
        device = _make_session(runtime)

        assert await runtime.set_control(device, "HEATER1", "OPEN") is False
        assert _sent_packets(device) == []

    asyncio.run(run())


def test_set_control_rejects_numeric_state_for_bool_control() -> None:
    async def run() -> None:
        runtime, _tracker, _state, _stream = _make_runtime()
        device = _make_session(runtime)

        assert await runtime.set_control(device, "VALVE1", "75") is False
        assert _sent_packets(device) == []

    asyncio.run(run())


def test_runtime_status_updates_reported_control_state() -> None:
    runtime, _tracker, state, stream = _make_runtime()
    device = _make_session(runtime)
    runtime.devices.register(device)
    runtime.state_adapter.register_device(device, control_handler=partial(runtime._write_control, device))

    runtime.handle_status(
        device,
        StatusPacket(
            header=PacketHeader(
                sequence=1,
                timestamp_us=0,
            ),
            ack_packet_type=PacketType.STATUS_REQUEST,
            ack_sequence=1,
            control_states=[ControlStatus(id=0, type=ControlType.BOOL, state=ControlState.OPEN)],
        ),
    )

    assert state.snapshot()["devices"][0]["controls"][0]["reported_state"] == "OPEN"
    assert stream.events[-1]["type"] == "control.updated"
    assert state.snapshot()["devices"][0]["controls"][0]["reported_status"] == "confirmed"
    assert state.snapshot()["devices"][0]["controls"][0]["settled"] is True


def test_runtime_unsolicited_status_skips_ack_tracking() -> None:
    """An unsolicited STATUS (ack_packet_type=NO_ACK) must not resolve a pending command."""
    runtime, tracker, state, stream = _make_runtime()
    device = _make_session(runtime)
    runtime.devices.register(device)
    runtime.state_adapter.register_device(device, control_handler=partial(runtime._write_control, device))

    pending = tracker.mark_sent(
        connection_key=device.connection_key,
        device_name=device.name,
        device_address=device.address,
        packet_type=PacketType.CONTROL,
        packet_sequence=1,
        now=0.0,
    )

    runtime.handle_status(
        device,
        StatusPacket(
            header=PacketHeader(sequence=2, timestamp_us=0),
            ack_packet_type=PacketType.NO_ACK,
            ack_sequence=1,  # deliberately matches the pending command's sequence
            control_states=[ControlStatus(id=0, type=ControlType.BOOL, state=ControlState.OPEN)],
        ),
    )

    assert tracker.pending == (pending,)
    assert state.snapshot()["devices"][0]["controls"][0]["reported_state"] == "OPEN"
    assert not any(event["type"] == "command.acked" for event in stream.events)


def test_runtime_status_error_preserves_last_known_state() -> None:
    runtime, _tracker, state, stream = _make_runtime()
    device = _make_session(runtime)
    runtime.devices.register(device)
    runtime.state_adapter.register_device(device, control_handler=partial(runtime._write_control, device))

    runtime.handle_status(
        device,
        StatusPacket(
            header=PacketHeader(sequence=1, timestamp_us=0),
            ack_packet_type=PacketType.STATUS_REQUEST,
            ack_sequence=1,
            control_states=[ControlStatus(id=0, type=ControlType.BOOL, state=ControlState.OPEN)],
        ),
    )
    runtime.handle_status(
        device,
        StatusPacket(
            header=PacketHeader(sequence=2, timestamp_us=0),
            ack_packet_type=PacketType.STATUS_REQUEST,
            ack_sequence=2,
            control_states=[ControlStatus(id=0, type=ControlType.BOOL, state=None, status=ControlConfirmStatus.ERROR)],
        ),
    )

    control = state.snapshot()["devices"][0]["controls"][0]
    assert control["reported_state"] == "OPEN"  # preserved, not clobbered by the undefined error state
    assert control["reported_status"] == "error"
    # "settled" only reflects the pending/waiting concept; error is a distinct fault signal
    # surfaced via reported_status, not folded into the same spinner semantics.
    assert control["settled"] is True
    assert stream.events[-1]["type"] == "control.error"


def test_runtime_status_with_mismatched_type_marks_error_and_keeps_the_node() -> None:
    """STATUS carries its own per-control type; one that disagrees with CONFIG must not drop the session."""
    runtime, _tracker, state, stream = _make_runtime()
    device = _make_session(runtime)
    runtime.devices.register(device)
    runtime.state_adapter.register_device(device)

    runtime.handle_status(
        device,
        StatusPacket(
            header=PacketHeader(sequence=1, timestamp_us=0),
            ack_packet_type=PacketType.NO_ACK,
            ack_sequence=0,
            control_states=[
                ControlStatus(id=0, type=ControlType.UINT32, state=1),  # VALVE1 is declared BOOL
                ControlStatus(id=1, type=ControlType.UINT32, state=75),
            ],
        ),
    )

    valve, heater, _ = state.snapshot()["devices"][0]["controls"]
    assert (valve["reported_state"], valve["reported_status"]) == (None, "error")
    assert (heater["reported_state"], heater["reported_status"]) == ("75", "confirmed")
    assert runtime.devices.by_address(device.address) is device
    assert [event["type"] for event in stream.events[-2:]] == ["control.error", "control.updated"]


def test_runtime_status_pending_control_is_not_settled() -> None:
    runtime, _tracker, state, _stream = _make_runtime()
    device = _make_session(runtime)
    runtime.devices.register(device)
    runtime.state_adapter.register_device(device, control_handler=partial(runtime._write_control, device))

    runtime.handle_status(
        device,
        StatusPacket(
            header=PacketHeader(sequence=1, timestamp_us=0),
            ack_packet_type=PacketType.STATUS_REQUEST,
            ack_sequence=1,
            control_states=[ControlStatus(id=0, type=ControlType.BOOL, state=ControlState.OPEN, status=ControlConfirmStatus.PENDING)],
        ),
    )

    control = state.snapshot()["devices"][0]["controls"][0]
    assert control["reported_state"] == "OPEN"
    assert control["reported_status"] == "pending"
    assert control["settled"] is False


def test_runtime_command_visibility_policy_for_status_request_and_estop() -> None:
    async def run() -> None:
        runtime, tracker, _state, stream = _make_runtime()
        device = _make_session(runtime)

        status_request = await runtime.send_tracked_command(
            device,
            StatusRequestPacket(
                header=PacketHeader(
                    sequence=30,
                    timestamp_us=0,
                ),
            ),
        )
        estop = await runtime.send_tracked_command(
            device,
            EstopPacket(
                header=PacketHeader(
                    sequence=31,
                    timestamp_us=0,
                ),
            ),
        )

        assert status_request.ack_expected is False
        assert estop.ack_expected is False
        assert tracker.pending == ()
        assert tracker.recent_completed == (estop,)
        assert stream.events == [
            {
                "type": "command.sent",
                "state_version": 1,
                "command": {
                    "command_id": estop.command_id,
                    "source_provider": "qlcp",
                    "source_key": device.name,
                    "connection_key": device.connection_key,
                    "device_address": device.address,
                    "device_name": device.name,
                    "packet_type": "ESTOP",
                    "sequence": 31,
                    "state": "sent",
                    "sent_at": estop.sent_at,
                    "ack_expected": False,
                    "acked_at": None,
                    "nacked_at": None,
                    "timed_out_at": None,
                    "nack_error_code": None,
                    "control_id": None,
                    "control_name": None,
                    "requested_state": None,
                },
            },
        ]

    asyncio.run(run())


def test_status_packet_with_no_controls_does_not_error(caplog: Any) -> None:
    import logging

    runtime, _tracker, state, stream = _make_runtime()
    device = _make_session(runtime)
    runtime.devices.register(device)
    runtime.state_adapter.register_device(device, control_handler=partial(runtime._write_control, device))

    empty_status = StatusPacket(
        header=PacketHeader(
            sequence=1,
            timestamp_us=0,
        ),
        ack_packet_type=PacketType.STATUS_REQUEST,
        ack_sequence=1,
        control_states=[],  # sensors-only board
    )

    with caplog.at_level(logging.ERROR):
        asyncio.run(runtime.handle_packet(device, empty_status))

    # Must not log an "unexpected packet type" error.
    assert not any("unexpected packet type" in record.message.lower() for record in caplog.records), (
        f"Unexpected error log: {[r.message for r in caplog.records]}"
    )


def test_remove_device_cleanup_before_mark_disconnected() -> None:
    close_called_at: list[str] = []

    runtime, _tracker, state, stream = _make_runtime()
    device = _make_session(runtime)
    runtime.devices.register(device)
    runtime.state_adapter.register_device(device, control_handler=partial(runtime._write_control, device))

    # Patch cleanup_device and mark_disconnected to record call order.
    original_cleanup = runtime.cleanup_device
    original_mark = runtime.state_adapter.mark_disconnected

    def recording_cleanup(session: Any, *, reason: str = "connection_cleanup") -> None:
        close_called_at.append("cleanup")
        original_cleanup(session, reason=reason)

    def recording_mark(session: Any) -> Any:
        close_called_at.append("mark_disconnected")
        return original_mark(session)

    runtime.cleanup_device = recording_cleanup  # type: ignore[assignment]
    runtime.state_adapter.mark_disconnected = recording_mark  # type: ignore[assignment]

    runtime.remove_device(device)

    assert close_called_at == ["cleanup", "mark_disconnected"], f"Wrong teardown order: {close_called_at}"


def test_close_all_cleanup_before_mark_disconnected() -> None:
    close_called_at: list[str] = []

    runtime, _tracker, state, stream = _make_runtime()
    device = _make_session(runtime)
    runtime.devices.register(device)
    runtime.state_adapter.register_device(device, control_handler=partial(runtime._write_control, device))

    original_cleanup = runtime.cleanup_device
    original_mark = runtime.state_adapter.mark_disconnected

    def recording_cleanup(session: Any, *, reason: str = "connection_cleanup") -> None:
        close_called_at.append("cleanup")
        original_cleanup(session, reason=reason)

    def recording_mark(session: Any) -> Any:
        close_called_at.append("mark_disconnected")
        return original_mark(session)

    runtime.cleanup_device = recording_cleanup  # type: ignore[assignment]
    runtime.state_adapter.mark_disconnected = recording_mark  # type: ignore[assignment]

    runtime.close_all()

    assert close_called_at == ["cleanup", "mark_disconnected"], f"Wrong teardown order in close_all: {close_called_at}"
    assert runtime.devices.snapshot_by_address() == {}, "Devices registry was not cleared"


def test_disconnection_metric_recorded_on_remove_device() -> None:
    from vector.runtime.metrics import Metrics

    tracker = CommandTracker()
    state = SystemState(core=Core())
    stream = FakeStateStream()
    metrics = Metrics()
    runtime = ESPConnectionRuntime(
        command_tracker=tracker,
        system_state=state,
        state_stream=stream,
        metrics=metrics,
    )
    device = _make_session(runtime)
    runtime.devices.register(device)
    runtime.state_adapter.register_device(device, control_handler=partial(runtime._write_control, device))

    runtime.remove_device(device)

    metrics_dict = metrics.to_dict()
    device_lifecycle = metrics_dict["device_lifecycle"]
    assert isinstance(device_lifecycle, dict)
    assert device_lifecycle["disconnections_total"] == {"connection_cleanup": 1}


def test_runtime_monitor_routes_packets_to_packet_handler() -> None:
    async def run() -> None:
        packet_seen = asyncio.Event()
        packets: list[object] = []
        runtime, _tracker, _state, _stream = _make_runtime()

        async def recording_handle_packet(session: ESPDeviceSession, packet: object) -> None:
            packets.append(packet)
            packet_seen.set()

        session_sock, peer_sock = socket.socketpair()
        session_sock.setblocking(False)
        peer_sock.setblocking(False)
        session = ESPDeviceSession(
            session_sock,
            "10.0.0.2",
            _make_config(),
            connection_key="conn-a",
        )
        runtime.handle_packet = recording_handle_packet  # type: ignore[method-assign]
        task = asyncio.create_task(runtime._monitor_session(session))

        try:
            loop = asyncio.get_running_loop()
            packet = AckPacket.create(_heartbeat(sequence=7))
            await loop.sock_sendall(peer_sock, packet.encode())
            await asyncio.wait_for(packet_seen.wait(), timeout=1)

            assert isinstance(packets[0], AckPacket)
            assert packets[0].ack_packet_type == PacketType.HEARTBEAT
            assert packets[0].ack_sequence == 7
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            session_sock.close()
            peer_sock.close()

    asyncio.run(run())



def test_core_dispatches_each_control_to_its_wire_id() -> None:
    async def run() -> None:
        runtime, tracker, state, _stream = _make_runtime()
        config = _make_config()
        config["controls"] = {
            "valve": {"AV101": {"type": "BOOL", "default_state": "CLOSED"}},
            "relay": {"RL101": {"type": "BOOL", "default_state": "CLOSED"}},
        }
        device = _make_session(runtime, config_dict=config)
        runtime.state_adapter.register_device(device, control_handler=partial(runtime._write_control, device))
        targets = device.core_source.controls

        command_ids = [await state.core.set_control(target, True) for target in targets]

        packets = _sent_packets(device)
        assert len(packets) == 2
        assert all(isinstance(packet, ControlPacket) for packet in packets)
        decoded = [decode_packet_client(cast("ControlPacket", packet).encode()) for packet in packets]
        assert all(isinstance(packet, ControlPacket) for packet in decoded)
        controls = [cast("ControlPacket", packet) for packet in decoded]
        assert [packet.control_id for packet in controls] == [0, 1]
        assert [packet.control_state for packet in controls] == [ControlState.OPEN, ControlState.OPEN]
        assert [command.control_id for command in tracker.pending] == [0, 1]
        assert command_ids == [command.command_id for command in tracker.pending]
        assert len(set(command_ids)) == 2

        runtime.handle_ack(device, AckPacket.create(cast("ControlPacket", packets[0])))
        assert [command.control_id for command in tracker.pending] == [1]
        runtime.handle_ack(device, AckPacket.create(cast("ControlPacket", packets[1])))
        assert tracker.pending == ()

    asyncio.run(run())


def test_core_dispatch_preserves_each_bindings_wire_type() -> None:
    async def run() -> None:
        runtime, _tracker, state, _stream = _make_runtime()
        config = _make_config()
        config["controls"] = {
            "valve": {"AV101": {"type": "BOOL", "default_state": "CLOSED"}},
            "heater": {"HTR101": {"type": "INT32", "default_state": "0"}},
        }
        device = _make_session(runtime, config_dict=config)
        runtime.state_adapter.register_device(device, control_handler=partial(runtime._write_control, device))
        boolean, integer = device.core_source.controls

        await state.core.set_control(boolean, True)
        await state.core.set_control(integer, -25)

        decoded = [decode_packet_client(cast("ControlPacket", packet).encode()) for packet in _sent_packets(device)]
        assert all(isinstance(packet, ControlPacket) for packet in decoded)
        controls = [cast("ControlPacket", packet) for packet in decoded]
        assert [(packet.control_id, packet.control_type, packet.control_state) for packet in controls] == [
            (0, ControlType.BOOL, ControlState.OPEN),
            (1, ControlType.INT32, -25),
        ]

        # The existing name-only entry point resolves case-insensitively within the node.
        assert await runtime.set_control(device, "htr101", "15")
        last_packet = cast("ControlPacket", _sent_packets(device)[-1])
        assert last_packet.control_id == 1
        assert last_packet.control_state == 15

    asyncio.run(run())


def test_out_of_range_setpoint_is_refused_without_dropping_the_node() -> None:
    async def run() -> None:
        runtime, tracker, state, _stream = _make_runtime()
        device = _make_session(runtime)
        runtime.devices.register(device)
        runtime.state_adapter.register_device(device, control_handler=partial(runtime._write_control, device))
        heater = device.core_source.control("HEATER1")  # declared UINT32

        for value in (-1, 2**32):
            with pytest.raises(ControlValidationError, match="UINT32"):
                await state.core.set_control(heater, value)

        assert _sent_packets(device) == []
        assert tracker.pending == ()
        assert runtime.devices.by_address(device.address) is device
        assert device.core_source.connected

    asyncio.run(run())
