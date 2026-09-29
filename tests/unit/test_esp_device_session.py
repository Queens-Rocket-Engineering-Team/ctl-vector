from __future__ import annotations
import asyncio
import socket
from typing import Any, cast

import pytest

from vector.core import Core
from vector.qlcp.enums import ControlState, PacketType
from vector.runtime.command_tracker import CommandLifecycle, CommandTracker
from vector.runtime.esp_connection_runtime import ESPConnectionRuntime, ESPDeviceSession
from vector.state.system_state import SystemState


class FakeStateStream:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def publish(self, event: dict[str, object] | None) -> None:
        if event is not None:
            self.events.append(event)


def _make_config() -> dict[str, Any]:
    return {
        "device_name": "TEST-DEVICE",
        "device_type": "Sensor Monitor",
        "sensors": {},
        "controls": {},
    }


def _make_session() -> tuple[ESPDeviceSession, socket.socket]:
    session_sock, peer_sock = socket.socketpair()
    session_sock.setblocking(False)
    peer_sock.setblocking(False)
    session = ESPDeviceSession(
        session_sock,
        "10.0.0.2",
        _make_config(),
        connection_key="conn-a",
    )
    return session, peer_sock


def _make_runtime() -> tuple[ESPConnectionRuntime, CommandTracker, FakeStateStream]:
    tracker = CommandTracker()
    state_stream = FakeStateStream()
    system_state = SystemState(core=Core())
    system_state.set_publisher(state_stream.publish)
    runtime = ESPConnectionRuntime(
        command_tracker=tracker,
        system_state=system_state,
        state_stream=state_stream,
    )
    return runtime, tracker, state_stream

def test_heartbeat_ack_resets_missed_heartbeat_state() -> None:
    session, peer_sock = _make_session()
    _runtime, tracker, _stream = _make_runtime()
    try:
        command = tracker.mark_sent(
            connection_key=session.connection_key,
            device_name=session.name,
            device_address=session.address,
            packet_type=PacketType.HEARTBEAT,
            packet_sequence=1,
            now=10.0,
        )

        assert session.register_missed_heartbeat() is False
        session.record_heartbeat_ack(command)

        assert session.missed_heartbeat_count == 0
    finally:
        session.close()
        peer_sock.close()


def test_runtime_marks_session_unresponsive_at_heartbeat_miss_limit() -> None:
    session, peer_sock = _make_session()
    runtime, tracker, _stream = _make_runtime()
    runtime.devices.register(session)
    runtime.state_adapter.register_device(session)
    try:
        command = tracker.mark_sent(
            connection_key=session.connection_key,
            device_name=session.name,
            device_address=session.address,
            packet_type=PacketType.HEARTBEAT,
            packet_sequence=1,
            now=10.0,
        )

        for _ in range(session.HEARTBEAT_ACK_MISS_LIMIT - 1):
            assert runtime._handle_missed_heartbeat(session, command) is False

        assert runtime._handle_missed_heartbeat(session, command) is True
        assert runtime.devices.by_address(session.address) is None
    finally:
        session.close()
        peer_sock.close()


def test_heartbeat_loop_exits_when_send_heartbeat_fails() -> None:
    async def run() -> None:
        session, peer_sock = _make_session()

        class RuntimeStub:
            expire_calls = 0
            send_calls = 0

            async def expire_command_timeouts(self, session: ESPDeviceSession) -> bool:
                self.expire_calls += 1
                return False

            async def send_heartbeat(self, session: ESPDeviceSession) -> bool:
                self.send_calls += 1
                return False

        runtime = RuntimeStub()
        try:
            await ESPConnectionRuntime._heartbeat_session(cast(ESPConnectionRuntime, runtime), session)

            assert runtime.expire_calls == 1
            assert runtime.send_calls == 1
        finally:
            session.close()
            peer_sock.close()

    asyncio.run(run())


def test_expired_control_command_emits_timed_out_event(monkeypatch: pytest.MonkeyPatch) -> None:
    """A timed-out CONTROL (or STREAM_START/STOP) command must be visible on the state stream."""

    async def run() -> None:
        session, peer_sock = _make_session()
        runtime, tracker, stream = _make_runtime()
        monkeypatch.setattr(session, "COMMAND_ACK_TIMEOUT_S", 0.0)
        try:
            command = tracker.mark_sent(
                connection_key=session.connection_key,
                device_name=session.name,
                device_address=session.address,
                packet_type=PacketType.CONTROL,
                packet_sequence=5,
                now=0.0,
                control_id=0,
                control_name="VALVE1",
                requested_state=ControlState.OPEN,
            )

            removed = await runtime.expire_command_timeouts(session)

            assert removed is False
            assert command.state == CommandLifecycle.TIMED_OUT
            assert stream.events[-1]["type"] == "command.timed_out"
        finally:
            session.close()
            peer_sock.close()

    asyncio.run(run())
