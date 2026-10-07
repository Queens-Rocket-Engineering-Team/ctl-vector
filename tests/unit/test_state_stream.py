from __future__ import annotations
import asyncio
import contextlib
from typing import Any, cast

import orjson
from fastapi import WebSocket

from tests.unit.test_system_state import _make_device
from tests.unit.test_system_state import _make_state as _state_fixture
from vector.core import ControlDefinition
from vector.runtime.state_stream import STATE_STREAM_MAX_QUEUE, StateStream
from vector.state.system_state import SystemState


class FakeWebSocket:
    def __init__(self, *, fail_after: int | None = None) -> None:
        self.accepted = False
        self.closed = False
        self.sent: list[dict[str, Any]] = []
        self._fail_after = fail_after
        self._send_count = 0

    async def accept(self) -> None:
        self.accepted = True

    async def send_text(self, data: str) -> None:
        if self._fail_after is not None and self._send_count >= self._fail_after:
            raise RuntimeError("websocket send failed")

        self._send_count += 1
        self.sent.append(orjson.loads(data))

    async def close(self) -> None:
        self.closed = True

    async def receive_text(self) -> str:
        # Block until the task is cancelled (simulates an open client that sends nothing).
        await asyncio.sleep(3600)
        return ""


def _as_websocket(websocket: FakeWebSocket) -> WebSocket:
    return cast(WebSocket, websocket)


def _make_state() -> SystemState:
    state, _, _, _ = _state_fixture()
    return state


async def _wait_for(condition: Any, *, timeout: float = 1.0, tick: float = 0.01) -> bool:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if condition():
            return True
        await asyncio.sleep(tick)
    return False


# ---------------------------------------------------------------------------
# connect_client / snapshot
# ---------------------------------------------------------------------------


def test_connect_client_sends_initial_snapshot() -> None:
    async def run() -> None:
        state, _, qlcp, _ = _state_fixture()
        qlcp.register_device(_make_device())
        stream = StateStream(state)
        websocket = FakeWebSocket()

        await stream.connect_client(_as_websocket(websocket))

        assert websocket.accepted is True
        assert stream.client_count == 1
        # Snapshot is in the queue, not yet sent (delivered via handle_client drain)

    asyncio.run(run())


def test_connect_client_increments_count() -> None:
    async def run() -> None:
        state = _make_state()
        stream = StateStream(state)
        ws_a = FakeWebSocket()
        ws_b = FakeWebSocket()

        await stream.connect_client(_as_websocket(ws_a))
        assert stream.client_count == 1

        await stream.connect_client(_as_websocket(ws_b))
        assert stream.client_count == 2

    asyncio.run(run())


# ---------------------------------------------------------------------------
# Ordering: snapshot first, events in order
# ---------------------------------------------------------------------------


def test_publish_events_are_ordered_after_snapshot() -> None:

    async def run() -> None:
        state, _, qlcp, _ = _state_fixture()
        qlcp.register_device(_make_device())
        stream = StateStream(state)
        ws = FakeWebSocket()

        client_task = asyncio.create_task(stream.handle_client(_as_websocket(ws)))
        # Yield to let handle_client start and call connect_client.
        await asyncio.sleep(0)

        event1 = {"type": "event.one", "state_version": 10}
        event2 = {"type": "event.two", "state_version": 11}
        stream.publish(event1)
        stream.publish(event2)

        # Wait for exactly 3 messages: snapshot + 2 events.
        reached = await _wait_for(lambda: len(ws.sent) >= 3)
        client_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await client_task

        assert reached, f"Expected 3 messages; got {ws.sent}"
        assert ws.sent[0]["type"] == "state.snapshot", f"First message was {ws.sent[0]['type']!r}"
        assert ws.sent[1]["type"] == "event.one", f"Second message was {ws.sent[1]['type']!r}"
        assert ws.sent[2]["type"] == "event.two", f"Third message was {ws.sent[2]['type']!r}"

    asyncio.run(run())


def test_publish_none_is_ignored() -> None:
    async def run() -> None:
        state = _make_state()
        stream = StateStream(state)
        ws = FakeWebSocket()
        await stream.connect_client(_as_websocket(ws))

        stream.publish(None)
        assert stream.client_count == 1

    asyncio.run(run())


def test_same_label_sources_remain_distinct_in_serialized_snapshot_and_updates() -> None:
    async def run() -> None:
        state = _make_state()
        sources = [
            state.core.register_source(provider, "sensor", name="Shared label", controls=[ControlDefinition("VALVE")])
            for provider in ("a", "b")
        ]
        stream = StateStream(state)
        state.set_publisher(stream.publish)
        ws = FakeWebSocket()
        client_task = asyncio.create_task(stream.handle_client(_as_websocket(ws)))
        try:
            assert await _wait_for(lambda: len(ws.sent) == 1)
            sources[0].report_control("VALVE", False)
            sources[1].report_control("VALVE", True)
            assert await _wait_for(lambda: len(ws.sent) == 3)

            devices = ws.sent[0]["state"]["devices"]
            assert [(device["source_provider"], device["source_key"]) for device in devices] == [("a", "sensor"), ("b", "sensor")]
            assert [device["name"] for device in devices] == ["Shared label", "Shared label"]
            updates = ws.sent[1:]
            assert [event["source_provider"] for event in updates] == ["a", "b"]
            assert [event["source_key"] for event in updates] == ["sensor", "sensor"]
            assert [event["connection_key"] for event in updates] == [source.connection_key for source in sources]
            assert [event["control"]["reported_state"] for event in updates] == ["CLOSED", "OPEN"]
        finally:
            client_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await client_task

    asyncio.run(run())


def test_publish_with_no_clients_is_a_noop() -> None:
    async def run() -> None:
        state = _make_state()
        stream = StateStream(state)
        # No clients connected — should not raise.
        stream.publish({"type": "device.disconnected", "state_version": 1})

    asyncio.run(run())


# ---------------------------------------------------------------------------
# Overflow: slow client is disconnected
# ---------------------------------------------------------------------------


def test_overflow_disconnects_client() -> None:

    async def run() -> None:
        state = _make_state()
        stream = StateStream(state)
        ws = FakeWebSocket()
        await stream.connect_client(_as_websocket(ws))

        assert stream.client_count == 1

        # Queue has 1 item (snapshot). Fill remaining slots.
        for i in range(STATE_STREAM_MAX_QUEUE - 1):
            stream.publish({"type": "event", "state_version": i})

        assert stream.client_count == 1, "Client should still be connected before overflow"

        # This one pushes past the limit.
        stream.publish({"type": "overflow", "state_version": 9999})

        assert stream.client_count == 0, "Client should be disconnected on queue overflow"

    asyncio.run(run())


def test_overflow_sentinel_unblocks_drain_loop() -> None:

    async def run() -> None:
        state = _make_state()
        stream = StateStream(state)
        ws = FakeWebSocket()  # healthy socket — all sends succeed

        client_task = asyncio.create_task(stream.handle_client(_as_websocket(ws)))

        # Wait for the drain loop to process the snapshot and block on queue.get().
        # At that point the queue is empty and the drain task is suspended on await.
        reached = await _wait_for(lambda: len(ws.sent) >= 1)
        assert reached, "Snapshot was never delivered; drain loop did not start"
        assert stream.client_count == 1

        # Fill the queue completely (STATE_STREAM_MAX_QUEUE slots free; drain is blocked).
        # All publish() calls are synchronous — the drain task cannot interleave.
        for i in range(STATE_STREAM_MAX_QUEUE):
            stream.publish({"type": "event", "state_version": i})

        # One more publish overflows: _disconnect_overflow_client clears the queue and
        # puts _DRAIN_SHUTDOWN.  The blocked drain task wakes, sees the sentinel, and exits.
        stream.publish({"type": "overflow", "state_version": 9999})

        assert stream.client_count == 0, "Client not removed on overflow"

        # handle_client should complete within a short timeout.
        # A hang here means the sentinel did not unblock the drain loop.
        try:
            await asyncio.wait_for(client_task, timeout=2.0)
        except asyncio.TimeoutError:
            client_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await client_task
            raise AssertionError(
                "handle_client hung after overflow — _DRAIN_SHUTDOWN sentinel did not "
                "unblock the drain loop"
            )

    asyncio.run(run())


# ---------------------------------------------------------------------------
# disconnect_client / client removal
# ---------------------------------------------------------------------------


def test_disconnect_client_removes_client() -> None:
    async def run() -> None:
        state = _make_state()
        stream = StateStream(state)
        ws = FakeWebSocket()
        await stream.connect_client(_as_websocket(ws))

        assert stream.client_count == 1
        await stream.disconnect_client(_as_websocket(ws))
        assert stream.client_count == 0
        assert ws.closed is True

    asyncio.run(run())


def test_disconnect_client_is_idempotent() -> None:
    async def run() -> None:
        state = _make_state()
        stream = StateStream(state)
        ws = FakeWebSocket()
        await stream.connect_client(_as_websocket(ws))

        await stream.disconnect_client(_as_websocket(ws))
        await stream.disconnect_client(_as_websocket(ws))  # second call must not raise
        assert stream.client_count == 0

    asyncio.run(run())


def test_send_failure_removes_broken_client() -> None:

    async def run() -> None:
        state = _make_state()
        stream = StateStream(state)
        broken = FakeWebSocket(fail_after=0)  # fails on the very first send

        client_task = asyncio.create_task(stream.handle_client(_as_websocket(broken)))
        await asyncio.sleep(0)

        stream.publish({"type": "control.updated", "state_version": 1})

        reached = await _wait_for(lambda: stream.client_count == 0)
        assert reached, "Broken client was not removed after send failure"

        client_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await client_task

    asyncio.run(run())


# ---------------------------------------------------------------------------
# snapshot_message
# ---------------------------------------------------------------------------


def test_snapshot_message_reflects_current_state() -> None:
    async def run() -> None:
        state, _, qlcp, _ = _state_fixture()
        device = _make_device()
        qlcp.register_device(device)
        stream = StateStream(state)

        msg = stream.snapshot_message()

        assert msg["type"] == "state.snapshot"
        assert msg["state_version"] == 1
        assert msg["state"]["state_version"] == 1
        assert msg["state"]["devices"][0]["name"] == "TEST-DEVICE"

    asyncio.run(run())
