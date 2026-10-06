from __future__ import annotations
import asyncio
import contextlib
import socket
from collections.abc import Callable
from types import SimpleNamespace
from typing import cast

from vector.core import Core, SensorDefinition, TelemetryBatch
from vector.qlcp.config_parser import parse_config
from vector.qlcp.packets import DataPacket, PacketHeader, SensorReading
from vector.runtime.esp_connection_runtime import ESPDeviceSession
from vector.runtime.telemetry_ingest import TelemetryRuntime


def _free_udp_port() -> int:
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]
    finally:
        probe.close()


def _listener() -> tuple[TelemetryRuntime, list[TelemetryBatch]]:
    core = Core()
    source = core.register_source(
        "qlcp", "MockDevice", address="127.0.0.1", connection_key="esp-1",
        sensors=(SensorDefinition("PT101", "pressure_transducer", "PSI"),),
    )
    config = parse_config({
        "device_name": "MockDevice",
        "sensors": {"pressure_transducer": {"PT101": {"unit": "PSI"}}},
        "controls": {},
    })
    session = cast("ESPDeviceSession", SimpleNamespace(
        name="MockDevice", address="127.0.0.1", connection_key="esp-1",
        qlcp_config=config, last_sync_time=1.0, core_source=source,
    ))
    batches: list[TelemetryBatch] = []
    core.subscribe_samples(batches.append)
    return TelemetryRuntime({session.address: session}.get), batches


async def _drive(listener: TelemetryRuntime, port: int, data: bytes, stop: Callable[[], bool]) -> None:
    """Send until the listener has processed a datagram, with a bounded timeout."""
    task = asyncio.create_task(listener.run_udp_listener(port=port))
    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        for _ in range(200):
            sender.sendto(data, ("127.0.0.1", port))
            await asyncio.sleep(0.02)
            if stop():
                break
    finally:
        sender.close()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def test_listener_publishes_decoded_batches_once() -> None:
    async def run() -> None:
        listener, batches = _listener()
        packet = DataPacket(
            header=PacketHeader(sequence=1, timestamp_us=1000000),
            readings=[SensorReading(sensor_id=0, value=12.5)],
        )
        await _drive(listener, _free_udp_port(), packet.encode(), stop=lambda: bool(batches))

        assert len(batches) == 1
        assert batches[0].source_name == "MockDevice"
        assert batches[0].source_address == "127.0.0.1"
        assert batches[0].readings[0].value == 12.5

    asyncio.run(run())


def test_listener_skips_publish_when_ingest_returns_none() -> None:
    async def run() -> None:
        listener, batches = _listener()
        received: list[str] = []
        handle_datagram = listener.handle_datagram

        def ingest(data: bytes, address: str) -> TelemetryBatch | None:
            received.append(address)
            return handle_datagram(data, address)

        listener.handle_datagram = ingest
        await _drive(listener, _free_udp_port(), b"invalid packet", stop=lambda: bool(received))

        assert received == ["127.0.0.1"]
        assert batches == []

    asyncio.run(run())
