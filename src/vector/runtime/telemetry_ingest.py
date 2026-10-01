"""QLCP UDP reception and translation into the shared core's sample interface."""

from __future__ import annotations
import asyncio
import logging
import socket
import time
from typing import TYPE_CHECKING

from vector.qlcp.decoding import decode_packet_server
from vector.qlcp.packets import DataPacket
from vector.runtime.metrics import Metrics


if TYPE_CHECKING:
    from collections.abc import Callable

    from vector.core import SensorBinding, TelemetryBatch, TimestampSource
    from vector.runtime.esp_connection_runtime import ESPDeviceSession


logger = logging.getLogger(__name__)
UDP_PORT = 50001
MICROSECONDS_PER_SECOND = 1_000_000


class TelemetryRuntime:
    """Receive QLCP data; the core owns taring and publication."""

    def __init__(
        self,
        device_for_address: Callable[[str], ESPDeviceSession | None],
        *,
        metrics: Metrics | None = None,
    ) -> None:
        self._device_for_address = device_for_address
        self.metrics = metrics or Metrics()

    def handle_datagram(self, data: bytes, address: str) -> TelemetryBatch | None:
        session = self._device_for_address(address)
        if session is None:
            self.metrics.record_telemetry_datagram(len(data))
            self.metrics.record_telemetry_unknown_source("unregistered_address")
            logger.error("Received UDP packet from unknown device %s", address)
            return None

        self.metrics.record_telemetry_datagram(len(data), device=session.name)
        try:
            packet = decode_packet_server(data)
        except Exception:
            self.metrics.record_telemetry_decode_error("decode")
            logger.exception("Error decoding UDP packet from %s", address)
            return None

        if not isinstance(packet, DataPacket):
            self.metrics.record_telemetry_decode_error("non_data")
            logger.error("Received non-DATA packet over UDP from %s. Ignoring.", session.name)
            return None
        return self.handle_packet(packet, session)

    def handle_packet(self, packet: DataPacket, session: ESPDeviceSession) -> TelemetryBatch | None:
        source = session.core_source
        if source is None or not source.connected:
            return None

        self.metrics.record_telemetry_data_packet(session.name)
        timestamp_s, timestamp_source, timestamp_synced = self._batch_timestamp(packet, session)
        samples: list[tuple[SensorBinding, float]] = []
        for reading in packet.readings:
            sensor = session.qlcp_config.sensors_by_id.get(reading.sensor_id)
            if sensor is None:
                self.metrics.record_telemetry_decode_error("unknown_sensor")
                logger.error("Received DATA reading for unknown sensor id %s from %s. Ignoring.", reading.sensor_id, session.name)
                continue
            samples.append((source.sensors[reading.sensor_id], reading.value))

        batch = source.publish_samples(
            samples,
            timestamp_s=timestamp_s,
            timestamp_source=timestamp_source,
            timestamp_synced=timestamp_synced,
        )
        if batch is not None:
            self.metrics.record_telemetry_readings(session.name, len(batch.readings))
        return batch

    @staticmethod
    def _batch_timestamp(packet: DataPacket, session: ESPDeviceSession) -> tuple[float, TimestampSource, bool]:
        if session.last_sync_time is None:
            return time.monotonic(), "server_receive", False
        return packet.header.timestamp_us / MICROSECONDS_PER_SECOND, "device_synced", True

    async def run_udp_listener(
        self,
        *,
        port: int = UDP_PORT,
        batch_size: int = 128,
        recv_buffer_bytes: int = 4 * 1024 * 1024,
    ) -> None:
        """Receive bounded groups of datagrams, yielding between groups."""
        loop = asyncio.get_running_loop()
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, recv_buffer_bytes)
            udp_socket.bind(("0.0.0.0", port))  # noqa: S104
            udp_socket.setblocking(False)
            logger.info("UDP listener started on port %s", port)

            while True:
                try:
                    data, addr = await loop.sock_recvfrom(udp_socket, 4096)
                    processed = 0
                    while True:
                        # Core ingestion publishes once for QLCP and other sources alike.
                        self.handle_datagram(data, addr[0])
                        processed += 1
                        if processed >= batch_size:
                            break
                        try:
                            data, addr = udp_socket.recvfrom(4096)
                        except BlockingIOError:
                            break
                    await asyncio.sleep(0)
                except asyncio.CancelledError:
                    logger.info("UDP listener cancelled")
                    raise
                except Exception:
                    logger.exception("Error in UDP listener")
                    await asyncio.sleep(0.1)
        finally:
            udp_socket.close()
