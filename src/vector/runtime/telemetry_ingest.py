from __future__ import annotations
import asyncio
import logging
import socket
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Mapping, Protocol

from vector.qlcp.config_models import DeviceConfig
from vector.qlcp.decoding import decode_packet_server
from vector.qlcp.packets import DataPacket
from vector.runtime.metrics import Metrics


logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from vector.runtime.esp_connection_runtime import ESPDeviceSession


UDP_PORT = 50001  # Distinct from the TCP port; a different number is useful for debugging.

MICROSECONDS_PER_SECOND = 1_000_000

# Raw samples retained per (device, sensor) so a tare can be captured from recent history.
TARE_SAMPLE_CAPACITY = 256
TARE_DEFAULT_SAMPLES = 16
# Samples older than this are treated as absent, so a disconnected device's last readings
# can never be used to capture a tare.
TARE_SAMPLE_MAX_AGE_S = 2.0


class TareCaptureError(Exception):
    """A tare could not be captured from recent samples.

    ``candidates`` lists the devices currently reporting the sensor when the failure was
    caused by an ambiguous name rather than by missing samples.
    """

    def __init__(self, message: str, *, candidates: tuple[str, ...] = ()) -> None:
        super().__init__(message)
        self.candidates = candidates


@dataclass(frozen=True, slots=True)
class TelemetryReading:
    """A single sensor reading after server-side taring.

    ``value`` is the tared value and ``tare`` is the offset that was subtracted, so the
    raw reading is recoverable as ``value + tare``.
    """

    sensor_id: int
    sensor_name: str
    value: float
    unit_name: str
    sensor_type: str
    tare: float = 0.0


@dataclass(slots=True)
class _SampleBuffer:
    """Recent raw (pre-tare) values for one device's sensor.

    Raw rather than tared, so re-taring an already-tared sensor computes a fresh absolute
    offset instead of compounding onto the previous one.
    """

    values: deque[float] = field(default_factory=lambda: deque(maxlen=TARE_SAMPLE_CAPACITY))
    last_updated_monotonic: float = 0.0


@dataclass(frozen=True, slots=True)
class TelemetryBatch:
    """Internal telemetry ingest batch; not a stable public API contract."""

    device_name: str
    device_address: str
    connection_key: str
    timestamp_s: float
    readings: tuple[TelemetryReading, ...]
    timestamp_source: Literal["device_synced", "server_receive"]
    timestamp_synced: bool


def _no_tare(sensor_name: str) -> float:  # noqa: ARG001
    return 0.0


class TelemetryPublisher(Protocol):
    """A telemetry fan-out target that accepts decoded batches synchronously."""

    def publish_batch(self, batch: TelemetryBatch) -> None: ...


class TelemetryRuntime:
    """Owns UDP telemetry ingest, decode, sensor mapping, and fan-out."""

    def __init__(
        self,
        device_for_address: Callable[[str], ESPDeviceSession | None],
        *publishers: TelemetryPublisher,
        tare_for: Callable[[str], float] | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        self._device_for_address = device_for_address
        self._tare_for = tare_for if tare_for is not None else _no_tare
        self.publishers = publishers
        self.metrics = metrics or Metrics()
        self._samples: dict[tuple[str, str], _SampleBuffer] = {}

    def handle_datagram(self, data: bytes, address: str) -> TelemetryBatch | None:
        session = self._session_for_udp_address(address)
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

    def handle_packet(self, packet: DataPacket, session: ESPDeviceSession) -> TelemetryBatch:
        self.metrics.record_telemetry_data_packet(session.name)
        timestamp_s, timestamp_source, timestamp_synced = self._batch_timestamp(packet, session)
        readings: list[TelemetryReading] = []
        now = time.monotonic()

        for reading in packet.readings:
            sensor = session.qlcp_config.sensors_by_id.get(reading.sensor_id)
            if sensor is None:
                self.metrics.record_telemetry_decode_error("unknown_sensor")
                logger.error(
                    "Received DATA reading for unknown sensor id %s from %s. Ignoring.",
                    reading.sensor_id,
                    session.name,
                )
                continue
            readings.append(self._make_reading(session.name, sensor, reading.value, now))

        batch = TelemetryBatch(
            device_name=session.name,
            device_address=session.address,
            connection_key=session.connection_key,
            timestamp_s=timestamp_s,
            timestamp_source=timestamp_source,
            timestamp_synced=timestamp_synced,
            readings=tuple(readings),
        )
        self.metrics.record_telemetry_readings(session.name, len(batch.readings))
        return batch

    def _make_reading(self, device_name: str, sensor: SensorConfig, raw_value: float, now: float) -> TelemetryReading:
        self._record_sample(device_name, sensor.name, raw_value, now)
        tare = self._tare_for(sensor.name)
        return TelemetryReading(
            sensor_id=sensor.id,
            sensor_name=sensor.name,
            value=raw_value - tare,
            unit_name=sensor.unit,
            sensor_type=sensor.group,
            tare=tare,
        )

    def ingest_virtual(
        self,
        config: DeviceConfig,
        values: Mapping[int, float],
        *,
        address: str,
        connection_key: str,
    ) -> TelemetryBatch:
        """Build, tare and publish a batch for a device with no QLCP session."""
        now = time.monotonic()
        readings: list[TelemetryReading] = []
        for sensor_id, raw_value in values.items():
            sensor = config.sensors_by_id.get(sensor_id)
            if sensor is None:
                self.metrics.record_telemetry_decode_error("unknown_sensor")
                logger.error("Virtual device %s produced unknown sensor id %s. Ignoring.", config.name, sensor_id)
                continue
            readings.append(self._make_reading(config.name, sensor, raw_value, now))

        batch = TelemetryBatch(
            device_name=config.name,
            device_address=address,
            connection_key=connection_key,
            timestamp_s=now,
            timestamp_source="server_receive",
            timestamp_synced=False,
            readings=tuple(readings),
        )
        self.metrics.record_telemetry_readings(config.name, len(batch.readings))
        for publisher in self.publishers:
            publisher.publish_batch(batch)
        return batch

    def _record_sample(self, device_name: str, sensor_name: str, raw_value: float, now: float) -> None:
        """Append a raw reading to the tare capture history. Runs per reading in the UDP loop."""
        key = (device_name, sensor_name)
        buffer = self._samples.get(key)
        if buffer is None:
            buffer = _SampleBuffer()
            self._samples[key] = buffer
        buffer.values.append(raw_value)
        buffer.last_updated_monotonic = now

    def capture_tare_offset(
        self,
        sensor_name: str,
        *,
        device_name: str | None = None,
        samples: int = TARE_DEFAULT_SAMPLES,
    ) -> tuple[float, str, int]:
        """Mean of the most recent raw readings for *sensor_name*, as ``(offset, device, count)``.

        Raises TareCaptureError when no device is currently reporting the sensor, or when
        more than one is and *device_name* does not say which to sample from.
        """
        if samples < 1:
            raise TareCaptureError("samples must be >= 1.")
        now = time.monotonic()
        candidates = {
            key[0]: buffer
            for key, buffer in self._samples.items()
            if key[1] == sensor_name
            and (device_name is None or key[0] == device_name)
            and buffer.values
            and now - buffer.last_updated_monotonic <= TARE_SAMPLE_MAX_AGE_S
        }

        if not candidates:
            scope = f" on {device_name}" if device_name is not None else ""
            message = f"No telemetry received for sensor {sensor_name!r}{scope} in the last {TARE_SAMPLE_MAX_AGE_S}s."
            raise TareCaptureError(message)

        if len(candidates) > 1:
            names = tuple(sorted(candidates))
            message = f"Sensor {sensor_name!r} is reported by multiple devices ({', '.join(names)}); specify which to sample from."
            raise TareCaptureError(message, candidates=names)

        sampled_device, buffer = next(iter(candidates.items()))
        window = list(buffer.values)[-samples:]
        return sum(window) / len(window), sampled_device, len(window)

    def _session_for_udp_address(self, address: str) -> ESPDeviceSession | None:
        return self._device_for_address(address)

    @staticmethod
    def _batch_timestamp(
        packet: DataPacket,
        session: ESPDeviceSession,
    ) -> tuple[float, Literal["device_synced", "server_receive"], bool]:
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
        """Bind the UDP socket and forward decoded batches until cancelled."""
        loop = asyncio.get_event_loop()
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, recv_buffer_bytes)
        udp_socket.bind(("0.0.0.0", port))  # noqa: S104
        udp_socket.setblocking(False)

        logger.info("UDP listener started on port %s", port)

        while True:
            try:
                data, addr = await loop.sock_recvfrom(udp_socket, 4096)

                # Process the first packet plus any already-buffered ones, up to batch_size.
                # This keeps the UDP listener from monopolizing the event loop while other
                # tasks (e.g. TCP command handling) need to run.
                processed = 0
                while True:
                    device_ip = addr[0]
                    batch = self.handle_datagram(data, device_ip)
                    if batch is not None:
                        for publisher in self.publishers:
                            publisher.publish_batch(batch)
                    processed += 1
                    if processed >= batch_size:
                        break
                    try:
                        data, addr = udp_socket.recvfrom(4096)
                    except BlockingIOError:
                        break

                await asyncio.sleep(0)  # Yield to let other tasks run

            except asyncio.CancelledError:
                logger.info("UDP listener cancelled")
                udp_socket.close()
                raise
            except Exception:
                logger.exception("Error in UDP listener")
                await asyncio.sleep(0.1)
