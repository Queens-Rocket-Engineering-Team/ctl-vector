from __future__ import annotations
import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any

import numpy as np
from tsdownsample import EveryNthDownsampler, M4Downsampler

from vector.runtime.metrics import Metrics
from vector.runtime.ws_fanout import BoundedWebSocketFanout


if TYPE_CHECKING:
    from collections.abc import Mapping

    from fastapi import WebSocket

    from vector.core import TelemetryBatch

DISPLAY_TARGET_HZ = 30.0
DISPLAY_POINTS_PER_BUCKET = 8  # M4 requires >= 8 (at least 2 windows x 4 points)
STREAM_METRIC_LABEL = "telemetry_display"


class DownsampleAlgorithm(StrEnum):
    """Downsampling algorithm a display client can request per connection."""

    M4 = "m4"
    DECIMATION = "decimation"


DEFAULT_DOWNSAMPLE_ALGORITHM = DownsampleAlgorithm.M4

# Takes (timestamps, values, n_out) and returns the indices to keep.
DownsampleFn = Callable[[np.ndarray, np.ndarray, int], np.ndarray]

_M4 = M4Downsampler()
_EVERY_NTH = EveryNthDownsampler()


def _m4_indices(ts: np.ndarray, vs: np.ndarray, n_out: int) -> np.ndarray:
    return _M4.downsample(ts, vs, n_out=n_out)


def _decimation_indices(_ts: np.ndarray, vs: np.ndarray, n_out: int) -> np.ndarray:
    # Values only: EveryNth picks by position, and tsdownsample warns if given x.
    return _EVERY_NTH.downsample(vs, n_out=n_out)


def default_downsamplers() -> dict[DownsampleAlgorithm, DownsampleFn]:
    """Strategy per algorithm. The downsamplers are stateless and shared."""
    return {
        DownsampleAlgorithm.M4: _m4_indices,
        DownsampleAlgorithm.DECIMATION: _decimation_indices,
    }


@dataclass(slots=True)
class _SensorBuffer:
    sensor_id: int
    sensor_name: str
    unit_name: str
    sensor_type: str
    timestamps: list[float] = field(default_factory=list)
    values: list[float] = field(default_factory=list)

    def add(self, t: float, v: float) -> None:
        self.timestamps.append(t)
        self.values.append(v)

    def to_points(self, downsample: DownsampleFn, n_out: int) -> list[dict[str, float]]:
        if not self.timestamps:
            return []
        ts = np.asarray(self.timestamps, dtype=np.float64)
        vs = np.asarray(self.values, dtype=np.float64)
        indices = downsample(ts, vs, n_out)
        return [{"t": float(ts[i]), "v": float(vs[i])} for i in indices]


@dataclass(slots=True)
class _DeviceBucket:
    source_provider: str
    source_key: str
    device_name: str
    device_address: str
    connection_key: str
    bucket_index: int
    bucket_start_s: float
    bucket_end_s: float
    last_updated_monotonic: float
    sensors: dict[int, _SensorBuffer] = field(default_factory=dict)


class TelemetryDisplayStream(BoundedWebSocketFanout):
    """Downsampled telemetry stream for the operator GUI at /ws/telemetry/display.

    Collects raw batches into fixed-width time buckets and emits downsampled point
    sets when each bucket closes. Each client picks its algorithm at connect time;
    bucket collection is shared, so only serialization forks per algorithm in use.

    run() must be started as a daemon task; it flushes the trailing partial bucket
    when no boundary-crossing batch arrives within one bucket interval.
    """

    def __init__(
        self,
        *,
        target_hz: float = DISPLAY_TARGET_HZ,
        points_per_bucket: int = DISPLAY_POINTS_PER_BUCKET,
        # A live display gains nothing from deep buffering: 16 messages is ~0.5 s
        # of production, which bounds worst-case staleness for a stalled client.
        max_queue: int = 16,
        metrics: Metrics | None = None,
        downsamplers: Mapping[DownsampleAlgorithm, DownsampleFn] | None = None,
    ) -> None:
        super().__init__(
            stream_metric_label=STREAM_METRIC_LABEL,
            max_queue=max_queue,
            metrics=metrics or Metrics(),
        )
        self._bucket_interval_s = 1.0 / target_hz
        self._points_per_bucket = points_per_bucket
        self._downsamplers = downsamplers if downsamplers is not None else default_downsamplers()
        self._client_algorithms: dict[WebSocket, DownsampleAlgorithm] = {}
        self._buckets: dict[tuple[str, str, str], _DeviceBucket] = {}

    async def handle_client(
        self,
        websocket: WebSocket,
        algorithm: DownsampleAlgorithm = DEFAULT_DOWNSAMPLE_ALGORITHM,
    ) -> None:
        # Cleanup lives here rather than in disconnect_client because the base
        # handle_client accepts the socket outside its own try/finally: a failing
        # accept() never reaches disconnect_client and would leak this entry.
        self._client_algorithms[websocket] = algorithm
        try:
            await super().handle_client(websocket)
        finally:
            self._client_algorithms.pop(websocket, None)

    def publish_batch(self, batch: TelemetryBatch) -> None:
        if not self._clients:
            return

        bucket_index = int(batch.timestamp_s / self._bucket_interval_s)
        # Labels and provider-supplied connection keys can coincide across sources.
        series_key = (batch.source_provider, batch.source_key, batch.connection_key)
        now = time.monotonic()

        device_bucket = self._buckets.get(series_key)
        if device_bucket is not None:
            bucket_changed = bucket_index != device_bucket.bucket_index

            if bucket_changed:
                self._emit_bucket(device_bucket)
                device_bucket = None

        if device_bucket is None:
            device_bucket = _DeviceBucket(
                source_provider=batch.source_provider,
                source_key=batch.source_key,
                device_name=batch.source_name,
                device_address=batch.source_address,
                connection_key=batch.connection_key,
                bucket_index=bucket_index,
                bucket_start_s=bucket_index * self._bucket_interval_s,
                bucket_end_s=(bucket_index + 1) * self._bucket_interval_s,
                last_updated_monotonic=now,
            )
            self._buckets[series_key] = device_bucket

        device_bucket.last_updated_monotonic = now
        for reading in batch.readings:
            buf = device_bucket.sensors.get(reading.sensor_id)
            if buf is None:
                buf = _SensorBuffer(
                    sensor_id=reading.sensor_id,
                    sensor_name=reading.sensor_name,
                    unit_name=reading.unit,
                    sensor_type=reading.group,
                )
                device_bucket.sensors[reading.sensor_id] = buf
            buf.add(batch.timestamp_s, reading.value)

    def serialize_bucket(
        self,
        bucket: _DeviceBucket,
        algorithm: DownsampleAlgorithm = DEFAULT_DOWNSAMPLE_ALGORITHM,
    ) -> dict[str, Any]:
        downsample = self._downsamplers[algorithm]
        return {
            "type": "telemetry.display_batch",
            "algorithm": algorithm.value,
            "source_provider": bucket.source_provider,
            "source_key": bucket.source_key,
            "device_name": bucket.device_name,
            "device_address": bucket.device_address,
            "connection_key": bucket.connection_key,
            "bucket_start_s": bucket.bucket_start_s,
            "bucket_end_s": bucket.bucket_end_s,
            "readings": [
                {
                    "sensor_id": s.sensor_id,
                    "sensor_name": s.sensor_name,
                    "unit": s.unit_name,
                    "sensor_type": s.sensor_type,
                    "points": s.to_points(downsample, self._points_per_bucket),
                }
                for s in bucket.sensors.values()
            ],
        }

    def _emit_bucket(self, bucket: _DeviceBucket) -> None:
        if not bucket.sensors:
            return
        # Group by algorithm so a bucket is downsampled once per algorithm in use
        # rather than once per client. Iterating _clients (not _client_algorithms)
        # keeps a socket that is still mid-handshake out of the fan-out.
        by_algorithm: dict[DownsampleAlgorithm, list[WebSocket]] = {}
        for socket in self._clients:
            algorithm = self._client_algorithms.get(socket, DEFAULT_DOWNSAMPLE_ALGORITHM)
            by_algorithm.setdefault(algorithm, []).append(socket)

        for algorithm, sockets in by_algorithm.items():
            self.publish_message_to(self.serialize_bucket(bucket, algorithm), sockets)

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self._bucket_interval_s)
            if not self._clients:
                continue
            now = time.monotonic()
            stale = [
                key for key, bucket in self._buckets.items()
                if now - bucket.last_updated_monotonic >= self._bucket_interval_s
            ]
            for key in stale:
                self._emit_bucket(self._buckets.pop(key))

    def _after_disconnect(self) -> None:
        if not self._clients:
            self._buckets.clear()
