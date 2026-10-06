from __future__ import annotations
from typing import TYPE_CHECKING, Any

from vector.runtime.metrics import Metrics
from vector.runtime.ws_fanout import BoundedWebSocketFanout


if TYPE_CHECKING:
    from vector.core import TelemetryBatch


STREAM_METRIC_LABEL = "telemetry_raw"


class TelemetryStreamRuntime(BoundedWebSocketFanout):
    """Forwards full-rate telemetry batches to ``/ws/telemetry/raw`` clients.

    Each client gets a bounded queue and an independent send loop. ``publish_batch``
    is synchronous and non-blocking so it is safe to call directly from the UDP ingest
    loop: a slow or stalled client never blocks ingest, its batches are dropped instead.
    Lossy under backpressure; dropped batches are not signaled in-band to the client.
    """

    def __init__(self, *, max_queue: int = 256, metrics: Metrics | None = None) -> None:
        super().__init__(
            stream_metric_label=STREAM_METRIC_LABEL,
            max_queue=max_queue,
            metrics=metrics or Metrics(),
        )

    def serialize_batch(self, batch: TelemetryBatch) -> dict[str, Any]:
        return {
            "type": "telemetry.raw_batch",
            "source_provider": batch.source_provider,
            "source_key": batch.source_key,
            "device_name": batch.source_name,
            "device_address": batch.source_address,
            "connection_key": batch.connection_key,
            "timestamp_s": batch.timestamp_s,
            "timestamp_source": batch.timestamp_source,
            "timestamp_synced": batch.timestamp_synced,
            "readings": [
                {
                    "sensor_id": reading.sensor_id,
                    "sensor_name": reading.sensor_name,
                    "value": reading.value,
                    # Server-applied tare offset. The untared reading is `value + tare`,
                    # so recordings taken from this stream stay reversible.
                    "tare": reading.tare,
                    "unit": reading.unit,
                    "sensor_type": reading.group,
                }
                for reading in batch.readings
            ],
        }

    def publish_batch(self, batch: TelemetryBatch) -> None:
        """Queue a serialized batch to every connected client without blocking ingest."""
        if not self._clients:
            return

        message = self.serialize_batch(batch)
        self.publish_message(message)
