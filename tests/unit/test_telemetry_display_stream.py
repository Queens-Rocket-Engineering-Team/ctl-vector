from __future__ import annotations
import asyncio
import contextlib
import time
import warnings
from typing import Any, cast

import numpy as np
import orjson
import pytest
from fastapi import WebSocket

from vector.api.routers.streams import parse_downsample_algorithm
from vector.core import TelemetryBatch, TelemetryReading
from vector.runtime.metrics import Metrics
from vector.runtime.telemetry_display_stream import (
    DEFAULT_DOWNSAMPLE_ALGORITHM,
    DISPLAY_POINTS_PER_BUCKET,
    DISPLAY_TARGET_HZ,
    DownsampleAlgorithm,
    TelemetryDisplayStream,
    _SensorBuffer,
    default_downsamplers,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _identity_indices(x: np.ndarray, _y: np.ndarray, n_out: int) -> np.ndarray:
    """Return the first n_out indices unchanged — predictable test output."""
    return np.arange(min(len(x), n_out), dtype=np.uint64)


def _reversed_indices(x: np.ndarray, y: np.ndarray, n_out: int) -> np.ndarray:
    """Return _identity_indices reversed — a second stub, distinguishable in routing tests."""
    return _identity_indices(x, y, n_out)[::-1]


class FakeWebSocket:
    def __init__(self, *, fail_after: int | None = None) -> None:
        self.accepted = False
        self.closed = False
        self.sent: list[dict[str, Any]] = []
        self._fail_after = fail_after
        self._send_count = 0

    async def accept(self) -> None:
        self.accepted = True

    async def send_json(self, message: dict[str, Any]) -> None:
        if self._fail_after is not None and self._send_count >= self._fail_after:
            raise RuntimeError("websocket send failed")
        self._send_count += 1
        self.sent.append(message)

    async def send_text(self, data: str) -> None:
        if self._fail_after is not None and self._send_count >= self._fail_after:
            raise RuntimeError("websocket send failed")
        self._send_count += 1
        self.sent.append(orjson.loads(data))

    async def close(self) -> None:
        self.closed = True


def _as_ws(ws: FakeWebSocket) -> WebSocket:
    return cast(WebSocket, ws)


def _make_reading(
    sensor_id: int = 0,
    sensor_name: str = "PT101",
    value: float = 100.0,
    unit: str = "PSI",
    group: str = "pressure_transducer",
) -> TelemetryReading:
    return TelemetryReading(
        sensor_id=sensor_id,
        sensor_name=sensor_name,
        value=value,
        unit=unit,
        group=group,
    )


def _make_batch(
    timestamp_s: float = 1.0,
    readings: tuple[TelemetryReading, ...] | None = None,
    device_name: str = "MockDevice",
    connection_key: str = "esp-1",
    source_provider: str = "qlcp",
    source_key: str = "MockDevice",
) -> TelemetryBatch:
    return TelemetryBatch(
        source_provider=source_provider,
        source_key=source_key,
        source_name=device_name,
        source_address="10.0.0.1",
        connection_key=connection_key,
        timestamp_s=timestamp_s,
        readings=readings if readings is not None else (_make_reading(),),
        timestamp_source="device_synced",
        timestamp_synced=True,
    )


def _bucket_key(
    source_key: str = "MockDevice", connection_key: str = "esp-1", source_provider: str = "qlcp",
) -> tuple[str, str, str]:
    return (source_provider, source_key, connection_key)


async def _connect(stream: TelemetryDisplayStream) -> FakeWebSocket:
    ws = FakeWebSocket()
    await stream.connect_client(_as_ws(ws))
    return ws


def _make_stream(
    *,
    target_hz: float = DISPLAY_TARGET_HZ,
    max_queue: int = 128,
    metrics: Metrics | None = None,
) -> TelemetryDisplayStream:
    return TelemetryDisplayStream(
        target_hz=target_hz,
        max_queue=max_queue,
        metrics=metrics,
        downsamplers={
            DownsampleAlgorithm.M4: _identity_indices,
            DownsampleAlgorithm.DECIMATION: _reversed_indices,
        },
    )


# ---------------------------------------------------------------------------
# _SensorBuffer
# ---------------------------------------------------------------------------


def test_sensor_buffer_empty_returns_no_points() -> None:
    buf = _SensorBuffer(0, "PT101", "PSI", "pressure_transducer")
    assert buf.to_points(_identity_indices, 8) == []


def test_sensor_buffer_collects_samples() -> None:
    buf = _SensorBuffer(0, "PT101", "PSI", "pressure_transducer")
    buf.add(1.0, 10.0)
    buf.add(2.0, 20.0)
    assert buf.timestamps == [1.0, 2.0]
    assert buf.values == [10.0, 20.0]


def test_sensor_buffer_to_points_uses_downsampler() -> None:
    buf = _SensorBuffer(0, "PT101", "PSI", "pressure_transducer")
    for i in range(5):
        buf.add(float(i), float(i * 10))

    points = buf.to_points(_identity_indices, 3)

    assert points == [{"t": 0.0, "v": 0.0}, {"t": 1.0, "v": 10.0}, {"t": 2.0, "v": 20.0}]


def _buffer_of(n: int) -> _SensorBuffer:
    buf = _SensorBuffer(0, "PT101", "PSI", "pressure_transducer")
    for i in range(n):
        buf.add(float(i), float(i % 5))  # samples with some variation
    return buf


def test_sensor_buffer_to_points_with_m4() -> None:
    # Sanity-check the real M4 integration: enough samples, correct output shape.
    points = _buffer_of(20).to_points(default_downsamplers()[DownsampleAlgorithm.M4], n_out=8)

    assert len(points) == 8
    ts = [p["t"] for p in points]
    assert ts == sorted(ts), "M4 indices must be in ascending time order"
    assert all("t" in p and "v" in p for p in points)


def test_sensor_buffer_to_points_with_decimation() -> None:
    # Sanity-check the real EveryNth integration. tsdownsample warns when x is passed
    # to EveryNth, so error-on-warning locks in the values-only call.
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        points = _buffer_of(20).to_points(default_downsamplers()[DownsampleAlgorithm.DECIMATION], n_out=8)

    assert [p["t"] for p in points] == [0.0, 2.0, 5.0, 7.0, 10.0, 12.0, 15.0, 17.0]
    assert [p["v"] for p in points] == [0.0, 2.0, 0.0, 2.0, 0.0, 2.0, 0.0, 2.0]


def test_default_downsamplers_cover_every_algorithm() -> None:
    assert set(default_downsamplers()) == set(DownsampleAlgorithm)


# ---------------------------------------------------------------------------
# publish_batch — accumulation and bucket boundaries
# ---------------------------------------------------------------------------


def test_publish_batch_no_clients_is_noop() -> None:
    stream = _make_stream()
    stream.publish_batch(_make_batch(timestamp_s=1.0))
    assert stream._buckets == {}


def test_publish_batch_creates_bucket_for_device() -> None:
    async def run() -> None:
        stream = _make_stream()
        await _connect(stream)
        stream.publish_batch(_make_batch(timestamp_s=1.0))
        assert _bucket_key() in stream._buckets

    asyncio.run(run())


def test_publish_batch_accumulates_within_bucket() -> None:
    async def run() -> None:
        stream = _make_stream(target_hz=30.0)
        ws = await _connect(stream)
        interval = 1.0 / 30.0

        stream.publish_batch(_make_batch(timestamp_s=1.0, readings=(_make_reading(value=10.0),)))
        stream.publish_batch(_make_batch(timestamp_s=1.0 + interval * 0.5, readings=(_make_reading(value=20.0),)))

        buf = stream._buckets[_bucket_key()].sensors[0]
        assert buf.values == [10.0, 20.0]
        assert stream._clients[_as_ws(ws)].qsize() == 0

    asyncio.run(run())


def test_publish_batch_emits_on_boundary_crossing() -> None:
    async def run() -> None:
        stream = _make_stream(target_hz=30.0)
        ws = await _connect(stream)
        interval = 1.0 / 30.0

        stream.publish_batch(_make_batch(timestamp_s=1.0, readings=(_make_reading(value=42.0),)))
        stream.publish_batch(_make_batch(timestamp_s=1.0 + interval * 1.5))

        queue = stream._clients[_as_ws(ws)]
        assert queue.qsize() == 1
        msg = queue.get_nowait()
        assert msg["type"] == "telemetry.display_batch"
        assert msg["device_name"] == "MockDevice"
        assert msg["readings"][0]["points"] == [{"t": 1.0, "v": 42.0}]

    asyncio.run(run())


def test_publish_batch_broadcasts_to_all_clients() -> None:
    async def run() -> None:
        stream = _make_stream(target_hz=30.0)
        ws1 = await _connect(stream)
        ws2 = await _connect(stream)
        interval = 1.0 / 30.0

        stream.publish_batch(_make_batch(timestamp_s=1.0))
        stream.publish_batch(_make_batch(timestamp_s=1.0 + interval * 1.5))

        assert stream._clients[_as_ws(ws1)].qsize() == 1
        assert stream._clients[_as_ws(ws2)].qsize() == 1

    asyncio.run(run())


def test_full_queue_increments_dropped_batches() -> None:
    async def run() -> None:
        metrics = Metrics()
        stream = _make_stream(target_hz=30.0, max_queue=1, metrics=metrics)
        await _connect(stream)
        interval = 1.0 / 30.0

        stream.publish_batch(_make_batch(timestamp_s=0.0))
        stream.publish_batch(_make_batch(timestamp_s=interval * 1.5))  # emits bucket 0
        stream.publish_batch(_make_batch(timestamp_s=interval * 2.5))  # emits bucket 1, queue full

        d = cast(dict[str, Any], metrics.to_dict())
        dropped = d["telemetry"]["streams"]["dropped_batches_total"]
        assert dropped["telemetry_display"] == 1

    asyncio.run(run())


def test_buckets_are_independent_per_device() -> None:
    async def run() -> None:
        stream = _make_stream(target_hz=30.0)
        ws = await _connect(stream)
        interval = 1.0 / 30.0

        stream.publish_batch(_make_batch(timestamp_s=1.0, device_name="DevA", source_key="DevA"))
        stream.publish_batch(_make_batch(timestamp_s=1.0, device_name="DevB", source_key="DevB"))
        stream.publish_batch(_make_batch(timestamp_s=1.0 + interval * 1.5, device_name="DevA", source_key="DevA"))

        queue = stream._clients[_as_ws(ws)]
        assert queue.qsize() == 1
        assert queue.get_nowait()["device_name"] == "DevA"
        assert _bucket_key("DevB") in stream._buckets

    asyncio.run(run())


def test_buckets_are_independent_per_connection_for_same_device_name() -> None:
    async def run() -> None:
        stream = _make_stream(target_hz=30.0)
        await _connect(stream)

        stream.publish_batch(_make_batch(timestamp_s=1.0, connection_key="old-conn"))
        stream.publish_batch(_make_batch(timestamp_s=1.0, connection_key="new-conn"))

        assert _bucket_key(connection_key="old-conn") in stream._buckets
        assert _bucket_key(connection_key="new-conn") in stream._buckets

    asyncio.run(run())


@pytest.mark.parametrize("other_identity", [("b", "sensor"), ("a", "other")])
def test_display_points_keep_sources_separate_when_labels_and_connections_collide(
    other_identity: tuple[str, str],
) -> None:
    async def run() -> None:
        stream = _make_stream(target_hz=1.0)
        ws = await _connect(stream)
        identities = [("a", "sensor"), other_identity]
        for (provider, key), value in zip(identities, (10.0, 20.0), strict=True):
            stream.publish_batch(_make_batch(
                timestamp_s=1.0, source_provider=provider, source_key=key,
                readings=(_make_reading(value=value),),
            ))
        for provider, key in identities:
            stream.publish_batch(_make_batch(timestamp_s=2.0, source_provider=provider, source_key=key))

        queue = stream._clients[_as_ws(ws)]
        assert queue.qsize() == 2
        messages = [queue.get_nowait(), queue.get_nowait()]
        assert [(message["source_provider"], message["source_key"]) for message in messages] == identities
        assert [message["readings"][0]["points"] for message in messages] == [
            [{"t": 1.0, "v": 10.0}], [{"t": 1.0, "v": 20.0}],
        ]
        assert {message["device_name"] for message in messages} == {"MockDevice"}
        assert {message["connection_key"] for message in messages} == {"esp-1"}

    asyncio.run(run())


# ---------------------------------------------------------------------------
# serialize_bucket wire format
# ---------------------------------------------------------------------------


def test_serialize_bucket_wire_format() -> None:
    async def run() -> None:
        stream = _make_stream(target_hz=30.0)
        ws = await _connect(stream)
        interval = 1.0 / 30.0

        stream.publish_batch(_make_batch(timestamp_s=1.0, readings=(_make_reading(value=42.0),)))
        stream.publish_batch(_make_batch(timestamp_s=1.0 + interval * 1.5))

        msg = stream._clients[_as_ws(ws)].get_nowait()

        assert msg["type"] == "telemetry.display_batch"
        assert msg["algorithm"] == "m4"
        assert msg["source_provider"] == "qlcp"
        assert msg["source_key"] == "MockDevice"
        assert msg["device_name"] == "MockDevice"
        assert msg["device_address"] == "10.0.0.1"
        assert msg["connection_key"] == "esp-1"
        assert msg["bucket_end_s"] > msg["bucket_start_s"]

        r = msg["readings"][0]
        assert r["sensor_id"] == 0
        assert r["sensor_name"] == "PT101"
        assert r["unit"] == "PSI"
        assert r["sensor_type"] == "pressure_transducer"
        assert "points" in r
        assert "m4" not in r

    asyncio.run(run())


# ---------------------------------------------------------------------------
# run() flush loop
# ---------------------------------------------------------------------------


def test_run_flushes_trailing_bucket() -> None:
    async def run() -> None:
        stream = _make_stream(target_hz=30.0)
        ws = await _connect(stream)

        stream.publish_batch(_make_batch(timestamp_s=1.0))
        stream._buckets[_bucket_key()].last_updated_monotonic = time.monotonic() - (1.0 / 30.0 + 0.01)

        task = asyncio.create_task(stream.run())
        await asyncio.sleep(1.0 / 30.0 + 0.05)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert stream._clients[_as_ws(ws)].qsize() == 1
        assert stream._clients[_as_ws(ws)].get_nowait()["type"] == "telemetry.display_batch"

    asyncio.run(run())


def test_run_skips_flush_when_no_clients() -> None:
    async def run() -> None:
        stream = _make_stream(target_hz=30.0)
        ws = await _connect(stream)

        stream.publish_batch(_make_batch(timestamp_s=1.0))
        await stream.disconnect_client(_as_ws(ws))

        # disconnect_client with no remaining clients clears buckets
        assert stream._buckets == {}

        # run() skips when no clients — add a bucket manually to confirm nothing happens
        task = asyncio.create_task(stream.run())
        await asyncio.sleep(1.0 / 30.0 + 0.05)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert stream._buckets == {}

    asyncio.run(run())


# ---------------------------------------------------------------------------
# Client lifecycle
# ---------------------------------------------------------------------------


def test_connect_client_accepts_websocket() -> None:
    async def run() -> None:
        stream = TelemetryDisplayStream()
        ws = FakeWebSocket()
        await stream.connect_client(_as_ws(ws))
        assert ws.accepted is True
        assert stream.client_count == 1

    asyncio.run(run())


def test_disconnect_last_client_clears_buckets() -> None:
    async def run() -> None:
        stream = _make_stream(target_hz=30.0)
        ws = await _connect(stream)

        stream.publish_batch(_make_batch(timestamp_s=1.0))
        assert _bucket_key() in stream._buckets

        await stream.disconnect_client(_as_ws(ws))

        assert stream.client_count == 0
        assert stream._buckets == {}

    asyncio.run(run())


def test_disconnect_non_last_client_preserves_buckets() -> None:
    async def run() -> None:
        stream = _make_stream(target_hz=30.0)
        ws1 = await _connect(stream)
        await _connect(stream)

        stream.publish_batch(_make_batch(timestamp_s=1.0))
        await stream.disconnect_client(_as_ws(ws1))

        assert stream.client_count == 1
        assert _bucket_key() in stream._buckets

    asyncio.run(run())


def test_handle_client_delivers_emitted_message() -> None:
    async def run() -> None:
        stream = _make_stream(target_hz=30.0)
        ws = FakeWebSocket()
        task = asyncio.create_task(stream.handle_client(_as_ws(ws)))
        await asyncio.sleep(0)

        interval = 1.0 / 30.0
        stream.publish_batch(_make_batch(timestamp_s=1.0))
        stream.publish_batch(_make_batch(timestamp_s=1.0 + interval * 1.5))
        await asyncio.sleep(0)

        assert ws.sent[0]["type"] == "telemetry.display_batch"

        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    asyncio.run(run())


def test_handle_client_cleans_up_on_send_failure() -> None:
    async def run() -> None:
        stream = _make_stream(target_hz=30.0)
        ws = FakeWebSocket(fail_after=0)
        task = asyncio.create_task(stream.handle_client(_as_ws(ws)))
        await asyncio.sleep(0)

        interval = 1.0 / 30.0
        stream.publish_batch(_make_batch(timestamp_s=1.0))
        stream.publish_batch(_make_batch(timestamp_s=1.0 + interval * 1.5))
        await task

        assert stream.client_count == 0
        assert ws.closed is True

    asyncio.run(run())


# ---------------------------------------------------------------------------
# Per-client algorithm selection
# ---------------------------------------------------------------------------


def _emit_one_bucket(stream: TelemetryDisplayStream) -> None:
    interval = stream._bucket_interval_s
    stream.publish_batch(_make_batch(timestamp_s=1.0, readings=(_make_reading(value=42.0),)))
    stream.publish_batch(_make_batch(timestamp_s=1.0 + interval * 1.5))


def test_client_without_choice_gets_default_algorithm() -> None:
    async def run() -> None:
        stream = _make_stream(target_hz=30.0)
        ws = await _connect(stream)

        _emit_one_bucket(stream)

        assert stream._clients[_as_ws(ws)].get_nowait()["algorithm"] == "m4"

    asyncio.run(run())


def test_each_client_receives_its_own_algorithm() -> None:
    async def run() -> None:
        stream = _make_stream(target_hz=30.0)
        # connect_client is the seam the other lifecycle tests use; handle_client
        # would block on its queue loop, so register the choices directly.
        m4_ws = await _connect(stream)
        dec_ws = await _connect(stream)
        stream._client_algorithms[_as_ws(m4_ws)] = DownsampleAlgorithm.M4
        stream._client_algorithms[_as_ws(dec_ws)] = DownsampleAlgorithm.DECIMATION

        # Two readings so the identity and reversed stubs produce different output.
        interval = 1.0 / 30.0
        readings = (_make_reading(value=1.0),)
        stream.publish_batch(_make_batch(timestamp_s=1.0, readings=readings))
        stream.publish_batch(_make_batch(timestamp_s=1.0 + interval * 0.1, readings=readings))
        stream.publish_batch(_make_batch(timestamp_s=1.0 + interval * 1.5))

        m4_queue = stream._clients[_as_ws(m4_ws)]
        dec_queue = stream._clients[_as_ws(dec_ws)]
        assert m4_queue.qsize() == 1, "each client gets exactly one message per bucket"
        assert dec_queue.qsize() == 1

        m4_msg = m4_queue.get_nowait()
        dec_msg = dec_queue.get_nowait()
        assert m4_msg["algorithm"] == "m4"
        assert dec_msg["algorithm"] == "decimation"

        m4_points = m4_msg["readings"][0]["points"]
        dec_points = dec_msg["readings"][0]["points"]
        assert dec_points == m4_points[::-1] != m4_points

    asyncio.run(run())


def test_handle_client_records_and_clears_algorithm() -> None:
    async def run() -> None:
        stream = _make_stream(target_hz=30.0)
        ws = FakeWebSocket()
        task = asyncio.create_task(stream.handle_client(_as_ws(ws), DownsampleAlgorithm.DECIMATION))
        await asyncio.sleep(0)

        assert stream._client_algorithms[_as_ws(ws)] is DownsampleAlgorithm.DECIMATION
        _emit_one_bucket(stream)
        await asyncio.sleep(0)
        assert ws.sent[0]["algorithm"] == "decimation"

        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert stream._client_algorithms == {}

    asyncio.run(run())


def test_handle_client_clears_algorithm_when_accept_fails() -> None:
    class _RejectingWebSocket(FakeWebSocket):
        async def accept(self) -> None:
            raise RuntimeError("handshake rejected")

    async def run() -> None:
        stream = _make_stream(target_hz=30.0)
        ws = _RejectingWebSocket()

        # accept() fails outside the base handle_client's try/finally, so the
        # error propagates and only our own finally can clean up.
        with pytest.raises(RuntimeError):
            await stream.handle_client(_as_ws(ws), DownsampleAlgorithm.DECIMATION)

        assert stream._client_algorithms == {}

    asyncio.run(run())


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("m4", DownsampleAlgorithm.M4),
        ("decimation", DownsampleAlgorithm.DECIMATION),
        ("garbage", DEFAULT_DOWNSAMPLE_ALGORITHM),
        ("", DEFAULT_DOWNSAMPLE_ALGORITHM),
    ],
)
def test_parse_downsample_algorithm(raw: str, expected: DownsampleAlgorithm) -> None:
    assert parse_downsample_algorithm(raw) is expected


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


def test_defaults() -> None:
    assert DISPLAY_TARGET_HZ == 30.0
    assert DISPLAY_POINTS_PER_BUCKET == 8
    assert DEFAULT_DOWNSAMPLE_ALGORITHM is DownsampleAlgorithm.M4
    stream = TelemetryDisplayStream()
    assert stream._bucket_interval_s == pytest.approx(1.0 / 30.0)
    assert set(stream._downsamplers) == set(DownsampleAlgorithm)
