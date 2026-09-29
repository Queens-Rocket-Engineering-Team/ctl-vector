# Telemetry

[Architecture overview](../ARCHITECTURE.md) · [Shared core](CORE.md) · [Client interface](CLIENTS.md) · [Recording](RECORDING.md)

Telemetry is the stream of sensor readings published through the [shared core](CORE.md). For QLCP nodes, [TelemetryRuntime](../src/vector/runtime/telemetry_ingest.py) receives DATA packets on UDP port `50001`, maps their IDs to sensor bindings, and normalizes timestamps. The core then applies tare offsets, retains readings, and publishes each batch to consumers. Other sensor providers enter this same path by calling their source's `publish_samples()` method.

## The data path

```mermaid
flowchart TB
    Node["Control Node"] -->|"UDP DATA"| Ingest["TelemetryRuntime: decode and map IDs"]
    Registry["ESP device registry"] -->|"Session by source IP"| Ingest
    Ingest -->|"Publish through registered source"| Core["Core: raw history, tare, latest readings"]
    Other["Other sensor providers"] --> Core
    Core --> Batch["TelemetryBatch"]
    Batch --> Raw["Full-rate WebSocket stream"]
    Batch --> Display["Display buckets and downsampling"]
    Batch --> Recording["Active session CSV writer"]
```

A DATA datagram contains sensor IDs and values; the corresponding TCP session supplies their names, groups, and units. Attribution uses the datagram's **source IP**, so the node must already be registered and reachable under that same address. Unknown senders, invalid packets, and non-DATA UDP packets are discarded. An unknown sensor ID is skipped while valid readings in that packet continue.

The resulting core `TelemetryBatch` contains the source's provider/key identity, display name, address, connection key, timestamp, and mapped readings. It retains the existing `device_*` field names for compatibility; a source need not be a node. Both WebSocket formats expose `source_provider` and `source_key`. Display buckets use this pair plus `connection_key`, so shared labels or connection keys from different providers cannot mix readings. `build_runtime()` subscribes three publishers to the core: [full-rate telemetry](../src/vector/runtime/telemetry_stream.py), [display telemetry](../src/vector/runtime/telemetry_display_stream.py), and the [session publisher](../src/vector/runtime/session_telemetry.py). The last is always present and becomes a no-op when no recording is active.

Publishers are called synchronously on the event loop. WebSocket publishers queue their output; the recorder performs buffered CSV writes. The UDP listener limits the datagrams processed per turn, then yields so commands and other tasks can run. Adding a publisher therefore adds work directly to ingest: it must return promptly.

## Clocks

QLCP uses microsecond timestamps. Once the node has acknowledged time synchronization, VECTOR uses the device timestamp in the server's monotonic timebase, converted to seconds. Before that, it uses the server's receive time. Full-rate messages expose `timestamp_source` and `timestamp_synced` so consumers can tell the difference.

Monotonic time measures elapsed time and is not a calendar date. It is used for telemetry, command timing, and timeouts. Recordings save both monotonic and Unix start times so telemetry can be aligned with wall-clock video filenames; see [recording time alignment](RECORDING.md#timestamps-and-recorded-values).

UDP favors timely readings over reliable delivery. Neither the full-rate WebSocket nor a CSV can recover a datagram that never reached VECTOR.

## Taring

A **tare** is an offset subtracted from a sensor's reading to zero it. The core keeps bounded raw history per source registration and sensor. The [tare API](../src/vector/api/routers/tares.py) uses the core to average recent samples and reject stale buffers. If multiple sources are recently reporting that name, the caller must identify which source to sample using the existing `device_name` parameter. When source display names collide, use the provider-qualified key, such as `wireless:pad-antenna`.

The resulting offset is stored in the core by sensor name and applies to every source reporting that name, following the [equipment naming convention](NODES.md#names-and-identity). `SystemState` presents these offsets to clients. This keeps all clients consistent and lets a tare survive a reconnect. Offsets are held in memory and disappear when VECTOR restarts. Re-taring uses raw samples, so offsets do not accumulate.

`core.latest_samples()` retains one last-published reading per binding with its timestamp, applied tare, and availability. Changing a tare affects subsequent samples; it does not change the retained sample's timestamp or value. Closing a source leaves its last-known sample visible as unavailable and discards its tare-capture history.

The `/ws/telemetry/raw` stream is full rate, but its `value` is already tared. It includes the offset as `tare`, allowing recovery of the original reading as `value + tare`. Display points and session CSV sensor values are also tared, but do not include a per-reading offset.

HELM displays these already-tared values in both desktop and tablet builds. Its [state consumer](https://github.com/Queens-Rocket-Engineering-Team/ctl-helm/blob/5ad1943c4b32c5b5fb1a8a9a2b2c53af0fd7c9a1/src/composables/useStateStream.js) uses `tares` to show which sensors are zeroed; clients must not subtract the offset again.

## Display and buffering

The display publisher groups readings into approximately 33 ms buckets per device connection. At each boundary it reduces each sensor's samples to a small point set, targeting eight points per bucket. Clients choose M4 or decimation; M4 is the default. A periodic task flushes the trailing bucket when input pauses. The rate of display updates is independent of the node's acquisition rate.

Both telemetry sockets have independent, bounded queues per client. When a queue fills, its oldest batch is dropped so a slow viewer cannot block ingest. These drops are counted in metrics but are not announced in the stream. Use the [recording path](RECORDING.md) for server-side capture; the display stream is intended for live plots.

## Changing this subsystem

QLCP sensor mapping and timestamp selection belong in `TelemetryRuntime`. Shared tare processing and latest readings belong in the core. Add consumers through `core.subscribe_samples()` and the composition in `build_runtime()`. Keep client serialization in the stream classes. The [provider walkthrough](CORE.md#a-reading-from-hardware-to-recording) shows how to add a sensor path without QLCP.

Relevant tests cover [ingest and tare capture](../tests/unit/test_telemetry_ingest.py), [UDP reception](../tests/unit/test_telemetry_udp_listener.py), [display bucketing](../tests/unit/test_telemetry_display_stream.py), and [raw-stream backpressure](../tests/unit/test_telemetry_stream.py). The [integration tests](../tests/integration/test_mock_device_integration.py) also check that applying a tare preserves the recoverable raw reading.
