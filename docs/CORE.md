# Shared sensor and control core

[Architecture overview](../ARCHITECTURE.md) · [Telemetry](TELEMETRY.md) · [Control Nodes](NODES.md)

`vector.core` is an internal Python library shared by VECTOR's services. `build_runtime()` constructs one `Core` object and passes it to the providers and consumers. The core owns resource declarations, latest readings, tares, control observations, and command routing. Services own connections and tasks; consumers own serialization, queues, and recording files.

A new sensor service only needs to register its definitions and publish measurements. It does not need a QLCP node or an ESP session. QLCP and Kasa use the same core as providers today.

## Three terms

- A **definition** describes an operator-facing sensor or control: its name, group, units, and, for controls, value type and optional default. Definitions are immutable and contain no transport objects or packet IDs.
- A **source** is one independently connected producer, identified by a provider and stable key. Examples are `("qlcp", "PANDA")` and `("kasa", "192.168.0.20")`.
- A **binding** joins a definition to a source. A command targets control bindings explicitly; a latest reading identifies the sensor binding that produced it.

One source can declare many sensors and controls. Several sources can declare the same resource name. Sensor lookup and tares use exact names; control lookup is case-insensitive. The core retains each source's metadata and readings independently, without selecting a preferred sensor path.

Source names are display labels and may repeat. Telemetry and state projections carry `source_provider` and `source_key`; use that pair for stable identity and `connection_key` to distinguish connections. For example, `("wireless", "pad")` and `("qlcp", "pad")` remain separate even if both are named `Pad`. CSV rows retain the label in `source` and append the same two identity columns.

## The interface

Import public types from `vector.core`. The implementation is split into [models.py](../src/vector/core/models.py) for values and [core.py](../src/vector/core/core.py) for behavior.

| Caller | Methods | Result |
|---|---|---|
| Provider | `core.register_source(provider, key, sensors=..., controls=..., control_handler=...)` | A `Source` handle for this registration. |
| Provider | `source.publish_samples(samples, timestamp_s=...)` | Submits raw physical-unit values as `(sensor_name, value)` or `(sensor_binding, value)` pairs. |
| Provider | `source.report_control(...)`, `source.accept_control(...)` | Updates observed hardware state or accepted requests separately. |
| Provider | `source.close()` | Disables its publishing and commands while retaining descriptions and last-known state. |
| Consumer | `core.sources()`, `sensors()`, `controls()`, `latest_samples()` | Reads the catalog and latest samples, including source identity and availability. |
| Consumer | `core.subscribe_samples(callback)`, `subscribe_changes(callback)` | Receives synchronous updates; returns an unsubscribe function. |
| Command caller | `await core.set_control(targets, value)` | Validates all explicit targets first, then returns one `DispatchResult` per target. |
| Tare caller | `core.capture_tare(...)`, `set_tare(...)`, `clear_tare(...)`, `tares()` | Shares an offset across sources reporting the exact sensor name. |

Control values are `bool`, `int`, or `float`; `ControlType` has `BOOL`, `UINT32`, `INT32`, and `FLOAT32` members. The QLCP adapter translates OPEN/CLOSED into booleans. Kasa declares a boolean `power` control with no default.

A control's `default` describes device policy; registration does not send it or treat it as observed state. Supply `initial_controls` only when the provider already has observations, as Kasa does after discovery readback.

Bindings have an ordinal within their source's declaration list. The QLCP adapter preserves CONFIG order so these ordinals match existing wire IDs; protocol IDs remain outside the definitions. Providers normally publish by name. The QLCP adapter uses explicit bindings to preserve distinct declarations even when a node repeats a name in different groups. Control observations also accept a name or explicit binding.

All calls run on the existing asyncio loop. Subscription callbacks must return promptly. Stream consumers queue messages, the display consumer downsamples, and the recorder performs buffered writes. Callbacks do not become independent tasks automatically.

Pass the existing core into a new service from `build_runtime()`; create a separate `Core()` only for an isolated test or tool. Subscribe once when a consumer starts and keep the unsubscribe function for cleanup. Subscriptions send future updates only. Read `latest_samples()` for an initial view, and inspect both `connected` and `timestamp_s`: an available source can still have an old reading.

## A reading from hardware to recording

1. A provider reads its hardware, converts the value to the declared unit, and chooses a timestamp in the server's monotonic timebase. QLCP's UDP adapter also maps packet IDs to sensor bindings here.
2. `source.publish_samples()` records raw history for tare capture, subtracts the shared offset, and retains the latest sample with its timestamp and applied tare.
3. The core calls each sample subscriber once. `build_runtime()` subscribes the full-rate stream, display stream, and session publisher. The latter writes a CSV row when a recording is active.
4. The recording schema comes from the common catalog through `SystemState`, so a sensor-only source participates in recording just as a node does.

A small provider can use this API without networking:

```python
import time

from vector.core import Core, SensorDefinition


def demonstrate_provider(core: Core) -> None:
    source = core.register_source(
        "wireless",
        "pad-antenna",
        name="Pad antenna",
        sensors=[SensorDefinition("PAD_RSSI", group="radio", unit="dBm")],
    )
    unsubscribe = core.subscribe_samples(lambda batch: print(batch.readings))
    try:
        source.publish_samples([("PAD_RSSI", -62.0)], timestamp_s=time.monotonic())
        latest, = core.latest_samples("PAD_RSSI")
        assert latest.reading.value == -62.0
        assert latest.connected
    finally:
        unsubscribe()
        source.close()
```

A real polling service keeps its source handle for its lifetime and publishes each poll through that handle. Polling, credentials, retries, and cleanup belong to the service.

Latest readings preserve the tare applied when published. Changing a tare affects future samples and does not fabricate a fresh reading. Tare captures that produce a non-finite offset are rejected without changing the existing tare. Closing a source marks retained readings unavailable and prevents its old raw history from being used for tare capture.

## A control request from API to feedback

1. The API or CLI resolves its existing target scope and converts operator input into a typed value. QLCP REST requests still target matching node controls; Kasa requests still target a selected plug.
2. `core.set_control()` validates every selected binding before invoking any provider. It calls the registered async `handler(binding, value)` with the exact selected binding and returns submission results, including a QLCP command ID when available. An unavailable or failed target has its own failure result.
3. The QLCP handler maps the binding's declaration ordinal to its wire control ID, then builds and sends CONTROL through the existing command tracker. This preserves distinct targets even when a node declares the same name in different groups. The Kasa handler writes power and refreshes the device to read it back.
4. Hardware feedback enters through `source.report_control()`. QLCP response correlation stays in its adapter and tracker; accepted requests and reported `confirmed`, `pending`, or `error` states remain distinct from successful transmission.
5. `SystemState` translates core changes into the existing GUI snapshots and events. It reads command history from the existing QLCP tracker rather than maintaining a second history.

A service requesting an actuation can use `core.source(provider, key)`, then `source.control(name)` to select a target. Both lookups can return `None`. Pass the binding to `await core.set_control([target], value)` and inspect the returned result's `submitted` and `error` fields. Queries include disconnected sources; dispatch reports these as unavailable.

Targets in one call are sent sequentially, and a later failure does not undo earlier sends. Separate callers can interleave while handlers await I/O. Inspect the reported observation when physical completion matters; submission alone does not establish it.

## Lifetime and extension

Registering the same `(provider, key)` replaces the previous registration. Keep the returned handle in each polling task, feedback callback, and async command handler: updates through an old or closed handle cannot affect its replacement. The core does not cancel the service's tasks or close its sockets; the service remains responsible for those resources.

After awaited control I/O, report through `target.source.report_control(target, observed_value)`. Looking up the current source again would attach an old operation's feedback to a replacement connection, bypassing the lifetime protection the handle provides.

The core has no supervisor, plugin loader, or sequence engine. A future sequencer can read the catalog, subscribe to observations, and submit ordinary controls. Sequence timing, conditions, progress, action advertisement, command ownership, and cancellation/ESTOP coordination would be separate work. Runtime service restart and GUI reconfiguration would likewise belong to the runtime lifecycle layer.

Start with the [core tests](../tests/unit/test_core.py) for resource lifetime and dispatch behavior, and the [provider integration tests](../tests/unit/test_core_pipeline.py) for the path into streams and recording.
