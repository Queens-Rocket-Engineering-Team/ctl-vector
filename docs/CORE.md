# Shared sensor and control core

[Architecture overview](../ARCHITECTURE.md) · [Telemetry](TELEMETRY.md) · [Control Nodes](NODES.md)

`vector.core` is an internal Python library shared by VECTOR's services. `build_runtime()` constructs one `Core` object and passes it to the providers and consumers. The core owns resource declarations, tares, control observations, and command routing. Services own connections and tasks; consumers own serialization, queues, and recording files.

A new sensor service only needs to register its definitions and publish measurements. It does not need a QLCP node or an ESP session. QLCP and Kasa use the same core as providers today.

## Three terms

- A **definition** describes an operator-facing sensor or control: its name, group, units, and, for controls, value type and optional default. Definitions are immutable and contain no transport objects or packet IDs.
- A **source** is one independently connected producer, identified by a provider and stable key. Examples are `("qlcp", "PANDA")` and `("kasa", "192.168.0.20")`.
- A **binding** joins a definition to a source. A command targets control bindings explicitly.

One source can declare many sensors and controls. Several sources can declare the same resource name. Within one source, control names are unique ignoring case, and a registration that repeats one is rejected. Sensor lookup and tares use exact names; control lookup is case-insensitive. The core keeps each source's metadata and control observations separate and does not select a preferred sensor path.

A control is identified by its source and name. The same control name on two sources is two controls, and the recorder gives each its own column.

Source names are display labels and may repeat. Telemetry and state projections carry `source_provider` and `source_key`; use that pair for stable identity and `connection_key` to distinguish connections. For example, `("wireless", "pad")` and `("qlcp", "pad")` remain separate even if both are named `Pad`. CSV rows retain the label in `source` and append the same two identity columns.

## The interface

Import public types from `vector.core`. The implementation is split into [models.py](../src/vector/core/models.py) for values and [core.py](../src/vector/core/core.py) for behavior.

| Caller | Methods | Result |
|---|---|---|
| Provider | `core.register_source(provider, key, sensors=..., controls=..., control_handler=...)` | A `Source` handle for this registration. |
| Provider | `source.publish_samples(samples, timestamp_s=...)` | Submits raw physical-unit values as `(sensor_name, value)` or `(sensor_binding, value)` pairs. |
| Provider | `source.report_control(...)` | Records the device's reported control state, read back as `control.reported`: a `ControlObservation` whose `status` is always a `ControlStatus`. |
| Provider | `source.close()` | Disables its publishing and commands while retaining descriptions and last-known state. |
| Consumer | `core.source()`, `sources()`, `sensors()`, then `source.controls` | Reads the catalog, including source identity and availability. |
| Consumer | `core.subscribe_samples(callback)`, `subscribe_changes(callback)` | Receives synchronous updates for the core's lifetime. |
| Command caller | `await core.set_control(target, value)` | Validates one explicit target and value, submits it, and returns the provider's command ID (or `None`). Raises `ControlValidationError` or `ControlDispatchError` when nothing was submitted. |
| Tare caller | `core.capture_tare_offset(...)`, `set_tare(...)`, `clear_tare(...)`, `tares()` | Computes an offset from recent raw samples, then shares it across sources reporting the exact sensor name. Capture does not apply the offset; `set_tare` does. |

A `CoreChange` is one of three types, so each carries only the fields that apply to it:

| Type | Fields | Meaning |
| --- | --- | --- |
| `SourceChanged` | `kind` (`"registered"` or `"closed"`), `source` | A registration began or ended. |
| `ControlChanged` | `control` | A provider reported feedback. Read the new state from `control.reported`; the source is `control.source`. |
| `TareChanged` | `sensor_name`, `offset` | A shared offset was set, or cleared when `offset` is `None`. |

Consumers branch with `match change:`. `SystemState` translates these into the existing client event types.

Control values are `bool`, `int`, or `float`; `ControlType` has `BOOL`, `UINT32`, `INT32`, and `FLOAT32` members. A `FLOAT32` value must be finite: the core refuses NaN and infinity for every provider, and records a non-finite report as an error. The QLCP adapter translates OPEN/CLOSED into booleans. Kasa declares a boolean `power` control with no default.

A control's `default` describes device policy; registration does not send it or treat it as observed state. Supply `initial_controls` only when the provider already has observations, as Kasa does after discovery readback.

Bindings have an ordinal within their source's declaration list. The QLCP adapter preserves CONFIG order so these ordinals match existing wire IDs; protocol IDs remain outside the definitions. Providers normally publish by name. The QLCP adapter publishes by explicit sensor binding, so a node that repeats a sensor name in different groups keeps distinct readings. Control observations also accept a name or explicit binding.

All calls run on the existing asyncio loop. Subscription callbacks must return promptly. Stream consumers queue messages, the display consumer downsamples, and the recorder performs buffered writes. Callbacks do not become independent tasks automatically.

Pass the existing core into a new service from `build_runtime()`; create a separate `Core()` only for an isolated test or tool. Subscribe once when a consumer starts. A subscription lasts for the core's lifetime and sends future updates only; a consumer that needs to pause, such as the recorder, does so on its own side.

Subscribers observe; they must not call `set_control`. A sensor reading can inform an operator or block a command, but it never issues one.

## A reading from hardware to recording

1. A provider reads its hardware, converts the value to the declared unit, and chooses a timestamp in the server's monotonic timebase. QLCP's UDP adapter also maps packet IDs to sensor bindings here.
2. `source.publish_samples()` records raw history for tare capture and subtracts the shared offset.
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
    core.subscribe_samples(lambda batch: print(batch.readings))
    try:
        source.publish_samples([("PAD_RSSI", -62.0)], timestamp_s=time.monotonic())
    finally:
        source.close()
```

A real polling service keeps its source handle for its lifetime and publishes each poll through that handle. Polling, credentials, retries, and cleanup belong to the service.

The core does not detect a source that goes quiet: `connected` changes only when a provider closes or replaces its registration. QLCP nodes are closed by their heartbeat loop. Kasa plugs are polled every five seconds and closed after three polls in a row go unanswered. Each such loop registers a health view with `SystemState`, which is what the device list's `heartbeat` reads: missed heartbeat ACKs for a node, missed polls for a plug. A provider without a loop leaves its source `connected` indefinitely and reports heartbeat `unknown`, never `ok`, because nothing has verified that it is still publishing.

A tare is keyed by exact sensor name across every provider, not just QLCP nodes. A new provider that reuses a tared name inherits that offset.

Changing a tare affects future samples and does not fabricate a fresh reading. Tare captures that produce a non-finite offset are rejected without changing the existing tare. Closing a source prevents its old raw history from being used for tare capture.

## A control request from API to feedback

`core.set_control()` is the single gate for actuation. Every QLCP CONTROL and Kasa power command passes through it, whichever endpoint or CLI command the operator used, so a guard that refuses a command has one place to live. `POST /v1/control` and the CLI are the only endpoints over this call; no provider has an entry point of its own.

ESTOP does not pass through the gate. The QLCP runtime sends it directly, so a guard added here can never refuse an abort. Keep it that way.

1. The API or CLI resolves its target and converts operator input into a typed value. `/v1/control` names one source and control and passes its JSON value to the gate unchanged.
2. `core.set_control()` validates the selected binding and value before invoking the provider. It calls the registered async `handler(binding, value)` with that exact binding and returns the handler's command ID: the QLCP tracker ID, or `None` for providers without one. A handler refuses a value it cannot encode, such as an out-of-range setpoint, by raising `ControlValidationError`; any other exception means the send failed. `set_control` therefore raises `ControlValidationError` when the target or value is invalid and `ControlDispatchError` when the source is unavailable or the provider failed, with the provider's exception as its `__cause__`. In both cases nothing was submitted. If the source closes while the handler awaits I/O, the handler's result still stands: a command that was sent is reported as sent.
3. The QLCP handler maps the binding's declaration ordinal to its wire control ID, then builds and sends CONTROL through the existing command tracker. A number the wire type cannot carry, such as a float beyond the 32-bit range, is refused with `ControlValidationError` before anything is tracked or sent, and the connection is untouched. The Kasa handler writes power and refreshes the device to read it back.
4. Hardware feedback enters through `source.report_control()`. QLCP response correlation stays in its adapter and tracker; reported `confirmed`, `pending`, or `error` states remain distinct from successful transmission. There is no separate accepted state: a requested value is never recorded as the control's state. A reported value never raises: one that does not fit the control's declared type is logged and recorded with `error` status, keeping the last known value, so the operator sees the fault and the provider's connection stays up.
5. `SystemState` translates core changes into the existing GUI snapshots and events. It reads command history from the existing QLCP tracker rather than maintaining a second history.

A service requesting an actuation can use `core.source(provider, key)`, then `source.control(name)` to select a target. Both lookups can return `None`. Pass the binding to `await core.set_control(target, value)` and catch `ControlValidationError` and `ControlDispatchError`; a return without an exception means the command was submitted. Queries include disconnected sources; dispatch reports these as unavailable.

A caller that fans out calls `set_control` once per target; a later failure does not undo earlier sends. Separate callers can interleave while handlers await I/O. Inspect the reported observation when physical completion matters; submission alone does not establish it.

## Lifetime and extension

Registering the same `(provider, key)` replaces the previous registration. Keep the returned handle in each polling task, feedback callback, and async command handler: updates through an old or closed handle cannot affect its replacement. The core does not cancel the service's tasks or close its sockets; the service remains responsible for those resources.

After awaited control I/O, report through `target.source.report_control(target, observed_value)`. Looking up the current source again would attach an old operation's feedback to a replacement connection, bypassing the lifetime protection the handle provides.

The core has no supervisor, plugin loader, or sequence engine. A future sequencer can read the catalog, subscribe to observations, and submit ordinary controls. Sequence timing, conditions, progress, action advertisement, command ownership, and cancellation/ESTOP coordination would be separate work. Runtime service restart and GUI reconfiguration would likewise belong to the runtime lifecycle layer.

Start with the [core tests](../tests/unit/test_core.py) for resource lifetime and dispatch behavior, and the [provider integration tests](../tests/unit/test_core_pipeline.py) for the path into streams and recording.
