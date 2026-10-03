# Clients

[Architecture overview](../ARCHITECTURE.md) · [Device commands](NODES.md) · [Telemetry](TELEMETRY.md)

[HELM](https://github.com/Queens-Rocket-Engineering-Team/ctl-helm) sends commands and settings to VECTOR over HTTP and receives ongoing updates over WebSockets. [FastAPI](../src/vector/api/fast_api.py) serves both on port `8000`. The CLI is a local interface to the same runtime objects; it does not make HTTP requests.

## The HELM side

HELM's Vue frontend runs inside Tauri on the ground-station laptop or as a web build on tablets. The frontend makes REST requests and consumes state and display telemetry in both builds. Tauri's Rust side handles desktop facilities, including the separate [laptop CSV recorder](RECORDING.md#helms-recording-workflow).

Operations normally use one command-capable HELM instance on the ground-station computer. Multiple instances can connect and command simultaneously, with VECTOR distributing the shared device and control state to all of them. There is no exclusive-controller role in VECTOR.

The links below pin the inspected HELM revision, `5ad1943`, so the client side of these contracts is easy to find.

| HELM module | Responsibility |
|---|---|
| [useServerApi.js](https://github.com/Queens-Rocket-Engineering-Team/ctl-helm/blob/5ad1943c4b32c5b5fb1a8a9a2b2c53af0fd7c9a1/src/composables/useServerApi.js) | Send REST commands, including client capability checks. |
| [useStateStream.js](https://github.com/Queens-Rocket-Engineering-Team/ctl-helm/blob/5ad1943c4b32c5b5fb1a8a9a2b2c53af0fd7c9a1/src/composables/useStateStream.js) | Build the GUI's device, control, command, tare, and session view from snapshots and deltas. |
| [useTelemetryStream.js](https://github.com/Queens-Rocket-Engineering-Team/ctl-helm/blob/5ad1943c4b32c5b5fb1a8a9a2b2c53af0fd7c9a1/src/composables/useTelemetryStream.js) | Consume `/ws/telemetry/display` and keep rolling chart history. |
| [App.vue](https://github.com/Queens-Rocket-Engineering-Team/ctl-helm/blob/5ad1943c4b32c5b5fb1a8a9a2b2c53af0fd7c9a1/src/App.vue) | Coordinate acquisition rates, recording, and device joins. |

Desktop HELM owns the acquisition policy: it requests preview readings outside recordings and the configured test rate during recording. It also polls control STATUS and can restart streaming when a node joins or remains silent during startup. Those requests affect all registered nodes through VECTOR's REST API and can briefly interrupt telemetry. This client acquisition logic is separate from VECTOR's abort watchdog.

## Requests and streams

REST routes validate input and call the appropriate runtime. The main groups are [devices and commands](../src/vector/api/routers/devices.py), [tares](../src/vector/api/routers/tares.py), [sessions](../src/vector/api/routers/sessions.py), [cameras](../src/vector/api/routers/cameras.py), and [Kasa outlets](../src/vector/api/routers/kasa.py). FastAPI's `/docs` page describes their request parameters. Some POST routes use query parameters, so consult the route rather than assuming every request takes JSON.

The [WebSocket routes](../src/vector/api/routers/streams.py) delegate each connection to a stream runtime:

| Endpoint | Messages | Slow-client behavior |
|---|---|---|
| `/ws/state` | Initial `state.snapshot`, then device, control, command, tare, and session events | Disconnect on queue overflow; reconnect for a fresh snapshot |
| `/ws/telemetry/raw` | `telemetry.raw_batch`: every ingested batch, with tared values and offsets | Drop oldest queued batches |
| `/ws/telemetry/display` | `telemetry.display_batch`: downsampled points for live plotting | Drop oldest queued batches |
| `/ws/logs` | Formatted log entries with `level`, `data`, and `timestamp_ws` | Drop oldest queued entries |

These sockets carry server-to-client data. Messages sent by a client on `/ws/state` are consumed to detect disconnects but do not execute commands. Use REST for commands.

Video has a separate path: VECTOR supplies camera metadata and configures MediaMTX, while the GUI receives the video from MediaMTX. See [recording and media](RECORDING.md#cameras-and-mediamtx).

## State snapshots and events

[SystemState](../src/vector/state/system_state.py) assembles the client-facing state. It projects resource declarations, control observations, and tares from the [shared core](CORE.md), command history and connection health through the QLCP adapter, and recording status from the session runtime. Telemetry readings flow separately; the state snapshot is not a sensor-history store.

[StateStream](../src/vector/runtime/state_stream.py) queues a snapshot before registering a new subscriber, then queues subsequent deltas in order. Losing a delta could leave a client permanently stale, so a full queue disconnects the client. There is no event replay on reconnect: the client should replace its view from the new snapshot. `GET /v1/state` provides the same state as a one-off read.

Events carry `state_version`, but it is not a revision of every value in the snapshot. Heartbeat, synchronization, and pending-command fields are sampled from live objects. Clients should process the events they receive rather than treating an unchanged version as proof that nothing changed. A device's `heartbeat.state` is `ok`, `missed`, `disconnected`, or `unknown`. `unknown` means the source has no transport liveness signal, so clients must not display it as healthy.

Key sources by `(source_provider, source_key)`, not by the display label: different providers can use the same name. Device and Kasa snapshots, their events, and telemetry batches carry those fields alongside `connection_key`. Use the connection key to reject feedback from a replaced connection, and a control's `id` to distinguish repeated control names within it. Existing `name`/`device_name` fields remain display labels; clients that key solely by name need to adopt the identity fields to show colliding labels separately.

## Command results and control state

There are several distinct observations along a command's path:

- The REST result (`sent`, `partial`, or an error) describes transmission to the selected nodes. It does not wait for a device response or return the tracker command IDs.
- A `command.acked` event means the tracked response arrived. For CONTROL, this is normally a correlated QLCP STATUS packet, even though the lifecycle name says `acked`.
- `reported_state` and `reported_status` describe the node's report. `pending` means the node reports ongoing actuation; `error` preserves the last known value while reporting the fault.

The `settled` field is derived from the absence of an outstanding CONTROL and a reported status other than `pending`. It can therefore be true for an error or an unknown state; it is not independent proof that the requested physical state was achieved. Read the reported status too.

The state model also exposes `accepted_state` and `control.accepted` for an explicit CONTROL ACK path. QLCP v3.1's normal CONTROL response is STATUS, so clients should not require a separate accepted event before handling reported state. See [Control Nodes](NODES.md) for matching and timeouts.

## Client capabilities and assumptions

The deployment assumes a trusted operations LAN. VECTOR currently has no authenticated viewer/operator roles. HELM's [platform capabilities](https://github.com/Queens-Rocket-Engineering-Team/ctl-helm/blob/5ad1943c4b32c5b5fb1a8a9a2b2c53af0fd7c9a1/src/lib/platform.js) and request guards disable actuator commands, ESTOP, tare changes, and recording controls in the tablet build. Its permitted actions include device discovery and camera PTZ/reconnect.

The tablet can also request preview streaming when a connected sensor node is silent, but never sends STOP. These capability checks live in HELM; VECTOR does not enforce a separate tablet permission set.

Every open `/ws/state` connection counts as GUI presence for the [watchdog](SAFETY.md#the-gui-watchdog), including tablets. REST polling and the telemetry sockets do not count. Tablets are expected to be disconnected during armed operations.

## Diagnostics

[Logging](../src/vector/runtime/logging.py) writes to stdout and a [thread-safe log queue](../src/vector/runtime/log_stream.py). `PROP_LOG_LEVEL` controls stdout verbosity; the WebSocket handler accepts DEBUG records. Logs are a live feed without replay. `GET /v1/metrics` exposes in-memory counters and recent diagnostic events for connections, commands, telemetry drops, HTTP requests, and watchdog trips. `/health` only confirms that the API responds; it does not check each runtime or device.

## Changing this subsystem

Change input validation in the route, behavior in the runtime, and state/event representation in `SystemState` and its publisher. Coordinate changes to JSON fields with HELM. The [state tests](../tests/unit/test_system_state.py), [snapshot/overflow tests](../tests/unit/test_state_stream.py), and [command route tests](../tests/unit/test_command_router.py) cover those boundaries. [Metrics](../tests/unit/test_api_metrics.py) and [log stream](../tests/unit/test_log_stream.py) tests cover diagnostics.
