# Recording and media

[Architecture overview](../ARCHITECTURE.md) · [Telemetry](TELEMETRY.md) · [Session API](../src/vector/api/routers/sessions.py)

A recording **session** collects a test's telemetry, camera video, available Mumble audio, and metadata into one directory. This is distinct from an `ESPDeviceSession`, which represents a node's TCP connection. [SessionRuntime](../src/vector/runtime/session_runtime.py) coordinates recording, while each component owns its I/O.

## Session lifetime

The state transitions are `idle → starting → active → stopping → idle`. An asyncio lock serializes start and stop requests, and conflicting operations return HTTP `409`.

On start, the runtime creates a directory, derives the CSV columns from known devices, and attaches its writer to the permanent `TelemetrySessionPublisher`. This attachment happens without an `await`, so the ingest loop cannot observe a partly constructed writer. It publishes recording state, writes `session.json`, and starts the camera and audio components. Opening the telemetry writer is mandatory. A camera or audio failure is recorded under `components` and the session continues: the remaining data is still useful.

Starting a recording does not start node streaming or change its rate. HELM or the CLI controls acquisition separately. A successfully opened CSV can therefore remain empty if no telemetry arrives.

On stop, the writer is detached before any awaited cleanup. The runtime flushes and closes telemetry, stops the media components, waits briefly for video files to finish growing, and writes final metadata. Shutdown attempts this same finalization with a bounded time budget. An abrupt process exit can leave incomplete files and metadata still marked `active`; restarting does not resume that recording.

The [session routes](../src/vector/api/routers/sessions.py) expose this lifecycle:

| Endpoint | Purpose |
|---|---|
| `POST /v1/sessions/start` | Start a session, with a JSON body such as `{"name": "Hot Fire 3"}` |
| `POST /v1/sessions/stop` | Finalize the active session |
| `GET /v1/sessions` | List sessions and free disk space |
| `GET /v1/sessions/{id}` | Read session metadata |
| `GET /v1/sessions/{id}/download` | Download the session as a streamed ZIP |
| `GET /v1/sessions/{id}/files/{path}` | Download an individual artifact |

## HELM's recording workflow

VECTOR's recordings are the default source for analysis. HELM also writes a CSV backup at the control point, preserving a separate copy if the server is damaged at the pad.

Desktop HELM's [test lifecycle](https://github.com/Queens-Rocket-Engineering-Team/ctl-helm/blob/5ad1943c4b32c5b5fb1a8a9a2b2c53af0fd7c9a1/src/App.vue) coordinates the separate calls: stop preview acquisition, start the test rate, start its laptop CSV recorder, then request a VECTOR session. If the server session fails to start, the laptop recorder can remain active while the operator retries. Stop attempts both recorders and restores preview once both are inactive.

The [laptop recorder](https://github.com/Queens-Rocket-Engineering-Team/ctl-helm/blob/5ad1943c4b32c5b5fb1a8a9a2b2c53af0fd7c9a1/src-tauri/src/telemetry_raw.rs) consumes `/ws/telemetry/raw` directly in Rust while recording, keeping full-rate batches out of Vue's rendering path. It writes its own CSV and depends on the ground-station link. VECTOR's session writes on the server and also coordinates media. The laptop file is separate from the server session archive.

## Files and CSV schema

```text
recordings/
  2026-08-10_143005_hot-fire-3/
    session.json
    telemetry.csv
    audio/...
    video/...
```

The [CSV writer](../src/vector/runtime/session_telemetry.py) writes one row per telemetry batch, with a source label and timestamp. Sensor columns are keyed by name. It samples the latest reported control and Kasa states into each row; this is not an event-by-event command log. The final `source_provider` and `source_key` columns identify the producer even when labels repeat or change. These columns are appended so existing column positions stay the same; parse CSV by header and honor quoted fields.

Columns remain fixed once rows have been written. The core retains disconnected source descriptions and `SystemState` presents their recording schema, so an ordinary reconnect can reuse the original columns. If a batch introduces sensor names absent from that layout, its source gets a `telemetry_late*.csv` side file. Late files are grouped by provider/key and reused only if their columns cover the batch; a reconnect that adds sensors can create another file. Every file stays in the session archive. Before the first row, the writer can instead rebuild the header. These rules preserve readings without rewriting an existing recording.

Writes use a large buffer, flushed roughly once a second; closing also calls `fsync`. That reduces per-batch overhead on the shared event loop. Filesystem stalls can still affect ingest, and periodic flushing is not a guarantee against power-loss data loss.

Sessions are retained until removed externally. Listing them reports free disk space. The [archive helper](../src/vector/runtime/session_archive.py) streams a ZIP without creating another full copy on disk, using the HTTP server's thread pool for file reads and compression. Downloading an active session archive returns `409`. Individual artifacts also have a download route.

## Cameras and MediaMTX

[CameraRuntime](../src/vector/runtime/camera_runtime.py) connects to the cameras listed in YAML. The [camera driver](../src/vector/drivers/camera.py) uses ONVIF to obtain device information, set the camera clock, and issue pan/tilt commands. VECTOR registers an RTSP source with MediaMTX for each camera; MediaMTX pulls the video, relays it to viewers, and writes recordings. Live video does not pass through VECTOR's Python process.

HELM's [camera panel](https://github.com/Queens-Rocket-Engineering-Team/ctl-helm/blob/5ad1943c4b32c5b5fb1a8a9a2b2c53af0fd7c9a1/src/windows/camera_panel.vue) gets stream paths from `/v1/cameras`, then negotiates WebRTC directly with MediaMTX using WHEP on port `8889`. Camera controls still go through VECTOR. Loading video is opt-in, and leaving the panel closes its streams.

Starting/stopping video uses the [MediaMTX client](../src/vector/integrations/mediamtx.py) to patch `record` and `recordPath` together. The patch deliberately contains only recording fields, allowing the pinned MediaMTX version to keep live viewers connected while its recorder changes destination. Compose pins `1.20.0`; this behavior requires at least [MediaMTX 1.15.1](https://github.com/bluenviron/mediamtx/releases/tag/v1.15.1). Earlier versions recreated the path when recording parameters changed, disconnecting viewers. If video drops at session start, check for `path destroyed` in MediaMTX's logs.

## Storage and deployment

VECTOR and MediaMTX must see the **same recordings directory**, often under different container paths. [RecordingPaths](../src/vector/runtime/recording_paths.py) translates between `services.recordings.root` and `mediamtx_container_root`. Both values must match the Compose mounts. Metadata records warnings when video fails to settle or cameras were started but produced no visible files.

The default mapping is:

| Location | Recordings path |
|---|---|
| Host checkout | `./recordings` |
| VECTOR container | `/app/recordings` (`root: ./recordings`, relative to `/app`) |
| MediaMTX container | `/recordings` (`mediamtx_container_root`) |

The containers use `DOCKER_UID`/`DOCKER_GID`, defaulting to `1000:1000`. Match these to the host user that owns the directory. For an older checkout with root-owned recordings, restore ownership from the repository root:

```bash
sudo chown -R "$(id -u):$(id -g)" recordings
```

There is no automatic retention policy; monitor `free_bytes` from `GET /v1/sessions` and archive or remove completed sessions as needed.

## Timestamps and recorded values

`session.json` includes device descriptions, component outcomes, tare offsets at start/stop, and a clock mapping:

```text
wall_clock = started_unix + (device_timestamp - started_monotonic)
```

Telemetry uses server-monotonic seconds; video filenames use MediaMTX's wall clock. This relationship allows comparison, assuming the media host's wall clock agrees with the server's.

CSV sensor values are tared and rounded to four decimal places. An empty sensor cell means that sensor was absent from the batch. Unlike the raw WebSocket, CSV does not include each reading's tare, so a mid-session tare change cannot be reconstructed from start/end offsets alone.

Control columns are named `<source>_<group>_<name>`, where the source is the producer's display label sanitized to letters, digits and underscores, with a numeric suffix if two sources share a label. The writer encodes a boolean in group `relay` as `1` for `CLOSED`, the energized state of a normally-closed relay; every other boolean group, such as `solenoid` or a plug's `power`, uses `1` for `OPEN`/true. Numeric controls retain their reported value. This is a CSV convention tied to the `relay` group name. The layout follows the earlier GUI recorder for analysis compatibility, with numeric controls added explicitly.

## Audio and older integrations

[AudioRuntime](../src/vector/runtime/audio_runtime.py) joins Mumble as a recorder, writes temporary WAV audio, and transcodes it to Opus through ffmpeg on stop. Connection and transcoding are blocking operations dispatched to threads by the session runtime.

Mumble is not currently used for operations communications. Its recording integration remains available, so a session may have no audio even when telemetry and video are present.

[KasaRuntime](../src/vector/runtime/kasa_runtime.py) is a legacy smart-outlet integration for tank heaters. The run-tank heater now uses a Control Node with a local PID loop. Plugs are discovered with nodes, polled for liveness, commanded through `/v1/control`, and recorded in their own CSV columns, but this path is outside QLCP command tracking and ESTOP.

## Changing this subsystem

Coordinate artifacts and metadata in `SessionRuntime`, CSV semantics in `SessionTelemetryWriter`, camera lifecycle in `CameraRuntime`, and MediaMTX HTTP details in its integration client. Preserve the attach/detach boundary and shared-path mapping.

The [session tests](../tests/unit/test_session_runtime.py) cover conflicts, partial failures, and shutdown. [CSV tests](../tests/unit/test_session_telemetry.py) cover columns and late devices; [camera tests](../tests/unit/test_camera_runtime.py) cover media control. The [recording integration test](../tests/integration/test_session_recording_integration.py) follows simulated telemetry into files and a downloadable archive; it uses substitutes for camera and audio services.
