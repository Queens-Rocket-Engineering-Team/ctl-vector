# VECTOR architecture

VECTOR is the central server for QRET's ground control system. It connects the hardware at the pad or test stand to [HELM](https://github.com/Queens-Rocket-Engineering-Team/ctl-helm), the operator GUI at the control point. This guide explains how that job is divided across the codebase and where to start when changing it. For installation and operating commands, see the [README](README.md).

## Where VECTOR fits

A **Control Node** reads sensors and operates controls, such as valves, relays, or heater setpoints. It describes its sensors and controls to VECTOR through QLCP; the node owns the hardware-specific I/O and control algorithms.

Nodes communicate with VECTOR over the pad LAN. HELM reaches VECTOR from the control point across a transparent wireless link. VECTOR sees ordinary network traffic; it has no radio-specific integration. Sensor data and control commands pass through VECTOR. Camera video travels directly from MediaMTX to the GUI.

```mermaid
flowchart TB
    subgraph pad["Pad or test stand LAN"]
        Nodes["Control Nodes"]
        Vector["VECTOR"]
        Cameras["IP cameras"]
        Media["MediaMTX"]
        Nodes <-->|"QLCP: TCP 50000, UDP 50001"| Vector
        Vector -->|"ONVIF control"| Cameras
        Cameras -->|"RTSP video"| Media
        Vector -->|"HTTP configuration: 9997"| Media
    end
    subgraph control["Control point"]
        Helm["HELM"]
    end
    Helm <-->|"REST / WebSocket: 8000 across wireless link"| Vector
    Media -->|"WebRTC video across wireless link"| Helm
```

HELM builds a Tauri desktop application for the control point and a browser version for field tablets from the same Vue frontend. The tablet build is view-only for actuators, with [limited discovery, camera, and preview-stream capabilities](docs/CLIENTS.md#client-capabilities-and-assumptions). Tablets are expected to be disconnected during armed operations; this matters to the [GUI watchdog](docs/SAFETY.md#the-gui-watchdog).

Current Control Nodes use ESP32s running [ctl-node-firmware](https://github.com/Queens-Rocket-Engineering-Team/ctl-node-firmware). VECTOR learns their sensors and controls from QLCP CONFIG and registers them in its shared core. Other providers can register sensors and controls directly; Kasa plugs already use this path for power control. Hardware-specific I/O stays with the provider or firmware.

## Code map

Paths below are relative to `src/vector/` unless stated otherwise.

| Area | Responsibility |
|---|---|
| [server.py](src/vector/server.py), [runtime/services.py](src/vector/runtime/services.py) | Build the shared runtime objects and manage process startup and shutdown. |
| [api/](src/vector/api/) | FastAPI routes, request validation, HTTP responses, and WebSocket entry points. Routes call runtime services. |
| [core/](src/vector/core/) | Protocol-independent sensor/control definitions, source registration, sample publication, tares, subscriptions, and control dispatch. See the [core walkthrough](docs/CORE.md). |
| [runtime/](src/vector/runtime/) | Coordinate ongoing work: connections, commands, telemetry, client streams, discovery, recording, and peripherals. A runtime owns the lifetime and behavior of its subsystem. |
| [drivers/](src/vector/drivers/) | Low-level operations on one device: ESP socket framing or camera ONVIF calls. |
| [state/](src/vector/state/) | Present core resources and transport diagnostics through existing client snapshots, events, and recording schemas; own active-recording presentation. |
| [qlcp/](src/vector/qlcp/) | Python packet types, CONFIG parsing, and calls into the C protocol library. |
| [integrations/](src/vector/integrations/) | The HTTP client for MediaMTX's configuration API. |
| [daemons/](src/vector/daemons/) | The interactive terminal interface, which calls the same runtime objects as the API. |
| [config.py](src/vector/config.py) | Load service and camera settings from YAML. Nodes supply their own sensor/control CONFIG. |
| [tests/](tests/) | Unit tests and socket-based node simulators, including integration tests. |
| [ctl-qlcp-lib/](https://github.com/Queens-Rocket-Engineering-Team/ctl-qlcp-lib/tree/3f37353920a323ef7feba61f3b0745106bce5ddf), [hatch_build.py](hatch_build.py) | The pinned C protocol implementation and the build hook that exposes it to Python. |

## Process and state ownership

`python -m vector` enters `server.main()`, loads `PROP_CONFIG` (default `./config.yaml`), and calls `build_runtime()`. That function constructs one `Core`, injects it into the services, and returns their `RuntimeServices` container. It also checks that the recordings directory is writable. FastAPI, the CLI, listeners, and runtime tasks then run on the same asyncio event loop. Blocking audio work and archive generation use threads.

The shared loop is an architectural assumption. For example, recording attaches its CSV writer without an `await`, so telemetry cannot arrive halfway through that operation. QLCP decoding also reuses buffers under this single-thread assumption. Moving work into another thread or process requires revisiting those boundaries.

The core owns declarations, tares, and control observations. The ESP runtime owns live connections, `CommandTracker` owns QLCP command lifecycles, and `SessionRuntime` owns the active recording. `SystemState` presents these through the client-facing view without retaining ESP sessions. The core retains disconnected descriptions and last-known control observations while live services remove their connections. Most state is in memory; restarting loses tares and command history. Completed recordings live on disk.

On shutdown, the server cancels its API and CLI tasks, attempts to finalize the recording, cancels runtime tasks, and closes device resources. Log streaming stops last. Runtime tasks are created directly; there is no service supervisor that restarts failed tasks inside the process.

## Subsystem reference

| Topic | Start here |
|---|---|
| Shared resources, provider registration, reading and command walkthroughs | [Core library](docs/CORE.md) |
| Device discovery, connection lifetime, commands, and responses | [Control Nodes](docs/NODES.md) |
| Sensor data, taring, timestamps, and display downsampling | [Telemetry](docs/TELEMETRY.md) |
| REST, state snapshots/events, WebSockets, and diagnostics | [Client interface](docs/CLIENTS.md) |
| Recording sessions, cameras, audio, and older integrations | [Recording and media](docs/RECORDING.md) |
| Shared protocol implementation, native build, and versioning | [QLCP bindings](docs/QLCP.md) |
| Hardware defaults, node watchdogs, and loss of GUI control | [Safety](docs/SAFETY.md) |

When adding behavior, put orchestration in a runtime, device I/O in a driver or integration, shared measurement/control behavior in the core, and client input/output in the API. Wire new shared services through `build_runtime()`. The subsystem pages identify the tests and contracts that usually change alongside each path.
