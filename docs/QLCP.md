# QLCP bindings and native build

[Architecture overview](../ARCHITECTURE.md) · [Control Nodes](NODES.md) · [Wire specification](https://github.com/Queens-Rocket-Engineering-Team/ctl-qlcp-lib/blob/3f37353920a323ef7feba61f3b0745106bce5ddf/PROTOCOL_SPECIFICATION.md)

QLCP is the QRET Launch Control Protocol. The [ctl-qlcp-lib submodule](https://github.com/Queens-Rocket-Engineering-Team/ctl-qlcp-lib/tree/3f37353920a323ef7feba61f3b0745106bce5ddf) contains its C reference implementation and authoritative specification. Both the control-node firmware and VECTOR use this library so packet definitions and protocol changes have one shared implementation. The pinned library revision also makes it possible to compare what the server and each firmware build actually use.

CFFI (C Foreign Function Interface) lets Python call this C library. Here it is used in compiled API mode: installation builds a Python extension against the library's header. The C library handles packet encoding, decoding, and framing helpers. Python owns sockets, device sessions, scheduling, and application behavior.

## Python boundary

| Module under `src/vector/qlcp/` | Responsibility |
|---|---|
| [packets.py](../src/vector/qlcp/packets.py) | Represent packets as Python objects and encode them through CFFI. |
| [decoding.py](../src/vector/qlcp/decoding.py) | Decode bytes through C and convert payloads to Python objects. Server-to-device decoding also supports the simulator. |
| [enums.py](../src/vector/qlcp/enums.py) | Expose packet types, control types, and status/error values from compiled C constants. |
| [native.py](../src/vector/qlcp/native.py) | Wrap C errors and framing helpers; provide timestamps, sequence numbers, and buffer bounds. |
| [config_parser.py](../src/vector/qlcp/config_parser.py), [config_models.py](../src/vector/qlcp/config_models.py) | Interpret device CONFIG JSON and assign IDs in transmitted member order. |
| [_bindings.py](../src/vector/qlcp/_bindings.py) | Import the generated extension's `ffi` and `lib` objects. |

An incoming packet flows from bytes to a C payload and then to a Python packet. The server decoder reuses module-level buffers to avoid repeated allocations on the telemetry path. Conversion finishes synchronously before another coroutine can decode. Calling it concurrently from threads would violate that assumption. Current capacities, such as the sensor/control counts and CONFIG buffer size, are local allocation bounds in `native.py` rather than protocol limits.

## What installation builds

[hatch_build.py](../hatch_build.py) is the Hatchling build hook declared in [pyproject.toml](../pyproject.toml). During an editable installation or wheel build, it:

1. Builds the submodule as a shared library with CMake and copies `libqlcp.so` into `src/vector/_lib/`.
2. Preprocesses the public header with GCC and selects the project declarations for CFFI.
3. Compiles the `_qlcp` Python extension and places it under `src/vector/_protocol/`.
4. Generates a type stub for the exposed constants and functions.

The extension links to the adjacent shared library using a relative runtime library path. Wheels include both native artifacts; the source distribution includes the submodule sources needed to build them. The `_lib/` and `_protocol/` directories are generated, so changes belong in the C library, Python wrappers, or build hook.

From the repository root, initialize the pinned submodule and install:

```bash
git submodule update --init --recursive
uv sync
```

Local builds need GCC, CMake, and a build tool such as make. To explicitly rebuild the project's native artifacts after a protocol change:

```bash
uv sync --reinstall-package vector
uv run --no-sync python -c 'from vector.qlcp.enums import PacketType; print(PacketType.ACK)'
```

`SKIP_PROTOCOL_BUILD=1` is used by the [Docker build](../Dockerfile) after copying already-built artifacts. It assumes compatible artifacts are present. Missing-extension or missing-`libqlcp.so` errors usually mean the install/build step was skipped or its output was not packaged. A header/extension mismatch also calls for a rebuild, rather than a hand edit of generated files.

## Versioning and protocol changes

Check the submodule revision and its release tag with:

```bash
git submodule status ctl-qlcp-lib
git -C ctl-qlcp-lib describe --tags --always
```

At the time of this documentation, VECTOR pins `3f37353` (`v3.1.0`). The inspected [firmware revision](https://github.com/Queens-Rocket-Engineering-Team/ctl-node-firmware/tree/58786c6d79764e23c3087b8c2cc9cc35f549a270/components) pins the same commit. Firmware already flashed onto a node still depends on the revision used for that build.

The wire header's version byte remains `3` for this release. That byte identifies wire compatibility, not the exact library release. VECTOR's own version is separate, and the submodule's CMake project version currently still says `3.0.0`; use the pinned commit/tag when comparing implementations.

For a protocol change, update the library/specification and the server's submodule pin, then adapt the Python packet wrappers, decoding, and CONFIG handling as needed. Rebuild and check the firmware's pin as well. Existing [CONFIG parser](../tests/unit/test_qlcp_config_parser.py), [STATUS packet](../tests/unit/test_qlcp_status_packet.py), [driver](../tests/unit/test_esp_driver.py), and [socket integration tests](../tests/integration/test_mock_device_integration.py) exercise the Python boundary. The C library has its own [unit tests](https://github.com/Queens-Rocket-Engineering-Team/ctl-qlcp-lib/blob/3f37353920a323ef7feba61f3b0745106bce5ddf/tests/unit/qlcp_lib_tests.c).
