"""Integration test: a recording session driven by a real MockSensorDevice.

Composes the real ESP/telemetry/state runtimes over loopback and records a session end
to end, so the CSV columns, the metadata and the archive are all exercised against
telemetry that actually travelled the protocol path. Cameras and Mumble are faked --
neither MediaMTX nor a Mumble server is a test dependency.
"""

from __future__ import annotations
import asyncio
import contextlib
import io
import json
import socket
import zipfile
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from tests.mock_device import MockSensorDevice
from vector.core import Core
from vector.runtime.command_tracker import CommandTracker
from vector.runtime.esp_connection_runtime import ESPConnectionRuntime
from vector.runtime.recording_paths import RecordingPaths
from vector.runtime.session_archive import iter_session_zip
from vector.runtime.session_runtime import SessionRuntime
from vector.runtime.session_telemetry import TelemetrySessionPublisher
from vector.runtime.telemetry_ingest import TelemetryRuntime
from vector.state.system_state import SystemState


if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


class _FakeStateStream:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def publish(self, event: dict[str, object] | None) -> None:
        if event is not None:
            self.events.append(event)


class _NoCameras:
    """No cameras registered, which is how a bench without MediaMTX behaves."""

    @staticmethod
    def cameras() -> list[object]:
        return []

    @staticmethod
    async def start_session_recording(_container_video_dir: PurePosixPath) -> dict[str, str | None]:
        return {}

    @staticmethod
    async def stop_session_recording() -> dict[str, str | None]:
        return {}

    @staticmethod
    async def settle_video(_video_dir: Path, **_kwargs: Any) -> bool:
        return True


class _UnreachableAudio:
    """Mumble is not running, which must degrade the session rather than fail it."""

    @staticmethod
    def start(_output_dir: Path) -> dict[str, str]:
        raise ConnectionRefusedError("mumble is not running")

    @staticmethod
    def stop() -> dict[str, str | None]:
        raise RuntimeError("not recording")


def _free_port(kind: int) -> int:
    probe = socket.socket(socket.AF_INET, kind)
    try:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]
    finally:
        probe.close()


@contextlib.asynccontextmanager
async def _harness(root: Path) -> AsyncGenerator[tuple[ESPConnectionRuntime, SystemState, SessionRuntime, int, int], None]:
    tcp_port = _free_port(socket.SOCK_STREAM)
    udp_port = _free_port(socket.SOCK_DGRAM)

    state = SystemState(core=Core())
    stream = _FakeStateStream()
    state.set_publisher(stream.publish)  # type: ignore[arg-type]
    esp_runtime = ESPConnectionRuntime(command_tracker=CommandTracker(), system_state=state)
    publisher = TelemetrySessionPublisher()
    telemetry_runtime = TelemetryRuntime(esp_runtime.get_device_by_address)
    state.core.subscribe_samples(publisher.publish_batch)
    session_runtime = SessionRuntime(
        paths=RecordingPaths.from_config({"root": str(root), "mediamtx_container_root": "/recordings"}),
        telemetry_publisher=publisher,
        system_state=state,
        camera_runtime=_NoCameras(),  # type: ignore[arg-type]
        audio_runtime=_UnreachableAudio(),  # type: ignore[arg-type]
    )

    tasks = [
        asyncio.create_task(esp_runtime.run_tcp_listener(port=tcp_port)),
        asyncio.create_task(telemetry_runtime.run_udp_listener(port=udp_port)),
    ]
    await asyncio.sleep(0)  # let both listeners bind before the mock connects

    try:
        yield esp_runtime, state, session_runtime, tcp_port, udp_port
    finally:
        for task in tasks:
            task.cancel()
        esp_runtime.close_all()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task


async def _wait_for(condition: Any, *, timeout_s: float = 3.0, tick: float = 0.02) -> bool:
    deadline = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < deadline:
        if condition():
            return True
        await asyncio.sleep(tick)
    return False


def test_session_records_telemetry_metadata_and_a_downloadable_archive(tmp_path: Path) -> None:

    async def run() -> None:
        async with (
            _harness(tmp_path) as (esp_runtime, state, session_runtime, tcp_port, udp_port),
            MockSensorDevice(server_ip="127.0.0.1", server_port=tcp_port, server_udp_port=udp_port) as device,
        ):
            await asyncio.wait_for(device.timesync_received.wait(), timeout=3.0)
            session = next(item for item in esp_runtime.devices.values() if item.name == device.device_name)

            status = await session_runtime.start("Hot Fire 3")
            session_id = status["id"]

            await esp_runtime.start_streaming(session, 100)
            assert await _wait_for(lambda: session_runtime.read_metadata(session_id)["telemetry"]["rows"] > 5)

            # Flip a valve mid-recording; the column must follow.
            device.control_handled.clear()
            await esp_runtime.set_control(session, "AV101", "CLOSED")
            await asyncio.wait_for(device.control_handled.wait(), timeout=3.0)
            assert await _wait_for(lambda: state.control_states().get("AV101") == "CLOSED")

            rows_at_flip = session_runtime.read_metadata(session_id)["telemetry"]["rows"]
            assert await _wait_for(lambda: session_runtime.read_metadata(session_id)["telemetry"]["rows"] > rows_at_flip + 5)

            await esp_runtime.stop_streaming(session)
            metadata = await session_runtime.stop()

        session_dir = tmp_path / session_id
        lines = (session_dir / "telemetry.csv").read_text().splitlines()
        header = lines[0].split(",")

        # Columns come from the device's declared QLCP groups, and the analog heater is
        # a real column rather than a boolean that could only ever read 0.
        assert header[:2] == ["device_timestamp", "source"]
        assert header[-2:] == ["source_provider", "source_key"]
        assert all(line.split(",")[-2:] == ["qlcp", device.device_name] for line in lines[1:])
        assert metadata["telemetry"]["columns"][-2:] == ["source_provider", "source_key"]
        assert "PT101 [PSI]" in header
        assert "heater_HEATER1" in header
        assert "relay_SAFE24" in header
        assert "valve_AV101" in header

        valve_index = header.index("valve_AV101")
        valve_column = [line.split(",")[valve_index] for line in lines[1:]]
        # AV101 defaults to OPEN (1 for a valve) and is commanded CLOSED (0) mid-run.
        assert valve_column[0] == "1"
        assert valve_column[-1] == "0"
        assert len(lines) - 1 == metadata["telemetry"]["rows"]

        # Metadata records what happened, including the component that could not start.
        assert metadata["status"] == "completed"
        assert metadata["components"]["telemetry"]["status"] == "ok"
        assert metadata["components"]["audio"]["status"] == "failed"
        assert metadata["components"]["cameras"]["status"] == "skipped"
        assert metadata["clock"]["timesync_timebase"] == "server_monotonic"
        assert json.loads((session_dir / "session.json").read_text())["id"] == session_id

        # And the whole session downloads as one archive.
        blob = b"".join(iter_session_zip(session_dir, session_id))
        with zipfile.ZipFile(io.BytesIO(blob)) as archive:
            assert archive.testzip() is None
            assert sorted(archive.namelist()) == [f"{session_id}/session.json", f"{session_id}/telemetry.csv"]
            assert archive.read(f"{session_id}/telemetry.csv") == (session_dir / "telemetry.csv").read_bytes()

    asyncio.run(run())
