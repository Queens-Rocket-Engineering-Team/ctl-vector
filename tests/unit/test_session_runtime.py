from __future__ import annotations
import asyncio
import json
from pathlib import Path, PurePosixPath
from typing import Any

import pytest

from vector.core import ControlDefinition, Core, SensorDefinition, TelemetryBatch, TelemetryReading
from vector.runtime.recording_paths import RecordingPaths
from vector.runtime.session_runtime import SessionConflictError, SessionRuntime, slugify
from vector.runtime.session_telemetry import TelemetrySessionPublisher
from vector.state.system_state import SystemState


def _register_device(state: SystemState, device_name: str = "MockDevice", address: str = "10.0.0.1") -> None:
    """Register resources so a recording started now knows its columns."""
    state.core.register_source(
        "test", device_name,
        address=address,
        connection_key=f"conn-{address}",
        sensors=(SensorDefinition("PT101", "pressure_transducer", "PSI"),),
        controls=(ControlDefinition("AV101", "valve", default=False),),
    )


class _FakeCamera:
    def __init__(self, address: str) -> None:
        self.address = address
        self.hostname = f"Cam-{address}"


class _FakeCameraRuntime:
    """Stands in for MediaMTX. `failures` maps camera IP -> failure detail."""

    def __init__(
        self,
        addresses: list[str] | None = None,
        failures: dict[str, str] | None = None,
        *,
        hangs_on_stop: bool = False,
        start_delay_s: float = 0.0,
    ) -> None:
        self._cameras = [_FakeCamera(address) for address in (addresses or [])]
        self._failures = failures or {}
        self._hangs_on_stop = hangs_on_stop
        self._start_delay_s = start_delay_s
        self.session_video_dir: PurePosixPath | None = None
        self.stopped = False

    def cameras(self) -> list[_FakeCamera]:
        return list(self._cameras)

    async def start_session_recording(self, container_video_dir: PurePosixPath) -> dict[str, str | None]:
        if self._start_delay_s:
            await asyncio.sleep(self._start_delay_s)
        self.session_video_dir = container_video_dir
        return {camera.address: self._failures.get(camera.address) for camera in self._cameras}

    async def stop_session_recording(self) -> dict[str, str | None]:
        if self._hangs_on_stop:
            await asyncio.sleep(3600)
        self.stopped = True
        self.session_video_dir = None
        return {camera.address: None for camera in self._cameras}

    @staticmethod
    async def settle_video(video_dir: Path, **_kwargs: Any) -> bool:  # noqa: ARG004
        return True


class _FakeAudioRuntime:
    def __init__(self, *, error: Exception | None = None) -> None:
        self._error = error
        self.output_dir: Path | None = None
        self.stopped = False

    def start(self, output_dir: Path) -> dict[str, str]:
        if self._error is not None:
            raise self._error
        self.output_dir = output_dir
        return {"status": "started"}

    def stop(self) -> dict[str, str | None]:
        if self._error is not None:
            raise RuntimeError("not recording")
        self.stopped = True
        return {"status": "stopped", "file": "voice.opus"}


class _FakeStateStream:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def publish(self, event: dict[str, object] | None) -> None:
        if event is not None:
            self.events.append(event)


def _make_runtime(
    tmp_path: Path,
    *,
    cameras: list[str] | None = None,
    camera_failures: dict[str, str] | None = None,
    audio_error: Exception | None = None,
    cameras_hang_on_stop: bool = False,
    camera_start_delay_s: float = 0.0,
    shutdown_timeout_s: float = 30.0,
) -> tuple[SessionRuntime, SystemState, TelemetrySessionPublisher, _FakeStateStream, _FakeCameraRuntime, _FakeAudioRuntime]:
    state = SystemState(core=Core())
    _register_device(state)
    publisher = TelemetrySessionPublisher()
    stream = _FakeStateStream()
    camera_runtime = _FakeCameraRuntime(cameras, camera_failures, hangs_on_stop=cameras_hang_on_stop, start_delay_s=camera_start_delay_s)
    audio_runtime = _FakeAudioRuntime(error=audio_error)
    runtime = SessionRuntime(
        paths=RecordingPaths.from_config({"root": str(tmp_path), "mediamtx_container_root": "/recordings"}),
        telemetry_publisher=publisher,
        system_state=state,
        state_stream=stream,  # type: ignore[arg-type]
        camera_runtime=camera_runtime,  # type: ignore[arg-type]
        audio_runtime=audio_runtime,  # type: ignore[arg-type]
        shutdown_timeout_s=shutdown_timeout_s,
    )
    return runtime, state, publisher, stream, camera_runtime, audio_runtime


def _batch(value: float = 1.0) -> TelemetryBatch:
    return TelemetryBatch(
        source_provider="qlcp",
        source_key="MockDevice",
        device_name="MockDevice",
        device_address="10.0.0.1",
        connection_key="10.0.0.1:1",
        timestamp_s=100.0,
        timestamp_source="server_receive",
        timestamp_synced=False,
        readings=(TelemetryReading(sensor_id=0, sensor_name="PT101", value=value, unit_name="PSI", sensor_type="pressure_transducer"),),
    )


# ---------------------------------------------------------------------------
# Slug and id
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("Hot Fire 3", "hot-fire-3"),
        ("  cold_flow!!  ", "cold-flow"),
        ("", "session"),
        ("///", "session"),
        ("x" * 100, "x" * 64),
    ],
)
def test_slugify_produces_ids_the_path_guard_accepts(name: str, expected: str) -> None:
    assert slugify(name) == expected


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def test_start_creates_a_session_and_records_telemetry(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, _state, publisher, stream, cameras, audio = _make_runtime(tmp_path, cameras=["10.0.0.5"])

        status = await runtime.start("Hot Fire 3")
        publisher.publish_batch(_batch())
        metadata = await runtime.stop()

        session_dir = tmp_path / status["id"]
        assert status["id"].endswith("_hot-fire-3")
        assert metadata["status"] == "completed"
        assert metadata["end_reason"] == "operator"
        assert metadata["telemetry"]["rows"] == 1
        assert (session_dir / "telemetry.csv").is_file()
        assert json.loads((session_dir / "session.json").read_text())["status"] == "completed"
        assert cameras.stopped and audio.stopped
        assert [event["type"] for event in stream.events] == ["session.started", "session.updated", "session.stopped"]

    asyncio.run(run())


def test_camera_video_dir_is_translated_into_the_media_server_filesystem(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, _state, _publisher, _stream, cameras, _audio = _make_runtime(tmp_path, cameras=["10.0.0.5"])

        status = await runtime.start("hotfire")
        video_dir = cameras.session_video_dir
        await runtime.stop()

        assert video_dir == PurePosixPath(f"/recordings/{status['id']}/video")

    asyncio.run(run())


def test_telemetry_is_only_recorded_between_start_and_stop(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, _state, publisher, _stream, _cameras, _audio = _make_runtime(tmp_path)

        publisher.publish_batch(_batch())  # before: dropped
        status = await runtime.start("hotfire")
        publisher.publish_batch(_batch())
        await runtime.stop()
        publisher.publish_batch(_batch())  # after: dropped

        rows = (tmp_path / status["id"] / "telemetry.csv").read_text().splitlines()
        assert len(rows) == 2  # header plus one row

    asyncio.run(run())


def test_starting_twice_conflicts(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, *_ = _make_runtime(tmp_path)
        await runtime.start("hotfire")
        try:
            with pytest.raises(SessionConflictError, match="already active"):
                await runtime.start("second")
        finally:
            await runtime.stop()

    asyncio.run(run())


def test_stopping_when_idle_conflicts(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, *_ = _make_runtime(tmp_path)
        with pytest.raises(SessionConflictError, match="No recording session is active"):
            await runtime.stop()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# Partial starts
# ---------------------------------------------------------------------------


def test_a_failing_camera_does_not_stop_the_others_or_the_session(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, *_ = _make_runtime(tmp_path, cameras=["10.0.0.5", "10.0.0.6"], camera_failures={"10.0.0.5": "status 500"})

        status = await runtime.start("hotfire")
        components = status["components"]

        assert components["camera:10.0.0.5"] == {"status": "failed", "detail": "status 500"}
        assert components["camera:10.0.0.6"]["status"] == "ok"
        assert components["telemetry"]["status"] == "ok"

        await runtime.stop()

    asyncio.run(run())


def test_audio_failure_is_recorded_but_the_session_still_runs(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, _state, publisher, _stream, _cameras, _audio = _make_runtime(tmp_path, audio_error=ConnectionRefusedError("mumble is down"))

        status = await runtime.start("hotfire")
        publisher.publish_batch(_batch())
        metadata = await runtime.stop()

        assert status["components"]["audio"]["status"] == "failed"
        assert "mumble is down" in status["components"]["audio"]["detail"]
        assert metadata["telemetry"]["rows"] == 1

    asyncio.run(run())


def test_no_cameras_registered_is_skipped_not_failed(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, *_ = _make_runtime(tmp_path)

        status = await runtime.start("hotfire")
        await runtime.stop()

        assert status["components"]["cameras"] == {"status": "skipped", "detail": "no cameras registered"}

    asyncio.run(run())


def test_a_session_that_cannot_open_its_csv_leaves_nothing_behind(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, *_ = _make_runtime(tmp_path)
        # Make the recordings root unwritable so the CSV open fails after mkdir.
        (tmp_path / "block").mkdir()

        original = SessionRuntime._open_session

        def explode(self: SessionRuntime, _name: str) -> None:
            directory = self._paths.session_dir("2026-01-01_000000_boom")
            directory.mkdir(parents=True)
            raise OSError("disk full")

        SessionRuntime._open_session = explode  # type: ignore[method-assign, assignment]
        try:
            with pytest.raises(OSError, match="disk full"):
                await runtime.start("boom")
        finally:
            SessionRuntime._open_session = original  # type: ignore[method-assign]

        # The phase must be back to idle so the next attempt is not blocked.
        assert await runtime.start("recovered")
        await runtime.stop()

    asyncio.run(run())


def test_a_failure_after_the_prologue_does_not_wedge_the_state_machine(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, _state, publisher, stream, *_ = _make_runtime(tmp_path)

        calls = {"n": 0}
        original = SessionRuntime._write_metadata

        def flaky(self: SessionRuntime, session: Any, *, status: str) -> None:
            calls["n"] += 1
            if calls["n"] == 2:  # the rewrite after components start
                raise OSError("disk full")
            original(self, session, status=status)

        SessionRuntime._write_metadata = flaky  # type: ignore[method-assign, assignment]
        try:
            with pytest.raises(OSError, match="disk full"):
                await runtime.start("doomed")
        finally:
            SessionRuntime._write_metadata = original  # type: ignore[method-assign]

        # The runtime must accept the next session rather than 409-ing until restart.
        assert publisher._writer is None
        assert stream.events[-1]["type"] == "session.stopped"
        status = await runtime.start("recovered")
        publisher.publish_batch(_batch())
        await runtime.stop()
        assert (tmp_path / status["id"] / "telemetry.csv").read_text().count("\n") == 2

    asyncio.run(run())


def test_cancelling_start_midway_does_not_wedge_the_state_machine(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, *_ = _make_runtime(tmp_path, cameras=["10.0.0.5"], cameras_hang_on_stop=False)

        async def slow_components(_self: SessionRuntime, _session: Any) -> None:
            await asyncio.sleep(3600)

        original = SessionRuntime._start_components
        SessionRuntime._start_components = slow_components  # type: ignore[method-assign, assignment]
        try:
            task = asyncio.create_task(runtime.start("interrupted"))
            await asyncio.sleep(0.05)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            SessionRuntime._start_components = original  # type: ignore[method-assign]

        assert await runtime.start("recovered")
        await runtime.stop()

    asyncio.run(run())


def test_a_failure_while_stopping_still_returns_the_runtime_to_idle(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, *_ = _make_runtime(tmp_path)
        await runtime.start("hotfire")

        original = SessionRuntime._finish

        async def explode(_self: SessionRuntime, _session: Any, *, end_reason: str) -> dict[str, Any]:  # noqa: ARG001
            raise OSError("disk full")

        SessionRuntime._finish = explode  # type: ignore[method-assign, assignment]
        try:
            with pytest.raises(OSError, match="disk full"):
                await runtime.stop()
        finally:
            SessionRuntime._finish = original  # type: ignore[method-assign]

        assert runtime.status() is None
        assert await runtime.start("recovered")
        await runtime.stop()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# Shutdown
# ---------------------------------------------------------------------------


def test_finalize_on_shutdown_closes_the_csv_even_when_cameras_hang(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, _state, publisher, _stream, _cameras, _audio = _make_runtime(
            tmp_path,
            cameras=["10.0.0.5"],
            cameras_hang_on_stop=True,
            shutdown_timeout_s=0.2,
        )

        status = await runtime.start("hotfire")
        publisher.publish_batch(_batch())

        await asyncio.wait_for(runtime.finalize_on_shutdown(), timeout=5.0)

        # The telemetry must be on disk and complete even though stopping the cameras
        # timed out, because the CSV is closed before the media server is touched.
        rows = (tmp_path / status["id"] / "telemetry.csv").read_text().splitlines()
        assert len(rows) == 2

    asyncio.run(run())


def test_finalize_on_shutdown_is_a_no_op_when_idle(tmp_path: Path) -> None:
    runtime, *_ = _make_runtime(tmp_path)
    asyncio.run(runtime.finalize_on_shutdown())


def test_finalize_on_shutdown_waits_for_a_start_still_in_flight(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, _state, publisher, _stream, _cameras, _audio = _make_runtime(
            tmp_path,
            cameras=["10.0.0.5"],
            camera_start_delay_s=0.3,
            shutdown_timeout_s=10.0,
        )

        starting = asyncio.ensure_future(runtime.start("hotfire"))
        await asyncio.sleep(0.05)

        # Shutdown lands while start() still holds the lock. Returning here would exit
        # the process with the CSV open and the session left "active" forever.
        await asyncio.wait_for(runtime.finalize_on_shutdown(), timeout=5.0)
        status = await starting

        metadata = json.loads((tmp_path / status["id"] / "session.json").read_text())
        assert metadata["status"] == "completed"
        assert metadata["end_reason"] == "server_shutdown"

    asyncio.run(run())


# ---------------------------------------------------------------------------
# Reads and path guards
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("session_id", ["../etc", "foo/bar", "/etc/passwd", "not-a-session", "2026-08-10_143005_Hotfire", ""])
def test_get_session_dir_rejects_ids_outside_the_root(tmp_path: Path, session_id: str) -> None:
    runtime, *_ = _make_runtime(tmp_path)

    with pytest.raises(ValueError, match="Invalid session"):
        runtime.get_session_dir(session_id)


def test_get_session_dir_raises_not_found_for_a_well_formed_unknown_id(tmp_path: Path) -> None:
    runtime, *_ = _make_runtime(tmp_path)

    with pytest.raises(FileNotFoundError):
        runtime.get_session_dir("2026-08-10_143005_hotfire")


def test_resolve_session_file_rejects_traversal_and_symlinks(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, *_ = _make_runtime(tmp_path)
        status = await runtime.start("hotfire")
        await runtime.stop()
        session_dir = tmp_path / status["id"]

        secret = tmp_path / "secret.txt"
        secret.write_text("not yours")
        (session_dir / "link.txt").symlink_to(secret)

        assert runtime.resolve_session_file(status["id"], "telemetry.csv").name == "telemetry.csv"
        with pytest.raises(ValueError, match="Invalid file path"):
            runtime.resolve_session_file(status["id"], "../secret.txt")
        # The symlink resolves outside the session directory, so it fails containment.
        with pytest.raises(ValueError, match="Invalid file path"):
            runtime.resolve_session_file(status["id"], "link.txt")
        with pytest.raises(FileNotFoundError):
            runtime.resolve_session_file(status["id"], "nosuchfile.csv")

    asyncio.run(run())


def test_list_sessions_is_newest_first_and_survives_unreadable_metadata(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, *_ = _make_runtime(tmp_path)
        first = (await runtime.start("first"))["id"]
        await runtime.stop()
        second = (await runtime.start("second"))["id"]
        await runtime.stop()

        # A session that died before writing metadata must still be listed.
        broken = tmp_path / "2020-01-01_000000_broken"
        broken.mkdir()
        (broken / "session.json").write_text("{not json")

        summaries = runtime.list_sessions()
        by_id = {summary["id"]: summary for summary in summaries}

        assert [summary["id"] for summary in summaries[:2]] == [second, first]
        assert by_id[broken.name]["status"] == "unknown"
        assert by_id[first]["name"] == "first"
        assert by_id[first]["size_bytes"] > 0

    asyncio.run(run())


def test_list_sessions_skips_symlinked_directories(tmp_path: Path) -> None:
    root = tmp_path / "recordings"
    root.mkdir()
    runtime, *_ = _make_runtime(root)

    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "secret.txt").write_text("not session data")
    # A link is listed as a directory by is_dir(), but get_session_dir refuses to open
    # it, so listing it would advertise a session nobody can download.
    (root / "2026-08-10_143005_linked").symlink_to(elsewhere, target_is_directory=True)

    assert runtime.list_sessions() == []


def test_read_metadata_returns_live_state_for_the_active_session(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, _state, publisher, *_ = _make_runtime(tmp_path)
        status = await runtime.start("hotfire")
        publisher.publish_batch(_batch())

        live = runtime.read_metadata(status["id"])
        assert live["status"] == "active"
        assert live["telemetry"]["rows"] == 1
        assert runtime.is_active(status["id"])

        await runtime.stop()
        assert runtime.read_metadata(status["id"])["status"] == "completed"
        assert not runtime.is_active(status["id"])

    asyncio.run(run())


def test_metadata_records_the_clock_relation_used_to_align_video(tmp_path: Path) -> None:

    async def run() -> None:
        runtime, *_ = _make_runtime(tmp_path)
        await runtime.start("hotfire")
        metadata = await runtime.stop()

        clock = metadata["clock"]
        assert clock["timesync_timebase"] == "server_monotonic"
        assert clock["stopped_unix"] >= clock["started_unix"]
        assert clock["stopped_monotonic"] >= clock["started_monotonic"]

    asyncio.run(run())
