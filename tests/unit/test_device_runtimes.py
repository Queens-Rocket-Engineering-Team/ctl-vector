from __future__ import annotations
import asyncio
from pathlib import Path, PurePosixPath
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from vector.config import MumbleConfig
from vector.core import Core
from vector.runtime.audio_runtime import AudioRuntime
from vector.runtime.camera_runtime import CameraRuntime
from vector.runtime.kasa_runtime import KasaRuntime
from vector.runtime.recording_paths import RecordingPaths


def _make_kasa_runtime() -> KasaRuntime:
    return KasaRuntime(core=Core())


# ---------------------------------------------------------------------------
# AudioRuntime
# ---------------------------------------------------------------------------


def _make_audio_config(tmp_path: Path) -> MumbleConfig:
    return cast(MumbleConfig, {
        "ip": "127.0.0.1",
        "port": 64738,
        "temp_recording_dir": str(tmp_path / "tmp"),
    })


def test_audio_start_raises_when_already_recording(tmp_path: Path) -> None:
    config = _make_audio_config(tmp_path)
    runtime = AudioRuntime(config)

    # Force the "already recording" branch by setting _mumble to a truthy value.
    runtime._mumble = MagicMock()  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match="already recording"):
        runtime.start(tmp_path / "audio")


def test_audio_stop_raises_when_not_recording(tmp_path: Path) -> None:
    config = _make_audio_config(tmp_path)
    runtime = AudioRuntime(config)

    with pytest.raises(RuntimeError, match="not recording"):
        runtime.stop()


# ---------------------------------------------------------------------------
# CameraRuntime
# ---------------------------------------------------------------------------


def _make_camera_runtime(tmp_path: Path) -> CameraRuntime:
    mediamtx = MagicMock()
    return CameraRuntime(
        mediamtx,
        cameras=[],
        camera_account={"username": "user", "password": "pass"},
        recording_paths=RecordingPaths.from_config({"root": str(tmp_path), "mediamtx_container_root": "/recordings"}),
    )


def _register_fake_camera(runtime: CameraRuntime, ip: str = "10.0.0.1") -> MagicMock:
    fake_camera = MagicMock()
    fake_camera.address = ip
    fake_camera.hostname = "Cam1"
    runtime._registry[ip] = fake_camera  # type: ignore[assignment]
    return fake_camera


def test_camera_require_camera_raises_key_error(tmp_path: Path) -> None:
    runtime = _make_camera_runtime(tmp_path)

    with pytest.raises(KeyError, match="10.0.0.99"):
        runtime._require_camera("10.0.0.99")  # type: ignore[attr-defined]


def test_camera_record_path_uses_the_idle_location_outside_a_session(tmp_path: Path) -> None:
    runtime = _make_camera_runtime(tmp_path)
    camera = _register_fake_camera(runtime)

    assert runtime._record_path_for(camera) == "/recordings/_unassigned/Cam1_%path_%Y%m%d_%H%M%S_%f"  # type: ignore[attr-defined]


def test_camera_record_path_targets_the_session_video_dir(tmp_path: Path) -> None:
    runtime = _make_camera_runtime(tmp_path)
    camera = _register_fake_camera(runtime)
    runtime._session_video_dir = PurePosixPath("/recordings/2026-08-10_143005_hotfire/video")  # type: ignore[assignment]

    assert runtime._record_path_for(camera) == "/recordings/2026-08-10_143005_hotfire/video/Cam1_%path_%Y%m%d_%H%M%S_%f"  # type: ignore[attr-defined]


def test_camera_start_session_recording_reports_per_camera_failures(tmp_path: Path) -> None:

    async def run() -> None:
        runtime = _make_camera_runtime(tmp_path)
        _register_fake_camera(runtime, "10.0.0.1")
        _register_fake_camera(runtime, "10.0.0.2")

        # Fail the first camera's PATCH, succeed the second.
        responses = {"10.0.0.1": MagicMock(status=500), "10.0.0.2": MagicMock(status=200)}
        runtime._mediamtx.set_record_config = AsyncMock(  # type: ignore[method-assign]
            side_effect=lambda _client, ip, **_kwargs: responses[ip],
        )

        try:
            results = await runtime.start_session_recording(PurePosixPath("/recordings/session/video"))
        finally:
            # CameraRuntime._get_http_session() creates a real aiohttp.ClientSession;
            # close it so the test does not leak an unclosed session.
            await runtime.close()

        assert results["10.0.0.2"] is None
        assert "status 500" in (results["10.0.0.1"] or "")

    asyncio.run(run())


def test_camera_stop_session_recording_clears_the_session_dir(tmp_path: Path) -> None:

    async def run() -> None:
        runtime = _make_camera_runtime(tmp_path)
        _register_fake_camera(runtime)
        runtime._mediamtx.set_record_config = AsyncMock(return_value=MagicMock(status=200))  # type: ignore[method-assign]
        runtime._session_video_dir = PurePosixPath("/recordings/session/video")  # type: ignore[assignment]

        try:
            results = await runtime.stop_session_recording()
        finally:
            await runtime.close()

        assert results == {"10.0.0.1": None}
        assert runtime._session_video_dir is None  # type: ignore[attr-defined]

    asyncio.run(run())


def test_settle_video_returns_true_once_sizes_stop_changing(tmp_path: Path) -> None:
    video_dir = tmp_path / "video"
    video_dir.mkdir()
    (video_dir / "clip.mp4").write_bytes(b"frames")

    settled = asyncio.run(CameraRuntime.settle_video(video_dir, timeout_s=2.0, quiet_s=0.1, tick_s=0.02))

    assert settled is True


def test_settle_video_times_out_while_a_file_keeps_growing(tmp_path: Path) -> None:
    video_dir = tmp_path / "video"
    video_dir.mkdir()
    clip = video_dir / "clip.mp4"
    clip.write_bytes(b"")

    async def run() -> bool:
        async def keep_writing() -> None:
            while True:
                with clip.open("ab") as handle:
                    handle.write(b"more")
                await asyncio.sleep(0.02)

        writer = asyncio.create_task(keep_writing())
        try:
            return await CameraRuntime.settle_video(video_dir, timeout_s=0.4, quiet_s=0.2, tick_s=0.02)
        finally:
            writer.cancel()
            await asyncio.gather(writer, return_exceptions=True)

    assert asyncio.run(run()) is False


# ---------------------------------------------------------------------------
# KasaRuntime
# ---------------------------------------------------------------------------


def test_kasa_get_device_returns_none_for_unknown_host() -> None:
    runtime = _make_kasa_runtime()
    assert runtime.get_device("192.168.1.99") is None


def test_kasa_require_device_raises_key_error() -> None:
    runtime = _make_kasa_runtime()
    with pytest.raises(KeyError, match="No Kasa device found"):
        runtime._require_device("192.168.1.99")  # type: ignore[attr-defined]


def test_kasa_set_device_state_raises_key_error_for_unknown_host() -> None:

    async def run() -> None:
        runtime = _make_kasa_runtime()
        with pytest.raises(KeyError):
            await runtime.set_state("192.168.1.99", True)

    asyncio.run(run())
