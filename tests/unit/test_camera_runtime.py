from __future__ import annotations
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, cast

import pytest

from vector.runtime.camera_runtime import CameraRuntime, _filename_token
from vector.runtime.recording_paths import RecordingPaths


if TYPE_CHECKING:
    from pathlib import Path

    from vector.drivers.camera import Camera


class _FakeCamera:
    def __init__(self, hostname: str, address: str = "10.0.0.5") -> None:
        self.hostname = hostname
        self.address = address


def _runtime(tmp_path: Path) -> CameraRuntime:
    return CameraRuntime(
        cast("Any", None),
        
        camera_account={"username": "user", "password": "pass"},
        recording_paths=RecordingPaths.from_config({"root": str(tmp_path), "mediamtx_container_root": "/recordings"}),
    )


@pytest.mark.parametrize(
    ("hostname", "expected"),
    [
        ("Cam1", "Cam1"),
        ("north_bay-2", "north_bay-2"),
        ("Camera One", "Camera-One"),
        # The hostname is whatever the camera reports over ONVIF: a separator would move
        # the recording out of the session directory, and a '%' would be eaten by
        # MediaMTX's own placeholder expansion.
        ("../../etc/cam", "etc-cam"),
        ("%path", "path"),
        ("", "camera"),
        ("///", "camera"),
    ],
)
def test_filename_token_keeps_only_characters_safe_in_a_record_path(hostname: str, expected: str) -> None:
    assert _filename_token(hostname) == expected


def test_record_path_is_confined_to_the_idle_directory_when_no_session_is_active(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)

    record_path = runtime._record_path_for(cast("Camera", _FakeCamera("../../etc/cam")))  # noqa: SLF001

    assert record_path == "/recordings/_unassigned/etc-cam_%path_%Y%m%d_%H%M%S_%f"


def test_record_path_is_confined_to_the_session_video_directory(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    runtime._session_video_dir = PurePosixPath("/recordings/2026-08-10_143005_hotfire/video")  # noqa: SLF001

    record_path = runtime._record_path_for(cast("Camera", _FakeCamera("../escape")))  # noqa: SLF001

    assert record_path == "/recordings/2026-08-10_143005_hotfire/video/escape_%path_%Y%m%d_%H%M%S_%f"
