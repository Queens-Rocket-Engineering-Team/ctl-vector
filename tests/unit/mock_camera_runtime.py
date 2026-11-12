from unittest.mock import MagicMock

from pathlib import Path

from vector.runtime.camera_runtime import CameraRuntime
from vector.runtime.recording_paths import RecordingPaths


def make_mock_camera_runtime(tmp_path: Path) -> CameraRuntime:
    mediamtx = MagicMock()
    camera_runtime = CameraRuntime(
        mediamtx,
        camera_account={"username": "user", "password": "pass"},
        recording_paths=RecordingPaths.from_config({"root": str(tmp_path), "mediamtx_container_root": "/recordings"}),
    )
    return camera_runtime