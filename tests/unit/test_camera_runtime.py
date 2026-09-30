from __future__ import annotations
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sys
from unittest.mock import MagicMock

# Define a fake camera class for mocking
class _FakeCamera:
    def __init__(self, address: str, port: int = 2020) -> None:
        self.address = address
        self.port = port
        self.hostname = address  # Initial hostname, will be updated on connect
        self.recording = False

    async def connect(self, username: str, password: str) -> None:
        # Simulate successful connection and ONVIF GetHostname
        self.hostname = f"Camera-{self.address}"

    def set_recording(self, recording: bool) -> None:
        self.recording = recording

# Mock the problematic modules to avoid QLCP bindings which are not built
sys.modules['vector.qlcp._bindings'] = MagicMock()
sys.modules['vector.drivers.esp'] = MagicMock()
mock_camera = MagicMock()
mock_camera.Camera = _FakeCamera
sys.modules['vector.drivers.camera'] = mock_camera

# Mock WSDiscovery for discovery tests
sys.modules['wsdiscovery'] = MagicMock()
sys.modules['wsdiscovery.service'] = MagicMock()

# Now import the module under test
from vector.runtime.camera_runtime import CameraRuntime, _filename_token
from vector.runtime.recording_paths import RecordingPaths


if TYPE_CHECKING:
    from pathlib import Path

    from vector.drivers.camera import Camera


def _runtime(tmp_path: Path) -> CameraRuntime:
    return CameraRuntime(
        cast("Any", None),
        cameras=[],
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


def test_connect_all_cameras_with_discovery(tmp_path: Path) -> None:
    """Test that connect_all_cameras uses discovery when enabled."""
    # Mock the discovery function to return a camera
    discovered_cameras = [{"ip": "192.168.0.100", "onvif_port": 2020}]
    
    runtime = _runtime(tmp_path)
    
    # Mock _configure_media_server_for_camera to do nothing
    async def mock_configure_media_server(*args, **kwargs):
        pass
    
    runtime._configure_media_server_for_camera = mock_configure_media_server
    
    # Mock the discovery function
    with patch("vector.runtime.camera_discovery.discover_onvif_cameras", return_value=discovered_cameras):
        # Enable discovery in the runtime
        runtime._discovery_enabled = True
        runtime._onvif_port = 2020
        runtime._discovery_timeout = 0.1
        runtime._discovery_max_retries = 1
        
        # Set up empty configured cameras (we'll rely on discovery)
        runtime._cameras = []
        
        # Call connect_all_cameras
        import asyncio
        asyncio.run(runtime.connect_all_cameras())
        
        # Should have registered the discovered camera
        assert len(runtime._registry) == 1
        assert "192.168.0.100" in runtime._registry
        camera = runtime._registry["192.168.0.100"]
        assert isinstance(camera, _FakeCamera)
        assert camera.hostname == "Camera-192.168.0.100"
        assert camera.address == "192.168.0.100"


def test_connect_all_cameras_with_configured_and_discovered(tmp_path: Path) -> None:
    """Test that connect_all_cameras uses both configured and discovered cameras."""
    # Mock the discovery function to return a camera
    discovered_cameras = [{"ip": "192.168.0.100", "onvif_port": 2020}]
    
    runtime = _runtime(tmp_path)
    
    # Mock _configure_media_server_for_camera to do nothing
    async def mock_configure_media_server(*args, **kwargs):
        pass
    
    runtime._configure_media_server_for_camera = mock_configure_media_server
    
    # Mock the discovery function
    with patch("vector.runtime.camera_discovery.discover_onvif_cameras", return_value=discovered_cameras):
        # Enable discovery in the runtime
        runtime._discovery_enabled = True
        runtime._onvif_port = 2020
        runtime._discovery_timeout = 0.1
        runtime._discovery_max_retries = 1
        
        # Set up configured cameras
        runtime._cameras = [
            {"ip": "192.168.0.101", "onvif_port": 2020},  # Different IP from discovered
            {"ip": "192.168.0.100", "onvif_port": 2020},   # Same IP as discovered (should not duplicate)
        ]
        
        # Call connect_all_cameras
        import asyncio
        asyncio.run(runtime.connect_all_cameras())
        
        # Should have registered both cameras (no duplicates)
        assert len(runtime._registry) == 2
        assert "192.168.0.100" in runtime._registry
        assert "192.168.0.101" in runtime._registry
        
        # Check the cameras
        camera_100 = runtime._registry["192.168.0.100"]
        camera_101 = runtime._registry["192.168.0.101"]
        assert isinstance(camera_100, _FakeCamera)
        assert isinstance(camera_101, _FakeCamera)
        assert camera_100.hostname == "Camera-192.168.0.100"
        assert camera_101.hostname == "Camera-192.168.0.101"


def test_connect_all_cameras_discovery_disabled(tmp_path: Path) -> None:
    """Test that connect_all_cameras uses only configured cameras when discovery is disabled."""
    runtime = _runtime(tmp_path)
    
    # Mock _configure_media_server_for_camera to do nothing
    async def mock_configure_media_server(*args, **kwargs):
        pass
    
    runtime._configure_media_server_for_camera = mock_configure_media_server
    
    # Set up configured cameras
    runtime._cameras = [
        {"ip": "192.168.0.100", "onvif_port": 2020},
        {"ip": "192.168.0.101", "onvif_port": 2020},
    ]
    
    # Disable discovery
    runtime._discovery_enabled = False
    
    # Call connect_all_cameras
    import asyncio
    asyncio.run(runtime.connect_all_cameras())
    
    # Should have registered only the configured cameras
    assert len(runtime._registry) == 2
    assert "192.168.0.100" in runtime._registry
    assert "192.168.0.101" in runtime._registry


def test_connect_all_cameras_no_cameras_found(tmp_path: Path) -> None:
    """Test that connect_all_cameras handles no cameras found gracefully."""
    runtime = _runtime(tmp_path)
    
    # Mock _configure_media_server_for_camera to do nothing
    async def mock_configure_media_server(*args, **kwargs):
        pass
    
    runtime._configure_media_server_for_camera = mock_configure_media_server
    
    # Mock discovery to return no cameras
    with patch("vector.runtime.camera_discovery.discover_onvif_cameras", return_value=[]):
        # Enable discovery in the runtime
        runtime._discovery_enabled = True
        runtime._onvif_port = 2020
        runtime._discovery_timeout = 0.1
        runtime._discovery_max_retries = 1
        
        # Set up empty configured cameras
        runtime._cameras = []
        
        # Call connect_all_cameras
        import asyncio
        asyncio.run(runtime.connect_all_cameras())
        
        # Should have registered no cameras
        assert len(runtime._registry) == 0