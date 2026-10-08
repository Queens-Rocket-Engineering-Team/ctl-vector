import asyncio
import logging
import re
import time
from pathlib import Path, PurePosixPath

import aiohttp

from onvif import ONVIFDiscovery

from vector.config import AccountServiceConfig, CameraConfig
from vector.drivers.camera import Camera
from vector.integrations.mediamtx import MediaMTXClient
from vector.runtime.recording_paths import RecordingPaths


logger = logging.getLogger(__name__)

# Where recordPath points when no session is active. Nothing is ever written there:
# paths are added with record disabled, but MediaMTX still wants a valid template.
IDLE_RECORD_SUBDIR = "_unassigned"

# MediaMTX finalizes an fmp4 segment on the path goroutine, after the config PATCH has
# already returned 200, so files keep growing for a moment after recording stops.
VIDEO_SETTLE_TIMEOUT_S = 6.0
VIDEO_SETTLE_QUIET_S = 1.5
VIDEO_SETTLE_TICK_S = 0.25

_UNSAFE_IN_FILENAME = re.compile(r"[^A-Za-z0-9_-]")


def _filename_token(text: str) -> str:
    """Reduce *text* to something safe to interpolate into a recordPath template.

    The hostname comes back from the camera over ONVIF, so it is whatever the device
    was configured with: a '/' would move recordings out of the session directory, and
    a literal '%' would corrupt MediaMTX's own placeholders. Only the filename is
    sanitized -- the real hostname still appears in session metadata and the UI.
    """
    return _UNSAFE_IN_FILENAME.sub("-", text).strip("-") or "camera"


class CameraRuntime:
    def __init__(
        self,
        mediamtx: MediaMTXClient,
        *,
        cameras: list[CameraConfig],
        camera_account: AccountServiceConfig,
        recording_paths: RecordingPaths,
    ) -> None:
        self._registry: dict[str, Camera] = {}
        self._mediamtx = mediamtx
        self._cameras = cameras
        self._camera_account = camera_account
        self._paths = recording_paths
        self._session_video_dir: PurePosixPath | None = None
        self._http_session: aiohttp.ClientSession | None = None

    def _get_http_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession()
        return self._http_session

    async def close(self) -> None:
        if self._http_session is not None and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None

    def cameras(self) -> list[Camera]:
        return list(self._registry.values())

    # TODO Needs testing in server
    def discover_cameras(self) -> None:
        cam_finder = ONVIFDiscovery(timeout=5)
        onvif_results = cam_finder.discover(search="NetworkVideoTransmitter")
        cameras: list[CameraConfig] = [
            # NOTE May need to change port to 2020 manually
            {"ip": res["host"], "onvif_port": res["port"]}
            for res in onvif_results
        ]
        self._cameras=cameras



    async def connect_all_cameras(self) -> None:
        """Connect to all configured cameras and register them, in parallel."""
        http_client = self._get_http_session()
        cam_username, cam_password = self._camera_credentials()

        async def connect_one(camera: CameraConfig) -> None:
            camera_object = await self.register_camera(camera["ip"], camera["onvif_port"])
            if camera_object is None:
                return
            await self._configure_media_server_for_camera(
                http_client,
                camera_object,
                username=cam_username,
                password=cam_password,
            )

        await asyncio.gather(*(connect_one(camera) for camera in self._cameras))

    async def register_camera(self, ip: str, port: int) -> Camera | None:
        """Register a camera with its IP and ONVIF port."""
        # Reset camera in registry if exists
        self._registry.pop(ip, None)

        logger.info("Attempting to connect to camera at %s", ip)

        try:
            # Create camera object and connect to it
            camera_object = Camera(ip, port)
            await camera_object.connect(*self._camera_credentials())

            logger.info("Connected to camera %s (%s)", camera_object.hostname, ip)

            self._registry[ip] = camera_object
            return camera_object
        except Exception:
            logger.exception("Failed to connect to camera at %s", ip)
            return None

    async def move_camera(self, ip: str, x: float, y: float) -> None:
        """Move a camera by relative pan/tilt amounts. Errors are logged, not raised (runs as a fire-and-forget BackgroundTask)."""
        try:
            cam = self._require_camera(ip)
            await cam.move_relative(x, y)
        except Exception:
            logger.exception("Failed to move camera at %s", ip)

    async def start_session_recording(self, container_video_dir: PurePosixPath) -> dict[str, str | None]:
        """Point every camera at *container_video_dir* and start recording.

        Returns a per-camera IP -> failure detail map, where None means the camera
        started successfully. A camera that fails does not stop the others: a session
        with two of three angles is still worth having.
        """
        self._session_video_dir = container_video_dir
        cameras = list(self._registry.values())
        results = await asyncio.gather(
            *(self._set_camera_recording(camera.address, recording=True) for camera in cameras),
            return_exceptions=True,
        )
        return {
            camera.address: None if not isinstance(result, BaseException) else str(result)
            for camera, result in zip(cameras, results, strict=True)
        }

    async def stop_session_recording(self) -> dict[str, str | None]:
        """Stop recording on every camera and send recordPath back to the idle location.

        Reverting the path matters: a later recording that landed inside a finished
        session directory would corrupt that session's archive.
        """
        self._session_video_dir = None
        cameras = list(self._registry.values())
        results = await asyncio.gather(
            *(self._set_camera_recording(camera.address, recording=False) for camera in cameras),
            return_exceptions=True,
        )
        return {
            camera.address: None if not isinstance(result, BaseException) else str(result)
            for camera, result in zip(cameras, results, strict=True)
        }

    @staticmethod
    async def settle_video(
        video_dir: Path,
        *,
        timeout_s: float = VIDEO_SETTLE_TIMEOUT_S,
        quiet_s: float = VIDEO_SETTLE_QUIET_S,
        tick_s: float = VIDEO_SETTLE_TICK_S,
    ) -> bool:
        """Wait until no ``*.mp4`` under *video_dir* has changed size for *quiet_s*.

        The config PATCH that stops recording returns before MediaMTX has run the path
        reload that closes the segment and rewrites its duration header, so an archive
        built immediately after stopping can catch a half-written file. Returns False
        on timeout, in which case the files are still readable but the last segment may
        lack a duration.
        """

        def snapshot() -> set[tuple[str, int]]:
            if not video_dir.is_dir():
                return set()
            return {(path.name, path.stat().st_size) for path in video_dir.glob("*.mp4") if path.is_file()}

        deadline = time.monotonic() + timeout_s
        previous = snapshot()
        unchanged_since = time.monotonic()

        while time.monotonic() < deadline:
            await asyncio.sleep(tick_s)
            current = snapshot()
            if current != previous:
                previous = current
                unchanged_since = time.monotonic()
            elif time.monotonic() - unchanged_since >= quiet_s:
                return True

        logger.warning("Timed out waiting for camera recordings in %s to settle", video_dir)
        return False

    def _camera_credentials(self) -> tuple[str, str]:
        return (
            self._camera_account["username"],
            self._camera_account["password"],
        )

    def _require_camera(self, ip: str) -> Camera:
        camera = self._registry.get(ip)
        if camera is None:
            msg = f"Camera {ip} does not exist"
            raise KeyError(msg)
        return camera

    def _record_path_for(self, camera: Camera) -> str:
        # MediaMTX resolves recordPath inside its own container filesystem, so the
        # session directory has to be translated across the mount point first.
        base = self._session_video_dir if self._session_video_dir is not None else self._paths.container_root / IDLE_RECORD_SUBDIR
        return str(base / f"{_filename_token(camera.hostname)}_%path_%Y%m%d_%H%M%S_%f")

    async def _configure_media_server_for_camera(
        self,
        http_client: aiohttp.ClientSession,
        camera: Camera,
        *,
        username: str,
        password: str,
    ) -> None:
        try:
            logger.info("Configuring media server for camera %s (%s)", camera.hostname, camera.address)
            await self._mediamtx.add_path(
                http_client,
                camera.address,
                source=camera.rtsp_stream_source(username, password),
                record_path=self._record_path_for(camera),
            )
        except TimeoutError:
            logger.exception("Media server configuration timed out for %s (%s)", camera.hostname, camera.address)

        # add_path resets the path's record flag, so a camera reconnecting mid-session
        # would silently stop capturing. Put it back before reading the state.
        if self._session_video_dir is not None:
            try:
                await self._set_camera_recording(camera.address, recording=True)
            except Exception:
                logger.exception("Failed to resume session recording for camera %s after reconnect", camera.address)

        await self._refresh_media_recording_state(http_client, camera)

    async def _refresh_media_recording_state(self, http_client: aiohttp.ClientSession, camera: Camera) -> None:
        record_state = await self._mediamtx.get_path_record_state(http_client, camera.address)
        if record_state is None:
            camera.set_recording(False)
            logger.error("Could not determine recording state from MediaMTX path %s; defaulting to False", camera.address)
        else:
            camera.set_recording(record_state)

    async def _set_camera_recording(self, ip: str, *, recording: bool) -> None:
        action = "start" if recording else "stop"
        action_title = "Starting" if recording else "Stopping"

        try:
            camera = self._require_camera(ip)
        except KeyError:
            logger.exception("Failed to %s recording for camera at %s: camera not registered", action, ip)
            raise

        http_client = self._get_http_session()
        try:
            logger.info("%s recording for camera at %s via media server API", action_title, ip)
            response = await self._mediamtx.set_record_config(
                http_client,
                ip,
                record=recording,
                record_path=self._record_path_for(camera),
            )

            if response.status != 200:
                msg = f"Media server API returned status {response.status}"
                raise RuntimeError(msg)

            camera.set_recording(recording)
        except asyncio.TimeoutError as err:
            logger.exception("Media server API request to %s recording timed out for camera at %s", action, ip)
            raise RuntimeError("Media server API request timed out") from err
        except RuntimeError:
            raise
        except Exception as e:
            logger.exception("Failed to %s recording for camera at %s", action, ip)

            msg = f"Failed to {action} recording for camera at {ip}: {e}"
            raise RuntimeError(msg) from e
