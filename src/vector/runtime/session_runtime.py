"""Recording sessions: one directory per test holding telemetry, audio and video."""

from __future__ import annotations
import asyncio
import contextlib
import logging
import re
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import orjson

import vector
from vector.runtime.session_telemetry import SessionTelemetryWriter, build_columns


if TYPE_CHECKING:
    from vector.runtime.audio_runtime import AudioRuntime
    from vector.runtime.camera_runtime import CameraRuntime
    from vector.runtime.recording_paths import RecordingPaths
    from vector.runtime.session_telemetry import TelemetrySessionPublisher
    from vector.runtime.state_stream import StateStream
    from vector.state.system_state import SystemState


logger = logging.getLogger(__name__)

SESSION_METADATA_FILENAME = "session.json"
TELEMETRY_FILENAME = "telemetry.csv"
AUDIO_SUBDIR = "audio"
VIDEO_SUBDIR = "video"

# How often buffered telemetry rows reach the page cache. Bounds what an abrupt exit
# loses to about a second of data rather than a megabyte of buffer.
FLUSH_INTERVAL_S = 1.0

# Shutdown must not hang on an unreachable media server or Mumble host.
SHUTDOWN_FINALIZE_TIMEOUT_S = 15.0

SESSION_ID_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{6}_[a-z0-9][a-z0-9-]{0,63}$")

_SLUG_SEPARATORS = re.compile(r"[^a-z0-9]+")

SessionPhase = Literal["idle", "starting", "active", "stopping"]
ComponentStatus = Literal["ok", "failed", "skipped"]


class SessionConflictError(RuntimeError):
    """A session operation was requested that the current phase does not allow."""


def slugify(name: str) -> str:
    """Reduce *name* to the character set session ids are validated against."""
    slug = _SLUG_SEPARATORS.sub("-", name.strip().lower()).strip("-")[:64].rstrip("-")
    return slug or "session"


@dataclass(frozen=True, slots=True)
class ComponentResult:
    status: ComponentStatus
    detail: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {"status": self.status, "detail": self.detail}


@dataclass(slots=True)
class _ActiveSession:
    session_id: str
    name: str
    directory: Path
    started_unix: float
    started_monotonic: float
    writer: SessionTelemetryWriter
    devices: list[dict[str, Any]]
    kasa: list[dict[str, Any]]
    tares_at_start: dict[str, float]
    components: dict[str, dict[str, Any]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


class SessionRuntime:
    """Starts and stops recording across telemetry, audio and every camera at once.

    Only telemetry is mandatory. A camera that fails to arm or a Mumble server that is
    down is recorded as a failed component and the session continues -- a test with two
    of three camera angles is still worth having, and the hardware really does fail
    this way.
    """

    def __init__(
        self,
        *,
        paths: RecordingPaths,
        telemetry_publisher: TelemetrySessionPublisher,
        system_state: SystemState,
        state_stream: StateStream,
        camera_runtime: CameraRuntime,
        audio_runtime: AudioRuntime,
        shutdown_timeout_s: float = SHUTDOWN_FINALIZE_TIMEOUT_S,
    ) -> None:
        self._paths = paths
        self._shutdown_timeout_s = shutdown_timeout_s
        self._publisher = telemetry_publisher
        self._system_state = system_state
        self._state_stream = state_stream
        self._camera_runtime = camera_runtime
        self._audio_runtime = audio_runtime
        self._lock = asyncio.Lock()
        self._phase: SessionPhase = "idle"
        self._session: _ActiveSession | None = None
        self._flush_task: asyncio.Task[None] | None = None

    # -- lifecycle ---------------------------------------------------------

    async def start(self, name: str) -> dict[str, Any]:
        """Begin recording everything into a new session directory."""
        async with self._lock:
            if self._phase != "idle":
                message = f"A recording session is already {self._phase}"
                raise SessionConflictError(message)
            self._phase = "starting"

            # Synchronous prologue: no awaits between opening the telemetry writer and
            # attaching it, so the UDP ingest loop cannot observe a half-built session.
            try:
                session = self._open_session(name)
            except Exception:
                self._phase = "idle"
                raise

            self._session = session
            try:
                self._emit(
                    self._system_state.start_session(
                        session_id=session.session_id,
                        name=session.name,
                        started_unix=session.started_unix,
                        started_monotonic=session.started_monotonic,
                    ),
                )
                self._write_metadata(session, status="active")
                self._flush_task = asyncio.get_running_loop().create_task(self._flush_loop())

                await self._start_components(session)
                self._write_metadata(session, status="active")
                self._emit(self._system_state.update_session_components({key: value["status"] for key, value in session.components.items()}))
            except BaseException:
                # BaseException, not Exception: a client that disconnects mid-start
                # cancels this task, and a session left half-started would refuse both
                # start and stop for the rest of the process's life.
                logger.exception("Recording session %s failed to start; abandoning it", session.session_id)
                self._abandon(session)
                raise
            self._phase = "active"

        logger.info("Started recording session %s", session.session_id)
        return self.status() or {}

    async def stop(self, *, end_reason: str = "operator") -> dict[str, Any]:
        """Stop recording, finish every artifact, and write the final metadata."""
        async with self._lock:
            if self._phase != "active":
                message = "No recording session is active"
                raise SessionConflictError(message)
            self._phase = "stopping"
            session = self._session
            assert session is not None

            # Synchronous epilogue, mirroring start: detach before anything can await.
            self._publisher.detach()
            self._session = None

            try:
                metadata = await self._finish(session, end_reason=end_reason)
            finally:
                # The session is already detached, so idle is the truth however this
                # went; staying at "stopping" would wedge every later session.
                self._phase = "idle"
                self._clear_session_state(end_reason="error")

        logger.info("Stopped recording session %s (%s rows of telemetry)", session.session_id, session.writer.rows)
        return metadata

    def _abandon(self, session: _ActiveSession) -> None:
        """Tear down a session that never finished starting, back to idle.

        Its directory is left on disk with ``status: "active"`` in the metadata rather
        than deleted -- whatever it did capture is still the operator's data.
        """
        self._publisher.detach()
        self._session = None
        if self._flush_task is not None:
            self._flush_task.cancel()
            self._flush_task = None
        with contextlib.suppress(Exception):
            session.writer.close()
        self._clear_session_state(end_reason="error")
        self._phase = "idle"

    def _clear_session_state(self, *, end_reason: str) -> None:
        """Ensure the state projection is not left advertising a session that has ended."""
        if self._system_state.session() is None:
            return
        with contextlib.suppress(Exception):
            self._emit(self._system_state.stop_session(stopped_unix=time.time(), end_reason=end_reason))

    async def finalize_on_shutdown(self) -> None:
        """Close out an in-progress session during server shutdown, best effort."""
        # A shutdown landing mid-transition would otherwise return immediately and let
        # the process exit with the CSV still open. start() and stop() hold the lock for
        # their whole duration, so taking it once waits out whichever is in flight. Half
        # the budget goes to that wait and half to the stop, bounding the total delay.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._lock.acquire(), timeout=self._shutdown_timeout_s / 2)
            self._lock.release()

        if self._phase != "active":
            if self._phase != "idle":
                session_id = self._session.session_id if self._session else "?"
                logger.error("Server is shutting down with session %s stuck in phase %s", session_id, self._phase)
            return
        logger.warning("Server is shutting down with session %s recording; finalizing", self._session.session_id if self._session else "?")
        # A timeout here still leaves the telemetry safe: stop() closes the CSV before it
        # touches the media server or Mumble, so only their cleanup is abandoned.
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self.stop(end_reason="server_shutdown"), timeout=self._shutdown_timeout_s / 2)

    # -- reads -------------------------------------------------------------

    @property
    def recordings_root(self) -> Path:
        return self._paths.root

    def status(self) -> dict[str, Any] | None:
        """Return the active session's metadata, or None when nothing is recording."""
        session = self._session
        if session is None:
            return None
        return self._metadata(session, status="active", end_reason=None)

    def is_active(self, session_id: str) -> bool:
        return self._session is not None and self._session.session_id == session_id

    def get_session_dir(self, session_id: str) -> Path:
        """Resolve a session id to its directory, rejecting anything that escapes the root.

        Two independent checks -- a pattern allowlist and a resolved-parent comparison --
        so neither one is load-bearing on its own.
        """
        if Path(session_id).name != session_id or not SESSION_ID_PATTERN.fullmatch(session_id):
            raise ValueError("Invalid session id")

        directory = (self._paths.root / session_id).resolve()
        if directory.parent != self._paths.root:
            raise ValueError("Invalid session path")
        if not directory.is_dir():
            raise FileNotFoundError(session_id)
        return directory

    def resolve_session_file(self, session_id: str, relative_path: str) -> Path:
        """Resolve one artifact inside a session, rejecting anything outside it.

        `resolve` dereferences symlinks before the containment check, so a link planted
        in a session directory that points elsewhere fails it rather than being served.
        """
        session_dir = self.get_session_dir(session_id)
        candidate = (session_dir / relative_path).resolve()
        if not candidate.is_relative_to(session_dir):
            raise ValueError("Invalid file path")
        if not candidate.is_file():
            raise FileNotFoundError(relative_path)
        return candidate

    def read_metadata(self, session_id: str) -> dict[str, Any]:
        """Read a session's metadata from disk, preferring live state for the active one."""
        if self.is_active(session_id):
            live = self.status()
            if live is not None:
                return live

        session_dir = self.get_session_dir(session_id)
        return self._read_metadata_file(session_dir)

    def list_sessions(self) -> list[dict[str, Any]]:
        """Summarize every session on disk, newest first."""
        if not self._paths.root.is_dir():
            return []

        summaries: list[dict[str, Any]] = []
        for directory in self._paths.root.iterdir():
            # is_dir() follows symlinks, so without this a link into the wider
            # filesystem would be listed as a session that get_session_dir then
            # refuses to open.
            if directory.is_symlink() or not directory.is_dir() or not SESSION_ID_PATTERN.fullmatch(directory.name):
                continue
            summaries.append(self._summarize(directory))

        summaries.sort(key=lambda summary: summary["started_unix"] or 0.0, reverse=True)
        return summaries

    # -- internals ---------------------------------------------------------

    def _open_session(self, name: str) -> _ActiveSession:
        started_unix = time.time()
        started_monotonic = time.monotonic()
        # Local time, deliberately: the id is what an operator reads off the directory
        # listing. session.json carries the unambiguous unix timestamps.
        session_id = f"{time.strftime('%Y-%m-%d_%H%M%S', time.localtime(started_unix))}_{slugify(name)}"
        directory = self._paths.session_dir(session_id)
        directory.mkdir(parents=True, exist_ok=False)

        try:
            schema = self._system_state.recording_schema()
            writer = SessionTelemetryWriter.open(directory / TELEMETRY_FILENAME, self._system_state, build_columns(schema))
        except Exception:
            # Telemetry is the point of a session; without it there is nothing to record.
            shutil.rmtree(directory, ignore_errors=True)
            raise

        self._publisher.attach(writer)
        snapshot = self._system_state.snapshot()
        return _ActiveSession(
            session_id=session_id,
            name=name,
            directory=directory,
            started_unix=started_unix,
            started_monotonic=started_monotonic,
            writer=writer,
            devices=snapshot["devices"],
            kasa=snapshot["kasa"],
            tares_at_start=snapshot["tares"],
            components={"telemetry": ComponentResult("ok").as_dict()},
        )

    async def _start_components(self, session: _ActiveSession) -> None:
        cameras = self._camera_runtime.cameras()
        if not cameras:
            session.components["cameras"] = ComponentResult("skipped", "no cameras registered").as_dict()
            camera_task: asyncio.Future[dict[str, str | None]] = asyncio.get_running_loop().create_future()
            camera_task.set_result({})
        else:
            video_dir = self._paths.to_container(session.directory / VIDEO_SUBDIR)
            camera_task = asyncio.ensure_future(self._camera_runtime.start_session_recording(video_dir))

        audio_task = asyncio.ensure_future(asyncio.to_thread(self._audio_runtime.start, session.directory / AUDIO_SUBDIR))
        camera_results, audio_result = await asyncio.gather(camera_task, audio_task, return_exceptions=True)

        if isinstance(camera_results, BaseException):
            logger.error("Failed to start camera recording: %s", camera_results)
            session.components["cameras"] = ComponentResult("failed", str(camera_results)).as_dict()
        else:
            for ip, detail in camera_results.items():
                session.components[f"camera:{ip}"] = ComponentResult("ok" if detail is None else "failed", detail).as_dict()

        if isinstance(audio_result, BaseException):
            logger.error("Failed to start audio recording: %s", audio_result)
            session.components["audio"] = ComponentResult("failed", str(audio_result)).as_dict()
        else:
            session.components["audio"] = ComponentResult("ok").as_dict()

    async def _finish(self, session: _ActiveSession, *, end_reason: str) -> dict[str, Any]:
        if self._flush_task is not None:
            self._flush_task.cancel()
            await asyncio.gather(self._flush_task, return_exceptions=True)
            self._flush_task = None

        try:
            session.writer.close()
            session.components["telemetry"] = ComponentResult("ok").as_dict()
        except Exception as exc:
            logger.exception("Failed to close the telemetry recording")
            session.components["telemetry"] = ComponentResult("failed", str(exc)).as_dict()

        camera_results, audio_result = await asyncio.gather(
            self._camera_runtime.stop_session_recording(),
            asyncio.to_thread(self._audio_runtime.stop),
            return_exceptions=True,
        )

        if isinstance(camera_results, BaseException):
            logger.error("Failed to stop camera recording: %s", camera_results)
        else:
            for ip, detail in camera_results.items():
                if detail is not None:
                    session.components[f"camera:{ip}"] = ComponentResult("failed", detail).as_dict()

        # An audio component that never started reports "not recording" here, which its
        # start status already captures; only record a genuinely new failure.
        if isinstance(audio_result, BaseException) and session.components.get("audio", {}).get("status") == "ok":
            session.components["audio"] = ComponentResult("failed", str(audio_result)).as_dict()

        video_dir = session.directory / VIDEO_SUBDIR
        if not await self._camera_runtime.settle_video(video_dir):
            session.warnings.append("video_settle_timeout")

        started_cameras = any(key.startswith("camera:") and value["status"] == "ok" for key, value in session.components.items())
        if started_cameras and not _files_in(video_dir):
            # Either the session was shorter than a segment, or the container path
            # mapping is wrong and MediaMTX wrote somewhere nobody can see.
            session.warnings.append("no_video_recorded")

        stopped_unix = time.time()
        metadata = self._metadata(session, status="completed", end_reason=end_reason, stopped_unix=stopped_unix, stopped_monotonic=time.monotonic())
        self._write_json(session.directory / SESSION_METADATA_FILENAME, metadata)
        self._emit(self._system_state.stop_session(stopped_unix=stopped_unix, end_reason=end_reason))
        return metadata

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(FLUSH_INTERVAL_S)
            session = self._session
            if session is None:
                continue
            try:
                session.writer.flush_if_dirty()
            except Exception:
                logger.exception("Failed to flush telemetry recording")

    def _metadata(
        self,
        session: _ActiveSession,
        *,
        status: str,
        end_reason: str | None,
        stopped_unix: float | None = None,
        stopped_monotonic: float | None = None,
    ) -> dict[str, Any]:
        writer = session.writer
        return {
            "schema_version": 1,
            "id": session.session_id,
            "name": session.name,
            "status": status,
            "end_reason": end_reason,
            "server_version": vector.__version__,
            "clock": {
                "started_unix": session.started_unix,
                "started_monotonic": session.started_monotonic,
                "stopped_unix": stopped_unix,
                "stopped_monotonic": stopped_monotonic,
                # Devices are synced to the server's monotonic clock, so this one
                # relation converts either timestamp source to wall clock and lines
                # telemetry up against the wall-clock-named video files.
                "timesync_timebase": "server_monotonic",
                "note": "wall_clock = started_unix + (device_timestamp - started_monotonic)",
            },
            "components": session.components,
            "devices": session.devices,
            "kasa": session.kasa,
            "tares": {"at_start": session.tares_at_start, "at_stop": self._system_state.core.tares()},
            "cameras": [
                {
                    "ip": camera.address,
                    "hostname": camera.hostname,
                    "files": sorted(path.name for path in _files_in(session.directory / VIDEO_SUBDIR)),
                }
                for camera in self._camera_runtime.cameras()
            ],
            "audio_files": sorted(path.name for path in _files_in(session.directory / AUDIO_SUBDIR)),
            "telemetry": {
                "file": TELEMETRY_FILENAME,
                "rows": writer.rows,
                "columns": list(writer.plan.column_names),
                "late_files": list(writer.late_files),
                "semantics": {
                    "device_timestamp": "batch timestamp in seconds on the server monotonic timebase",
                    "source": "display label of the source the row came from; labels may repeat or change",
                    "source_provider": "provider namespace; combine with source_key to identify the source",
                    "source_key": "stable key within the provider; retained across reconnects and label changes",
                    "sensor_columns": "'<NAME> [<unit>]', tared value to 4 decimals; an empty cell means the sensor was absent from that batch",
                    "control_columns": "'<group>_<NAME>' using the group declared in the device's QLCP config",
                    "valve_controls": "1 when the reported state is OPEN, else 0",
                    "other_boolean_controls": "1 when the reported state is CLOSED, else 0 -- inverted vs valves (normally-closed wiring)",
                    "analog_controls": "the reported setpoint to 4 decimals; an empty cell means it has not been reported",
                    "kasa_columns": "1 when the outlet is powered; the key is the alias (or host) with non-alphanumerics replaced by '_'",
                    "column_order": (
                        "device_timestamp, source, then sensors / controls / kasa, each block sorted alphabetically by raw name; "
                        "source_provider and source_key are last"
                    ),
                },
            },
            "paths": {"root": str(self._paths.root), "mediamtx_container_root": str(self._paths.container_root)},
            "warnings": session.warnings,
        }

    def _write_metadata(self, session: _ActiveSession, *, status: str) -> None:
        self._write_json(session.directory / SESSION_METADATA_FILENAME, self._metadata(session, status=status, end_reason=None))

    @staticmethod
    def _write_json(path: Path, payload: dict[str, Any]) -> None:
        path.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))

    @staticmethod
    def _read_metadata_file(session_dir: Path) -> dict[str, Any]:
        metadata_path = session_dir / SESSION_METADATA_FILENAME
        if not metadata_path.is_file():
            raise FileNotFoundError(str(metadata_path))
        return orjson.loads(metadata_path.read_bytes())

    def _summarize(self, directory: Path) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "id": directory.name,
            "name": directory.name,
            "status": "unknown",
            "started_unix": None,
            "size_bytes": sum(path.stat().st_size for path in directory.rglob("*") if path.is_file() and not path.is_symlink()),
            "download_path": f"/v1/sessions/{directory.name}/download",
        }
        try:
            metadata = self._read_metadata_file(directory)
        except Exception:
            # A crashed or half-written session should still be listed and downloadable,
            # rather than taking the whole listing down with it.
            logger.warning("Session %s has no readable metadata", directory.name)
            return summary

        summary["name"] = metadata.get("name", directory.name)
        summary["status"] = metadata.get("status", "unknown")
        summary["started_unix"] = metadata.get("clock", {}).get("started_unix")
        return summary

    def _emit(self, event: dict[str, object] | None) -> None:
        self._state_stream.publish(event)


def _files_in(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    return [path for path in directory.iterdir() if path.is_file()]
