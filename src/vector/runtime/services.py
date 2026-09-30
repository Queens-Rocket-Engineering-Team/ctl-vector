"""Composition root and lifecycle container for the server's runtime graph."""

from __future__ import annotations
import asyncio
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from vector.integrations.mediamtx import MediaMTXClient
from vector.runtime.audio_runtime import AudioRuntime
from vector.runtime.camera_runtime import CameraRuntime
from vector.runtime.command_tracker import CommandTracker
from vector.runtime.discovery import DiscoveryService
from vector.runtime.esp_connection_runtime import ESPConnectionRuntime
from vector.runtime.gui_watchdog import GUIWatchdog
from vector.runtime.kasa_runtime import KasaRuntime
from vector.runtime.log_stream import LogStream
from vector.runtime.metrics import Metrics
from vector.runtime.recording_paths import RecordingPaths
from vector.runtime.session_runtime import SessionRuntime
from vector.runtime.session_telemetry import TelemetrySessionPublisher
from vector.runtime.state_stream import StateStream
from vector.runtime.telemetry_display_stream import TelemetryDisplayStream
from vector.runtime.telemetry_ingest import TelemetryRuntime
from vector.runtime.telemetry_stream import TelemetryStreamRuntime
from vector.state import SystemState


if TYPE_CHECKING:
    from vector.config import ServerConfig


logger = logging.getLogger(__name__)


@dataclass
class RuntimeServices:
    """Wired runtime object graph for the server process.

    Owns runtime daemon lifecycle and startup actions.
    """

    command_tracker: CommandTracker
    metrics: Metrics
    system_state: SystemState
    state_stream: StateStream
    log_stream: LogStream
    discovery_service: DiscoveryService
    telemetry_stream: TelemetryStreamRuntime
    telemetry_display_stream: TelemetryDisplayStream
    esp_runtime: ESPConnectionRuntime
    telemetry_runtime: TelemetryRuntime
    audio_runtime: AudioRuntime
    camera_runtime: CameraRuntime
    kasa_runtime: KasaRuntime
    session_runtime: SessionRuntime
    gui_watchdog: GUIWatchdog
    _tasks: dict[str, asyncio.Task[None]] = field(default_factory=dict, init=False, repr=False)

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        """Launch runtime daemon tasks onto *loop*.

        Must be called after the event loop is running (i.e. from inside an
        async context or after ``asyncio.get_event_loop()`` is valid).
        """

        # QLCP/ESP runtime daemons
        logger.info("Starting QLCP daemon tasks...")
        self._tasks["tcp_listener"] = loop.create_task(self.esp_runtime.run_tcp_listener())
        self._tasks["udp_listener"] = loop.create_task(self.telemetry_runtime.run_udp_listener())
        self._tasks["telemetry_display_flush"] = loop.create_task(self.telemetry_display_stream.run())
        self._tasks["auto_discovery"] = loop.create_task(self.discovery_service.run())

        # Safety daemon: ESTOPs every node if the GUI stays away (armed from construction).
        logger.info("Starting GUI watchdog daemon...")
        self._tasks["gui_watchdog"] = loop.create_task(self.gui_watchdog.run())

        # Camera discovery daemons
        logger.info("Starting camera discovery daemon...")
        self._tasks["camera_connector"] = loop.create_task(self.camera_runtime.connect_all_cameras())

        # Kasa discovery daemon
        logger.info("Starting Kasa discovery daemon...")
        self._tasks["kasa_discoverer"] = loop.create_task(self.kasa_runtime.discover())

        # Log stream daemon
        logger.info("Starting log stream daemon...")
        self._tasks["log_stream"] = loop.create_task(self.log_stream.run())


    async def stop(self) -> None:
        """Cancel and await all runtime daemon tasks, then close device connections."""
        # Before anything else, so a session in progress is finished properly rather
        # than losing its telemetry buffer and its unfinalized video segments.
        await self.session_runtime.finalize_on_shutdown()

        log_task = self._tasks.get("log_stream")
        runtime_tasks = {name: task for name, task in self._tasks.items() if name != "log_stream"}

        for name, task in runtime_tasks.items():
            if not task.done():
                task.cancel()
                logger.info("Cancelled daemon task: %s", name)
        await asyncio.gather(*runtime_tasks.values(), return_exceptions=True)

        # Close all non-daemon runtime resources (device connections, etc.)
        self.esp_runtime.close_all()
        self.audio_runtime.close()
        await self.camera_runtime.close()

        await asyncio.sleep(0)
        if log_task is not None and not log_task.done():
            log_task.cancel()
        if log_task is not None:
            await asyncio.gather(log_task, return_exceptions=True)


def build_runtime(config: ServerConfig) -> RuntimeServices:
    """Top-level composition root for the server's runtime object graph. Build once at startup and pass around the resulting object."""
    metrics = Metrics()
    command_tracker = CommandTracker(metrics=metrics)
    system_state = SystemState(command_tracker=command_tracker)
    state_stream = StateStream(system_state, metrics=metrics)
    log_stream = LogStream(metrics=metrics)
    discovery_service = DiscoveryService()
    telemetry_stream = TelemetryStreamRuntime(metrics=metrics)
    telemetry_display_stream = TelemetryDisplayStream(metrics=metrics)
    esp_runtime = ESPConnectionRuntime(
        command_tracker=command_tracker,
        system_state=system_state,
        state_stream=state_stream,
        metrics=metrics,
    )
    # Permanently registered rather than swapped in when recording starts, so the ingest
    # loop's publisher tuple is never mutated; it is a no-op until a session attaches.
    telemetry_session = TelemetrySessionPublisher()
    telemetry_runtime = TelemetryRuntime(
        esp_runtime.get_device_by_address,
        telemetry_stream,
        telemetry_display_stream,
        telemetry_session,
        tare_for=system_state.tare_for,
        metrics=metrics,
    )
    recording_paths = RecordingPaths.from_config(config["services"]["recordings"])
    recording_paths.ensure_root()
    audio_runtime = AudioRuntime(config["services"]["mumble"])
    mediamtx = MediaMTXClient(config["services"]["mediamtx"])
    camera_runtime = CameraRuntime(
        mediamtx=mediamtx,
        cameras=config["cameras"],
        camera_account=config["accounts"]["camera"],
        recording_paths=recording_paths,
        discovery_enabled=config["services"]["discovery"]["enabled"],
        onvif_port=config["services"]["discovery"]["onvif_port"],
        discovery_timeout=config["services"]["discovery"]["timeout"],
        discovery_max_retries=config["services"]["discovery"]["max_retries"],
    )
    gui_watchdog = GUIWatchdog(state_stream=state_stream, esp_runtime=esp_runtime, metrics=metrics)
    kasa_runtime = KasaRuntime(system_state=system_state, state_stream=state_stream)
    session_runtime = SessionRuntime(
        paths=recording_paths,
        telemetry_publisher=telemetry_session,
        system_state=system_state,
        state_stream=state_stream,
        camera_runtime=camera_runtime,
        audio_runtime=audio_runtime,
    )
    return RuntimeServices(
        command_tracker=command_tracker,
        metrics=metrics,
        system_state=system_state,
        state_stream=state_stream,
        log_stream=log_stream,
        discovery_service=discovery_service,
        telemetry_stream=telemetry_stream,
        telemetry_display_stream=telemetry_display_stream,
        esp_runtime=esp_runtime,
        telemetry_runtime=telemetry_runtime,
        audio_runtime=audio_runtime,
        camera_runtime=camera_runtime,
        kasa_runtime=kasa_runtime,
        session_runtime=session_runtime,
        gui_watchdog=gui_watchdog,
    )
