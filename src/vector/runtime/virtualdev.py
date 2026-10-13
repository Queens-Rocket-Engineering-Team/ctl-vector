import asyncio
import logging
import random
import time

from vector.qlcp.config_models import DeviceConfig, SensorConfig
from vector.runtime.state_stream import StateStream
from vector.runtime.telemetry_ingest import TelemetryRuntime
from vector.state.system_state import SystemState

logger = logging.getLogger(__name__)

DEVICE_NAME = "VirtualDevice"
ADDRESS = "virtual"
CONNECTION_KEY = "virtual:rssi"

POLL_PERIOD_S = 0.2
POLL_TIMEOUT_S = 0.5
STALE_AFTER_FAILURES = 3


class _FlakyRssiSource:
    """Fake link: up for `up_s`, down for `down_s`, repeating.

    Even-numbered outages raise immediately; odd-numbered ones hang so the poll timeout fires.
    """

    def __init__(self, up_s: float = 10.0, down_s: float = 5.0) -> None:
        self._up_s = up_s
        self._down_s = down_s
        self._t0 = time.monotonic()

    def _current_outage(self) -> int | None:
        elapsed = time.monotonic() - self._t0
        period = self._up_s + self._down_s
        if elapsed % period < self._up_s:
            return None
        return int(elapsed // period)

    async def poll(self) -> dict[int, float]:
        outage = self._current_outage()
        if outage is None:
            return {0: random.uniform(-100, -30)}
        if outage % 2 == 0:
            raise ConnectionError(f"simulated link down (outage {outage})")
        await asyncio.sleep(3600)  # simulated hang; wait_for cancels this
        return {}


class VirtualDeviceService:
    """Test service: a flaky fake RSSI source for exercising virtual-device liveness."""

    def __init__(
        self,
        system_state: SystemState,
        state_stream: StateStream,
        telemetry: TelemetryRuntime,
        *,
        crash_after_s: float | None = None,
    ) -> None:
        self._system_state = system_state
        self._state_stream = state_stream
        self._telemetry = telemetry
        self._crash_after_s = crash_after_s
        self._source = _FlakyRssiSource()
        self._task: asyncio.Task[None] | None = None
        self._config = DeviceConfig(
            name=DEVICE_NAME,
            sensors_by_id={0: SensorConfig(id=0, name="RSSI", unit="dBm", group="signal")},
            controls_by_id={},
        )

    async def start(self):
        """Register the device (disconnected until the first good poll) and start polling."""
        self._emit(
            self._system_state.register_virtual_device(
                self._config, address=ADDRESS, connection_key=CONNECTION_KEY, connected=False
            )
        )
        self._task = asyncio.create_task(self.run(), name="virtual-device")
        self._task.add_done_callback(self._on_done)

    async def run(self) -> None:
        failures = 0
        started = time.monotonic()

        while True:
            if self._crash_after_s is not None and time.monotonic() - started > self._crash_after_s:
                raise RuntimeError("simulated poller crash")

            try:
                values = await asyncio.wait_for(self._source.poll(), timeout=POLL_TIMEOUT_S)
            except Exception as exc:  # TimeoutError included; CancelledError is not an Exception
                failures += 1
                if failures == 1:
                    logger.warning("Virtual device poll failed: %r", exc)
                if failures == STALE_AFTER_FAILURES:
                    logger.warning("Virtual device marked disconnected after %d failed polls", failures)
                    self._set_connected(False)
            else:
                if failures:
                    logger.info("Virtual device recovered after %d failed polls", failures)
                failures = 0
                self._set_connected(True)  # no-op (None) when already connected
                self._telemetry.ingest_virtual(
                    config=self._config,
                    values=values,
                    address=ADDRESS,
                    connection_key=CONNECTION_KEY,
                )

            await asyncio.sleep(POLL_PERIOD_S)

    def _on_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return  # normal shutdown
        exc = task.exception()
        logger.error("Virtual device poller exited unexpectedly", exc_info=exc)
        self._set_connected(False)

    def _set_connected(self, connected: bool) -> None:
        self._emit(self._system_state.set_virtual_connected(DEVICE_NAME, connected))

    def _emit(self, event: dict[str, object] | None) -> None:
        if event is not None:
            self._state_stream.publish(event)