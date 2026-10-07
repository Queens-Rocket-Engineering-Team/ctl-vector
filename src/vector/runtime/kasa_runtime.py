"""Kasa discovery and device I/O behind the shared control interface."""

from __future__ import annotations
import asyncio
import logging
import time
from dataclasses import dataclass
from functools import partial

from kasa import Device, Discover, KasaException

from vector.core import ControlBinding, ControlDefinition, ControlObservation, ControlStatus, ControlType, ControlValue, Core, Source
from vector.state.system_state import TransportHealth


logger = logging.getLogger(__name__)

# Plugs do not report on their own, so the server polls them at a fixed interval
# and treats repeated silence as a disconnect, as the QLCP heartbeat loop does.
POLL_INTERVAL_S = 5.0
POLL_MISS_LIMIT = 3


@dataclass(slots=True)
class _KasaEntry:
    device: Device
    # The plug's one control; its source is this discovery's registration.
    power: ControlBinding
    misses: int = 0


class KasaRuntime:
    def __init__(self, *, core: Core) -> None:
        self.core = core
        self._registry: dict[str, _KasaEntry] = {}

    def health(self, source: Source) -> TransportHealth | None:
        """Liveness for one of this runtime's plugs: how many polls in a row it has missed."""
        entry = self._registry.get(source.key)
        if source.provider != "kasa" or entry is None or entry.power.source is not source:
            return None
        return TransportHealth(last_sync_time=None, consecutive_misses=entry.misses)

    async def run(self) -> None:
        """Poll each known plug so one that stops answering is closed; discovery registers them."""
        while True:
            await asyncio.sleep(POLL_INTERVAL_S)
            await self.poll_once()

    async def poll_once(self) -> None:
        """Refresh every plug; close one that has failed ``POLL_MISS_LIMIT`` polls in a row."""
        await asyncio.gather(*(self._poll(entry) for entry in list(self._registry.values())))

    async def _poll(self, entry: _KasaEntry) -> None:
        host = entry.device.host
        try:
            await entry.device.update()
        except (KasaException, OSError) as exc:
            entry.misses += 1
            logger.warning("Kasa device at %s did not answer poll (%d/%d): %s", host, entry.misses, POLL_MISS_LIMIT, exc)
        else:
            entry.misses = 0
            self._report(entry)
            return
        # Only the current registration may be closed; a rediscovery may have replaced this entry.
        if entry.misses >= POLL_MISS_LIMIT and self._registry.get(host) is entry:
            logger.error("Kasa device at %s unresponsive; closing its source", host)
            self._remove_device(host)

    @staticmethod
    def _report(entry: _KasaEntry) -> None:
        """Record the plug's observed power if it differs from the last report."""
        power = entry.power
        if power.source.connected and (power.reported is None or power.reported.value != entry.device.is_on):
            power.source.report_control(power, entry.device.is_on)

    async def discover(self) -> None:
        """Register plugs not already known; the poll loop keeps known ones current."""
        try:
            logger.info("Sending kasa discovery request...")
            devices = await Discover.discover()
            new = [dev for host, dev in devices.items() if host not in self._registry]
            await asyncio.gather(*(self._register_discovered_device(dev) for dev in new))
        except Exception:
            logger.exception("Failed to discover Kasa devices")

    async def _write_power(self, dev: Device, target: ControlBinding, value: ControlValue) -> None:
        """Write power, then report the observed value after a successful refresh."""
        source = target.source
        active = bool(value)
        try:
            if active:
                await dev.turn_on()
            else:
                await dev.turn_off()
            await dev.update()
            source.report_control(target, dev.is_on)
            logger.info("Set Kasa device at %s: active=%s", dev.host, active)
        except KasaException:
            logger.exception("Kasa error controlling device at %s", dev.host)
            # A delayed failure from an old discovery must not remove its replacement.
            if source.connected:
                self._remove_device(dev.host)
            raise
        except Exception:
            logger.exception("Failed to control Kasa device at %s", dev.host)
            raise

    async def _register_discovered_device(self, dev: Device) -> None:
        await dev.update()
        logger.info("Discovered Kasa device: %s (%s)", dev.alias or "<No Alias>", dev.host)

        source = self.core.register_source(
            "kasa",
            dev.host,
            name=dev.alias or dev.host,
            address=dev.host,
            metadata={"alias": dev.alias or "", "model": dev.model},
            controls=[ControlDefinition(name="power", group="power", type=ControlType.BOOL)],
            control_handler=partial(self._write_power, dev),
            initial_controls={"power": ControlObservation(value=dev.is_on, timestamp=time.monotonic(), status=ControlStatus.CONFIRMED)},
        )
        self._registry[dev.host] = _KasaEntry(dev, source.controls[0])

    def _remove_device(self, host: str) -> None:
        entry = self._registry.pop(host, None)
        if entry is not None:
            entry.power.source.close()
