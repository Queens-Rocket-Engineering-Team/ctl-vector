"""Kasa discovery and device I/O behind the shared control interface."""

from __future__ import annotations
import asyncio
import logging
import time
from dataclasses import dataclass
from functools import partial

from kasa import Device, Discover, KasaException

from vector.core import ControlBinding, ControlDefinition, ControlObservation, ControlStatus, ControlType, ControlValue, Core


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

    def get_device(self, host: str) -> Device | None:
        entry = self._registry.get(host)
        return entry.device if entry is not None else None

    async def get_devices(self) -> list[Device]:
        entries = list(self._registry.values())
        await asyncio.gather(*(entry.device.update() for entry in entries))
        for entry in entries:
            self._report(entry)
        return [entry.device for entry in entries]

    async def run(self) -> None:
        """Discover plugs, then poll each one so a plug that stops answering is closed."""
        await self.discover()
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
        try:
            logger.info("Sending kasa discovery request...")
            devices = await Discover.discover()
            await asyncio.gather(*(self._register_discovered_device(dev) for dev in devices.values()))
        except Exception:
            logger.exception("Failed to discover Kasa devices")

    async def set_state(self, host: str, active: bool) -> Device:
        """Compatibility entry point used by the existing Kasa HTTP endpoint."""
        entry = self._require_device(host)
        await self.core.set_control(entry.power, active)
        return entry.device

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

    def _require_device(self, host: str) -> _KasaEntry:
        entry = self._registry.get(host)
        if entry is None:
            raise KeyError("No Kasa device found")
        return entry

    def _remove_device(self, host: str) -> None:
        entry = self._registry.pop(host, None)
        if entry is not None:
            entry.power.source.close()
