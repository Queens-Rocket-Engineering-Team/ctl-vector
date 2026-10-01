"""Kasa discovery and device I/O behind the shared control interface."""

from __future__ import annotations
import asyncio
import logging
import time
from functools import partial

from kasa import Device, Discover, KasaException

from vector.core import ControlBinding, ControlDefinition, ControlObservation, ControlStatus, ControlType, ControlValue, Core, DispatchResult, Source


logger = logging.getLogger(__name__)


class KasaRuntime:
    def __init__(self, *, core: Core) -> None:
        self.core = core
        self._registry: dict[str, tuple[Device, Source]] = {}

    def get_device(self, host: str) -> Device | None:
        entry = self._registry.get(host)
        return entry[0] if entry is not None else None

    async def get_devices(self) -> list[Device]:
        entries = list(self._registry.values())
        await asyncio.gather(*(dev.update() for dev, _ in entries))
        for dev, source in entries:
            if source.connected:
                control = source.control("power")
                if control is not None and (control.reported is None or control.reported.value != dev.is_on):
                    source.report_control("power", dev.is_on)
        return [dev for dev, _ in entries]

    async def discover(self) -> None:
        try:
            logger.info("Sending kasa discovery request...")
            devices = await Discover.discover()
            await asyncio.gather(*(self._register_discovered_device(dev) for dev in devices.values()))
        except Exception:
            logger.exception("Failed to discover Kasa devices")

    async def set_state(self, host: str, active: bool) -> Device:
        """Compatibility entry point used by the existing Kasa HTTP endpoint."""
        dev, source = self._require_device(host)
        target = source.control("power")
        assert target is not None
        result = await self.core.set_control(target, active)
        if not result.submitted:
            if result.cause is not None:
                raise result.cause
            raise RuntimeError(result.error or "Kasa control failed")
        return dev

    async def _write_power(self, dev: Device, target: ControlBinding, value: ControlValue) -> DispatchResult:
        """Write power, then report the observed value after a successful refresh."""
        source = target.source
        active = bool(value)
        try:
            if active:
                await dev.turn_on()
            else:
                await dev.turn_off()
            await dev.update()
            source.report_control("power", dev.is_on)
            logger.info("Set Kasa device at %s: active=%s", dev.host, active)
            return DispatchResult(submitted=True)
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
        self._registry[dev.host] = (dev, source)

    def _require_device(self, host: str) -> tuple[Device, Source]:
        entry = self._registry.get(host)
        if entry is None:
            raise KeyError("No Kasa device found")
        return entry

    def _remove_device(self, host: str) -> None:
        entry = self._registry.pop(host, None)
        if entry is not None:
            entry[1].close()
