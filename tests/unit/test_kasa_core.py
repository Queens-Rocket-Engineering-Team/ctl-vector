"""Kasa uses ordinary core dispatch and only publishes successful readback."""

from __future__ import annotations
import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest
from kasa import KasaException

from vector.api.routers.kasa import control_kasa_device, get_kasa_devices
from vector.core import Core
from vector.runtime.kasa_runtime import KasaRuntime
from vector.runtime.session_telemetry import build_columns
from vector.state.system_state import SystemState


if TYPE_CHECKING:
    from vector.state.system_state import StateEvent


class _Device:
    def __init__(self, host: str = "192.168.1.5", *, active: bool = False, alias: str = "Pump") -> None:
        self.host = host
        self.alias = alias
        self.model = "HS110"
        self.is_on = active
        self.hardware_state = active
        self.observed_override: bool | None = None
        self.refresh_started: asyncio.Event | None = None
        self.refresh_release: asyncio.Event | None = None
        self.readback_error: Exception | None = None
        self.writes: list[bool] = []

    async def turn_on(self) -> None:
        self.writes.append(True)
        self.hardware_state = True

    async def turn_off(self) -> None:
        self.writes.append(False)
        self.hardware_state = False

    async def update(self) -> None:
        if self.refresh_started is not None:
            self.refresh_started.set()
        if self.refresh_release is not None:
            await self.refresh_release.wait()
        if self.readback_error is not None:
            raise self.readback_error
        self.is_on = self.hardware_state if self.observed_override is None else self.observed_override


async def _discover(runtime: KasaRuntime, monkeypatch: pytest.MonkeyPatch, *devices: _Device) -> None:
    monkeypatch.setattr("vector.runtime.kasa_runtime.Discover.discover", AsyncMock(return_value={device.host: device for device in devices}))
    await runtime.discover()


def test_wrapper_dispatches_core_and_waits_for_observation(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        core = Core()
        state = SystemState(core=core)
        events: list[StateEvent] = []
        state.set_publisher(events.append)
        runtime = KasaRuntime(core=core)
        device = _Device()
        await _discover(runtime, monkeypatch, device)
        source = core.source("kasa", device.host)
        assert source is not None
        power = source.control("power")
        assert power is not None
        assert power.default is None
        assert power.accepted is None
        dispatcher = AsyncMock(wraps=core.set_control)
        monkeypatch.setattr(core, "set_control", dispatcher)
        device.refresh_started = asyncio.Event()
        device.refresh_release = asyncio.Event()

        command = asyncio.create_task(runtime.set_state(device.host, True))
        await asyncio.wait_for(device.refresh_started.wait(), timeout=1.0)
        assert device.writes == [True]
        assert power.reported is not None
        assert power.reported.value is False
        assert state.kasa_active() == {device.host: False}
        device.refresh_release.set()
        assert await command is device

        dispatcher.assert_awaited_once_with([power], True)
        assert power.reported is not None
        assert power.reported.value is True
        assert power.accepted is None
        assert [event["type"] for event in events] == ["kasa.registered", "kasa.updated"]
        assert state.snapshot()["devices"] == []
        assert state.recording_schema().controls == ()
        assert build_columns(state.recording_schema()).header == "device_timestamp,source,kasa_Pump,source_provider,source_key\n"

    asyncio.run(run())


def test_reports_readback_even_when_it_differs_from_requested_value(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        core = Core()
        runtime = KasaRuntime(core=core)
        device = _Device()
        await _discover(runtime, monkeypatch, device)
        device.observed_override = False

        returned = await runtime.set_state(device.host, True)

        assert returned is device
        assert device.writes == [True]
        power, = core.controls(provider="kasa")
        assert power.reported is not None
        assert power.reported.value is False

    asyncio.run(run())


def test_failed_readback_disconnects_only_that_source(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        core = Core()
        state = SystemState(core=core)
        runtime = KasaRuntime(core=core)
        broken = _Device("192.168.1.5")
        healthy = _Device("192.168.1.6", active=True)
        await _discover(runtime, monkeypatch, broken, healthy)
        broken.readback_error = KasaException("refresh failed")

        with pytest.raises(KasaException, match="refresh failed"):
            await runtime.set_state(broken.host, True)

        broken_source = core.source("kasa", broken.host)
        healthy_source = core.source("kasa", healthy.host)
        assert broken_source is not None
        assert not broken_source.connected
        assert healthy_source is not None
        assert healthy_source.connected
        assert runtime.get_device(broken.host) is None
        assert runtime.get_device(healthy.host) is healthy
        assert state.kasa_active() == {broken.host: False, healthy.host: True}

    asyncio.run(run())


@pytest.mark.parametrize("fail_old_readback", [False, True])
def test_delayed_old_readback_cannot_change_rediscovered_source(monkeypatch: pytest.MonkeyPatch, *, fail_old_readback: bool) -> None:
    async def run() -> None:
        core = Core()
        state = SystemState(core=core)
        runtime = KasaRuntime(core=core)
        old = _Device()
        await _discover(runtime, monkeypatch, old)
        old_source = core.source("kasa", old.host)
        old.refresh_started = asyncio.Event()
        old.refresh_release = asyncio.Event()
        pending = asyncio.create_task(runtime.set_state(old.host, True))
        await asyncio.wait_for(old.refresh_started.wait(), timeout=1.0)

        replacement = _Device(old.host, active=False, alias="Replacement")
        await _discover(runtime, monkeypatch, replacement)
        new_source = core.source("kasa", replacement.host)
        assert new_source is not None
        assert new_source is not old_source
        if fail_old_readback:
            old.readback_error = KasaException("old refresh failed")
        old.refresh_release.set()
        with pytest.raises(KasaException if fail_old_readback else RuntimeError):
            await pending

        assert runtime.get_device(replacement.host) is replacement
        assert core.source("kasa", replacement.host) is new_source
        assert new_source.connected
        assert state.kasa_active() == {replacement.host: False}
        assert state.snapshot()["kasa"][0]["alias"] == "Replacement"

    asyncio.run(run())


def test_existing_rest_models_and_refresh_shape_are_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        core = Core()
        state = SystemState(core=core)
        runtime = KasaRuntime(core=core)
        device = _Device()
        await _discover(runtime, monkeypatch, device)
        services = SimpleNamespace(kasa_runtime=runtime)

        result = await control_kasa_device(services, device.host, True)
        assert result.model_dump() == {"alias": "Pump", "host": device.host, "model": "HS110", "active": True}
        device.hardware_state = False
        refreshed = await get_kasa_devices(services)
        assert [entry.model_dump() for entry in refreshed] == [{"alias": "Pump", "host": device.host, "model": "HS110", "active": False}]
        assert state.kasa_active() == {device.host: False}

    asyncio.run(run())
