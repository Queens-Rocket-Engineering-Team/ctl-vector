"""Kasa plugs are ordinary core sources: dispatched, observed, discovered, and polled like any other."""

from __future__ import annotations
import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest
from kasa import KasaException

from vector.api.routers.controls import ControlRequest, set_control
from vector.core import ControlDispatchError, Core
from vector.runtime.kasa_runtime import POLL_MISS_LIMIT, KasaRuntime
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


def test_power_is_reported_only_after_readback(monkeypatch: pytest.MonkeyPatch) -> None:
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
        device.refresh_started = asyncio.Event()
        device.refresh_release = asyncio.Event()

        command = asyncio.create_task(core.set_control(power, True))
        await asyncio.wait_for(device.refresh_started.wait(), timeout=1.0)
        assert device.writes == [True]
        assert power.reported is not None
        assert power.reported.value is False
        assert state.kasa_active() == {device.host: False}
        device.refresh_release.set()
        assert await command is None  # Kasa has no command IDs

        assert power.reported is not None
        assert power.reported.value is True
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
        power, = core.source("kasa", device.host).controls

        await core.set_control(power, True)

        assert device.writes == [True]
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
        broken_source = core.source("kasa", broken.host)
        healthy_source = core.source("kasa", healthy.host)
        assert broken_source is not None
        assert healthy_source is not None

        with pytest.raises(ControlDispatchError, match="refresh failed") as raised:
            await core.set_control(broken_source.controls[0], True)
        assert isinstance(raised.value.__cause__, KasaException)

        assert not broken_source.connected
        assert healthy_source.connected
        assert state.kasa_active() == {broken.host: False, healthy.host: True}

    asyncio.run(run())


def test_polling_reports_hand_toggles_and_closes_an_unresponsive_plug(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        core = Core()
        state = SystemState(core=core)
        events: list[StateEvent] = []
        state.set_publisher(events.append)
        runtime = KasaRuntime(core=core)
        quiet = _Device("192.168.1.5")
        toggled = _Device("192.168.1.6")
        await _discover(runtime, monkeypatch, quiet, toggled)
        toggled.hardware_state = True  # switched at the wall, not through VECTOR
        quiet.readback_error = KasaException("no route to host")

        state.add_health_view(runtime.health)
        for _ in range(POLL_MISS_LIMIT - 1):
            await runtime.poll_once()
        quiet_source = core.source("kasa", quiet.host)
        toggled_source = core.source("kasa", toggled.host)
        assert quiet_source is not None
        assert toggled_source is not None
        assert quiet_source.connected
        assert state.kasa_active() == {quiet.host: False, toggled.host: True}
        assert state.snapshot_heartbeat(quiet_source) == {"state": "missed", "consecutive_misses": POLL_MISS_LIMIT - 1}
        assert state.snapshot_heartbeat(toggled_source) == {"state": "ok", "consecutive_misses": 0}

        await runtime.poll_once()
        assert not quiet_source.connected
        assert state.snapshot_heartbeat(quiet_source)["state"] == "disconnected"
        assert [event["type"] for event in events[-2:]] == ["kasa.updated", "kasa.disconnected"]

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
        assert old_source is not None
        old.refresh_started = asyncio.Event()
        old.refresh_release = asyncio.Event()
        pending = asyncio.create_task(core.set_control(old_source.controls[0], True))
        await asyncio.wait_for(old.refresh_started.wait(), timeout=1.0)

        # The poll loop gave up on the plug while its write was still in flight, then it reappeared.
        runtime._remove_device(old.host)  # noqa: SLF001
        replacement = _Device(old.host, active=False, alias="Replacement")
        await _discover(runtime, monkeypatch, replacement)
        new_source = core.source("kasa", replacement.host)
        assert new_source is not None
        assert new_source is not old_source
        if fail_old_readback:
            old.readback_error = KasaException("old refresh failed")
        old.refresh_release.set()
        if fail_old_readback:
            with pytest.raises(ControlDispatchError) as raised:
                await pending
            assert isinstance(raised.value.__cause__, KasaException)
        else:
            # The write was sent, so the command reports success; only its late
            # readback is kept away from the replacement source.
            assert await pending is None

        assert core.source("kasa", replacement.host) is new_source
        assert new_source.connected
        assert state.kasa_active() == {replacement.host: False}
        assert state.snapshot()["kasa"][0]["alias"] == "Replacement"

    asyncio.run(run())


def test_rediscovery_keeps_known_plugs_and_registers_new_ones(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        core = Core()
        state = SystemState(core=core)
        events: list[StateEvent] = []
        state.set_publisher(events.append)
        runtime = KasaRuntime(core=core)
        first = _Device("192.168.1.5")
        await _discover(runtime, monkeypatch, first)
        first_source = core.source("kasa", first.host)

        second = _Device("192.168.1.6", alias="Heater")
        await _discover(runtime, monkeypatch, _Device(first.host, alias="Same plug, new object"), second)

        assert core.source("kasa", first.host) is first_source
        assert core.source("kasa", second.host) is not None
        assert [event["type"] for event in events] == ["kasa.registered", "kasa.registered"]

    asyncio.run(run())


def test_generic_control_endpoint_drives_kasa_power(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        core = Core()
        runtime = KasaRuntime(core=core)
        device = _Device()
        await _discover(runtime, monkeypatch, device)
        request = ControlRequest(source=f"kasa:{device.host}", control="power", value=True)

        result = await set_control(request, SimpleNamespace(core=core))

        assert result.model_dump() == {"source": f"kasa:{device.host}", "control": "power", "submitted": True, "command_id": None}
        assert device.writes == [True]
        assert core.source("kasa", device.host).controls[0].reported.value is True

    asyncio.run(run())
