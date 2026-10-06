from __future__ import annotations
import asyncio

import pytest

from vector.qlcp.decoding import decode_packet_client
from vector.qlcp.enums import PacketType
from vector.qlcp.packets import DiscoveryPacket
from vector.runtime.discovery import DiscoveryService


class FakeSocket:
    def __init__(self) -> None:
        self.sent: list[tuple[bytes, tuple[str, int]]] = []
        self.closed = False

    def sendto(self, data: bytes, address: tuple[str, int]) -> None:
        self.sent.append((data, address))

    def close(self) -> None:
        self.closed = True


class _StopLoop(Exception):
    pass


def test_discover_starts_every_provider_without_waiting_for_it(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        runs = 0

        async def provider() -> None:
            nonlocal runs
            runs += 1
            started.set()
            await release.wait()

        service = DiscoveryService(providers=[provider])
        monkeypatch.setattr(service, "_create_socket", FakeSocket)

        service.discover()  # returns before the provider finishes
        await asyncio.wait_for(started.wait(), timeout=1.0)
        assert runs == 1
        release.set()
        await asyncio.sleep(0)

    asyncio.run(run())


def test_default_config_is_periodic_discovery() -> None:
    service = DiscoveryService()

    assert service.periodic_enabled is True
    assert service.periodic_interval_s == 30.0

    service.periodic_enabled = False
    service.periodic_interval_s = 5.0
    assert service.periodic_enabled is False
    assert service.periodic_interval_s == 5.0


def test_discover_lazily_creates_socket_once_and_sends_request(monkeypatch: pytest.MonkeyPatch) -> None:
    service = DiscoveryService(multicast_address="239.100.0.1", multicast_port=10000)
    fake = FakeSocket()
    created = 0

    def _create_socket() -> FakeSocket:
        nonlocal created
        created += 1
        return fake

    monkeypatch.setattr(service, "_create_socket", _create_socket)

    service.discover()
    service.discover()

    assert created == 1  # socket created once and reused
    assert len(fake.sent) == 2
    payload, address = fake.sent[0]
    assert address == ("239.100.0.1", 10000)

    packet = decode_packet_client(payload)
    assert isinstance(packet, DiscoveryPacket)
    assert packet.packet_type == PacketType.DISCOVERY

def test_run_issues_discovery_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        service = DiscoveryService(periodic_enabled=True, periodic_interval_s=7.0)
        discover_calls = 0
        sleeps: list[float] = []

        def _discover() -> None:
            nonlocal discover_calls
            discover_calls += 1

        async def _fake_sleep(delay: float) -> None:
            sleeps.append(delay)
            raise _StopLoop  # break out after the first loop iteration

        monkeypatch.setattr(service, "discover", _discover)
        monkeypatch.setattr("vector.runtime.discovery.asyncio.sleep", _fake_sleep)

        with pytest.raises(_StopLoop):
            await service.run()

        assert discover_calls == 1
        assert sleeps == [7.0]

    asyncio.run(run())


def test_run_recovers_when_discover_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        service = DiscoveryService(periodic_enabled=True, periodic_interval_s=7.0)
        fake = FakeSocket()
        service._socket = fake  # type: ignore[assignment]
        sleeps: list[float] = []

        def _discover() -> None:
            raise OSError("network unreachable")

        async def _fake_sleep(delay: float) -> None:
            sleeps.append(delay)
            raise _StopLoop

        monkeypatch.setattr(service, "discover", _discover)
        monkeypatch.setattr("vector.runtime.discovery.asyncio.sleep", _fake_sleep)

        with pytest.raises(_StopLoop):
            await service.run()

        # The loop survived the error, dropped the socket, and slept the normal interval.
        assert fake.closed is True
        assert service._socket is None
        assert sleeps == [7.0]

    asyncio.run(run())


def test_run_skips_discovery_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run() -> None:
        service = DiscoveryService(periodic_enabled=False)
        discover_calls = 0
        sleeps: list[float] = []

        def _discover() -> None:
            nonlocal discover_calls
            discover_calls += 1

        async def _fake_sleep(delay: float) -> None:
            sleeps.append(delay)
            raise _StopLoop

        monkeypatch.setattr(service, "discover", _discover)
        monkeypatch.setattr("vector.runtime.discovery.asyncio.sleep", _fake_sleep)

        with pytest.raises(_StopLoop):
            await service.run()

        assert discover_calls == 0
        assert sleeps == [0.5]  # short poll interval while disabled

    asyncio.run(run())
