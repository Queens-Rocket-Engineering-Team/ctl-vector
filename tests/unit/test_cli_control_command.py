"""The CLI's control, open, and close commands dispatch through the core by source and name."""

from __future__ import annotations
import asyncio
from typing import cast
from unittest.mock import MagicMock

from vector.core import ControlBinding, ControlDefinition, ControlType, ControlValue, Core
from vector.daemons.cli_terminal import handle_device_command
from vector.runtime.services import RuntimeServices


def _make_runtime() -> tuple[RuntimeServices, list[tuple[str, str, ControlValue]]]:
    writes: list[tuple[str, str, ControlValue]] = []

    async def handler(target: ControlBinding, value: ControlValue) -> int:
        writes.append((target.source.key, target.name, value))
        return 1

    core = Core()
    core.register_source(
        "qlcp", "PANDA", control_handler=handler,
        controls=[ControlDefinition("AV101", "valve"), ControlDefinition("HTR101", "heater", type=ControlType.UINT32)],
    )
    core.register_source("kasa", "192.168.1.5", controls=[ControlDefinition("power")], control_handler=handler)
    runtime = MagicMock(spec=RuntimeServices)
    runtime.core = core
    return cast("RuntimeServices", runtime), writes


def _run(runtime: RuntimeServices, command: str, *args: str) -> None:
    asyncio.run(handle_device_command(runtime, command, list(args)))


def test_control_open_and_close_address_a_qlcp_device_by_bare_name() -> None:
    runtime, writes = _make_runtime()

    _run(runtime, "control", "panda", "av101", "open")
    _run(runtime, "open", "PANDA", "AV101")
    _run(runtime, "close", "PANDA", "AV101")
    _run(runtime, "control", "PANDA", "HTR101", "75")

    assert writes == [("PANDA", "AV101", True), ("PANDA", "AV101", True), ("PANDA", "AV101", False), ("PANDA", "HTR101", 75)]


def test_provider_key_addresses_any_source() -> None:
    runtime, writes = _make_runtime()

    _run(runtime, "control", "kasa:192.168.1.5", "power", "on")
    _run(runtime, "control", "qlcp:PANDA", "AV101", "closed")

    assert writes == [("192.168.1.5", "power", True), ("PANDA", "AV101", False)]


def test_bad_targets_and_values_send_nothing() -> None:
    runtime, writes = _make_runtime()

    _run(runtime, "control", "NOPE", "AV101", "open")
    _run(runtime, "control", "PANDA", "AV999", "open")
    _run(runtime, "control", "PANDA", "AV101", "sideways")
    _run(runtime, "control", "PANDA", "HTR101", "1.5")
    _run(runtime, "open", "PANDA")

    assert writes == []
