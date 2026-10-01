from __future__ import annotations
import asyncio
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from vector.core import Core, SensorDefinition, TareCaptureError
from vector.daemons.cli_terminal import handle_server_command
from vector.runtime.services import RuntimeServices
from vector.state.system_state import SystemState


def _make_runtime(capture: Any = None, *, core: Core | None = None) -> tuple[RuntimeServices, SystemState, list[dict]]:
    """Build real core/state objects and stub only sample capture."""
    if core is None:
        core = Core()
        core.capture_tare_offset = MagicMock(side_effect=capture)
    system_state = SystemState(core=core)
    published: list[dict] = []

    runtime = MagicMock(spec=RuntimeServices)
    runtime.core = core
    runtime.system_state = system_state
    runtime.state_stream = MagicMock()
    runtime.state_stream.publish.side_effect = published.append
    system_state.set_publisher(published.append)
    return cast("RuntimeServices", runtime), system_state, published


def _run(runtime: RuntimeServices, *args: str) -> None:
    asyncio.run(handle_server_command(runtime, "TARE", list(args)))


def test_tare_captures_an_offset_and_publishes_it() -> None:
    runtime, system_state, published = _make_runtime(capture=lambda *_a, **_kw: (15.0, "PANDA", 16))

    _run(runtime, "PT101")

    assert system_state.core.tares().get("PT101", 0.0) == 15.0
    assert published == [{"type": "tare.updated", "state_version": 1, "sensor_name": "PT101", "offset": 15.0}]


def test_tare_passes_an_explicit_sample_count() -> None:
    runtime, _system_state, _published = _make_runtime(capture=lambda *_a, **_kw: (1.0, "PANDA", 200))

    _run(runtime, "PT101", "200")

    runtime.core.capture_tare_offset.assert_called_once_with("PT101", device_name=None, samples=200)


def test_tare_clear_removes_the_offset() -> None:
    runtime, system_state, published = _make_runtime()
    system_state.core.set_tare("PT101", 15.0)
    published.clear()

    _run(runtime, "PT101", "clear")

    assert system_state.core.tares().get("PT101", 0.0) == 0.0
    assert published == [{"type": "tare.cleared", "state_version": 2, "sensor_name": "PT101"}]


def test_tare_clear_on_an_untared_sensor_publishes_nothing() -> None:
    runtime, _system_state, published = _make_runtime()

    _run(runtime, "PT101", "clear")

    assert published == []


def test_tare_capture_failure_leaves_state_untouched() -> None:
    def _fail(*_args: object, **_kwargs: object) -> tuple[float, str, int]:
        raise TareCaptureError("no samples")

    runtime, system_state, published = _make_runtime(capture=_fail)

    _run(runtime, "PT101")

    assert system_state.core.tares().get("PT101", 0.0) == 0.0
    assert published == []


@pytest.mark.parametrize(
    "values",
    [
        pytest.param((float("nan"),), id="nan"),
        pytest.param((float("inf"),), id="positive-infinity"),
        pytest.param((float("-inf"),), id="negative-infinity"),
        pytest.param((1e308, 1e308), id="sum-overflow"),
    ],
)
def test_tare_rejects_non_finite_capture_and_accepts_next_valid_command(values: tuple[float, ...]) -> None:
    core = Core()
    source = core.register_source("test", "PANDA", sensors=[SensorDefinition("PT101")])
    runtime, system_state, published = _make_runtime(core=core)
    core.set_tare("PT101", 15.0)
    published.clear()
    version = system_state.state_version
    for value in values:
        source.publish_samples([("PT101", value)], timestamp_s=1.0)

    async def run() -> None:
        await handle_server_command(runtime, "TARE", ["PT101"])

        assert core.tares().get("PT101", 0.0) == 15.0
        assert system_state.state_version == version
        assert published == []

        source.publish_samples([("PT101", 9.0)], timestamp_s=2.0)
        await handle_server_command(runtime, "TARE", ["PT101", "1"])

    asyncio.run(run())

    assert core.tares().get("PT101", 0.0) == 9.0
    assert published == [{"type": "tare.updated", "state_version": version + 1, "sensor_name": "PT101", "offset": 9.0}]


def test_tare_rejects_a_non_numeric_sample_count() -> None:
    runtime, _system_state, published = _make_runtime()

    _run(runtime, "PT101", "lots")

    runtime.core.capture_tare_offset.assert_not_called()
    assert published == []


def test_tare_rejects_a_sample_count_larger_than_the_buffer() -> None:
    runtime, _system_state, published = _make_runtime()

    _run(runtime, "PT101", "9999")

    runtime.core.capture_tare_offset.assert_not_called()
    assert published == []


def test_bare_tare_lists_applied_offsets_without_changing_state() -> None:
    runtime, system_state, published = _make_runtime()
    system_state.core.set_tare("PT101", 15.0)
    published.clear()

    _run(runtime)

    assert system_state.snapshot()["tares"] == {"PT101": 15.0}
    assert published == []
