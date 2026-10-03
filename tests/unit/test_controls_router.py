"""POST /v1/control addresses one control on one source and reports the provider's outcome."""

from __future__ import annotations
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from vector.api.fast_api import app
from vector.core import ControlBinding, ControlDefinition, ControlHandler, ControlType, ControlValue, Core, DispatchResult
from vector.runtime.services import RuntimeServices


def _install(handler: ControlHandler) -> Core:
    core = Core()
    core.register_source(
        "test", "PANDA",
        controls=[ControlDefinition("AV101", "valve"), ControlDefinition("HTR101", "heater", type=ControlType.UINT32)],
        control_handler=handler,
    )
    rt = MagicMock(spec=RuntimeServices)
    rt.core = core
    app.state.runtime = rt
    return core


async def _accept(_target: ControlBinding, _value: ControlValue) -> DispatchResult:
    return DispatchResult(True, command_id=7)


def test_sets_a_control_by_source_and_name() -> None:
    writes = []

    async def handler(target: ControlBinding, value: ControlValue) -> DispatchResult:
        writes.append((target.name, value))
        return DispatchResult(True, command_id=7)

    _install(handler)
    with TestClient(app) as client:
        resp = client.post("/v1/control", json={"source": "test:PANDA", "control": "av101", "value": True})

    assert resp.status_code == 200
    assert resp.json() == {"source": "test:PANDA", "control": "AV101", "submitted": True, "command_id": 7}
    assert writes == [("AV101", True)]


def test_unknown_or_malformed_targets_are_rejected() -> None:
    _install(_accept)
    with TestClient(app) as client:
        unknown_source = client.post("/v1/control", json={"source": "test:OTHER", "control": "AV101", "value": True})
        unknown_control = client.post("/v1/control", json={"source": "test:PANDA", "control": "AV999", "value": True})
        no_provider = client.post("/v1/control", json={"source": "PANDA", "control": "AV101", "value": True})

    assert (unknown_source.status_code, unknown_control.status_code, no_provider.status_code) == (404, 404, 422)


def test_values_the_control_cannot_take_are_400_and_only_range_reaches_the_provider() -> None:
    writes = []

    async def handler(target: ControlBinding, value: ControlValue) -> DispatchResult:
        writes.append((target.name, value))
        if isinstance(value, int) and value < 0:
            message = f"{value} does not fit UINT32."
            raise ValueError(message)
        return DispatchResult(True)

    _install(handler)
    with TestClient(app) as client:
        wrong_type = client.post("/v1/control", json={"source": "test:PANDA", "control": "AV101", "value": 1})
        not_a_value = client.post("/v1/control", json={"source": "test:PANDA", "control": "AV101", "value": "OPEN"})
        out_of_range = client.post("/v1/control", json={"source": "test:PANDA", "control": "HTR101", "value": -1})

    assert (wrong_type.status_code, not_a_value.status_code, out_of_range.status_code) == (400, 422, 400)
    assert "UINT32" in out_of_range.json()["detail"]
    assert writes == [("HTR101", -1)]


def test_disconnected_source_is_409_and_a_failed_send_is_502() -> None:
    async def failing(_target: ControlBinding, _value: ControlValue) -> DispatchResult:
        return DispatchResult(False, error="Device is disconnected")

    core = _install(failing)
    with TestClient(app) as client:
        failed = client.post("/v1/control", json={"source": "test:PANDA", "control": "AV101", "value": True})
        core.source("test", "PANDA").close()
        closed = client.post("/v1/control", json={"source": "test:PANDA", "control": "AV101", "value": True})

    assert (failed.status_code, closed.status_code) == (502, 409)
