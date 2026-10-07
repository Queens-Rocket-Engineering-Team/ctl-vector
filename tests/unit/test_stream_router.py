"""/v1/stream holds the stand-wide DATA rate; /v1/control is the only control route."""

from __future__ import annotations
from unittest.mock import AsyncMock, MagicMock

from fastapi.testclient import TestClient

from vector.api.fast_api import app
from vector.runtime.qlcp_state import StreamSetting
from vector.runtime.services import RuntimeServices


def _install_esp_runtime(setting: StreamSetting, *, applied_to: list[str]) -> MagicMock:
    esp = MagicMock()
    esp.state_adapter.stream = setting
    esp.set_stream = AsyncMock(return_value=applied_to)
    rt = MagicMock(spec=RuntimeServices)
    rt.esp_runtime = esp
    app.state.runtime = rt
    return esp


def test_stream_setting_is_read_and_partially_updated() -> None:
    esp = _install_esp_runtime(StreamSetting(enabled=False, frequency_hz=30), applied_to=["PANDA"])

    with TestClient(app) as client:
        before = client.get("/v1/stream")
        enabled = client.post("/v1/stream", json={"enabled": True})
        too_fast = client.post("/v1/stream", json={"frequency_hz": 70000})

    assert before.json() == {"enabled": False, "frequency_hz": 30}
    assert enabled.json() == {"enabled": True, "frequency_hz": 30, "applied_to": ["PANDA"]}
    esp.set_stream.assert_awaited_once_with(StreamSetting(enabled=True, frequency_hz=30))
    assert too_fast.status_code == 422


def test_command_route_is_gone() -> None:
    _install_esp_runtime(StreamSetting(), applied_to=[])

    with TestClient(app) as client:
        resp = client.post("/v1/command", json={"command": "CONTROL", "control_name": "AV101", "control_state": "OPEN"})

    assert resp.status_code == 404
