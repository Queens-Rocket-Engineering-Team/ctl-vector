from __future__ import annotations
from unittest.mock import AsyncMock, MagicMock

from fastapi.testclient import TestClient

from vector.api.fast_api import app
from vector.qlcp.config_models import ControlConfig
from vector.qlcp.enums import ControlState, ControlType
from vector.runtime.qlcp_state import StreamSetting
from vector.runtime.services import RuntimeServices


# ---------------------------------------------------------------------------
# Fake runtime helpers
# ---------------------------------------------------------------------------


def _bool_control(name: str) -> ControlConfig:
    return ControlConfig(
        id=0, name=name, group="valve", default=ControlState.CLOSED, type=ControlType.BOOL, unit=None
    )


def _variable_control(name: str, control_type: ControlType) -> ControlConfig:
    default = 0 if control_type is ControlType.UINT32 else 0.0
    return ControlConfig(id=0, name=name, group="heater", default=default, type=control_type, unit="%")


def _make_fake_session(*, name: str = "TEST-DEVICE", controls: dict | None = None) -> MagicMock:
    session = MagicMock()
    session.name = name
    session.controls = controls if controls is not None else {"AV101": MagicMock()}
    return session


def _make_fake_esp_runtime(sessions: list[MagicMock] | None = None) -> MagicMock:
    rt = MagicMock()
    device_map = {f"10.0.0.{i}": s for i, s in enumerate(sessions or [])}
    rt.get_registered_devices.return_value = device_map
    rt.get_single = AsyncMock()
    rt.stop_streaming = AsyncMock()
    rt.start_streaming = AsyncMock()
    rt.set_control = AsyncMock()
    rt.emergency_stop = AsyncMock()
    return rt


def _install_runtime(esp_runtime: MagicMock) -> RuntimeServices:
    """Wire a fake esp_runtime into a minimal RuntimeServices and install it on app.state."""
    rt = MagicMock(spec=RuntimeServices)
    rt.esp_runtime = esp_runtime
    app.state.runtime = rt
    return rt


# ---------------------------------------------------------------------------
# /v1/stream
# ---------------------------------------------------------------------------


def test_stream_setting_is_read_and_partially_updated() -> None:
    esp = _make_fake_esp_runtime()
    esp.state_adapter.stream = StreamSetting(enabled=False, frequency_hz=30)
    esp.set_stream = AsyncMock(return_value=["PANDA"])
    _install_runtime(esp)

    with TestClient(app) as client:
        before = client.get("/v1/stream")
        enabled = client.post("/v1/stream", json={"enabled": True})
        too_fast = client.post("/v1/stream", json={"frequency_hz": 70000})

    assert before.json() == {"enabled": False, "frequency_hz": 30}
    assert enabled.json() == {"enabled": True, "frequency_hz": 30, "applied_to": ["PANDA"]}
    esp.set_stream.assert_awaited_once_with(StreamSetting(enabled=True, frequency_hz=30))
    assert too_fast.status_code == 422


# ---------------------------------------------------------------------------
# /v1/command — GET_SINGLE (GETS)
# ---------------------------------------------------------------------------


def test_gets_command_awaits_get_single() -> None:
    session = _make_fake_session()
    esp_rt = _make_fake_esp_runtime([session])
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post("/v1/command", json={"command": "GETS"})

    assert resp.status_code == 200
    esp_rt.get_single.assert_awaited_once_with(session)


# ---------------------------------------------------------------------------
# /v1/command — STOP
# ---------------------------------------------------------------------------


def test_stop_command_awaits_stop_streaming() -> None:
    session = _make_fake_session()
    esp_rt = _make_fake_esp_runtime([session])
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post("/v1/command", json={"command": "STOP"})

    assert resp.status_code == 200
    esp_rt.stop_streaming.assert_awaited_once_with(session)


# ---------------------------------------------------------------------------
# /v1/command — STREAM
# ---------------------------------------------------------------------------


def test_stream_command_awaits_start_streaming() -> None:
    session = _make_fake_session()
    esp_rt = _make_fake_esp_runtime([session])
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post("/v1/command", json={"command": "STREAM", "frequency_hz": 10})

    assert resp.status_code == 200
    esp_rt.start_streaming.assert_awaited_once_with(session, 10)


# ---------------------------------------------------------------------------
# /v1/command — CONTROL
# ---------------------------------------------------------------------------


def test_control_command_awaits_set_control() -> None:
    session = _make_fake_session(controls={"AV101": _bool_control("AV101")})
    esp_rt = _make_fake_esp_runtime([session])
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post(
            "/v1/command",
            json={"command": "CONTROL", "control_name": "AV101", "control_state": "OPEN"},
        )

    assert resp.status_code == 200
    esp_rt.set_control.assert_awaited_once_with(session, "AV101", "OPEN")


def test_control_command_forwards_integer_state_for_variable_control() -> None:
    session = _make_fake_session(controls={"HEATER1": _variable_control("HEATER1", ControlType.UINT32)})
    esp_rt = _make_fake_esp_runtime([session])
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post(
            "/v1/command",
            json={"command": "CONTROL", "control_name": "HEATER1", "control_state": 75},
        )

    assert resp.status_code == 200
    esp_rt.set_control.assert_awaited_once_with(session, "HEATER1", "75")


def test_control_command_forwards_float_state_for_variable_control() -> None:
    session = _make_fake_session(controls={"HEATER2": _variable_control("HEATER2", ControlType.FLOAT32)})
    esp_rt = _make_fake_esp_runtime([session])
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post(
            "/v1/command",
            json={"command": "CONTROL", "control_name": "HEATER2", "control_state": 62.5},
        )

    assert resp.status_code == 200
    esp_rt.set_control.assert_awaited_once_with(session, "HEATER2", "62.5")


def test_control_command_rejects_non_numeric_non_bool_state() -> None:
    session = _make_fake_session(controls={"HEATER1": _variable_control("HEATER1", ControlType.UINT32)})
    esp_rt = _make_fake_esp_runtime([session])
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post(
            "/v1/command",
            json={"command": "CONTROL", "control_name": "HEATER1", "control_state": "HOT"},
        )

    assert resp.status_code == 422
    esp_rt.set_control.assert_not_awaited()


def test_control_command_rejects_numeric_state_for_bool_control() -> None:
    session = _make_fake_session(controls={"AV101": _bool_control("AV101")})
    esp_rt = _make_fake_esp_runtime([session])
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post(
            "/v1/command",
            json={"command": "CONTROL", "control_name": "AV101", "control_state": 75},
        )

    assert resp.status_code == 400
    assert "BOOL" in resp.json()["detail"]
    esp_rt.set_control.assert_not_awaited()


def test_control_command_rejects_bool_state_for_variable_control() -> None:
    session = _make_fake_session(controls={"HEATER1": _variable_control("HEATER1", ControlType.UINT32)})
    esp_rt = _make_fake_esp_runtime([session])
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post(
            "/v1/command",
            json={"command": "CONTROL", "control_name": "HEATER1", "control_state": "OPEN"},
        )

    assert resp.status_code == 400
    assert "UINT32" in resp.json()["detail"]
    esp_rt.set_control.assert_not_awaited()


def test_control_command_rejects_before_sending_to_any_device() -> None:
    """A state valid for one device but not another is rejected without dispatching to either."""
    variable = _make_fake_session(name="DEVICE-VAR", controls={"HEATER1": _variable_control("HEATER1", ControlType.UINT32)})
    boolean = _make_fake_session(name="DEVICE-BOOL", controls={"HEATER1": _bool_control("HEATER1")})
    esp_rt = _make_fake_esp_runtime([variable, boolean])
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post(
            "/v1/command",
            json={"command": "CONTROL", "control_name": "HEATER1", "control_state": 75},
        )

    assert resp.status_code == 400
    assert "DEVICE-BOOL" in resp.json()["detail"]
    esp_rt.set_control.assert_not_awaited()


def test_control_command_skips_device_without_matching_control() -> None:
    """When no device has the named control, the router returns 400."""
    session = _make_fake_session(controls={})  # no controls
    esp_rt = _make_fake_esp_runtime([session])
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post(
            "/v1/command",
            json={"command": "CONTROL", "control_name": "NONEXISTENT", "control_state": "OPEN"},
        )

    assert resp.status_code == 400
    esp_rt.set_control.assert_not_awaited()


# ---------------------------------------------------------------------------
# /v1/command — no devices registered
# ---------------------------------------------------------------------------


def test_command_returns_400_when_no_devices_registered() -> None:
    esp_rt = _make_fake_esp_runtime([])  # empty device map
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post("/v1/command", json={"command": "GETS"})

    assert resp.status_code == 400
    assert "No valid target devices" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# /v1/command — multiple devices
# ---------------------------------------------------------------------------


def test_command_is_sent_to_all_registered_devices() -> None:
    sessions = [_make_fake_session(name=f"DEVICE-{i}") for i in range(3)]
    esp_rt = _make_fake_esp_runtime(sessions)
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post("/v1/command", json={"command": "GETS"})

    assert resp.status_code == 200
    assert esp_rt.get_single.await_count == 3


def test_command_returns_502_when_all_sends_fail() -> None:
    sessions = [_make_fake_session(name=f"DEVICE-{i}") for i in range(2)]
    esp_rt = _make_fake_esp_runtime(sessions)
    esp_rt.get_single.return_value = False
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post("/v1/command", json={"command": "GETS"})

    assert resp.status_code == 502
    detail = resp.json()["detail"]
    assert "DEVICE-0" in detail
    assert "DEVICE-1" in detail


def test_command_reports_partial_when_some_sends_fail() -> None:
    sessions = [_make_fake_session(name=f"DEVICE-{i}") for i in range(2)]
    esp_rt = _make_fake_esp_runtime(sessions)
    esp_rt.get_single.side_effect = [True, False]
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post("/v1/command", json={"command": "GETS"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "partial"
    assert "DEVICE-0" in body["message"]
    assert "DEVICE-1" in body["message"]


# ---------------------------------------------------------------------------
# /v1/estop
# ---------------------------------------------------------------------------


def test_estop_awaits_emergency_stop_on_each_device() -> None:
    sessions = [_make_fake_session(name=f"DEVICE-{i}") for i in range(2)]
    esp_rt = _make_fake_esp_runtime(sessions)
    esp_rt.emergency_stop.return_value = True
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post("/v1/estop")

    assert resp.status_code == 200
    assert resp.json()["status"] == "sent"
    assert esp_rt.emergency_stop.await_count == 2


def test_estop_returns_400_when_no_devices_registered() -> None:
    esp_rt = _make_fake_esp_runtime([])
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post("/v1/estop")

    assert resp.status_code == 400
    assert "No valid target devices" in resp.json()["detail"]
    esp_rt.emergency_stop.assert_not_awaited()


def test_estop_returns_502_when_all_sends_fail() -> None:
    sessions = [_make_fake_session(name=f"DEVICE-{i}") for i in range(2)]
    esp_rt = _make_fake_esp_runtime(sessions)
    esp_rt.emergency_stop.return_value = False
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post("/v1/estop")

    assert resp.status_code == 502
    assert "DEVICE-0" in resp.json()["detail"]
    assert "DEVICE-1" in resp.json()["detail"]


def test_estop_reports_partial_when_some_sends_fail() -> None:
    sessions = [_make_fake_session(name=f"DEVICE-{i}") for i in range(2)]
    esp_rt = _make_fake_esp_runtime(sessions)
    esp_rt.emergency_stop.side_effect = [True, False]
    _install_runtime(esp_rt)

    with TestClient(app) as client:
        resp = client.post("/v1/estop")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "partial"
    assert "DEVICE-1" in body["message"]
