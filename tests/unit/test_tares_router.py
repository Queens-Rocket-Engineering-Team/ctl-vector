from __future__ import annotations
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from vector.api.fast_api import app
from vector.core import Core, SensorDefinition
from vector.qlcp.config_parser import parse_config
from vector.qlcp.packets import DataPacket, PacketHeader, SensorReading
from vector.runtime.services import RuntimeServices
from vector.runtime.telemetry_ingest import TelemetryRuntime
from vector.state.system_state import SystemState


if TYPE_CHECKING:
    from vector.runtime.esp_connection_runtime import ESPDeviceSession


# ---------------------------------------------------------------------------
# Fake runtime helpers
#
# SystemState and TelemetryRuntime are real here, not mocks: the state_version
# bookkeeping and the sample capture are the behaviour under test.
# ---------------------------------------------------------------------------


def _make_config(device_name: str) -> dict[str, Any]:
    return {
        "device_name": device_name,
        "sensors": {
            "pressure_transducer": {
                "PT101": {"sensor_index": "PT1", "unit": "PSI"},
            },
        },
        "controls": {},
    }


def _make_session(*, device_name: str = "PANDA", address: str = "10.0.0.2") -> ESPDeviceSession:
    config = parse_config(_make_config(device_name))
    session = SimpleNamespace(
        name=config.name,
        address=address,
        connection_key=f"conn-{address}",
        qlcp_config=config,
        last_sync_time=1.0,
        sensors={sensor.name: sensor for sensor in config.sensors_by_id.values()},
    )
    return cast("ESPDeviceSession", session)


def _install_runtime(*sessions: ESPDeviceSession) -> tuple[RuntimeServices, SystemState, TelemetryRuntime, list[dict]]:
    """Install a runtime whose state/telemetry are real, and capture published state events."""
    core = Core()
    for session in sessions:
        session.core_source = core.register_source(
            "test", session.name, address=session.address, connection_key=session.connection_key,
            sensors=tuple(SensorDefinition(sensor.name, sensor.group, sensor.unit) for sensor in session.qlcp_config.sensors_by_id.values()),
        )
    system_state = SystemState(core=core)
    telemetry_runtime = TelemetryRuntime(
        {session.address: session for session in sessions}.get,
    )
    published: list[dict] = []

    esp_runtime = MagicMock()
    esp_runtime.get_registered_devices.return_value = {session.address: session for session in sessions}

    rt = MagicMock(spec=RuntimeServices)
    rt.core = core
    rt.system_state = system_state
    rt.telemetry_runtime = telemetry_runtime
    rt.esp_runtime = esp_runtime
    rt.state_stream = MagicMock()
    rt.state_stream.publish.side_effect = published.append
    system_state.set_publisher(published.append)
    app.state.runtime = rt
    return rt, system_state, telemetry_runtime, published


def _feed(telemetry_runtime: TelemetryRuntime, session: ESPDeviceSession, *values: float) -> None:
    for value in values:
        packet = DataPacket(
            header=PacketHeader(sequence=1, timestamp_us=12345),
            readings=[SensorReading(sensor_id=0, value=value)],
        )
        telemetry_runtime.handle_packet(packet, session)


# ---------------------------------------------------------------------------
# POST /v1/tares — capture
# ---------------------------------------------------------------------------


def test_capture_averages_recent_readings_and_publishes_state_event() -> None:
    session = _make_session()
    _rt, system_state, telemetry_runtime, published = _install_runtime(session)
    _feed(telemetry_runtime, session, 10.0, 12.0, 14.0, 16.0)

    with TestClient(app) as client:
        resp = client.post("/v1/tares", json={"sensor_name": "PT101", "samples": 2})

    assert resp.status_code == 200
    assert resp.json() == {
        "sensor_name": "PT101",
        "offset": 15.0,
        "sampled_device": "PANDA",
        "sample_count": 2,
        "applies_to": ["PANDA"],
    }
    assert system_state.core.tares().get("PT101", 0.0) == 15.0
    assert published == [{"type": "tare.updated", "state_version": 1, "sensor_name": "PT101", "offset": 15.0}]


def test_capture_without_recent_telemetry_is_rejected() -> None:
    session = _make_session()
    _rt, system_state, _telemetry_runtime, published = _install_runtime(session)

    with TestClient(app) as client:
        resp = client.post("/v1/tares", json={"sensor_name": "PT101"})

    assert resp.status_code == 409
    assert "No telemetry received" in resp.json()["detail"]
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
def test_non_finite_capture_returns_conflict_and_preserves_existing_tare(values: tuple[float, ...]) -> None:
    runtime, system_state, _telemetry_runtime, published = _install_runtime()
    source = runtime.core.register_source("test", "PANDA", sensors=[SensorDefinition("PT101")])
    runtime.core.set_tare("PT101", 15.0)
    published.clear()
    version = system_state.state_version
    for value in values:
        source.publish_samples([("PT101", value)], timestamp_s=1.0)

    with TestClient(app) as client:
        response = client.post("/v1/tares", json={"sensor_name": "PT101"})

    assert response.status_code == 409
    assert "finite" in response.json()["detail"].lower()
    assert runtime.core.tares().get("PT101", 0.0) == 15.0
    assert system_state.state_version == version
    assert published == []


def test_capture_is_rejected_when_two_devices_report_the_sensor() -> None:
    """During handoff the name is ambiguous; the caller must say which device to sample."""
    ground = _make_session(device_name="GROUND", address="10.0.0.2")
    flight = _make_session(device_name="FLIGHT", address="10.0.0.3")
    _rt, _system_state, telemetry_runtime, _published = _install_runtime(ground, flight)
    _feed(telemetry_runtime, ground, 10.0)
    _feed(telemetry_runtime, flight, 90.0)

    with TestClient(app) as client:
        resp = client.post("/v1/tares", json={"sensor_name": "PT101"})
        assert resp.status_code == 409
        assert "FLIGHT, GROUND" in resp.json()["detail"]

        resp = client.post("/v1/tares", json={"sensor_name": "PT101", "device_name": "FLIGHT"})

    body = resp.json()
    assert resp.status_code == 200
    assert body["offset"] == 90.0
    assert body["sampled_device"] == "FLIGHT"
    # The offset is captured from one device but applies to every device carrying the name.
    assert body["applies_to"] == ["FLIGHT", "GROUND"]


# ---------------------------------------------------------------------------
# POST /v1/tares — explicit offset
# ---------------------------------------------------------------------------


def test_explicit_offset_skips_capture() -> None:
    session = _make_session()
    _rt, system_state, _telemetry_runtime, published = _install_runtime(session)

    with TestClient(app) as client:
        resp = client.post("/v1/tares", json={"sensor_name": "PT101", "offset": 4.5})

    assert resp.status_code == 200
    assert resp.json()["sampled_device"] is None
    assert system_state.core.tares().get("PT101", 0.0) == 4.5
    assert len(published) == 1


def test_explicit_offset_is_accepted_for_a_sensor_no_device_reports_yet() -> None:
    """A tare can be pre-staged before the device carrying that sensor connects."""
    _rt, system_state, _telemetry_runtime, _published = _install_runtime()

    with TestClient(app) as client:
        resp = client.post("/v1/tares", json={"sensor_name": "PT999", "offset": 1.0})

    assert resp.status_code == 200
    assert resp.json()["applies_to"] == []
    assert system_state.core.tares().get("PT999", 0.0) == 1.0


def test_non_finite_offset_is_rejected() -> None:
    """An inf offset would poison every subsequent reading for the sensor."""
    _rt, system_state, _telemetry_runtime, published = _install_runtime()

    with TestClient(app) as client:
        # Sent as raw content: json.dumps refuses to encode inf in the first place.
        for literal in ("Infinity", "-Infinity", "NaN"):
            resp = client.post(
                "/v1/tares",
                content=f'{{"sensor_name": "PT101", "offset": {literal}}}',
                headers={"content-type": "application/json"},
            )
            assert resp.status_code == 400, literal

    assert system_state.core.tares().get("PT101", 0.0) == 0.0
    assert published == []


def test_sample_count_outside_the_buffer_size_is_rejected() -> None:
    _rt, _system_state, _telemetry_runtime, _published = _install_runtime()

    with TestClient(app) as client:
        assert client.post("/v1/tares", json={"sensor_name": "PT101", "samples": 0}).status_code == 422
        assert client.post("/v1/tares", json={"sensor_name": "PT101", "samples": 1000}).status_code == 422


# ---------------------------------------------------------------------------
# GET / DELETE /v1/tares
# ---------------------------------------------------------------------------


def test_get_tares_lists_every_applied_offset() -> None:
    _rt, system_state, _telemetry_runtime, _published = _install_runtime()
    system_state.core.set_tare("PT201", 1.0)
    system_state.core.set_tare("PT101", 2.0)

    with TestClient(app) as client:
        resp = client.get("/v1/tares")

    assert resp.json() == {"PT101": 2.0, "PT201": 1.0}


def test_delete_clears_the_offset_and_publishes() -> None:
    session = _make_session()
    _rt, system_state, _telemetry_runtime, published = _install_runtime(session)
    system_state.core.set_tare("PT101", 15.0)
    published.clear()

    with TestClient(app) as client:
        resp = client.delete("/v1/tares", params={"sensor_name": "PT101"})

    assert resp.status_code == 200
    assert resp.json()["offset"] == 0.0
    assert system_state.core.tares().get("PT101", 0.0) == 0.0
    assert published == [{"type": "tare.cleared", "state_version": 2, "sensor_name": "PT101"}]


def test_delete_on_an_untared_sensor_succeeds_without_publishing() -> None:
    _rt, _system_state, _telemetry_runtime, published = _install_runtime()

    with TestClient(app) as client:
        resp = client.delete("/v1/tares", params={"sensor_name": "PT101"})

    assert resp.status_code == 200
    assert published == []


def test_delete_handles_sensor_names_containing_a_slash() -> None:
    """Sensor names come from device CONFIG keys, which is why the name is a query param."""
    _rt, system_state, _telemetry_runtime, _published = _install_runtime()
    system_state.core.set_tare("PT101/A", 3.0)

    with TestClient(app) as client:
        resp = client.delete("/v1/tares", params={"sensor_name": "PT101/A"})

    assert resp.status_code == 200
    assert system_state.core.tares().get("PT101/A", 0.0) == 0.0
