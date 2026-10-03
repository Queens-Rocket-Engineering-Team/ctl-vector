from types import SimpleNamespace
from typing import Any, cast

from vector.core import ControlDefinition, ControlObservation, ControlStatus, Core, Source
from vector.qlcp.config_parser import parse_config
from vector.qlcp.enums import ControlState, ErrorCode, PacketType
from vector.runtime.command_tracker import CommandRecord, CommandTracker
from vector.runtime.qlcp_state import QLCPStateAdapter
from vector.state.system_state import StateEvent, SystemState


def _make_config(name: str = "TEST-DEVICE") -> dict[str, Any]:
    return {
        "device_name": name,
        "sensors": {
            "thermocouple": {
                "TC1": {
                    "sensor_index": "TC1",
                    "type": "K",
                    "unit": "C",
                },
            },
        },
        "controls": {
            "valve": {
                "VALVE1": {
                    "control_index": "VALVE1",
                    "type": "BOOL",
                    "default_state": "CLOSED",
                },
            },
        },
    }


def _make_device(
    *,
    address: str = "10.0.0.2",
    name: str = "TEST-DEVICE",
    connection_key: str = "conn-a",
    heartbeat_misses: int = 0,
) -> Any:
    config = parse_config(_make_config(name=name))
    return SimpleNamespace(
        address=address,
        connection_key=connection_key,
        name=config.name,
        qlcp_config=config,
        last_sync_time=None,
        missed_heartbeat_count=heartbeat_misses,
        core_source=None,
    )


def _make_state() -> tuple[SystemState, CommandTracker, QLCPStateAdapter, list[StateEvent]]:
    tracker = CommandTracker()
    core = Core()
    state = SystemState(core=core)
    events: list[StateEvent] = []
    state.set_publisher(events.append)
    return state, tracker, QLCPStateAdapter(state, tracker), events


def _register_kasa(core: Core, host: str, alias: str, model: str, active: bool) -> Source:
    return core.register_source(
        "kasa", host,
        controls=(ControlDefinition(name="power"),),
        metadata={"alias": alias, "model": model},
        initial_controls={"power": ControlObservation(active, 0.0, ControlStatus.CONFIRMED)},
    )


def _record_kasa(core: Core, host: str, active: bool) -> None:
    source = core.source("kasa", host)
    if source is not None:
        source.report_control("power", active)


def _disconnect_kasa(core: Core, host: str) -> None:
    source = core.source("kasa", host)
    if source is not None:
        source.close()


def _mark_sent(
    tracker: CommandTracker,
    device: Any,
    *,
    packet_type: PacketType = PacketType.CONTROL,
    sequence: int = 12,
    now: float = 10.0,
    control_id: int | None = None,
    control_name: str | None = None,
    requested_state: ControlState | None = None,
    ack_expected: bool | None = None,
) -> CommandRecord:
    return tracker.mark_sent(
        connection_key=device.connection_key,
        device_name=device.name,
        device_address=device.address,
        packet_type=packet_type,
        packet_sequence=sequence,
        now=now,
        control_id=control_id,
        control_name=control_name,
        requested_state=requested_state,
        ack_expected=ack_expected,
    )


def test_register_device_produces_expected_snapshot() -> None:
    state, _, qlcp, events = _make_state()
    device = _make_device()

    qlcp.register_device(device)
    event = events[-1] if events else None
    snapshot = state.snapshot()

    assert event["type"] == "device.registered"
    assert event["state_version"] == 1
    assert state.state_version == 1
    assert snapshot["state_version"] == 1
    assert len(snapshot["devices"]) == 1

    device_snapshot = snapshot["devices"][0]
    assert device_snapshot["source_provider"] == "qlcp"
    assert device_snapshot["source_key"] == "TEST-DEVICE"
    assert device_snapshot["connection_key"] == "conn-a"
    assert device_snapshot["name"] == "TEST-DEVICE"
    assert device_snapshot["connected"] is True
    assert device_snapshot["address"] == "10.0.0.2"
    assert device_snapshot["sensors"][0]["name"] == "TC1"
    assert device_snapshot["sensors"][0]["unit"] == "C"
    assert device_snapshot["controls"][0]["name"] == "VALVE1"
    assert device_snapshot["controls"][0]["reported_state"] is None
    assert device_snapshot["heartbeat"]["state"] == "ok"


def test_replacing_device_with_same_name_updates_snapshot() -> None:
    state, _, qlcp, _events = _make_state()
    old_device = _make_device(address="10.0.0.2", name="TEST-DEVICE", connection_key="conn-a")
    new_device = _make_device(address="10.0.0.3", name="TEST-DEVICE", connection_key="conn-b")

    qlcp.register_device(old_device)
    qlcp.register_device(new_device)
    snapshot = state.snapshot()

    assert len(snapshot["devices"]) == 1
    assert snapshot["devices"][0]["address"] == "10.0.0.3"


def test_duplicate_source_labels_retain_identity_in_snapshots_and_registration_events() -> None:
    state, _, _, events = _make_state()
    registrations = [("b", "sensor"), ("a", "sensor-2"), ("a", "sensor")]
    for provider, key in registrations:
        source = state.core.register_source(provider, key, name="Shared label", controls=[ControlDefinition("VALVE")])
        registered = cast("dict[str, Any]", events[-1]["device"])
        assert (registered["source_provider"], registered["source_key"]) == (provider, key)
        assert registered["connection_key"] == source.connection_key

    devices = state.snapshot()["devices"]
    assert [device["name"] for device in devices] == ["Shared label"] * 3
    assert [(device["source_provider"], device["source_key"]) for device in devices] == sorted(registrations)
    assert len({device["connection_key"] for device in devices}) == 3


def test_duplicate_source_labels_retain_identity_in_control_and_disconnect_events() -> None:
    state, _, _, events = _make_state()
    sources = [
        state.core.register_source(provider, "sensor", name="Shared label", controls=[ControlDefinition("VALVE")])
        for provider in ("a", "b")
    ]
    events.clear()

    for source in sources:
        source.accept_control("VALVE", True)
        source.report_control("VALVE", True)
        source.report_control("VALVE", None, status=ControlStatus.ERROR)
        source.close()
        assert [event["type"] for event in events[-4:]] == ["control.accepted", "control.updated", "control.error", "device.disconnected"]
        for event in events[-4:]:
            assert event["device_name"] == "Shared label"
            assert event["source_provider"] == source.provider
            assert event["source_key"] == source.key
            assert event["connection_key"] == source.connection_key


def test_replaced_source_cannot_emit_control_or_disconnect_events_for_new_generation() -> None:
    state, _, _, events = _make_state()
    old = state.core.register_source("a", "sensor", name="Shared label", controls=[ControlDefinition("VALVE")])
    other = state.core.register_source("b", "sensor", name="Shared label", controls=[ControlDefinition("VALVE")])
    current = state.core.register_source("a", "sensor", name="Shared label", controls=[ControlDefinition("VALVE")])
    version = state.state_version
    events.clear()

    old.accept_control("VALVE", True)
    old.report_control("VALVE", True)
    old.close()
    assert events == []
    assert state.state_version == version

    current.report_control("VALVE", False)
    other.report_control("VALVE", True)
    assert [event["connection_key"] for event in events] == [current.connection_key, other.connection_key]
    assert old.connection_key != current.connection_key
    devices = state.snapshot()["devices"]
    assert [device["controls"][0]["reported_state"] for device in devices] == ["CLOSED", "OPEN"]
    assert all(device["connected"] for device in devices)


def test_old_connection_disconnect_does_not_disconnect_replaced_device() -> None:
    state, _, qlcp, _events = _make_state()
    old_device = _make_device(address="10.0.0.2", name="TEST-DEVICE", connection_key="conn-a")
    new_device = _make_device(address="10.0.0.3", name="TEST-DEVICE", connection_key="conn-b")

    qlcp.register_device(old_device)
    qlcp.register_device(new_device)
    qlcp.mark_disconnected(old_device)
    snapshot = state.snapshot()

    assert len(snapshot["devices"]) == 1
    assert snapshot["devices"][0]["address"] == "10.0.0.3"
    assert snapshot["devices"][0]["connected"] is True


def test_old_connection_with_same_address_does_not_disconnect_replaced_device() -> None:
    state, _, qlcp, _events = _make_state()
    old_device = _make_device(address="10.0.0.2", name="TEST-DEVICE", connection_key="conn-a")
    new_device = _make_device(address="10.0.0.2", name="TEST-DEVICE", connection_key="conn-b")

    qlcp.register_device(old_device)
    qlcp.register_device(new_device)
    qlcp.mark_disconnected(old_device)
    snapshot = state.snapshot()

    assert len(snapshot["devices"]) == 1
    assert snapshot["devices"][0]["address"] == "10.0.0.2"
    assert snapshot["devices"][0]["connected"] is True


def test_status_update_changes_reported_control_state() -> None:
    state, _, qlcp, events = _make_state()
    device = _make_device()

    qlcp.register_device(device)
    qlcp.record_reported_control_state(device, 0, ControlState.OPEN, now=42.0)
    event = events[-1] if events else None
    snapshot = state.snapshot()

    assert event is not None
    assert event["type"] == "control.updated"
    assert event["state_version"] == 2
    assert event["device_name"] == "TEST-DEVICE"
    event_control = cast("dict[str, Any]", event["control"])
    assert event_control["id"] == 0
    assert event_control["name"] == "VALVE1"
    assert event_control["reported_state"] == "OPEN"
    control = snapshot["devices"][0]["controls"][0]
    assert control["reported_state"] == "OPEN"
    assert control["reported_timestamp"] == 42.0


def test_status_update_does_not_register_unconnected_device() -> None:
    state, _, qlcp, events = _make_state()
    device = _make_device()

    qlcp.record_reported_control_state(device, 0, ControlState.OPEN, now=42.0)
    event = events[-1] if events else None
    snapshot = state.snapshot()

    assert event is None
    assert snapshot["state_version"] == 0
    assert snapshot["devices"] == []


def test_old_connection_status_does_not_update_replaced_device() -> None:
    state, _, qlcp, _events = _make_state()
    old_device = _make_device(address="10.0.0.2", name="TEST-DEVICE", connection_key="conn-a")
    new_device = _make_device(address="10.0.0.3", name="TEST-DEVICE", connection_key="conn-b")

    qlcp.register_device(old_device)
    qlcp.register_device(new_device)
    qlcp.record_reported_control_state(old_device, 0, ControlState.OPEN, now=42.0)
    snapshot = state.snapshot()

    control = snapshot["devices"][0]["controls"][0]
    assert control["reported_state"] is None


def test_pending_command_tracker_data_appears_in_snapshot() -> None:
    state, tracker, qlcp, _events = _make_state()
    device = _make_device()
    qlcp.register_device(device)

    command = _mark_sent(
        tracker,
        device,
        control_id=0,
        control_name="VALVE1",
        requested_state=ControlState.CLOSED,
    )

    snapshot = state.snapshot()

    assert snapshot["commands"]["pending"][0]["command_id"] == command.command_id
    assert snapshot["commands"]["pending"][0]["connection_key"] == "conn-a"
    assert snapshot["commands"]["pending"][0]["source_provider"] == "qlcp"
    assert snapshot["commands"]["pending"][0]["source_key"] == "TEST-DEVICE"
    assert snapshot["commands"]["pending"][0]["device_name"] == "TEST-DEVICE"
    assert snapshot["commands"]["pending"][0]["packet_type"] == "CONTROL"
    assert snapshot["commands"]["pending"][0]["state"] == "sent"
    assert snapshot["commands"]["pending"][0]["ack_expected"] is True
    assert snapshot["commands"]["pending"][0]["control_name"] == "VALVE1"
    assert snapshot["commands"]["pending"][0]["requested_state"] == "CLOSED"
    assert snapshot["commands"]["recent"] == []
    assert snapshot["devices"][0]["controls"][0]["pending_command_id"] == command.command_id


def test_recent_completed_command_tracker_data_appears_in_snapshot() -> None:
    state, tracker, qlcp, _events = _make_state()
    device = _make_device()
    qlcp.register_device(device)

    command = _mark_sent(
        tracker,
        device,
        control_id=0,
        control_name="VALVE1",
        requested_state=ControlState.CLOSED,
    )
    tracker.mark_acked(device.connection_key, PacketType.CONTROL, 12, now=11.0)

    snapshot = state.snapshot()

    assert snapshot["commands"]["pending"] == []
    assert snapshot["commands"]["recent"][0]["command_id"] == command.command_id
    assert snapshot["commands"]["recent"][0]["connection_key"] == "conn-a"
    assert snapshot["commands"]["recent"][0]["ack_expected"] is True
    assert snapshot["commands"]["recent"][0]["control_name"] == "VALVE1"
    assert snapshot["commands"]["recent"][0]["state"] == "acked"


def test_heartbeat_commands_are_summarized_not_listed_in_snapshot() -> None:
    state, tracker, qlcp, _events = _make_state()
    device = _make_device()
    qlcp.register_device(device)

    _mark_sent(tracker, device, packet_type=PacketType.HEARTBEAT, now=10.0)
    tracker.mark_acked(device.connection_key, PacketType.HEARTBEAT, 12, now=11.0)

    snapshot = state.snapshot()

    assert snapshot["commands"]["pending"] == []
    assert snapshot["commands"]["recent"] == []
    assert snapshot["devices"][0]["heartbeat"]["state"] == "ok"
    assert snapshot["devices"][0]["heartbeat"]["consecutive_misses"] == 0


def test_pending_heartbeat_is_summarized_not_listed_in_snapshot() -> None:
    state, tracker, qlcp, _events = _make_state()
    device = _make_device()
    qlcp.register_device(device)

    _mark_sent(tracker, device, packet_type=PacketType.HEARTBEAT, now=10.0)

    snapshot = state.snapshot()

    assert snapshot["commands"]["pending"] == []
    assert snapshot["devices"][0]["heartbeat"]["state"] == "ok"


def test_missed_heartbeat_state_is_summarized() -> None:
    state, _, qlcp, _events = _make_state()
    device = _make_device(heartbeat_misses=2)
    qlcp.register_device(device)

    snapshot = state.snapshot()

    assert snapshot["devices"][0]["heartbeat"]["state"] == "missed"
    assert snapshot["devices"][0]["heartbeat"]["consecutive_misses"] == 2


def test_disconnected_device_is_marked_disconnected() -> None:
    state, _, qlcp, events = _make_state()
    device = _make_device()

    qlcp.register_device(device)
    qlcp.mark_disconnected(device)
    event = events[-1] if events else None
    snapshot = state.snapshot()

    assert event is not None
    assert event["type"] == "device.disconnected"
    assert event["state_version"] == 2
    assert event["device_name"] == "TEST-DEVICE"
    assert snapshot["devices"][0]["connected"] is False
    assert snapshot["devices"][0]["heartbeat"]["state"] == "disconnected"


def test_command_lifecycle_events_increment_state_version() -> None:
    state, tracker, qlcp, _events = _make_state()
    device = _make_device()
    qlcp.register_device(device)
    command = _mark_sent(
        tracker,
        device,
        control_id=0,
        control_name="VALVE1",
        requested_state=ControlState.CLOSED,
    )

    sent_event = qlcp.record_command_sent(command)
    tracker.mark_acked(device.connection_key, PacketType.CONTROL, 12, now=11.0)
    acked_event = qlcp.record_command_acked(command)

    assert sent_event is not None
    assert sent_event["type"] == "command.sent"
    assert sent_event["state_version"] == 2
    sent_payload = cast(dict[str, object], sent_event["command"])
    assert sent_payload["source_provider"] == "qlcp"
    assert sent_payload["source_key"] == "TEST-DEVICE"
    assert sent_payload["connection_key"] == "conn-a"
    assert sent_payload["ack_expected"] is True
    assert sent_payload["control_name"] == "VALVE1"
    assert acked_event is not None
    assert acked_event["type"] == "command.acked"
    assert acked_event["state_version"] == 3
    acked_payload = cast(dict[str, object], acked_event["command"])
    assert acked_payload["ack_expected"] is True
    assert acked_payload["control_name"] == "VALVE1"
    assert state.state_version == 3


def test_command_nack_and_timeout_events_include_command_state() -> None:
    _state, tracker, qlcp, _events = _make_state()
    device = _make_device()
    qlcp.register_device(device)
    nacked_command = _mark_sent(
        tracker,
        device,
        sequence=12,
        control_id=0,
        control_name="VALVE1",
    )
    timed_out_command = _mark_sent(
        tracker,
        device,
        sequence=13,
        control_id=0,
        control_name="VALVE1",
    )

    tracker.mark_nacked(
        device.connection_key,
        PacketType.CONTROL,
        12,
        ErrorCode.INVALID_ID,
        now=11.0,
    )
    nacked_event = qlcp.record_command_nacked(nacked_command)
    tracker.expire_pending(now=25.0, timeout_s=10.0)
    timed_out_event = qlcp.record_command_timed_out(timed_out_command)

    assert nacked_event is not None
    assert nacked_event["type"] == "command.nacked"
    nacked_payload = cast(dict[str, object], nacked_event["command"])
    assert nacked_payload["state"] == "nacked"
    assert nacked_payload["control_name"] == "VALVE1"
    assert nacked_payload["nack_error_code"] == "INVALID_ID"
    assert timed_out_event is not None
    assert timed_out_event["type"] == "command.timed_out"
    timed_out_payload = cast(dict[str, object], timed_out_event["command"])
    assert timed_out_payload["state"] == "timed_out"
    assert timed_out_payload["control_name"] == "VALVE1"


def test_heartbeat_event_summarizes_heartbeat_state() -> None:
    _state, tracker, qlcp, _events = _make_state()
    device = _make_device()
    qlcp.register_device(device)
    heartbeat = _mark_sent(tracker, device, packet_type=PacketType.HEARTBEAT, now=10.0)

    event = qlcp.record_command_sent(heartbeat)

    assert event is not None
    assert event["type"] == "heartbeat.updated"
    assert event["state_version"] == 2
    assert event["device_name"] == "TEST-DEVICE"
    assert event["source_provider"] == "qlcp"
    assert event["source_key"] == "TEST-DEVICE"
    assert event["connection_key"] == "conn-a"
    assert event["heartbeat"] == {
        "state": "ok",
        "consecutive_misses": 0,
    }


def test_old_heartbeat_does_not_emit_an_event_after_source_reconnects() -> None:
    state, tracker, qlcp, events = _make_state()
    old_device = _make_device(connection_key="conn-a")
    qlcp.register_device(old_device)
    heartbeat = _mark_sent(tracker, old_device, packet_type=PacketType.HEARTBEAT)
    qlcp.register_device(_make_device(connection_key="conn-b"))
    version = state.state_version
    events.clear()

    assert qlcp.record_command_acked(heartbeat) is None
    assert events == []
    assert state.state_version == version


def test_estop_commands_are_operator_visible_without_pending_ack() -> None:
    state, tracker, qlcp, _events = _make_state()
    device = _make_device()
    qlcp.register_device(device)
    estop = _mark_sent(tracker, device, packet_type=PacketType.ESTOP)

    sent_event = qlcp.record_command_sent(estop)
    snapshot = state.snapshot()
    expired = tracker.expire_pending(now=25.0, timeout_s=10.0)

    assert estop.ack_expected is False
    assert sent_event is not None
    assert sent_event["type"] == "command.sent"
    payload = cast(dict[str, object], sent_event["command"])
    assert payload["packet_type"] == "ESTOP"
    assert payload["ack_expected"] is False
    assert snapshot["commands"]["pending"] == []
    assert snapshot["commands"]["recent"][0]["command_id"] == estop.command_id
    assert snapshot["commands"]["recent"][0]["packet_type"] == "ESTOP"
    assert snapshot["commands"]["recent"][0]["ack_expected"] is False
    assert expired == []


def test_status_request_commands_are_not_operator_visible() -> None:
    state, tracker, qlcp, _events = _make_state()
    device = _make_device()
    qlcp.register_device(device)
    status_request = _mark_sent(tracker, device, packet_type=PacketType.STATUS_REQUEST)

    sent_event = qlcp.record_command_sent(status_request)
    pending_snapshot = state.snapshot()
    tracker.mark_acked(device.connection_key, PacketType.STATUS_REQUEST, 12, now=11.0)
    acked_event = qlcp.record_command_acked(status_request)
    completed_snapshot = state.snapshot()

    assert sent_event is None
    assert acked_event is None
    assert status_request.ack_expected is False
    assert state.state_version == 1
    assert tracker.pending == ()
    assert pending_snapshot["commands"]["pending"] == []
    assert completed_snapshot["commands"]["recent"] == []


def test_snapshot_serializes_to_dict() -> None:
    state, _, qlcp, _events = _make_state()
    device = _make_device()

    qlcp.register_device(device)
    snapshot_dict = state.snapshot()

    assert snapshot_dict["devices"][0]["name"] == "TEST-DEVICE"
    assert snapshot_dict["state_version"] == 1
    assert snapshot_dict["commands"] == {"pending": [], "recent": []}


# ---------------------------------------------------------------------------
# Kasa device state
# ---------------------------------------------------------------------------


def test_register_kasa_device_appears_in_snapshot() -> None:
    state, _, _qlcp, events = _make_state()

    _register_kasa(state.core, "192.168.1.1", "My Plug", "HS110", True)
    event = events[-1] if events else None
    snapshot = state.snapshot()

    assert event["type"] == "kasa.registered"
    assert event["state_version"] == 1
    assert len(snapshot["kasa"]) == 1
    kasa = snapshot["kasa"][0]
    assert kasa["source_provider"] == "kasa"
    assert kasa["source_key"] == "192.168.1.1"
    assert kasa["connection_key"] == state.core.source("kasa", "192.168.1.1").connection_key
    assert event["kasa"] == kasa
    assert kasa["host"] == "192.168.1.1"
    assert kasa["alias"] == "My Plug"
    assert kasa["model"] == "HS110"
    assert kasa["active"] is True
    assert kasa["connected"] is True


def test_register_kasa_device_upserts_existing_host() -> None:
    state, _, _qlcp, _events = _make_state()

    _register_kasa(state.core, "192.168.1.1", "Old Alias", "HS110", False)
    _register_kasa(state.core, "192.168.1.1", "New Alias", "HS110", True)
    snapshot = state.snapshot()

    assert len(snapshot["kasa"]) == 1
    assert snapshot["kasa"][0]["alias"] == "New Alias"
    assert snapshot["kasa"][0]["active"] is True


def test_same_alias_kasa_sources_keep_identity_through_updates_and_replacement() -> None:
    state, _, _, events = _make_state()
    old = _register_kasa(state.core, "192.168.1.1", "Shared label", "HS110", False)
    other = _register_kasa(state.core, "192.168.1.2", "Shared label", "HS110", False)
    current = _register_kasa(state.core, "192.168.1.1", "Shared label", "HS110", True)
    events.clear()
    old.report_control("power", False)
    old.close()
    assert events == []

    other.report_control("power", True)
    current.close()
    assert [event["type"] for event in events] == ["kasa.updated", "kasa.disconnected"]
    for event, source in zip(events, (other, current), strict=True):
        payload = cast("dict[str, Any]", event["kasa"])
        assert payload["alias"] == "Shared label"
        assert payload["source_provider"] == "kasa"
        assert payload["source_key"] == source.key
        assert payload["connection_key"] == source.connection_key


def test_record_kasa_state_updates_active_flag() -> None:
    state, _, _qlcp, events = _make_state()

    _register_kasa(state.core, "192.168.1.1", "My Plug", "HS110", False)
    _record_kasa(state.core, "192.168.1.1", True)
    event = events[-1] if events else None
    snapshot = state.snapshot()

    assert event is not None
    assert event["type"] == "kasa.updated"
    assert event["state_version"] == 2
    assert snapshot["kasa"][0]["active"] is True
    assert snapshot["kasa"][0]["connected"] is True


def test_record_kasa_state_returns_none_for_unknown_host() -> None:
    state, _, _qlcp, events = _make_state()

    _record_kasa(state.core, "192.168.1.99", True)
    event = events[-1] if events else None

    assert event is None
    assert state.state_version == 0


def test_mark_kasa_unavailable_sets_connected_false() -> None:
    state, _, _qlcp, events = _make_state()

    _register_kasa(state.core, "192.168.1.1", "My Plug", "HS110", True)
    _disconnect_kasa(state.core, "192.168.1.1")
    event = events[-1] if events else None
    snapshot = state.snapshot()

    assert event is not None
    assert event["type"] == "kasa.disconnected"
    assert event["state_version"] == 2
    assert snapshot["kasa"][0]["connected"] is False
    assert snapshot["kasa"][0]["active"] is True  # active state is preserved


def test_mark_kasa_unavailable_returns_none_for_unknown_host() -> None:
    state, _, _qlcp, events = _make_state()

    _disconnect_kasa(state.core, "192.168.1.99")
    event = events[-1] if events else None

    assert event is None
    assert state.state_version == 0


def test_snapshot_kasa_sorted_by_host() -> None:
    state, _, _qlcp, _events = _make_state()

    _register_kasa(state.core, "192.168.1.2", "B", "HS110", True)
    _register_kasa(state.core, "192.168.1.1", "A", "HS110", True)
    snapshot = state.snapshot()

    hosts = [k["host"] for k in snapshot["kasa"]]
    assert hosts == ["192.168.1.1", "192.168.1.2"]


def test_snapshot_includes_kasa_key_even_when_empty() -> None:
    state, _, _qlcp, _events = _make_state()

    snapshot = state.snapshot()

    assert "kasa" in snapshot
    assert snapshot["kasa"] == []


def test_kasa_events_increment_state_version() -> None:
    state, _, _qlcp, _events = _make_state()

    _register_kasa(state.core, "192.168.1.1", "My Plug", "HS110", True)
    _record_kasa(state.core, "192.168.1.1", False)
    _disconnect_kasa(state.core, "192.168.1.1")

    assert state.state_version == 3


def test_set_tare_emits_versioned_event_and_appears_in_snapshot() -> None:
    state, _, _qlcp, events = _make_state()

    state.core.set_tare("PT101", 14.7)
    event = events[-1] if events else None

    assert event == {"type": "tare.updated", "state_version": 1, "sensor_name": "PT101", "offset": 14.7}
    assert state.core.tares().get("PT101", 0.0) == 14.7
    assert state.snapshot()["tares"] == {"PT101": 14.7}


def test_set_tare_replaces_the_previous_offset() -> None:
    state, _, _qlcp, _events = _make_state()

    state.core.set_tare("PT101", 14.7)
    state.core.set_tare("PT101", 3.2)

    assert state.core.tares().get("PT101", 0.0) == 3.2
    assert state.state_version == 2


def test_clear_tare_emits_event_and_removes_the_offset() -> None:
    state, _, _qlcp, events = _make_state()
    state.core.set_tare("PT101", 14.7)

    state.core.clear_tare("PT101")
    event = events[-1] if events else None

    assert event == {"type": "tare.cleared", "state_version": 2, "sensor_name": "PT101"}
    assert state.core.tares().get("PT101", 0.0) == 0.0
    assert state.snapshot()["tares"] == {}


def test_clear_tare_on_an_untared_sensor_is_a_no_op() -> None:
    state, _, _qlcp, _events = _make_state()

    assert state.core.clear_tare("PT101") is False
    assert state.state_version == 0


def test_tares_survive_device_disconnect() -> None:
    """Flight handoff: the replacement device carries the same sensor name and must stay tared."""
    state, _, qlcp, _events = _make_state()
    device = _make_device()
    qlcp.register_device(device)
    state.core.set_tare("TC1", 5.0)

    qlcp.mark_disconnected(device)

    assert state.core.tares().get("TC1", 0.0) == 5.0


def test_snapshot_includes_tares_key_even_when_empty() -> None:
    state, _, _qlcp, _events = _make_state()

    assert state.snapshot()["tares"] == {}


def test_snapshot_tares_are_sorted_by_sensor_name() -> None:
    state, _, _qlcp, _events = _make_state()
    state.core.set_tare("PT201", 1.0)
    state.core.set_tare("PT101", 2.0)

    assert list(state.snapshot()["tares"]) == ["PT101", "PT201"]


def test_core_sensor_provider_appears_in_snapshot_and_recording_schema() -> None:
    from vector.core import SensorDefinition

    state, _, _, events = _make_state()
    sensor = SensorDefinition(name="PAD_RSSI", group="radio", unit="dBm")
    source = state.core.register_source("wireless", "pad", name="Pad antenna", sensors=(sensor,))

    assert state.recording_schema().sensors == (sensor,)
    device = state.snapshot()["devices"][0]
    assert device["sensors"] == [{"id": 0, "name": "PAD_RSSI", "group": "radio", "unit": "dBm"}]
    # No transport liveness signal: never present the source as healthy.
    assert device["last_sync_time"] is None
    assert device["heartbeat"] == {"state": "unknown", "consecutive_misses": 0}
    assert [event["type"] for event in events] == ["device.registered"]

    source.close()

    assert state.recording_schema().sensors == (sensor,)
    assert state.snapshot()["devices"][0]["connected"] is False
    assert [event["type"] for event in events] == ["device.registered", "device.disconnected"]


def test_non_qlcp_source_sharing_a_node_key_does_not_borrow_its_health() -> None:
    from vector.core import SensorDefinition

    state, _, qlcp, _ = _make_state()
    qlcp.register_device(_make_device(name="PAD", connection_key="conn-a"))
    state.core.register_source("wireless", "PAD", connection_key="conn-a", sensors=(SensorDefinition("RSSI"),))

    states = {(device["source_provider"], device["heartbeat"]["state"]) for device in state.snapshot()["devices"]}
    assert states == {("qlcp", "ok"), ("wireless", "unknown")}


def test_kasa_control_has_only_its_existing_recording_column() -> None:
    state, _, _, _ = _make_state()
    _register_kasa(state.core, "192.168.1.1", "Pump", "HS110", True)

    schema = state.recording_schema()

    assert schema.controls == ()
    assert [(entry.host, entry.alias) for entry in schema.kasa] == [("192.168.1.1", "Pump")]
    assert state.control_states() == {}
    assert state.kasa_active() == {"192.168.1.1": True}
    assert len(state.core.source("kasa", "192.168.1.1").controls) == 1


def test_transport_health_is_sampled_live_without_advancing_state_version() -> None:
    state, _, qlcp, _ = _make_state()
    device = _make_device()
    qlcp.register_device(device)

    device.last_sync_time = 123456.0
    device.missed_heartbeat_count = 2
    snapshot = state.snapshot()

    assert snapshot["state_version"] == 1
    assert snapshot["devices"][0]["last_sync_time"] == 123456.0
    assert snapshot["devices"][0]["heartbeat"] == {"state": "missed", "consecutive_misses": 2}


def test_error_report_preserves_last_value_and_emits_one_error_event() -> None:
    from vector.qlcp.enums import ControlConfirmStatus

    state, _, qlcp, events = _make_state()
    device = _make_device()
    qlcp.register_device(device)
    qlcp.record_reported_control_state(device, 0, ControlState.OPEN, now=1.0)
    qlcp.record_reported_control_state(device, 0, None, status=ControlConfirmStatus.ERROR, now=2.0)

    control = state.snapshot()["devices"][0]["controls"][0]
    assert control["reported_state"] == "OPEN"
    assert control["reported_status"] == "error"
    assert control["reported_timestamp"] == 2.0
    assert [event["type"] for event in events] == ["device.registered", "control.updated", "control.error"]
