"""An ordinary sensor provider reaches every consumer without a control node."""

from __future__ import annotations
import asyncio
import csv
from typing import TYPE_CHECKING, Any, cast

import orjson
import pytest

from vector.core import Core, SensorDefinition
from vector.runtime.session_telemetry import SessionTelemetryWriter, TelemetrySessionPublisher, build_columns
from vector.runtime.telemetry_display_stream import TelemetryDisplayStream
from vector.runtime.telemetry_stream import TelemetryStreamRuntime
from vector.state.system_state import SystemState


if TYPE_CHECKING:
    from pathlib import Path

    from fastapi import WebSocket


class _Socket:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    async def send_text(self, message: str) -> None:
        self.sent.append(orjson.loads(message))

    async def accept(self) -> None:
        pass

    async def close(self) -> None:
        pass


def test_sensor_provider_reaches_raw_display_tare_and_recording(tmp_path: Path) -> None:
    async def run() -> None:
        core = Core()
        state = SystemState(core=core)
        source = core.register_source(
            "wireless", "pad", name="Pad antenna",
            sensors=(SensorDefinition("PAD_RSSI", group="radio", unit="dBm"),),
        )
        raw = TelemetryStreamRuntime()
        display = TelemetryDisplayStream(target_hz=1.0)
        raw_socket = _Socket()
        display_socket = _Socket()
        clients = [
            asyncio.create_task(raw.handle_client(cast("WebSocket", raw_socket))),
            asyncio.create_task(display.handle_client(cast("WebSocket", display_socket))),
        ]
        await asyncio.sleep(0)
        writer = SessionTelemetryWriter.open(tmp_path / "telemetry.csv", state, build_columns(state.recording_schema()))
        recording = TelemetrySessionPublisher()
        recording.attach(writer)
        core.subscribe_samples(raw.publish_batch)
        core.subscribe_samples(display.publish_batch)
        core.subscribe_samples(recording.publish_batch)

        source.publish_samples([("PAD_RSSI", -62.0)], 1.1)
        source.publish_samples([("PAD_RSSI", -60.0)], 1.2)
        assert core.capture_tare_offset("PAD_RSSI", samples=2) == (-61.0, "Pad antenna", 2)
        core.set_tare("PAD_RSSI", -61.0)
        source.publish_samples([("PAD_RSSI", -59.0)], 2.1)
        latest = source.publish_samples([("PAD_RSSI", -58.0)], 3.1)

        assert latest is not None
        assert latest.readings[0].value == 3.0
        assert latest.readings[0].tare == -61.0
        assert latest.timestamp_s == 3.1
        assert state.core.tares() == {"PAD_RSSI": -61.0}
        assert writer.rows == 4

        await asyncio.sleep(0)
        raw_messages = raw_socket.sent
        assert len(raw_messages) == 4
        assert raw_messages[2]["readings"] == [{
            "sensor_id": 0, "sensor_name": "PAD_RSSI", "value": 2.0,
            "tare": -61.0, "unit": "dBm", "sensor_type": "radio",
        }]
        assert raw_messages[2]["device_name"] == "Pad antenna"
        assert raw_messages[2]["timestamp_source"] == "server_receive"
        assert raw_messages[2]["timestamp_synced"] is False
        untared_bucket, tared_bucket = display_socket.sent
        assert untared_bucket["readings"][0]["points"] == [{"t": 1.1, "v": -62.0}, {"t": 1.2, "v": -60.0}]
        assert tared_bucket["readings"][0]["points"] == [{"t": 2.1, "v": 2.0}]

        source.close()
        assert not source.connected
        assert source.publish_samples([("PAD_RSSI", 99.0)], 4.1) is None
        assert writer.rows == 4
        recording.detach()
        writer.close()
        for client in clients:
            client.cancel()
        await asyncio.gather(*clients, return_exceptions=True)

    asyncio.run(run())
    assert (tmp_path / "telemetry.csv").read_text() == (
        "device_timestamp,source,PAD_RSSI [dBm],source_provider,source_key\n"
        "1.1000,Pad antenna,-62.0000,wireless,pad\n"
        "1.2000,Pad antenna,-60.0000,wireless,pad\n"
        "2.1000,Pad antenna,2.0000,wireless,pad\n"
        "3.1000,Pad antenna,3.0000,wireless,pad\n"
    )


def test_new_provider_preserves_frozen_recording_and_uses_late_file(tmp_path: Path) -> None:
    core = Core()
    state = SystemState(core=core)
    antenna = core.register_source("wireless", "pad", name="Pad", sensors=(SensorDefinition("PAD_RSSI", unit="dBm"),))
    writer = SessionTelemetryWriter.open(tmp_path / "telemetry.csv", state, build_columns(state.recording_schema()))
    core.subscribe_samples(writer.write_batch)
    antenna.publish_samples([("PAD_RSSI", -62.0)], 1.0)

    latency = core.register_source("poller", "latency", name="Latency", sensors=(SensorDefinition("PAD_LATENCY", unit="ms"),))
    latency.publish_samples([("PAD_LATENCY", 1.5)], 2.0)
    latency.close()
    assert {sensor.name for sensor in state.recording_schema().sensors} == {"PAD_RSSI", "PAD_LATENCY"}
    assert writer.rows == 1
    assert writer.late_files == ("telemetry_late.csv",)
    writer.close()

    assert (tmp_path / "telemetry.csv").read_text() == (
        "device_timestamp,source,PAD_RSSI [dBm],source_provider,source_key\n1.0000,Pad,-62.0000,wireless,pad\n"
    )
    assert (tmp_path / "telemetry_late.csv").read_text() == (
        "device_timestamp,source,PAD_LATENCY [ms],PAD_RSSI [dBm],source_provider,source_key\n"
        "2.0000,Latency,1.5000,,poller,latency\n"
    )


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


@pytest.mark.parametrize("identities", [(("a", "sensor"), ("b", "sensor")), (("a", "first"), ("a", "second"))])
def test_recorded_rows_distinguish_sources_with_the_same_name(
    tmp_path: Path, identities: tuple[tuple[str, str], tuple[str, str]],
) -> None:
    core = Core()
    state = SystemState(core=core)
    sources = [core.register_source(*identity, name="Shared", sensors=(SensorDefinition("VALUE"),)) for identity in identities]
    writer = SessionTelemetryWriter.open(tmp_path / "telemetry.csv", state, build_columns(state.recording_schema()))
    core.subscribe_samples(writer.write_batch)
    for index, source in enumerate(sources, start=1):
        source.publish_samples([("VALUE", index)], index)
    writer.close()

    assert writer.late_files == ()
    assert _csv_rows(tmp_path / "telemetry.csv") == [
        {
            "device_timestamp": f"{index:.4f}", "source": "Shared", "VALUE": f"{index:.4f}",
            "source_provider": provider, "source_key": key,
        }
        for index, (provider, key) in enumerate(identities, start=1)
    ]


@pytest.mark.parametrize("identities", [(("a", "sensor"), ("b", "sensor")), (("a", "first"), ("a", "second"))])
def test_late_sources_with_the_same_name_keep_every_reading(
    tmp_path: Path, identities: tuple[tuple[str, str], tuple[str, str]],
) -> None:
    core = Core()
    state = SystemState(core=core)
    initial = core.register_source("initial", "device", sensors=(SensorDefinition("BASE"),))
    writer = SessionTelemetryWriter.open(tmp_path / "telemetry.csv", state, build_columns(state.recording_schema()))
    core.subscribe_samples(writer.write_batch)
    initial.publish_samples([("BASE", 1.0)], 1.0)
    first = core.register_source(*identities[0], name="Shared", sensors=(SensorDefinition("FIRST"),))
    first.publish_samples([("FIRST", 11.0)], 2.0)
    second = core.register_source(*identities[1], name="Shared", sensors=(SensorDefinition("SECOND"),))
    second.publish_samples([("SECOND", 21.0)], 3.0)
    first.publish_samples([("FIRST", 12.0)], 4.0)
    second.publish_samples([("SECOND", 22.0)], 5.0)
    writer.close()

    assert writer.rows == 1
    assert writer.late_files == ("telemetry_late.csv", "telemetry_late_2.csv")
    for filename, identity, sensor_name, expected in zip(
        writer.late_files, identities, ("FIRST", "SECOND"), (["11.0000", "12.0000"], ["21.0000", "22.0000"]), strict=True,
    ):
        rows = _csv_rows(tmp_path / filename)
        assert [row[sensor_name] for row in rows] == expected
        assert {(row["source_provider"], row["source_key"]) for row in rows} == {identity}
        assert {row["source"] for row in rows} == {"Shared"}


def test_late_source_rename_and_repeated_schema_changes_reuse_only_compatible_files(tmp_path: Path) -> None:
    core = Core()
    state = SystemState(core=core)
    initial = core.register_source("initial", "device", sensors=(SensorDefinition("BASE"),))
    writer = SessionTelemetryWriter.open(tmp_path / "telemetry.csv", state, build_columns(state.recording_schema()))
    core.subscribe_samples(writer.write_batch)
    initial.publish_samples([("BASE", 1.0)], 1.0)

    source = core.register_source("poller", "link", name="Original", sensors=(SensorDefinition("A"),))
    source.publish_samples([("A", 10.0)], 2.0)
    source = core.register_source("poller", "link", name="Renamed", sensors=(SensorDefinition("A"),))
    source.publish_samples([("A", 11.0)], 3.0)
    assert writer.late_files == ("telemetry_late.csv",)

    source = core.register_source("poller", "link", name="Renamed", sensors=(SensorDefinition("A"), SensorDefinition("B")))
    source.publish_samples([("A", 12.0), ("B", 20.0)], 4.0)
    source = core.register_source("poller", "link", name="Renamed", sensors=(SensorDefinition("B"), SensorDefinition("C")))
    source.publish_samples([("B", 21.0), ("C", 30.0)], 5.0)
    source.publish_samples([("B", 22.0)], 6.0)
    # Removing a declaration does not delete its earlier file; a later registration
    # can still write into that compatible schema without overwriting earlier rows.
    source = core.register_source("poller", "link", name="Restored", sensors=(SensorDefinition("A"),))
    source.publish_samples([("A", 13.0)], 7.0)
    writer.flush_if_dirty()

    assert writer.late_files == ("telemetry_late.csv", "telemetry_late_2.csv", "telemetry_late_3.csv")
    rows = [_csv_rows(tmp_path / filename) for filename in writer.late_files]
    assert [[row["device_timestamp"] for row in file_rows] for file_rows in rows] == [
        ["2.0000", "3.0000"], ["4.0000", "7.0000"], ["5.0000", "6.0000"],
    ]
    assert [(row["source"], row["A"]) for row in rows[0]] == [("Original", "10.0000"), ("Renamed", "11.0000")]
    assert [(row["A"], row["B"]) for row in rows[1]] == [("12.0000", "20.0000"), ("13.0000", "")]
    assert [(row["B"], row["C"]) for row in rows[2]] == [("21.0000", "30.0000"), ("22.0000", "")]
    assert {(row["source_provider"], row["source_key"]) for file_rows in rows for row in file_rows} == {("poller", "link")}
    writer.close()
    assert [_csv_rows(tmp_path / filename) for filename in writer.late_files] == rows
