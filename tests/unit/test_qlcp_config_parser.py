import pytest

from vector.qlcp.config_models import DeviceConfig, SensorConfig
from vector.qlcp.config_parser import QLCPConfigError, parse_config
from vector.qlcp.enums import ControlState, ControlType


TEST_CONFIG_VALID_FULL = {
    "device_name": "TEST-DEVICE-1",
    "device_type": "Sensor Monitor",
    "sensors": {
        "thermocouple": {
            "TC1": {
                "sensor_index": "TC1",
                "type": "K",
                "unit": "C",
            },
        },
        "pressure_transducer": {
            "PT1": {
                "sensor_index": "PT1",
                "resistor_ohms": 350,
                "max_pressure_PSI": 500,
                "unit": "PSI",
            },
            "PT2": {
                "sensor_index": "PT2",
                "resistor_ohms": 400,
                "max_pressure_PSI": 600,
                "unit": "PSI",
            },
        },
        "load_cell": {
            "LC1": {
                "sensor_index": "LC1",
                "load_rating_N": 1000,
                "excitation_V": 5.0,
                "sensitivity_vV": 2.0,
                "unit": "N",
            },
        },
        "resistance_sensor": {
            "RS1": {
                "sensor_index": "RS1",
                "injected_current_uA": 1000,
                "r_short": 50,
                "unit": "ohms",
            },
        },
        "current_sensor": {
            "CS1": {
                "sensor_index": "CS1",
                "shunt_resistor_ohms": 0.1,
                "csa_gain": 50,
                "unit": "A",
            },
        },
    },
    "controls": {
        "valve": {
            "VALVE1": {
                "control_index": "VALVE1",
                "type": "BOOL",
                "default_state": "OPEN",
            },
            "VALVE2": {
                "control_index": "VALVE2",
                "type": "BOOL",
                "default_state": "CLOSED",
            },
        },
    },
}


def test_parse_config_valid_full() -> None:
    result = parse_config(TEST_CONFIG_VALID_FULL)

    assert result.name == "TEST-DEVICE-1"

    assert len(result.sensors_by_id) == 6

    s = result.sensors_by_id[0]
    assert isinstance(s, SensorConfig)
    assert s.name == "TC1"
    assert s.group == "thermocouple"
    assert s.unit == "C"

    s = result.sensors_by_id[1]
    assert isinstance(s, SensorConfig)
    assert s.name == "PT1"
    assert s.group == "pressure_transducer"
    assert s.unit == "PSI"

    s = result.sensors_by_id[2]
    assert isinstance(s, SensorConfig)
    assert s.name == "PT2"
    assert s.group == "pressure_transducer"
    assert s.unit == "PSI"

    s = result.sensors_by_id[3]
    assert isinstance(s, SensorConfig)
    assert s.name == "LC1"
    assert s.group == "load_cell"
    assert s.unit == "N"

    s = result.sensors_by_id[4]
    assert isinstance(s, SensorConfig)
    assert s.name == "RS1"
    assert s.group == "resistance_sensor"
    assert s.unit == "ohms"

    s = result.sensors_by_id[5]
    assert isinstance(s, SensorConfig)
    assert s.name == "CS1"
    assert s.group == "current_sensor"
    assert s.unit == "A"

    assert len(result.controls_by_id) == 2
    assert result.controls_by_id[0].name == "VALVE1"
    assert result.controls_by_id[0].group == "valve"
    assert result.controls_by_id[0].type == ControlType.BOOL
    assert result.controls_by_id[0].default == ControlState.OPEN
    assert result.controls_by_id[1].name == "VALVE2"
    assert result.controls_by_id[1].group == "valve"
    assert result.controls_by_id[1].type == ControlType.BOOL
    assert result.controls_by_id[1].default == ControlState.CLOSED

TEST_CONFIG_SENSORS_ONLY = {
    "device_name": "TEST-DEVICE-2",
    "sensors": {
        "thermocouple": {
            "TC1": {
                "sensor_index": "TC1",
                "unit": "C",
            },
        },
        "pressure_transducer": {
            "PT1": {
                "sensor_index": "PT1",
                "unit": "PSI",
            },
        },
    },
}


def test_parse_config_sensors_only() -> None:
    result = parse_config(TEST_CONFIG_SENSORS_ONLY)

    assert result.name == "TEST-DEVICE-2"

    assert len(result.sensors_by_id) == 2

    s = result.sensors_by_id[0]
    assert isinstance(s, SensorConfig)
    assert s.name == "TC1"
    assert s.group == "thermocouple"
    assert s.unit == "C"

    s = result.sensors_by_id[1]
    assert isinstance(s, SensorConfig)
    assert s.name == "PT1"
    assert s.group == "pressure_transducer"
    assert s.unit == "PSI"

    assert len(result.controls_by_id) == 0


def test_parse_config_known_sensor_types_only_require_common_fields() -> None:
    config = {
        "device_name": "TEST-DEVICE-MINIMAL",
        "sensors": {
            "thermocouple": {"TC1": {"unit": "C"}},
            "pressure_transducer": {"PT1": {"unit": "PSI"}},
            "load_cell": {"LC1": {"unit": "N"}},
            "resistance_sensor": {"RS1": {"unit": "ohms"}},
            "current_sensor": {"CS1": {"unit": "A"}},
        },
    }

    result = parse_config(config)

    assert [type(sensor) for sensor in result.sensors_by_id.values()] == [SensorConfig] * 5
    assert [sensor.group for sensor in result.sensors_by_id.values()] == [
        "thermocouple",
        "pressure_transducer",
        "load_cell",
        "resistance_sensor",
        "current_sensor",
    ]
    assert [sensor.unit for sensor in result.sensors_by_id.values()] == [
        "C",
        "PSI",
        "N",
        "ohms",
        "A",
    ]



TEST_CONFIG_CONTROLS_ONLY = {
    "device_name": "TEST-DEVICE-3",
    "controls": {
        "valve": {
            "VALVE1": {"control_index": "PT202", "type": "BOOL", "default_state": "OPEN"},
        },
    },
}


def test_parse_config_controls_only() -> None:
    result = parse_config(TEST_CONFIG_CONTROLS_ONLY)

    assert result.name == "TEST-DEVICE-3"

    assert len(result.sensors_by_id) == 0

    assert len(result.controls_by_id) == 1
    assert result.controls_by_id[0].name == "VALVE1"
    assert result.controls_by_id[0].group == "valve"
    assert result.controls_by_id[0].default == ControlState.OPEN
    assert result.controls_by_id[0].type == ControlType.BOOL


TETS_CONFIG_DUPLICATE_SENSOR_NAMES = {
    "device_name": "TEST-DEVICE-4",
    "sensors": {
        "thermocouple": {
            "SENSOR1": {"sensor_index": "TC101", "type": "K", "unit": "C"},
        },
        "pressure_transducer": {
            "SENSOR1": {
                "sensor_index": "PT101",
                "resistor_ohms": 350,
                "max_pressure_PSI": 500,
                "unit": "PSI",
            },
        },
    },
}


def test_parse_config_duplicate_sensor_names() -> None:
    result = parse_config(TETS_CONFIG_DUPLICATE_SENSOR_NAMES)

    assert result.name == "TEST-DEVICE-4"

    assert len(result.sensors_by_id) == 2
    assert result.sensors_by_id[0].name == "SENSOR1"
    assert result.sensors_by_id[0].group == "thermocouple"
    assert result.sensors_by_id[0].unit == "C"

    assert result.sensors_by_id[1].name == "SENSOR1"
    assert result.sensors_by_id[1].group == "pressure_transducer"
    assert result.sensors_by_id[1].unit == "PSI"

    assert len(result.controls_by_id) == 0


TEST_CONFIG_UNKNOWN_SENSOR = {
    "device_name": "TEST-DEVICE-5",
    "sensors": {
        "future_sensor": {
            "FS1": {"sensor_index": "FS1", "unit": "unitless"},
        },
    },
}


def test_parse_config_unknown_sensor_falls_back_to_generic_config() -> None:
    result = parse_config(TEST_CONFIG_UNKNOWN_SENSOR)

    assert len(result.sensors_by_id) == 1
    assert type(result.sensors_by_id[0]) is SensorConfig
    assert result.sensors_by_id[0].name == "FS1"
    assert result.sensors_by_id[0].group == "future_sensor"
    assert result.sensors_by_id[0].unit == "unitless"


@pytest.mark.parametrize("missing_field", ["type", "default_state"])
def test_parse_config_missing_control_field_raises(missing_field: str) -> None:
    control_details = {
        "type": "BOOL",
        "default_state": "CLOSED",
    }
    del control_details[missing_field]

    config = {
        "device_name": "TEST-DEVICE-6",
        "controls": { "valve": { "VALVE1": control_details }},
    }

    with pytest.raises(
        QLCPConfigError,
        match=f"control 'VALVE1' missing required field: {missing_field}",
    ):
        parse_config(config)


@pytest.mark.parametrize("missing_field", ["unit"])
def test_parse_config_missing_sensor_field_raises(missing_field: str) -> None:
    sensor_details = {
        "type": "K",
        "unit": "C",
    }
    del sensor_details[missing_field]

    config = {
        "device_name": "TEST-DEVICE-7",
        "sensors": {"thermocouple": {"TC1": sensor_details}},
    }

    with pytest.raises(
        QLCPConfigError,
        match=f"thermocouple sensor 'TC1' missing required field: {missing_field}",
    ):
        parse_config(config)


@pytest.mark.parametrize("ids", [(1,), (0, 2)], ids=["not-from-zero", "gap"])
def test_device_config_rejects_ids_that_are_not_declaration_ordinals(ids: tuple[int, ...]) -> None:
    sensors = {i: SensorConfig(id=i, name=f"S{i}", group="pressure", unit="psi") for i in ids}
    with pytest.raises(ValueError, match="contiguous"):
        DeviceConfig(name="PANDA", sensors_by_id=sensors, controls_by_id={})
    with pytest.raises(ValueError, match="contiguous"):
        DeviceConfig(name="PANDA", sensors_by_id={0: SensorConfig(id=1, name="S", group="pressure", unit="psi")}, controls_by_id={})
