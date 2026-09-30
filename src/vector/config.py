from typing import TypedDict, cast

import yaml  # type: ignore[import-untyped]


class AccountServiceConfig(TypedDict):
    username: str
    password: str

class CameraConfig(TypedDict):
    ip: str
    onvif_port: int

class MediaMTXConfig(TypedDict):
    ip: str
    api_port: int

class MumbleConfig(TypedDict):
    ip: str
    port: int
    password: str
    temp_recording_dir: str

class RecordingsConfig(TypedDict):
    root: str
    mediamtx_container_root: str


class DiscoveryConfig(TypedDict, total=False):
    enabled: bool
    onvif_port: int
    timeout: float
    max_retries: int


class ServicesConfig(TypedDict):
    mediamtx: MediaMTXConfig
    mumble: MumbleConfig
    recordings: RecordingsConfig
    discovery: DiscoveryConfig

class ServerConfig(TypedDict):
    accounts: dict[str, AccountServiceConfig]
    cameras: list[CameraConfig]
    services: ServicesConfig

RECORDINGS_DEFAULTS: RecordingsConfig = {
    "root": "./recordings",
    # Must match the media service's bind mount in the compose files.
    "mediamtx_container_root": "/recordings",
}

DISCOVERY_DEFAULTS: DiscoveryConfig = {
    "enabled": True,
    "onvif_port": 2020,
    "timeout": 5.0,
    "max_retries": 3,
}

def load_config(config_path: str) -> ServerConfig:
    try:
        with open(config_path, "r") as file:
            loaded = yaml.safe_load(file.read())
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Configuration file not found: {config_path}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"Failed to parse YAML configuration file '{config_path}': {exc}") from exc

    if not isinstance(loaded, dict):
        # An empty or scalar config file otherwise fails much later, as an AttributeError
        # on None somewhere in startup.
        raise ValueError(f"Configuration file '{config_path}' must contain a YAML mapping")

    config = cast("ServerConfig", loaded)
    services = config.setdefault("services", {})  # type: ignore[typeddict-item]
    services["recordings"] = {**RECORDINGS_DEFAULTS, **services.get("recordings", {})}
    services["discovery"] = {**DISCOVERY_DEFAULTS, **services.get("discovery", {})}
    return config
