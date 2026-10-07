import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from vector.api.deps import get_runtime
from vector.api.models import CommandResponse
from vector.runtime.qlcp_state import StreamSetting
from vector.runtime.services import RuntimeServices


logger = logging.getLogger(__name__)
router = APIRouter(tags=["devices"])


class StreamRequest(BaseModel):
    """A partial update; a field left out keeps its current value."""

    enabled: bool | None = None
    frequency_hz: int | None = Field(default=None, ge=1, le=65535)


class StreamInfo(BaseModel):
    enabled: bool
    frequency_hz: int


class StreamApplied(StreamInfo):
    # Nodes the setting reached just now; later arrivals receive it at registration.
    applied_to: list[str]


class AutoDiscoveryConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    enabled: bool
    interval_seconds: float = Field(alias="intervalSeconds")


@router.get("/v1/stream", summary="Get the stand-wide DATA stream setting")
async def get_stream(rt: Annotated[RuntimeServices, Depends(get_runtime)]) -> StreamInfo:
    setting = rt.esp_runtime.state_adapter.stream
    return StreamInfo(enabled=setting.enabled, frequency_hz=setting.frequency_hz)


@router.post("/v1/stream", summary="Set the DATA stream rate for every node, now and as nodes connect")
async def set_stream(body: StreamRequest, rt: Annotated[RuntimeServices, Depends(get_runtime)]) -> StreamApplied:
    current = rt.esp_runtime.state_adapter.stream
    setting = StreamSetting(
        enabled=current.enabled if body.enabled is None else body.enabled,
        frequency_hz=current.frequency_hz if body.frequency_hz is None else body.frequency_hz,
    )
    logger.info("User set stream: enabled=%s, frequency_hz=%s", setting.enabled, setting.frequency_hz)
    applied_to = await rt.esp_runtime.set_stream(setting)
    return StreamApplied(enabled=setting.enabled, frequency_hz=setting.frequency_hz, applied_to=applied_to)


@router.get("/v1/autodiscovery", summary="Get autodiscovery settings")
async def get_autodiscovery_settings(rt: Annotated[RuntimeServices, Depends(get_runtime)]) -> AutoDiscoveryConfig:
    ds = rt.discovery_service
    return AutoDiscoveryConfig(
        enabled=ds.periodic_enabled,
        intervalSeconds=ds.periodic_interval_s,
    )


@router.post("/v1/autodiscovery", summary="Update autodiscovery settings")
async def update_autodiscovery_settings(
    rt: Annotated[RuntimeServices, Depends(get_runtime)],
    enabled: bool | None = None,
    interval_seconds: Annotated[float | None, Query(alias="intervalSeconds")] = None,
) -> AutoDiscoveryConfig:
    if interval_seconds is not None and interval_seconds <= 0:
        raise HTTPException(400, "intervalSeconds must be greater than 0")

    ds = rt.discovery_service

    if enabled is not None:
        ds.periodic_enabled = enabled

    if interval_seconds is not None:
        ds.periodic_interval_s = interval_seconds

    logger.info("User updated autodiscovery: enabled=%s, intervalSeconds=%ss", ds.periodic_enabled, ds.periodic_interval_s)

    return AutoDiscoveryConfig(
        enabled=ds.periodic_enabled,
        intervalSeconds=ds.periodic_interval_s,
    )


@router.post("/v1/discover", summary="Discover new devices on every provider")
async def discover_devices(rt: Annotated[RuntimeServices, Depends(get_runtime)]) -> CommandResponse:
    logger.info("User sent device discover command")
    rt.discovery_service.discover()
    return CommandResponse(
        status="sent",
        message="Discovery started. Nodes will auto-connect; plugs register as they answer.",
    )


@router.post("/v1/estop", summary="Emergency stop - stops all streaming and control commands immediately")
async def emergency_stop(rt: Annotated[RuntimeServices, Depends(get_runtime)]) -> CommandResponse:
    devices = rt.esp_runtime.get_registered_devices()
    targeted = [device.name for device in devices.values()]
    if not targeted:
        raise HTTPException(400, "No valid target devices for the command")

    sent = [device.name for device in devices.values() if await rt.esp_runtime.emergency_stop(device)]
    if not sent:
        logger.error("ESTOP failed to send to all target devices: %s", ", ".join(targeted))
        raise HTTPException(502, f"ESTOP failed to send to all target devices: {', '.join(targeted)}.")

    if len(sent) < len(targeted):
        failed = [name for name in targeted if name not in sent]
        logger.error("ESTOP failed to send to: %s", ", ".join(failed))
        return CommandResponse(
            status="partial",
            message=f"ESTOP sent to {', '.join(sent)}; failed for {', '.join(failed)}.",
        )

    return CommandResponse(status="sent", message=f"ESTOP sent to {', '.join(sent)}.")


@router.post(
    "/v1/status-request",
    summary="Request each device report its current control states via a STATUS packet",
)
async def request_status(rt: Annotated[RuntimeServices, Depends(get_runtime)]) -> CommandResponse:
    devices = rt.esp_runtime.get_registered_devices()
    targeted = [device.name for device in devices.values()]
    if not targeted:
        raise HTTPException(400, "No valid target devices for the command")

    sent = [device.name for device in devices.values() if await rt.esp_runtime.get_status(device)]
    if not sent:
        raise HTTPException(502, f"STATUS_REQUEST failed to send to all target devices: {', '.join(targeted)}.")

    if len(sent) < len(targeted):
        failed = [name for name in targeted if name not in sent]
        return CommandResponse(
            status="partial",
            message=f"STATUS_REQUEST sent to {', '.join(sent)}; failed for {', '.join(failed)}.",
        )

    return CommandResponse(status="sent", message=f"STATUS_REQUEST sent to {', '.join(sent)}.")
