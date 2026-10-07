import logging
import math
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from vector.api.deps import get_runtime
from vector.core import (
    TARE_DEFAULT_SAMPLES,
    TARE_SAMPLE_CAPACITY,
    TareCaptureError,
)
from vector.runtime.services import RuntimeServices


logger = logging.getLogger(__name__)
router = APIRouter(tags=["tares"])


class TareRequest(BaseModel):
    sensor_name: str = Field(min_length=1)
    # Only needed while more than one connected device reports this sensor name.
    device_name: str | None = None
    samples: int = Field(default=TARE_DEFAULT_SAMPLES, ge=1, le=TARE_SAMPLE_CAPACITY)
    # Skips capture and sets the offset directly, e.g. to restore a known tare.
    # Finiteness is checked in the handler rather than with allow_inf_nan: Pydantic's
    # rejection embeds the offending inf in the 422 body, which FastAPI then cannot encode.
    offset: float | None = None


class TareInfo(BaseModel):
    sensor_name: str
    offset: float
    sampled_device: str | None = None
    sample_count: int = 0
    applies_to: list[str] = []


def _devices_with_sensor(rt: RuntimeServices, sensor_name: str) -> list[str]:
    """Return connected devices carrying *sensor_name*. Empty is valid: tares may be pre-staged."""
    return sorted({binding.source.name for binding in rt.core.sensors(sensor_name) if binding.source.connected})


@router.get("/v1/tares", summary="Get the tare offset currently applied to each sensor")
async def get_tares(rt: Annotated[RuntimeServices, Depends(get_runtime)]) -> dict[str, float]:
    return rt.core.tares()


@router.post("/v1/tares", summary="Tare a sensor by name across every device reporting it")
async def set_tare(
    body: TareRequest,
    rt: Annotated[RuntimeServices, Depends(get_runtime)],
) -> TareInfo:
    sampled_device: str | None = None
    sample_count = 0

    if body.offset is not None:
        # An inf or nan offset would poison every subsequent reading for this sensor.
        if not math.isfinite(body.offset):
            raise HTTPException(400, "offset must be a finite number.")
        offset = body.offset
    else:
        try:
            offset, sampled_device, sample_count = rt.core.capture_tare_offset(
                body.sensor_name,
                device_name=body.device_name,
                samples=body.samples,
            )
        except TareCaptureError as exc:
            raise HTTPException(409, str(exc)) from None

    rt.core.set_tare(body.sensor_name, offset)
    logger.info("User tared sensor %s to offset %s (sampled %s readings from %s)", body.sensor_name, offset, sample_count, sampled_device)

    return TareInfo(
        sensor_name=body.sensor_name,
        offset=offset,
        sampled_device=sampled_device,
        sample_count=sample_count,
        applies_to=_devices_with_sensor(rt, body.sensor_name),
    )


@router.delete("/v1/tares", summary="Clear a sensor's tare offset")
async def clear_tare(
    rt: Annotated[RuntimeServices, Depends(get_runtime)],
    # A query parameter rather than a path segment: sensor names come from device CONFIG
    # JSON keys and may contain characters that do not survive a path.
    sensor_name: Annotated[str, Query(min_length=1)],
) -> TareInfo:
    if rt.core.clear_tare(sensor_name):
        logger.info("User cleared tare for sensor %s", sensor_name)
    else:
        logger.info("User cleared tare for %s, which was not tared", sensor_name)

    return TareInfo(
        sensor_name=sensor_name,
        offset=0.0,
        applies_to=_devices_with_sensor(rt, sensor_name),
    )
