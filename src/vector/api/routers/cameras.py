import logging
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends
from pydantic import BaseModel

from vector.api.deps import get_runtime
from vector.api.models import CommandResponse
from vector.runtime.services import RuntimeServices


logger = logging.getLogger(__name__)
router = APIRouter(tags=["cameras"])


class CameraInfo(BaseModel):
    ip: str
    hostname: str
    stream_path: str
    recording: bool


class CameraList(BaseModel):
    cameras: list[CameraInfo]


@router.get("/v1/cameras", summary="Get the list of connected cameras")
async def get_cameras(rt: Annotated[RuntimeServices, Depends(get_runtime)]) -> CameraList:
    return CameraList(cameras=[
        CameraInfo(ip=cam.address, hostname=cam.hostname, stream_path=cam.stream_path, recording=cam.recording)
        for cam in rt.camera_runtime.cameras()
    ])


@router.post("/v1/cameras/reconnect", summary="Reconnect all cameras")
async def reconnect_cameras(rt: Annotated[RuntimeServices, Depends(get_runtime)]) -> CameraList:
    logger.info("User sent camera reconnect")
    await rt.camera_runtime.discover_cameras()
    return CameraList(cameras=[
        CameraInfo(ip=cam.address, hostname=cam.hostname, stream_path=cam.stream_path, recording=cam.recording)
        for cam in rt.camera_runtime.cameras()
    ])


@router.post("/v1/camera", summary="Control a camera's movement")
async def control_camera(
    rt: Annotated[RuntimeServices, Depends(get_runtime)],
    ip: str,
    x_movement: float,
    y_movement: float,
    bg_tasks: BackgroundTasks,
) -> CommandResponse:
    logger.info("User sent camera move command to %s: <%s, %s>", ip, x_movement, y_movement)
    bg_tasks.add_task(rt.camera_runtime.move_camera, ip, x_movement, y_movement)
    return CommandResponse(
        status="sent",
        message=f"User sent camera move command to {ip}: <{x_movement}, {y_movement}>",
    )



