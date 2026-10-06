"""One control endpoint over the core gate, for every provider."""

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from vector.api.deps import get_runtime
from vector.core import ControlDispatchError, ControlValidationError
from vector.runtime.services import RuntimeServices


logger = logging.getLogger(__name__)
router = APIRouter(tags=["controls"])


class ControlRequest(BaseModel):
    # A source is addressed as the state snapshot names it: "<source_provider>:<source_key>".
    source: str = Field(pattern=r"^[^:]+:.+$", examples=["qlcp:PANDA"])
    control: str = Field(min_length=1)
    # JSON true/false for BOOL, an integer for UINT32 and INT32, a number for FLOAT32.
    value: bool | int | float


class ControlResult(BaseModel):
    source: str
    control: str
    submitted: bool
    command_id: int | None = None


@router.post("/v1/control", summary="Set one control on one source through the shared core")
async def set_control(body: ControlRequest, rt: Annotated[RuntimeServices, Depends(get_runtime)]) -> ControlResult:
    provider, _, key = body.source.partition(":")
    source = rt.core.source(provider, key)
    target = source.control(body.control) if source is not None else None
    if source is None or target is None:
        raise HTTPException(404, f"No control {body.control!r} on source {body.source!r}.")
    if not source.connected:
        raise HTTPException(409, f"Source {body.source!r} is disconnected.")

    try:
        command_id = await rt.core.set_control(target, body.value)
    except ControlValidationError as exc:
        raise HTTPException(400, str(exc)) from None
    except ControlDispatchError as exc:
        logger.warning("Control %s on %s was not submitted: %s", target.name, body.source, exc)
        raise HTTPException(502, str(exc)) from None

    logger.info("User set %s on %s to %r (command %s)", target.name, body.source, body.value, command_id)
    return ControlResult(source=body.source, control=target.name, submitted=True, command_id=command_id)
