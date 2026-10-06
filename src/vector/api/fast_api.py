import logging
import time

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from vector.api.routers import cameras, controls, devices, sessions, streams, system, tares
from vector.runtime.services import RuntimeServices


logger = logging.getLogger(__name__)


app = FastAPI()


class AccessLogMiddleware(BaseHTTPMiddleware):
    """Middleware to log requests instead of writing access logs to stdout."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        start_time = time.perf_counter()
        response = await call_next(request)
        duration_s = time.perf_counter() - start_time
        duration_ms = duration_s * 1000

        runtime = getattr(request.app.state, "runtime", None)
        metrics = getattr(runtime, "metrics", None)
        if metrics is not None:
            metrics.observe_http_request(request.method, request.url.path, response.status_code, duration_s)

        client = request.client
        if client is None:
            client_host = "unknown"
            client_port = "unknown"
        else:
            client_host = client.host
            client_port = str(client.port)

        logger.info(
            '%s:%s - "%s %s HTTP/1.1" %s (%.0fms)',
            client_host,
            client_port,
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
        )
        return response


app.add_middleware(AccessLogMiddleware)

# Server runs exclusively on propnet and is not publicly available
# CSRF is not a concern here
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(system.router)
app.include_router(streams.router)
app.include_router(devices.router)
app.include_router(controls.router)
app.include_router(cameras.router)
app.include_router(sessions.router)
app.include_router(tares.router)


async def start_api(runtime: RuntimeServices) -> None:
    """Start the FastAPI server."""
    app.state.runtime = runtime
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=8000,
        loop="asyncio",
        log_level="warning",  # Suppress INFO-level access logs
        access_log=False,  # Disable uvicorn access logging (handled by middleware)
    )
    server = uvicorn.Server(config)
    await server.serve()
