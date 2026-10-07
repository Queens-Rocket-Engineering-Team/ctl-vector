import asyncio
import logging
import os

from onvif import ONVIFDiscovery

import vector
from vector import config
from vector.api import fast_api
from vector.daemons.cli_terminal import command_processor
from vector.runtime.logging import configure_logging
from vector.runtime.services import build_runtime


logger = logging.getLogger(__name__)


async def main() -> None:
    """Run the server."""

    # -------
    # INITIALIZATION
    # -------

    # Load server configuration
    config_path = os.getenv("PROP_CONFIG", "./config.yaml")
    server_config = config.load_config(config_path)

    # TODO add camera search function here.
    # Search for cameras on network
    discovery = ONVIFDiscovery(timeout=5)
    onvif_results = discovery.discover(search="Tapo")
    for res in onvif_results:
        # TODO Check if I set port to the correct value,
        # TODO also could be set to 2020 like in the YAML
        cfg_item: config.CameraConfig = { "ip": res["host"], "onvif_port": res["port"]}
        server_config["cameras"].append(cfg_item)
    # Build the runtime object graph (pure construction; no sockets or tasks yet).
    runtime = build_runtime(server_config)
    configure_logging(runtime.log_stream)
    logger.info("Starting server (version: %s)...", vector.__version__)

    loop = asyncio.get_event_loop()
    daemons: dict[str, asyncio.Task[None]] = {}

    # Fire up the FastAPI app and add it as a daemon task
    daemons["fast_api"] = loop.create_task(fast_api.start_api(runtime))

    # Start all runtime services
    runtime.start(loop)

    # Command line interface daemon
    daemons["command_processor"] = loop.create_task(command_processor(runtime))


    try:
        # -------
        # MAIN SERVER LOOP
        # -------
        stop_event = asyncio.Event()

        try:
            await stop_event.wait()
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt: stopping server.")
            stop_event.set()
        except asyncio.CancelledError:
            logger.info("Server main loop cancelled.")

    # -------
    # CLEANUP
    # -------
    finally:
        # Write all collected devices to the log stream on exit.
        devices = runtime.esp_runtime.get_registered_devices()
        if devices:
            logger.info("Registered devices at shutdown: %s", ", ".join(devices.keys()))

        # Cancel app-level interface tasks.
        for name, task in daemons.items():
            if not task.done():
                task.cancel()
                logger.info("Cancelled daemon task: %s", name)
        await asyncio.gather(*daemons.values(), return_exceptions=True)

        # Stop runtime tasks and close device connections
        await runtime.stop()

        print("\nServer stopped.\n")

if __name__ == "__main__":
    asyncio.run(main())
