from __future__ import annotations
import asyncio
import logging
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

import aioconsole

from vector.core import (
    TARE_DEFAULT_SAMPLES,
    TARE_SAMPLE_CAPACITY,
    TareCaptureError,
)


if TYPE_CHECKING:
    from vector.runtime.esp_connection_runtime import ESPDeviceSession
    from vector.runtime.services import RuntimeServices


logger = logging.getLogger(__name__)


def _find_device(devices: dict[str, ESPDeviceSession], name: str) -> ESPDeviceSession | None:
    for device in devices.values():
        if device.name.lower() == name.lower():
            return device
    return None


SERVER_COMMANDS = [
    "QUIT",
    "EXIT",
    "DISCOVER",
    "AUTODISCOVERY",
    "AUTOD",
    "LIST",
    "HELP",
    "INFO",
    "REMOVE",
    "ESTOP",
    "TARE",
]

DEVICE_COMMANDS = [
    "GETS",
    "STREAM",
    "STOP",
    "CONTROL",
    "OPEN",
    "CLOSE",
    "STATUS",
]


async def _handle_tare_command(runtime: RuntimeServices, args: list) -> None:
    """Handle `tare`, `tare <sensor> [samples]`, and `tare <sensor> clear`."""
    if not args:
        tares = runtime.core.tares()
        if not tares:
            logger.info("No sensors are tared.")
            logger.info("  Try: tare <sensor_name>")
            return
        logger.info(f"Tared sensors ({len(tares)}):")
        for sensor_name, offset in tares.items():
            logger.info(f"  {sensor_name} - {offset}")
        return

    sensor_name = args[0]

    if len(args) > 1 and args[1].lower() in ("clear", "reset", "off"):
        if not runtime.core.clear_tare(sensor_name):
            logger.info(f"Sensor '{sensor_name}' is not tared")
            return
        logger.info(f"Cleared tare for '{sensor_name}'")
        return

    samples = TARE_DEFAULT_SAMPLES
    if len(args) > 1:
        try:
            samples = int(args[1])
        except ValueError:
            logger.info(f"Invalid sample count: {args[1]!r}. Usage: tare <sensor> [samples|clear]")
            return
        if not 1 <= samples <= TARE_SAMPLE_CAPACITY:
            logger.info(f"Sample count must be between 1 and {TARE_SAMPLE_CAPACITY}")
            return

    try:
        offset, device_name, count = runtime.core.capture_tare_offset(sensor_name, samples=samples)
    except TareCaptureError as exc:
        logger.info(str(exc))
        return

    runtime.core.set_tare(sensor_name, offset)
    logger.info(f"Tared '{sensor_name}' to {offset} from {count} readings on {device_name}")


async def handle_server_command(runtime: RuntimeServices, command: str, args: list) -> None:
    logger.info(f"Server command received: {command}")
    cmd = command.upper()
    if cmd in ("QUIT", "EXIT"):
        logger.info("Shutting down server...")
        await asyncio.sleep(0.1)
    elif cmd == "DISCOVER":
        logger.info("Sending discovery broadcast...")
        runtime.discovery_service.discover()
        logger.info("Discovery sent. Devices will auto-connect.")
    elif cmd in ("AUTODISCOVERY", "AUTOD"):
        if not args:
            logger.info(
                f"Autodiscovery: enabled={runtime.discovery_service.periodic_enabled}, interval={runtime.discovery_service.periodic_interval_s}s",
            )
            logger.info("Usage: autodiscovery <on|off|interval <seconds>|status>")
            return

        sub = args[0].lower()
        if sub in ("status", "show"):
            logger.info(
                f"Autodiscovery: enabled={runtime.discovery_service.periodic_enabled}, interval={runtime.discovery_service.periodic_interval_s}s",
            )
        elif sub in ("on", "enable", "enabled", "true"):
            runtime.discovery_service.periodic_enabled = True
            logger.info("Autodiscovery enabled")
        elif sub in ("off", "disable", "disabled", "false"):
            runtime.discovery_service.periodic_enabled = False
            logger.info("Autodiscovery disabled")
        elif sub == "interval":
            if len(args) < 2:
                logger.info("Usage: autodiscovery interval <seconds>")
                return
            try:
                interval = float(args[1])
            except ValueError:
                logger.info(f"Invalid interval: {args[1]!r}. Must be a positive number.")
                return

            if interval <= 0:
                logger.info("Autodiscovery interval must be greater than 0 seconds")
                return

            runtime.discovery_service.periodic_interval_s = interval
            logger.info(f"Autodiscovery interval set to {interval}s")
        else:
            logger.info("Usage: autodiscovery <on|off|interval <seconds>|status>")
    elif cmd == "LIST":
        devices = runtime.esp_runtime.get_registered_devices()
        if not devices:
            logger.info("No devices connected.")
            logger.info("  Try: discover")
        else:
            logger.info(f"Connected devices ({len(devices)}):")
            for registered_device in devices.values():
                logger.info(f"  {registered_device.name} - {registered_device.address}")
                logger.info(f"    Sensors: {len(registered_device.sensors)}, Controls: {len(registered_device.controls)}")
    elif cmd == "REMOVE":
        if not args:
            logger.info("Usage: remove <device_name>")
            return
        devices = runtime.esp_runtime.get_registered_devices()
        device = _find_device(devices, args[0])
        if not device:
            logger.info(f"Device '{args[0]}' not currently registered. Use \"LIST\" to see devices.")
            return
        runtime.esp_runtime.remove_device(device)
        logger.info(f"Removed device '{device.name}'")

    elif cmd == "INFO":
        if not args:
            logger.info("Usage: info <device_name>")
            return
        devices = runtime.esp_runtime.get_registered_devices()
        device = _find_device(devices, args[0])
        if not device:
            logger.info(f"Device '{args[0]}' not found")
            return
        logger.info(f"Device: {device.name}")
        logger.info(f"  Address: {device.address}")
        logger.info(f"  Sensors ({len(device.sensors)}):")
        for idx, name in enumerate(device.sensors.keys()):
            logger.info(f"    [{idx}] {name}")
        logger.info(f"  Controls ({len(device.controls)}):")
        for idx, name in enumerate(device.controls.keys()):
            logger.info(f"    [{idx}] {name}")
    elif cmd == "TARE":
        await _handle_tare_command(runtime, args)
    elif cmd == "HELP":
        logger.info("Available commands:")
        logger.info("  discover           - Discover devices")
        logger.info("  autodiscovery      - Show autodiscovery status")
        logger.info("  autodiscovery on   - Enable periodic discovery")
        logger.info("  autodiscovery off  - Disable periodic discovery")
        logger.info("  autodiscovery interval <seconds> - Set discovery interval")
        logger.info("  list               - Show connected devices")
        logger.info("  info <device>      - Show device details")
        logger.info("  stream <dev> <hz>  - Start streaming")
        logger.info("  stop <device>      - Stop streaming")
        logger.info("  open <dev> <ctrl>  - Open valve/control")
        logger.info("  close <dev> <ctrl> - Close valve/control")
        logger.info("  status <device>    - Get device status / control states")
        logger.info("  tare               - Show applied sensor tares")
        logger.info("  tare <sensor> [n]  - Zero a sensor using its last n readings")
        logger.info("  tare <sensor> clear - Remove a sensor's tare")
        logger.info("  quit               - Exit")
    elif cmd == "ESTOP":
        devices = runtime.esp_runtime.get_registered_devices()
        for device in devices.values():
            await runtime.esp_runtime.emergency_stop(device)
        logger.info("Emergency stop sent to all devices")


async def handle_device_command(runtime: RuntimeServices, command: str, args: list) -> None:
    if not args:
        logger.info(f"Usage: {command.lower()} <device_name> [args...]")
        return

    device_name = args[0]
    devices = runtime.esp_runtime.get_registered_devices()
    device = _find_device(devices, device_name)

    if not device:
        logger.info(f"Device '{device_name}' not found. Use 'list' to see devices.")
        return

    cmd = command.upper()
    try:
        if cmd == "GETS":
            await runtime.esp_runtime.get_single(device)
            logger.info(f"Requested data from {device.name}")
        elif cmd == "STREAM":
            if len(args) < 2:
                logger.info("Usage: stream <device> <frequency_hz>")
                return
            freq = int(args[1])
            await runtime.esp_runtime.start_streaming(device, freq)
            logger.info(f"Streaming from {device.name} at {freq} Hz")
        elif cmd == "STOP":
            await runtime.esp_runtime.stop_streaming(device)
            logger.info(f"Stopped streaming from {device.name}")
        elif cmd == "CONTROL":
            if len(args) < 3:
                logger.info("Usage: control <device> <name> <OPEN|CLOSED|value>")
                return
            await runtime.esp_runtime.set_control(device, args[1], args[2])
            logger.info(f"Sent {args[2]} to {args[1]} on {device.name}")
        elif cmd == "OPEN":
            if len(args) < 2:
                logger.info("Usage: open <device> <control_name>")
                return
            await runtime.esp_runtime.set_control(device, args[1], "OPEN")
            logger.info(f"Opened {args[1]} on {device.name}")
        elif cmd == "CLOSE":
            if len(args) < 2:
                logger.info("Usage: close <device> <control_name>")
                return
            await runtime.esp_runtime.set_control(device, args[1], "CLOSED")
            logger.info(f"Closed {args[1]} on {device.name}")
        elif cmd == "STATUS":
            await runtime.esp_runtime.get_status(device)
            logger.info(f"Requested status from {device.name}")
    except Exception as e:
        logger.error(f"Error: {e}")


async def process_command(runtime: RuntimeServices, command: str) -> None:
    """Process a command and send it to all connected devices."""
    full_command = command.strip()
    cmd = full_command.split(" ")[0]
    args = full_command.split(" ")[1:]

    if cmd.upper() in SERVER_COMMANDS:
        await handle_server_command(runtime, cmd, args)
    elif cmd.upper() in DEVICE_COMMANDS:
        await handle_device_command(runtime, cmd, args)
    else:
        logger.error(f"Unknown command: {cmd}. Available commands: {', '.join(SERVER_COMMANDS + DEVICE_COMMANDS)}")


async def cli_reader() -> AsyncGenerator[str, None]:
    """Read commands from CLI input."""
    while True:
        try:
            command = await aioconsole.ainput("QRET> ")
            if command.lower() in ["quit", "exit"]:
                raise KeyboardInterrupt
            yield command.strip()
        except asyncio.CancelledError:
            break


async def command_processor(runtime: RuntimeServices) -> None:
    """Process commands from various input sources."""
    logger.info("Started command processor daemon task.")

    try:
        async for command in cli_reader():
            if command:
                await process_command(runtime, command)
    except KeyboardInterrupt:
        logger.info("Command processor stopped by user")
        raise
