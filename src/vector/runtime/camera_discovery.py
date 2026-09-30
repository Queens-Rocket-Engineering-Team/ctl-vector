"""ONVIF camera discovery using WS-Discovery."""

from __future__ import annotations
import contextlib
import logging

from wsdiscovery import WSDiscovery


logger = logging.getLogger(__name__)

# ONVIF device service type
ONVIF_DEVICE_TYPE = "dn:NetworkVideoTransmitter"


def discover_onvif_cameras(
    timeout: float = 5.0,
    max_retries: int = 3,
) -> list[dict[str, str]]:
    """Discover ONVIF cameras on the local network using WS-Discovery.

    Args:
        timeout: Seconds to wait for each discovery probe.
        max_retries: Number of times to retry discovery if no cameras found.

    Returns:
        List of dictionaries containing camera information:
        [{"ip": str, "onvif_port": int, "hostname": str | None}]

    """
    for attempt in range(max_retries):
        logger.info(
            "Attempting ONVIF camera discovery (attempt %d/%d)",
            attempt + 1,
            max_retries,
        )
        try:
            wsd = WSDiscovery()
            wsd.start()
            services = wsd.searchServices(
                timeout=timeout,
                types=[ONVIF_DEVICE_TYPE],
            )
            wsd.stop()

            cameras: list[dict[str, str]] = []
            seen_ips = set()
            for service in services:
                # Extract IP address and port from the service's XAddrs
                xaddrs = service.getXAddrs()
                if not xaddrs:
                    continue

                # Use the first XAddr
                xaddr = xaddrs[0]
                # Parse URL like http://192.168.0.100:2020/onvif/device_service
                try:
                    # Remove protocol and path
                    host_port = xaddr.split("//")[1].split("/")[0]
                    if ":" in host_port:
                        ip, port_str = host_port.split(":", 1)
                        port = int(port_str)
                    else:
                        ip = host_port
                        port = 2020  # Default ONVIF port
                except (IndexError, ValueError) as e:
                    logger.warning(
                        "Failed to parse XAddr %s: %s",
                        xaddr,
                        e,
                    )
                    continue

                # Skip duplicate IPs (same camera might announce multiple services)
                if ip in seen_ips:
                    continue
                seen_ips.add(ip)

                # Try to get hostname from the device (optional)
                hostname = None
                with contextlib.suppress(Exception):
                    # We could make an ONVIF call here, but that would be slow and
                    # require credentials. Skip for now; hostname can be obtained
                    # later when connecting to the camera.
                    pass

                cameras.append(
                    {
                        "ip": ip,
                        "onvif_port": port,
                        "hostname": hostname,
                    },
                )

            if cameras:
                logger.info(
                    "Discovered %d ONVIF camera(s): %s",
                    len(cameras),
                    [c["ip"] for c in cameras],
                )
                return cameras
            logger.warning(
                "No ONVIF cameras discovered on attempt %d/%d",
                attempt + 1,
                max_retries,
            )

        except Exception:
            logger.exception(
                "Error during ONVIF discovery attempt %d/%d",
                attempt + 1,
                max_retries,
            )

    logger.error(
        "Failed to discover any ONVIF cameras after %d attempts",
        max_retries,
    )
    return []


def discover_onvif_cameras_sync(
    timeout: float = 5.0,
    max_retries: int = 3,
) -> list[dict[str, str]]:
    """Provide a synchronous wrapper for discover_onvif_cameras."""
    return discover_onvif_cameras(timeout=timeout, max_retries=max_retries)
