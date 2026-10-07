from __future__ import annotations
import asyncio
import contextlib
import logging
import socket
from typing import TYPE_CHECKING

from vector.qlcp.packets import DiscoveryPacket


if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Sequence


logger = logging.getLogger(__name__)

# Defined in QLCP spec
MULTICAST_ADDRESS = "239.100.0.1"
MULTICAST_PORT = 10000

# How long the periodic loop sleeps between checks while periodic discovery is disabled.
_DISABLED_POLL_INTERVAL_S = 0.5


class DiscoveryService:
    """Owns device discovery for every provider: a periodic loop and one-shot requests.

    QLCP nodes answer a multicast request by connecting. Other providers supply
    an async discovery callable, run as a background task on each request.
    """

    def __init__(
        self,
        *,
        providers: Sequence[Callable[[], Coroutine[None, None, None]]] = (),
        periodic_enabled: bool = True,
        periodic_interval_s: float = 30.0,
        multicast_address: str = MULTICAST_ADDRESS,
        multicast_port: int = MULTICAST_PORT,
    ) -> None:
        self.providers = tuple(providers)
        self.periodic_enabled = periodic_enabled
        self.periodic_interval_s = periodic_interval_s
        self.multicast_address = multicast_address
        self.multicast_port = multicast_port
        self._socket: socket.socket | None = None
        self._provider_tasks: set[asyncio.Task[None]] = set()

    def discover(self) -> None:
        """Send a QLCP discovery request and start every other provider's discovery."""
        if self._socket is None:
            self._socket = self._create_socket()

        logger.debug("Sending discovery request.")

        packet = DiscoveryPacket.create().encode()
        self._socket.sendto(packet, (self.multicast_address, self.multicast_port))
        for provider in self.providers:
            # Provider discovery can take seconds; keep a reference so the task is not collected mid-run.
            task = asyncio.get_running_loop().create_task(provider())
            self._provider_tasks.add(task)
            task.add_done_callback(self._provider_tasks.discard)

    async def run(self) -> None:
        """Periodically issue discovery requests while periodic discovery is enabled."""
        while True:
            if self.periodic_enabled:
                try:
                    self.discover()
                except Exception:
                    # Drop the socket so the next attempt recreates it (e.g. after a network outage).
                    logger.exception("Discovery request failed")
                    if self._socket is not None:
                        with contextlib.suppress(OSError):
                            self._socket.close()
                        self._socket = None
                await asyncio.sleep(self.periodic_interval_s)
            else:
                await asyncio.sleep(_DISABLED_POLL_INTERVAL_S)

    def _create_socket(self) -> socket.socket:
        """Create a send-only socket for issuing multicast discovery requests."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        with contextlib.suppress(OSError):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

        # Choose outbound interface
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)

        sock.setblocking(False)
        logger.info(f"Discovery socket initialized for {self.multicast_address}:{self.multicast_port}")
        return sock
