"""
Region server entry point.

Usage:
    python -m server.main --server server1
    python -m server.main --server server2

Start server2 first or ensure both are up before clients cross the border.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
import time

from shared.config import SERVER1_GRPC_PORT, SERVER1_ID, SERVER2_GRPC_PORT, SERVER2_ID
from server.game_state import RegionGameState
from server.grpc_servicer import serve_grpc
from server.match_manager import MatchManager
from server.mutex import RicartAgrawalaMutex
from server.peer_client import PeerRegionClient
from server.rabbitmq_client import RabbitPublisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

SERVER_CONFIG = {
    SERVER1_ID: {"port": SERVER1_GRPC_PORT},
    SERVER2_ID: {"port": SERVER2_GRPC_PORT},
}


def _timer_monitor_loop(match: MatchManager, stop_event: threading.Event) -> None:
    """Coordinator thread: end the match when the 2-minute clock runs out."""
    while not stop_event.is_set():
        match.check_and_end_if_expired()
        stop_event.wait(1.0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Mushroom RPG region server")
    parser.add_argument(
        "--server",
        choices=[SERVER1_ID, SERVER2_ID],
        required=True,
        help="Which map region this process owns",
    )
    args = parser.parse_args()
    server_id = args.server
    port = SERVER_CONFIG[server_id]["port"]

    state = RegionGameState(server_id)

    peer = PeerRegionClient(server_id)
    publisher = RabbitPublisher()

    def peer_request_fn(mushroom_id: str, player_id: str, lamport_ts: int) -> bool:
        return peer.request_mushroom_lock(mushroom_id, player_id, lamport_ts)

    mutex = RicartAgrawalaMutex(server_id, peer_request_fn)
    match = MatchManager(server_id, state, peer, publisher)

    try:
        publisher.connect()
    except Exception as exc:
        logger.error(
            "Failed to connect to RabbitMQ. Start it first (docker compose up -d). Error: %s",
            exc,
        )
        sys.exit(1)

    try:
        peer.connect()
    except Exception as exc:
        logger.warning("Peer not reachable yet (start other server): %s", exc)

    grpc_server = serve_grpc(server_id, port, state, mutex, peer, publisher, match)

    timer_stop = threading.Event()
    if match.is_coordinator:
        threading.Thread(
            target=_timer_monitor_loop,
            args=(match, timer_stop),
            daemon=True,
            name="MatchTimer",
        ).start()
        logger.info("Match coordinator timer thread started (2-minute limit)")

    def shutdown(signum, frame):
        logger.info("Shutting down %s...", server_id)
        timer_stop.set()
        grpc_server.stop(grace=2)
        publisher.close()
        peer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger.info(
        "Region server '%s' ready. Left/right split at border. Port %d.",
        server_id,
        port,
    )
    grpc_server.wait_for_termination()


if __name__ == "__main__":
    main()
