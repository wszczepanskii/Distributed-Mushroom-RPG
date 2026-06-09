"""
Distributed match timer and win condition.

Server 1 is the match coordinator (source of truth for the 2-minute clock).
Server 2 mirrors match state via inter-server gRPC.

When the timer expires, the coordinator gathers scores from both regions,
declares a winner, and broadcasts GAME_ENDED to all clients.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from typing import TYPE_CHECKING, List, Optional

from shared.config import (
    ACTIVE_MUSHROOM_COUNT,
    MATCH_DURATION_SECONDS,
    SERVER1_ID,
    SERVER2_ID,
)
from shared.events import EventType
from shared.models import Mushroom, Player

if TYPE_CHECKING:
    from server.game_state import RegionGameState
    from server.peer_client import PeerRegionClient
    from server.rabbitmq_client import RabbitPublisher

logger = logging.getLogger(__name__)


class MatchManager:
    def __init__(
        self,
        server_id: str,
        state: "RegionGameState",
        peer: "PeerRegionClient",
        publisher: "RabbitPublisher",
    ):
        self.server_id = server_id
        self.state = state
        self.peer = peer
        self.publisher = publisher
        self.is_coordinator = server_id == SERVER1_ID
        self._lock = threading.RLock()
        self.match_end_time_unix: float = 0.0
        self.game_over = False
        self.winner_name = ""
        self.winner_player_id = ""
        self.winner_score = 0

    # ------------------------------------------------------------------
    # Match lifecycle
    # ------------------------------------------------------------------

    def ensure_started(self) -> None:
        """Start the 2-minute match on first player join (coordinator only)."""
        with self._lock:
            if self.match_end_time_unix > 0:
                return

        if self.is_coordinator:
            self._start_match_as_coordinator()
        else:
            state = self.peer.ensure_match_started()
            if state:
                self._apply_remote_state(state)

    def _start_match_as_coordinator(self) -> None:
        with self._lock:
            if self.match_end_time_unix > 0:
                return
            self.match_end_time_unix = time.time() + MATCH_DURATION_SECONDS
            logger.info("Match started — ends at unix %.0f", self.match_end_time_unix)

        self.ensure_mushroom_quota()
        self.publisher.publish(
            EventType.GAME_STARTED,
            {"end_time_unix": self.match_end_time_unix},
        )

    def apply_remote_state(
        self,
        end_time_unix: float,
        game_over: bool,
        winner_name: str,
        winner_player_id: str,
        winner_score: int,
    ) -> None:
        with self._lock:
            self.match_end_time_unix = end_time_unix
            self.game_over = game_over
            self.winner_name = winner_name
            self.winner_player_id = winner_player_id
            self.winner_score = winner_score

    def _apply_remote_state(self, state: dict) -> None:
        self.apply_remote_state(
            state["end_time_unix"],
            state.get("game_over", False),
            state.get("winner_name", ""),
            state.get("winner_player_id", ""),
            state.get("winner_score", 0),
        )

    def remaining_seconds(self) -> int:
        with self._lock:
            if self.game_over or self.match_end_time_unix <= 0:
                return 0
            return max(0, int(self.match_end_time_unix - time.time()))

    def is_active(self) -> bool:
        with self._lock:
            return self.match_end_time_unix > 0 and not self.game_over

    def to_dict(self) -> dict:
        with self._lock:
            return {
                "end_time_unix": self.match_end_time_unix,
                "remaining_seconds": self.remaining_seconds(),
                "game_over": self.game_over,
                "winner_name": self.winner_name,
                "winner_player_id": self.winner_player_id,
                "winner_score": self.winner_score,
            }

    # ------------------------------------------------------------------
    # Timer monitor (coordinator only)
    # ------------------------------------------------------------------

    def check_and_end_if_expired(self) -> None:
        if not self.is_coordinator:
            return
        with self._lock:
            if self.game_over or self.match_end_time_unix <= 0:
                return
            if time.time() < self.match_end_time_unix:
                return
        self.end_game()

    def end_game(self) -> None:
        """Compute winner across both servers and broadcast result."""
        with self._lock:
            if self.game_over:
                return

        players = self._all_players_cluster_wide()
        winner: Optional[Player] = None
        top_score = 0

        if players:
            top_score = max(p.score for p in players)
            leaders = [p for p in players if p.score == top_score]
            if len(leaders) == 1:
                winner = leaders[0]

        with self._lock:
            self.game_over = True
            if winner:
                self.winner_name = winner.name
                self.winner_player_id = winner.player_id
                self.winner_score = winner.score
            elif players:
                self.winner_name = "Draw"
                self.winner_player_id = ""
                self.winner_score = top_score
            else:
                self.winner_name = "Nobody"
                self.winner_player_id = ""
                self.winner_score = 0

        payload = {
            "winner_name": self.winner_name,
            "winner_player_id": self.winner_player_id,
            "winner_score": self.winner_score,
            "end_time_unix": self.match_end_time_unix,
        }
        self.publisher.publish(EventType.GAME_ENDED, payload)
        self.peer.notify_game_ended(self.to_dict())
        logger.info(
            "Match ended — winner: %s (%d mushrooms)",
            self.winner_name,
            self.winner_score,
        )

    def receive_game_ended(self, payload: dict) -> None:
        """Mirror end state on non-coordinator server."""
        self.apply_remote_state(
            payload.get("end_time_unix", self.match_end_time_unix),
            True,
            payload.get("winner_name", ""),
            payload.get("winner_player_id", ""),
            payload.get("winner_score", 0),
        )

    def _all_players_cluster_wide(self) -> List[Player]:
        local = {p.player_id: p for p in self.state.all_players()}
        for p in self.peer.get_active_players():
            local.setdefault(p.player_id, p)
        return list(local.values())

    # ------------------------------------------------------------------
    # Mushroom pool (always ACTIVE_MUSHROOM_COUNT on the map)
    # ------------------------------------------------------------------

    def _cluster_mushroom_count(self) -> int:
        return len(self.state.all_mushrooms()) + len(self.peer.get_active_mushrooms())

    def ensure_mushroom_quota(self) -> None:
        """Top up mushrooms cluster-wide to ACTIVE_MUSHROOM_COUNT (coordinator only)."""
        if self.game_over:
            return
        if not self.is_coordinator:
            self.peer.ensure_mushroom_quota()
            return

        # Re-count after every spawn so peer/local balance stays accurate.
        while self._cluster_mushroom_count() < ACTIVE_MUSHROOM_COUNT:
            if not self._spawn_one_on_random_server():
                logger.warning("Could not spawn mushroom — map may be full")
                break

    def _spawn_on_server(self, target_server: str) -> Optional[Mushroom]:
        """Spawn exactly one mushroom on the given region server."""
        if target_server == self.server_id:
            mushroom = self.state.try_spawn_random_mushroom()
            if mushroom:
                self.publisher.publish(EventType.MUSHROOM_SPAWNED, mushroom.to_dict())
                logger.info("Spawned mushroom %s on %s", mushroom.mushroom_id, self.server_id)
            return mushroom
        return self.peer.request_spawn_mushroom()

    def _spawn_one_on_random_server(self) -> Optional[Mushroom]:
        """
        Each mushroom independently picks a random server (50/50).

        If the chosen server cannot place a mushroom (peer offline / full region),
        try the other server before giving up.
        """
        servers = [SERVER1_ID, SERVER2_ID]
        random.shuffle(servers)
        for target_server in servers:
            mushroom = self._spawn_on_server(target_server)
            if mushroom:
                return mushroom
        return None
