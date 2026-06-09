"""
Authoritative game state for one region server.

Each server is authoritative for players currently connected to it and for
mushrooms in its half of the map. Border mushrooms use distributed locking
before mutation (see mutex.py).
"""

from __future__ import annotations

import random
import threading
from typing import List, Optional, Tuple

from shared.config import (
    BORDER_X,
    INITIAL_MUSHROOM_COUNT,
    MAP_HEIGHT,
    MAP_WIDTH,
    SERVER1_ID,
    is_near_border,
    region_for_x,
)
from shared.models import Mushroom, Player, WorldState, new_id


class RegionGameState:
    def __init__(self, server_id: str):
        self.server_id = server_id
        self.world = WorldState()
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def seed_mushrooms_for_region(self, count: int = INITIAL_MUSHROOM_COUNT // 2) -> None:
        """Spawn mushrooms only on this server's half of the map."""
        with self._lock:
            spawned = 0
            attempts = 0
            while spawned < count and attempts < count * 20:
                attempts += 1
                x, y = self._random_cell_in_region()
                if self._cell_occupied(x, y):
                    continue
                mush = Mushroom(
                    mushroom_id=new_id("mush"),
                    x=x,
                    y=y,
                    owner_region=self.server_id,
                )
                self.world.mushrooms[mush.mushroom_id] = mush
                spawned += 1

    def _random_cell_in_region(self) -> Tuple[int, int]:
        if self.server_id == SERVER1_ID:
            x = random.randint(0, BORDER_X - 1)
        else:
            x = random.randint(BORDER_X, MAP_WIDTH - 1)
        y = random.randint(0, MAP_HEIGHT - 1)
        return x, y

    def _cell_occupied(self, x: int, y: int) -> bool:
        for p in self.world.players.values():
            if p.x == x and p.y == y:
                return True
        for m in self.world.mushrooms.values():
            if m.x == x and m.y == y:
                return True
        return False

    # ------------------------------------------------------------------
    # Players
    # ------------------------------------------------------------------

    def add_player(self, name: str) -> Player:
        with self._lock:
            x, y = self._spawn_point()
            player = Player(
                player_id=new_id("player"),
                name=name,
                x=x,
                y=y,
                connected_server=self.server_id,
            )
            self.world.players[player.player_id] = player
            return player

    def _spawn_point(self) -> Tuple[int, int]:
        for _ in range(50):
            x, y = self._random_cell_in_region()
            if not self._cell_occupied(x, y):
                return x, y
        return (0 if self.server_id == SERVER1_ID else BORDER_X, MAP_HEIGHT // 2)

    def remove_player(self, player_id: str) -> Optional[Player]:
        with self._lock:
            return self.world.players.pop(player_id, None)

    def get_player(self, player_id: str) -> Optional[Player]:
        with self._lock:
            return self.world.players.get(player_id)

    def move_player(self, player_id: str, dx: int, dy: int) -> Tuple[bool, str, Optional[Player]]:
        """
        Apply movement if valid. Returns (success, message, updated_player).
        Caller checks handoff when x crosses BORDER_X.
        """
        with self._lock:
            player = self.world.players.get(player_id)
            if not player:
                return False, "Unknown player", None

            nx = max(0, min(MAP_WIDTH - 1, player.x + dx))
            ny = max(0, min(MAP_HEIGHT - 1, player.y + dy))

            if self._occupied_by_other(player_id, nx, ny):
                return False, "Cell blocked", player

            player.x = nx
            player.y = ny
            return True, "OK", player

    def _occupied_by_other(self, player_id: str, x: int, y: int) -> bool:
        for pid, p in self.world.players.items():
            if pid != player_id and p.x == x and p.y == y:
                return True
        return False

    def import_player_from_handoff(self, player: Player) -> None:
        """Peer server transfers a player entering this region."""
        with self._lock:
            player.connected_server = self.server_id
            self.world.players[player.player_id] = player

    def export_player_for_handoff(self, player_id: str) -> Optional[Player]:
        with self._lock:
            return self.world.players.pop(player_id, None)

    # ------------------------------------------------------------------
    # Mushrooms
    # ------------------------------------------------------------------

    def mushroom_at_player_feet(self, player: Player) -> Optional[Mushroom]:
        with self._lock:
            for m in self.world.mushrooms.values():
                if m.x == player.x and m.y == player.y:
                    return m
            return None

    def remove_mushroom(self, mushroom_id: str) -> Optional[Mushroom]:
        with self._lock:
            return self.world.mushrooms.pop(mushroom_id, None)

    def add_mushroom(self, mushroom: Mushroom) -> None:
        with self._lock:
            self.world.mushrooms[mushroom.mushroom_id] = mushroom

    def requires_distributed_lock(self, mushroom: Mushroom) -> bool:
        """
        Border mushrooms are reachable from both regions simultaneously,
        so pickup must use Ricart-Agrawala across servers.
        """
        return is_near_border(mushroom.x) or mushroom.owner_region != self.server_id

    def all_players(self) -> List[Player]:
        with self._lock:
            return list(self.world.players.values())

    def all_mushrooms(self) -> List[Mushroom]:
        with self._lock:
            return list(self.world.mushrooms.values())
