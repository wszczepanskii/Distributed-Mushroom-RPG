"""
gRPC client stub for talking to the peer region server.

Used for:
  - Ricart-Agrawala lock REQUEST / RELEASE
  - Player handoff
  - Mushroom removal replication
"""

from __future__ import annotations

import logging
from typing import Optional

import grpc

from generated import game_pb2, game_pb2_grpc
from shared.config import SERVER_PEER_ADDRESSES
from shared.models import Mushroom, Player

logger = logging.getLogger(__name__)


class PeerRegionClient:
    def __init__(self, server_id: str):
        self.server_id = server_id
        self.peer_address = SERVER_PEER_ADDRESSES[server_id]
        self._channel: Optional[grpc.Channel] = None
        self._stub: Optional[game_pb2_grpc.RegionSyncStub] = None

    def connect(self) -> None:
        self._channel = grpc.insecure_channel(self.peer_address)
        self._stub = game_pb2_grpc.RegionSyncStub(self._channel)
        logger.info("Peer gRPC channel to %s", self.peer_address)

    def close(self) -> None:
        if self._channel:
            self._channel.close()

    def request_mushroom_lock(
        self, mushroom_id: str, player_id: str, lamport_timestamp: int
    ) -> bool:
        assert self._stub
        request = game_pb2.MushroomLockRequest(
            requester_server=self.server_id,
            mushroom_id=mushroom_id,
            player_id=player_id,
            lamport_timestamp=lamport_timestamp,
        )
        try:
            reply = self._stub.RequestMushroomLock(request, timeout=3)
            return reply.granted
        except grpc.RpcError as exc:
            logger.error("Peer lock request failed: %s", exc)
            return False

    def release_mushroom_lock(self, mushroom_id: str, lamport_timestamp: int) -> None:
        assert self._stub
        request = game_pb2.MushroomLockRelease(
            releaser_server=self.server_id,
            mushroom_id=mushroom_id,
            lamport_timestamp=lamport_timestamp,
        )
        try:
            self._stub.ReleaseMushroomLock(request, timeout=3)
        except grpc.RpcError as exc:
            logger.error("Peer lock release failed: %s", exc)

    def notify_mushroom_removed(
        self, mushroom_id: str, player_id: str, new_score: int
    ) -> None:
        assert self._stub
        request = game_pb2.MushroomRemovedNotification(
            mushroom_id=mushroom_id,
            removed_by_player=player_id,
            player_new_score=new_score,
        )
        try:
            self._stub.NotifyMushroomRemoved(request, timeout=3)
        except grpc.RpcError as exc:
            logger.error("Mushroom removal notify failed: %s", exc)

    def get_active_mushrooms(self) -> list[Mushroom]:
        assert self._stub
        try:
            response = self._stub.GetActiveMushrooms(
                game_pb2.ActiveMushroomsRequest(), timeout=3
            )
            return [
                Mushroom(
                    mushroom_id=m.mushroom_id,
                    x=m.x,
                    y=m.y,
                    owner_region=m.owner_region,
                )
                for m in response.mushrooms
            ]
        except grpc.RpcError as exc:
            logger.warning("Could not fetch peer mushrooms: %s", exc)
            return []

    def get_active_players(self) -> list[Player]:
        assert self._stub
        try:
            response = self._stub.GetActivePlayers(
                game_pb2.ActivePlayersRequest(), timeout=3
            )
            return [
                Player(
                    player_id=p.player_id,
                    name=p.name,
                    x=p.x,
                    y=p.y,
                    score=p.score,
                    connected_server=p.connected_server,
                )
                for p in response.players
            ]
        except grpc.RpcError as exc:
            logger.warning("Could not fetch peer players: %s", exc)
            return []

    def handoff_player(self, player: Player, to_server: str) -> bool:
        assert self._stub
        request = game_pb2.PlayerHandoffRequest(
            player=game_pb2.PlayerInfo(
                player_id=player.player_id,
                name=player.name,
                x=player.x,
                y=player.y,
                score=player.score,
                connected_server=to_server,
            ),
            from_server=self.server_id,
            to_server=to_server,
        )
        try:
            response = self._stub.HandoffPlayer(request, timeout=3)
            return response.success
        except grpc.RpcError as exc:
            logger.error("Player handoff failed: %s", exc)
            return False
