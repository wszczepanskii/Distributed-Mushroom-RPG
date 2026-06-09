"""
gRPC servicer implementations for GameService and RegionSync.

This module wires together:
  - RegionGameState (authoritative data)
  - RicartAgrawalaMutex (distributed pickup exclusion)
  - PeerRegionClient (inter-server RPC)
  - RabbitPublisher (client broadcast)
"""

from __future__ import annotations

import logging
from concurrent import futures
from typing import TYPE_CHECKING

import grpc

from generated import game_pb2, game_pb2_grpc
from shared.config import (
    BORDER_X,
    MAP_HEIGHT,
    MAP_WIDTH,
    SERVER_CLIENT_ADDRESSES,
    SERVER1_ID,
    region_for_x,
)
from shared.events import EventType
from shared.models import Mushroom, Player

if TYPE_CHECKING:
    from server.game_state import RegionGameState
    from server.match_manager import MatchManager
    from server.mutex import RicartAgrawalaMutex
    from server.peer_client import PeerRegionClient
    from server.rabbitmq_client import RabbitPublisher

logger = logging.getLogger(__name__)


def _player_to_proto(player: Player) -> game_pb2.PlayerInfo:
    return game_pb2.PlayerInfo(
        player_id=player.player_id,
        name=player.name,
        x=player.x,
        y=player.y,
        score=player.score,
        connected_server=player.connected_server,
    )


def _mushroom_to_proto(mushroom: Mushroom) -> game_pb2.MushroomInfo:
    return game_pb2.MushroomInfo(
        mushroom_id=mushroom.mushroom_id,
        x=mushroom.x,
        y=mushroom.y,
        owner_region=mushroom.owner_region,
    )


def build_game_state_proto(state: "RegionGameState") -> game_pb2.GameState:
    return game_pb2.GameState(
        players=[_player_to_proto(p) for p in state.all_players()],
        mushrooms=[_mushroom_to_proto(m) for m in state.all_mushrooms()],
        map_width=MAP_WIDTH,
        map_height=MAP_HEIGHT,
        border_x=BORDER_X,
    )


def _attach_match_fields(
    proto: game_pb2.GameState, match: "MatchManager"
) -> game_pb2.GameState:
    info = match.to_dict()
    proto.match_end_time_unix = int(info["end_time_unix"])
    proto.remaining_seconds = info["remaining_seconds"]
    proto.game_over = info["game_over"]
    proto.winner_name = info["winner_name"]
    proto.winner_player_id = info["winner_player_id"]
    proto.winner_score = info["winner_score"]
    return proto


def build_merged_client_state_proto(
    state: "RegionGameState",
    peer: "PeerRegionClient",
    match: "MatchManager",
) -> game_pb2.GameState:
    """
    Full-map view for clients: local region + peer region entities.

    Each server remains authoritative only for its own half; this merge is
    read-only for rendering and avoids relying on RabbitMQ startup broadcasts
    (which late-joining clients would miss).
    """
    players = {p.player_id: p for p in state.all_players()}
    for peer_player in peer.get_active_players():
        players.setdefault(peer_player.player_id, peer_player)

    mushrooms = {m.mushroom_id: m for m in state.all_mushrooms()}
    for peer_mushroom in peer.get_active_mushrooms():
        mushrooms.setdefault(peer_mushroom.mushroom_id, peer_mushroom)

    proto = game_pb2.GameState(
        players=[_player_to_proto(p) for p in players.values()],
        mushrooms=[_mushroom_to_proto(m) for m in mushrooms.values()],
        map_width=MAP_WIDTH,
        map_height=MAP_HEIGHT,
        border_x=BORDER_X,
    )
    return _attach_match_fields(proto, match)


class GameServiceServicer(game_pb2_grpc.GameServiceServicer):
  """Client-facing API for one region server."""

  def __init__(
      self,
      server_id: str,
      state: "RegionGameState",
      mutex: "RicartAgrawalaMutex",
      peer: "PeerRegionClient",
      publisher: "RabbitPublisher",
      match: "MatchManager",
  ):
      self.server_id = server_id
      self.state = state
      self.mutex = mutex
      self.peer = peer
      self.publisher = publisher
      self.match = match

  def JoinGame(self, request, context):
      self.match.ensure_started()
      player = self.state.add_player(request.name or "Adventurer")
      self.publisher.publish(EventType.PLAYER_JOINED, player.to_dict())

      return game_pb2.JoinResponse(
          success=True,
          message="Welcome! Collect mushrooms — 2 minute limit!",
          player_id=player.player_id,
          server_id=self.server_id,
          server_address=SERVER_CLIENT_ADDRESSES[self.server_id],
          initial_state=build_merged_client_state_proto(
              self.state, self.peer, self.match
          ),
      )

  def MovePlayer(self, request, context):
      if self.match.game_over:
          return game_pb2.MoveResponse(success=False, message="Match is over")

      ok, msg, player = self.state.move_player(request.player_id, request.dx, request.dy)
      if not ok or not player:
          return game_pb2.MoveResponse(success=False, message=msg)

      handoff_required = region_for_x(player.x) != self.server_id

      if handoff_required:
          target_server = region_for_x(player.x)
          exported = self.state.export_player_for_handoff(player.player_id)
          if not exported:
              return game_pb2.MoveResponse(success=False, message="Handoff failed")

          if not self.peer.handoff_player(exported, target_server):
              # Roll back: put player back on this server.
              self.state.import_player_from_handoff(exported)
              return game_pb2.MoveResponse(success=False, message="Peer rejected handoff")

          exported.connected_server = target_server
          self.publisher.publish(
              EventType.PLAYER_HANDOFF,
              {
                  **exported.to_dict(),
                  "from_server": self.server_id,
                  "to_server": target_server,
              },
          )
          return game_pb2.MoveResponse(
              success=True,
              message="Crossed border — reconnecting to peer server",
              x=player.x,
              y=player.y,
              handoff_required=True,
              new_server_address=SERVER_CLIENT_ADDRESSES[target_server],
              new_server_id=target_server,
          )

      self.publisher.publish(
          EventType.PLAYER_MOVED,
          {"player_id": player.player_id, "x": player.x, "y": player.y},
      )
      return game_pb2.MoveResponse(
          success=True,
          message=msg,
          x=player.x,
          y=player.y,
      )

  def PickupMushroom(self, request, context):
      """Distributed-safe mushroom pickup with post-collect respawn."""
      if self.match.game_over:
          return game_pb2.PickupResponse(success=False, message="Match is over")

      player = self.state.get_player(request.player_id)
      if not player:
          return game_pb2.PickupResponse(success=False, message="Unknown player")

      mushroom = self.state.mushroom_at_player_feet(player)
      if not mushroom:
          return game_pb2.PickupResponse(success=False, message="No mushroom here")

      needs_lock = self.state.requires_distributed_lock(mushroom)
      lock_acquired = False
      target_mushroom_id = mushroom.mushroom_id

      try:
          if needs_lock:
              lock_acquired = self.mutex.acquire(
                  target_mushroom_id, player.player_id
              )
              if not lock_acquired:
                  return game_pb2.PickupResponse(
                      success=False,
                      message="Could not acquire distributed lock (contention)",
                  )

          # Critical section — re-validate after lock.
          mushroom = self.state.mushroom_at_player_feet(player)
          if not mushroom:
              return game_pb2.PickupResponse(
                  success=False,
                  message="Mushroom already taken",
              )

          removed = self.state.remove_mushroom(mushroom.mushroom_id)
          if not removed:
              return game_pb2.PickupResponse(success=False, message="Race lost")

          player.score += 1
          target_mushroom_id = removed.mushroom_id

          self.publisher.publish(
              EventType.MUSHROOM_REMOVED,
              {
                  "mushroom_id": removed.mushroom_id,
                  "x": removed.x,
                  "y": removed.y,
                  "player_id": player.player_id,
              },
          )
          self.publisher.publish(
              EventType.SCORE_UPDATED,
              {"player_id": player.player_id, "score": player.score},
          )

          # Keep peer's mushroom map consistent.
          self.peer.notify_mushroom_removed(
              removed.mushroom_id, player.player_id, player.score
          )

          # Top up mushroom pool to ACTIVE_MUSHROOM_COUNT across both servers.
          self.match.ensure_mushroom_quota()

          return game_pb2.PickupResponse(
              success=True,
              message="Mushroom collected!",
              new_score=player.score,
              mushroom_id=removed.mushroom_id,
          )
      finally:
          if needs_lock and lock_acquired:
              ts = self.mutex.get_reply_timestamp()
              self.mutex.release(target_mushroom_id)
              self.peer.release_mushroom_lock(target_mushroom_id, ts)

  def GetGameState(self, request, context):
      return build_merged_client_state_proto(self.state, self.peer, self.match)

  def LeaveGame(self, request, context):
      removed = self.state.remove_player(request.player_id)
      if removed:
          self.publisher.publish(EventType.PLAYER_LEFT, {"player_id": removed.player_id})
      return game_pb2.LeaveResponse(success=True)


class RegionSyncServicer(game_pb2_grpc.RegionSyncServicer):
  """Peer server API — handoffs and Ricart-Agrawala messages."""

  def __init__(
      self,
      server_id: str,
      state: "RegionGameState",
      mutex: "RicartAgrawalaMutex",
      publisher: "RabbitPublisher",
      match: "MatchManager",
  ):
      self.server_id = server_id
      self.state = state
      self.mutex = mutex
      self.publisher = publisher
      self.match = match

  def HandoffPlayer(self, request, context):
      player = Player(
          player_id=request.player.player_id,
          name=request.player.name,
          x=request.player.x,
          y=request.player.y,
          score=request.player.score,
          connected_server=self.server_id,
      )
      self.state.import_player_from_handoff(player)
      self.publisher.publish(EventType.PLAYER_JOINED, player.to_dict())
      logger.info("Handoff accepted: %s -> %s", request.from_server, self.server_id)
      return game_pb2.PlayerHandoffResponse(success=True, message="Handoff complete")

  def RequestMushroomLock(self, request, context):
      """
      Ricart-Agrawala REQUEST handler.

      Peer asks permission to enter pickup critical section. We apply defer/grant
      rules locally and reply synchronously (GRANT or implicit DEFER via granted=False).
      """
      granted = self.mutex.on_peer_lock_request(
          request.requester_server,
          request.mushroom_id,
          request.player_id,
          request.lamport_timestamp,
      )
      return game_pb2.MushroomLockReply(
          granted=granted,
          message="GRANT" if granted else "DEFER",
      )

  def ReleaseMushroomLock(self, request, context):
      self.mutex.on_peer_lock_release(request.lamport_timestamp)
      return game_pb2.MushroomLockReleaseAck(success=True)

  def GetActivePlayers(self, request, context):
      return game_pb2.ActivePlayersResponse(
          players=[_player_to_proto(p) for p in self.state.all_players()],
      )

  def GetActiveMushrooms(self, request, context):
      return game_pb2.ActiveMushroomsResponse(
          mushrooms=[_mushroom_to_proto(m) for m in self.state.all_mushrooms()],
      )

  def EnsureMatchStarted(self, request, context):
      self.match.ensure_started()
      info = self.match.to_dict()
      return game_pb2.EnsureMatchStartedResponse(
          state=game_pb2.MatchState(
              end_time_unix=int(info["end_time_unix"]),
              game_over=info["game_over"],
              winner_name=info["winner_name"],
              winner_player_id=info["winner_player_id"],
              winner_score=info["winner_score"],
          )
      )

  def SpawnMushroom(self, request, context):
      mushroom = self.state.try_spawn_random_mushroom()
      if not mushroom:
          return game_pb2.SpawnMushroomResponse(success=False)
      self.publisher.publish(EventType.MUSHROOM_SPAWNED, mushroom.to_dict())
      return game_pb2.SpawnMushroomResponse(
          success=True,
          mushroom=_mushroom_to_proto(mushroom),
      )

  def EnsureMushroomQuota(self, request, context):
      self.match.ensure_mushroom_quota()
      return game_pb2.EnsureMushroomQuotaAck(success=True)

  def NotifyGameEnded(self, request, context):
      self.match.receive_game_ended(
          {
              "end_time_unix": request.state.end_time_unix,
              "winner_name": request.state.winner_name,
              "winner_player_id": request.state.winner_player_id,
              "winner_score": request.state.winner_score,
          }
      )
      self.publisher.publish(
          EventType.GAME_ENDED,
          {
              "winner_name": request.state.winner_name,
              "winner_player_id": request.state.winner_player_id,
              "winner_score": request.state.winner_score,
              "end_time_unix": request.state.end_time_unix,
          },
      )
      return game_pb2.NotifyGameEndedAck(success=True)

  def NotifyMushroomRemoved(self, request, context):
      """Peer removed a mushroom — mirror deletion and score if player known."""
      self.state.remove_mushroom(request.mushroom_id)
      player = self.state.get_player(request.removed_by_player)
      if player:
          player.score = request.player_new_score
      self.publisher.publish(
          EventType.MUSHROOM_REMOVED,
          {
              "mushroom_id": request.mushroom_id,
              "player_id": request.removed_by_player,
          },
      )
      return game_pb2.MushroomRemovedAck(success=True)


def serve_grpc(
    server_id: str,
    port: int,
    state: "RegionGameState",
    mutex: "RicartAgrawalaMutex",
    peer: "PeerRegionClient",
    publisher: "RabbitPublisher",
    match: "MatchManager",
) -> grpc.Server:
    """Create and start a combined gRPC server exposing both services."""
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    game_pb2_grpc.add_GameServiceServicer_to_server(
        GameServiceServicer(server_id, state, mutex, peer, publisher, match),
        grpc_server,
    )
    game_pb2_grpc.add_RegionSyncServicer_to_server(
        RegionSyncServicer(server_id, state, mutex, publisher, match),
        grpc_server,
    )
    grpc_server.add_insecure_port(f"[::]:{port}")
    grpc_server.start()
    logger.info("gRPC listening on port %d (%s)", port, server_id)
    return grpc_server
