"""
gRPC client wrapper for GameService.

Handles transparent server handoff when the player crosses the map border.
"""

from __future__ import annotations

from typing import Optional, Tuple

import grpc

from generated import game_pb2, game_pb2_grpc


class GameGrpcClient:
    def __init__(self, server_address: str):
        self.server_address = server_address
        self._channel: Optional[grpc.Channel] = None
        self._stub: Optional[game_pb2_grpc.GameServiceStub] = None
        self.player_id: Optional[str] = None
        self.server_id: Optional[str] = None

    def connect(self) -> None:
        self._channel = grpc.insecure_channel(self.server_address)
        self._stub = game_pb2_grpc.GameServiceStub(self._channel)

    def reconnect(self, new_address: str) -> None:
        if self._channel:
            self._channel.close()
        self.server_address = new_address
        self.connect()

    def close(self) -> None:
        if self._channel:
            self._channel.close()

    def join(self, name: str) -> game_pb2.JoinResponse:
        assert self._stub
        response = self._stub.JoinGame(game_pb2.JoinRequest(name=name), timeout=5)
        if response.success:
            self.player_id = response.player_id
            self.server_id = response.server_id
        return response

    def move(self, dx: int, dy: int) -> Tuple[game_pb2.MoveResponse, Optional[game_pb2.GameState]]:
        assert self._stub and self.player_id
        response = self._stub.MovePlayer(
            game_pb2.MoveRequest(player_id=self.player_id, dx=dx, dy=dy),
            timeout=5,
        )
        refreshed_state = None
        if response.handoff_required and response.new_server_address:
            self.reconnect(response.new_server_address)
            self.server_id = response.new_server_id
            refreshed_state = self.get_game_state()
        return response, refreshed_state

    def get_game_state(self) -> game_pb2.GameState:
        assert self._stub and self.player_id
        return self._stub.GetGameState(
            game_pb2.StateRequest(player_id=self.player_id),
            timeout=5,
        )

    def pickup(self) -> game_pb2.PickupResponse:
        assert self._stub and self.player_id
        return self._stub.PickupMushroom(
            game_pb2.PickupRequest(player_id=self.player_id),
            timeout=10,
        )

    def leave(self) -> None:
        if self._stub and self.player_id:
            self._stub.LeaveGame(
                game_pb2.LeaveRequest(player_id=self.player_id),
                timeout=3,
            )
