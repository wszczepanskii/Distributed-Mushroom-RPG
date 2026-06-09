"""
In-memory domain models shared by server components.

These are plain Python objects; gRPC/protobuf messages are built from them
in the servicer layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import uuid


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@dataclass
class Player:
    player_id: str
    name: str
    x: int
    y: int
    score: int = 0
    connected_server: str = ""

    def to_dict(self) -> dict:
        return {
            "player_id": self.player_id,
            "name": self.name,
            "x": self.x,
            "y": self.y,
            "score": self.score,
            "connected_server": self.connected_server,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Player":
        return cls(
            player_id=data["player_id"],
            name=data["name"],
            x=data["x"],
            y=data["y"],
            score=data.get("score", 0),
            connected_server=data.get("connected_server", ""),
        )


@dataclass
class Mushroom:
    mushroom_id: str
    x: int
    y: int
    owner_region: str

    def to_dict(self) -> dict:
        return {
            "mushroom_id": self.mushroom_id,
            "x": self.x,
            "y": self.y,
            "owner_region": self.owner_region,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Mushroom":
        return cls(
            mushroom_id=data["mushroom_id"],
            x=data["x"],
            y=data["y"],
            owner_region=data["owner_region"],
        )


@dataclass
class WorldState:
    """Authoritative per-cluster view; servers merge peer knowledge via RabbitMQ + gRPC."""

    players: Dict[str, Player] = field(default_factory=dict)
    mushrooms: Dict[str, Mushroom] = field(default_factory=dict)

    def players_list(self) -> List[Player]:
        return list(self.players.values())

    def mushrooms_list(self) -> List[Mushroom]:
        return list(self.mushrooms.values())
