"""
JSON event payloads published to RabbitMQ.

Fanout exchange means every connected client receives the same events without
routing keys. Servers publish after authoritative state changes.
"""

from __future__ import annotations

import json
from enum import Enum
from typing import Any, Dict


class EventType(str, Enum):
    PLAYER_JOINED = "player_joined"
    PLAYER_LEFT = "player_left"
    PLAYER_MOVED = "player_moved"
    PLAYER_HANDOFF = "player_handoff"
    MUSHROOM_SPAWNED = "mushroom_spawned"
    MUSHROOM_REMOVED = "mushroom_removed"
    SCORE_UPDATED = "score_updated"


def encode_event(event_type: EventType, payload: Dict[str, Any]) -> bytes:
    body = {"type": event_type.value, "payload": payload}
    return json.dumps(body).encode("utf-8")


def decode_event(body: bytes) -> tuple[EventType, Dict[str, Any]]:
    data = json.loads(body.decode("utf-8"))
    return EventType(data["type"]), data["payload"]
