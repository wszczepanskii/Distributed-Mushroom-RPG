"""
Global game and infrastructure configuration.

The map is split horizontally: Server 1 owns columns [0, BORDER_X),
Server 2 owns columns [BORDER_X, MAP_WIDTH).
"""

from __future__ import annotations

# --- Map geometry (grid cells) ---
MAP_WIDTH = 40
MAP_HEIGHT = 20
TILE_SIZE = 32
BORDER_X = MAP_WIDTH // 2  # vertical border between regions

# --- Network defaults ---
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASSWORD = "guest"
RABBITMQ_EXCHANGE = "game.updates"  # fanout: all clients receive broadcasts

SERVER1_GRPC_HOST = "localhost"
SERVER1_GRPC_PORT = 50051
SERVER2_GRPC_HOST = "localhost"
SERVER2_GRPC_PORT = 50052

SERVER1_ID = "server1"
SERVER2_ID = "server2"

# Peer addresses used for inter-server gRPC (server1 -> server2 and vice versa)
SERVER_PEER_ADDRESSES = {
    SERVER1_ID: f"{SERVER2_GRPC_HOST}:{SERVER2_GRPC_PORT}",
    SERVER2_ID: f"{SERVER1_GRPC_HOST}:{SERVER1_GRPC_PORT}",
}

SERVER_CLIENT_ADDRESSES = {
    SERVER1_ID: f"{SERVER1_GRPC_HOST}:{SERVER1_GRPC_PORT}",
    SERVER2_ID: f"{SERVER2_GRPC_HOST}:{SERVER2_GRPC_PORT}",
}

# Mushrooms within this many cells of the border require distributed locking
# because players from either region can reach them at nearly the same time.
BORDER_LOCK_RADIUS = 2

# Gameplay
MATCH_DURATION_SECONDS = 120  # 2-minute rounds
ACTIVE_MUSHROOM_COUNT = 5     # always maintain this many mushrooms on the map
PLAYER_MOVE_COOLDOWN_MS = 100

# Colors (RGB) for Pygame client
COLOR_BG = (34, 45, 34)
COLOR_GRID = (45, 58, 45)
COLOR_BORDER = (200, 180, 60)
COLOR_PLAYER1 = (80, 160, 255)
COLOR_PLAYER2 = (255, 120, 80)
COLOR_MUSHROOM = (220, 60, 80)
COLOR_TEXT = (240, 240, 230)


def region_for_x(x: int) -> str:
    """Return SERVER1_ID or SERVER2_ID based on grid x coordinate."""
    if x < BORDER_X:
        return SERVER1_ID
    return SERVER2_ID


def is_near_border(x: int) -> bool:
    """True when entity is close enough to the region border to need distributed locks."""
    return abs(x - BORDER_X) <= BORDER_LOCK_RADIUS
