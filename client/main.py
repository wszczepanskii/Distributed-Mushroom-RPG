"""
Pygame client — simple top-down grid RPG.

Controls:
  WASD / Arrow keys — move
  SPACE             — pick up mushroom at current tile
  ESC               — quit

Run two instances for 2-player testing:
  python -m client.main --name Alice --server localhost:50051
  python -m client.main --name Bob   --server localhost:50052
"""

from __future__ import annotations

import argparse
import queue
import sys

import pygame

from shared.config import (
    COLOR_BG,
    COLOR_BORDER,
    COLOR_GRID,
    COLOR_MUSHROOM,
    COLOR_PLAYER1,
    COLOR_PLAYER2,
    COLOR_TEXT,
    SERVER_CLIENT_ADDRESSES,
    SERVER1_ID,
    TILE_SIZE,
)
from shared.events import EventType
from client.grpc_client import GameGrpcClient
from client.rabbitmq_consumer import ClientWorldView, RabbitConsumerThread


def draw_world(screen, font, world: ClientWorldView, local_player_id: str):
    players, mushrooms, map_w, map_h, border_x = world.snapshot()
    screen.fill(COLOR_BG)

    # Grid
    for x in range(map_w + 1):
        px = x * TILE_SIZE
        color = COLOR_BORDER if x == border_x else COLOR_GRID
        pygame.draw.line(screen, color, (px, 0), (px, map_h * TILE_SIZE), 1)
    for y in range(map_h + 1):
        py = y * TILE_SIZE
        pygame.draw.line(screen, COLOR_GRID, (0, py), (map_w * TILE_SIZE, py), 1)

    # Region labels
    label = font.render("Server 1 (Left)", True, COLOR_TEXT)
    screen.blit(label, (8, 8))
    label2 = font.render("Server 2 (Right)", True, COLOR_TEXT)
    screen.blit(label2, (border_x * TILE_SIZE + 8, 8))

    # Mushrooms
    for m in mushrooms.values():
        rect = pygame.Rect(
            m["x"] * TILE_SIZE + 8,
            m["y"] * TILE_SIZE + 8,
            TILE_SIZE - 16,
            TILE_SIZE - 16,
        )
        pygame.draw.ellipse(screen, COLOR_MUSHROOM, rect)

    # Players
    for i, p in enumerate(players.values()):
        color = COLOR_PLAYER1 if i % 2 == 0 else COLOR_PLAYER2
        if p["player_id"] == local_player_id:
            color = tuple(min(255, c + 40) for c in color)
        cx = p["x"] * TILE_SIZE + TILE_SIZE // 2
        cy = p["y"] * TILE_SIZE + TILE_SIZE // 2
        pygame.draw.circle(screen, color, (cx, cy), TILE_SIZE // 3)
        name_surf = font.render(f"{p['name']} ({p['score']})", True, COLOR_TEXT)
        screen.blit(name_surf, (p["x"] * TILE_SIZE, p["y"] * TILE_SIZE - 18))


def main():
    parser = argparse.ArgumentParser(description="Mushroom RPG Pygame client")
    parser.add_argument("--name", default="Player", help="Display name")
    parser.add_argument(
        "--server",
        default=SERVER_CLIENT_ADDRESSES[SERVER1_ID],
        help="gRPC address host:port (default: server1)",
    )
    args = parser.parse_args()

    pygame.init()
    screen = pygame.display.set_mode((40 * TILE_SIZE, 20 * TILE_SIZE + 40))
    pygame.display.set_caption("Distributed Mushroom RPG")
    clock = pygame.time.Clock()
    font = pygame.font.SysFont("consolas", 14)

    grpc_client = GameGrpcClient(args.server)
    grpc_client.connect()

    join_resp = grpc_client.join(args.name)
    if not join_resp.success:
        print("Join failed:", join_resp.message)
        sys.exit(1)

    world = ClientWorldView()
    world.load_initial(join_resp.initial_state)

    event_queue: queue.Queue = queue.Queue()
    consumer = RabbitConsumerThread(event_queue)
    consumer.start()

    status_msg = f"Connected to {grpc_client.server_id} as {args.name}"
    running = True

    while running:
        # Apply RabbitMQ deltas
        while True:
            try:
                event_type, payload = event_queue.get_nowait()
                world.apply_event(event_type, payload)
            except queue.Empty:
                break

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            elif event.type == pygame.KEYDOWN:
                if event.key == pygame.K_ESCAPE:
                    running = False
                elif event.key in (pygame.K_UP, pygame.K_w):
                    resp, refreshed = grpc_client.move(0, -1)
                    if not resp.success:
                        status_msg = resp.message
                    elif resp.handoff_required:
                        if refreshed:
                            world.load_initial(refreshed)
                        status_msg = f"Handed off to {grpc_client.server_id}"
                elif event.key in (pygame.K_DOWN, pygame.K_s):
                    resp, refreshed = grpc_client.move(0, 1)
                    if resp.handoff_required:
                        if refreshed:
                            world.load_initial(refreshed)
                        status_msg = f"Handed off to {grpc_client.server_id}"
                elif event.key in (pygame.K_LEFT, pygame.K_a):
                    resp, refreshed = grpc_client.move(-1, 0)
                    if resp.handoff_required:
                        if refreshed:
                            world.load_initial(refreshed)
                        status_msg = f"Handed off to {grpc_client.server_id}"
                elif event.key in (pygame.K_RIGHT, pygame.K_d):
                    resp, refreshed = grpc_client.move(1, 0)
                    if resp.handoff_required:
                        if refreshed:
                            world.load_initial(refreshed)
                        status_msg = f"Handed off to {grpc_client.server_id}"
                elif event.key == pygame.K_SPACE:
                    resp = grpc_client.pickup()
                    status_msg = resp.message

        draw_world(screen, font, world, grpc_client.player_id or "")

        hud = font.render(
            f"{status_msg} | WASD move, SPACE pickup, ESC quit",
            True,
            COLOR_TEXT,
        )
        screen.blit(hud, (8, 20 * TILE_SIZE + 10))
        pygame.display.flip()
        clock.tick(30)

    grpc_client.leave()
    grpc_client.close()
    consumer.stop()
    pygame.quit()


if __name__ == "__main__":
    main()
