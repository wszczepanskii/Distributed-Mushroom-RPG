"""
Background RabbitMQ consumer thread.

Applies fanout events to a thread-safe snapshot the Pygame main thread reads
each frame. This decouples network I/O from rendering.
"""

from __future__ import annotations

import logging
import queue
import threading
from typing import Any, Dict, Optional

import pika

from shared.config import (
    RABBITMQ_EXCHANGE,
    RABBITMQ_HOST,
    RABBITMQ_PASSWORD,
    RABBITMQ_PORT,
    RABBITMQ_USER,
)
from shared.events import EventType, decode_event

logger = logging.getLogger(__name__)


class RabbitConsumerThread(threading.Thread):
    def __init__(self, event_queue: queue.Queue):
        super().__init__(daemon=True, name="RabbitConsumer")
        self.event_queue = event_queue
        self._stop_event = threading.Event()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._consume_loop()
            except Exception as exc:
                logger.error("RabbitMQ consumer error: %s — retrying in 3s", exc)
                self._stop_event.wait(3)

    def stop(self) -> None:
        self._stop_event.set()

    def _consume_loop(self) -> None:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=600,
        )
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.exchange_declare(exchange=RABBITMQ_EXCHANGE, exchange_type="fanout")
        result = channel.queue_declare(queue="", exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=RABBITMQ_EXCHANGE, queue=queue_name)

        def callback(ch, method, properties, body):
            try:
                event_type, payload = decode_event(body)
                self.event_queue.put((event_type, payload))
            except Exception as exc:
                logger.warning("Bad event payload: %s", exc)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
        logger.info("RabbitMQ consumer bound to '%s'", RABBITMQ_EXCHANGE)

        while not self._stop_event.is_set():
            connection.process_data_events(time_limit=1)

        connection.close()


class ClientWorldView:
    """
    Thread-safe local cache of world state driven by RabbitMQ events.

    Initial state comes from gRPC JoinGame; subsequent deltas from the broker.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self.players: Dict[str, Dict[str, Any]] = {}
        self.mushrooms: Dict[str, Dict[str, Any]] = {}
        self.map_width = 40
        self.map_height = 20
        self.border_x = 20

    def load_initial(self, game_state) -> None:
        with self._lock:
            self.map_width = game_state.map_width
            self.map_height = game_state.map_height
            self.border_x = game_state.border_x
            self.players = {
                p.player_id: {
                    "player_id": p.player_id,
                    "name": p.name,
                    "x": p.x,
                    "y": p.y,
                    "score": p.score,
                }
                for p in game_state.players
            }
            self.mushrooms = {
                m.mushroom_id: {
                    "mushroom_id": m.mushroom_id,
                    "x": m.x,
                    "y": m.y,
                }
                for m in game_state.mushrooms
            }

    def apply_event(self, event_type: EventType, payload: Dict[str, Any]) -> None:
        with self._lock:
            if event_type == EventType.PLAYER_JOINED:
                pid = payload["player_id"]
                self.players[pid] = payload
            elif event_type == EventType.PLAYER_LEFT:
                self.players.pop(payload["player_id"], None)
            elif event_type == EventType.PLAYER_MOVED:
                pid = payload["player_id"]
                if pid in self.players:
                    self.players[pid]["x"] = payload["x"]
                    self.players[pid]["y"] = payload["y"]
            elif event_type == EventType.PLAYER_HANDOFF:
                pid = payload["player_id"]
                self.players[pid] = payload
            elif event_type == EventType.MUSHROOM_SPAWNED:
                mid = payload["mushroom_id"]
                self.mushrooms[mid] = payload
            elif event_type == EventType.MUSHROOM_REMOVED:
                self.mushrooms.pop(payload["mushroom_id"], None)
            elif event_type == EventType.SCORE_UPDATED:
                pid = payload["player_id"]
                if pid in self.players:
                    self.players[pid]["score"] = payload["score"]

    def snapshot(self):
        with self._lock:
            return (
                dict(self.players),
                dict(self.mushrooms),
                self.map_width,
                self.map_height,
                self.border_x,
            )
