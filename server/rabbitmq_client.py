"""
RabbitMQ publisher for broadcasting authoritative state changes.

Architecture note:
  gRPC is used for client actions (move, pickup) and server-server coordination.
  RabbitMQ fanout is used to push read-only updates to all Pygame clients so they
  stay synchronized without polling gRPC every frame.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import pika

from shared.config import (
    RABBITMQ_EXCHANGE,
    RABBITMQ_HOST,
    RABBITMQ_PASSWORD,
    RABBITMQ_PORT,
    RABBITMQ_USER,
)
from shared.events import EventType, encode_event

logger = logging.getLogger(__name__)


class RabbitPublisher:
    def __init__(self) -> None:
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.channel.Channel] = None

    def connect(self) -> None:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
        )
        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()
        # Fanout: every bound queue receives every message (all clients).
        self._channel.exchange_declare(
            exchange=RABBITMQ_EXCHANGE,
            exchange_type="fanout",
            durable=False,
        )
        logger.info("RabbitMQ publisher connected to exchange '%s'", RABBITMQ_EXCHANGE)

    def publish(self, event_type: EventType, payload: Dict[str, Any]) -> None:
        if not self._channel:
            raise RuntimeError("RabbitPublisher not connected")
        body = encode_event(event_type, payload)
        self._channel.basic_publish(
            exchange=RABBITMQ_EXCHANGE,
            routing_key="",
            body=body,
        )

    def close(self) -> None:
        if self._connection and self._connection.is_open:
            self._connection.close()
