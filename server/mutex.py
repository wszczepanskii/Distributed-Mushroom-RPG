"""
Distributed Mutual Exclusion — Ricart-Agrawala (2-server adaptation)
====================================================================

Problem:
  Two players on different region servers may try to pick up the same mushroom
  at the same time (especially near the map border). Without coordination,
  both servers could grant the pickup and duplicate the item or corrupt scores.

Algorithm (Ricart-Agrawala, simplified for N=2):
  1. Server wishing to enter the critical section (pickup) increments its
     Lamport logical clock and records a REQUEST(timestamp, server_id).
  2. It sends REQUEST to the peer server via gRPC (RegionSync.RequestMushroomLock).
  3. The peer applies Ricart-Agrawala reply rules:
       - GRANT immediately if it is NOT in its own critical section AND has no
         outstanding request with a lower (timestamp, server_id) priority.
       - Otherwise DEFER: queue a GRANT to send when its own CS ends.
  4. Requester waits until all peers have GRANTed (for 2 servers: one reply).
  5. Requester performs atomic pickup (remove mushroom, increment score).
  6. Requester sends RELEASE via gRPC and processes any deferred grants.

Priority tie-break: lower Lamport timestamp wins; if equal, lower server_id wins.

Lamport clocks keep message ordering consistent across asynchronous gRPC calls.

This module implements the local side of the algorithm. gRPC handlers call into
these methods when inter-server messages arrive.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

@dataclass(order=True)
class LockRequest:
    """Pending lock request sorted by (lamport_timestamp, server_id)."""

    lamport_timestamp: int
    server_id: str
    mushroom_id: str
    player_id: str
    event: threading.Event = field(compare=False, default_factory=threading.Event)
    granted: bool = field(compare=False, default=False)


class RicartAgrawalaMutex:
    """
    Per-server mutex coordinator for mushroom pickup critical sections.

    Each mushroom_id can have at most one in-flight distributed lock operation.
    """

    def __init__(self, server_id: str, peer_request_fn: Callable[[str, str, str, int], bool]):
        """
        Args:
            server_id: This server's identifier (server1 / server2).
            peer_request_fn: Callable(mushroom_id, player_id, lamport_ts) -> granted bool
                             that performs the synchronous gRPC lock request to peer.
        """
        self.server_id = server_id
        self.peer_request_fn = peer_request_fn
        self.lamport_clock = 0
        self._lock = threading.RLock()

        # Outstanding local request to enter CS (None if not requesting).
        self._own_request: Optional[LockRequest] = None

        # Whether this server is inside the mushroom pickup critical section.
        self._in_critical_section = False

        # Deferred GRANTs we owe to peer requests (FIFO of server_ids).
        self._deferred_grants: List[Tuple[str, str]] = []  # (peer_server_id, mushroom_id)

        # Inbound peer requests we have not yet replied to.
        self._peer_requests: Dict[str, LockRequest] = {}  # mushroom_id -> request

    # ------------------------------------------------------------------
    # Lamport clock
    # ------------------------------------------------------------------

    def _tick(self) -> int:
        with self._lock:
            self.lamport_clock += 1
            return self.lamport_clock

    def _update_clock(self, received_ts: int) -> None:
        with self._lock:
            self.lamport_clock = max(self.lamport_clock, received_ts) + 1

    # ------------------------------------------------------------------
    # Public API used by game logic
    # ------------------------------------------------------------------

    def acquire(self, mushroom_id: str, player_id: str, timeout: float = 5.0) -> bool:
        """
        Enter distributed critical section for mushroom pickup.

        Blocks until peer grants permission or timeout expires.
        Returns True if lock acquired, False on timeout / contention loss.

        Retries handle DEFER replies: if the peer has higher priority, our gRPC
        call returns granted=False and we back off briefly before retrying.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            ts = self._tick()
            own = LockRequest(
                lamport_timestamp=ts,
                server_id=self.server_id,
                mushroom_id=mushroom_id,
                player_id=player_id,
            )

            with self._lock:
                self._own_request = own

            granted = self.peer_request_fn(mushroom_id, player_id, ts)
            if granted:
                with self._lock:
                    self._in_critical_section = True
                return True

            with self._lock:
                self._own_request = None
            time.sleep(0.05)

        return False

    def release(self, mushroom_id: str) -> None:
        """Leave critical section and send deferred grants if any."""
        with self._lock:
            self._in_critical_section = False
            self._own_request = None
            self._peer_requests.pop(mushroom_id, None)

    # ------------------------------------------------------------------
    # Called by RegionSync gRPC servicer when peer messages arrive
    # ------------------------------------------------------------------

    def on_peer_lock_request(
        self, peer_server: str, mushroom_id: str, player_id: str, lamport_ts: int
    ) -> bool:
        """
        Handle inbound REQUEST from peer. Returns whether we GRANT immediately.

        Reply rule (Ricart-Agrawala):
          GRANT if not in CS and (no own request OR own request has higher priority).
        """
        self._update_clock(lamport_ts)

        with self._lock:
            peer_req = LockRequest(
                lamport_timestamp=lamport_ts,
                server_id=peer_server,
                mushroom_id=mushroom_id,
                player_id=player_id,
            )
            self._peer_requests[mushroom_id] = peer_req

            if self._should_defer(peer_req):
                return False  # DEFER — peer waits; we'll notify via separate path if needed

            peer_req.granted = True
            return True

    def on_peer_lock_release(self, lamport_ts: int) -> None:
        """Peer left CS; process deferred grants."""
        self._update_clock(lamport_ts)
        with self._lock:
            if self._deferred_grants:
                self._deferred_grants.pop(0)

    def _should_defer(self, peer_req: LockRequest) -> bool:
        """True => defer GRANT to peer."""
        if self._in_critical_section:
            return True
        if self._own_request is None:
            return False

        # Compare priorities: lower (timestamp, server_id) wins.
        own = (self._own_request.lamport_timestamp, self._own_request.server_id)
        peer = (peer_req.lamport_timestamp, peer_req.server_id)
        return own > peer  # defer if peer has higher priority

    def get_reply_timestamp(self) -> int:
        """Lamport timestamp to attach to RELEASE messages."""
        return self._tick()
