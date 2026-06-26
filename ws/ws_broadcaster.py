"""
ws_broadcaster.py

The correct pattern for worker thread → asyncio WebSocket delivery:

    broadcaster.send(revision_id, msg)          ← called from any worker thread
        → asyncio.run_coroutine_threadsafe()    ← schedules on the event loop immediately
            → _deliver() coroutine              ← runs in the asyncio thread, sends to browser

No polling loop. No queue. No 50 ms lag.
`run_coroutine_threadsafe` is specifically designed for this: push work from a
non-asyncio thread onto a running event loop, thread-safe, fire-and-forget.

The GhostRoom
---------------
Every message for a revision is recorded in its own GhostRoom — a persistent
in-memory log that survives client disconnects. When a client connects (first
time or reconnect), the full GhostRoom is replayed so they receive a complete
picture of everything that happened, not just what arrives after they joined.

The GhostRoom is only cleared when the revision reaches a terminal state
(completed or failed), at which point the full result is already persisted to
the DB and the GhostRoom is no longer needed.
"""

import asyncio
import json
import logging
import threading
from collections import defaultdict

logger = logging.getLogger(__name__)


class WSBroadcaster:

    def __init__(self) -> None:
        self._lock:         threading.Lock                      = threading.Lock()
        self._clients:      dict[int, set]                      = defaultdict(set)
        self._GhostRoom:  dict[int, list]                     = defaultdict(list)
        self._loop:         asyncio.AbstractEventLoop | None    = None

    # ------------------------------------------------------------------
    # Called once when the WS server starts (asyncio thread)
    # ------------------------------------------------------------------

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Store the event loop so worker threads can schedule onto it."""
        self._loop = loop

    # ------------------------------------------------------------------
    # Called from worker threads (any thread)
    # ------------------------------------------------------------------

    def send(self, revision_id: int, message: dict) -> None:
        """
        Record in the GhostRoom and deliver to all connected clients.
        Returns immediately — delivery is fire-and-forget on the event loop.
        """
        with self._lock:
            self._GhostRoom[revision_id].append(message)

        if self._loop is None or self._loop.is_closed():
            return  # no event loop yet — message is safely stored in GhostRoom

        asyncio.run_coroutine_threadsafe(
            self._deliver(revision_id, message),
            self._loop,
        )

    def clear_GhostRoom(self, revision_id: int) -> None:
        """
        Wipe the GhostRoom for a revision that has reached a terminal state.
        Called by the Dispatcher after broadcasting 'done' (completed or failed).
        """
        with self._lock:
            self._GhostRoom.pop(revision_id, None)
        logger.debug('GhostRoom cleared for revision %s', revision_id)

    # ------------------------------------------------------------------
    # Called from the asyncio event loop thread
    # ------------------------------------------------------------------

    def register(self, revision_id: int, websocket) -> None:
        with self._lock:
            self._clients[revision_id].add(websocket)
            # Read — never pop — so the GhostRoom survives multiple reconnects
            replay = list(self._GhostRoom[revision_id])

        logger.debug('WS client registered for revision %s (%d messages in GhostRoom)',
                     revision_id, len(replay))

        if replay:
            asyncio.create_task(self._replay_GhostRoom(revision_id, websocket, replay))

    def unregister(self, revision_id: int, websocket) -> None:
        with self._lock:
            self._clients[revision_id].discard(websocket)
            if not self._clients[revision_id]:
                self._clients.pop(revision_id, None)

    # ------------------------------------------------------------------
    # Private coroutines (asyncio thread only)
    # ------------------------------------------------------------------

    async def _deliver(self, revision_id: int, message: dict) -> None:
        """Deliver one message to all clients currently connected for revision_id."""
        with self._lock:
            clients = list(self._clients.get(revision_id, set()))

        if not clients:
            return  # no clients connected — message already recorded in GhostRoom

        payload = json.dumps(message)
        dead    = []
        for ws in clients:
            try:
                await ws.send(payload)
            except Exception as exc:
                logger.warning('WS send failed for revision %s: %s', revision_id, exc)
                dead.append(ws)

        if dead:
            with self._lock:
                for ws in dead:
                    self._clients[revision_id].discard(ws)

    async def _replay_GhostRoom(self, revision_id: int, websocket, messages: list) -> None:
        """Replay the full GhostRoom history to a newly connected client, in order."""
        for message in messages:
            try:
                await websocket.send(json.dumps(message))
            except Exception as exc:
                logger.warning('GhostRoom replay failed for revision %s: %s', revision_id, exc)
                break


# Module-level singleton
broadcaster = WSBroadcaster()
