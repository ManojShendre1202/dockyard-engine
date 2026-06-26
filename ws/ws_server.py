"""
ws_server.py — asyncio WebSocket server (Thread C).

Listens on WS_PORT. Nginx proxies /ws/ here.
URL pattern: ws://host/ws/revision/{revision_id}

Auth is cookie-based: the browser automatically sends the session_token
httpOnly cookie in the HTTP upgrade headers.
"""

import asyncio
import logging
import os

import websockets

from workflow.engine.ws.ws_auth import validate_connection
from workflow.engine.ws.ws_broadcaster import broadcaster

logger = logging.getLogger(__name__)

WS_PORT       = int(os.environ.get('WS_PORT', 8003))
PING_INTERVAL = 25  # seconds


def _parse_revision_id(path: str) -> int | None:
    path = path.split('?')[0].rstrip('/')
    parts = path.split('/')
    # Expected: ['', 'ws', 'revision', '{id}']
    if len(parts) == 4 and parts[1] == 'ws' and parts[2] == 'revision':
        try:
            return int(parts[3])
        except ValueError:
            pass
    return None


async def _ping_loop(websocket) -> None:
    try:
        while True:
            await asyncio.sleep(PING_INTERVAL)
            await websocket.ping()
    except (websockets.ConnectionClosed, asyncio.CancelledError):
        pass


async def _handle_connection(websocket) -> None:
    path = websocket.request.path

    revision_id = _parse_revision_id(path)
    if revision_id is None:
        logger.warning('WS rejected: invalid path %s', path)
        await websocket.close(4000, 'Invalid path')
        return

    loop = asyncio.get_event_loop()
    username = await loop.run_in_executor(None, validate_connection, websocket, revision_id)
    if username is None:
        await websocket.close(4001, 'Unauthorized')
        return

    logger.info('WS client connected: user=%s revision=%s', username, revision_id)

    # register() will also flush any messages that arrived before connect
    broadcaster.register(revision_id, websocket)

    ping_task = asyncio.create_task(_ping_loop(websocket))
    try:
        async for _ in websocket:
            pass  # client-to-server messages not expected
    except websockets.ConnectionClosed:
        pass
    finally:
        ping_task.cancel()
        try:
            await ping_task
        except asyncio.CancelledError:
            pass
        broadcaster.unregister(revision_id, websocket)
        logger.info('WS client disconnected: user=%s revision=%s', username, revision_id)


async def _serve() -> None:
    loop = asyncio.get_running_loop()
    broadcaster.set_loop(loop)   # give worker threads a reference to this loop

    async with websockets.serve(_handle_connection, '0.0.0.0', WS_PORT):
        logger.info('WebSocket server listening on port %d', WS_PORT)
        await asyncio.Future()  # run forever


def start_ws_server() -> None:
    asyncio.run(_serve())
