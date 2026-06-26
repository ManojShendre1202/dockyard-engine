"""
signal_listener.py — Thread A: TCP signal receiver.

Django connects to 127.0.0.1:{SIGNAL_PORT} over TCP, sends a payload of the
form "{revision_id}:{stage_name}" (e.g. "42:Node Identification"), and closes
the connection. This listener accepts each connection, reads the payload, and
puts a (revision_id, stage_name) tuple into notify_queue for the Dispatcher.

The stage_name is explicit in the signal — the Dispatcher never needs to guess
which stage to run next from DB state.

notify_queue is a module-level singleton imported by the Dispatcher.
"""

import logging
import os
import socket
import threading

logger = logging.getLogger(__name__)

SIGNAL_PORT  = int(os.environ.get('SIGNAL_PORT', 9000))
SIGNAL_HOST  = '127.0.0.1'
BUFFER_SIZE  = 128  # "revision_id:stage_name" — 128 bytes is more than enough

# Module-level queue — Dispatcher imports this directly
# Each item is a (revision_id: int, stage_name: str) tuple
notify_queue: 'queue.Queue[tuple[int, str]]'

import queue as _queue_module
notify_queue = _queue_module.Queue()


def _listen() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((SIGNAL_HOST, SIGNAL_PORT))
    sock.listen(5)
    logger.info('Signal listener bound to %s:%d (TCP)', SIGNAL_HOST, SIGNAL_PORT)

    while True:
        try:
            conn, _ = sock.accept()
            with conn:
                data = conn.recv(BUFFER_SIZE)
                if not data:
                    continue
                payload = data.decode().strip()
                if ':' not in payload:
                    logger.warning('Signal listener received payload with no stage_name: %r — ignoring', payload)
                    conn.send(b'OK')
                    continue
                revision_id_str, stage_name = payload.split(':', 1)
                revision_id = int(revision_id_str)
                logger.info('Signal received: revision_id=%d stage="%s"', revision_id, stage_name)
                notify_queue.put((revision_id, stage_name))
                conn.send(b'OK')  # acknowledge so Django knows the signal was received
        except ValueError:
            logger.warning('Signal listener received non-integer revision_id in payload: %r', data)
        except Exception as exc:
            logger.error('Signal listener error: %s', exc)


def start_signal_listener() -> threading.Thread:
    """
    Start the TCP listener in a daemon thread and return it.
    Daemon thread — dies automatically when the main process exits.
    """
    t = threading.Thread(target=_listen, name='SignalListener', daemon=True)
    t.start()
    return t
