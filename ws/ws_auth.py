"""
ws_auth.py — Session token validation for WebSocket connections.

The project uses custom session token auth (httpOnly cookie), not JWT.
When the browser opens a WebSocket to the same origin, it automatically
includes the session_token httpOnly cookie in the HTTP upgrade headers —
no manual token passing is needed from the Angular side.

Public function:
    validate_connection(websocket, revision_id) → username str | None
"""

import http.cookies
import logging

from workflow.engine.db.db_sync import get_revision_owner, validate_session_token

logger = logging.getLogger(__name__)


def validate_connection(websocket, revision_id: int) -> str | None:
    """
    Authenticate a WebSocket connection using the session_token cookie.

    Reads the Cookie header from the WS HTTP upgrade request.
    Validates the session token against the DB.
    Checks the authenticated user owns the requested revision.

    Returns the username on success, None on any failure.
    """
    # Read cookie header (websockets library normalises header names to title-case)
    cookie_header = (
        websocket.request.headers.get('Cookie')
        or websocket.request.headers.get('cookie')
        or ''
    )

    if not cookie_header:
        logger.warning('WS auth rejected: no Cookie header for revision %s', revision_id)
        return None

    jar = http.cookies.SimpleCookie()
    jar.load(cookie_header)

    morsel = jar.get('sessionid')
    if morsel is None:
        logger.warning('WS auth rejected: no sessionid cookie for revision %s', revision_id)
        return None

    session_key = morsel.value

    try:
        username = validate_session_token(session_key)
    except Exception as exc:
        logger.error('WS auth: DB error validating session for revision %s — %s', revision_id, exc)
        return None

    if username is None:
        logger.warning('WS auth rejected: invalid/expired session for revision %s', revision_id)
        return None

    try:
        owner = get_revision_owner(revision_id)
    except Exception as exc:
        logger.error('WS auth: DB error fetching revision owner %s — %s', revision_id, exc)
        return None

    if owner is None:
        logger.warning('WS auth rejected: revision %s not found', revision_id)
        return None

    if username != owner:
        logger.warning(
            'WS auth rejected: user %s does not own revision %s (owner: %s)',
            username, revision_id, owner,
        )
        return None

    return username
