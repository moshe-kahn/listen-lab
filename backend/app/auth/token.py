from __future__ import annotations

from datetime import UTC, datetime

from fastapi import HTTPException, Request, status

from backend.app.auth.session import _require_user_id
from backend.app.spotify_token_store import SpotifyTokenStoreError, refresh_access_token_if_needed
from backend.app.utils.time_helpers import _parse_iso_utc


def _require_token(request: Request) -> str:
    user_id = _require_user_id(request)
    try:
        token_row = refresh_access_token_if_needed(user_id)
    except SpotifyTokenStoreError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Spotify session expired. Please reconnect Spotify. ({exc})",
        ) from exc

    request.session["user_id"] = user_id
    request.session["token_type"] = "Bearer"
    expires_at = str(token_row.get("expires_at") or "")
    if expires_at:
        try:
            remaining = int((_parse_iso_utc(expires_at) - datetime.now(UTC)).total_seconds())
            request.session["expires_in"] = max(0, remaining)
        except ValueError:
            request.session["expires_in"] = None
    return str(token_row["access_token"])


async def _refresh_spotify_access_token(request: Request) -> str:
    return _require_token(request)
