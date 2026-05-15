from __future__ import annotations

from datetime import UTC, datetime

from fastapi import HTTPException, Request, status

from backend.app.db import list_spotify_auth_users
from backend.app.spotify_token_store import SpotifyTokenStoreError, refresh_access_token_if_needed
from backend.app.utils.time_helpers import _parse_iso_utc


def _session_user_id(request: Request) -> str | None:
    user_id = request.session.get("user_id")
    if user_id:
        return str(user_id)
    spotify_user = request.session.get("spotify_user") or {}
    if spotify_user.get("id"):
        return str(spotify_user["id"])
    return None


def _require_user_id(request: Request) -> str:
    user_id = _session_user_id(request)
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated with Spotify.",
        )
    return user_id


def _restore_session_user_from_token_store(request: Request) -> str | None:
    existing_user_id = _session_user_id(request)
    if existing_user_id:
        return existing_user_id

    active_users = list_spotify_auth_users(active_only=True, limit=2)
    if len(active_users) != 1:
        return None

    candidate_user_id = str(active_users[0].get("user_id") or "").strip()
    if not candidate_user_id:
        return None

    try:
        token_row = refresh_access_token_if_needed(candidate_user_id)
    except SpotifyTokenStoreError:
        return None

    request.session["user_id"] = candidate_user_id
    request.session["token_type"] = "Bearer"
    expires_at = str(token_row.get("expires_at") or "")
    if expires_at:
        try:
            remaining = int((_parse_iso_utc(expires_at) - datetime.now(UTC)).total_seconds())
            request.session["expires_in"] = max(0, remaining)
        except ValueError:
            request.session["expires_in"] = None
    request.session["spotify_user"] = {
        "id": str(token_row.get("spotify_user_id") or candidate_user_id),
        "display_name": None,
        "email": None,
    }
    return candidate_user_id
