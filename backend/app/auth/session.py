from __future__ import annotations

from fastapi import HTTPException, Request, status


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
