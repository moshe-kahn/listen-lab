from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from backend.app.spotify_http import _spotify_get, _spotify_post, _spotify_put

QUEUE_PLAYLIST_NAME = "ListenLab Queue"
MAX_QUEUE_PLAYLIST_URIS = 100
SPOTIFY_API_BASE_URL = "https://api.spotify.com/v1"


def validate_queue_playlist_uris(value: Any) -> list[str]:
    if not isinstance(value, list) or not value:
        raise HTTPException(status_code=400, detail="uris must be a non-empty list.")

    uris: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.startswith("spotify:track:"):
            raise HTTPException(status_code=400, detail="Only spotify:track: URIs are supported.")
        uris.append(item)

    return uris[:MAX_QUEUE_PLAYLIST_URIS]


async def _find_queue_playlist(access_token: str, spotify_user_id: str) -> dict[str, Any] | None:
    offset = 0
    limit = 50
    while True:
        payload = await _spotify_get(
            access_token,
            f"{SPOTIFY_API_BASE_URL}/me/playlists",
            {"limit": limit, "offset": offset},
        )
        items = payload.get("items") or []
        for item in items:
            if not isinstance(item, dict):
                continue
            owner = item.get("owner") or {}
            if (
                item.get("name") == QUEUE_PLAYLIST_NAME
                and item.get("public") is False
                and owner.get("id") == spotify_user_id
            ):
                return item

        if len(items) < limit:
            break
        offset += len(items)
    return None


async def _create_queue_playlist(access_token: str, spotify_user_id: str) -> dict[str, Any]:
    return await _spotify_post(
        access_token,
        f"{SPOTIFY_API_BASE_URL}/users/{spotify_user_id}/playlists",
        {
            "name": QUEUE_PLAYLIST_NAME,
            "public": False,
            "description": "Private playback queue mirrored by ListenLab.",
        },
    )


def _playlist_result(playlist: dict[str, Any], item_count: int) -> dict[str, Any]:
    external_urls = playlist.get("external_urls") or {}
    return {
        "playlist_id": playlist.get("id"),
        "playlist_uri": playlist.get("uri"),
        "playlist_url": external_urls.get("spotify"),
        "name": playlist.get("name") or QUEUE_PLAYLIST_NAME,
        "item_count": item_count,
    }


async def sync_queue_playlist(
    *,
    access_token: str,
    spotify_user_id: str,
    uris: list[str],
) -> dict[str, Any]:
    playlist = await _find_queue_playlist(access_token, spotify_user_id)
    if playlist is None:
        playlist = await _create_queue_playlist(access_token, spotify_user_id)

    playlist_id = str(playlist.get("id") or "")
    if not playlist_id:
        raise HTTPException(status_code=502, detail="Spotify did not return a playlist id.")

    capped_uris = uris[:MAX_QUEUE_PLAYLIST_URIS]
    await _spotify_put(
        access_token,
        f"{SPOTIFY_API_BASE_URL}/playlists/{playlist_id}/tracks",
        {"uris": capped_uris},
    )

    return _playlist_result(playlist, len(capped_uris))
