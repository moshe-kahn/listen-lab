from __future__ import annotations

from typing import Any

from backend.app.spotify_http import _spotify_get
from backend.app.spotify_normalization import _normalize_track


async def _fetch_album_track_refs(
    access_token: str,
    album_id: str,
    max_tracks: int = 50,
    market: str | None = None,
) -> list[dict[str, Any]]:
    offset = 0
    limit = 50
    tracks: list[dict[str, Any]] = []

    while offset < max_tracks:
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if market:
            params["market"] = market
        payload = await _spotify_get(
            access_token,
            f"https://api.spotify.com/v1/albums/{album_id}/tracks",
            params,
        )
        items = payload.get("items") or []
        if not items:
            break

        tracks.extend(items)
        offset += len(items)
        if len(items) < limit:
            break

    return tracks[:max_tracks]


async def _fetch_tracks_by_ids(
    access_token: str,
    track_ids: list[str],
    market: str | None = None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for start in range(0, len(track_ids), 50):
        batch = [track_id for track_id in track_ids[start:start + 50] if track_id]
        if not batch:
            continue
        params: dict[str, Any] = {"ids": ",".join(batch)}
        if market:
            params["market"] = market
        payload = await _spotify_get(
            access_token,
            "https://api.spotify.com/v1/tracks",
            params,
        )
        results.extend([track for track in (payload.get("tracks") or []) if track])
    return results


def _choose_representative_track(
    tracks: list[dict[str, Any]],
    album_track_numbers: dict[str, int] | None = None,
) -> dict[str, Any] | None:
    if not tracks:
        return None

    def sort_key(track: dict[str, Any]) -> tuple[int, int, int, str]:
        track_id = track.get("id") or ""
        preview_bonus = 1 if track.get("preview_url") else 0
        popularity = int(track.get("popularity") or 0)
        track_number = 9999
        if album_track_numbers:
            track_number = int(album_track_numbers.get(track_id, track.get("track_number") or 9999))
        else:
            track_number = int(track.get("track_number") or 9999)
        return (preview_bonus, popularity, -track_number, track.get("name") or "")

    return sorted(tracks, key=sort_key, reverse=True)[0]


async def _fetch_artist_representative_track(
    access_token: str,
    artist_id: str,
    market: str | None = None,
) -> dict[str, Any] | None:
    params = {"market": market} if market else None
    payload = await _spotify_get(
        access_token,
        f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks",
        params,
    )
    track = _choose_representative_track(payload.get("tracks") or [])
    return _normalize_track(track) if track else None


async def _fetch_album_representative_track(
    access_token: str,
    album_id: str,
    market: str | None = None,
) -> dict[str, Any] | None:
    album_tracks = await _fetch_album_track_refs(access_token, album_id, market=market)
    ordered_ids = [item.get("id") for item in album_tracks if item.get("id")]
    if not ordered_ids:
        return None

    track_number_lookup = {
        item["id"]: int(item.get("track_number") or 9999)
        for item in album_tracks
        if item.get("id")
    }
    full_tracks = await _fetch_tracks_by_ids(access_token, ordered_ids, market=market)
    track = _choose_representative_track(full_tracks, album_track_numbers=track_number_lookup)
    return _normalize_track(track) if track else None
