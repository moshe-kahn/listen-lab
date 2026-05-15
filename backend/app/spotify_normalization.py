from __future__ import annotations

from typing import Any


def _normalize_artist(artist: dict[str, Any]) -> dict[str, Any]:
    images = artist.get("images") or []
    external_urls = artist.get("external_urls") or {}
    followers = artist.get("followers") or {}
    genres = artist.get("genres") or []
    return {
        "artist_id": artist.get("id"),
        "name": artist.get("name"),
        "followers_total": followers.get("total"),
        "genres": genres[:2],
        "popularity": artist.get("popularity"),
        "url": external_urls.get("spotify"),
        "image_url": images[0].get("url") if images else None,
    }


def _artist_lookup_key(artist_name: str | None) -> str | None:
    if not artist_name:
        return None
    normalized = " ".join(str(artist_name).strip().lower().split())
    return normalized or None


def _normalize_track(track: dict[str, Any]) -> dict[str, Any]:
    album = track.get("album") or {}
    artists = track.get("artists") or []
    external_urls = track.get("external_urls") or {}
    album_external_urls = album.get("external_urls") or {}
    release_date = album.get("release_date")
    duration_ms_raw = track.get("duration_ms")
    duration_ms = int(duration_ms_raw) if isinstance(duration_ms_raw, (int, float)) and int(duration_ms_raw) > 0 else None
    return {
        "track_id": track.get("id"),
        "track_name": track.get("name"),
        "artist_name": ", ".join(artist.get("name", "") for artist in artists if artist.get("name")),
        "album_name": album.get("name"),
        "album_release_year": str(release_date)[:4] if release_date else None,
        "duration_ms": duration_ms,
        "duration_seconds": round(duration_ms / 1000.0, 3) if duration_ms is not None else None,
        "uri": track.get("uri"),
        "preview_url": track.get("preview_url"),
        "url": external_urls.get("spotify"),
        "album_url": album_external_urls.get("spotify"),
        "image_url": ((album.get("images") or [{}])[0]).get("url"),
        "album_id": album.get("id"),
        "artists": [
            {
                "artist_id": artist.get("id"),
                "name": artist.get("name"),
            }
            for artist in artists
            if artist.get("name")
        ],
    }


def _album_lookup_key(album_name: str | None, artist_name: str | None) -> str | None:
    if not album_name:
        return None
    album_part = " ".join(str(album_name).strip().lower().split())
    artist_part = " ".join(str(artist_name or "").strip().lower().split())
    if not album_part:
        return None
    return f"{album_part}|||{artist_part}"


def _track_identity_key(track_name: str | None, artist_name: str | None) -> str | None:
    if not track_name or not artist_name:
        return None
    track_part = " ".join(str(track_name).strip().lower().split())
    artist_part = " ".join(str(artist_name).strip().lower().split())
    if not track_part or not artist_part:
        return None
    return f"{track_part}|||{artist_part}"
