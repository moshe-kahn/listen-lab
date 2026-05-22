from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException

from backend.app.cache.static_metadata_cache import (
    _artist_lookup_key,
    _remember_artist_metadata,
    _remember_track_metadata,
    _static_metadata_get,
)
from backend.app.db import sqlite_connection
from backend.app.spotify_http import _spotify_get
from backend.app.spotify_normalization import _album_lookup_key

SPOTIFY_ALBUM_URL = "https://api.spotify.com/v1/albums"
SPOTIFY_ARTISTS_URL = "https://api.spotify.com/v1/artists"
MAX_ARTWORK_SPOTIFY_FETCHES = 20


def _first_image_url_from_images(images: Any) -> str | None:
    if not isinstance(images, list):
        return None
    for image in images:
        if not isinstance(image, dict):
            continue
        url = str(image.get("url") or "").strip()
        if url:
            return url
    return None


def _first_image_url_from_json(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return _first_image_url_from_images(json.loads(value))
    except (TypeError, ValueError):
        return None


def _first_album_image_url_from_track_json(value: str | None) -> str | None:
    if not value:
        return None
    try:
        payload = json.loads(value)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    album = payload.get("album") if isinstance(payload.get("album"), dict) else {}
    return _first_image_url_from_images(album.get("images"))


def _catalog_album_images(album_ids: set[str]) -> dict[str, str]:
    normalized_ids = sorted({str(album_id).strip() for album_id in album_ids if str(album_id).strip()})
    if not normalized_ids:
        return {}
    placeholders = ",".join("?" for _ in normalized_ids)
    found: dict[str, str] = {}
    with sqlite_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT spotify_album_id, images_json
            FROM spotify_album_catalog
            WHERE spotify_album_id IN ({placeholders})
            """,
            normalized_ids,
        ).fetchall()
    for album_id, images_json in rows:
        image_url = _first_image_url_from_json(images_json)
        if image_url:
            found[str(album_id)] = image_url
    return found


def _catalog_track_images(track_ids: set[str]) -> dict[str, str]:
    normalized_ids = sorted({str(track_id).strip() for track_id in track_ids if str(track_id).strip()})
    if not normalized_ids:
        return {}
    placeholders = ",".join("?" for _ in normalized_ids)
    found: dict[str, str] = {}
    with sqlite_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT spotify_track_id, raw_json
            FROM spotify_track_catalog
            WHERE spotify_track_id IN ({placeholders})
            """,
            normalized_ids,
        ).fetchall()
    for track_id, raw_json in rows:
        image_url = _first_album_image_url_from_track_json(raw_json)
        if image_url:
            found[str(track_id)] = image_url
    return found


def _upsert_catalog_album_image(*, album_id: str, album: dict[str, Any], image_url: str) -> None:
    images = album.get("images") if isinstance(album.get("images"), list) else [{"url": image_url}]
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            INSERT INTO spotify_album_catalog (
              spotify_album_id, name, album_type, release_date, release_date_precision, total_tracks,
              artists_json, images_json, raw_json, market, fetched_at, last_status, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, datetime('now'), 'ok', NULL)
            ON CONFLICT(spotify_album_id) DO UPDATE SET
              images_json = CASE
                WHEN excluded.images_json IS NOT NULL AND trim(excluded.images_json) NOT IN ('', '[]')
                THEN excluded.images_json ELSE spotify_album_catalog.images_json END,
              raw_json = CASE
                WHEN spotify_album_catalog.raw_json IS NULL
                  OR trim(COALESCE(spotify_album_catalog.raw_json, '')) IN ('', '{}')
                THEN excluded.raw_json ELSE spotify_album_catalog.raw_json END,
              fetched_at = COALESCE(spotify_album_catalog.fetched_at, excluded.fetched_at),
              last_status = COALESCE(spotify_album_catalog.last_status, excluded.last_status)
            """,
            (
                album_id,
                str(album.get("name") or "") or None,
                str(album.get("album_type") or "") or None,
                str(album.get("release_date") or "") or None,
                str(album.get("release_date_precision") or "") or None,
                int(album["total_tracks"]) if isinstance(album.get("total_tracks"), int) else None,
                json.dumps(album.get("artists") if isinstance(album.get("artists"), list) else []),
                json.dumps(images),
                json.dumps(album),
            ),
        )


async def _fetch_album_images_from_spotify(access_token: str, album_ids: list[str]) -> dict[str, str]:
    found: dict[str, str] = {}
    for album_id in album_ids[:MAX_ARTWORK_SPOTIFY_FETCHES]:
        payload = await _spotify_get(access_token, f"{SPOTIFY_ALBUM_URL}/{album_id}")
        image_url = _first_image_url_from_images(payload.get("images") if isinstance(payload, dict) else None)
        if image_url:
            found[album_id] = image_url
            _upsert_catalog_album_image(album_id=album_id, album=payload, image_url=image_url)
    return found


async def _fetch_artist_images_from_spotify(access_token: str, artist_ids: list[str]) -> dict[str, dict[str, Any]]:
    normalized_ids = []
    seen_ids: set[str] = set()
    for artist_id in artist_ids:
        normalized_id = str(artist_id or "").strip()
        if normalized_id and normalized_id not in seen_ids:
            seen_ids.add(normalized_id)
            normalized_ids.append(normalized_id)
    if not normalized_ids:
        return {}

    payload = await _spotify_get(
        access_token,
        SPOTIFY_ARTISTS_URL,
        {"ids": ",".join(normalized_ids[:MAX_ARTWORK_SPOTIFY_FETCHES])},
    )
    artists = payload.get("artists") if isinstance(payload, dict) else None
    if not isinstance(artists, list):
        return {}

    found: dict[str, dict[str, Any]] = {}
    for artist in artists:
        if not isinstance(artist, dict):
            continue
        artist_id = str(artist.get("id") or "").strip()
        if not artist_id:
            continue
        image_url = _first_image_url_from_images(artist.get("images"))
        url = artist.get("external_urls", {}).get("spotify") if isinstance(artist.get("external_urls"), dict) else None
        normalized = {
            "artist_id": artist_id,
            "id": artist_id,
            "name": str(artist.get("name") or "").strip() or None,
            "url": str(url).strip() if url else f"https://open.spotify.com/artist/{artist_id}",
            "image_url": image_url,
            "followers_total": (
                artist.get("followers", {}).get("total") if isinstance(artist.get("followers"), dict) else None
            ),
            "genres": artist.get("genres") if isinstance(artist.get("genres"), list) else [],
            "popularity": artist.get("popularity") if isinstance(artist.get("popularity"), int) else None,
        }
        found[artist_id] = normalized
        if normalized["name"]:
            _remember_artist_metadata(normalized)
    return found


async def resolve_artist_artwork(
    artists: list[dict[str, Any]],
    *,
    access_token: str | None = None,
    allow_spotify_fetch: bool = True,
) -> list[dict[str, Any]]:
    if not artists:
        return artists

    resolved = [dict(artist) for artist in artists]
    missing_indexes = [index for index, artist in enumerate(resolved) if not artist.get("image_url")]

    for index in list(missing_indexes):
        artist = resolved[index]
        artist_key = _artist_lookup_key(artist.get("name"))
        cached = _static_metadata_get("artists_by_name", artist_key) if artist_key else None
        if isinstance(cached, dict) and cached.get("image_url"):
            artist["image_url"] = cached["image_url"]
            artist["artist_id"] = artist.get("artist_id") or artist.get("id") or cached.get("artist_id")
            artist["id"] = artist.get("id") or artist.get("artist_id")
            artist["url"] = artist.get("url") or cached.get("url")

    missing_indexes = [index for index, artist in enumerate(resolved) if not artist.get("image_url")]
    if allow_spotify_fetch and access_token and missing_indexes:
        artist_ids = []
        seen_artist_ids: set[str] = set()
        for index in missing_indexes:
            artist = resolved[index]
            artist_id = str(artist.get("artist_id") or artist.get("id") or "").strip()
            if artist_id and artist_id not in seen_artist_ids:
                seen_artist_ids.add(artist_id)
                artist_ids.append(artist_id)
        try:
            fetched = await _fetch_artist_images_from_spotify(access_token, artist_ids)
        except HTTPException:
            fetched = {}
        for index in missing_indexes:
            artist = resolved[index]
            artist_id = str(artist.get("artist_id") or artist.get("id") or "").strip()
            metadata = fetched.get(artist_id)
            if not metadata:
                continue
            artist["image_url"] = metadata.get("image_url") or artist.get("image_url")
            artist["name"] = artist.get("name") or metadata.get("name")
            artist["artist_id"] = artist.get("artist_id") or metadata.get("artist_id")
            artist["id"] = artist.get("id") or metadata.get("id") or artist.get("artist_id")
            artist["url"] = artist.get("url") or metadata.get("url")

    for artist in resolved:
        if artist.get("name") and (artist.get("image_url") or artist.get("url") or artist.get("artist_id") or artist.get("id")):
            _remember_artist_metadata(
                {
                    "artist_id": artist.get("artist_id") or artist.get("id"),
                    "name": artist.get("name"),
                    "url": artist.get("url"),
                    "image_url": artist.get("image_url"),
                }
            )
    return resolved


async def resolve_track_artwork(
    tracks: list[dict[str, Any]],
    *,
    access_token: str | None = None,
    allow_spotify_fetch: bool = True,
) -> list[dict[str, Any]]:
    if not tracks:
        return tracks

    resolved = [dict(track) for track in tracks]
    artist_entries: list[dict[str, Any]] = []
    artist_positions: list[tuple[int, int]] = []
    for track_index, track in enumerate(resolved):
        artists = track.get("artists")
        if not isinstance(artists, list):
            continue
        normalized_artists: list[dict[str, Any]] = []
        for artist_index, artist in enumerate(artists):
            if not isinstance(artist, dict):
                continue
            normalized_artists.append(dict(artist))
            artist_entries.append(dict(artist))
            artist_positions.append((track_index, artist_index))
        track["artists"] = normalized_artists

    if artist_entries:
        resolved_artists = await resolve_artist_artwork(
            artist_entries,
            access_token=access_token,
            allow_spotify_fetch=allow_spotify_fetch,
        )
        for (track_index, artist_index), artist in zip(artist_positions, resolved_artists, strict=False):
            artists = resolved[track_index].get("artists")
            if isinstance(artists, list) and artist_index < len(artists):
                artists[artist_index] = artist

    missing_indexes = [index for index, track in enumerate(resolved) if not track.get("image_url")]
    if not missing_indexes:
        return resolved

    for index in list(missing_indexes):
        track = resolved[index]
        track_id = str(track.get("track_id") or "").strip()
        cached = _static_metadata_get("tracks_by_id", track_id) if track_id else None
        if isinstance(cached, dict) and cached.get("image_url"):
            track["image_url"] = cached["image_url"]

    missing_indexes = [index for index, track in enumerate(resolved) if not track.get("image_url")]
    for index in list(missing_indexes):
        track = resolved[index]
        album_key = _album_lookup_key(track.get("album_name"), track.get("artist_name"))
        cached = _static_metadata_get("albums_by_key", album_key) if album_key else None
        if isinstance(cached, dict) and cached.get("image_url"):
            track["image_url"] = cached["image_url"]
            track["album_id"] = track.get("album_id") or cached.get("album_id")

    missing_indexes = [index for index, track in enumerate(resolved) if not track.get("image_url")]
    track_image_by_id = _catalog_track_images({str(resolved[index].get("track_id") or "") for index in missing_indexes})
    for index in list(missing_indexes):
        track = resolved[index]
        image_url = track_image_by_id.get(str(track.get("track_id") or ""))
        if image_url:
            track["image_url"] = image_url

    missing_indexes = [index for index, track in enumerate(resolved) if not track.get("image_url")]
    album_image_by_id = _catalog_album_images({str(resolved[index].get("album_id") or "") for index in missing_indexes})
    for index in list(missing_indexes):
        track = resolved[index]
        image_url = album_image_by_id.get(str(track.get("album_id") or ""))
        if image_url:
            track["image_url"] = image_url

    missing_indexes = [index for index, track in enumerate(resolved) if not track.get("image_url")]
    if allow_spotify_fetch and access_token and missing_indexes:
        album_ids = []
        seen_album_ids: set[str] = set()
        for index in missing_indexes:
            album_id = str(resolved[index].get("album_id") or "").strip()
            if album_id and album_id not in seen_album_ids:
                seen_album_ids.add(album_id)
                album_ids.append(album_id)
        try:
            fetched = await _fetch_album_images_from_spotify(access_token, album_ids)
        except HTTPException:
            fetched = {}
        for index in missing_indexes:
            track = resolved[index]
            image_url = fetched.get(str(track.get("album_id") or ""))
            if image_url:
                track["image_url"] = image_url

    for track in resolved:
        if track.get("image_url"):
            _remember_track_metadata(track)
    return resolved
