from __future__ import annotations

import time
from typing import Any

from backend.app.cache.file_cache import _cache_dir, _read_json_file, _write_json_file
from backend.app.spotify_normalization import _album_lookup_key, _artist_lookup_key, _track_identity_key

STATIC_METADATA_CACHE_FILE = "spotify_static_metadata.json"
STATIC_METADATA_CACHE_VERSION = 1
STATIC_METADATA_CACHE_SCHEMA = "spotify_static_metadata.v1"
STATIC_METADATA_MAX_ARTISTS = 4_000
STATIC_METADATA_MAX_ALBUMS = 6_000
STATIC_METADATA_MAX_TRACKS_BY_ID = 12_000
STATIC_METADATA_MAX_TRACKS_BY_KEY = 12_000

STATIC_METADATA_CACHE: dict[str, Any] | None = None
STATIC_METADATA_DIRTY_CONTENT = False
STATIC_METADATA_DIRTY_ACCESS = False


def _static_metadata_cache_path():
    return _cache_dir() / STATIC_METADATA_CACHE_FILE


def _static_bucket_caps() -> dict[str, int]:
    return {
        "artists_by_name": STATIC_METADATA_MAX_ARTISTS,
        "albums_by_key": STATIC_METADATA_MAX_ALBUMS,
        "tracks_by_id": STATIC_METADATA_MAX_TRACKS_BY_ID,
        "tracks_by_key": STATIC_METADATA_MAX_TRACKS_BY_KEY,
    }


def _is_static_cache_entry(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and isinstance(value.get("data"), dict)
        and isinstance(value.get("created_at"), (int, float))
        and isinstance(value.get("last_accessed"), (int, float))
    )


def _normalize_static_cache_bucket(raw_bucket: Any, *, default_created_at: float) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    if not isinstance(raw_bucket, dict):
        return normalized
    for key, value in raw_bucket.items():
        if not key:
            continue
        if _is_static_cache_entry(value):
            normalized[str(key)] = {
                "data": value["data"],
                "created_at": float(value["created_at"]),
                "last_accessed": float(value["last_accessed"]),
            }
            continue
        if isinstance(value, dict):
            normalized[str(key)] = {
                "data": value,
                "created_at": default_created_at,
                "last_accessed": default_created_at,
            }
            continue
        # Drop non-dict values safely.
    return normalized


def _load_static_metadata_cache() -> dict[str, Any]:
    global STATIC_METADATA_CACHE
    global STATIC_METADATA_DIRTY_ACCESS
    global STATIC_METADATA_DIRTY_CONTENT
    if STATIC_METADATA_CACHE is not None:
        return STATIC_METADATA_CACHE
    now = time.time()
    payload = _read_json_file(_static_metadata_cache_path()) or {}
    if (
        payload.get("cache_version") != STATIC_METADATA_CACHE_VERSION
        or payload.get("schema") != STATIC_METADATA_CACHE_SCHEMA
    ):
        payload = {}
    default_created_at = float(payload.get("stored_at") or now)
    STATIC_METADATA_CACHE = {
        "cache_version": STATIC_METADATA_CACHE_VERSION,
        "schema": STATIC_METADATA_CACHE_SCHEMA,
        "stored_at": float(payload.get("stored_at") or now),
        "artists_by_name": _normalize_static_cache_bucket(
            payload.get("artists_by_name"),
            default_created_at=default_created_at,
        ),
        "albums_by_key": _normalize_static_cache_bucket(
            payload.get("albums_by_key"),
            default_created_at=default_created_at,
        ),
        "tracks_by_id": _normalize_static_cache_bucket(
            payload.get("tracks_by_id"),
            default_created_at=default_created_at,
        ),
        "tracks_by_key": _normalize_static_cache_bucket(
            payload.get("tracks_by_key"),
            default_created_at=default_created_at,
        ),
    }
    STATIC_METADATA_DIRTY_ACCESS = False
    STATIC_METADATA_DIRTY_CONTENT = False
    return STATIC_METADATA_CACHE


def _static_metadata_get(bucket_name: str, key: str | None) -> dict[str, Any] | None:
    global STATIC_METADATA_DIRTY_ACCESS
    if not key:
        return None
    cache = _load_static_metadata_cache()
    bucket = cache.get(bucket_name) or {}
    entry = bucket.get(key)
    if not _is_static_cache_entry(entry):
        return None
    now = time.time()
    entry["last_accessed"] = now
    STATIC_METADATA_DIRTY_ACCESS = True
    return entry.get("data")


def _static_metadata_set(bucket_name: str, key: str | None, data: dict[str, Any]) -> None:
    global STATIC_METADATA_DIRTY_ACCESS
    global STATIC_METADATA_DIRTY_CONTENT
    if not key or not isinstance(data, dict):
        return
    cache = _load_static_metadata_cache()
    bucket = cache.get(bucket_name)
    if not isinstance(bucket, dict):
        bucket = {}
        cache[bucket_name] = bucket
    now = time.time()
    existing = bucket.get(key)
    if _is_static_cache_entry(existing):
        if existing.get("data") != data:
            existing["data"] = data
            STATIC_METADATA_DIRTY_CONTENT = True
        existing["last_accessed"] = now
        STATIC_METADATA_DIRTY_ACCESS = True
        return
    bucket[key] = {
        "data": data,
        "created_at": now,
        "last_accessed": now,
    }
    STATIC_METADATA_DIRTY_ACCESS = True
    STATIC_METADATA_DIRTY_CONTENT = True


def _remember_artist_metadata(artist: dict[str, Any]) -> None:
    artist_key = _artist_lookup_key(artist.get("name"))
    if not artist_key:
        return
    normalized = {
        "artist_id": artist.get("artist_id"),
        "followers_total": artist.get("followers_total"),
        "genres": artist.get("genres") or [],
        "popularity": artist.get("popularity"),
        "url": artist.get("url"),
        "image_url": artist.get("image_url"),
    }
    if normalized["image_url"] or normalized["url"] or normalized["artist_id"]:
        _static_metadata_set("artists_by_name", artist_key, normalized)


def _remember_track_metadata(track: dict[str, Any]) -> None:
    normalized = {
        "track_id": track.get("track_id"),
        "track_name": track.get("track_name"),
        "artist_name": track.get("artist_name"),
        "album_name": track.get("album_name"),
        "album_release_year": track.get("album_release_year"),
        "url": track.get("url"),
        "album_url": track.get("album_url"),
        "image_url": track.get("image_url"),
        "album_id": track.get("album_id"),
        "uri": track.get("uri"),
    }
    track_id = normalized.get("track_id")
    track_key = _track_identity_key(normalized.get("track_name"), normalized.get("artist_name"))
    album_key = _album_lookup_key(normalized.get("album_name"), normalized.get("artist_name"))
    if track_id:
        _static_metadata_set("tracks_by_id", str(track_id), normalized)
    if track_key:
        _static_metadata_set("tracks_by_key", track_key, normalized)
    if album_key and (normalized.get("image_url") or normalized.get("album_url") or normalized.get("album_id")):
        _static_metadata_set(
            "albums_by_key",
            album_key,
            {
                "album_id": normalized.get("album_id"),
                "url": normalized.get("album_url") or normalized.get("url"),
                "image_url": normalized.get("image_url"),
                "release_year": normalized.get("album_release_year"),
            },
        )


def _trim_static_metadata_cache(cache: dict[str, Any]) -> bool:
    trimmed = False
    for bucket_name, cap in _static_bucket_caps().items():
        bucket = cache.get(bucket_name)
        if not isinstance(bucket, dict) or len(bucket) <= cap:
            continue
        ranked = sorted(
            bucket.items(),
            key=lambda item: (
                float((item[1] or {}).get("last_accessed", 0.0)),
                float((item[1] or {}).get("created_at", 0.0)),
                item[0],
            ),
        )
        overflow = len(bucket) - cap
        for key, _entry in ranked[:overflow]:
            bucket.pop(key, None)
        trimmed = True
    return trimmed


def _save_static_metadata_cache(cache: dict[str, Any], *, persist_access_only: bool = False) -> None:
    global STATIC_METADATA_DIRTY_ACCESS
    global STATIC_METADATA_DIRTY_CONTENT
    trimmed = _trim_static_metadata_cache(cache)
    if trimmed:
        STATIC_METADATA_DIRTY_CONTENT = True
    should_write = STATIC_METADATA_DIRTY_CONTENT or (persist_access_only and STATIC_METADATA_DIRTY_ACCESS)
    if not should_write:
        return
    cache["cache_version"] = STATIC_METADATA_CACHE_VERSION
    cache["schema"] = STATIC_METADATA_CACHE_SCHEMA
    cache["stored_at"] = time.time()
    payload = {
        "cache_version": cache["cache_version"],
        "schema": cache["schema"],
        "stored_at": cache["stored_at"],
    }
    for bucket_name in _static_bucket_caps():
        payload[bucket_name] = cache.get(bucket_name) or {}
    _write_json_file(_static_metadata_cache_path(), payload)
    STATIC_METADATA_DIRTY_ACCESS = False
    STATIC_METADATA_DIRTY_CONTENT = False


def _hydrate_artists_from_static_cache(artists: list[dict[str, Any]]) -> list[dict[str, Any]]:
    hydrated: list[dict[str, Any]] = []
    for artist in artists:
        artist_key = _artist_lookup_key(artist.get("name"))
        cached = _static_metadata_get("artists_by_name", artist_key) if artist_key else None
        if isinstance(cached, dict):
            hydrated.append(
                {
                    **artist,
                    "artist_id": artist.get("artist_id") or cached.get("artist_id"),
                    "followers_total": artist.get("followers_total") or cached.get("followers_total"),
                    "genres": artist.get("genres") or cached.get("genres") or [],
                    "popularity": artist.get("popularity") if artist.get("popularity") is not None else cached.get("popularity"),
                    "url": artist.get("url") or cached.get("url"),
                    "image_url": artist.get("image_url") or cached.get("image_url"),
                }
            )
        else:
            hydrated.append(artist)
    return hydrated


def _hydrate_albums_from_static_cache(albums: list[dict[str, Any]]) -> list[dict[str, Any]]:
    hydrated: list[dict[str, Any]] = []
    for album in albums:
        album_key = _album_lookup_key(album.get("name"), album.get("artist_name"))
        cached = _static_metadata_get("albums_by_key", album_key) if album_key else None
        if isinstance(cached, dict):
            hydrated.append(
                {
                    **album,
                    "album_id": album.get("album_id") or cached.get("album_id"),
                    "url": album.get("url") or cached.get("url"),
                    "image_url": album.get("image_url") or cached.get("image_url"),
                    "release_year": album.get("release_year") or cached.get("release_year"),
                }
            )
        else:
            hydrated.append(album)
    return hydrated
