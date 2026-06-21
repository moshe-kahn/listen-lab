from __future__ import annotations

import json
import time
from collections.abc import Awaitable, Callable
from typing import Any, Literal

import httpx
from fastapi import HTTPException, status

from backend.app.db import sqlite_connection
from backend.app.release_track_metadata import enrich_track_rows_with_release_metadata
from backend.app.spotify_http import _spotify_get

LIKED_TRACK_SYNC_KEY = "spotify_liked_tracks"
SPOTIFY_SAVED_TRACKS_URL = "https://api.spotify.com/v1/me/tracks"
SPOTIFY_SAVED_TRACK_PAGE_LIMIT = 50
QUICK_SYNC_PAGE_LIMIT = 2
FULL_SYNC_RUNTIME_LIMIT_SECONDS = 300.0

SyncMode = Literal["quick", "full"]
StoppedReason = Literal[
    "natural_end",
    "cap_reached",
    "timeout",
    "rate_limited",
    "auth_error",
    "forbidden",
    "missing_scope",
    "network_error",
    "parse_error",
    "unexpected_response",
]
SpotifyGet = Callable[[str, str, dict[str, Any] | None], Awaitable[dict[str, Any]]]
SIMULATED_SYNC_FAILURE_REASONS: set[str] = {
    "auth_error",
    "forbidden",
    "missing_scope",
    "rate_limited",
    "network_error",
    "parse_error",
    "unexpected_response",
}


def _utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _http_exception_reason(exc: HTTPException) -> StoppedReason:
    if exc.status_code == status.HTTP_401_UNAUTHORIZED:
        return "auth_error"
    if exc.status_code == status.HTTP_403_FORBIDDEN:
        detail = str(exc.detail or "").lower()
        if "scope" in detail or "permission" in detail:
            return "missing_scope"
        return "forbidden"
    if exc.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
        return "rate_limited"
    return "unexpected_response"


def _normalize_saved_track_item(item: Any, warnings: list[str]) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        warnings.append("Skipped saved-track item with unsupported shape.")
        return None
    added_at = item.get("added_at")
    track = item.get("track")
    if not isinstance(added_at, str) or not added_at:
        warnings.append("Skipped saved-track item without added_at.")
        return None
    if not isinstance(track, dict):
        warnings.append("Skipped saved-track item without track payload.")
        return None
    track_id = str(track.get("id") or "").strip()
    name = str(track.get("name") or "").strip()
    if not track_id or not name:
        warnings.append("Skipped saved-track item without track id/name.")
        return None

    album = track.get("album") if isinstance(track.get("album"), dict) else {}
    album_images = album.get("images") if isinstance(album.get("images"), list) else []
    album_image_url = next(
        (
            str(image.get("url")).strip()
            for image in album_images
            if isinstance(image, dict) and str(image.get("url") or "").strip()
        ),
        None,
    )
    artists_payload = track.get("artists") if isinstance(track.get("artists"), list) else []
    artist_names = []
    artist_ids = []
    for artist in artists_payload:
        if not isinstance(artist, dict):
            continue
        artist_name = str(artist.get("name") or "").strip()
        artist_id = str(artist.get("id") or "").strip()
        if artist_name:
            artist_names.append(artist_name)
            artist_ids.append(artist_id)

    return {
        "spotify_track_id": track_id,
        "uri": track.get("uri"),
        "name": name,
        "artist_names": artist_names,
        "artist_ids": artist_ids,
        "album_name": album.get("name") if isinstance(album, dict) else None,
        "album_spotify_id": album.get("id") if isinstance(album, dict) else None,
        "album_image_url": album_image_url,
        "duration_ms": int(track["duration_ms"]) if isinstance(track.get("duration_ms"), int) else None,
        "popularity": int(track["popularity"]) if isinstance(track.get("popularity"), int) else None,
        "explicit": 1 if track.get("explicit") is True else 0 if track.get("explicit") is False else None,
        "liked_at": added_at,
    }


def _validate_saved_tracks_page(payload: Any) -> tuple[list[Any] | None, bool]:
    if not isinstance(payload, dict):
        return None, False
    items = payload.get("items")
    if not isinstance(items, list):
        return None, False
    if not isinstance(payload.get("limit"), int):
        return None, False
    if not isinstance(payload.get("offset"), int):
        return None, False
    if not isinstance(payload.get("total"), int):
        return None, False
    next_url = payload.get("next")
    if next_url is not None and not isinstance(next_url, str):
        return None, False
    return items, True


def _page_is_natural_end(payload: dict[str, Any], items: list[Any], offset: int) -> bool:
    next_url = payload.get("next")
    total = payload.get("total")
    if len(items) == 0:
        return True
    if next_url is None:
        return True
    if isinstance(total, int) and offset + len(items) >= total:
        return True
    return False


def upsert_liked_tracks(user_id: str, tracks: list[dict[str, Any]], observed_at: str) -> int:
    if not tracks:
        return 0
    with sqlite_connection(write=True) as connection:
        connection.executemany(
            """
            INSERT INTO spotify_liked_track_cache (
              user_id,
              spotify_track_id,
              uri,
              name,
              artist_names,
              artist_ids,
              album_name,
              album_spotify_id,
              album_image_url,
              duration_ms,
              popularity,
              explicit,
              liked_at,
              is_liked,
              first_seen_at,
              last_seen_at,
              unliked_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, NULL)
            ON CONFLICT(user_id, spotify_track_id) DO UPDATE SET
              uri = excluded.uri,
              name = excluded.name,
              artist_names = excluded.artist_names,
              artist_ids = excluded.artist_ids,
              album_name = excluded.album_name,
              album_spotify_id = excluded.album_spotify_id,
              album_image_url = excluded.album_image_url,
              duration_ms = excluded.duration_ms,
              popularity = excluded.popularity,
              explicit = excluded.explicit,
              liked_at = excluded.liked_at,
              is_liked = 1,
              last_seen_at = excluded.last_seen_at,
              unliked_at = NULL
            """,
            [
                (
                    user_id,
                    track["spotify_track_id"],
                    track.get("uri"),
                    track["name"],
                    json.dumps(track.get("artist_names") or []),
                    json.dumps(track.get("artist_ids") or []),
                    track.get("album_name"),
                    track.get("album_spotify_id"),
                    track.get("album_image_url"),
                    track.get("duration_ms"),
                    track.get("popularity"),
                    track.get("explicit"),
                    track["liked_at"],
                    observed_at,
                    observed_at,
                )
                for track in tracks
            ],
        )
    return len(tracks)


def mark_missing_liked_tracks_unliked(user_id: str, fetched_track_ids: set[str], unliked_at: str) -> int:
    with sqlite_connection(write=True) as connection:
        if fetched_track_ids:
            placeholders = ",".join("?" for _ in fetched_track_ids)
            cursor = connection.execute(
                f"""
                UPDATE spotify_liked_track_cache
                SET is_liked = 0,
                    unliked_at = ?
                WHERE user_id = ?
                  AND is_liked = 1
                  AND spotify_track_id NOT IN ({placeholders})
                """,
                [unliked_at, user_id, *sorted(fetched_track_ids)],
            )
        else:
            cursor = connection.execute(
                """
                UPDATE spotify_liked_track_cache
                SET is_liked = 0,
                    unliked_at = ?
                WHERE user_id = ?
                  AND is_liked = 1
                """,
                (unliked_at, user_id),
            )
    return int(cursor.rowcount or 0)


def count_active_liked_tracks(user_id: str) -> int:
    with sqlite_connection() as connection:
        row = connection.execute(
            """
            SELECT count(*)
            FROM spotify_liked_track_cache
            WHERE user_id = ?
              AND is_liked = 1
            """,
            (user_id,),
        ).fetchone()
    return int(row[0] or 0)


def get_liked_track_sync_metadata(user_id: str) -> dict[str, Any] | None:
    with sqlite_connection() as connection:
        connection.row_factory = None
        row = connection.execute(
            """
            SELECT
              sync_key,
              last_quick_sync_at,
              last_completed_full_sync_at,
              last_attempted_sync_at,
              last_sync_mode,
              last_stopped_reason,
              last_full_completed,
              last_active_count,
              last_tracks_seen,
              last_pages_seen,
              updated_at
            FROM spotify_liked_track_sync_state
            WHERE user_id = ?
              AND sync_key = ?
            """,
            (user_id, LIKED_TRACK_SYNC_KEY),
        ).fetchone()
    if row is None:
        return None
    return {
        "sync_key": row[0],
        "last_quick_sync_at": row[1],
        "last_completed_full_sync_at": row[2],
        "last_attempted_sync_at": row[3],
        "last_sync_mode": row[4],
        "last_stopped_reason": row[5],
        "last_full_completed": bool(row[6]),
        "last_active_count": row[7],
        "last_tracks_seen": row[8],
        "last_pages_seen": row[9],
        "updated_at": row[10],
    }


def upsert_liked_track_sync_metadata(
    user_id: str,
    *,
    sync_mode: SyncMode,
    stopped_reason: str,
    full_completed: bool,
    active_likes: int,
    tracks_seen: int,
    pages_seen: int,
    attempted_at: str,
    completed_at: str | None = None,
) -> dict[str, Any]:
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            INSERT INTO spotify_liked_track_sync_state (
              user_id,
              sync_key,
              last_quick_sync_at,
              last_completed_full_sync_at,
              last_attempted_sync_at,
              last_sync_mode,
              last_stopped_reason,
              last_full_completed,
              last_active_count,
              last_tracks_seen,
              last_pages_seen,
              updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, sync_key) DO UPDATE SET
              last_quick_sync_at = CASE
                WHEN excluded.last_quick_sync_at IS NOT NULL THEN excluded.last_quick_sync_at
                ELSE spotify_liked_track_sync_state.last_quick_sync_at
              END,
              last_completed_full_sync_at = CASE
                WHEN excluded.last_completed_full_sync_at IS NOT NULL THEN excluded.last_completed_full_sync_at
                ELSE spotify_liked_track_sync_state.last_completed_full_sync_at
              END,
              last_attempted_sync_at = excluded.last_attempted_sync_at,
              last_sync_mode = excluded.last_sync_mode,
              last_stopped_reason = excluded.last_stopped_reason,
              last_full_completed = excluded.last_full_completed,
              last_active_count = excluded.last_active_count,
              last_tracks_seen = excluded.last_tracks_seen,
              last_pages_seen = excluded.last_pages_seen,
              updated_at = excluded.updated_at
            """,
            (
                user_id,
                LIKED_TRACK_SYNC_KEY,
                attempted_at if sync_mode == "quick" and stopped_reason in {"natural_end", "cap_reached"} else None,
                completed_at if sync_mode == "full" and full_completed else None,
                attempted_at,
                sync_mode,
                stopped_reason,
                1 if full_completed else 0,
                active_likes,
                tracks_seen,
                pages_seen,
                attempted_at,
            ),
        )
    return get_liked_track_sync_metadata(user_id) or {}


def list_cached_liked_tracks(user_id: str, *, limit: int = 50, offset: int = 0, active_only: bool = True) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 200))
    bounded_offset = max(0, int(offset))
    active_clause = "AND is_liked = 1" if active_only else ""
    with sqlite_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT
              spotify_track_id,
              uri,
              name,
              artist_names,
              artist_ids,
              album_name,
              album_spotify_id,
              album_image_url,
              duration_ms,
              popularity,
              explicit,
              liked_at,
              is_liked,
              first_seen_at,
              last_seen_at,
              unliked_at
            FROM spotify_liked_track_cache
            WHERE user_id = ?
              {active_clause}
            ORDER BY liked_at DESC, spotify_track_id ASC
            LIMIT ?
            OFFSET ?
            """,
            (user_id, bounded_limit + 1, bounded_offset),
        ).fetchall()
    has_more = len(rows) > bounded_limit
    rows = rows[:bounded_limit]
    items: list[dict[str, Any]] = []
    for row in rows:
        try:
            artist_names = json.loads(row[3] or "[]")
        except ValueError:
            artist_names = []
        try:
            artist_ids = json.loads(row[4] or "[]")
        except ValueError:
            artist_ids = []
        artist_text = ", ".join(str(name) for name in artist_names if str(name).strip())
        artists = []
        for index, name in enumerate(artist_names):
            artist_name = str(name).strip()
            artist_id = str(artist_ids[index]).strip() if index < len(artist_ids) else ""
            if artist_name or artist_id:
                artists.append(
                    {
                        "artist_id": artist_id or None,
                        "id": artist_id or None,
                        "name": artist_name or None,
                        "url": f"https://open.spotify.com/artist/{artist_id}" if artist_id else None,
                    }
                )
        items.append(
            {
                "track_id": row[0],
                "uri": row[1],
                "track_name": row[2],
                "artist_name": artist_text or None,
                "artists": artists,
                "album_name": row[5],
                "album_id": row[6],
                "image_url": row[7],
                "duration_ms": row[8],
                "popularity": row[9],
                "spotify_explicit": bool(row[10]) if row[10] is not None else None,
                "liked_at": row[11],
                "spotify_played_at": row[11],
                "is_liked": bool(row[12]),
                "first_seen_at": row[13],
                "last_seen_at": row[14],
                "unliked_at": row[15],
                "source_label": "liked_cache",
            }
        )
    return {
        "items": enrich_track_rows_with_release_metadata(items),
        "has_more": has_more,
        "limit": bounded_limit,
        "offset": bounded_offset,
        "metadata": get_liked_track_sync_metadata(user_id),
    }


def build_simulated_liked_track_sync_failure(user_id: str, *, mode: SyncMode, stopped_reason: str) -> dict[str, Any]:
    if stopped_reason not in SIMULATED_SYNC_FAILURE_REASONS:
        raise ValueError("Unsupported liked-track sync simulation reason.")
    return {
        "sync_mode": mode,
        "full_completed": False,
        "stopped_reason": stopped_reason,
        "pages_seen": 0,
        "tracks_seen": 0,
        "tracks_upserted": 0,
        "active_likes": count_active_liked_tracks(user_id),
        "marked_unliked": 0,
        "warnings": ["Simulated liked-track sync failure for local QA."],
        "errors": [f"Simulated {stopped_reason} liked-track sync failure."],
        "metadata": get_liked_track_sync_metadata(user_id),
    }


def is_liked_track_cached(user_id: str, spotify_track_id: str) -> bool:
    normalized_track_id = str(spotify_track_id or "").strip()
    if not normalized_track_id:
        return False
    with sqlite_connection() as connection:
        row = connection.execute(
            """
            SELECT 1
            FROM spotify_liked_track_cache
            WHERE user_id = ?
              AND spotify_track_id = ?
              AND is_liked = 1
            LIMIT 1
            """,
            (str(user_id), normalized_track_id),
        ).fetchone()
    return row is not None


def cached_liked_track_statuses(user_id: str, spotify_track_ids: list[str]) -> dict[str, bool]:
    normalized_ids = list(dict.fromkeys(
        str(track_id or "").strip()
        for track_id in spotify_track_ids
        if str(track_id or "").strip()
    ))
    if not normalized_ids:
        return {}
    placeholders = ",".join("?" for _ in normalized_ids)
    with sqlite_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT spotify_track_id, is_liked
            FROM spotify_liked_track_cache
            WHERE user_id = ?
              AND spotify_track_id IN ({placeholders})
            """,
            (str(user_id), *normalized_ids),
        ).fetchall()
    return {str(row[0]): bool(row[1]) for row in rows}


async def sync_spotify_liked_tracks(
    *,
    user_id: str,
    access_token: str,
    mode: SyncMode,
    spotify_get: SpotifyGet = _spotify_get,
    quick_page_limit: int = QUICK_SYNC_PAGE_LIMIT,
    full_page_cap: int | None = None,
    max_runtime_seconds: float = FULL_SYNC_RUNTIME_LIMIT_SECONDS,
) -> dict[str, Any]:
    attempted_at = _utc_now()
    offset = 0
    pages_seen = 0
    tracks_seen = 0
    tracks_upserted = 0
    marked_unliked = 0
    warnings: list[str] = []
    errors: list[str] = []
    fetched_track_ids: set[str] = set()
    stopped_reason: StoppedReason = "unexpected_response"
    full_completed = False
    started = time.monotonic()

    page_cap = quick_page_limit if mode == "quick" else full_page_cap

    while True:
        if page_cap is not None and pages_seen >= page_cap:
            stopped_reason = "cap_reached"
            break
        if mode == "full" and time.monotonic() - started > max_runtime_seconds:
            stopped_reason = "timeout"
            break

        try:
            payload = await spotify_get(
                access_token,
                SPOTIFY_SAVED_TRACKS_URL,
                {"limit": SPOTIFY_SAVED_TRACK_PAGE_LIMIT, "offset": offset},
            )
        except HTTPException as exc:
            stopped_reason = _http_exception_reason(exc)
            errors.append(str(exc.detail or exc.status_code))
            break
        except ValueError as exc:
            stopped_reason = "parse_error"
            errors.append(str(exc))
            break
        except (httpx.HTTPError, OSError) as exc:
            stopped_reason = "network_error"
            errors.append(str(exc))
            break

        items, valid_page = _validate_saved_tracks_page(payload)
        if not valid_page or items is None:
            stopped_reason = "unexpected_response"
            errors.append("Spotify saved-track response did not include an items list.")
            break

        pages_seen += 1
        observed_at = _utc_now()
        normalized_tracks: list[dict[str, Any]] = []
        for item in items:
            normalized = _normalize_saved_track_item(item, warnings)
            if normalized is None:
                continue
            normalized_tracks.append(normalized)
            fetched_track_ids.add(str(normalized["spotify_track_id"]))

        tracks_seen += len(normalized_tracks)
        tracks_upserted += upsert_liked_tracks(user_id, normalized_tracks, observed_at)

        if _page_is_natural_end(payload, items, offset):
            stopped_reason = "natural_end"
            full_completed = mode == "full"
            break

        offset += SPOTIFY_SAVED_TRACK_PAGE_LIMIT

    if mode == "full" and full_completed:
        marked_unliked = mark_missing_liked_tracks_unliked(user_id, fetched_track_ids, _utc_now())

    active_likes = count_active_liked_tracks(user_id)
    metadata = upsert_liked_track_sync_metadata(
        user_id,
        sync_mode=mode,
        stopped_reason=stopped_reason,
        full_completed=full_completed,
        active_likes=active_likes,
        tracks_seen=tracks_seen,
        pages_seen=pages_seen,
        attempted_at=attempted_at,
        completed_at=_utc_now() if full_completed else None,
    )

    return {
        "sync_mode": mode,
        "full_completed": full_completed,
        "stopped_reason": stopped_reason,
        "pages_seen": pages_seen,
        "tracks_seen": tracks_seen,
        "tracks_upserted": tracks_upserted,
        "active_likes": active_likes,
        "marked_unliked": marked_unliked,
        "warnings": warnings,
        "errors": errors,
        "metadata": metadata,
    }
