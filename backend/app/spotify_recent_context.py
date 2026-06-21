from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

from backend.app.db import sqlite_connection


def _album_id_from_context(context: dict[str, Any]) -> str | None:
    if str(context.get("type") or "").lower() != "album":
        return None
    uri = str(context.get("uri") or "")
    match = re.match(r"^spotify:album:([^:]+)$", uri)
    if match:
        return match.group(1)
    href = str(context.get("href") or "")
    match = re.search(r"/albums/([^/?]+)", href)
    return match.group(1) if match else None


def _normalized(value: Any) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).split())


def _artist_keys(artists: Any) -> set[str]:
    if isinstance(artists, str):
        try:
            artists = json.loads(artists)
        except json.JSONDecodeError:
            artists = []
    if not isinstance(artists, list):
        return set()
    keys: set[str] = set()
    for artist in artists:
        if not isinstance(artist, dict):
            continue
        artist_id = str(artist.get("id") or "").strip()
        artist_name = _normalized(artist.get("name"))
        if artist_id:
            keys.add(f"id:{artist_id}")
        if artist_name:
            keys.add(f"name:{artist_name}")
    return keys


def resolve_recent_context_track(item: dict[str, Any]) -> dict[str, Any] | None:
    track = item.get("track") if isinstance(item.get("track"), dict) else {}
    album = track.get("album") if isinstance(track.get("album"), dict) else {}
    context = item.get("context") if isinstance(item.get("context"), dict) else {}
    context_album_id = _album_id_from_context(context)
    returned_album_id = str(album.get("id") or "").strip() or None
    if not context_album_id or context_album_id == returned_album_id:
        return None

    track_name = _normalized(track.get("name"))
    track_duration_ms = track.get("duration_ms")
    track_artists = _artist_keys(track.get("artists"))
    if not track_name or not isinstance(track_duration_ms, int) or not track_artists:
        return None

    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            """
            WITH context_tracks AS (
              SELECT spotify_track_id, name, duration_ms, artists_json
              FROM spotify_album_track
              WHERE spotify_album_id = ?
              UNION ALL
              SELECT spotify_track_id, name, duration_ms, artists_json
              FROM spotify_track_catalog
              WHERE album_id = ?
                AND NOT EXISTS (
                  SELECT 1 FROM spotify_album_track sat
                  WHERE sat.spotify_album_id = ?
                    AND sat.spotify_track_id = spotify_track_catalog.spotify_track_id
                )
            )
            SELECT
              ct.spotify_track_id,
              ct.name,
              ct.duration_ms,
              ct.artists_json,
              sac.name AS album_name,
              sac.album_type,
              sac.release_date,
              sac.total_tracks,
              sac.artists_json AS album_artists_json,
              sac.images_json
            FROM context_tracks ct
            LEFT JOIN spotify_album_catalog sac ON sac.spotify_album_id = ?
            """,
            (context_album_id, context_album_id, context_album_id, context_album_id),
        ).fetchall()

    matches = [
        row for row in rows
        if _normalized(row["name"]) == track_name
        and row["duration_ms"] is not None
        and abs(int(row["duration_ms"]) - track_duration_ms) <= 2_000
        and bool(_artist_keys(row["artists_json"]) & track_artists)
    ]
    if len(matches) != 1:
        return None
    match = matches[0]
    spotify_track_id = str(match["spotify_track_id"] or "").strip()
    if not spotify_track_id:
        return None
    return {
        "spotify_track_id": spotify_track_id,
        "spotify_track_uri": f"spotify:track:{spotify_track_id}",
        "spotify_album_id": context_album_id,
        "album_name": str(match["album_name"] or "").strip() or None,
        "album_type": str(match["album_type"] or "").strip() or None,
        "album_release_date": str(match["release_date"] or "").strip() or None,
        "album_total_tracks": int(match["total_tracks"]) if match["total_tracks"] is not None else None,
        "album_artists": json.loads(str(match["album_artists_json"] or "[]")),
        "album_images": json.loads(str(match["images_json"] or "[]")),
        "returned_spotify_track_id": str(track.get("id") or "").strip() or None,
        "returned_spotify_album_id": returned_album_id,
    }
