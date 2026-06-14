from __future__ import annotations

import json
import sqlite3
from typing import Any, Literal, TypedDict

from backend.app.db import sqlite_connection


PlaybackReason = Literal["context_source", "preferred_playable_source", "unavailable"]


class ReleaseTrackDetailNotFound(LookupError):
    pass


class ReleaseTrackDetailArtist(TypedDict, total=False):
    artist_id: str | None
    id: str | None
    name: str
    uri: str | None
    url: str | None
    role: str | None
    billing_index: int | None


class ReleaseTrackDetailSourceVersion(TypedDict):
    source_track_id: int
    spotify_track_id: str | None
    uri: str | None
    name: str | None
    artists: list[ReleaseTrackDetailArtist]
    album_id: str | None
    album_name: str | None
    album_image_url: str | None
    album_type: str | None
    album_release_date: str | None
    album_release_year: str | None
    album_total_tracks: int | None
    duration_ms: int | None
    explicit: bool | None
    playable: bool | None
    play_count: int
    first_played_at: str | None
    last_played_at: str | None
    spotify_url: str | None
    is_context: bool
    is_playback_choice: bool
    is_representative_choice: bool


class ReleaseTrackDetailPayload(TypedDict):
    release_track: dict[str, Any]
    display: dict[str, Any]
    playback: dict[str, Any]
    listen_counts: dict[str, int]
    source_versions: list[ReleaseTrackDetailSourceVersion]


def _json_list(value: Any) -> list[Any]:
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _spotify_track_id_from_source(source_name: str | None, external_id: str | None, external_uri: str | None) -> str | None:
    source_name_value = str(source_name or "").strip()
    external_id_value = str(external_id or "").strip()
    external_uri_value = str(external_uri or "").strip()
    if source_name_value == "spotify" and external_id_value:
        return external_id_value
    if source_name_value == "spotify_uri" and external_id_value.startswith("spotify:track:"):
        return external_id_value.rsplit(":", 1)[-1] or None
    if external_uri_value.startswith("spotify:track:"):
        return external_uri_value.rsplit(":", 1)[-1] or None
    return None


def _spotify_uri(track_id: str | None, source_name: str | None = None, external_id: str | None = None, external_uri: str | None = None) -> str | None:
    external_id_value = str(external_id or "").strip()
    external_uri_value = str(external_uri or "").strip()
    if external_uri_value.startswith("spotify:track:"):
        return external_uri_value
    if str(source_name or "").strip() == "spotify_uri" and external_id_value.startswith("spotify:track:"):
        return external_id_value
    return f"spotify:track:{track_id}" if track_id else None


def _spotify_track_url(track_id: str | None) -> str | None:
    return f"https://open.spotify.com/track/{track_id}" if track_id else None


def _spotify_artist_url(artist_id: str | None) -> str | None:
    return f"https://open.spotify.com/artist/{artist_id}" if artist_id else None


def _first_album_image_url(images_json: Any) -> str | None:
    for image in _json_list(images_json):
        if isinstance(image, dict) and image.get("url"):
            return str(image["url"])
    return None


def _artists_from_catalog_json(value: Any) -> list[ReleaseTrackDetailArtist]:
    artists: list[ReleaseTrackDetailArtist] = []
    for index, artist in enumerate(_json_list(value)):
        if not isinstance(artist, dict):
            continue
        name = str(artist.get("name") or "").strip()
        if not name:
            continue
        artist_id = str(artist.get("id") or artist.get("artist_id") or "").strip() or None
        artist_uri = str(artist.get("uri") or "").strip() or (f"spotify:artist:{artist_id}" if artist_id else None)
        external_urls = artist.get("external_urls") if isinstance(artist.get("external_urls"), dict) else {}
        artist_url = str(artist.get("url") or external_urls.get("spotify") or "").strip() or _spotify_artist_url(artist_id)
        artists.append(
            {
                "artist_id": artist_id,
                "id": artist_id,
                "name": name,
                "uri": artist_uri,
                "url": artist_url,
                "role": None,
                "billing_index": index,
            }
        )
    return artists


def _first_text(*values: Any) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def _artist_name(artists: list[ReleaseTrackDetailArtist]) -> str | None:
    names = [artist["name"] for artist in artists if artist.get("name")]
    return ", ".join(names) if names else None


def _optional_int(value: Any) -> int | None:
    return int(value) if isinstance(value, int) else None


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _is_usable_source(version: ReleaseTrackDetailSourceVersion) -> bool:
    return bool(version["spotify_track_id"] and version["uri"])


def get_release_track_detail(
    release_track_id: int,
    *,
    context_spotify_track_id: str | None = None,
) -> ReleaseTrackDetailPayload:
    normalized_context_id = str(context_spotify_track_id or "").strip() or None
    if release_track_id <= 0:
        raise ReleaseTrackDetailNotFound(f"release_track_id={release_track_id} was not found.")

    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        release_row = connection.execute(
            """
            SELECT id, primary_name, duration_ms
            FROM release_track
            WHERE id = ?
            """,
            (release_track_id,),
        ).fetchone()
        if release_row is None:
            raise ReleaseTrackDetailNotFound(f"release_track_id={release_track_id} was not found.")

        artist_rows = connection.execute(
            """
            SELECT a.canonical_name, ta.role, ta.billing_index
            FROM track_artist ta
            JOIN artist a
              ON a.id = ta.artist_id
            WHERE ta.release_track_id = ?
            ORDER BY COALESCE(ta.billing_index, 999999), ta.id, a.canonical_name
            """,
            (release_track_id,),
        ).fetchall()

        source_rows = connection.execute(
            """
            WITH source_play_counts AS (
              SELECT
                spotify_track_id,
                play_count,
                first_played_at,
                last_played_at
              FROM source_track_play_count_cache
            )
            SELECT
              st.id AS source_track_id,
              st.source_name,
              st.external_id,
              st.external_uri,
              st.source_name_raw,
              stc.name AS catalog_name,
              stc.duration_ms AS catalog_duration_ms,
              stc.explicit AS catalog_explicit,
              stc.album_id AS catalog_album_id,
              stc.artists_json AS catalog_artists_json,
              json_extract(st.raw_payload_json, '$.track.name') AS raw_track_name,
              json_extract(st.raw_payload_json, '$.track.duration_ms') AS raw_duration_ms,
              json_extract(st.raw_payload_json, '$.track.explicit') AS raw_explicit,
              json_extract(st.raw_payload_json, '$.track.album.id') AS raw_album_id,
              json_extract(st.raw_payload_json, '$.track.album.name') AS raw_album_name,
              json_extract(st.raw_payload_json, '$.track.album.images') AS raw_album_images_json,
              json_extract(st.raw_payload_json, '$.track.album.album_type') AS raw_album_type,
              json_extract(st.raw_payload_json, '$.track.album.release_date') AS raw_album_release_date,
              json_extract(st.raw_payload_json, '$.track.artists') AS raw_artists_json,
              stc.last_status AS catalog_last_status,
              sac.name AS catalog_album_name,
              sac.album_type AS catalog_album_type,
              sac.images_json AS catalog_album_images_json,
              sac.release_date AS catalog_album_release_date,
              sac.total_tracks AS catalog_album_total_tracks,
              COALESCE(spc.play_count, 0) AS play_count,
              spc.first_played_at AS first_played_at,
              spc.last_played_at AS last_played_at
            FROM source_track_map stm
            JOIN source_track st
              ON st.id = stm.source_track_id
            LEFT JOIN spotify_track_catalog stc
              ON stc.spotify_track_id = CASE
                WHEN st.source_name = 'spotify' THEN st.external_id
                WHEN st.source_name = 'spotify_uri' THEN replace(st.external_id, 'spotify:track:', '')
                WHEN st.external_uri LIKE 'spotify:track:%' THEN replace(st.external_uri, 'spotify:track:', '')
                ELSE NULL
              END
            LEFT JOIN spotify_album_catalog sac
              ON sac.spotify_album_id = stc.album_id
            LEFT JOIN source_play_counts spc
              ON spc.spotify_track_id = CASE
                WHEN st.source_name = 'spotify' THEN st.external_id
                WHEN st.source_name = 'spotify_uri' THEN replace(st.external_id, 'spotify:track:', '')
                WHEN st.external_uri LIKE 'spotify:track:%' THEN replace(st.external_uri, 'spotify:track:', '')
                ELSE NULL
              END
            WHERE stm.release_track_id = ?
              AND stm.status = 'accepted'
              AND st.source_name IN ('spotify', 'spotify_uri')
            ORDER BY st.id ASC
            """,
            (release_track_id,),
        ).fetchall()

    if not source_rows:
        raise ReleaseTrackDetailNotFound(f"release_track_id={release_track_id} has no accepted Spotify source versions.")

    canonical_artists: list[ReleaseTrackDetailArtist] = [
        {
            "name": str(row["canonical_name"]),
            "artist_id": None,
            "id": None,
            "uri": None,
            "url": None,
            "role": str(row["role"]) if row["role"] is not None else None,
            "billing_index": _optional_int(row["billing_index"]),
        }
        for row in artist_rows
        if row["canonical_name"]
    ]

    versions: list[ReleaseTrackDetailSourceVersion] = []
    for row in source_rows:
        spotify_track_id = _spotify_track_id_from_source(row["source_name"], row["external_id"], row["external_uri"])
        uri = _spotify_uri(spotify_track_id, row["source_name"], row["external_id"], row["external_uri"])
        catalog_artists = _artists_from_catalog_json(row["catalog_artists_json"])
        raw_artists = _artists_from_catalog_json(row["raw_artists_json"])
        album_images_json = row["catalog_album_images_json"] or row["raw_album_images_json"]
        album_release_date = row["catalog_album_release_date"] or row["raw_album_release_date"]
        versions.append(
            {
                "source_track_id": int(row["source_track_id"]),
                "spotify_track_id": spotify_track_id,
                "uri": uri,
                "name": _first_text(row["catalog_name"], row["raw_track_name"], row["source_name_raw"]),
                "artists": catalog_artists or raw_artists or canonical_artists,
                "album_id": _first_text(row["catalog_album_id"], row["raw_album_id"]),
                "album_name": _first_text(row["catalog_album_name"], row["raw_album_name"]),
                "album_image_url": _first_album_image_url(album_images_json),
                "album_type": _first_text(row["catalog_album_type"], row["raw_album_type"]),
                "album_release_date": str(album_release_date) if album_release_date else None,
                "album_release_year": (
                    str(album_release_date)[:4]
                    if album_release_date and str(album_release_date)[:4].isdigit()
                    else None
                ),
                "album_total_tracks": _optional_int(row["catalog_album_total_tracks"]),
                "duration_ms": _optional_int(row["catalog_duration_ms"]) or _optional_int(row["raw_duration_ms"]),
                "explicit": _optional_bool(row["catalog_explicit"] if row["catalog_explicit"] is not None else row["raw_explicit"]),
                "playable": None,
                "play_count": int(row["play_count"] or 0),
                "first_played_at": str(row["first_played_at"]) if row["first_played_at"] else None,
                "last_played_at": str(row["last_played_at"]) if row["last_played_at"] else None,
                "spotify_url": _spotify_track_url(spotify_track_id),
                "is_context": bool(normalized_context_id and spotify_track_id == normalized_context_id),
                "is_playback_choice": False,
                "is_representative_choice": False,
            }
        )

    representative_choice = next((version for version in versions if _is_usable_source(version)), None)
    if representative_choice is not None:
        for version in versions:
            version["is_representative_choice"] = version["source_track_id"] == representative_choice["source_track_id"]

    playback_choice: ReleaseTrackDetailSourceVersion | None = None
    playback_reason: PlaybackReason = "unavailable"
    context_choice = next((version for version in versions if version["is_context"] and _is_usable_source(version)), None)
    if context_choice is not None:
        playback_choice = context_choice
        playback_reason = "context_source"
    else:
        playback_choice = next((version for version in versions if _is_usable_source(version)), None)
        playback_reason = "preferred_playable_source" if playback_choice is not None else "unavailable"

    if playback_choice is not None:
        for version in versions:
            version["is_playback_choice"] = version["source_track_id"] == playback_choice["source_track_id"]

    display_source = playback_choice or next((version for version in versions if version["is_context"]), None) or versions[0]
    release_name = str(release_row["primary_name"] or "")
    display_title = display_source["name"] or release_name
    display_artist_name = _artist_name(display_source["artists"]) or _artist_name(canonical_artists)
    release_track_play_count = sum(int(version["play_count"] or 0) for version in versions)
    playback_source_play_count = int(playback_choice["play_count"] or 0) if playback_choice else 0

    return {
        "release_track": {
            "id": int(release_row["id"]),
            "name": release_name,
            "artists": canonical_artists,
            "duration_ms": _optional_int(release_row["duration_ms"]),
            "source_count": len(versions),
        },
        "display": {
            "title": display_title,
            "artist_name": display_artist_name,
            "image_url": display_source["album_image_url"],
            "album_name": display_source["album_name"],
            "spotify_url": display_source["spotify_url"],
            "source_spotify_track_id": display_source["spotify_track_id"],
        },
        "playback": {
            "spotify_track_id": playback_choice["spotify_track_id"] if playback_choice else None,
            "uri": playback_choice["uri"] if playback_choice else None,
            "reason": playback_reason,
        },
        "listen_counts": {
            "release_track_play_count": release_track_play_count,
            "playback_source_play_count": playback_source_play_count,
            "source_versions_play_count": release_track_play_count,
        },
        "source_versions": versions,
    }
