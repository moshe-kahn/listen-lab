from __future__ import annotations

import json
import sqlite3
from typing import Any, Literal

from backend.app.db import sqlite_connection

ArtistAlbumRelationship = Literal["album", "appears_on", "unknown"]


def _json_list(value: Any) -> list[Any]:
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _normalize_name(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _artist_names_from_json(value: Any) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for item in _json_list(value):
        name = ""
        if isinstance(item, dict):
            name = str(item.get("name") or "").strip()
        elif isinstance(item, str):
            name = item.strip()
        key = _normalize_name(name)
        if name and key and key not in seen:
            seen.add(key)
            names.append(name)
    return names


def _first_image_url(value: Any) -> str | None:
    for item in _json_list(value):
        if isinstance(item, dict):
            url = str(item.get("url") or "").strip()
            if url:
                return url
    return None


def _release_year(value: Any) -> str | None:
    text = str(value or "").strip()
    return text[:4] if len(text) >= 4 else None


def _spotify_album_url(album_id: str | None) -> str | None:
    return f"https://open.spotify.com/album/{album_id}" if album_id else None


def _relationship_for_single(
    target_name: str,
    album_artist_names: list[str],
    track_match_count: int,
    cached_track_count: int,
    tracklist_complete: bool,
) -> tuple[ArtistAlbumRelationship, str]:
    target_key = _normalize_name(target_name)
    album_artist_keys = {_normalize_name(name) for name in album_artist_names}
    if target_key in album_artist_keys:
        return "album", "Album artist match"
    if tracklist_complete:
        evidence = f"Complete cached tracklist; artist appears on {track_match_count}/{cached_track_count} tracks"
        if track_match_count > cached_track_count / 2:
            return "album", evidence
        return "appears_on", evidence
    return "unknown", "Incomplete cached tracklist; artist appears on cached tracks only"


def _relationship_for_shared(
    target_names: list[str],
    album_artist_names: list[str],
    matching_track_count_by_artist: dict[str, int],
    cached_track_count: int,
    tracklist_complete: bool,
) -> tuple[ArtistAlbumRelationship, str]:
    album_artist_keys = {_normalize_name(name) for name in album_artist_names}
    target_keys = {_normalize_name(name) for name in target_names}
    if target_keys and target_keys.issubset(album_artist_keys):
        return "album", "All selected artists are album artists"
    if tracklist_complete and cached_track_count > 0:
        majority_targets = [
            name for name in target_names if matching_track_count_by_artist.get(name, 0) > cached_track_count / 2
        ]
        if len(majority_targets) == len(target_names):
            return "album", "Complete cached tracklist; all selected artists appear on a majority of tracks"
    return "unknown", "All selected artists present in album/track evidence"


def list_artist_album_evidence(
    artist_names: list[str],
    source_album_id: str | None = None,
    source_album_name: str | None = None,
) -> list[dict[str, Any]]:
    targets: list[str] = []
    seen_targets: set[str] = set()
    for name in artist_names:
        clean_name = str(name or "").strip()
        key = _normalize_name(clean_name)
        if clean_name and key and key not in seen_targets:
            seen_targets.add(key)
            targets.append(clean_name)
    if not targets:
        return []

    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        album_rows = connection.execute(
            """
            SELECT spotify_album_id, name, release_date, total_tracks, artists_json, images_json
            FROM spotify_album_catalog
            WHERE lower(COALESCE(last_status, '')) != 'error'
            """
        ).fetchall()
        track_rows = connection.execute(
            """
            SELECT spotify_album_id, spotify_track_id, artists_json
            FROM spotify_album_track
            WHERE lower(COALESCE(last_status, '')) != 'error'
            """
        ).fetchall()

    tracks_by_album: dict[str, list[sqlite3.Row]] = {}
    for row in track_rows:
        album_id = str(row["spotify_album_id"] or "").strip()
        if album_id:
            tracks_by_album.setdefault(album_id, []).append(row)

    source_id = str(source_album_id or "").strip()
    source_name_key = _normalize_name(source_album_name)
    target_keys_by_name = {name: _normalize_name(name) for name in targets}
    items: list[dict[str, Any]] = []

    for row in album_rows:
        album_id = str(row["spotify_album_id"] or "").strip() or None
        album_name = str(row["name"] or "").strip()
        if not album_name:
            continue

        album_artist_names = _artist_names_from_json(row["artists_json"])
        album_artist_keys = {_normalize_name(name) for name in album_artist_names}
        album_tracks = tracks_by_album.get(album_id or "", [])
        cached_track_ids = {
            str(track_row["spotify_track_id"] or "").strip()
            for track_row in album_tracks
            if str(track_row["spotify_track_id"] or "").strip()
        }
        cached_track_count = len(cached_track_ids) if cached_track_ids else len(album_tracks)
        total_tracks = row["total_tracks"]
        total_track_count = int(total_tracks) if total_tracks is not None else None
        tracklist_complete = bool(
            total_track_count is not None
            and cached_track_count > 0
            and cached_track_count >= total_track_count
        )

        matching_track_count_by_artist: dict[str, int] = {name: 0 for name in targets}
        track_artist_keys_by_track: list[set[str]] = []
        for track_row in album_tracks:
            track_artist_keys = {_normalize_name(name) for name in _artist_names_from_json(track_row["artists_json"])}
            track_artist_keys_by_track.append(track_artist_keys)
        for target_name, target_key in target_keys_by_name.items():
            matching_track_count_by_artist[target_name] = sum(
                1 for track_artist_keys in track_artist_keys_by_track if target_key in track_artist_keys
            )

        matching_artist_names = [
            name
            for name, target_key in target_keys_by_name.items()
            if target_key in album_artist_keys or matching_track_count_by_artist.get(name, 0) > 0
        ]
        all_targets_present = len(matching_artist_names) == len(targets)
        if not all_targets_present:
            continue

        if len(targets) == 1:
            relationship, evidence = _relationship_for_single(
                targets[0],
                album_artist_names,
                matching_track_count_by_artist.get(targets[0], 0),
                cached_track_count,
                tracklist_complete,
            )
        else:
            relationship, evidence = _relationship_for_shared(
                targets,
                album_artist_names,
                matching_track_count_by_artist,
                cached_track_count,
                tracklist_complete,
            )

        items.append(
            {
                "album_id": album_id,
                "album_name": album_name,
                "album_artist_names": album_artist_names,
                "image_url": _first_image_url(row["images_json"]),
                "url": _spotify_album_url(album_id),
                "release_year": _release_year(row["release_date"]),
                "total_tracks": total_track_count,
                "cached_track_count": cached_track_count,
                "matching_artist_names": matching_artist_names,
                "matching_track_count_by_artist": matching_track_count_by_artist,
                "all_targets_present": all_targets_present,
                "tracklist_complete": tracklist_complete,
                "relationship": relationship,
                "evidence": evidence,
            }
        )

    relationship_rank = {"album": 0, "appears_on": 1, "unknown": 2}

    def sort_key(item: dict[str, Any]) -> tuple[int, int, str]:
        item_album_id = str(item.get("album_id") or "").strip()
        item_album_name_key = _normalize_name(item.get("album_name"))
        source_match = bool(
            (source_id and item_album_id and source_id == item_album_id)
            or (source_name_key and item_album_name_key == source_name_key)
        )
        return (
            0 if source_match else 1,
            relationship_rank.get(str(item.get("relationship") or "unknown"), 2) if len(targets) == 1 else 0,
            item_album_name_key,
        )

    return sorted(items, key=sort_key)
