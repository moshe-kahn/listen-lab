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


def _first_text(*values: Any) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def _spotify_album_url(album_id: str | None) -> str | None:
    return f"https://open.spotify.com/album/{album_id}" if album_id else None


def _metadata_candidate_matches_release(candidate: dict[str, Any], entry: dict[str, Any]) -> bool:
    candidate_id = str(candidate.get("album_id") or "").strip()
    entry_id = str(entry.get("spotify_album_id") or "").strip()
    if candidate_id and entry_id and candidate_id == entry_id:
        return True
    candidate_name = _normalize_name(candidate.get("album_name"))
    entry_name = _normalize_name(entry.get("album_name"))
    return bool(candidate_name and entry_name and candidate_name == entry_name)


def _apply_entity_album_metadata(
    albums: dict[int, dict[str, Any]],
    metadata_rows: list[sqlite3.Row],
) -> None:
    for row in metadata_rows:
        release_album_id = int(row["release_album_id"])
        entry = albums.get(release_album_id)
        if not entry:
            continue
        candidate = {
            "album_id": _first_text(row["catalog_album_id"], row["raw_album_id"]),
            "album_name": _first_text(row["catalog_album_name"], row["raw_album_name"]),
            "image_url": _first_image_url(row["catalog_album_images_json"]) or _first_image_url(row["raw_album_images_json"]),
            "release_year": _release_year(row["catalog_album_release_date"]) or _release_year(row["raw_album_release_date"]),
            "total_tracks": row["catalog_total_tracks"],
        }
        if not _metadata_candidate_matches_release(candidate, entry):
            continue
        if not entry.get("spotify_album_id") and candidate["album_id"]:
            entry["spotify_album_id"] = candidate["album_id"]
        if not entry.get("image_url") and candidate["image_url"]:
            entry["image_url"] = candidate["image_url"]
        if not entry.get("release_year") and candidate["release_year"]:
            entry["release_year"] = candidate["release_year"]
        if not entry.get("total_tracks") and candidate["total_tracks"] is not None:
            entry["total_tracks"] = int(candidate["total_tracks"])


def _dedupe_key(item: dict[str, Any]) -> str:
    album_id = str(item.get("album_id") or "").strip()
    if album_id:
        return f"id:{album_id}"
    return f"name:{_normalize_name(item.get('album_name'))}"


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


def _entity_album_evidence(
    connection: sqlite3.Connection,
    targets: list[str],
) -> list[dict[str, Any]]:
    target_keys = {_normalize_name(name) for name in targets}
    if not target_keys:
        return []

    album_rows = connection.execute(
        """
        SELECT
          ra.id AS release_album_id,
          ra.primary_name AS album_name,
          ra.release_year AS release_year,
          a.canonical_name AS artist_name,
          aa.billing_index AS billing_index,
          sa.source_name AS source_name,
          sa.external_id AS source_album_external_id,
          COUNT(DISTINCT at.release_track_id) AS track_count
        FROM album_artist aa
        JOIN artist a
          ON a.id = aa.artist_id
        JOIN release_album ra
          ON ra.id = aa.release_album_id
        LEFT JOIN album_track at
          ON at.release_album_id = ra.id
        LEFT JOIN source_album_map sam
          ON sam.release_album_id = ra.id
         AND sam.status = 'accepted'
        LEFT JOIN source_album sa
          ON sa.id = sam.source_album_id
         AND sa.source_name = 'spotify'
        WHERE aa.role = 'primary'
        GROUP BY ra.id, a.id, sa.id
        ORDER BY ra.primary_name, aa.billing_index, a.canonical_name
        """
    ).fetchall()
    metadata_rows = connection.execute(
        """
        SELECT
          at.release_album_id AS release_album_id,
          stc.album_id AS catalog_album_id,
          sac.name AS catalog_album_name,
          sac.images_json AS catalog_album_images_json,
          sac.release_date AS catalog_album_release_date,
          sac.total_tracks AS catalog_total_tracks,
          CASE
            WHEN json_valid(st.raw_payload_json)
            THEN COALESCE(
              json_extract(st.raw_payload_json, '$.track.album.id'),
              json_extract(st.raw_payload_json, '$.album.id')
            )
            ELSE NULL
          END AS raw_album_id,
          CASE
            WHEN json_valid(st.raw_payload_json)
            THEN COALESCE(
              json_extract(st.raw_payload_json, '$.track.album.name'),
              json_extract(st.raw_payload_json, '$.album.name')
            )
            ELSE NULL
          END AS raw_album_name,
          CASE
            WHEN json_valid(st.raw_payload_json)
            THEN COALESCE(
              json_extract(st.raw_payload_json, '$.track.album.images'),
              json_extract(st.raw_payload_json, '$.album.images')
            )
            ELSE NULL
          END AS raw_album_images_json,
          CASE
            WHEN json_valid(st.raw_payload_json)
            THEN COALESCE(
              json_extract(st.raw_payload_json, '$.track.album.release_date'),
              json_extract(st.raw_payload_json, '$.album.release_date')
            )
            ELSE NULL
          END AS raw_album_release_date
        FROM album_track at
        JOIN source_track_map stm
          ON stm.release_track_id = at.release_track_id
         AND stm.status = 'accepted'
        JOIN source_track st
          ON st.id = stm.source_track_id
         AND st.source_name IN ('spotify', 'spotify_uri')
        LEFT JOIN spotify_track_catalog stc
          ON stc.spotify_track_id = CASE
            WHEN st.source_name = 'spotify' THEN st.external_id
            WHEN st.source_name = 'spotify_uri' THEN replace(st.external_id, 'spotify:track:', '')
            WHEN st.external_uri LIKE 'spotify:track:%' THEN replace(st.external_uri, 'spotify:track:', '')
            ELSE NULL
          END
         AND lower(COALESCE(stc.last_status, '')) != 'error'
        LEFT JOIN spotify_album_catalog sac
          ON sac.spotify_album_id = stc.album_id
         AND lower(COALESCE(sac.last_status, '')) != 'error'
        ORDER BY at.release_album_id, at.id, st.id
        """
    ).fetchall()

    albums: dict[int, dict[str, Any]] = {}
    for row in album_rows:
        release_album_id = int(row["release_album_id"])
        entry = albums.setdefault(
            release_album_id,
            {
                "album_name": str(row["album_name"] or "").strip(),
                "release_year": str(row["release_year"]) if row["release_year"] is not None else None,
                "spotify_album_id": None,
                "image_url": None,
                "track_count": 0,
                "total_tracks": None,
                "album_artist_names": [],
                "album_artist_keys": set(),
            },
        )
        artist_name = str(row["artist_name"] or "").strip()
        artist_key = _normalize_name(artist_name)
        if artist_name and artist_key and artist_key not in entry["album_artist_keys"]:
            entry["album_artist_keys"].add(artist_key)
            entry["album_artist_names"].append(artist_name)
        source_album_id = str(row["source_album_external_id"] or "").strip()
        if source_album_id and not entry["spotify_album_id"]:
            entry["spotify_album_id"] = source_album_id
        entry["track_count"] = max(int(entry["track_count"] or 0), int(row["track_count"] or 0))

    _apply_entity_album_metadata(albums, metadata_rows)

    items_by_name: dict[str, dict[str, Any]] = {}
    for entry in albums.values():
        album_name = str(entry["album_name"] or "").strip()
        if not album_name:
            continue
        album_artist_keys = set(entry["album_artist_keys"])
        if not target_keys.issubset(album_artist_keys):
            continue
        album_id = str(entry["spotify_album_id"] or "").strip() or None
        track_count = int(entry["track_count"] or 0)
        item = {
            "album_id": album_id,
            "album_name": album_name,
            "album_artist_names": list(entry["album_artist_names"]),
            "image_url": entry["image_url"],
            "url": _spotify_album_url(album_id),
            "release_year": entry["release_year"],
            "total_tracks": entry["total_tracks"] or track_count or None,
            "cached_track_count": track_count,
            "matching_artist_names": targets,
            "matching_track_count_by_artist": {name: 0 for name in targets},
            "all_targets_present": True,
            "tracklist_complete": False,
            "relationship": "album",
            "evidence": "Internal album artist link",
        }
        name_key = _normalize_name(album_name)
        existing = items_by_name.get(name_key)
        if not existing or (not existing.get("album_id") and album_id):
            items_by_name[name_key] = item
    return list(items_by_name.values())


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
        entity_items = _entity_album_evidence(connection, targets)

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

    existing_keys = {_dedupe_key(item) for item in items}
    for entity_item in entity_items:
        key = _dedupe_key(entity_item)
        if key not in existing_keys:
            existing_keys.add(key)
            items.append(entity_item)

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
