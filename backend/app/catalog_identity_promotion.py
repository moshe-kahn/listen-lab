from __future__ import annotations

import json
import sqlite3
from typing import Any

from backend.app.db import (
    _ensure_source_album_mapping_with_connection,
    _ensure_source_artist_mapping_with_connection,
    _ensure_source_track_mapping_with_connection,
    mark_generated_recording_track_clusters_dirty_with_connection,
    sqlite_connection,
)
from backend.app.recording_track_candidates import refresh_generated_recording_track_clusters_for_release_tracks


IDENTITY_TABLES = (
    "artist",
    "source_artist",
    "source_artist_map",
    "release_album",
    "source_album",
    "source_album_map",
    "release_track",
    "source_track",
    "source_track_map",
    "album_artist",
    "track_artist",
    "album_track",
)


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _json_load(value: str | None) -> Any:
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _release_year(release_date: str | None) -> int | None:
    if not release_date or len(release_date) < 4:
        return None
    try:
        return int(release_date[:4])
    except ValueError:
        return None


def _isrc_from_raw_json(raw_json: str | None) -> str | None:
    payload = _json_load(raw_json)
    if not isinstance(payload, dict):
        return None
    external_ids = payload.get("external_ids")
    if not isinstance(external_ids, dict):
        return None
    isrc = str(external_ids.get("isrc") or "").strip()
    return isrc or None


def _artist_refs(artists_json: str | None) -> list[dict[str, str | None]]:
    artists = _json_load(artists_json)
    if not isinstance(artists, list):
        return []
    refs: list[dict[str, str | None]] = []
    seen: set[str] = set()
    for artist in artists:
        if not isinstance(artist, dict):
            continue
        external_id = str(artist.get("id") or "").strip()
        if not external_id or external_id in seen:
            continue
        seen.add(external_id)
        refs.append(
            {
                "external_id": external_id,
                "external_uri": str(artist.get("uri") or "").strip() or f"spotify:artist:{external_id}",
                "name": str(artist.get("name") or "").strip() or None,
            }
        )
    return refs


def _table_counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        table_name: int(connection.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0])
        for table_name in IDENTITY_TABLES
    }


def _count_delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    return {table_name: int(after[table_name] - before[table_name]) for table_name in IDENTITY_TABLES}


def _album_row(connection: sqlite3.Connection, album_id: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT
          spotify_album_id,
          name,
          album_type,
          release_date,
          release_date_precision,
          total_tracks,
          artists_json,
          raw_json
        FROM spotify_album_catalog
        WHERE spotify_album_id = ?
        LIMIT 1
        """,
        (album_id,),
    ).fetchone()


def _album_track_rows(connection: sqlite3.Connection, album_id: str) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
          sat.spotify_album_id,
          sat.spotify_track_id,
          sat.disc_number,
          sat.track_number,
          sat.name,
          sat.duration_ms,
          sat.artists_json,
          sat.raw_json,
          st.id AS source_track_id,
          stm.release_track_id,
          rt.primary_name AS release_track_name
        FROM spotify_album_track sat
        LEFT JOIN source_track st
          ON st.source_name = 'spotify'
         AND st.external_id = sat.spotify_track_id
        LEFT JOIN source_track_map stm
          ON stm.source_track_id = st.id
         AND stm.status = 'accepted'
        LEFT JOIN release_track rt
          ON rt.id = stm.release_track_id
        WHERE sat.spotify_album_id = ?
        ORDER BY COALESCE(sat.disc_number, 1), COALESCE(sat.track_number, 999999), sat.spotify_track_id
        """,
        (album_id,),
    ).fetchall()


def _ensure_album_artist_links(
    connection: sqlite3.Connection,
    *,
    release_album_id: int,
    artists_json: str | None,
    raw_payload_json: str | None,
) -> None:
    for billing_index, artist_ref in enumerate(_artist_refs(artists_json)):
        external_id = artist_ref.get("external_id")
        if not external_id:
            continue
        artist_name = str(artist_ref.get("name") or "").strip()
        if artist_name:
            duplicate_name_row = connection.execute(
                """
                SELECT 1
                FROM album_artist aa
                JOIN artist a ON a.id = aa.artist_id
                WHERE aa.release_album_id = ?
                  AND aa.role = 'primary'
                  AND lower(a.canonical_name) = lower(?)
                LIMIT 1
                """,
                (release_album_id, artist_name),
            ).fetchone()
            if duplicate_name_row is not None:
                continue
        artist_id = _ensure_source_artist_mapping_with_connection(
            connection,
            external_id=external_id,
            external_uri=artist_ref.get("external_uri"),
            artist_name=artist_ref.get("name"),
            raw_payload_json=raw_payload_json,
        )
        connection.execute(
            """
            INSERT INTO album_artist (
              release_album_id,
              artist_id,
              role,
              billing_index,
              credited_as,
              match_method,
              confidence,
              source_basis
            )
            VALUES (?, ?, 'primary', ?, ?, 'spotify_catalog_album_track', 0.85, 'spotify_album_catalog')
            ON CONFLICT(release_album_id, artist_id, role) DO UPDATE SET
              billing_index = COALESCE(album_artist.billing_index, excluded.billing_index),
              credited_as = COALESCE(album_artist.credited_as, excluded.credited_as),
              match_method = CASE
                WHEN album_artist.match_method = 'backfill' THEN excluded.match_method
                ELSE album_artist.match_method
              END,
              confidence = CASE
                WHEN album_artist.match_method = 'backfill' THEN excluded.confidence
                ELSE album_artist.confidence
              END,
              source_basis = COALESCE(album_artist.source_basis, excluded.source_basis)
            """,
            (release_album_id, artist_id, billing_index, artist_ref.get("name")),
        )


def _ensure_track_artist_links(
    connection: sqlite3.Connection,
    *,
    release_track_id: int,
    artists_json: str | None,
    raw_payload_json: str | None,
) -> None:
    for billing_index, artist_ref in enumerate(_artist_refs(artists_json)):
        external_id = artist_ref.get("external_id")
        if not external_id:
            continue
        artist_name = str(artist_ref.get("name") or "").strip()
        if artist_name:
            duplicate_name_row = connection.execute(
                """
                SELECT 1
                FROM track_artist ta
                JOIN artist a ON a.id = ta.artist_id
                WHERE ta.release_track_id = ?
                  AND ta.role = 'primary'
                  AND lower(a.canonical_name) = lower(?)
                LIMIT 1
                """,
                (release_track_id, artist_name),
            ).fetchone()
            if duplicate_name_row is not None:
                continue
        artist_id = _ensure_source_artist_mapping_with_connection(
            connection,
            external_id=external_id,
            external_uri=artist_ref.get("external_uri"),
            artist_name=artist_ref.get("name"),
            raw_payload_json=raw_payload_json,
        )
        connection.execute(
            """
            INSERT INTO track_artist (
              release_track_id,
              artist_id,
              role,
              billing_index,
              credited_as,
              match_method,
              confidence,
              source_basis
            )
            VALUES (?, ?, 'primary', ?, ?, 'spotify_catalog_album_track', 0.85, 'spotify_album_track')
            ON CONFLICT(release_track_id, artist_id, role) DO UPDATE SET
              billing_index = COALESCE(track_artist.billing_index, excluded.billing_index),
              credited_as = COALESCE(track_artist.credited_as, excluded.credited_as),
              match_method = CASE
                WHEN track_artist.match_method = 'backfill' THEN excluded.match_method
                ELSE track_artist.match_method
              END,
              confidence = CASE
                WHEN track_artist.match_method = 'backfill' THEN excluded.confidence
                ELSE track_artist.confidence
              END,
              source_basis = COALESCE(track_artist.source_basis, excluded.source_basis)
            """,
            (release_track_id, artist_id, billing_index, artist_ref.get("name")),
        )


def plan_catalog_album_identity_promotion(*, album_ids: list[str]) -> dict[str, Any]:
    normalized_album_ids = [album_id.strip() for album_id in album_ids if album_id.strip()]
    albums: list[dict[str, Any]] = []
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        for album_id in normalized_album_ids:
            album = _album_row(connection, album_id)
            tracks = _album_track_rows(connection, album_id)
            albums.append(
                {
                    "spotify_album_id": album_id,
                    "album_name": str(album["name"] or "") if album else None,
                    "release_date": str(album["release_date"] or "") if album else None,
                    "total_tracks": int(album["total_tracks"]) if album and album["total_tracks"] is not None else None,
                    "catalog_track_count": len(tracks),
                    "missing_album_catalog": album is None,
                    "tracks": [
                        {
                            "spotify_track_id": str(row["spotify_track_id"] or ""),
                            "name": str(row["name"] or ""),
                            "disc_number": row["disc_number"],
                            "track_number": row["track_number"],
                            "has_source_track": row["source_track_id"] is not None,
                            "release_track_id": int(row["release_track_id"]) if row["release_track_id"] is not None else None,
                            "release_track_name": str(row["release_track_name"] or "") if row["release_track_name"] else None,
                            "would_create_identity": row["release_track_id"] is None,
                        }
                        for row in tracks
                    ],
                }
            )
    return {
        "apply": False,
        "album_count": len(albums),
        "albums": albums,
        "planned_track_count": sum(len(album["tracks"]) for album in albums),
        "planned_new_identity_count": sum(
            1
            for album in albums
            for track in album["tracks"]
            if track["would_create_identity"]
        ),
    }


def promote_catalog_album_tracks_to_identity(
    *,
    album_ids: list[str],
    apply: bool = False,
    refresh_clusters: bool = True,
) -> dict[str, Any]:
    normalized_album_ids = [album_id.strip() for album_id in album_ids if album_id.strip()]
    if not apply:
        return plan_catalog_album_identity_promotion(album_ids=normalized_album_ids)

    touched_release_track_ids: set[int] = set()
    promoted_tracks: list[dict[str, Any]] = []
    before: dict[str, int]
    after: dict[str, int]

    with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
        before = _table_counts(connection)
        for album_id in normalized_album_ids:
            album = _album_row(connection, album_id)
            if album is None:
                promoted_tracks.append(
                    {
                        "spotify_album_id": album_id,
                        "status": "skipped_missing_album_catalog",
                    }
                )
                continue

            release_album_id = _ensure_source_album_mapping_with_connection(
                connection,
                external_id=album_id,
                external_uri=f"spotify:album:{album_id}",
                album_name=str(album["name"] or "") or None,
                raw_payload_json=album["raw_json"],
            )
            release_year = _release_year(str(album["release_date"] or "") if album["release_date"] else None)
            if release_year is not None:
                connection.execute(
                    """
                    UPDATE release_album
                    SET release_year = COALESCE(release_year, ?)
                    WHERE id = ?
                    """,
                    (release_year, release_album_id),
                )
            _ensure_album_artist_links(
                connection,
                release_album_id=release_album_id,
                artists_json=album["artists_json"],
                raw_payload_json=album["raw_json"],
            )

            for row in _album_track_rows(connection, album_id):
                spotify_track_id = str(row["spotify_track_id"] or "").strip()
                if not spotify_track_id:
                    continue
                identity_was_missing = row["release_track_id"] is None
                release_track_id = _ensure_source_track_mapping_with_connection(
                    connection,
                    source_name="spotify",
                    external_id=spotify_track_id,
                    external_uri=f"spotify:track:{spotify_track_id}",
                    isrc=_isrc_from_raw_json(row["raw_json"]),
                    track_name=str(row["name"] or "") or None,
                    track_duration_ms=int(row["duration_ms"]) if row["duration_ms"] is not None else None,
                    raw_payload_json=row["raw_json"],
                    create_match_method="spotify_catalog_album_track",
                    create_confidence=0.72,
                    create_explanation=f"Promoted from cached Spotify album tracklist for album {album_id}",
                )
                _ensure_track_artist_links(
                    connection,
                    release_track_id=release_track_id,
                    artists_json=row["artists_json"],
                    raw_payload_json=row["raw_json"],
                )
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO album_track (
                      release_album_id,
                      release_track_id
                    )
                    VALUES (?, ?)
                    """,
                    (release_album_id, release_track_id),
                )
                album_link_created = int(cursor.rowcount or 0) > 0
                if identity_was_missing or album_link_created:
                    touched_release_track_ids.add(release_track_id)
                    mark_generated_recording_track_clusters_dirty_with_connection(
                        connection,
                        [release_track_id],
                        reason="catalog_album_track_identity_promotion",
                    )
                promoted_tracks.append(
                    {
                        "spotify_album_id": album_id,
                        "spotify_track_id": spotify_track_id,
                        "name": str(row["name"] or ""),
                        "release_album_id": release_album_id,
                        "release_track_id": release_track_id,
                    }
                )
        after = _table_counts(connection)

    refresh_result: dict[str, Any] | None = None
    if refresh_clusters and touched_release_track_ids:
        refresh_result = refresh_generated_recording_track_clusters_for_release_tracks(touched_release_track_ids)

    return {
        "apply": True,
        "album_count": len(normalized_album_ids),
        "promoted_track_count": len(promoted_tracks),
        "promoted_tracks": promoted_tracks,
        "table_count_delta": _count_delta(before, after),
        "touched_release_track_ids": sorted(touched_release_track_ids),
        "cluster_refresh": refresh_result,
    }
