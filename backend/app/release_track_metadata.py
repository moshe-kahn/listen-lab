from __future__ import annotations

import sqlite3
from typing import Any

from backend.app.db import _normalize_fallback_artist_text, _stable_text_key, sqlite_connection


def release_track_metadata_for_spotify_ids(spotify_track_ids: list[str]) -> dict[str, dict[str, Any]]:
    normalized_ids = sorted({str(track_id or "").strip() for track_id in spotify_track_ids if str(track_id or "").strip()})
    if not normalized_ids:
        return {}
    track_placeholders = ",".join("?" for _ in normalized_ids)
    uri_ids = [f"spotify:track:{track_id}" for track_id in normalized_ids]
    uri_placeholders = ",".join("?" for _ in uri_ids)
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            f"""
            WITH matched AS (
              SELECT
                CASE
                  WHEN st.source_name = 'spotify_uri' THEN replace(st.external_id, 'spotify:track:', '')
                  ELSE st.external_id
                END AS spotify_track_id,
                stm.release_track_id
              FROM source_track_map stm
              JOIN source_track st
                ON st.id = stm.source_track_id
              WHERE stm.status = 'accepted'
                AND (
                  (st.source_name = 'spotify' AND st.external_id IN ({track_placeholders}))
                  OR (st.source_name = 'spotify_uri' AND st.external_id IN ({uri_placeholders}))
                )
            ),
            source_counts AS (
              SELECT
                stm.release_track_id,
                count(DISTINCT stm.source_track_id) AS source_track_count
              FROM source_track_map stm
              WHERE stm.status = 'accepted'
                AND stm.release_track_id IN (SELECT DISTINCT release_track_id FROM matched)
              GROUP BY stm.release_track_id
            )
            SELECT
              matched.spotify_track_id,
              matched.release_track_id,
              rt.primary_name AS release_track_name,
              source_counts.source_track_count
            FROM matched
            JOIN release_track rt
              ON rt.id = matched.release_track_id
            JOIN source_counts
              ON source_counts.release_track_id = matched.release_track_id
            ORDER BY source_counts.source_track_count DESC, matched.release_track_id ASC
            """,
            (*normalized_ids, *uri_ids),
        ).fetchall()

    release_track_ids = sorted({int(row["release_track_id"]) for row in rows if row["release_track_id"] is not None})
    from backend.app.recording_track_candidates import candidate_cluster_metadata_for_release_track_ids

    cluster_metadata_by_release_track_id = candidate_cluster_metadata_for_release_track_ids(release_track_ids)

    metadata: dict[str, dict[str, Any]] = {}
    for row in rows:
        spotify_track_id = str(row["spotify_track_id"] or "").strip()
        if not spotify_track_id or spotify_track_id in metadata:
            continue
        release_track_id = int(row["release_track_id"])
        source_track_count = int(row["source_track_count"] or 0)
        cluster_metadata = cluster_metadata_by_release_track_id.get(release_track_id)
        cluster_member_count = int(cluster_metadata["cluster_member_count"]) if cluster_metadata else 0
        metadata[spotify_track_id] = {
            "release_track_id": release_track_id,
            "release_track_name": str(row["release_track_name"] or ""),
            "release_track_source_count": max(source_track_count, cluster_member_count),
            "has_release_track_siblings": source_track_count > 1 or cluster_member_count > 1,
        }
    return metadata


def release_track_metadata_for_history_raw_keys(text_keys: list[str]) -> dict[str, dict[str, Any]]:
    normalized_keys = sorted({str(text_key or "").strip() for text_key in text_keys if str(text_key or "").strip()})
    if not normalized_keys:
        return {}
    placeholders = ",".join("?" for _ in normalized_keys)
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            f"""
            WITH matched AS (
              SELECT
                st.external_id AS history_raw_key,
                stm.release_track_id
              FROM source_track_map stm
              JOIN source_track st
                ON st.id = stm.source_track_id
              WHERE stm.status = 'accepted'
                AND st.source_name = 'history_raw'
                AND st.external_id IN ({placeholders})
            ),
            source_counts AS (
              SELECT
                stm.release_track_id,
                count(DISTINCT stm.source_track_id) AS source_track_count
              FROM source_track_map stm
              WHERE stm.status = 'accepted'
                AND stm.release_track_id IN (SELECT DISTINCT release_track_id FROM matched)
              GROUP BY stm.release_track_id
            )
            SELECT
              matched.history_raw_key,
              matched.release_track_id,
              rt.primary_name AS release_track_name,
              source_counts.source_track_count
            FROM matched
            JOIN release_track rt
              ON rt.id = matched.release_track_id
            JOIN source_counts
              ON source_counts.release_track_id = matched.release_track_id
            ORDER BY source_counts.source_track_count DESC, matched.release_track_id ASC
            """,
            normalized_keys,
        ).fetchall()

    release_track_ids = sorted({int(row["release_track_id"]) for row in rows if row["release_track_id"] is not None})
    from backend.app.recording_track_candidates import candidate_cluster_metadata_for_release_track_ids

    cluster_metadata_by_release_track_id = candidate_cluster_metadata_for_release_track_ids(release_track_ids)

    metadata: dict[str, dict[str, Any]] = {}
    for row in rows:
        history_raw_key = str(row["history_raw_key"] or "").strip()
        if not history_raw_key or history_raw_key in metadata:
            continue
        release_track_id = int(row["release_track_id"])
        source_track_count = int(row["source_track_count"] or 0)
        cluster_metadata = cluster_metadata_by_release_track_id.get(release_track_id)
        cluster_member_count = int(cluster_metadata["cluster_member_count"]) if cluster_metadata else 0
        metadata[history_raw_key] = {
            "release_track_id": release_track_id,
            "release_track_name": str(row["release_track_name"] or ""),
            "release_track_source_count": max(source_track_count, cluster_member_count),
            "has_release_track_siblings": source_track_count > 1 or cluster_member_count > 1,
        }
    return metadata


def _history_raw_key_for_item(item: dict[str, Any]) -> str | None:
    track_name = str(item.get("track_name") or item.get("track_name_raw") or "").strip()
    if not track_name:
        return None
    artist_name = item.get("artist_name") or item.get("artist_name_raw")
    album_name = item.get("album_name") or item.get("album_name_raw")
    fallback_artist_key = _normalize_fallback_artist_text(str(artist_name)) if artist_name is not None else None
    return _stable_text_key("history_raw_track", track_name, fallback_artist_key, str(album_name) if album_name is not None else None)


def enrich_track_rows_with_release_metadata(
    items: list[dict[str, Any]],
    *,
    track_id_key: str = "track_id",
) -> list[dict[str, Any]]:
    metadata_by_track_id = release_track_metadata_for_spotify_ids([
        str(item.get(track_id_key) or "").strip()
        for item in items
        if isinstance(item, dict)
    ])
    metadata_by_history_raw_key = release_track_metadata_for_history_raw_keys([
        history_raw_key
        for item in items
        if isinstance(item, dict)
        for history_raw_key in [_history_raw_key_for_item(item)]
        if history_raw_key is not None
    ])
    enriched: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        track_id = str(item.get(track_id_key) or "").strip()
        row = dict(item)
        metadata = metadata_by_track_id.get(track_id)
        if metadata is None:
            history_raw_key = _history_raw_key_for_item(row)
            metadata = metadata_by_history_raw_key.get(history_raw_key or "")
        if metadata:
            row.update(metadata)
        else:
            row.setdefault("release_track_id", None)
            row.setdefault("release_track_name", None)
            row.setdefault("release_track_source_count", 0)
            row.setdefault("has_release_track_siblings", False)
        enriched.append(row)
    return enriched


def enrich_album_track_rows_with_release_metadata(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return enrich_track_rows_with_release_metadata(items, track_id_key="id")
