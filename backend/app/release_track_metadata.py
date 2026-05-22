from __future__ import annotations

import sqlite3
from typing import Any

from backend.app.db import sqlite_connection


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

    metadata: dict[str, dict[str, Any]] = {}
    for row in rows:
        spotify_track_id = str(row["spotify_track_id"] or "").strip()
        if not spotify_track_id or spotify_track_id in metadata:
            continue
        source_track_count = int(row["source_track_count"] or 0)
        metadata[spotify_track_id] = {
            "release_track_id": int(row["release_track_id"]),
            "release_track_name": str(row["release_track_name"] or ""),
            "release_track_source_count": source_track_count,
            "has_release_track_siblings": source_track_count > 1,
        }
    return metadata


def enrich_album_track_rows_with_release_metadata(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    metadata_by_track_id = release_track_metadata_for_spotify_ids([
        str(item.get("id") or "").strip()
        for item in items
        if isinstance(item, dict)
    ])
    enriched: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        track_id = str(item.get("id") or "").strip()
        row = dict(item)
        metadata = metadata_by_track_id.get(track_id)
        if metadata:
            row.update(metadata)
        else:
            row.setdefault("release_track_id", None)
            row.setdefault("release_track_name", None)
            row.setdefault("release_track_source_count", 0)
            row.setdefault("has_release_track_siblings", False)
        enriched.append(row)
    return enriched
