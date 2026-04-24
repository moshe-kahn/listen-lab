from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from typing import TypedDict

from backend.app.db import sqlite_connection
from backend.app.track_sections import CanonicalTrackSectionItem, CanonicalTrackSectionPayload


class RecentTopTrackQueryRow(TypedDict):
    track_identity: str
    spotify_track_id: str | None
    spotify_track_uri: str | None
    spotify_album_id: str | None
    track_name_raw: str | None
    artist_name_raw: str | None
    album_name_raw: str | None
    recent_play_count: int
    all_time_play_count: int
    first_played_at: str | None
    last_played_at: str | None


def query_recent_top_track_rows(
    *,
    limit: int,
    recent_window_days: int,
    as_of_iso: str | None = None,
) -> list[RecentTopTrackQueryRow]:
    bounded_limit = max(1, int(limit))
    bounded_window_days = max(0, int(recent_window_days))
    as_of_dt = (
        datetime.fromisoformat(as_of_iso.replace("Z", "+00:00"))
        if as_of_iso
        else datetime.now(UTC)
    )
    recent_cutoff_iso = (
        (as_of_dt - timedelta(days=bounded_window_days))
        .astimezone(UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )

    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        raw_rows = connection.execute(
            """
            WITH normalized AS (
              SELECT
                id,
                canonical_ended_at AS played_at,
                CASE
                  WHEN spotify_track_id IS NOT NULL AND spotify_track_id != '' THEN spotify_track_id
                  WHEN spotify_track_uri IS NOT NULL AND spotify_track_uri != '' THEN spotify_track_uri
                  ELSE '__unknown__:' || LOWER(TRIM(COALESCE(track_name_canonical, ''))) || ':' || LOWER(TRIM(COALESCE(artist_name_canonical, '')))
                END AS track_identity,
                spotify_track_id,
                spotify_track_uri,
                spotify_album_id,
                track_name_canonical AS track_name_raw,
                artist_name_canonical AS artist_name_raw,
                album_name_canonical AS album_name_raw
              FROM v_fact_play_event_with_sources
              WHERE canonical_ended_at IS NOT NULL
            ),
            agg AS (
              SELECT
                track_identity,
                COUNT(*) AS all_time_play_count,
                SUM(CASE WHEN played_at >= ? THEN 1 ELSE 0 END) AS recent_play_count,
                MIN(played_at) AS first_played_at,
                MAX(played_at) AS last_played_at
              FROM normalized
              GROUP BY track_identity
              HAVING SUM(CASE WHEN played_at >= ? THEN 1 ELSE 0 END) > 0
            )
            SELECT
              agg.track_identity AS track_identity,
              (
                SELECT n.spotify_track_id
                FROM normalized n
                WHERE n.track_identity = agg.track_identity
                ORDER BY n.played_at DESC, n.id DESC
                LIMIT 1
              ) AS spotify_track_id,
              (
                SELECT n.spotify_track_uri
                FROM normalized n
                WHERE n.track_identity = agg.track_identity
                ORDER BY n.played_at DESC, n.id DESC
                LIMIT 1
              ) AS spotify_track_uri,
              (
                SELECT n.spotify_album_id
                FROM normalized n
                WHERE n.track_identity = agg.track_identity
                ORDER BY n.played_at DESC, n.id DESC
                LIMIT 1
              ) AS spotify_album_id,
              COALESCE(
                (
                  SELECT n.track_name_raw
                  FROM normalized n
                  WHERE n.track_identity = agg.track_identity
                  ORDER BY n.played_at DESC, n.id DESC
                  LIMIT 1
                ),
                'Unknown track'
              ) AS track_name_raw,
              COALESCE(
                (
                  SELECT n.artist_name_raw
                  FROM normalized n
                  WHERE n.track_identity = agg.track_identity
                  ORDER BY n.played_at DESC, n.id DESC
                  LIMIT 1
                ),
                'Unknown artist'
              ) AS artist_name_raw,
              (
                SELECT n.album_name_raw
                FROM normalized n
                WHERE n.track_identity = agg.track_identity
                ORDER BY n.played_at DESC, n.id DESC
                LIMIT 1
              ) AS album_name_raw,
              agg.recent_play_count AS recent_play_count,
              agg.all_time_play_count AS all_time_play_count,
              agg.first_played_at AS first_played_at,
              agg.last_played_at AS last_played_at
            FROM agg
            ORDER BY
              agg.recent_play_count DESC,
              agg.last_played_at DESC,
              agg.all_time_play_count DESC,
              agg.track_identity ASC
            LIMIT ?
            """,
            (recent_cutoff_iso, recent_cutoff_iso, bounded_limit),
        ).fetchall()

    rows: list[RecentTopTrackQueryRow] = []
    for row in raw_rows:
        rows.append(
            {
                "track_identity": str(row["track_identity"]),
                "spotify_track_id": row["spotify_track_id"],
                "spotify_track_uri": row["spotify_track_uri"],
                "spotify_album_id": row["spotify_album_id"],
                "track_name_raw": row["track_name_raw"],
                "artist_name_raw": row["artist_name_raw"],
                "album_name_raw": row["album_name_raw"],
                "recent_play_count": int(row["recent_play_count"]),
                "all_time_play_count": int(row["all_time_play_count"]),
                "first_played_at": row["first_played_at"],
                "last_played_at": row["last_played_at"],
            }
        )
    return rows


def map_recent_top_track_row_to_canonical_item(row: RecentTopTrackQueryRow) -> CanonicalTrackSectionItem:
    recent_play_count = int(row["recent_play_count"])
    all_time_play_count = int(row["all_time_play_count"])
    return {
        "track_id": row.get("spotify_track_id") or row["track_identity"],
        "track_name": row.get("track_name_raw"),
        "artist_name": row.get("artist_name_raw"),
        "album_name": row.get("album_name_raw"),
        "album_release_year": None,
        "artists": None,
        "duration_ms": None,
        "duration_seconds": None,
        "uri": row.get("spotify_track_uri"),
        "preview_url": None,
        "url": None,
        "image_url": None,
        "album_id": row.get("spotify_album_id"),
        "album_url": None,
        "spotify_played_at": None,
        "spotify_played_at_unix_ms": None,
        "spotify_context_type": None,
        "spotify_context_uri": None,
        "spotify_context_url": None,
        "spotify_context_href": None,
        "spotify_is_local": None,
        "spotify_track_type": None,
        "spotify_track_number": None,
        "spotify_disc_number": None,
        "spotify_explicit": None,
        "spotify_popularity": None,
        "spotify_album_type": None,
        "spotify_album_total_tracks": None,
        "spotify_available_markets_count": None,
        "played_at_gap_ms": None,
        "estimated_played_ms": None,
        "estimated_played_seconds": None,
        "estimated_completion_ratio": None,
        "play_count": recent_play_count,
        "all_time_play_count": all_time_play_count,
        "recent_play_count": recent_play_count,
        "first_played_at": row.get("first_played_at"),
        "last_played_at": row.get("last_played_at"),
        "listening_span_days": None,
        "listening_span_years": None,
        "active_months_count": None,
        "span_months_count": None,
        "consistency_ratio": None,
        "longevity_score": None,
        "debug": {
            "source": "db",
            "primary_source": "db",
            "fallback_source": None,
            "section_kind": "top_tracks",
            "section_window": "recent",
        },
    }


def build_recent_top_tracks_section_from_db(
    *,
    limit: int,
    recent_range: str,
    recent_window_days: int,
    as_of_iso: str | None = None,
) -> CanonicalTrackSectionPayload:
    rows = query_recent_top_track_rows(
        limit=limit,
        recent_window_days=recent_window_days,
        as_of_iso=as_of_iso,
    )
    items = [map_recent_top_track_row_to_canonical_item(row) for row in rows]
    return {
        "items": items,
        "available": bool(items),
        "recent_range": recent_range if recent_range in {"short_term", "medium_term"} else None,
        "recent_window_days": int(recent_window_days),
        "debug": {
            "source": "db",
            "primary_source": "db",
            "fallback_source": None,
            "section_kind": "top_tracks",
            "section_window": "recent",
        },
    }
