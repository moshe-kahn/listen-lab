from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, TypedDict

from backend.app.db import sqlite_connection


ListeningLogSourceFilter = Literal["all", "api", "history", "both"]
ListeningLogSourceLabel = Literal["api", "history", "both"]


class ListeningLogItem(TypedDict, total=False):
    event_id: int
    track_id: str | None
    track_name: str | None
    artist_name: str | None
    album_name: str | None
    album_id: str | None
    uri: str | None
    spotify_played_at: str | None
    spotify_played_at_unix_ms: int | None
    estimated_played_ms: int | None
    estimated_played_seconds: float | None
    spotify_context_type: str | None
    spotify_context_uri: str | None
    source_label: ListeningLogSourceLabel
    has_recent_source: bool
    has_history_source: bool
    raw_spotify_recent_id: int | None
    raw_spotify_history_id: int | None
    timing_source: str | None
    matched_state: str | None
    spotify_skipped: bool | None
    spotify_shuffle: bool | None
    spotify_offline: bool | None


class ListeningLogPayload(TypedDict):
    items: list[ListeningLogItem]
    limit: int
    offset: int
    has_more: bool
    source_filter: ListeningLogSourceFilter


def query_listening_log(
    *,
    limit: int = 50,
    offset: int = 0,
    source_filter: ListeningLogSourceFilter = "all",
) -> ListeningLogPayload:
    bounded_limit = max(1, min(int(limit), 200))
    bounded_offset = max(0, int(offset))
    normalized_source_filter: ListeningLogSourceFilter = (
        source_filter if source_filter in {"all", "api", "history", "both"} else "all"
    )

    where_clause = ""
    if normalized_source_filter == "api":
        where_clause = "AND raw_spotify_recent_id IS NOT NULL AND raw_spotify_history_id IS NULL"
    elif normalized_source_filter == "history":
        where_clause = "AND raw_spotify_history_id IS NOT NULL AND raw_spotify_recent_id IS NULL"
    elif normalized_source_filter == "both":
        where_clause = "AND raw_spotify_recent_id IS NOT NULL AND raw_spotify_history_id IS NOT NULL"

    with sqlite_connection(row_factory=None) as connection:
        rows = connection.execute(
            f"""
            SELECT
              id,
              canonical_ended_at,
              canonical_ms_played,
              canonical_context_type,
              canonical_context_uri,
              spotify_track_id,
              spotify_track_uri,
              spotify_album_id,
              track_name_canonical,
              artist_name_canonical,
              album_name_canonical,
              timing_source,
              matched_state,
              canonical_skipped,
              canonical_shuffle,
              canonical_offline,
              raw_spotify_recent_id,
              raw_spotify_history_id
            FROM v_fact_play_event_with_sources
            WHERE canonical_ended_at IS NOT NULL
              {where_clause}
            ORDER BY canonical_ended_at DESC, id DESC
            LIMIT ?
            OFFSET ?
            """,
            (bounded_limit + 1, bounded_offset),
        ).fetchall()

    has_more = len(rows) > bounded_limit
    rows = rows[:bounded_limit]

    items: list[ListeningLogItem] = []
    for row in rows:
        (
            event_id,
            canonical_ended_at,
            canonical_ms_played,
            canonical_context_type,
            canonical_context_uri,
            spotify_track_id,
            spotify_track_uri,
            spotify_album_id,
            track_name_canonical,
            artist_name_canonical,
            album_name_canonical,
            timing_source,
            matched_state,
            canonical_skipped,
            canonical_shuffle,
            canonical_offline,
            raw_spotify_recent_id,
            raw_spotify_history_id,
        ) = row

        has_recent_source = raw_spotify_recent_id is not None
        has_history_source = raw_spotify_history_id is not None
        source_label: ListeningLogSourceLabel = (
            "both" if has_recent_source and has_history_source else "history" if has_history_source else "api"
        )

        played_at_unix_ms: int | None = None
        if isinstance(canonical_ended_at, str):
            try:
                played_at_unix_ms = int(datetime.fromisoformat(canonical_ended_at.replace("Z", "+00:00")).timestamp() * 1000)
            except ValueError:
                played_at_unix_ms = None

        estimated_played_ms = int(canonical_ms_played) if isinstance(canonical_ms_played, int) else None

        items.append(
            {
                "event_id": int(event_id),
                "track_id": spotify_track_id,
                "track_name": track_name_canonical,
                "artist_name": artist_name_canonical,
                "album_name": album_name_canonical,
                "album_id": spotify_album_id,
                "uri": spotify_track_uri,
                "spotify_played_at": canonical_ended_at,
                "spotify_played_at_unix_ms": played_at_unix_ms,
                "estimated_played_ms": estimated_played_ms,
                "estimated_played_seconds": round(estimated_played_ms / 1000.0, 3) if isinstance(estimated_played_ms, int) and estimated_played_ms >= 0 else None,
                "spotify_context_type": canonical_context_type,
                "spotify_context_uri": canonical_context_uri,
                "source_label": source_label,
                "has_recent_source": has_recent_source,
                "has_history_source": has_history_source,
                "raw_spotify_recent_id": int(raw_spotify_recent_id) if isinstance(raw_spotify_recent_id, int) else None,
                "raw_spotify_history_id": int(raw_spotify_history_id) if isinstance(raw_spotify_history_id, int) else None,
                "timing_source": timing_source,
                "matched_state": matched_state,
                "spotify_skipped": bool(canonical_skipped) if canonical_skipped is not None else None,
                "spotify_shuffle": bool(canonical_shuffle) if canonical_shuffle is not None else None,
                "spotify_offline": bool(canonical_offline) if canonical_offline is not None else None,
            }
        )

    return {
        "items": items,
        "limit": bounded_limit,
        "offset": bounded_offset,
        "has_more": has_more,
        "source_filter": normalized_source_filter,
    }
