from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from backend.app.db import sqlite_connection
from backend.app.recent_tracks_db import build_recent_tracks_section_from_db
from backend.app.release_track_metadata import enrich_track_rows_with_release_metadata


VERSION_HINT_PATTERN = re.compile(r"\b(live|remix|remaster|demo|edit|version|mono|stereo|acoustic)\b", re.IGNORECASE)


def _ratio(numerator: int, denominator: int) -> float:
    return round((numerator * 100.0) / denominator, 2) if denominator > 0 else 0.0


def _spotify_id_from_uri(uri: str | None) -> str | None:
    if isinstance(uri, str) and uri.startswith("spotify:track:"):
        return uri.rsplit(":", 1)[-1] or None
    return None


def _track_key(row: dict[str, Any]) -> str | None:
    track_id = str(row.get("track_id") or "").strip()
    if track_id:
        return track_id
    uri_id = _spotify_id_from_uri(row.get("uri"))
    if uri_id:
        return uri_id
    name = str(row.get("track_name") or "").strip().lower()
    artist = str(row.get("artist_name") or "").strip().lower()
    return f"fallback:{name}:{artist}" if name and artist else None


def _compact_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": row.get("event_id"),
        "track_id": row.get("track_id"),
        "uri": row.get("uri"),
        "release_track_id": row.get("release_track_id"),
        "track_name": row.get("track_name"),
        "artist_name": row.get("artist_name"),
        "album_name": row.get("album_name"),
        "played_at": row.get("spotify_played_at") or row.get("last_played_at"),
        "play_count": row.get("play_count"),
    }


def _summarize_rows(rows: list[dict[str, Any]], *, sample_limit: int) -> dict[str, Any]:
    rows_with_release = [row for row in rows if isinstance(row.get("release_track_id"), int)]
    rows_missing_release = [row for row in rows if not isinstance(row.get("release_track_id"), int)]
    track_keys = {_track_key(row) for row in rows}
    release_ids = {int(row["release_track_id"]) for row in rows_with_release}

    rows_by_release: dict[int, list[dict[str, Any]]] = defaultdict(list)
    track_ids_by_release: dict[int, set[str]] = defaultdict(set)
    artists_by_release: dict[int, set[str]] = defaultdict(set)
    albums_by_release: dict[int, set[str]] = defaultdict(set)
    durations_by_release: dict[int, list[int]] = defaultdict(list)
    names_by_release: dict[int, set[str]] = defaultdict(set)
    for row in rows_with_release:
        release_track_id = int(row["release_track_id"])
        rows_by_release[release_track_id].append(row)
        track_key = _track_key(row)
        if track_key:
            track_ids_by_release[release_track_id].add(track_key)
        artist_name = str(row.get("artist_name") or "").strip()
        if artist_name:
            artists_by_release[release_track_id].add(artist_name)
        album_name = str(row.get("album_name") or "").strip()
        if album_name:
            albums_by_release[release_track_id].add(album_name)
        track_name = str(row.get("track_name") or "").strip()
        if track_name:
            names_by_release[release_track_id].add(track_name)
        duration_ms = row.get("duration_ms")
        if isinstance(duration_ms, int) and duration_ms > 0:
            durations_by_release[release_track_id].append(duration_ms)

    sibling_groups = [
        {
            "release_track_id": release_track_id,
            "source_track_count": len(track_ids),
            "row_count": len(rows_by_release[release_track_id]),
            "track_ids": sorted(track_ids)[:10],
            "examples": [_compact_row(row) for row in rows_by_release[release_track_id][:3]],
        }
        for release_track_id, track_ids in track_ids_by_release.items()
        if len(track_ids) > 1
    ]
    sibling_groups.sort(key=lambda item: (-int(item["source_track_count"]), -int(item["row_count"]), int(item["release_track_id"])))

    suspicious_groups: list[dict[str, Any]] = []
    for release_track_id, group_rows in rows_by_release.items():
        reasons: list[str] = []
        durations = durations_by_release.get(release_track_id, [])
        names = names_by_release.get(release_track_id, set())
        if len(artists_by_release.get(release_track_id, set())) > 1:
            reasons.append("multiple_artist_strings")
        if durations and max(durations) - min(durations) > 15_000:
            reasons.append("duration_spread_over_15s")
        if any(VERSION_HINT_PATTERN.search(name) for name in names) and len(names) > 1:
            reasons.append("version_hint_in_sibling_names")
        if len(albums_by_release.get(release_track_id, set())) > 4:
            reasons.append("many_album_strings")
        if not reasons:
            continue
        suspicious_groups.append(
            {
                "release_track_id": release_track_id,
                "reasons": reasons,
                "source_track_count": len(track_ids_by_release.get(release_track_id, set())),
                "row_count": len(group_rows),
                "track_names": sorted(names)[:8],
                "artist_names": sorted(artists_by_release.get(release_track_id, set()))[:8],
                "album_names": sorted(albums_by_release.get(release_track_id, set()))[:8],
                "duration_ms_range": [min(durations), max(durations)] if durations else None,
                "examples": [_compact_row(row) for row in group_rows[:3]],
            }
        )
    suspicious_groups.sort(key=lambda item: (-int(item["source_track_count"]), -int(item["row_count"]), int(item["release_track_id"])))

    return {
        "total_rows_sampled": len(rows),
        "rows_with_release_track_id": len(rows_with_release),
        "rows_missing_release_track_id": len(rows_missing_release),
        "release_track_id_coverage_percent": _ratio(len(rows_with_release), len(rows)),
        "distinct_source_track_ids": len({key for key in track_keys if key}),
        "distinct_release_track_ids": len(release_ids),
        "release_track_groups_with_multiple_source_track_ids": len(sibling_groups),
        "examples_sibling_groups_that_would_collapse": sibling_groups[:sample_limit],
        "examples_missing_release_track_id": [_compact_row(row) for row in rows_missing_release[:sample_limit]],
        "examples_suspicious_groups": suspicious_groups[:sample_limit],
    }


def _query_backing_play_events(*, limit: int) -> list[dict[str, Any]]:
    bounded_limit = max(1, min(int(limit), 5_000))
    with sqlite_connection() as connection:
        connection.row_factory = None
        rows = connection.execute(
            """
            SELECT
              v.id,
              v.spotify_track_id,
              v.spotify_track_uri,
              v.track_name_canonical,
              v.artist_name_canonical,
              v.album_name_canonical,
              COALESCE(rr.track_duration_ms, rp.track_duration_ms, stc.duration_ms),
              v.canonical_ended_at,
              v.raw_spotify_recent_id,
              v.raw_spotify_history_id,
              v.raw_listenlab_player_play_id
            FROM v_fact_play_event_with_sources v
            LEFT JOIN raw_spotify_recent rr
              ON rr.id = v.raw_spotify_recent_id
            LEFT JOIN raw_listenlab_player_play rp
              ON rp.id = v.raw_listenlab_player_play_id
            LEFT JOIN raw_spotify_history rh
              ON rh.id = v.raw_spotify_history_id
            LEFT JOIN spotify_track_catalog stc
              ON stc.spotify_track_id = v.spotify_track_id
            WHERE v.canonical_ended_at IS NOT NULL
              AND (
                rh.id IS NULL
                OR json_extract(rh.raw_payload_json, '$.spotify_episode_uri') IS NULL
              )
            ORDER BY v.canonical_ended_at DESC, v.id DESC
            LIMIT ?
            """,
            (bounded_limit,),
        ).fetchall()
    items = [
        {
            "event_id": int(row[0]),
            "track_id": row[1],
            "uri": row[2],
            "track_name": row[3],
            "artist_name": row[4],
            "album_name": row[5],
            "duration_ms": int(row[6]) if isinstance(row[6], int) else None,
            "spotify_played_at": row[7],
            "raw_spotify_recent_id": row[8],
            "raw_spotify_history_id": row[9],
            "raw_listenlab_player_play_id": row[10],
        }
        for row in rows
    ]
    return enrich_track_rows_with_release_metadata(items)


def build_activity_release_track_coverage_audit(
    *,
    activity_limit: int = 50,
    backing_limit: int = 1_000,
    sample_limit: int = 5,
) -> dict[str, Any]:
    bounded_activity_limit = max(1, min(int(activity_limit), 200))
    bounded_backing_limit = max(1, min(int(backing_limit), 5_000))
    bounded_sample_limit = max(1, min(int(sample_limit), 20))

    activity_payload = build_recent_tracks_section_from_db(
        limit=bounded_activity_limit,
        recent_range="short_term",
        recent_window_days=28,
    )
    activity_rows = [dict(row) for row in activity_payload.get("items", []) if isinstance(row, dict)]
    backing_rows = _query_backing_play_events(limit=bounded_backing_limit)

    return {
        "scope": {
            "visible_activity_surface": "Dashboard Activity -> Completed/Skipped/Liked uses profile.recent_tracks",
            "visible_activity_backend": "/me or /me/recent -> backend.app.recent_tracks_db.build_recent_tracks_section_from_db",
            "larger_backing_surface": "Listen Log uses /debug/listening-log -> backend.app.listening_log.query_listening_log",
            "note": "/debug/listening-log currently does not expose release_track_id in its payload; this audit joins the DB source map directly for coverage.",
        },
        "required_payload_fields": {
            "spotify_source_track_id": "track_id",
            "release_track_id": "release_track_id",
            "display_title": "track_name",
            "artist_display": "artist_name",
            "album_display": "album_name",
            "played_at_recency": "spotify_played_at or last_played_at",
            "playback_target": "uri, falling back to spotify track_id",
        },
        "visible_activity_sample": _summarize_rows(activity_rows, sample_limit=bounded_sample_limit),
        "backing_play_event_sample": _summarize_rows(backing_rows, sample_limit=bounded_sample_limit),
        "grouping_risk_notes": [
            "Grouping by release_track_id should be applied to Activity display rows only, not raw play-event history rows.",
            "Grouped rows must preserve a play/event count and max played_at; grouping must not delete or mutate raw play events.",
            "Playback must keep a representative Spotify track_id/uri selected from the grouped siblings.",
        ],
    }
