from __future__ import annotations

import json
import re
import sqlite3
from datetime import UTC, datetime
from typing import Any

from backend.app.db import sqlite_connection
from backend.app.recording_track_candidates import recording_representatives_for_release_track_ids


LIBRARY_RULE_VERSION = 1
LIBRARY_STRENGTHS = ("primary", "contextual", "potential", "ephemeral")
FAVORITE_PLAYLIST_CATEGORY_NAMES = {"favorite", "favorites", "liked", "likes"}
STRENGTH_RANK = {
    "ephemeral": 0,
    "potential": 1,
    "contextual": 2,
    "primary": 3,
}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _artist_name_from_json(value: Any) -> str | None:
    names: list[str] = []
    for artist in _json_list(value):
        if isinstance(artist, dict):
            name = str(artist.get("name") or "").strip()
            if name:
                names.append(name)
        elif isinstance(artist, str) and artist.strip():
            names.append(artist.strip())
    return ", ".join(dict.fromkeys(names)) or None


def _normalize_artist_name_value(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    return _artist_name_from_json(raw) or raw


def _is_identifier_title(title: Any, spotify_track_id: Any) -> bool:
    normalized_title = str(title or "").strip().lower()
    normalized_id = str(spotify_track_id or "").strip().lower()
    return bool(
        normalized_title
        and normalized_id
        and normalized_title in {normalized_id, f"spotify:track:{normalized_id}"}
    )


def _display_track_name(title: Any, spotify_track_id: Any) -> str:
    raw = str(title or "").strip()
    return "Unknown track" if not raw or _is_identifier_title(raw, spotify_track_id) else raw


def _has_display_title(title: Any, spotify_track_id: Any) -> bool:
    raw = str(title or "").strip()
    return bool(raw and not _is_identifier_title(raw, spotify_track_id))


def _is_unavailable_candidate(candidate: dict[str, Any], spotify_track_id: str) -> bool:
    evidence_reasons = set(candidate.get("reasons") or {})
    return (
        not (evidence_reasons & {"liked", "listened_1_2", "listened_3_plus", "observed_only"})
        and not bool(candidate.get("is_liked"))
        and not _has_display_title(candidate.get("track_name"), spotify_track_id)
        and not str(candidate.get("artist_name") or "").strip()
        and not str(candidate.get("album_name") or "").strip()
        and not str(candidate.get("image_url") or "").strip()
        and candidate.get("duration_ms") is None
        and int(candidate.get("play_count") or 0) <= 0
    )


def _images_first_url(value: Any) -> str | None:
    for image in _json_list(value):
        if isinstance(image, dict):
            url = str(image.get("url") or "").strip()
            if url:
                return url
    return None


def _track_url(track_id: str | None) -> str | None:
    normalized = str(track_id or "").strip()
    return f"https://open.spotify.com/track/{normalized}" if normalized else None


def _normalize_strength(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in STRENGTH_RANK else None


def _merge_text(current: Any, incoming: Any) -> Any:
    if current is not None and str(current).strip():
        return current
    if incoming is not None and str(incoming).strip():
        return incoming
    return current


def _merge_int(current: Any, incoming: Any) -> Any:
    return current if current is not None else incoming


def _merge_time_min(current: str | None, incoming: str | None) -> str | None:
    if not current:
        return incoming
    if not incoming:
        return current
    return min(current, incoming)


def _merge_time_max(current: str | None, incoming: str | None) -> str | None:
    if not current:
        return incoming
    if not incoming:
        return current
    return max(current, incoming)


def _add_candidate(
    candidates: dict[str, dict[str, Any]],
    *,
    spotify_track_id: str | None,
    strength: str,
    reason: str,
    reason_label: str,
    track_name: str | None = None,
    artist_name: str | None = None,
    album_name: str | None = None,
    album_id: str | None = None,
    image_url: str | None = None,
    uri: str | None = None,
    url: str | None = None,
    duration_ms: int | None = None,
    artists_json: str | None = None,
    play_count: int | None = None,
    first_played_at: str | None = None,
    last_played_at: str | None = None,
    liked_at: str | None = None,
    is_liked: bool | None = None,
    playlist_id: str | None = None,
    playlist_name: str | None = None,
    release_track_id: int | None = None,
    recording_representative_release_track_id: int | None = None,
    evidence_at: str | None = None,
) -> None:
    track_id = str(spotify_track_id or "").strip()
    normalized_strength = _normalize_strength(strength)
    if not track_id or normalized_strength is None:
        return
    candidate = candidates.setdefault(
        track_id,
        {
            "spotify_track_id": track_id,
            "track_name": None,
            "artist_name": None,
            "album_name": None,
            "album_id": None,
            "image_url": None,
            "uri": None,
            "url": _track_url(track_id),
            "duration_ms": None,
            "artists_json": None,
            "strength": "ephemeral",
            "reasons": {},
            "play_count": 0,
            "first_played_at": None,
            "last_played_at": None,
            "playlist_ids": set(),
            "liked_at": None,
            "is_liked": False,
            "source_playlist_id": None,
            "source_playlist_name": None,
            "source_album_id": None,
            "source_album_name": None,
            "evidence_first_seen_at": None,
            "evidence_last_seen_at": None,
            "release_track_id": None,
            "recording_representative_release_track_id": None,
        },
    )
    if STRENGTH_RANK[normalized_strength] > STRENGTH_RANK[str(candidate["strength"])]:
        candidate["strength"] = normalized_strength
    candidate["reasons"][reason] = reason_label
    candidate["track_name"] = _merge_text(candidate["track_name"], track_name)
    candidate["artist_name"] = _merge_text(candidate["artist_name"], _normalize_artist_name_value(artist_name) or _artist_name_from_json(artists_json))
    candidate["album_name"] = _merge_text(candidate["album_name"], album_name)
    candidate["album_id"] = _merge_text(candidate["album_id"], album_id)
    candidate["image_url"] = _merge_text(candidate["image_url"], image_url)
    candidate["uri"] = _merge_text(candidate["uri"], uri or f"spotify:track:{track_id}")
    candidate["url"] = _merge_text(candidate["url"], url or _track_url(track_id))
    candidate["duration_ms"] = _merge_int(candidate["duration_ms"], duration_ms)
    candidate["artists_json"] = _merge_text(candidate["artists_json"], artists_json)
    candidate["play_count"] = max(int(candidate["play_count"] or 0), int(play_count or 0))
    candidate["first_played_at"] = _merge_time_min(candidate["first_played_at"], first_played_at)
    candidate["last_played_at"] = _merge_time_max(candidate["last_played_at"], last_played_at)
    if is_liked is True:
        candidate["is_liked"] = True
    candidate["liked_at"] = _merge_time_max(candidate["liked_at"], liked_at)
    if playlist_id:
        candidate["playlist_ids"].add(str(playlist_id))
        candidate["source_playlist_id"] = _merge_text(candidate["source_playlist_id"], str(playlist_id))
        candidate["source_playlist_name"] = _merge_text(candidate["source_playlist_name"], playlist_name)
    if album_id:
        candidate["source_album_id"] = _merge_text(candidate["source_album_id"], album_id)
        candidate["source_album_name"] = _merge_text(candidate["source_album_name"], album_name)
    evidence_candidates = [evidence_at, liked_at, last_played_at, first_played_at]
    for timestamp in evidence_candidates:
        candidate["evidence_first_seen_at"] = _merge_time_min(candidate["evidence_first_seen_at"], timestamp)
        candidate["evidence_last_seen_at"] = _merge_time_max(candidate["evidence_last_seen_at"], timestamp)
    if release_track_id is not None and int(release_track_id) > 0:
        candidate["release_track_id"] = int(release_track_id)
    if recording_representative_release_track_id is not None and int(recording_representative_release_track_id) > 0:
        candidate["recording_representative_release_track_id"] = int(recording_representative_release_track_id)


def _favorite_playlist_ids(connection: sqlite3.Connection, user_id: str) -> set[str]:
    rows = connection.execute(
        """
        SELECT DISTINCT m.playlist_id
        FROM playlist_category_member m
        JOIN playlist_category c
          ON c.user_id = m.user_id
         AND c.id = m.category_id
        WHERE m.user_id = ?
          AND lower(trim(c.name)) IN ({placeholders})
        """.format(placeholders=",".join("?" for _ in FAVORITE_PLAYLIST_CATEGORY_NAMES)),
        (user_id, *sorted(FAVORITE_PLAYLIST_CATEGORY_NAMES)),
    ).fetchall()
    return {str(row["playlist_id"]) for row in rows if row["playlist_id"]}


def _album_context_album_ids(candidates: dict[str, dict[str, Any]]) -> set[str]:
    album_ids: set[str] = set()
    for candidate in candidates.values():
        strength = str(candidate.get("strength") or "")
        album_id = str(candidate.get("album_id") or "").strip()
        if album_id and STRENGTH_RANK.get(strength, 0) >= STRENGTH_RANK["potential"]:
            album_ids.add(album_id)
    return album_ids


def _hydrate_candidate_identity(connection: sqlite3.Connection, candidates: dict[str, dict[str, Any]]) -> None:
    track_ids = sorted(candidates.keys())
    if not track_ids:
        return
    release_by_track_id: dict[str, int] = {}
    for start in range(0, len(track_ids), 500):
        chunk = track_ids[start:start + 500]
        track_uris = [f"spotify:track:{track_id}" for track_id in chunk]
        id_placeholders = ",".join("?" for _ in chunk)
        uri_placeholders = ",".join("?" for _ in track_uris)
        rows = connection.execute(
            f"""
            WITH mapped AS (
              SELECT
                CASE
                  WHEN st.external_id IN ({id_placeholders}) THEN st.external_id
                  WHEN st.external_id IN ({uri_placeholders}) THEN replace(st.external_id, 'spotify:track:', '')
                  WHEN st.external_uri IN ({uri_placeholders}) THEN replace(st.external_uri, 'spotify:track:', '')
                  ELSE NULL
                END AS track_id,
                stm.release_track_id,
                row_number() OVER (
                  PARTITION BY CASE
                    WHEN st.external_id IN ({id_placeholders}) THEN st.external_id
                    WHEN st.external_id IN ({uri_placeholders}) THEN replace(st.external_id, 'spotify:track:', '')
                    WHEN st.external_uri IN ({uri_placeholders}) THEN replace(st.external_uri, 'spotify:track:', '')
                    ELSE NULL
                  END
                  ORDER BY
                    CASE WHEN stm.status = 'accepted' THEN 0 ELSE 1 END,
                    stm.confidence DESC,
                    stm.release_track_id ASC
                ) AS match_rank
              FROM source_track st
              JOIN source_track_map stm
                ON stm.source_track_id = st.id
              WHERE st.source_name = 'spotify'
                AND (
                  st.external_id IN ({id_placeholders})
                  OR st.external_id IN ({uri_placeholders})
                  OR st.external_uri IN ({uri_placeholders})
                )
            )
            SELECT track_id, release_track_id
            FROM mapped
            WHERE match_rank = 1
              AND track_id IS NOT NULL
            """,
            (
                *chunk,
                *track_uris,
                *track_uris,
                *chunk,
                *track_uris,
                *track_uris,
                *chunk,
                *track_uris,
                *track_uris,
            ),
        ).fetchall()
        for row in rows:
            track_id = str(row["track_id"] or "").strip()
            if track_id and row["release_track_id"] is not None:
                release_by_track_id[track_id] = int(row["release_track_id"])
    representative_by_release_id = recording_representatives_for_release_track_ids(list(release_by_track_id.values()))
    for track_id, release_track_id in release_by_track_id.items():
        candidate = candidates.get(track_id)
        if candidate is None:
            continue
        candidate["release_track_id"] = release_track_id
        candidate["recording_representative_release_track_id"] = representative_by_release_id.get(release_track_id, release_track_id)


def _mark_rebuild_state(
    user_id: str,
    *,
    status: str,
    stale: bool,
    started_at: str | None = None,
    completed_at: str | None = None,
    row_count: int | None = None,
    latest_error: str | None = None,
) -> None:
    now = _utc_now()
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            INSERT INTO personal_library_rebuild_state (
              user_id, status, rule_version, stale, started_at, completed_at,
              row_count, latest_error, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
              status = excluded.status,
              rule_version = excluded.rule_version,
              stale = excluded.stale,
              started_at = COALESCE(excluded.started_at, personal_library_rebuild_state.started_at),
              completed_at = excluded.completed_at,
              row_count = COALESCE(excluded.row_count, personal_library_rebuild_state.row_count),
              latest_error = excluded.latest_error,
              updated_at = excluded.updated_at
            """,
            (
                user_id,
                status,
                LIBRARY_RULE_VERSION,
                1 if stale else 0,
                started_at,
                completed_at,
                0 if row_count is None else row_count,
                latest_error,
                now,
            ),
        )


def rebuild_personal_library(user_id: str) -> dict[str, Any]:
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        raise ValueError("user_id is required.")
    started_at = _utc_now()
    _mark_rebuild_state(normalized_user_id, status="running", stale=True, started_at=started_at)
    try:
        candidates: dict[str, dict[str, Any]] = {}
        with sqlite_connection(row_factory=sqlite3.Row) as connection:
            liked_rows = connection.execute(
                """
                SELECT *
                FROM spotify_liked_track_cache
                WHERE user_id = ?
                  AND is_liked = 1
                """,
                (normalized_user_id,),
            ).fetchall()
            for row in liked_rows:
                _add_candidate(
                    candidates,
                    spotify_track_id=row["spotify_track_id"],
                    strength="primary",
                    reason="liked",
                    reason_label="Liked",
                    track_name=row["name"],
                    artist_name=row["artist_names"],
                    album_name=row["album_name"],
                    album_id=row["album_spotify_id"],
                    image_url=row["album_image_url"],
                    uri=row["uri"],
                    duration_ms=row["duration_ms"],
                    liked_at=row["liked_at"],
                    is_liked=True,
                    evidence_at=row["last_seen_at"],
                )

            play_rows = connection.execute(
                """
                SELECT
                  pc.spotify_track_id,
                  pc.play_count,
                  pc.first_played_at,
                  pc.last_played_at,
                  pc.track_name AS history_track_name,
                  pc.artist_name AS history_artist_name,
                  pc.album_name AS history_album_name,
                  pc.album_id AS history_album_id,
                  stc.name AS catalog_name,
                  stc.duration_ms AS catalog_duration_ms,
                  stc.album_id AS catalog_album_id,
                  stc.artists_json AS catalog_artists_json,
                  sac.name AS catalog_album_name,
                  json_extract(sac.images_json, '$[0].url') AS catalog_image_url,
                  sat.name AS album_track_name,
                  sat.duration_ms AS album_track_duration_ms,
                  sat.spotify_album_id AS album_track_album_id,
                  sat.artists_json AS album_track_artists_json,
                  sat_album.name AS album_track_album_name,
                  json_extract(sat_album.images_json, '$[0].url') AS album_track_image_url
                FROM source_track_play_count_cache pc
                LEFT JOIN spotify_track_catalog stc
                  ON stc.spotify_track_id = pc.spotify_track_id
                LEFT JOIN spotify_album_catalog sac
                  ON sac.spotify_album_id = stc.album_id
                LEFT JOIN spotify_album_track sat
                  ON sat.spotify_track_id = pc.spotify_track_id
                LEFT JOIN spotify_album_catalog sat_album
                  ON sat_album.spotify_album_id = sat.spotify_album_id
                GROUP BY pc.spotify_track_id
                """,
            ).fetchall()
            for row in play_rows:
                play_count = int(row["play_count"] or 0)
                if play_count >= 3:
                    strength = "primary"
                    reason_label = f"{play_count} listens"
                    reason = "listened_3_plus"
                elif play_count > 0:
                    strength = "contextual"
                    reason_label = f"{play_count} listen{'s' if play_count != 1 else ''}"
                    reason = "listened_1_2"
                else:
                    strength = "ephemeral"
                    reason_label = "Observed"
                    reason = "observed_only"
                _add_candidate(
                    candidates,
                    spotify_track_id=row["spotify_track_id"],
                    strength=strength,
                    reason=reason,
                    reason_label=reason_label,
                    track_name=row["catalog_name"] or row["album_track_name"] or row["history_track_name"],
                    artist_name=(
                        _artist_name_from_json(row["catalog_artists_json"] or row["album_track_artists_json"])
                        or row["history_artist_name"]
                    ),
                    album_name=row["catalog_album_name"] or row["album_track_album_name"] or row["history_album_name"],
                    album_id=row["catalog_album_id"] or row["album_track_album_id"] or row["history_album_id"],
                    image_url=row["catalog_image_url"] or row["album_track_image_url"],
                    duration_ms=row["catalog_duration_ms"] or row["album_track_duration_ms"],
                    artists_json=row["catalog_artists_json"] or row["album_track_artists_json"],
                    play_count=play_count,
                    first_played_at=row["first_played_at"],
                    last_played_at=row["last_played_at"],
                    evidence_at=row["last_played_at"],
                )

            favorite_playlist_ids = _favorite_playlist_ids(connection, normalized_user_id)
            playlist_rows = connection.execute(
                """
                SELECT
                  ptc.*,
                  pc.name AS playlist_name,
                  pc.is_owned,
                  pc.playlist_category,
                  pc.hidden_by_user,
                  sac.name AS catalog_album_name,
                  json_extract(sac.images_json, '$[0].url') AS catalog_album_image_url
                FROM spotify_playlist_track_cache ptc
                JOIN spotify_playlist_cache pc
                  ON pc.user_id = ptc.user_id
                 AND pc.playlist_id = ptc.playlist_id
                LEFT JOIN spotify_album_catalog sac
                  ON sac.spotify_album_id = ptc.album_id
                WHERE ptc.user_id = ?
                  AND COALESCE(pc.hidden_by_user, 0) = 0
                """,
                (normalized_user_id,),
            ).fetchall()
            for row in playlist_rows:
                playlist_id = str(row["playlist_id"])
                is_owned = bool(row["is_owned"])
                if is_owned:
                    strength = "primary"
                    reason = "own_playlist"
                    reason_label = "Own playlist"
                elif playlist_id in favorite_playlist_ids:
                    strength = "contextual"
                    reason = "favorite_playlist"
                    reason_label = "Favorite playlist"
                else:
                    strength = "potential"
                    reason = "followed_playlist"
                    reason_label = "Followed playlist"
                _add_candidate(
                    candidates,
                    spotify_track_id=row["spotify_track_id"],
                    strength=strength,
                    reason=reason,
                    reason_label=reason_label,
                    track_name=row["track_name"],
                    artist_name=row["artist_name"],
                    album_name=row["catalog_album_name"] or row["album_name"],
                    album_id=row["album_id"],
                    image_url=row["catalog_album_image_url"] or row["image_url"],
                    uri=row["uri"],
                    url=row["url"],
                    duration_ms=row["duration_ms"],
                    artists_json=row["artists_json"],
                    playlist_id=playlist_id,
                    playlist_name=row["playlist_name"],
                    evidence_at=row["added_at"] or row["cached_at"],
                )

            album_ids = _album_context_album_ids(candidates)
            if album_ids:
                placeholders = ",".join("?" for _ in album_ids)
                album_rows = connection.execute(
                    f"""
                    SELECT
                      sat.spotify_track_id,
                      sat.spotify_album_id,
                      sat.name AS track_name,
                      sat.duration_ms,
                      sat.artists_json,
                      sac.name AS album_name,
                      json_extract(sac.images_json, '$[0].url') AS image_url
                    FROM spotify_album_track sat
                    LEFT JOIN spotify_album_catalog sac
                      ON sac.spotify_album_id = sat.spotify_album_id
                    WHERE sat.spotify_album_id IN ({placeholders})
                      AND COALESCE(lower(sat.last_status), '') != 'error'
                    """,
                    tuple(sorted(album_ids)),
                ).fetchall()
                for row in album_rows:
                    _add_candidate(
                        candidates,
                        spotify_track_id=row["spotify_track_id"],
                        strength="contextual",
                        reason="album_context",
                        reason_label="Album context",
                        track_name=row["track_name"],
                        artist_name=_artist_name_from_json(row["artists_json"]),
                        album_name=row["album_name"],
                        album_id=row["spotify_album_id"],
                        image_url=row["image_url"],
                        duration_ms=row["duration_ms"],
                        artists_json=row["artists_json"],
                    )

            _hydrate_candidate_identity(connection, candidates)

        rebuilt_at = _utc_now()
        rows: list[tuple[Any, ...]] = []
        for track_id, candidate in sorted(candidates.items()):
            if _is_unavailable_candidate(candidate, track_id):
                continue
            reasons = [
                {"reason": key, "label": value}
                for key, value in sorted(candidate["reasons"].items())
            ]
            rows.append(
                (
                    normalized_user_id,
                    track_id,
                    _display_track_name(candidate["track_name"], track_id),
                    candidate["artist_name"],
                    candidate["album_name"],
                    candidate["album_id"],
                    candidate["image_url"],
                    candidate["uri"],
                    candidate["url"],
                    candidate["duration_ms"],
                    candidate["artists_json"],
                    candidate["strength"],
                    json.dumps(reasons, sort_keys=True),
                    candidate["play_count"],
                    candidate["first_played_at"],
                    candidate["last_played_at"],
                    len(candidate["playlist_ids"]),
                    candidate["liked_at"],
                    1 if candidate["is_liked"] else 0,
                    candidate["source_playlist_id"],
                    candidate["source_playlist_name"],
                    candidate["source_album_id"],
                    candidate["source_album_name"],
                    candidate["evidence_first_seen_at"],
                    candidate["evidence_last_seen_at"],
                    candidate["release_track_id"],
                    candidate["recording_representative_release_track_id"],
                    LIBRARY_RULE_VERSION,
                    rebuilt_at,
                )
            )
        with sqlite_connection(write=True) as connection:
            connection.execute(
                "DELETE FROM personal_library_track_cache WHERE user_id = ?",
                (normalized_user_id,),
            )
            connection.executemany(
                """
                INSERT INTO personal_library_track_cache (
                  user_id, spotify_track_id, track_name, artist_name, album_name,
                  album_id, image_url, uri, url, duration_ms, artists_json,
                  strength, reasons_json, play_count, first_played_at,
                  last_played_at, playlist_count, liked_at, is_liked,
                  source_playlist_id, source_playlist_name, source_album_id,
                  source_album_name, evidence_first_seen_at, evidence_last_seen_at,
                  release_track_id, recording_representative_release_track_id,
                  rule_version, rebuilt_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            connection.execute(
                """
                INSERT INTO personal_library_rebuild_state (
                  user_id, status, rule_version, stale, started_at, completed_at,
                  row_count, latest_error, updated_at
                )
                VALUES (?, 'complete', ?, 0, ?, ?, ?, NULL, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                  status = excluded.status,
                  rule_version = excluded.rule_version,
                  stale = excluded.stale,
                  started_at = excluded.started_at,
                  completed_at = excluded.completed_at,
                  row_count = excluded.row_count,
                  latest_error = NULL,
                  updated_at = excluded.updated_at
                """,
                (normalized_user_id, LIBRARY_RULE_VERSION, started_at, rebuilt_at, len(rows), rebuilt_at),
            )
        return {
            "status": "complete",
            "rule_version": LIBRARY_RULE_VERSION,
            "row_count": len(rows),
            "started_at": started_at,
            "completed_at": rebuilt_at,
        }
    except Exception as exc:
        completed_at = _utc_now()
        _mark_rebuild_state(
            normalized_user_id,
            status="error",
            stale=True,
            completed_at=completed_at,
            latest_error=str(exc),
        )
        raise


def personal_library_status(user_id: str) -> dict[str, Any]:
    normalized_user_id = str(user_id or "").strip()
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        state = connection.execute(
            """
            SELECT *
            FROM personal_library_rebuild_state
            WHERE user_id = ?
            """,
            (normalized_user_id,),
        ).fetchone()
        count_rows = connection.execute(
            """
            SELECT strength, count(*) AS count
            FROM personal_library_track_cache
            WHERE user_id = ?
            GROUP BY strength
            """,
            (normalized_user_id,),
        ).fetchall()
    counts = {strength: 0 for strength in LIBRARY_STRENGTHS}
    for row in count_rows:
        counts[str(row["strength"])] = int(row["count"] or 0)
    total = sum(counts.values())
    if state is None:
        return {
            "status": "missing",
            "rule_version": LIBRARY_RULE_VERSION,
            "cache_rule_version": None,
            "stale": True,
            "row_count": total,
            "counts": counts,
            "started_at": None,
            "completed_at": None,
            "latest_error": None,
            "updated_at": None,
        }
    cache_rule_version = int(state["rule_version"] or 0)
    stale = bool(state["stale"]) or cache_rule_version != LIBRARY_RULE_VERSION
    return {
        "status": state["status"],
        "rule_version": LIBRARY_RULE_VERSION,
        "cache_rule_version": cache_rule_version,
        "stale": stale,
        "row_count": int(state["row_count"] or total),
        "counts": counts,
        "started_at": state["started_at"],
        "completed_at": state["completed_at"],
        "latest_error": state["latest_error"],
        "updated_at": state["updated_at"],
    }


def _library_row_to_item(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "kind": "track",
        "spotify_track_id": row["spotify_track_id"],
        "track_id": row["spotify_track_id"],
        "track_name": _display_track_name(row["track_name"], row["spotify_track_id"]),
        "artist_name": _normalize_artist_name_value(row["artist_name"]),
        "album_name": row["album_name"],
        "album_id": row["album_id"],
        "image_url": row["image_url"],
        "uri": row["uri"],
        "url": row["url"],
        "duration_ms": row["duration_ms"],
        "artists": _json_list(row["artists_json"]),
        "strength": row["strength"],
        "reasons": _json_list(row["reasons_json"]),
        "play_count": row["play_count"],
        "first_played_at": row["first_played_at"],
        "last_played_at": row["last_played_at"],
        "playlist_count": row["playlist_count"],
        "liked_at": row["liked_at"],
        "is_liked": bool(row["is_liked"]),
        "source_playlist_id": row["source_playlist_id"],
        "source_playlist_name": row["source_playlist_name"],
        "source_album_id": row["source_album_id"],
        "source_album_name": row["source_album_name"],
        "evidence_first_seen_at": row["evidence_first_seen_at"],
        "evidence_last_seen_at": row["evidence_last_seen_at"],
        "release_track_id": row["release_track_id"],
        "recording_representative_release_track_id": row["recording_representative_release_track_id"],
        "rebuilt_at": row["rebuilt_at"],
        "source_label": "library_cache",
    }


def _item_sort_timestamp(item: dict[str, Any]) -> str:
    return str(item.get("last_played_at") or item.get("evidence_last_seen_at") or item.get("liked_at") or "")


def _merge_reasons(left: list[Any], right: list[Any]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for value in [*left, *right]:
        if not isinstance(value, dict):
            continue
        key = str(value.get("reason") or value.get("label") or "").strip()
        if key:
            merged[key] = {"reason": key, "label": str(value.get("label") or key)}
    return [merged[key] for key in sorted(merged)]


FEATURE_PARENTHETICAL_RE = re.compile(r"\s*[\[(]\s*(?:feat\.?|featuring|ft\.?)\s+[^)\]]+[\])]\s*", re.IGNORECASE)


def _normalize_library_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _feature_stripped_title(value: Any) -> tuple[str, bool]:
    raw = _normalize_library_text(value)
    stripped = _normalize_library_text(FEATURE_PARENTHETICAL_RE.sub(" ", raw))
    return stripped, bool(stripped and stripped != raw)


def _library_artist_signature(item: dict[str, Any]) -> str:
    names: list[str] = []
    for artist in item.get("artists") or []:
        if not isinstance(artist, dict):
            continue
        name = _normalize_library_text(artist.get("name"))
        if name:
            names.append(name)
    if not names:
        names = [_normalize_library_text(part) for part in str(item.get("artist_name") or "").split(",")]
    return "|".join(dict.fromkeys(name for name in names if name))


def _feature_equivalence_seed(item: dict[str, Any]) -> tuple[str, bool] | None:
    title, stripped = _feature_stripped_title(item.get("track_name"))
    artist_signature = _library_artist_signature(item)
    if not title or title == "unknown track" or not artist_signature:
        return None
    return f"{artist_signature}::{title}", stripped


def _feature_equivalence_keys(items: list[dict[str, Any]]) -> set[str]:
    grouped: dict[str, list[bool]] = {}
    for item in items:
        if item.get("recording_representative_release_track_id") is not None:
            continue
        seed = _feature_equivalence_seed(item)
        if seed is None:
            continue
        key, stripped = seed
        grouped.setdefault(key, []).append(stripped)
    return {
        key
        for key, stripped_values in grouped.items()
        if len(stripped_values) > 1 and any(stripped_values)
    }


def _track_group_key(item: dict[str, Any], feature_equivalence_keys: set[str] | None = None) -> str:
    recording_id = item.get("recording_representative_release_track_id")
    if recording_id is not None:
        return f"recording:{recording_id}"
    seed = _feature_equivalence_seed(item)
    if seed is not None and feature_equivalence_keys and seed[0] in feature_equivalence_keys:
        return f"feature_equivalent:{seed[0]}"
    release_id = item.get("release_track_id")
    if release_id is not None:
        return f"release:{release_id}"
    return f"spotify:{item['spotify_track_id']}"


def _track_item_rank(item: dict[str, Any]) -> tuple[int, int, str, str]:
    return (
        STRENGTH_RANK.get(str(item.get("strength") or ""), 0),
        int(item.get("play_count") or 0),
        _item_sort_timestamp(item),
        str(item.get("track_name") or ""),
    )


def _version_entry(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "spotify_track_id": item.get("spotify_track_id"),
        "track_id": item.get("spotify_track_id"),
        "track_name": item.get("track_name"),
        "artist_name": item.get("artist_name"),
        "album_name": item.get("album_name"),
        "album_id": item.get("album_id"),
        "image_url": item.get("image_url"),
        "uri": item.get("uri"),
        "url": item.get("url"),
        "release_track_id": item.get("release_track_id"),
        "strength": item.get("strength"),
        "reasons": item.get("reasons") or [],
        "play_count": item.get("play_count") or 0,
        "last_played_at": item.get("last_played_at"),
    }


def _group_track_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    grouped_versions: dict[str, dict[int, dict[str, Any]]] = {}
    raw_versions: dict[str, list[dict[str, Any]]] = {}
    feature_equivalence_keys = _feature_equivalence_keys(items)
    for item in items:
        key = _track_group_key(item, feature_equivalence_keys)
        current = grouped.get(key)
        if current is None or _track_item_rank(item) > _track_item_rank(current):
            next_item = dict(item)
            next_item["library_group_key"] = key
            grouped[key] = next_item
        current_group = grouped[key]
        if STRENGTH_RANK.get(str(item.get("strength") or ""), 0) > STRENGTH_RANK.get(str(current_group.get("strength") or ""), 0):
            current_group["strength"] = item.get("strength")
        current_group["play_count"] = int(current_group.get("play_count") or 0) + int(item.get("play_count") or 0)
        current_group["playlist_count"] = int(current_group.get("playlist_count") or 0) + int(item.get("playlist_count") or 0)
        current_group["is_liked"] = bool(current_group.get("is_liked")) or bool(item.get("is_liked"))
        current_group["reasons"] = _merge_reasons(current_group.get("reasons") or [], item.get("reasons") or [])
        current_group["first_played_at"] = _merge_time_min(current_group.get("first_played_at"), item.get("first_played_at"))
        current_group["last_played_at"] = _merge_time_max(current_group.get("last_played_at"), item.get("last_played_at"))
        current_group["evidence_first_seen_at"] = _merge_time_min(current_group.get("evidence_first_seen_at"), item.get("evidence_first_seen_at"))
        current_group["evidence_last_seen_at"] = _merge_time_max(current_group.get("evidence_last_seen_at"), item.get("evidence_last_seen_at"))
        release_track_id = item.get("release_track_id")
        if isinstance(release_track_id, int):
            by_release = grouped_versions.setdefault(key, {})
            if release_track_id not in by_release or _track_item_rank(item) > _track_item_rank(by_release[release_track_id]):
                by_release[release_track_id] = item
        raw_versions.setdefault(key, []).append(item)
    for key, item in grouped.items():
        release_versions = grouped_versions.get(key, {})
        has_version_group = (
            (str(key).startswith("recording:") or str(key).startswith("feature_equivalent:"))
            and len(release_versions) > 1
        )
        item["version_count"] = len(release_versions) if has_version_group else 0
        item["versions"] = [
            _version_entry(version)
            for version in sorted(release_versions.values(), key=_track_item_rank, reverse=True)
        ] if has_version_group else []
        item["source_version_count"] = len(raw_versions.get(key, []))
    return list(grouped.values())


def _sort_items(items: list[dict[str, Any]], sort: str) -> list[dict[str, Any]]:
    if sort == "name":
        return sorted(items, key=lambda item: (str(item.get("track_name") or item.get("name") or "").lower(), str(item.get("artist_name") or "").lower()))
    if sort == "listen_count":
        return sorted(items, key=lambda item: (int(item.get("play_count") or 0), _item_sort_timestamp(item), str(item.get("track_name") or "")), reverse=True)
    if sort == "playlist_count":
        return sorted(items, key=lambda item: (int(item.get("playlist_count") or 0), str(item.get("track_name") or "")), reverse=True)
    name_sorted = sorted(items, key=lambda item: str(item.get("track_name") or item.get("name") or "").lower())
    return sorted(name_sorted, key=lambda item: (_item_sort_timestamp(item) != "", _item_sort_timestamp(item)), reverse=True)


def _library_items_from_rows(raw_items: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    if kind == "track":
        return _group_track_items(raw_items)
    if kind == "all":
        return [
            *_group_track_items(raw_items),
            *_aggregate_entity_items(raw_items, "artist"),
            *_aggregate_entity_items(raw_items, "album"),
            *_aggregate_entity_items(raw_items, "playlist"),
        ]
    return _aggregate_entity_items(raw_items, kind)


def _query_library_rows(
    user_id: str,
    *,
    strength: str,
    q: str,
    deep: bool = False,
) -> list[dict[str, Any]]:
    clauses = ["user_id = ?"]
    params: list[Any] = [user_id]
    if strength != "all":
        clauses.append("strength = ?")
        params.append(strength)
    if q and deep:
        clauses.append(
            """(
              (
                lower(COALESCE(track_name, '')) LIKE ?
                AND lower(COALESCE(track_name, '')) NOT IN (
                  lower(COALESCE(spotify_track_id, '')),
                  lower('spotify:track:' || COALESCE(spotify_track_id, '')),
                  'unknown track'
                )
              )
              OR lower(COALESCE(artist_name, '')) LIKE ?
              OR lower(COALESCE(album_name, '')) LIKE ?
              OR lower(COALESCE(source_playlist_name, '')) LIKE ?
            )"""
        )
        like = f"%{q}%"
        params.extend([like, like, like, like])
    where_sql = " AND ".join(clauses)
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            f"""
            SELECT *
            FROM personal_library_track_cache
            WHERE {where_sql}
            """,
            tuple(params),
        ).fetchall()
    return [_library_row_to_item(row) for row in rows]


def _library_item_matches_query(item: dict[str, Any], q: str) -> bool:
    normalized_query = str(q or "").strip().lower()
    if not normalized_query:
        return True
    kind = str(item.get("kind") or "track")
    if kind == "track":
        title = str(item.get("track_name") or "").strip()
        spotify_track_id = str(item.get("spotify_track_id") or "").strip()
        if not title or _is_identifier_title(title, spotify_track_id) or title.lower() == "unknown track":
            return False
        return normalized_query in title.lower()
    title = str(item.get("label") or item.get("name") or "").strip()
    if not title:
        return False
    return normalized_query in title.lower()


def _filter_library_items_for_query(items: list[dict[str, Any]], q: str, *, deep: bool) -> list[dict[str, Any]]:
    if deep or not str(q or "").strip():
        return items
    return [item for item in items if _library_item_matches_query(item, q)]


def _entity_strength(current: str | None, incoming: str | None) -> str:
    current_strength = _normalize_strength(current) or "ephemeral"
    incoming_strength = _normalize_strength(incoming) or "ephemeral"
    return incoming_strength if STRENGTH_RANK[incoming_strength] > STRENGTH_RANK[current_strength] else current_strength


def _first_artist_entry(item: dict[str, Any]) -> tuple[str, str | None, str | None, str | None]:
    for artist in item.get("artists") or []:
        if not isinstance(artist, dict):
            continue
        name = str(artist.get("name") or "").strip()
        if not name:
            continue
        artist_id = str(artist.get("id") or artist.get("artist_id") or "").strip() or None
        url = str(artist.get("url") or "").strip() or (f"https://open.spotify.com/artist/{artist_id}" if artist_id else None)
        image_url = str(artist.get("image_url") or "").strip() or None
        return name, artist_id, url, image_url
    name = str(item.get("artist_name") or "").split(",")[0].strip()
    return name, None, None, None


def _aggregate_entity_items(items: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for item in items:
        if kind == "album":
            entity_id = str(item.get("album_id") or "").strip()
            name = str(item.get("album_name") or "").strip()
            if not entity_id and not name:
                continue
            key = entity_id or f"name:{name.lower()}"
            current = grouped.setdefault(key, {
                "kind": "album",
                "album_id": entity_id or None,
                "entity_id": entity_id or None,
                "name": name or "Unknown album",
                "label": name or "Unknown album",
                "artist_name": item.get("artist_name"),
                "image_url": item.get("image_url"),
                "url": f"https://open.spotify.com/album/{entity_id}" if entity_id else "",
                "strength": item.get("strength") or "ephemeral",
                "reasons": [],
                "track_count": 0,
                "play_count": 0,
                "playlist_count": 0,
                "last_played_at": None,
                "evidence_last_seen_at": None,
            })
        elif kind == "playlist":
            entity_id = str(item.get("source_playlist_id") or "").strip()
            if not entity_id:
                continue
            key = entity_id
            name = str(item.get("source_playlist_name") or "").strip() or "Untitled playlist"
            current = grouped.setdefault(key, {
                "kind": "playlist",
                "playlist_id": entity_id,
                "entity_id": entity_id,
                "name": name,
                "label": name,
                "artist_name": None,
                "image_url": item.get("image_url"),
                "url": f"https://open.spotify.com/playlist/{entity_id}",
                "strength": item.get("strength") or "ephemeral",
                "reasons": [],
                "track_count": 0,
                "play_count": 0,
                "playlist_count": 0,
                "last_played_at": None,
                "evidence_last_seen_at": None,
            })
        else:
            artist_name, artist_id, artist_url, artist_image_url = _first_artist_entry(item)
            if not artist_name:
                continue
            key = artist_id or f"name:{artist_name.lower()}"
            current = grouped.setdefault(key, {
                "kind": "artist",
                "artist_id": artist_id,
                "entity_id": artist_id,
                "name": artist_name,
                "label": artist_name,
                "artist_name": artist_name,
                "image_url": artist_image_url or item.get("image_url"),
                "url": artist_url or "",
                "strength": item.get("strength") or "ephemeral",
                "reasons": [],
                "track_count": 0,
                "play_count": 0,
                "playlist_count": 0,
                "last_played_at": None,
                "evidence_last_seen_at": None,
            })
        current["strength"] = _entity_strength(current.get("strength"), item.get("strength"))
        current["reasons"] = _merge_reasons(current.get("reasons") or [], item.get("reasons") or [])
        current["track_count"] = int(current.get("track_count") or 0) + 1
        current["play_count"] = int(current.get("play_count") or 0) + int(item.get("play_count") or 0)
        current["playlist_count"] = int(current.get("playlist_count") or 0) + int(item.get("playlist_count") or 0)
        current["last_played_at"] = _merge_time_max(current.get("last_played_at"), item.get("last_played_at"))
        current["evidence_last_seen_at"] = _merge_time_max(current.get("evidence_last_seen_at"), item.get("evidence_last_seen_at"))
        current["image_url"] = _merge_text(current.get("image_url"), item.get("image_url"))
    return list(grouped.values())


def list_personal_library_tracks(
    user_id: str,
    *,
    strength: str = "all",
    q: str = "",
    sort: str = "recent",
    limit: int = 50,
    offset: int = 0,
    deep: bool = False,
) -> dict[str, Any]:
    normalized_user_id = str(user_id or "").strip()
    normalized_strength = str(strength or "all").strip().lower()
    if normalized_strength != "all" and normalized_strength not in STRENGTH_RANK:
        normalized_strength = "all"
    normalized_query = " ".join(str(q or "").split()).lower()
    normalized_sort = str(sort or "recent").strip().lower()
    if normalized_sort not in {"recent", "name", "listen_count", "playlist_count"}:
        normalized_sort = "recent"
    page_limit = max(1, min(int(limit or 50), 100))
    page_offset = max(0, int(offset or 0))
    grouped_items = _sort_items(_group_track_items(_query_library_rows(
        normalized_user_id,
        strength=normalized_strength,
        q=normalized_query,
        deep=deep,
    )), normalized_sort)
    grouped_items = _filter_library_items_for_query(grouped_items, normalized_query, deep=deep)
    total = len(grouped_items)
    page_items = grouped_items[page_offset:page_offset + page_limit]
    return {
        "kind": "track",
        "items": page_items,
        "limit": page_limit,
        "offset": page_offset,
        "total": total,
        "has_more": page_offset + len(page_items) < total,
        "status": personal_library_status(normalized_user_id),
    }


def list_personal_library_items(
    user_id: str,
    *,
    kind: str = "all",
    strength: str = "all",
    q: str = "",
    sort: str = "recent",
    limit: int = 50,
    offset: int = 0,
    deep: bool = False,
) -> dict[str, Any]:
    normalized_kind = str(kind or "track").strip().lower()
    if normalized_kind not in {"all", "track", "artist", "album", "playlist"}:
        normalized_kind = "all"
    if normalized_kind == "track":
        return list_personal_library_tracks(
            user_id,
            strength=strength,
            q=q,
            sort=sort,
            limit=limit,
            offset=offset,
            deep=deep,
        )
    normalized_user_id = str(user_id or "").strip()
    normalized_strength = str(strength or "all").strip().lower()
    if normalized_strength != "all" and normalized_strength not in STRENGTH_RANK:
        normalized_strength = "all"
    normalized_query = " ".join(str(q or "").split()).lower()
    normalized_sort = str(sort or "recent").strip().lower()
    if normalized_sort not in {"recent", "name", "listen_count", "playlist_count"}:
        normalized_sort = "recent"
    page_limit = max(1, min(int(limit or 50), 100))
    page_offset = max(0, int(offset or 0))
    raw_items = _query_library_rows(normalized_user_id, strength=normalized_strength, q=normalized_query, deep=deep)
    library_items = _filter_library_items_for_query(
        _sort_items(_library_items_from_rows(raw_items, normalized_kind), normalized_sort),
        normalized_query,
        deep=deep,
    )
    total = len(library_items)
    page_items = library_items[page_offset:page_offset + page_limit]
    return {
        "kind": normalized_kind,
        "items": page_items,
        "limit": page_limit,
        "offset": page_offset,
        "total": total,
        "has_more": page_offset + len(page_items) < total,
        "status": personal_library_status(normalized_user_id),
    }
