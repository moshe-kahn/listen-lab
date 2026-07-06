from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.app.db import sqlite_connection
from backend.app.spotify_http import _spotify_get
from backend.app.spotify_normalization import _normalize_track


PLAYLIST_TRACK_PAGE_LIMIT = 500
PLAYLIST_INDEX_STALE_SECONDS = 24 * 60 * 60
PLAYLIST_INDEX_DENIED_RETRY_SECONDS = 24 * 60 * 60


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


def _playlist_track_cache_row_to_item(row: sqlite3.Row) -> dict[str, Any]:
    item = {
        "track_id": row["spotify_track_id"],
        "track_name": row["track_name"],
        "artist_name": row["artist_name"],
        "album_name": row["album_name"],
        "duration_ms": row["duration_ms"],
        "duration_seconds": round(int(row["duration_ms"]) / 1000.0, 3) if row["duration_ms"] else None,
        "uri": row["uri"],
        "preview_url": row["preview_url"],
        "url": row["url"],
        "image_url": row["image_url"],
        "album_id": row["album_id"],
        "artists": _json_list(row["artists_json"]),
        "playlist_position": int(row["position"]),
        "playlist_added_at": row["added_at"],
        "playlist_added_by": {
            "user_id": row["added_by_user_id"],
            "id": row["added_by_user_id"],
            "display_name": row["added_by_display_name"],
            "uri": row["added_by_uri"],
            "url": row["added_by_url"],
        } if row["added_by_user_id"] or row["added_by_display_name"] else None,
        "source_track_id": row["source_track_id"],
        "release_track_id": row["release_track_id"],
        "recording_cluster_id": row["recording_cluster_id"],
        "recording_representative_release_track_id": row["representative_release_track_id"],
    }
    return item


def upsert_playlist_metadata(user_id: str, playlists: list[dict[str, Any]], *, cached_at: str | None = None) -> None:
    timestamp = cached_at or _utc_now()
    rows: list[tuple[Any, ...]] = []
    for playlist in playlists:
        playlist_id = str(playlist.get("playlist_id") or "").strip()
        if not playlist_id:
            continue
        rows.append(
            (
                str(user_id),
                playlist_id,
                playlist.get("name"),
                playlist.get("description"),
                playlist.get("owner_id"),
                playlist.get("owner_name"),
                1 if playlist.get("is_public") is True else 0 if playlist.get("is_public") is False else None,
                1 if playlist.get("is_collaborative") else 0,
                1 if playlist.get("is_owned") else 0,
                1 if playlist.get("owner_followed_by_you") else 0,
                playlist.get("playlist_category"),
                playlist.get("snapshot_id"),
                playlist.get("track_count"),
                playlist.get("followers_total"),
                playlist.get("url"),
                playlist.get("image_url"),
                timestamp,
                json.dumps(playlist, sort_keys=True),
            )
        )
    if not rows:
        return
    with sqlite_connection(write=True) as connection:
        connection.executemany(
            """
            INSERT INTO spotify_playlist_cache (
              user_id, playlist_id, name, description, owner_id, owner_name,
              is_public, is_collaborative, is_owned, owner_followed_by_you,
              playlist_category, snapshot_id, track_count, followers_total, url, image_url,
              metadata_cached_at, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, playlist_id) DO UPDATE SET
              name = excluded.name,
              description = excluded.description,
              owner_id = excluded.owner_id,
              owner_name = excluded.owner_name,
              is_public = excluded.is_public,
              is_collaborative = excluded.is_collaborative,
              is_owned = excluded.is_owned,
              owner_followed_by_you = excluded.owner_followed_by_you,
              playlist_category = excluded.playlist_category,
              tracks_cache_complete = CASE
                WHEN spotify_playlist_cache.snapshot_id IS NOT NULL
                 AND excluded.snapshot_id IS NOT NULL
                 AND spotify_playlist_cache.snapshot_id != excluded.snapshot_id THEN 0
                WHEN spotify_playlist_cache.track_count IS NOT NULL
                 AND excluded.track_count IS NOT NULL
                 AND spotify_playlist_cache.track_count != excluded.track_count THEN 0
                ELSE spotify_playlist_cache.tracks_cache_complete
              END,
              snapshot_id = excluded.snapshot_id,
              track_count = excluded.track_count,
              followers_total = COALESCE(excluded.followers_total, spotify_playlist_cache.followers_total),
              url = excluded.url,
              image_url = excluded.image_url,
              metadata_cached_at = excluded.metadata_cached_at,
              raw_json = excluded.raw_json
            """,
            rows,
        )


def cached_playlist_metadata_for_user(user_id: str) -> dict[str, dict[str, Any]]:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            """
            SELECT playlist_id, followers_total, hidden_by_user
            FROM spotify_playlist_cache
            WHERE user_id = ?
            """,
            (str(user_id),),
        ).fetchall()
    return {
        str(row["playlist_id"]): {
            "followers_total": row["followers_total"],
            "hidden_by_user": bool(row["hidden_by_user"]),
        }
        for row in rows
    }


def _looks_like_spotify_user_id(value: str | None) -> bool:
    text = str(value or "").strip()
    return len(text) >= 18 and text.isalnum() and any(char.isdigit() for char in text) and any(char.isupper() for char in text)


def playlist_contributor_summaries_for_user(user_id: str) -> dict[str, dict[str, Any]]:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            """
            SELECT
              pt.playlist_id,
              pt.added_by_user_id,
              pt.added_by_display_name,
              pc.owner_id,
              pc.owner_name,
              pc.is_owned,
              MIN(position) AS first_position,
              COUNT(*) AS track_count
            FROM spotify_playlist_track_cache pt
            JOIN spotify_playlist_cache pc
              ON pc.user_id = pt.user_id
             AND pc.playlist_id = pt.playlist_id
            WHERE pt.user_id = ?
              AND (
                pt.added_by_user_id IS NOT NULL
                OR pt.added_by_display_name IS NOT NULL
              )
            GROUP BY pt.playlist_id, pt.added_by_user_id, pt.added_by_display_name, pc.owner_id, pc.owner_name, pc.is_owned
            ORDER BY
              pt.playlist_id,
              CASE
                WHEN pc.is_owned = 1 AND pt.added_by_user_id = ? THEN 0
                WHEN pc.is_owned = 0 AND pc.owner_id IS NOT NULL AND pt.added_by_user_id = pc.owner_id THEN 0
                ELSE 1
              END,
              first_position
            """,
            (str(user_id), str(user_id)),
        ).fetchall()
    summaries: dict[str, dict[str, Any]] = {}
    for row in rows:
        playlist_id = str(row["playlist_id"] or "").strip()
        if not playlist_id:
            continue
        contributor_user_id = str(row["added_by_user_id"] or "").strip()
        display_name = str(row["added_by_display_name"] or "").strip()
        label = "You" if contributor_user_id == str(user_id) else display_name or contributor_user_id
        if label != "You" and _looks_like_spotify_user_id(label):
            label = "Unknown"
        owner_user_id = str(row["owner_id"] or "").strip()
        owner_display_name = str(row["owner_name"] or "").strip()
        owner_label = "You" if bool(row["is_owned"]) else owner_display_name or owner_user_id
        if owner_label != "You" and _looks_like_spotify_user_id(owner_label):
            owner_label = "Unknown"
        summary = summaries.setdefault(
            playlist_id,
            {
                "total": 0,
                "names": [],
                "track_counts": {},
                "owner_user_id": owner_user_id or None,
                "owner_display_name": owner_label or None,
            },
        )
        if label not in summary["names"]:
            summary["names"].append(label)
            summary["total"] += 1
        summary["track_counts"][label] = int(row["track_count"] or 0)
    return summaries


def set_playlist_hidden(user_id: str, playlist_id: str, hidden: bool) -> None:
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            UPDATE spotify_playlist_cache
            SET hidden_by_user = ?
            WHERE user_id = ?
              AND playlist_id = ?
            """,
            (1 if hidden else 0, str(user_id), str(playlist_id)),
        )


def hidden_playlist_ids_for_user(user_id: str) -> set[str]:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            """
            SELECT playlist_id
            FROM spotify_playlist_cache
            WHERE user_id = ?
              AND COALESCE(hidden_by_user, 0) = 1
            """,
            (str(user_id),),
        ).fetchall()
    return {str(row["playlist_id"]) for row in rows}


def playlist_categories_for_user(user_id: str) -> list[dict[str, Any]]:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            """
            SELECT
              c.id,
              c.name,
              COALESCE(
                json_group_array(m.playlist_id) FILTER (WHERE m.playlist_id IS NOT NULL),
                '[]'
              ) AS playlist_ids_json
            FROM playlist_category c
            LEFT JOIN playlist_category_member m
              ON m.user_id = c.user_id
             AND m.category_id = c.id
            WHERE c.user_id = ?
            GROUP BY c.id, c.name
            ORDER BY lower(c.name), c.id
            """,
            (str(user_id),),
        ).fetchall()
    categories: list[dict[str, Any]] = []
    for row in rows:
        try:
            playlist_ids = json.loads(row["playlist_ids_json"] or "[]")
        except (TypeError, ValueError):
            playlist_ids = []
        categories.append({
            "id": str(row["id"]),
            "name": str(row["name"]),
            "playlistIds": [str(value) for value in playlist_ids if str(value or "").strip()],
        })
    return categories


def create_playlist_category(user_id: str, name: str) -> dict[str, Any]:
    normalized_name = " ".join(str(name or "").split())
    if not normalized_name:
        raise ValueError("Category name is required.")
    now = _utc_now()
    with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
        connection.execute(
            """
            INSERT INTO playlist_category (user_id, name, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, name) DO UPDATE SET
              updated_at = excluded.updated_at
            """,
            (str(user_id), normalized_name, now, now),
        )
        row = connection.execute(
            """
            SELECT
              c.id,
              c.name,
              COALESCE(
                json_group_array(m.playlist_id) FILTER (WHERE m.playlist_id IS NOT NULL),
                '[]'
              ) AS playlist_ids_json
            FROM playlist_category c
            LEFT JOIN playlist_category_member m
              ON m.user_id = c.user_id
             AND m.category_id = c.id
            WHERE c.user_id = ?
              AND c.name = ?
            GROUP BY c.id, c.name
            """,
            (str(user_id), normalized_name),
        ).fetchone()
    if row is None:
        raise RuntimeError("Playlist category could not be created.")
    try:
        playlist_ids = json.loads(row["playlist_ids_json"] or "[]")
    except (TypeError, ValueError):
        playlist_ids = []
    return {
        "id": str(row["id"]),
        "name": str(row["name"]),
        "playlistIds": [str(value) for value in playlist_ids if str(value or "").strip()],
    }


def set_playlist_category_membership(user_id: str, category_id: str, playlist_id: str, included: bool) -> dict[str, Any]:
    normalized_category_id = str(category_id or "").strip()
    normalized_playlist_id = str(playlist_id or "").strip()
    if not normalized_category_id:
        raise ValueError("category_id is required.")
    if not normalized_playlist_id:
        raise ValueError("playlist_id is required.")
    try:
        parsed_category_id = int(normalized_category_id)
    except ValueError as exc:
        raise ValueError("category_id must be an integer.") from exc
    with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
        category = connection.execute(
            """
            SELECT id, name
            FROM playlist_category
            WHERE user_id = ?
              AND id = ?
            """,
            (str(user_id), parsed_category_id),
        ).fetchone()
        if category is None:
            raise LookupError("Playlist category was not found.")
        if included:
            connection.execute(
                """
                INSERT OR IGNORE INTO playlist_category_member (
                  user_id, category_id, playlist_id, created_at
                )
                VALUES (?, ?, ?, ?)
                """,
                (str(user_id), parsed_category_id, normalized_playlist_id, _utc_now()),
            )
        else:
            connection.execute(
                """
                DELETE FROM playlist_category_member
                WHERE user_id = ?
                  AND category_id = ?
                  AND playlist_id = ?
                """,
                (str(user_id), parsed_category_id, normalized_playlist_id),
            )
    return {
        "category_id": str(parsed_category_id),
        "playlist_id": normalized_playlist_id,
        "included": bool(included),
    }


def playlist_needs_track_sync(user_id: str, playlist: dict[str, Any]) -> bool:
    playlist_id = str(playlist.get("playlist_id") or "").strip()
    if not playlist_id:
        return False
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        row = connection.execute(
            """
            SELECT snapshot_id, track_count, tracks_cached_at, tracks_cache_complete,
                   last_sync_completed_at, last_sync_error
            FROM spotify_playlist_cache
            WHERE user_id = ?
              AND playlist_id = ?
            """,
            (str(user_id), playlist_id),
        ).fetchone()
    if row is None:
        return True
    last_error = str(row["last_sync_error"] or "")
    if last_error.startswith("403:"):
        if not row["last_sync_completed_at"]:
            return False
        try:
            last_denied_at = datetime.fromisoformat(str(row["last_sync_completed_at"]).replace("Z", "+00:00"))
        except ValueError:
            return False
        if datetime.now(UTC) - last_denied_at <= timedelta(seconds=PLAYLIST_INDEX_DENIED_RETRY_SECONDS):
            return False
    if row["tracks_cache_complete"] != 1:
        return True
    if playlist.get("snapshot_id") and row["snapshot_id"] and playlist.get("snapshot_id") != row["snapshot_id"]:
        return True
    if playlist.get("track_count") is not None and row["track_count"] is not None and int(playlist["track_count"]) != int(row["track_count"]):
        return True
    if not row["tracks_cached_at"]:
        return True
    try:
        cached_at = datetime.fromisoformat(str(row["tracks_cached_at"]).replace("Z", "+00:00"))
    except ValueError:
        return True
    return datetime.now(UTC) - cached_at > timedelta(seconds=PLAYLIST_INDEX_STALE_SECONDS)


def mark_playlist_sync_started(user_id: str, playlist_id: str) -> None:
    now = _utc_now()
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            UPDATE spotify_playlist_cache
            SET last_sync_started_at = ?,
                last_sync_error = NULL
            WHERE user_id = ?
              AND playlist_id = ?
            """,
            (now, str(user_id), playlist_id),
        )


def mark_playlist_sync_completed(user_id: str, playlist_id: str, *, complete: bool, error: str | None = None) -> None:
    now = _utc_now()
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            UPDATE spotify_playlist_cache
            SET tracks_cached_at = CASE WHEN ? THEN ? ELSE tracks_cached_at END,
                tracks_cache_complete = ?,
                last_sync_completed_at = ?,
                last_sync_error = ?
            WHERE user_id = ?
              AND playlist_id = ?
            """,
            (1 if complete and not error else 0, now, 1 if complete and not error else 0, now, error, str(user_id), playlist_id),
        )


def cache_playlist_track_page(
    user_id: str,
    playlist_id: str,
    tracks: list[dict[str, Any]],
    *,
    offset: int,
    total: int | None,
) -> None:
    now = _utc_now()
    rows: list[tuple[Any, ...]] = []
    for index, track in enumerate(tracks):
        position = offset + index
        spotify_track_id = str(track.get("track_id") or "").strip()
        if not spotify_track_id:
            continue
        added_by = track.get("playlist_added_by") if isinstance(track.get("playlist_added_by"), dict) else {}
        rows.append(
            (
                str(user_id),
                playlist_id,
                position,
                spotify_track_id,
                track.get("uri"),
                track.get("track_name"),
                track.get("artist_name"),
                track.get("album_name"),
                track.get("album_id"),
                track.get("duration_ms"),
                track.get("image_url"),
                track.get("preview_url"),
                track.get("url"),
                json.dumps(track.get("artists") or [], sort_keys=True),
                track.get("playlist_added_at"),
                added_by.get("user_id") or added_by.get("id"),
                added_by.get("display_name"),
                added_by.get("uri"),
                added_by.get("url"),
                now,
                json.dumps(track, sort_keys=True),
            )
        )
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO spotify_playlist_cache (
              user_id, playlist_id, metadata_cached_at
            )
            VALUES (?, ?, ?)
            """,
            (str(user_id), playlist_id, now),
        )
        if tracks:
            connection.execute(
                """
                DELETE FROM spotify_playlist_track_cache
                WHERE user_id = ?
                  AND playlist_id = ?
                  AND position >= ?
                  AND position < ?
                """,
                (str(user_id), playlist_id, offset, offset + len(tracks)),
            )
        if rows:
            connection.executemany(
                """
                INSERT INTO spotify_playlist_track_cache (
                  user_id, playlist_id, position, spotify_track_id, uri, track_name,
                  artist_name, album_name, album_id, duration_ms, image_url, preview_url,
                  url, artists_json, added_at, added_by_user_id, added_by_display_name,
                  added_by_uri, added_by_url, cached_at, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, playlist_id, position) DO UPDATE SET
                  spotify_track_id = excluded.spotify_track_id,
                  uri = excluded.uri,
                  track_name = excluded.track_name,
                  artist_name = excluded.artist_name,
                  album_name = excluded.album_name,
                  album_id = excluded.album_id,
                  duration_ms = excluded.duration_ms,
                  image_url = excluded.image_url,
                  preview_url = excluded.preview_url,
                  url = excluded.url,
                  artists_json = excluded.artists_json,
                  added_at = excluded.added_at,
                  added_by_user_id = excluded.added_by_user_id,
                  added_by_display_name = excluded.added_by_display_name,
                  added_by_uri = excluded.added_by_uri,
                  added_by_url = excluded.added_by_url,
                  cached_at = excluded.cached_at,
                  raw_json = excluded.raw_json
                """,
                rows,
            )
        if total is not None:
            connection.execute(
                """
                DELETE FROM spotify_playlist_track_cache
                WHERE user_id = ?
                  AND playlist_id = ?
                  AND position >= ?
                """,
                (str(user_id), playlist_id, int(total)),
            )
        connection.execute(
            """
            UPDATE spotify_playlist_cache
            SET track_count = COALESCE(?, track_count),
                tracks_cached_at = ?,
                tracks_cache_complete = CASE
                  WHEN ? IS NOT NULL
                   AND (
                    SELECT count(*)
                    FROM spotify_playlist_track_cache
                    WHERE user_id = ?
                      AND playlist_id = ?
                  ) >= ? THEN 1
                  ELSE tracks_cache_complete
                END
            WHERE user_id = ?
              AND playlist_id = ?
            """,
            (total, now, total, str(user_id), playlist_id, total or 0, str(user_id), playlist_id),
        )
    refresh_playlist_track_identity(user_id, playlist_id)


def refresh_playlist_track_identity(user_id: str, playlist_id: str) -> None:
    now = _utc_now()
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            DELETE FROM spotify_playlist_track_identity
            WHERE user_id = ?
              AND playlist_id = ?
            """,
            (str(user_id), playlist_id),
        )
        connection.execute(
            """
            INSERT INTO spotify_playlist_track_identity (
              user_id, playlist_id, position, spotify_track_id,
              source_track_id, release_track_id, recording_cluster_id,
              representative_release_track_id, match_status, matched_at
            )
            WITH ranked_identity AS (
              SELECT
                p.user_id,
                p.playlist_id,
                p.position,
                p.spotify_track_id,
                st.id AS source_track_id,
                stm.release_track_id,
                gcm.cluster_id AS recording_cluster_id,
                grtc.representative_release_track_id,
                CASE
                  WHEN stm.release_track_id IS NOT NULL THEN 'matched'
                  ELSE 'unmatched'
                END AS match_status,
                ROW_NUMBER() OVER (
                  PARTITION BY p.user_id, p.playlist_id, p.position
                  ORDER BY
                    CASE WHEN stm.release_track_id IS NOT NULL THEN 0 ELSE 1 END,
                    CASE WHEN gcm.cluster_id IS NOT NULL THEN 0 ELSE 1 END,
                    st.id,
                    stm.release_track_id,
                    gcm.cluster_id
                ) AS identity_rank
              FROM spotify_playlist_track_cache p
              LEFT JOIN source_track st
                ON st.source_name = 'spotify'
               AND st.external_id = p.spotify_track_id
              LEFT JOIN source_track_map stm
                ON stm.source_track_id = st.id
               AND stm.status = 'accepted'
              LEFT JOIN generated_recording_track_cluster_member gcm
                ON gcm.release_track_id = stm.release_track_id
              LEFT JOIN generated_recording_track_cluster grtc
                ON grtc.id = gcm.cluster_id
              WHERE p.user_id = ?
                AND p.playlist_id = ?
            )
            SELECT
              user_id,
              playlist_id,
              position,
              spotify_track_id,
              source_track_id,
              release_track_id,
              recording_cluster_id,
              representative_release_track_id,
              match_status,
              ? AS matched_at
            FROM ranked_identity
            WHERE identity_rank = 1
            """,
            (str(user_id), playlist_id, now),
        )


def cached_playlist_tracks(user_id: str, playlist_id: str, *, limit: int, offset: int) -> dict[str, Any] | None:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        playlist = connection.execute(
            """
            SELECT track_count, tracks_cache_complete
            FROM spotify_playlist_cache
            WHERE user_id = ?
              AND playlist_id = ?
            """,
            (str(user_id), playlist_id),
        ).fetchone()
        if playlist is None:
            return None
        total_rows = connection.execute(
            """
            SELECT count(*) AS count
            FROM spotify_playlist_track_cache
            WHERE user_id = ?
              AND playlist_id = ?
            """,
            (str(user_id), playlist_id),
        ).fetchone()
        cached_total = int(total_rows["count"] or 0) if total_rows else 0
        if cached_total <= 0:
            return None
        rows = connection.execute(
            """
            SELECT p.*, i.source_track_id, i.release_track_id, i.recording_cluster_id,
                   i.representative_release_track_id
            FROM spotify_playlist_track_cache p
            LEFT JOIN spotify_playlist_track_identity i
              ON i.user_id = p.user_id
             AND i.playlist_id = p.playlist_id
             AND i.position = p.position
            WHERE p.user_id = ?
              AND p.playlist_id = ?
              AND p.position >= ?
            ORDER BY p.position
            LIMIT ?
            """,
            (str(user_id), playlist_id, offset, limit),
        ).fetchall()
    total = int(playlist["track_count"] or cached_total)
    if not rows and offset >= cached_total:
        return None
    items = [_playlist_track_cache_row_to_item(row) for row in rows]
    next_offset = offset + len(items)
    return {
        "items": items,
        "total": total,
        "offset": offset,
        "next_offset": next_offset,
        "has_more": next_offset < total,
        "complete": bool(playlist["tracks_cache_complete"]),
    }


def playlist_memberships_for_track(
    user_id: str,
    *,
    spotify_track_id: str | None,
    release_track_ids: list[int] | None = None,
    include_recording_cluster: bool = True,
) -> list[dict[str, Any]]:
    release_ids = sorted({int(value) for value in (release_track_ids or []) if int(value) > 0})
    params: list[Any] = [str(user_id)]
    conditions: list[str] = []
    if spotify_track_id:
        conditions.append("p.spotify_track_id = ?")
        params.append(spotify_track_id)
    if release_ids:
        placeholders = ",".join("?" for _ in release_ids)
        conditions.append(f"i.release_track_id IN ({placeholders})")
        params.extend(release_ids)
        if include_recording_cluster:
            conditions.append(
                f"""
                i.recording_cluster_id IN (
                  SELECT cluster_id
                  FROM generated_recording_track_cluster_member
                  WHERE release_track_id IN ({placeholders})
                )
                """
            )
            params.extend(release_ids)
    if not conditions:
        return []
    where_sql = " OR ".join(f"({condition})" for condition in conditions)
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            f"""
            SELECT
              p.playlist_id,
              pc.name AS playlist_name,
              pc.url AS playlist_url,
              pc.image_url AS playlist_image_url,
              pc.owner_name,
              pc.owner_id,
              pc.is_collaborative,
              pc.is_owned,
              p.position,
              p.spotify_track_id,
              p.track_name,
              p.artist_name,
              p.added_at,
              i.release_track_id,
              i.recording_cluster_id,
              i.representative_release_track_id
            FROM spotify_playlist_track_cache p
            JOIN spotify_playlist_cache pc
              ON pc.user_id = p.user_id
             AND pc.playlist_id = p.playlist_id
            LEFT JOIN spotify_playlist_track_identity i
              ON i.user_id = p.user_id
             AND i.playlist_id = p.playlist_id
             AND i.position = p.position
            WHERE p.user_id = ?
              AND ({where_sql})
              AND COALESCE(pc.hidden_by_user, 0) = 0
            ORDER BY pc.name COLLATE NOCASE, p.position
            LIMIT 100
            """,
            tuple(params),
        ).fetchall()
    return [
        {
            "playlist_id": row["playlist_id"],
            "playlist_name": row["playlist_name"],
            "playlist_url": row["playlist_url"],
            "playlist_image_url": row["playlist_image_url"],
            "owner_name": row["owner_name"],
            "owner_id": row["owner_id"],
            "is_collaborative": bool(row["is_collaborative"]),
            "is_owned": bool(row["is_owned"]),
            "position": int(row["position"]),
            "spotify_track_id": row["spotify_track_id"],
            "track_name": row["track_name"],
            "artist_name": row["artist_name"],
            "added_at": row["added_at"],
            "release_track_id": row["release_track_id"],
            "recording_cluster_id": row["recording_cluster_id"],
            "representative_release_track_id": row["representative_release_track_id"],
        }
        for row in rows
    ]


def enrich_rows_with_playlist_membership_counts(user_id: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return rows
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        for row in rows:
            track_id = str(row.get("id") or row.get("track_id") or row.get("spotify_track_id") or "").strip()
            release_track_id = row.get("release_track_id")
            exact_release_ids = [int(release_track_id)] if isinstance(release_track_id, int) and release_track_id > 0 else []
            recording_release_ids = [
                int(value)
                for value in (row.get("recording_release_track_ids") or exact_release_ids)
                if isinstance(value, int) and value > 0
            ]
            if exact_release_ids:
                recording_release_ids = sorted({*recording_release_ids, *exact_release_ids})

            source_conditions: list[str] = []
            source_params: list[Any] = [str(user_id)]
            if track_id:
                source_conditions.append("p.spotify_track_id = ?")
                source_params.append(track_id)
            if exact_release_ids:
                source_conditions.append("i.release_track_id = ?")
                source_params.append(exact_release_ids[0])
            source_count = 0
            if source_conditions:
                source_where = " OR ".join(f"({condition})" for condition in source_conditions)
                source_row = connection.execute(
                    f"""
                    SELECT count(DISTINCT p.playlist_id) AS playlist_count
                    FROM spotify_playlist_track_cache p
                    JOIN spotify_playlist_cache pc
                      ON pc.user_id = p.user_id
                     AND pc.playlist_id = p.playlist_id
                    LEFT JOIN spotify_playlist_track_identity i
                      ON i.user_id = p.user_id
                     AND i.playlist_id = p.playlist_id
                     AND i.position = p.position
                    WHERE p.user_id = ?
                      AND COALESCE(pc.hidden_by_user, 0) = 0
                      AND ({source_where})
                    """,
                    tuple(source_params),
                ).fetchone()
                source_count = int(source_row["playlist_count"] or 0) if source_row else 0

            recording_conditions: list[str] = []
            recording_params: list[Any] = [str(user_id)]
            if track_id:
                recording_conditions.append("p.spotify_track_id = ?")
                recording_params.append(track_id)
            if recording_release_ids:
                placeholders = ",".join("?" for _ in recording_release_ids)
                recording_conditions.append(f"i.release_track_id IN ({placeholders})")
                recording_params.extend(recording_release_ids)
            recording_count = source_count
            if recording_conditions:
                recording_where = " OR ".join(f"({condition})" for condition in recording_conditions)
                recording_row = connection.execute(
                    f"""
                    SELECT count(DISTINCT p.playlist_id) AS playlist_count
                    FROM spotify_playlist_track_cache p
                    JOIN spotify_playlist_cache pc
                      ON pc.user_id = p.user_id
                     AND pc.playlist_id = p.playlist_id
                    LEFT JOIN spotify_playlist_track_identity i
                      ON i.user_id = p.user_id
                     AND i.playlist_id = p.playlist_id
                     AND i.position = p.position
                    WHERE p.user_id = ?
                      AND COALESCE(pc.hidden_by_user, 0) = 0
                      AND ({recording_where})
                    """,
                    tuple(recording_params),
                ).fetchone()
                recording_count = int(recording_row["playlist_count"] or 0) if recording_row else 0

            row["source_playlist_count"] = source_count
            row["recording_playlist_count"] = recording_count
            row["playlist_count"] = recording_count
    return rows


def playlist_index_status_for_user(user_id: str) -> dict[str, Any]:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        playlist_row = connection.execute(
            """
            SELECT
              count(*) AS playlist_count,
              sum(CASE WHEN tracks_cache_complete = 1 THEN 1 ELSE 0 END) AS complete_playlist_count
            FROM spotify_playlist_cache
            WHERE user_id = ?
            """,
            (str(user_id),),
        ).fetchone()
        track_row = connection.execute(
            """
            SELECT count(*) AS track_count
            FROM spotify_playlist_track_cache
            WHERE user_id = ?
              AND playlist_id IN (
                SELECT playlist_id
                FROM spotify_playlist_cache
                WHERE user_id = ?
                  AND COALESCE(hidden_by_user, 0) = 0
              )
            """,
            (str(user_id), str(user_id)),
        ).fetchone()
        identity_row = connection.execute(
            """
            SELECT count(*) AS identity_count
            FROM spotify_playlist_track_identity
            WHERE user_id = ?
              AND playlist_id IN (
                SELECT playlist_id
                FROM spotify_playlist_cache
                WHERE user_id = ?
                  AND COALESCE(hidden_by_user, 0) = 0
              )
            """,
            (str(user_id), str(user_id)),
        ).fetchone()
    playlist_count = int(playlist_row["playlist_count"] or 0) if playlist_row else 0
    complete_playlist_count = int(playlist_row["complete_playlist_count"] or 0) if playlist_row else 0
    track_count = int(track_row["track_count"] or 0) if track_row else 0
    identity_count = int(identity_row["identity_count"] or 0) if identity_row else 0
    return {
        "playlist_count": playlist_count,
        "complete_playlist_count": complete_playlist_count,
        "track_count": track_count,
        "identity_count": identity_count,
        "has_playlist_metadata": playlist_count > 0,
        "has_track_cache": track_count > 0,
        "has_identity_index": identity_count > 0,
        "complete": playlist_count > 0 and complete_playlist_count >= playlist_count,
    }


async def fetch_playlist_track_page_from_spotify(
    access_token: str,
    playlist_id: str,
    *,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    current_offset = offset
    page_limit = min(100, max(1, limit))
    total: int | None = None
    while len(items) < limit:
        payload = await _spotify_get(
            access_token,
            f"https://api.spotify.com/v1/playlists/{playlist_id}/items",
            {
                "limit": min(page_limit, limit - len(items)),
                "offset": current_offset,
                "fields": "items(added_at,added_by(id,display_name,uri,external_urls),item(id,name,uri,duration_ms,preview_url,external_urls,album(id,name,release_date,images,external_urls),artists(id,name))),total,next",
            },
        )
        if total is None and isinstance(payload.get("total"), int):
            total = int(payload["total"])
        page_items = payload.get("items") or []
        if not page_items:
            break
        for item in page_items:
            if not isinstance(item, dict):
                continue
            track = item.get("item") or item.get("track") or {}
            if not isinstance(track, dict) or not track.get("id"):
                continue
            normalized = _normalize_track(track)
            normalized["playlist_added_at"] = item.get("added_at")
            added_by = item.get("added_by")
            if isinstance(added_by, dict):
                added_by_urls = added_by.get("external_urls")
                normalized["playlist_added_by"] = {
                    "user_id": added_by.get("id"),
                    "id": added_by.get("id"),
                    "display_name": added_by.get("display_name"),
                    "uri": added_by.get("uri"),
                    "url": added_by_urls.get("spotify") if isinstance(added_by_urls, dict) else None,
                }
            else:
                normalized["playlist_added_by"] = None
            items.append(normalized)
            if len(items) >= limit:
                break
        current_offset += len(page_items)
        if not payload.get("next"):
            break
    return {
        "items": items,
        "total": total if total is not None else len(items),
        "offset": offset,
        "next_offset": current_offset,
        "has_more": total is not None and current_offset < total,
    }
