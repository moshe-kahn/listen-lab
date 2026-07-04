from __future__ import annotations

import sqlite3
from typing import Any

from backend.app.db import _normalize_fallback_artist_text, _stable_text_key, sqlite_connection
from backend.app.track_variant_policy import interpret_track_variant_title


def _normalize_match_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _title_match_keys(value: Any) -> set[str]:
    raw = str(value or "").strip()
    normalized = _normalize_match_text(raw)
    keys = {normalized} if normalized else set()
    if not raw:
        return keys
    interpretation = interpret_track_variant_title(raw)
    base = _normalize_match_text(interpretation.base_title_anchor)
    if base:
        keys.add(base)
    for candidate in (normalized, base):
        if candidate.endswith(")") and "(" in candidate:
            stripped = candidate[: candidate.rfind("(")].strip()
            if stripped:
                keys.add(stripped)
    for suffix in (" - main theme", " main theme"):
        if base.endswith(suffix):
            stripped = base[: -len(suffix)].strip()
            if stripped:
                keys.add(stripped)
    return keys


def _sqlite_like_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _artist_match_names_from_row(row: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    artists = row.get("artists")
    if isinstance(artists, list):
        for artist in artists:
            if isinstance(artist, dict):
                normalized = _normalize_match_text(artist.get("name"))
                if normalized:
                    names.add(normalized)
            elif isinstance(artist, str):
                normalized = _normalize_match_text(artist)
                if normalized:
                    names.add(normalized)
    artist_name = row.get("artist_name")
    if isinstance(artist_name, str):
        for part in artist_name.replace(";", ",").split(","):
            normalized = _normalize_match_text(part)
            if normalized:
                names.add(normalized)
    return names


def _fallback_recording_play_history_for_rows(rows: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    targets: dict[int, dict[str, Any]] = {}
    title_values: set[str] = set()
    for row in rows:
        release_track_id = row.get("release_track_id")
        if not isinstance(release_track_id, int):
            continue
        title_keys = _title_match_keys(row.get("name") or row.get("track_name") or row.get("release_track_name"))
        artist_names = _artist_match_names_from_row(row)
        if not title_keys or not artist_names:
            continue
        targets[release_track_id] = {
            "title_keys": title_keys,
            "artist_names": artist_names,
        }
        title_values.update(title_keys)
    if not targets or not title_values:
        return {}
    placeholders = ",".join("?" for _ in title_values)
    like_clauses = " OR ".join("lower(trim(fact.track_name_canonical)) LIKE ? ESCAPE '\\'" for _ in title_values)
    title_filter_sql = f"(lower(trim(fact.track_name_canonical)) IN ({placeholders})"
    if like_clauses:
        title_filter_sql += f" OR {like_clauses}"
    title_filter_sql += ")"
    sorted_title_values = tuple(sorted(title_values))
    like_values = tuple(f"{_sqlite_like_escape(value)}%" for value in sorted_title_values)
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        candidate_rows = connection.execute(
            f"""
            SELECT DISTINCT
              fact.spotify_track_id,
              fact.track_name_canonical,
              fact.artist_name_canonical,
              play_counts.play_count,
              play_counts.first_played_at,
              play_counts.last_played_at,
              mapped.release_track_id AS mapped_release_track_id
            FROM fact_play_event fact
            JOIN source_track_play_count_cache play_counts
              ON play_counts.spotify_track_id = fact.spotify_track_id
            LEFT JOIN source_track source
              ON source.source_name IN ('spotify', 'spotify_uri')
             AND (
               source.external_id = fact.spotify_track_id
               OR source.external_id = 'spotify:track:' || fact.spotify_track_id
             )
            LEFT JOIN source_track_map mapped
              ON mapped.source_track_id = source.id
             AND mapped.status = 'accepted'
            WHERE {title_filter_sql}
              AND fact.spotify_track_id IS NOT NULL
            """,
            (*sorted_title_values, *like_values),
        ).fetchall()
    fallback: dict[int, dict[str, Any]] = {}
    seen_track_ids_by_release_track_id: dict[int, set[str]] = {}
    for candidate in candidate_rows:
        title_keys = _title_match_keys(candidate["track_name_canonical"])
        artist_names = {
            _normalize_match_text(part)
            for part in str(candidate["artist_name_canonical"] or "").replace(";", ",").split(",")
        }
        artist_names.discard("")
        if not title_keys or not artist_names:
            continue
        spotify_track_id = str(candidate["spotify_track_id"] or "").strip()
        if not spotify_track_id:
            continue
        for release_track_id, target in targets.items():
            if not title_keys.intersection(target["title_keys"]) or not artist_names.intersection(target["artist_names"]):
                continue
            seen_track_ids = seen_track_ids_by_release_track_id.setdefault(release_track_id, set())
            if spotify_track_id in seen_track_ids:
                continue
            seen_track_ids.add(spotify_track_id)
            play_count = int(candidate["play_count"] or 0)
            current = fallback.setdefault(
                release_track_id,
                {
                    "play_count": 0,
                    "first_played_at": None,
                    "last_played_at": None,
                    "release_track_ids": set(),
                },
            )
            current["play_count"] += play_count
            first_played_at = candidate["first_played_at"]
            last_played_at = candidate["last_played_at"]
            if first_played_at and (current["first_played_at"] is None or str(first_played_at) < str(current["first_played_at"])):
                current["first_played_at"] = first_played_at
            if last_played_at and (current["last_played_at"] is None or str(last_played_at) > str(current["last_played_at"])):
                current["last_played_at"] = last_played_at
            mapped_release_track_id = candidate["mapped_release_track_id"]
            if isinstance(mapped_release_track_id, int):
                current["release_track_ids"].add(mapped_release_track_id)
    return {
        release_track_id: {
            "play_count": int(history["play_count"] or 0),
            "first_played_at": history["first_played_at"],
            "last_played_at": history["last_played_at"],
            "release_track_ids": sorted(history["release_track_ids"]),
        }
        for release_track_id, history in fallback.items()
        if int(history["play_count"] or 0) > 0
    }


def _fallback_text_play_history_for_unmatched_rows(
    rows: list[dict[str, Any]],
    *,
    track_id_key: str,
) -> dict[str, dict[str, Any]]:
    targets: dict[str, dict[str, Any]] = {}
    title_values: set[str] = set()
    for row in rows:
        if isinstance(row.get("release_track_id"), int):
            continue
        track_id = str(row.get(track_id_key) or "").strip()
        if not track_id:
            continue
        title_keys = _title_match_keys(row.get("name") or row.get("track_name") or row.get("release_track_name"))
        artist_names = _artist_match_names_from_row(row)
        if not title_keys or not artist_names:
            continue
        targets[track_id] = {
            "title_keys": title_keys,
            "artist_names": artist_names,
        }
        title_values.update(title_keys)
    if not targets or not title_values:
        return {}
    placeholders = ",".join("?" for _ in title_values)
    like_clauses = " OR ".join("lower(trim(fact.track_name_canonical)) LIKE ? ESCAPE '\\'" for _ in title_values)
    title_filter_sql = f"(lower(trim(fact.track_name_canonical)) IN ({placeholders})"
    if like_clauses:
        title_filter_sql += f" OR {like_clauses}"
    title_filter_sql += ")"
    sorted_title_values = tuple(sorted(title_values))
    like_values = tuple(f"{_sqlite_like_escape(value)}%" for value in sorted_title_values)
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        candidate_rows = connection.execute(
            f"""
            SELECT DISTINCT
              fact.spotify_track_id,
              fact.track_name_canonical,
              fact.artist_name_canonical,
              play_counts.play_count,
              play_counts.first_played_at,
              play_counts.last_played_at
            FROM fact_play_event fact
            JOIN source_track_play_count_cache play_counts
              ON play_counts.spotify_track_id = fact.spotify_track_id
            WHERE {title_filter_sql}
              AND fact.spotify_track_id IS NOT NULL
            """,
            (*sorted_title_values, *like_values),
        ).fetchall()
    fallback: dict[str, dict[str, Any]] = {}
    seen_track_ids_by_target: dict[str, set[str]] = {}
    for candidate in candidate_rows:
        title_keys = _title_match_keys(candidate["track_name_canonical"])
        artist_names = {
            _normalize_match_text(part)
            for part in str(candidate["artist_name_canonical"] or "").replace(";", ",").split(",")
        }
        artist_names.discard("")
        if not title_keys or not artist_names:
            continue
        spotify_track_id = str(candidate["spotify_track_id"] or "").strip()
        if not spotify_track_id:
            continue
        for target_track_id, target in targets.items():
            if not title_keys.intersection(target["title_keys"]) or not artist_names.intersection(target["artist_names"]):
                continue
            seen_track_ids = seen_track_ids_by_target.setdefault(target_track_id, set())
            if spotify_track_id in seen_track_ids:
                continue
            seen_track_ids.add(spotify_track_id)
            play_count = int(candidate["play_count"] or 0)
            current = fallback.setdefault(
                target_track_id,
                {
                    "play_count": 0,
                    "first_played_at": None,
                    "last_played_at": None,
                },
            )
            current["play_count"] += play_count
            first_played_at = candidate["first_played_at"]
            last_played_at = candidate["last_played_at"]
            if first_played_at and (current["first_played_at"] is None or str(first_played_at) < str(current["first_played_at"])):
                current["first_played_at"] = first_played_at
            if last_played_at and (current["last_played_at"] is None or str(last_played_at) > str(current["last_played_at"])):
                current["last_played_at"] = last_played_at
    return {
        target_track_id: {
            "play_count": int(history["play_count"] or 0),
            "first_played_at": history["first_played_at"],
            "last_played_at": history["last_played_at"],
        }
        for target_track_id, history in fallback.items()
        if int(history["play_count"] or 0) > 0
    }


def release_track_metadata_for_spotify_ids(
    spotify_track_ids: list[str],
    *,
    refresh_dirty_clusters: bool = True,
) -> dict[str, dict[str, Any]]:
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

    cluster_metadata_by_release_track_id = candidate_cluster_metadata_for_release_track_ids(
        release_track_ids,
        refresh_dirty=refresh_dirty_clusters,
    )
    recording_release_track_ids_by_release_track_id = recording_release_track_ids_for_release_track_ids(
        release_track_ids,
        refresh_dirty_clusters=refresh_dirty_clusters,
    )

    metadata: dict[str, dict[str, Any]] = {}
    for row in rows:
        spotify_track_id = str(row["spotify_track_id"] or "").strip()
        if not spotify_track_id or spotify_track_id in metadata:
            continue
        release_track_id = int(row["release_track_id"])
        source_track_count = int(row["source_track_count"] or 0)
        cluster_metadata = cluster_metadata_by_release_track_id.get(release_track_id)
        cluster_member_count = int(cluster_metadata["cluster_member_count"]) if cluster_metadata else 0
        cluster_candidate_type = str(cluster_metadata["cluster_candidate_type"] or "") if cluster_metadata else None
        cluster_relationship_kind = str(cluster_metadata["cluster_relationship_kind"] or "") if cluster_metadata else None
        metadata[spotify_track_id] = {
            "release_track_id": release_track_id,
            "release_track_name": str(row["release_track_name"] or ""),
            "release_track_source_count": max(source_track_count, cluster_member_count),
            "release_track_duplicate_source_count": source_track_count,
            "has_release_track_siblings": source_track_count > 1 or cluster_member_count > 1,
            "release_track_cluster_candidate_type": cluster_candidate_type,
            "release_track_cluster_relationship_kind": cluster_relationship_kind,
            "recording_release_track_ids": recording_release_track_ids_by_release_track_id.get(release_track_id, [release_track_id]),
        }
    return metadata


def release_track_play_history_for_release_track_ids(release_track_ids: list[int]) -> dict[int, dict[str, Any]]:
    normalized_ids = sorted({int(release_track_id) for release_track_id in release_track_ids if int(release_track_id) > 0})
    if not normalized_ids:
        return {}
    placeholders = ",".join("?" for _ in normalized_ids)
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            f"""
            WITH release_sources AS (
              SELECT DISTINCT
                stm.release_track_id,
                CASE
                  WHEN st.source_name = 'spotify_uri' THEN replace(st.external_id, 'spotify:track:', '')
                  ELSE st.external_id
                END AS spotify_track_id,
                CASE
                  WHEN st.source_name = 'spotify_uri' THEN st.external_id
                  ELSE 'spotify:track:' || st.external_id
                END AS spotify_track_uri
              FROM source_track_map stm
              JOIN source_track st
                ON st.id = stm.source_track_id
              WHERE stm.status = 'accepted'
                AND stm.release_track_id IN ({placeholders})
                AND st.source_name IN ('spotify', 'spotify_uri')
            )
            SELECT
              release_sources.release_track_id,
              sum(COALESCE(play_counts.play_count, 0)) AS play_count,
              min(play_counts.first_played_at) AS first_played_at,
              max(play_counts.last_played_at) AS last_played_at
            FROM release_sources
            JOIN source_track_play_count_cache play_counts
              ON play_counts.spotify_track_id = release_sources.spotify_track_id
            GROUP BY release_sources.release_track_id
            """,
            normalized_ids,
        ).fetchall()
    return {
        int(row["release_track_id"]): {
            "play_count": int(row["play_count"] or 0),
            "first_played_at": row["first_played_at"],
            "last_played_at": row["last_played_at"],
        }
        for row in rows
    }


def recording_play_history_for_release_track_ids(
    release_track_ids: list[int],
    *,
    refresh_dirty_clusters: bool = True,
) -> dict[int, dict[str, Any]]:
    normalized_ids = sorted({int(release_track_id) for release_track_id in release_track_ids if int(release_track_id) > 0})
    if not normalized_ids:
        return {}
    from backend.app.recording_track_candidates import candidate_cluster_metadata_for_release_track_ids

    candidate_cluster_metadata_for_release_track_ids(
        normalized_ids,
        refresh_dirty=refresh_dirty_clusters,
    )
    placeholders = ",".join("?" for _ in normalized_ids)
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            f"""
            WITH ranked_clusters AS (
              SELECT
                member.release_track_id AS target_release_track_id,
                cluster.id AS cluster_id,
                row_number() OVER (
                  PARTITION BY member.release_track_id
                  ORDER BY cluster.member_count DESC, cluster.confidence DESC, cluster.id ASC
                ) AS cluster_rank
              FROM generated_recording_track_cluster_member member
              JOIN generated_recording_track_cluster cluster
                ON cluster.id = member.cluster_id
              WHERE member.release_track_id IN ({placeholders})
                AND cluster.candidate_type = 'recording_track_candidate'
            ),
            recording_sources AS (
              SELECT DISTINCT
                ranked.target_release_track_id,
                CASE
                  WHEN source.source_name = 'spotify_uri' THEN replace(source.external_id, 'spotify:track:', '')
                  ELSE source.external_id
                END AS spotify_track_id
              FROM ranked_clusters ranked
              JOIN generated_recording_track_cluster_member cluster_member
                ON cluster_member.cluster_id = ranked.cluster_id
              JOIN source_track_map source_map
                ON source_map.release_track_id = cluster_member.release_track_id
               AND source_map.status = 'accepted'
              JOIN source_track source
                ON source.id = source_map.source_track_id
               AND source.source_name IN ('spotify', 'spotify_uri')
              WHERE ranked.cluster_rank = 1
            )
            SELECT
              recording_sources.target_release_track_id,
              sum(play_counts.play_count) AS play_count,
              min(play_counts.first_played_at) AS first_played_at,
              max(play_counts.last_played_at) AS last_played_at
            FROM recording_sources
            JOIN source_track_play_count_cache play_counts
              ON play_counts.spotify_track_id = recording_sources.spotify_track_id
            GROUP BY recording_sources.target_release_track_id
            """,
            normalized_ids,
        ).fetchall()
    return {
        int(row["target_release_track_id"]): {
            "play_count": int(row["play_count"] or 0),
            "first_played_at": row["first_played_at"],
            "last_played_at": row["last_played_at"],
        }
        for row in rows
    }


def recording_release_track_ids_for_release_track_ids(
    release_track_ids: list[int],
    *,
    refresh_dirty_clusters: bool = True,
) -> dict[int, list[int]]:
    normalized_ids = sorted({int(release_track_id) for release_track_id in release_track_ids if int(release_track_id) > 0})
    if not normalized_ids:
        return {}
    from backend.app.recording_track_candidates import candidate_cluster_metadata_for_release_track_ids

    candidate_cluster_metadata_for_release_track_ids(
        normalized_ids,
        refresh_dirty=refresh_dirty_clusters,
    )
    placeholders = ",".join("?" for _ in normalized_ids)
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            f"""
            WITH ranked_clusters AS (
              SELECT
                member.release_track_id AS target_release_track_id,
                cluster.id AS cluster_id,
                row_number() OVER (
                  PARTITION BY member.release_track_id
                  ORDER BY cluster.member_count DESC, cluster.confidence DESC, cluster.id ASC
                ) AS cluster_rank
              FROM generated_recording_track_cluster_member member
              JOIN generated_recording_track_cluster cluster
                ON cluster.id = member.cluster_id
              WHERE member.release_track_id IN ({placeholders})
                AND cluster.candidate_type = 'recording_track_candidate'
            )
            SELECT
              ranked.target_release_track_id,
              cluster_member.release_track_id AS recording_release_track_id
            FROM ranked_clusters ranked
            JOIN generated_recording_track_cluster_member cluster_member
              ON cluster_member.cluster_id = ranked.cluster_id
            WHERE ranked.cluster_rank = 1
            ORDER BY ranked.target_release_track_id ASC, cluster_member.release_track_id ASC
            """,
            normalized_ids,
        ).fetchall()
    result: dict[int, list[int]] = {}
    for row in rows:
        target_release_track_id = int(row["target_release_track_id"])
        result.setdefault(target_release_track_id, []).append(int(row["recording_release_track_id"]))
    return result


def play_history_for_spotify_ids(spotify_track_ids: list[str]) -> dict[str, dict[str, Any]]:
    normalized_ids = sorted({str(track_id or "").strip() for track_id in spotify_track_ids if str(track_id or "").strip()})
    if not normalized_ids:
        return {}
    placeholders = ",".join("?" for _ in normalized_ids)
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            f"""
            SELECT
              spotify_track_id,
              play_count,
              first_played_at,
              last_played_at
            FROM source_track_play_count_cache
            WHERE spotify_track_id IN ({placeholders})
            """,
            normalized_ids,
        ).fetchall()
    return {
        str(row["spotify_track_id"] or ""): {
            "play_count": int(row["play_count"] or 0),
            "first_played_at": row["first_played_at"],
            "last_played_at": row["last_played_at"],
        }
        for row in rows
        if str(row["spotify_track_id"] or "")
    }


def release_track_metadata_for_history_raw_keys(
    text_keys: list[str],
    *,
    refresh_dirty_clusters: bool = True,
) -> dict[str, dict[str, Any]]:
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

    cluster_metadata_by_release_track_id = candidate_cluster_metadata_for_release_track_ids(
        release_track_ids,
        refresh_dirty=refresh_dirty_clusters,
    )

    metadata: dict[str, dict[str, Any]] = {}
    for row in rows:
        history_raw_key = str(row["history_raw_key"] or "").strip()
        if not history_raw_key or history_raw_key in metadata:
            continue
        release_track_id = int(row["release_track_id"])
        source_track_count = int(row["source_track_count"] or 0)
        cluster_metadata = cluster_metadata_by_release_track_id.get(release_track_id)
        cluster_member_count = int(cluster_metadata["cluster_member_count"]) if cluster_metadata else 0
        cluster_candidate_type = str(cluster_metadata["cluster_candidate_type"] or "") if cluster_metadata else None
        cluster_relationship_kind = str(cluster_metadata["cluster_relationship_kind"] or "") if cluster_metadata else None
        metadata[history_raw_key] = {
            "release_track_id": release_track_id,
            "release_track_name": str(row["release_track_name"] or ""),
            "release_track_source_count": max(source_track_count, cluster_member_count),
            "release_track_duplicate_source_count": source_track_count,
            "has_release_track_siblings": source_track_count > 1 or cluster_member_count > 1,
            "release_track_cluster_candidate_type": cluster_candidate_type,
            "release_track_cluster_relationship_kind": cluster_relationship_kind,
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
    refresh_dirty_clusters: bool = True,
) -> list[dict[str, Any]]:
    metadata_by_track_id = release_track_metadata_for_spotify_ids(
        [
            str(item.get(track_id_key) or "").strip()
            for item in items
            if isinstance(item, dict)
        ],
        refresh_dirty_clusters=refresh_dirty_clusters,
    )
    metadata_by_history_raw_key = release_track_metadata_for_history_raw_keys(
        [
            history_raw_key
            for item in items
            if isinstance(item, dict)
            for history_raw_key in [_history_raw_key_for_item(item)]
            if history_raw_key is not None
        ],
        refresh_dirty_clusters=refresh_dirty_clusters,
    )
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
            row.setdefault("release_track_duplicate_source_count", 0)
            row.setdefault("has_release_track_siblings", False)
            row.setdefault("release_track_cluster_candidate_type", None)
            row.setdefault("release_track_cluster_relationship_kind", None)
        enriched.append(row)
    play_history_by_release_track_id = release_track_play_history_for_release_track_ids([
        int(row["release_track_id"])
        for row in enriched
        if isinstance(row.get("release_track_id"), int)
    ])
    recording_history_by_release_track_id = recording_play_history_for_release_track_ids(
        [
            int(row["release_track_id"])
            for row in enriched
            if isinstance(row.get("release_track_id"), int)
        ],
        refresh_dirty_clusters=refresh_dirty_clusters,
    )
    recording_release_track_ids_by_release_track_id = recording_release_track_ids_for_release_track_ids(
        [
            int(row["release_track_id"])
            for row in enriched
            if isinstance(row.get("release_track_id"), int)
        ],
        refresh_dirty_clusters=refresh_dirty_clusters,
    )
    fallback_recording_history_by_release_track_id = _fallback_recording_play_history_for_rows(enriched)
    fallback_text_history_by_track_id = _fallback_text_play_history_for_unmatched_rows(
        enriched,
        track_id_key=track_id_key,
    )
    play_history_by_track_id = play_history_for_spotify_ids([
        str(row.get(track_id_key) or "").strip()
        for row in enriched
        if isinstance(row, dict)
    ])
    for row in enriched:
        release_track_id = row.get("release_track_id")
        track_id = str(row.get(track_id_key) or "").strip()
        exact_play_history = play_history_by_track_id.get(track_id)
        if exact_play_history:
            row["source_play_count"] = exact_play_history["play_count"]
            row["source_first_played_at"] = exact_play_history["first_played_at"]
            row["source_last_played_at"] = exact_play_history["last_played_at"]
        else:
            row.setdefault("source_play_count", 0)
            row.setdefault("source_first_played_at", None)
            row.setdefault("source_last_played_at", None)
        play_history = (
            play_history_by_release_track_id.get(release_track_id)
            if isinstance(release_track_id, int)
            else None
        ) or exact_play_history or fallback_text_history_by_track_id.get(track_id)
        if play_history:
            row["play_count"] = play_history["play_count"]
            row["first_played_at"] = play_history["first_played_at"]
            row["last_played_at"] = play_history["last_played_at"]
        else:
            row.setdefault("play_count", 0)
            row.setdefault("first_played_at", None)
            row.setdefault("last_played_at", None)
        recording_history = (
            recording_history_by_release_track_id.get(release_track_id)
            if isinstance(release_track_id, int)
            else None
        ) or (
            fallback_recording_history_by_release_track_id.get(release_track_id)
            if isinstance(release_track_id, int)
            else None
        ) or play_history
        recording_release_track_ids = (
            recording_release_track_ids_by_release_track_id.get(release_track_id)
            if isinstance(release_track_id, int)
            else None
        )
        if recording_release_track_ids:
            row["recording_release_track_ids"] = recording_release_track_ids
        elif isinstance(release_track_id, int):
            fallback_release_track_ids = (
                fallback_recording_history_by_release_track_id.get(release_track_id, {}).get("release_track_ids", [])
                if isinstance(release_track_id, int)
                else []
            )
            row["recording_release_track_ids"] = sorted({release_track_id, *fallback_release_track_ids})
        else:
            row["recording_release_track_ids"] = []
        if recording_history:
            row["recording_play_count"] = recording_history["play_count"]
            row["recording_first_played_at"] = recording_history["first_played_at"]
            row["recording_last_played_at"] = recording_history["last_played_at"]
        else:
            row.setdefault("recording_play_count", 0)
            row.setdefault("recording_first_played_at", None)
            row.setdefault("recording_last_played_at", None)
    return enriched


def enrich_album_track_rows_with_release_metadata(
    items: list[dict[str, Any]],
    *,
    refresh_dirty_clusters: bool = True,
) -> list[dict[str, Any]]:
    return enrich_track_rows_with_release_metadata(
        items,
        track_id_key="id",
        refresh_dirty_clusters=refresh_dirty_clusters,
    )
