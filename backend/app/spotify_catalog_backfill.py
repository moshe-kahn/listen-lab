from __future__ import annotations

import json
import re
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

import httpx

from backend.app.db import sqlite_connection
from backend.app.track_identity_audit import track_identity_readiness_source_ctes


TRACK_BATCH_SIZE = 50
ALBUM_BATCH_SIZE = 20
ALBUM_TRACK_PAGE_SIZE = 50
MIN_REQUEST_DELAY_SECONDS = 0.20
MAX_REQUEST_DELAY_SECONDS = 5.0
DEFAULT_REQUEST_DELAY_SECONDS = 2.0
MAX_LIMIT = 1000
DEFAULT_LIMIT = 200
DEFAULT_MAX_RUNTIME_SECONDS = 60
MAX_RUNTIME_SECONDS = 900
DEFAULT_MAX_REQUESTS = 150
DEFAULT_MAX_ERRORS = 10
DEFAULT_MAX_ALBUM_TRACKS_PAGES_PER_ALBUM = 10
DEFAULT_MAX_429 = 1
FIRST_429_STOP_WARNING = "Stopped after first Spotify 429; cooldown recommended"
ALBUM_TRACKLIST_POLICIES = {"all", "priority_only", "relevant_albums", "none"}
CATALOG_BACKFILL_RUN_MODES = {"metadata_only", "tracklists_relevant", "full_catalog"}
CATALOG_BACKFILL_REASONS = {
    "identity_metadata",
    "manual_priority",
    "tracklist_completion",
    "full_backfill",
    "resolution_evidence",
}
CATALOG_BACKFILL_TARGETS = {"tracks", "albums", "album_tracklists", "all"}
TRACK_METADATA_PRIORITY_SCOPES = {"identity_and_top_listened", "all"}
DEFAULT_TRACK_METADATA_PRIORITY_SCOPE = "identity_and_top_listened"
METADATA_ONLY_QUEUE_REASONS = {"identity_metadata"}
TRACK_BATCH_FORBIDDEN_WARNING = "Spotify batch track endpoint unavailable/forbidden; using single-track fallback"
ALBUM_BATCH_FORBIDDEN_WARNING = "Spotify batch album endpoint unavailable/forbidden; using single-album fallback"
TOP_TRACK_PRIORITY_LIMIT = 500
TOP_ALBUM_PRIORITY_LIMIT = 100
TOP_ARTIST_PRIORITY_LIMIT = 100
RECENT_TRACK_REPEAT_DAYS = 90
RECENT_TRACK_REPEAT_MIN_PLAYS = 3


class _PartialStop(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _json_object(value: Any) -> dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_list(value: Any) -> list[Any]:
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _warnings_from_json_text(value: Any) -> list[str]:
    if value is None:
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if str(item).strip()]


def _to_int_bool(value: Any) -> int | None:
    if isinstance(value, bool):
        return 1 if value else 0
    return None


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _normalize_delay_seconds(raw_delay: float) -> float:
    return max(MIN_REQUEST_DELAY_SECONDS, min(float(raw_delay), MAX_REQUEST_DELAY_SECONDS))


def _normalize_identity_text(text: Any) -> str:
    raw = str(text or "").strip().lower()
    if not raw:
        return ""
    # Strip simple punctuation while preserving alphanumeric and spacing semantics.
    stripped = re.sub(r"[^\w\s]", " ", raw)
    return " ".join(stripped.split())


def _primary_artist_key(artist_name: Any) -> str:
    raw = str(artist_name or "")
    parts = [part.strip() for part in raw.split(",")]
    normalized_seen: set[str] = set()
    normalized_parts: list[str] = []
    for part in parts:
        normalized = _normalize_identity_text(part)
        if not normalized:
            continue
        if normalized in normalized_seen:
            continue
        normalized_seen.add(normalized)
        normalized_parts.append(normalized)
    if not normalized_parts:
        return ""
    return normalized_parts[0]


_TRACK_VERSION_REVIEW_PATTERN = re.compile(
    r"\b("
    r"live|remaster(?:ed)?|remix|mix|edit|demo|acoustic|instrumental|karaoke|"
    r"clean|explicit|version|alternate|bonus|deluxe|anniversary|mono|stereo|"
    r"session|rework|radio|video"
    r")\b",
    re.IGNORECASE,
)


def _source_release_confirmation_preview(group: dict[str, Any]) -> dict[str, Any]:
    sources = list(group.get("sources") or [])
    reasons: list[str] = []
    evidence: dict[str, Any] = {"source_count": len(sources)}

    incomplete_sources = [
        source.get("external_id") or source.get("source_track_id")
        for source in sources
        if not bool(source.get("metadata_complete"))
    ]
    if len(sources) < 2:
        reasons.append("At least two source rows are required for source-to-release confirmation.")
    if incomplete_sources:
        reasons.append("One or more source rows are missing required source-track metadata.")
        evidence["incomplete_sources"] = incomplete_sources

    normalized_album_names = sorted({
        _normalize_identity_text(source.get("album_name_display") or source.get("album_name"))
        for source in sources
        if _normalize_identity_text(source.get("album_name_display") or source.get("album_name"))
    })
    positions = sorted({
        f"{source.get('disc_number')}.{source.get('track_number')}"
        for source in sources
        if source.get("disc_number") is not None and source.get("track_number") is not None
    })
    normalized_track_names = sorted({
        _normalize_identity_text(source.get("spotify_track_name") or source.get("source_name_raw"))
        for source in sources
        if _normalize_identity_text(source.get("spotify_track_name") or source.get("source_name_raw"))
    })
    durations = [
        int(source["duration_ms"])
        for source in sources
        if source.get("duration_ms") is not None
    ]
    isrc_values = sorted({
        str(source.get("isrc") or "").strip().upper()
        for source in sources
        if str(source.get("isrc") or "").strip()
    })
    version_flag_sources = [
        {
            "spotify_track_id": source.get("external_id"),
            "track_name": source.get("spotify_track_name") or source.get("source_name_raw"),
        }
        for source in sources
        if _TRACK_VERSION_REVIEW_PATTERN.search(str(source.get("spotify_track_name") or source.get("source_name_raw") or ""))
    ]

    evidence.update(
        {
            "normalized_album_names": normalized_album_names,
            "positions": positions,
            "normalized_track_names": normalized_track_names,
            "duration_delta_ms": (max(durations) - min(durations)) if len(durations) >= 2 else 0,
            "isrc_values": isrc_values,
            "version_flag_sources": version_flag_sources,
        }
    )

    if len(normalized_album_names) != 1:
        reasons.append("Source rows do not all share the same normalized album name.")
    if len(positions) != 1:
        reasons.append("Source rows do not all share the same disc and track position.")
    if len(normalized_track_names) != 1:
        reasons.append("Source rows do not all share the same normalized track name.")
    if evidence["duration_delta_ms"] > 2_000:
        reasons.append("Source row durations differ by more than two seconds.")
    if len(isrc_values) > 1:
        reasons.append("Source rows have conflicting ISRC values.")
    if version_flag_sources:
        reasons.append("One or more source track names contain version-like wording that needs review.")

    unsafe_reasons = {
        "At least two source rows are required for source-to-release confirmation.",
        "One or more source rows are missing required source-track metadata.",
        "Source rows do not all share the same normalized album name.",
        "Source rows do not all share the same disc and track position.",
    }
    if any(reason in unsafe_reasons for reason in reasons):
        readiness = "unsafe"
    elif reasons:
        readiness = "needs_review"
    else:
        readiness = "safe_candidate"

    return {
        "readiness": readiness,
        "action": "read_only_preview",
        "reasons": reasons or ["All complete source rows share album name, track position, title, duration, and ISRC evidence."],
        "evidence": evidence,
    }


def _duration_display(duration_ms: Any) -> str | None:
    if duration_ms is None:
        return None
    try:
        total_ms = int(duration_ms)
    except (TypeError, ValueError):
        return None
    if total_ms < 0:
        return None
    total_seconds = total_ms // 1000
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes}:{seconds:02d}"


def _known_track_ids(*, limit: int, offset: int) -> tuple[list[str], bool]:
    bounded_limit = max(1, min(int(limit), MAX_LIMIT))
    bounded_offset = max(0, int(offset))

    with sqlite_connection() as connection:
        rows = connection.execute(
            """
            WITH raw_track_listens AS (
              SELECT
                spotify_track_id,
                count(*) AS listen_count
              FROM raw_play_event
              WHERE spotify_track_id IS NOT NULL
                AND spotify_track_id != ''
              GROUP BY spotify_track_id
            ),
            mapped_release_track_candidates AS (
              SELECT
                st.external_id AS spotify_track_id,
                stm.release_track_id,
                COALESCE(rtl.listen_count, 0) AS listen_count,
                st.id AS source_track_row_id,
                stm.id AS source_track_map_row_id
              FROM source_track st
              JOIN source_track_map stm
                ON stm.source_track_id = st.id
              LEFT JOIN raw_track_listens rtl
                ON rtl.spotify_track_id = st.external_id
              WHERE st.source_name = 'spotify'
                AND st.external_id IS NOT NULL
                AND st.external_id != ''
                AND stm.status = 'accepted'
            ),
            mapped_release_track_ids AS (
              SELECT spotify_track_id
              FROM (
                SELECT
                  spotify_track_id,
                  row_number() OVER (
                    PARTITION BY release_track_id
                    ORDER BY
                      listen_count DESC,
                      spotify_track_id ASC,
                      source_track_map_row_id ASC,
                      source_track_row_id ASC
                  ) AS rn
                FROM mapped_release_track_candidates
              )
              WHERE rn = 1
            ),
            unmapped_source_track_ids AS (
              SELECT st.external_id AS spotify_track_id
              FROM source_track st
              LEFT JOIN source_track_map stm
                ON stm.source_track_id = st.id
              WHERE st.source_name = 'spotify'
                AND st.external_id IS NOT NULL
                AND st.external_id != ''
                AND stm.id IS NULL
            ),
            raw_track_ids AS (
              SELECT spotify_track_id AS spotify_track_id
              FROM raw_spotify_recent
              WHERE spotify_track_id IS NOT NULL AND spotify_track_id != ''
              UNION
              SELECT spotify_track_id AS spotify_track_id
              FROM raw_spotify_history
              WHERE spotify_track_id IS NOT NULL AND spotify_track_id != ''
            ),
            unmapped_raw_track_ids AS (
              SELECT raw.spotify_track_id
              FROM raw_track_ids raw
              LEFT JOIN source_track st
                ON st.source_name = 'spotify'
               AND st.external_id = raw.spotify_track_id
              LEFT JOIN source_track_map stm
                ON stm.source_track_id = st.id
              WHERE stm.id IS NULL
            ),
            known_ids AS (
              SELECT spotify_track_id FROM mapped_release_track_ids
              UNION
              SELECT spotify_track_id FROM unmapped_source_track_ids
              UNION
              SELECT spotify_track_id FROM unmapped_raw_track_ids
            )
            SELECT spotify_track_id
            FROM known_ids
            ORDER BY spotify_track_id ASC
            LIMIT ?
            OFFSET ?
            """,
            (bounded_limit + 1, bounded_offset),
        ).fetchall()

    ids = [str(row[0]) for row in rows if row and row[0]]
    has_more = len(ids) > bounded_limit
    return ids[:bounded_limit], has_more


def _known_track_ids_missing_metadata(*, limit: int, offset: int) -> tuple[list[str], bool]:
    return _known_track_ids_missing_metadata_for_scope(limit=limit, offset=offset, priority_scope="all")


def _track_metadata_priority_ctes() -> str:
    return """
            WITH fact_track_listens AS (
              SELECT
                spotify_track_id,
                count(*) AS listen_count,
                max(canonical_ended_at) AS last_played_at,
                sum(
                  CASE
                    WHEN julianday(canonical_ended_at) >= (
                      SELECT julianday(max(canonical_ended_at)) - ?
                      FROM fact_play_event
                      WHERE canonical_ended_at IS NOT NULL
                    )
                    THEN 1 ELSE 0
                  END
                ) AS recent_listen_count
              FROM fact_play_event
              WHERE spotify_track_id IS NOT NULL
                AND spotify_track_id != ''
              GROUP BY spotify_track_id
            ),
            ranked_fact_tracks AS (
              SELECT
                spotify_track_id,
                listen_count,
                recent_listen_count,
                row_number() OVER (
                  ORDER BY listen_count DESC, last_played_at DESC, spotify_track_id ASC
                ) AS track_rank
              FROM fact_track_listens
            ),
            fact_album_listens AS (
              SELECT
                spotify_album_id,
                count(*) AS album_listen_count,
                max(canonical_ended_at) AS last_played_at
              FROM fact_play_event
              WHERE spotify_album_id IS NOT NULL
                AND spotify_album_id != ''
              GROUP BY spotify_album_id
            ),
            top_album_ids AS (
              SELECT spotify_album_id
              FROM (
                SELECT
                  spotify_album_id,
                  row_number() OVER (
                    ORDER BY album_listen_count DESC, last_played_at DESC, spotify_album_id ASC
                  ) AS album_rank
                FROM fact_album_listens
              )
              WHERE album_rank <= ?
            ),
            fact_artist_listens AS (
              SELECT
                lower(trim(COALESCE(artist_name_canonical, ''))) AS artist_key,
                count(*) AS artist_listen_count,
                max(canonical_ended_at) AS last_played_at
              FROM fact_play_event
              WHERE trim(COALESCE(artist_name_canonical, '')) != ''
              GROUP BY artist_key
            ),
            top_artist_keys AS (
              SELECT artist_key
              FROM (
                SELECT
                  artist_key,
                  row_number() OVER (
                    ORDER BY artist_listen_count DESC, last_played_at DESC, artist_key ASC
                  ) AS artist_rank
                FROM fact_artist_listens
              )
              WHERE artist_rank <= ?
            ),
            top_list_track_ids AS (
              SELECT spotify_track_id
              FROM ranked_fact_tracks
              WHERE track_rank <= ?
                 OR recent_listen_count >= ?
              UNION
              SELECT DISTINCT fpe.spotify_track_id
              FROM fact_play_event fpe
              JOIN top_album_ids top_albums
                ON top_albums.spotify_album_id = fpe.spotify_album_id
              WHERE fpe.spotify_track_id IS NOT NULL
                AND fpe.spotify_track_id != ''
              UNION
              SELECT DISTINCT fpe.spotify_track_id
              FROM fact_play_event fpe
              JOIN top_artist_keys top_artists
                ON top_artists.artist_key = lower(trim(COALESCE(fpe.artist_name_canonical, '')))
              WHERE fpe.spotify_track_id IS NOT NULL
                AND fpe.spotify_track_id != ''
            ),
            fact_track_artist_listens AS (
              SELECT
                track_stats.spotify_track_id,
                max(artist_stats.artist_listen_count) AS artist_listen_count
              FROM (
                SELECT
                  spotify_track_id,
                  lower(trim(COALESCE(artist_name_canonical, ''))) AS artist_key
                FROM fact_play_event
                WHERE spotify_track_id IS NOT NULL
                  AND spotify_track_id != ''
                  AND trim(COALESCE(artist_name_canonical, '')) != ''
                GROUP BY spotify_track_id, artist_key
              ) track_stats
              JOIN fact_artist_listens artist_stats
                ON artist_stats.artist_key = track_stats.artist_key
              GROUP BY track_stats.spotify_track_id
            ),
            raw_track_listens AS (
              SELECT
                spotify_track_id,
                count(*) AS listen_count
              FROM raw_play_event
              WHERE spotify_track_id IS NOT NULL
                AND spotify_track_id != ''
              GROUP BY spotify_track_id
            ),
            accepted_source_tracks AS (
              SELECT
                st.id AS source_track_id,
                st.external_id AS spotify_track_id,
                stm.release_track_id,
                COALESCE(NULLIF(rt.primary_name, ''), NULLIF(st.source_name_raw, ''), st.external_id) AS track_name,
                max(COALESCE(ftl.listen_count, 0), COALESCE(rtl.listen_count, 0)) AS track_listen_count,
                COALESCE(ftal.artist_listen_count, 0) AS artist_listen_count,
                st.id AS source_track_row_id,
                stm.id AS source_track_map_row_id
              FROM source_track st
              JOIN source_track_map stm
                ON stm.source_track_id = st.id
              LEFT JOIN release_track rt
                ON rt.id = stm.release_track_id
              LEFT JOIN fact_track_listens ftl
                ON ftl.spotify_track_id = st.external_id
              LEFT JOIN fact_track_artist_listens ftal
                ON ftal.spotify_track_id = st.external_id
              LEFT JOIN raw_track_listens rtl
                ON rtl.spotify_track_id = st.external_id
              WHERE st.source_name = 'spotify'
                AND st.external_id IS NOT NULL
                AND st.external_id != ''
                AND stm.status = 'accepted'
            ),
            duplicate_spotify_track_ids AS (
              SELECT spotify_track_id
              FROM accepted_source_tracks
              GROUP BY spotify_track_id
              HAVING count(DISTINCT release_track_id) > 1
            ),
            split_release_track_ids AS (
              SELECT release_track_id
              FROM accepted_source_tracks
              GROUP BY release_track_id
              HAVING count(DISTINCT spotify_track_id) > 1
            ),
            suggested_analysis_release_track_ids AS (
              SELECT atm.release_track_id
              FROM analysis_track_map atm
              JOIN (
                SELECT analysis_track_id
                FROM analysis_track_map
                WHERE status = 'suggested'
                GROUP BY analysis_track_id
                HAVING count(DISTINCT release_track_id) > 1
              ) suggested_groups
                ON suggested_groups.analysis_track_id = atm.analysis_track_id
              WHERE atm.status = 'suggested'
            ),
            latest_fact_track_text AS (
              SELECT spotify_track_id, track_name_canonical, artist_name_canonical
              FROM (
                SELECT
                  spotify_track_id,
                  track_name_canonical,
                  artist_name_canonical,
                  row_number() OVER (
                    PARTITION BY spotify_track_id
                    ORDER BY canonical_ended_at DESC, id DESC
                  ) AS rn
                FROM fact_play_event
                WHERE spotify_track_id IS NOT NULL
                  AND spotify_track_id != ''
              )
              WHERE rn = 1
            ),
            missing_track_priority AS (
              SELECT
                source_track_id,
                spotify_track_id,
                release_track_id,
                track_name,
                artist_name,
                track_listen_count,
                artist_listen_count,
                identity_relevant,
                top_list_relevant
              FROM (
                SELECT
                  ast.source_track_id,
                  ast.spotify_track_id,
                  ast.release_track_id,
                  COALESCE(NULLIF(ast.track_name, ''), NULLIF(lft.track_name_canonical, ''), ast.spotify_track_id) AS track_name,
                  lft.artist_name_canonical AS artist_name,
                  ast.track_listen_count,
                  ast.artist_listen_count,
                  CASE
                    WHEN dup.spotify_track_id IS NOT NULL THEN 1
                    WHEN split.release_track_id IS NOT NULL THEN 1
                    WHEN suggested.release_track_id IS NOT NULL THEN 1
                    ELSE 0
                  END AS identity_relevant,
                  CASE
                    WHEN top_list.spotify_track_id IS NOT NULL THEN 1
                    ELSE 0
                  END AS top_list_relevant,
                  row_number() OVER (
                    PARTITION BY ast.spotify_track_id
                    ORDER BY
                      CASE
                        WHEN dup.spotify_track_id IS NOT NULL THEN 1
                        WHEN split.release_track_id IS NOT NULL THEN 1
                        WHEN suggested.release_track_id IS NOT NULL THEN 1
                        ELSE 0
                      END DESC,
                      CASE WHEN top_list.spotify_track_id IS NOT NULL THEN 1 ELSE 0 END DESC,
                      ast.track_listen_count DESC,
                      ast.artist_listen_count DESC,
                      ast.release_track_id ASC,
                      ast.source_track_map_row_id ASC,
                      ast.source_track_row_id ASC
                  ) AS rn
                FROM accepted_source_tracks ast
                LEFT JOIN duplicate_spotify_track_ids dup
                  ON dup.spotify_track_id = ast.spotify_track_id
                LEFT JOIN split_release_track_ids split
                  ON split.release_track_id = ast.release_track_id
                LEFT JOIN suggested_analysis_release_track_ids suggested
                  ON suggested.release_track_id = ast.release_track_id
                LEFT JOIN top_list_track_ids top_list
                  ON top_list.spotify_track_id = ast.spotify_track_id
                LEFT JOIN latest_fact_track_text lft
                  ON lft.spotify_track_id = ast.spotify_track_id
                LEFT JOIN spotify_track_catalog stc
                  ON stc.spotify_track_id = ast.spotify_track_id
                WHERE stc.spotify_track_id IS NULL
                   OR stc.duration_ms IS NULL
                   OR stc.disc_number IS NULL
                   OR stc.track_number IS NULL
                   OR stc.album_id IS NULL
                   OR json_extract(COALESCE(stc.raw_json, '{}'), '$.external_ids.isrc') IS NULL
                   OR json_extract(COALESCE(stc.raw_json, '{}'), '$.external_ids.isrc') = ''
                   OR lower(COALESCE(stc.last_status, '')) = 'error'
              )
              WHERE rn = 1
            )
            """


def _track_metadata_priority_params() -> tuple[int, int, int, int, int]:
    return (
        RECENT_TRACK_REPEAT_DAYS,
        TOP_ALBUM_PRIORITY_LIMIT,
        TOP_ARTIST_PRIORITY_LIMIT,
        TOP_TRACK_PRIORITY_LIMIT,
        RECENT_TRACK_REPEAT_MIN_PLAYS,
    )


def _known_track_ids_missing_metadata_for_scope(
    *,
    limit: int,
    offset: int,
    priority_scope: str = "all",
) -> tuple[list[str], bool]:
    bounded_limit = max(1, min(int(limit), MAX_LIMIT))
    bounded_offset = max(0, int(offset))
    normalized_scope = str(priority_scope or "all").strip().lower()
    if normalized_scope not in TRACK_METADATA_PRIORITY_SCOPES:
        normalized_scope = "all"
    priority_filter = ""
    if normalized_scope == "identity_and_top_listened":
        priority_filter = "WHERE identity_relevant = 1 OR top_list_relevant = 1"

    with sqlite_connection() as connection:
        rows = connection.execute(
            f"""
            {_track_metadata_priority_ctes()}
            SELECT spotify_track_id
            FROM missing_track_priority
            {priority_filter}
            ORDER BY
              identity_relevant DESC,
              top_list_relevant DESC,
              track_listen_count DESC,
              artist_listen_count DESC,
              spotify_track_id ASC
            LIMIT ?
            OFFSET ?
            """,
            (*_track_metadata_priority_params(), bounded_limit + 1, bounded_offset),
        ).fetchall()

    ids = [str(row[0]) for row in rows if row and row[0]]
    has_more = len(ids) > bounded_limit
    return ids[:bounded_limit], has_more


def get_identity_readiness_track_metadata_priority_comparison(*, sample_limit: int = 5) -> dict[str, Any]:
    bounded_sample_limit = max(0, min(int(sample_limit), 25))
    readiness_sql = f"""
        {track_identity_readiness_source_ctes()},
        duplicate_case_sources AS (
          SELECT
            cases.case_type,
            cases.case_key,
            sources.source_track_id,
            sources.spotify_track_id,
            sources.release_track_id,
            sources.release_track_name,
            sources.source_name_raw,
            CASE
              WHEN sources.catalog_fetched_at IS NULL
                OR lower(COALESCE(sources.catalog_last_status, '')) = 'error'
                OR sources.catalog_name IS NULL
                OR trim(COALESCE(sources.catalog_name, '')) = ''
                OR sources.catalog_duration_ms IS NULL
                OR sources.catalog_album_id IS NULL
                OR trim(COALESCE(sources.catalog_album_id, '')) = ''
                OR sources.catalog_artists_json IS NULL
                OR trim(COALESCE(sources.catalog_artists_json, '')) IN ('', '[]')
                OR (
                  (sources.source_isrc IS NULL OR trim(COALESCE(sources.source_isrc, '')) = '')
                  AND (
                    json_extract(COALESCE(sources.catalog_raw_json, '{{}}'), '$.external_ids.isrc') IS NULL
                    OR json_extract(COALESCE(sources.catalog_raw_json, '{{}}'), '$.external_ids.isrc') = ''
                  )
                )
                OR sources.catalog_album_id IS NULL
                OR sources.catalog_album_id = ''
              THEN 1 ELSE 0
            END AS track_metadata_missing,
            CASE
              WHEN sources.album_fetched_at IS NULL
                OR lower(COALESCE(sources.album_last_status, '')) = 'error'
                OR sources.album_name IS NULL
                OR trim(COALESCE(sources.album_name, '')) = ''
                OR sources.album_release_date IS NULL
                OR trim(COALESCE(sources.album_release_date, '')) = ''
              THEN 1 ELSE 0
            END AS album_metadata_missing
          FROM cases
          JOIN accepted_spotify_sources sources
            ON sources.spotify_track_id = cases.case_key
          WHERE cases.case_type = 'duplicate_spotify_track_id'
        ),
        split_case_sources AS (
          SELECT
            cases.case_type,
            cases.case_key,
            sources.source_track_id,
            sources.spotify_track_id,
            sources.release_track_id,
            sources.release_track_name,
            sources.source_name_raw,
            CASE
              WHEN sources.catalog_fetched_at IS NULL
                OR lower(COALESCE(sources.catalog_last_status, '')) = 'error'
                OR sources.catalog_name IS NULL
                OR trim(COALESCE(sources.catalog_name, '')) = ''
                OR sources.catalog_duration_ms IS NULL
                OR sources.catalog_album_id IS NULL
                OR trim(COALESCE(sources.catalog_album_id, '')) = ''
                OR sources.catalog_artists_json IS NULL
                OR trim(COALESCE(sources.catalog_artists_json, '')) IN ('', '[]')
                OR (
                  (sources.source_isrc IS NULL OR trim(COALESCE(sources.source_isrc, '')) = '')
                  AND (
                    json_extract(COALESCE(sources.catalog_raw_json, '{{}}'), '$.external_ids.isrc') IS NULL
                    OR json_extract(COALESCE(sources.catalog_raw_json, '{{}}'), '$.external_ids.isrc') = ''
                  )
                )
                OR sources.catalog_album_id IS NULL
                OR sources.catalog_album_id = ''
              THEN 1 ELSE 0
            END AS track_metadata_missing,
            CASE
              WHEN sources.album_fetched_at IS NULL
                OR lower(COALESCE(sources.album_last_status, '')) = 'error'
                OR sources.album_name IS NULL
                OR trim(COALESCE(sources.album_name, '')) = ''
                OR sources.album_release_date IS NULL
                OR trim(COALESCE(sources.album_release_date, '')) = ''
              THEN 1 ELSE 0
            END AS album_metadata_missing
          FROM cases
          JOIN accepted_spotify_sources sources
            ON sources.release_track_id = CAST(cases.case_key AS INTEGER)
          WHERE cases.case_type = 'release_track_source_split'
        ),
        case_sources AS (
          SELECT * FROM duplicate_case_sources
          UNION ALL
          SELECT * FROM split_case_sources
        ),
        blocked_cases AS (
          SELECT case_type, case_key
          FROM case_sources
          GROUP BY case_type, case_key
          HAVING max(track_metadata_missing) = 1
              OR max(album_metadata_missing) = 1
        )
        SELECT DISTINCT
          cs.source_track_id,
          cs.spotify_track_id,
          cs.release_track_id,
          cs.release_track_name,
          cs.source_name_raw,
          cs.track_metadata_missing,
          cs.album_metadata_missing
        FROM case_sources cs
        JOIN blocked_cases bc
          ON bc.case_type = cs.case_type
         AND bc.case_key = cs.case_key
        ORDER BY cs.spotify_track_id ASC, cs.release_track_id ASC, cs.source_track_id ASC
    """
    blocked_groups_sql = f"""
        {track_identity_readiness_source_ctes()},
        duplicate_case_sources AS (
          SELECT
            cases.case_type,
            cases.case_key,
            CASE
              WHEN sources.catalog_fetched_at IS NULL
                OR lower(COALESCE(sources.catalog_last_status, '')) = 'error'
                OR sources.catalog_name IS NULL
                OR trim(COALESCE(sources.catalog_name, '')) = ''
                OR sources.catalog_duration_ms IS NULL
                OR sources.catalog_album_id IS NULL
                OR trim(COALESCE(sources.catalog_album_id, '')) = ''
                OR sources.catalog_artists_json IS NULL
                OR trim(COALESCE(sources.catalog_artists_json, '')) IN ('', '[]')
                OR (
                  (sources.source_isrc IS NULL OR trim(COALESCE(sources.source_isrc, '')) = '')
                  AND (
                    json_extract(COALESCE(sources.catalog_raw_json, '{{}}'), '$.external_ids.isrc') IS NULL
                    OR json_extract(COALESCE(sources.catalog_raw_json, '{{}}'), '$.external_ids.isrc') = ''
                  )
                )
              THEN 1 ELSE 0
            END AS track_metadata_missing,
            CASE
              WHEN sources.album_fetched_at IS NULL
                OR lower(COALESCE(sources.album_last_status, '')) = 'error'
                OR sources.album_name IS NULL
                OR trim(COALESCE(sources.album_name, '')) = ''
                OR sources.album_release_date IS NULL
                OR trim(COALESCE(sources.album_release_date, '')) = ''
              THEN 1 ELSE 0
            END AS album_metadata_missing
          FROM cases
          JOIN accepted_spotify_sources sources
            ON sources.spotify_track_id = cases.case_key
          WHERE cases.case_type = 'duplicate_spotify_track_id'
        ),
        split_case_sources AS (
          SELECT
            cases.case_type,
            cases.case_key,
            CASE
              WHEN sources.catalog_fetched_at IS NULL
                OR lower(COALESCE(sources.catalog_last_status, '')) = 'error'
                OR sources.catalog_name IS NULL
                OR trim(COALESCE(sources.catalog_name, '')) = ''
                OR sources.catalog_duration_ms IS NULL
                OR sources.catalog_album_id IS NULL
                OR trim(COALESCE(sources.catalog_album_id, '')) = ''
                OR sources.catalog_artists_json IS NULL
                OR trim(COALESCE(sources.catalog_artists_json, '')) IN ('', '[]')
                OR (
                  (sources.source_isrc IS NULL OR trim(COALESCE(sources.source_isrc, '')) = '')
                  AND (
                    json_extract(COALESCE(sources.catalog_raw_json, '{{}}'), '$.external_ids.isrc') IS NULL
                    OR json_extract(COALESCE(sources.catalog_raw_json, '{{}}'), '$.external_ids.isrc') = ''
                  )
                )
              THEN 1 ELSE 0
            END AS track_metadata_missing,
            CASE
              WHEN sources.album_fetched_at IS NULL
                OR lower(COALESCE(sources.album_last_status, '')) = 'error'
                OR sources.album_name IS NULL
                OR trim(COALESCE(sources.album_name, '')) = ''
                OR sources.album_release_date IS NULL
                OR trim(COALESCE(sources.album_release_date, '')) = ''
              THEN 1 ELSE 0
            END AS album_metadata_missing
          FROM cases
          JOIN accepted_spotify_sources sources
            ON sources.release_track_id = CAST(cases.case_key AS INTEGER)
          WHERE cases.case_type = 'release_track_source_split'
        ),
        case_sources AS (
          SELECT * FROM duplicate_case_sources
          UNION ALL
          SELECT * FROM split_case_sources
        )
        SELECT count(*)
        FROM (
          SELECT case_type, case_key
          FROM case_sources
          GROUP BY case_type, case_key
          HAVING max(track_metadata_missing) = 1
              OR max(album_metadata_missing) = 1
        )
    """
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        readiness_rows = connection.execute(readiness_sql).fetchall()
        blocked_group_count = int(connection.execute(blocked_groups_sql).fetchone()[0] or 0)
        priority_rows = connection.execute(
            f"""
            {_track_metadata_priority_ctes()}
            SELECT spotify_track_id, identity_relevant, top_list_relevant
            FROM missing_track_priority
            """,
            _track_metadata_priority_params(),
        ).fetchall()

    priority_by_track_id = {
        str(row["spotify_track_id"]): {
            "identity_relevant": bool(row["identity_relevant"]),
            "top_list_relevant": bool(row["top_list_relevant"]),
        }
        for row in priority_rows
        if row["spotify_track_id"]
    }
    blocker_by_track_id: dict[str, dict[str, Any]] = {}
    for row in readiness_rows:
        spotify_track_id = str(row["spotify_track_id"] or "")
        if not spotify_track_id:
            continue
        item = blocker_by_track_id.setdefault(
            spotify_track_id,
            {
                "spotify_track_id": spotify_track_id,
                "source_track_id": int(row["source_track_id"]),
                "release_track_id": int(row["release_track_id"]),
                "release_track_name": str(row["release_track_name"] or ""),
                "source_name_raw": row["source_name_raw"],
                "track_metadata_missing": False,
                "album_metadata_missing": False,
            },
        )
        item["track_metadata_missing"] = bool(item["track_metadata_missing"] or row["track_metadata_missing"])
        item["album_metadata_missing"] = bool(item["album_metadata_missing"] or row["album_metadata_missing"])

    blockers = list(blocker_by_track_id.values())
    track_metadata_blockers = [item for item in blockers if item["track_metadata_missing"]]
    included = [
        item
        for item in track_metadata_blockers
        if priority_by_track_id.get(item["spotify_track_id"], {}).get("identity_relevant")
        or priority_by_track_id.get(item["spotify_track_id"], {}).get("top_list_relevant")
    ]
    not_included = [item for item in track_metadata_blockers if item not in included]

    def _sample(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        sampled: list[dict[str, Any]] = []
        for item in sorted(items, key=lambda value: value["spotify_track_id"])[:bounded_sample_limit]:
            priority = priority_by_track_id.get(item["spotify_track_id"], {})
            sampled.append(
                {
                    **item,
                    "identity_relevant": bool(priority.get("identity_relevant")),
                    "top_list_relevant": bool(priority.get("top_list_relevant")),
                }
            )
        return sampled

    return {
        "priority_scope": DEFAULT_TRACK_METADATA_PRIORITY_SCOPE,
        "blocked_groups": blocked_group_count,
        "distinct_spotify_source_tracks_needed": len(blockers),
        "distinct_spotify_source_tracks_missing_track_metadata": len(track_metadata_blockers),
        "included_by_priority_scope": len(included),
        "not_included_by_priority_scope": len(not_included),
        "album_metadata_only_blockers": len(
            [item for item in blockers if item["album_metadata_missing"] and not item["track_metadata_missing"]]
        ),
        "samples": {
            "not_included_by_priority_scope": _sample(not_included),
            "included_by_priority_scope": _sample(included),
        },
    }


def _known_source_album_ids(*, limit: int, offset: int) -> tuple[list[str], bool]:
    bounded_limit = max(1, min(int(limit), MAX_LIMIT))
    bounded_offset = max(0, int(offset))

    with sqlite_connection() as connection:
        rows = connection.execute(
            """
            WITH accepted_source_albums AS (
              SELECT sa.external_id AS spotify_album_id
              FROM source_album sa
              JOIN source_album_map sam
                ON sam.source_album_id = sa.id
              WHERE sa.source_name = 'spotify'
                AND sa.external_id IS NOT NULL
                AND sa.external_id != ''
                AND sam.status = 'accepted'
            ),
            raw_album_ids AS (
              SELECT spotify_album_id
              FROM raw_play_event
              WHERE spotify_album_id IS NOT NULL
                AND spotify_album_id != ''
            ),
            known_ids AS (
              SELECT spotify_album_id FROM accepted_source_albums
              UNION
              SELECT spotify_album_id FROM raw_album_ids
            )
            SELECT spotify_album_id
            FROM known_ids
            ORDER BY spotify_album_id ASC
            LIMIT ?
            OFFSET ?
            """,
            (bounded_limit + 1, bounded_offset),
        ).fetchall()

    ids = [str(row[0]) for row in rows if row and row[0]]
    has_more = len(ids) > bounded_limit
    return ids[:bounded_limit], has_more


def _known_album_ids_missing_metadata(*, limit: int, offset: int) -> tuple[list[str], bool]:
    bounded_limit = max(1, min(int(limit), MAX_LIMIT))
    bounded_offset = max(0, int(offset))

    with sqlite_connection() as connection:
        rows = connection.execute(
            """
            WITH accepted_source_albums AS (
              SELECT sa.external_id AS spotify_album_id
              FROM source_album sa
              JOIN source_album_map sam
                ON sam.source_album_id = sa.id
              WHERE sa.source_name = 'spotify'
                AND sa.external_id IS NOT NULL
                AND sa.external_id != ''
                AND sam.status = 'accepted'
            ),
            raw_album_ids AS (
              SELECT spotify_album_id
              FROM raw_play_event
              WHERE spotify_album_id IS NOT NULL
                AND spotify_album_id != ''
            ),
            known_ids AS (
              SELECT spotify_album_id FROM accepted_source_albums
              UNION
              SELECT spotify_album_id FROM raw_album_ids
            ),
            classified AS (
              SELECT
                known_ids.spotify_album_id,
                CASE
                  WHEN sac.spotify_album_id IS NULL
                    OR lower(COALESCE(sac.last_status, '')) = 'error'
                    THEN 1
                  WHEN sac.release_date IS NULL
                    OR sac.release_date = ''
                    THEN 2
                  ELSE 3
                END AS metadata_priority
              FROM known_ids
              LEFT JOIN spotify_album_catalog sac
                ON sac.spotify_album_id = known_ids.spotify_album_id
              WHERE sac.spotify_album_id IS NULL
                 OR lower(COALESCE(sac.last_status, '')) = 'error'
                 OR sac.release_date IS NULL
                 OR sac.release_date = ''
                 OR (
                    json_extract(COALESCE(sac.raw_json, '{}'), '$.external_ids.upc') IS NULL
                    AND json_extract(COALESCE(sac.raw_json, '{}'), '$.external_ids.ean') IS NULL
                  )
            )
            SELECT spotify_album_id
            FROM classified
            ORDER BY metadata_priority ASC, spotify_album_id ASC
            LIMIT ?
            OFFSET ?
            """,
            (bounded_limit + 1, bounded_offset),
        ).fetchall()

    ids = [str(row[0]) for row in rows if row and row[0]]
    has_more = len(ids) > bounded_limit
    return ids[:bounded_limit], has_more


def get_spotify_track_metadata_priority_debug(*, sample_limit: int = 5) -> dict[str, Any]:
    bounded_sample_limit = max(0, min(int(sample_limit), 25))
    readiness_comparison = get_identity_readiness_track_metadata_priority_comparison(
        sample_limit=bounded_sample_limit
    )
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        counts_row = connection.execute(
            f"""
            {_track_metadata_priority_ctes()}
            SELECT
              count(*) AS total_missing_accepted_source_track_metadata,
              sum(CASE WHEN identity_relevant = 1 THEN 1 ELSE 0 END) AS missing_identity_ambiguous_track_metadata,
              sum(CASE WHEN top_list_relevant = 1 THEN 1 ELSE 0 END) AS missing_top_track_metadata,
              sum(CASE WHEN identity_relevant = 1 OR top_list_relevant = 1 THEN 1 ELSE 0 END) AS missing_priority_track_metadata,
              sum(CASE WHEN identity_relevant = 1 AND top_list_relevant = 1 THEN 1 ELSE 0 END) AS identity_top_overlap,
              sum(CASE WHEN identity_relevant = 0 AND top_list_relevant = 0 THEN 1 ELSE 0 END) AS missing_deferred_track_metadata
            FROM missing_track_priority
            """,
            _track_metadata_priority_params(),
        ).fetchone()

        def _sample(where_sql: str) -> list[dict[str, Any]]:
            if bounded_sample_limit <= 0:
                return []
            rows = connection.execute(
                f"""
                {_track_metadata_priority_ctes()}
                SELECT
                  source_track_id,
                  spotify_track_id,
                  track_name,
                  artist_name,
                  identity_relevant,
                  top_list_relevant,
                  track_listen_count,
                  artist_listen_count
                FROM missing_track_priority
                WHERE {where_sql}
                ORDER BY
                  identity_relevant DESC,
                  top_list_relevant DESC,
                  track_listen_count DESC,
                  artist_listen_count DESC,
                  spotify_track_id ASC
                LIMIT ?
                """,
                (*_track_metadata_priority_params(), bounded_sample_limit),
            ).fetchall()
            return [
                {
                    "source_track_id": int(row["source_track_id"]),
                    "spotify_track_id": row["spotify_track_id"],
                    "track_name": row["track_name"],
                    "artist_name": row["artist_name"],
                    "identity_relevant": bool(row["identity_relevant"]),
                    "top_list_relevant": bool(row["top_list_relevant"]),
                    "deferred_backlog": not bool(row["identity_relevant"] or row["top_list_relevant"]),
                    "track_listen_count": int(row["track_listen_count"] or 0),
                    "artist_listen_count": int(row["artist_listen_count"] or 0),
                }
                for row in rows
            ]

        counts = dict(counts_row or {})
        samples = {
            "identity_ambiguous": _sample("identity_relevant = 1"),
            "top_list": _sample("top_list_relevant = 1"),
            "deferred": _sample("identity_relevant = 0 AND top_list_relevant = 0"),
        }

    return {
        "priority_scope": DEFAULT_TRACK_METADATA_PRIORITY_SCOPE,
        "top_thresholds": {
            "top_track_limit": TOP_TRACK_PRIORITY_LIMIT,
            "top_album_limit": TOP_ALBUM_PRIORITY_LIMIT,
            "top_artist_limit": TOP_ARTIST_PRIORITY_LIMIT,
            "recent_track_repeat_days": RECENT_TRACK_REPEAT_DAYS,
            "recent_track_repeat_min_plays": RECENT_TRACK_REPEAT_MIN_PLAYS,
        },
        "counts": {
            key: int(counts.get(key) or 0)
            for key in (
                "total_missing_accepted_source_track_metadata",
                "missing_priority_track_metadata",
                "missing_identity_ambiguous_track_metadata",
                "missing_top_track_metadata",
                "identity_top_overlap",
                "missing_deferred_track_metadata",
            )
        },
        "samples": samples,
        "identity_readiness_blockers": readiness_comparison,
    }


def _representative_album_ids(album_ids: set[str]) -> list[str]:
    normalized_ids = sorted({str(album_id).strip() for album_id in album_ids if str(album_id).strip()})
    if not normalized_ids:
        return []
    placeholders = ",".join("?" for _ in normalized_ids)

    with sqlite_connection() as connection:
        rows = connection.execute(
            f"""
            WITH raw_album_listens AS (
              SELECT
                spotify_album_id,
                count(*) AS listen_count
              FROM raw_play_event
              WHERE spotify_album_id IS NOT NULL
                AND spotify_album_id != ''
              GROUP BY spotify_album_id
            ),
            mapped_candidates AS (
              SELECT
                sa.external_id AS spotify_album_id,
                sam.release_album_id,
                COALESCE(ral.listen_count, 0) AS listen_count,
                sa.id AS source_album_row_id,
                sam.id AS source_album_map_row_id
              FROM source_album sa
              JOIN source_album_map sam
                ON sam.source_album_id = sa.id
              LEFT JOIN raw_album_listens ral
                ON ral.spotify_album_id = sa.external_id
              WHERE sa.source_name = 'spotify'
                AND sa.external_id IN ({placeholders})
                AND sam.status = 'accepted'
            ),
            ranked AS (
              SELECT
                spotify_album_id,
                release_album_id,
                row_number() OVER (
                  PARTITION BY release_album_id
                  ORDER BY
                    listen_count DESC,
                    spotify_album_id ASC,
                    source_album_map_row_id ASC,
                    source_album_row_id ASC
                ) AS rn
              FROM mapped_candidates
            )
            SELECT spotify_album_id, rn
            FROM ranked
            ORDER BY release_album_id ASC, rn ASC, spotify_album_id ASC
            """,
            tuple(normalized_ids),
        ).fetchall()

    chosen_ids = [str(row[0]) for row in rows if row and row[0] and int(row[1]) == 1]
    mapped_ids = {str(row[0]) for row in rows if row and row[0]}
    unmapped_ids = [album_id for album_id in normalized_ids if album_id not in mapped_ids]
    return sorted(chosen_ids + unmapped_ids)


def _split_track_ids_for_fetch(*, track_ids: list[str], require_identity_metadata: bool = False) -> tuple[list[str], set[str]]:
    normalized_ids = [str(track_id).strip() for track_id in track_ids if str(track_id).strip()]
    if not normalized_ids:
        return [], set()
    placeholders = ",".join("?" for _ in normalized_ids)
    with sqlite_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT spotify_track_id, album_id
            FROM spotify_track_catalog
            WHERE spotify_track_id IN ({placeholders})
            """,
            tuple(normalized_ids),
        ).fetchall()
        album_by_id = {str(row[0]): (str(row[1]) if row[1] else None) for row in rows if row and row[0]}
        to_fetch: list[str] = []
        known_album_ids: set[str] = set()
        for track_id in normalized_ids:
            if require_identity_metadata:
                is_complete = _is_track_metadata_complete(connection=connection, spotify_track_id=track_id)
            else:
                is_complete = _is_track_catalog_complete(connection=connection, spotify_track_id=track_id)
            if is_complete:
                known_album_id = album_by_id.get(track_id)
                if known_album_id:
                    known_album_ids.add(known_album_id)
                continue
            to_fetch.append(track_id)
    return to_fetch, known_album_ids


def _split_album_ids_for_fetch(*, album_ids: list[str]) -> list[str]:
    normalized_ids = [str(album_id).strip() for album_id in album_ids if str(album_id).strip()]
    if not normalized_ids:
        return []
    placeholders = ",".join("?" for _ in normalized_ids)
    with sqlite_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT spotify_album_id, total_tracks, last_status
            FROM spotify_album_catalog
            WHERE spotify_album_id IN ({placeholders})
            """,
            tuple(normalized_ids),
        ).fetchall()

    by_id: dict[str, tuple[Any, Any]] = {
        str(row[0]): (row[1], row[2]) for row in rows if row and row[0]
    }
    to_fetch: list[str] = []
    for album_id in normalized_ids:
        row = by_id.get(album_id)
        if row is None:
            to_fetch.append(album_id)
            continue
        total_tracks, last_status = row
        status_is_error = str(last_status or "").strip().lower() == "error"
        if total_tracks is not None and not status_is_error:
            continue
        to_fetch.append(album_id)
    return to_fetch


def _split_album_metadata_ids_for_fetch(*, album_ids: list[str]) -> list[str]:
    normalized_ids = [str(album_id).strip() for album_id in album_ids if str(album_id).strip()]
    if not normalized_ids:
        return []
    to_fetch: list[str] = []
    with sqlite_connection() as connection:
        for album_id in normalized_ids:
            if not _is_album_metadata_complete(connection=connection, spotify_album_id=album_id):
                to_fetch.append(album_id)
    return to_fetch


def _existing_complete_album_tracklist_ids(*, album_ids: list[str]) -> set[str]:
    normalized_ids = [str(album_id).strip() for album_id in album_ids if str(album_id).strip()]
    if not normalized_ids:
        return set()
    placeholders = ",".join("?" for _ in normalized_ids)
    with sqlite_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT
              ac.spotify_album_id,
              ac.total_tracks,
              count(at.spotify_track_id) AS track_row_count,
              sum(CASE WHEN lower(COALESCE(at.last_status, '')) = 'error' THEN 1 ELSE 0 END) AS error_row_count
            FROM spotify_album_catalog ac
            LEFT JOIN spotify_album_track at
              ON at.spotify_album_id = ac.spotify_album_id
            WHERE ac.spotify_album_id IN ({placeholders})
              AND ac.total_tracks IS NOT NULL
            GROUP BY ac.spotify_album_id, ac.total_tracks
            """,
            tuple(normalized_ids),
        ).fetchall()

    complete_ids: set[str] = set()
    for row in rows:
        if not row or not row[0]:
            continue
        album_id = str(row[0])
        total_tracks = int(row[1] or 0)
        track_row_count = int(row[2] or 0)
        error_row_count = int(row[3] or 0)
        if track_row_count >= total_tracks and error_row_count == 0:
            complete_ids.add(album_id)
    return complete_ids


def _album_relevance_stats(*, album_ids: list[str]) -> dict[str, tuple[int, int]]:
    normalized_ids = [str(album_id).strip() for album_id in album_ids if str(album_id).strip()]
    if not normalized_ids:
        return {}
    placeholders = ",".join("?" for _ in normalized_ids)
    with sqlite_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT
              spotify_album_id,
              count(DISTINCT CASE WHEN spotify_track_id IS NOT NULL AND spotify_track_id != '' THEN spotify_track_id END)
                AS listened_track_count,
              count(*) AS total_album_play_count
            FROM raw_play_event
            WHERE spotify_album_id IN ({placeholders})
            GROUP BY spotify_album_id
            """,
            tuple(normalized_ids),
        ).fetchall()
    stats: dict[str, tuple[int, int]] = {}
    for row in rows:
        if not row or not row[0]:
            continue
        stats[str(row[0])] = (int(row[1] or 0), int(row[2] or 0))
    return stats


def _is_track_catalog_complete(*, connection: sqlite3.Connection, spotify_track_id: str) -> bool:
    normalized_track_id = str(spotify_track_id or "").strip()
    if not normalized_track_id:
        return False
    row = connection.execute(
        """
        SELECT duration_ms, album_id, last_status
        FROM spotify_track_catalog
        WHERE spotify_track_id = ?
        """,
        (normalized_track_id,),
    ).fetchone()
    if row is None:
        return False
    duration_ms, album_id, last_status = row
    if duration_ms is None:
        return False
    if not str(album_id or "").strip():
        return False
    status_is_error = str(last_status or "").strip().lower() == "error"
    return not status_is_error


def _is_track_metadata_complete(*, connection: sqlite3.Connection, spotify_track_id: str) -> bool:
    return bool(_track_metadata_completion_state(connection=connection, spotify_track_id=spotify_track_id)["complete"])


def _track_metadata_completion_state(*, connection: sqlite3.Connection, spotify_track_id: str) -> dict[str, Any]:
    normalized_track_id = str(spotify_track_id or "").strip()
    if not normalized_track_id:
        return {"complete": False, "gaps": ["spotify_track_id"], "album_id": None}
    row = connection.execute(
        """
        SELECT name, duration_ms, album_id, disc_number, track_number, artists_json, raw_json, last_status
        FROM spotify_track_catalog
        WHERE spotify_track_id = ?
        """,
        (normalized_track_id,),
    ).fetchone()
    if row is None:
        return {"complete": False, "gaps": ["missing_track_catalog_row"], "album_id": None}
    name, duration_ms, album_id, disc_number, track_number, artists_json, raw_json, last_status = row
    gaps: list[str] = []
    if not str(name or "").strip():
        gaps.append("track_name")
    if duration_ms is None:
        gaps.append("duration_ms")
    if not str(album_id or "").strip():
        gaps.append("album_id")
    if disc_number is None or track_number is None:
        if disc_number is None:
            gaps.append("disc_number")
        if track_number is None:
            gaps.append("track_number")
    status_is_error = str(last_status or "").strip().lower() == "error"
    if status_is_error:
        gaps.append("last_status")
    artist_payload = _json_list(artists_json)
    if not artist_payload or not any(str(item.get("name") or "").strip() for item in artist_payload if isinstance(item, dict)):
        gaps.append("artists")
    raw_payload = _json_object(raw_json)
    external_ids = raw_payload.get("external_ids") if isinstance(raw_payload, dict) else {}
    if not isinstance(external_ids, dict):
        external_ids = {}
    if not str(external_ids.get("isrc") or "").strip():
        gaps.append("isrc")
    embedded_album = raw_payload.get("album") if isinstance(raw_payload.get("album"), dict) else {}
    embedded_album_name = str(embedded_album.get("name") or "").strip()
    local_album_name = None
    if embedded_album_name and str(album_id or "").strip():
        album_row = connection.execute(
            """
            SELECT name
            FROM spotify_album_catalog
            WHERE spotify_album_id = ?
            """,
            (str(album_id),),
        ).fetchone()
        local_album_name = album_row[0] if album_row else None
        if not str(local_album_name or "").strip():
            gaps.append("album_display_name")
    return {
        "complete": not gaps,
        "gaps": gaps,
        "album_id": album_id,
        "embedded_album_name": embedded_album_name or None,
        "local_album_name": local_album_name,
    }


def _is_album_metadata_complete(*, connection: sqlite3.Connection, spotify_album_id: str) -> bool:
    normalized_album_id = str(spotify_album_id or "").strip()
    if not normalized_album_id:
        return False
    row = connection.execute(
        """
        SELECT release_date, total_tracks, raw_json, last_status
        FROM spotify_album_catalog
        WHERE spotify_album_id = ?
        """,
        (normalized_album_id,),
    ).fetchone()
    if row is None:
        return False
    release_date, total_tracks, raw_json, last_status = row
    if not str(release_date or "").strip():
        return False
    if total_tracks is None:
        return False
    status_is_error = str(last_status or "").strip().lower() == "error"
    if status_is_error:
        return False
    try:
        raw_payload = json.loads(raw_json or "{}")
    except json.JSONDecodeError:
        raw_payload = {}
    external_ids = raw_payload.get("external_ids") if isinstance(raw_payload, dict) else {}
    if not isinstance(external_ids, dict):
        external_ids = {}
    has_upc_or_ean = bool(str(external_ids.get("upc") or "").strip() or str(external_ids.get("ean") or "").strip())
    return has_upc_or_ean


def _is_album_catalog_complete(*, connection: sqlite3.Connection, spotify_album_id: str) -> bool:
    normalized_album_id = str(spotify_album_id or "").strip()
    if not normalized_album_id:
        return False
    row = connection.execute(
        """
        SELECT total_tracks, last_status
        FROM spotify_album_catalog
        WHERE spotify_album_id = ?
        """,
        (normalized_album_id,),
    ).fetchone()
    if row is None:
        return False
    total_tracks, last_status = row
    if total_tracks is None:
        return False
    status_is_error = str(last_status or "").strip().lower() == "error"
    if status_is_error:
        return False
    track_row = connection.execute(
        """
        SELECT
          count(*) AS track_count,
          sum(CASE WHEN lower(COALESCE(last_status, '')) = 'error' THEN 1 ELSE 0 END) AS error_count
        FROM spotify_album_track
        WHERE spotify_album_id = ?
        """,
        (normalized_album_id,),
    ).fetchone()
    track_count = int(track_row[0] or 0) if track_row else 0
    error_count = int(track_row[1] or 0) if track_row else 0
    return track_count >= int(total_tracks) and error_count == 0


def _track_catalog_completion_info(*, spotify_track_id: str) -> tuple[bool, str | None]:
    normalized_track_id = str(spotify_track_id or "").strip()
    if not normalized_track_id:
        return False, None
    with sqlite_connection() as connection:
        row = connection.execute(
            """
            SELECT duration_ms, album_id, last_status
            FROM spotify_track_catalog
            WHERE spotify_track_id = ?
            """,
            (normalized_track_id,),
        ).fetchone()
        is_complete = _is_track_catalog_complete(connection=connection, spotify_track_id=normalized_track_id)
    if row is None:
        return False, None
    _, album_id, _ = row
    return is_complete, (str(album_id).strip() if str(album_id or "").strip() else None)


def _album_catalog_is_complete(*, spotify_album_id: str) -> bool:
    normalized_album_id = str(spotify_album_id or "").strip()
    if not normalized_album_id:
        return False
    with sqlite_connection() as connection:
        return _is_album_catalog_complete(connection=connection, spotify_album_id=normalized_album_id)


def _album_tracklist_is_complete(*, spotify_album_id: str) -> bool:
    with sqlite_connection() as connection:
        return _is_album_catalog_complete(connection=connection, spotify_album_id=spotify_album_id)


def _album_tracklist_needs_fetch(*, connection: sqlite3.Connection, album_id: str) -> bool:
    normalized_album_id = str(album_id or "").strip()
    if not normalized_album_id:
        return False
    catalog_row = connection.execute(
        """
        SELECT total_tracks
        FROM spotify_album_catalog
        WHERE spotify_album_id = ?
        """,
        (normalized_album_id,),
    ).fetchone()
    if catalog_row is None:
        return False
    total_tracks = catalog_row[0]
    if total_tracks is None:
        return False
    track_row = connection.execute(
        """
        SELECT
          count(*) AS track_count,
          sum(CASE WHEN lower(COALESCE(last_status, '')) = 'error' THEN 1 ELSE 0 END) AS error_count
        FROM spotify_album_track
        WHERE spotify_album_id = ?
        """,
        (normalized_album_id,),
    ).fetchone()
    track_count = int(track_row[0] or 0) if track_row else 0
    error_count = int(track_row[1] or 0) if track_row else 0
    return track_count < int(total_tracks) or error_count > 0


def _album_track_resume_offset(*, connection: sqlite3.Connection, album_id: str, force_refresh: bool) -> int:
    if force_refresh:
        return 0
    normalized_album_id = str(album_id or "").strip()
    if not normalized_album_id:
        return 0
    catalog_row = connection.execute(
        """
        SELECT total_tracks
        FROM spotify_album_catalog
        WHERE spotify_album_id = ?
        """,
        (normalized_album_id,),
    ).fetchone()
    if catalog_row is None or catalog_row[0] is None:
        return 0
    total_tracks = int(catalog_row[0] or 0)
    track_row = connection.execute(
        """
        SELECT
          count(*) AS track_count,
          sum(CASE WHEN lower(COALESCE(last_status, '')) = 'error' THEN 1 ELSE 0 END) AS error_count
        FROM spotify_album_track
        WHERE spotify_album_id = ?
        """,
        (normalized_album_id,),
    ).fetchone()
    track_count = int(track_row[0] or 0) if track_row else 0
    error_count = int(track_row[1] or 0) if track_row else 0
    if error_count > 0:
        return 0
    if track_count > 0 and track_count < total_tracks:
        return track_count
    return 0


def _queue_mark_done(*, queue_id: int) -> None:
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            UPDATE spotify_catalog_backfill_queue
            SET
              status = 'done',
              last_attempted_at = ?,
              last_error = NULL
            WHERE id = ?
            """,
            (_utc_now(), int(queue_id)),
        )


def _queue_mark_error(*, queue_id: int, error_message: str) -> None:
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            UPDATE spotify_catalog_backfill_queue
            SET
              status = 'error',
              last_attempted_at = ?,
              attempts = COALESCE(attempts, 0) + 1,
              last_error = ?
            WHERE id = ?
            """,
            (_utc_now(), str(error_message), int(queue_id)),
        )


def _repair_queue_status_for_item(*, connection: sqlite3.Connection, entity_type: str, spotify_id: str) -> str:
    normalized_entity_type = str(entity_type or "").strip().lower()
    normalized_spotify_id = str(spotify_id or "").strip()
    if normalized_entity_type not in {"track", "album"} or not normalized_spotify_id:
        return "pending"
    if normalized_entity_type == "track":
        is_complete = _is_track_catalog_complete(connection=connection, spotify_track_id=normalized_spotify_id)
    else:
        is_complete = _is_album_catalog_complete(connection=connection, spotify_album_id=normalized_spotify_id)
    next_status = "done" if is_complete else "pending"
    connection.execute(
        """
        UPDATE spotify_catalog_backfill_queue
        SET
          status = ?,
          last_error = CASE WHEN ? = 'pending' THEN NULL ELSE last_error END
        WHERE entity_type = ? AND spotify_id = ?
        """,
        (next_status, next_status, normalized_entity_type, normalized_spotify_id),
    )
    return next_status


def _pending_queue_items(*, limit: int) -> list[dict[str, Any]]:
    bounded_limit = max(1, int(limit))
    with sqlite_connection() as connection:
        rows = connection.execute(
            """
            SELECT
              id,
              entity_type,
              spotify_id,
              reason,
              priority,
              status,
              requested_at,
              last_attempted_at,
              attempts,
              last_error
            FROM spotify_catalog_backfill_queue
            WHERE status = 'pending'
            ORDER BY
              CASE
                WHEN reason LIKE '%identity_metadata%' THEN 0
                WHEN reason LIKE '%manual_priority%' THEN 1
                WHEN reason LIKE '%tracklist_completion%' THEN 2
                WHEN reason LIKE '%full_backfill%' THEN 3
                ELSE 4
              END ASC,
              priority DESC,
              requested_at ASC,
              id ASC
            LIMIT ?
            """,
            (bounded_limit,),
        ).fetchall()
    return [
        {
            "id": int(row[0]),
            "entity_type": str(row[1]),
            "spotify_id": str(row[2]),
            "reason": row[3],
            "priority": int(row[4] or 0),
            "status": str(row[5]),
            "requested_at": row[6],
            "last_attempted_at": row[7],
            "attempts": int(row[8] or 0),
            "last_error": row[9],
        }
        for row in rows
    ]


def enqueue_spotify_catalog_backfill_items(*, items: list[dict[str, Any]] | None) -> dict[str, Any]:
    normalized_items = items if isinstance(items, list) else []
    received = len(normalized_items)
    invalid = 0
    already_complete = 0
    enqueued = 0
    updated = 0

    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for raw_item in normalized_items:
        if not isinstance(raw_item, dict):
            invalid += 1
            continue
        entity_type = str(raw_item.get("entity_type") or "").strip().lower()
        spotify_id = str(raw_item.get("spotify_id") or "").strip()
        if entity_type not in {"track", "album"} or not spotify_id:
            invalid += 1
            continue
        reason = str(raw_item.get("reason") or "").strip() or None
        priority_raw = raw_item.get("priority", 0)
        try:
            priority = int(priority_raw)
        except (TypeError, ValueError):
            priority = 0
        dedupe_key = (entity_type, spotify_id)
        existing = deduped.get(dedupe_key)
        if existing is None:
            deduped[dedupe_key] = {
                "entity_type": entity_type,
                "spotify_id": spotify_id,
                "reason": reason,
                "priority": priority,
            }
            continue
        existing["priority"] = max(int(existing.get("priority", 0)), priority)
        if reason:
            current_reason = str(existing.get("reason") or "").strip()
            if not current_reason:
                existing["reason"] = reason
            elif reason not in current_reason.split(" | "):
                existing["reason"] = f"{current_reason} | {reason}"

    for item in deduped.values():
        entity_type = str(item["entity_type"])
        spotify_id = str(item["spotify_id"])
        reason = item.get("reason")
        priority = int(item.get("priority", 0))

        with sqlite_connection(write=True) as connection:
            if entity_type == "track":
                is_complete = _is_track_catalog_complete(connection=connection, spotify_track_id=spotify_id)
            else:
                is_complete = _is_album_catalog_complete(connection=connection, spotify_album_id=spotify_id)
        if is_complete:
            already_complete += 1
            continue

        with sqlite_connection(write=True) as connection:
            existing_row = connection.execute(
                """
                SELECT id, priority, reason, status
                FROM spotify_catalog_backfill_queue
                WHERE entity_type = ? AND spotify_id = ?
                """,
                (entity_type, spotify_id),
            ).fetchone()
            if existing_row is None:
                connection.execute(
                    """
                    INSERT INTO spotify_catalog_backfill_queue (
                      entity_type, spotify_id, reason, priority, status, requested_at, attempts
                    ) VALUES (?, ?, ?, ?, 'pending', ?, 0)
                    """,
                    (entity_type, spotify_id, reason, priority, _utc_now()),
                )
                enqueued += 1
                continue

            row_id = int(existing_row[0])
            row_priority = int(existing_row[1] or 0)
            row_reason = str(existing_row[2] or "").strip()
            row_status = str(existing_row[3] or "").strip().lower()
            next_priority = max(row_priority, priority)
            next_reason = row_reason
            if reason:
                if not next_reason:
                    next_reason = str(reason)
                elif str(reason) not in next_reason.split(" | "):
                    next_reason = f"{next_reason} | {reason}"
            next_status = "pending" if row_status in {"error", "done"} else row_status or "pending"
            connection.execute(
                """
                UPDATE spotify_catalog_backfill_queue
                SET
                  reason = ?,
                  priority = ?,
                  status = ?,
                  last_error = CASE WHEN ? = 'pending' THEN NULL ELSE last_error END
                WHERE id = ?
                """,
                (next_reason or None, next_priority, next_status, next_status, row_id),
            )
            if row_status == "done":
                # If a previously done item is re-enqueued and no longer complete, reopen to pending.
                _repair_queue_status_for_item(connection=connection, entity_type=entity_type, spotify_id=spotify_id)
            updated += 1

    return {
        "ok": True,
        "received": received,
        "enqueued": enqueued,
        "already_complete": already_complete,
        "updated": updated,
        "invalid": invalid,
    }


def list_spotify_catalog_backfill_queue(
    *,
    status_filter: str | None = None,
    reason_filter: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 200))
    bounded_offset = max(0, int(offset))
    normalized_status = str(status_filter or "").strip().lower()
    if normalized_status not in {"pending", "done", "error"}:
        normalized_status = ""
    normalized_reason = str(reason_filter or "").strip().lower()
    if normalized_reason not in CATALOG_BACKFILL_REASONS:
        normalized_reason = ""

    counts_sql = """
        SELECT
          sum(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_count,
          sum(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done_count,
          sum(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_count
        FROM spotify_catalog_backfill_queue
    """
    with sqlite_connection() as connection:
        counts_row = connection.execute(counts_sql).fetchone()
        counts = {
            "pending": int((counts_row[0] if counts_row else 0) or 0),
            "done": int((counts_row[1] if counts_row else 0) or 0),
            "error": int((counts_row[2] if counts_row else 0) or 0),
        }
        reason_counts_rows = connection.execute(
            """
            SELECT
              CASE
                WHEN reason LIKE '%identity_metadata%' THEN 'identity_metadata'
                WHEN reason LIKE '%manual_priority%' THEN 'manual_priority'
                WHEN reason LIKE '%tracklist_completion%' THEN 'tracklist_completion'
                WHEN reason LIKE '%full_backfill%' THEN 'full_backfill'
                ELSE 'other'
              END AS reason_key,
              count(*)
            FROM spotify_catalog_backfill_queue
            GROUP BY reason_key
            """
        ).fetchall()
        reason_counts = {
            "identity_metadata": 0,
            "manual_priority": 0,
            "tracklist_completion": 0,
            "full_backfill": 0,
            "other": 0,
        }
        for row in reason_counts_rows:
            reason_counts[str(row[0] or "other")] = int(row[1] or 0)

        where_clauses = []
        params: list[Any] = []
        if normalized_status:
            where_clauses.append("status = ?")
            params.append(normalized_status)
        if normalized_reason:
            where_clauses.append("reason LIKE ?")
            params.append(f"%{normalized_reason}%")
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        order_sql = """
            ORDER BY
              CASE
                WHEN reason LIKE '%identity_metadata%' THEN 0
                WHEN reason LIKE '%manual_priority%' THEN 1
                WHEN reason LIKE '%tracklist_completion%' THEN 2
                WHEN reason LIKE '%full_backfill%' THEN 3
                ELSE 4
              END ASC,
              priority DESC,
              requested_at ASC,
              id ASC
        """
        if where_clauses:
            total = int(
                connection.execute(
                    f"SELECT count(*) FROM spotify_catalog_backfill_queue {where_sql}",
                    params,
                ).fetchone()[0]
            )
            rows = connection.execute(
                f"""
                SELECT
                  id,
                  entity_type,
                  spotify_id,
                  reason,
                  priority,
                  status,
                  requested_at,
                  last_attempted_at,
                  attempts,
                  last_error
                FROM spotify_catalog_backfill_queue
                {where_sql}
                {order_sql}
                LIMIT ?
                OFFSET ?
                """,
                [*params, bounded_limit, bounded_offset],
            ).fetchall()
        else:
            total = int(connection.execute("SELECT count(*) FROM spotify_catalog_backfill_queue").fetchone()[0])
            rows = connection.execute(
                f"""
                SELECT
                  id,
                  entity_type,
                  spotify_id,
                  reason,
                  priority,
                  status,
                  requested_at,
                  last_attempted_at,
                  attempts,
                  last_error
                FROM spotify_catalog_backfill_queue
                {order_sql}
                LIMIT ?
                OFFSET ?
                """,
                (bounded_limit, bounded_offset),
            ).fetchall()

    items = [
        {
            "id": int(row[0]),
            "entity_type": str(row[1]),
            "spotify_id": str(row[2]),
            "reason": row[3],
            "priority": int(row[4] or 0),
            "status": str(row[5]),
            "requested_at": row[6],
            "last_attempted_at": row[7],
            "attempts": int(row[8] or 0),
            "last_error": row[9],
        }
        for row in rows
    ]
    return {"ok": True, "items": items, "total": total, "counts": counts, "reason_counts": reason_counts}


def inspect_spotify_catalog_queue_resolution_evidence() -> dict[str, Any]:
    ambiguous_ctes = track_identity_readiness_source_ctes()
    report_sql = f"""
        {ambiguous_ctes},
        duplicate_case_sources AS (
          SELECT sources.*
          FROM cases
          JOIN accepted_spotify_sources sources
            ON sources.spotify_track_id = cases.case_key
          WHERE cases.case_type = 'duplicate_spotify_track_id'
        ),
        split_case_sources AS (
          SELECT sources.*
          FROM cases
          JOIN accepted_spotify_sources sources
            ON sources.release_track_id = CAST(cases.case_key AS INTEGER)
          WHERE cases.case_type = 'release_track_source_split'
        ),
        ambiguous_sources AS (
          SELECT * FROM duplicate_case_sources
          UNION
          SELECT * FROM split_case_sources
        ),
        candidate_albums AS (
          SELECT DISTINCT catalog_album_id AS spotify_album_id
          FROM ambiguous_sources
          WHERE catalog_album_id IS NOT NULL
            AND trim(catalog_album_id) != ''
        ),
        sibling_tracks AS (
          SELECT DISTINCT sat.spotify_track_id
          FROM spotify_album_track sat
          JOIN candidate_albums ca
            ON ca.spotify_album_id = sat.spotify_album_id
        ),
        classified_queue AS (
          SELECT
            q.id,
            q.entity_type,
            q.spotify_id,
            q.reason,
            q.priority,
            q.status,
            q.requested_at,
            q.last_attempted_at,
            q.attempts,
            q.last_error,
            CASE
              WHEN q.entity_type = 'track' THEN 'track_metadata'
              WHEN q.entity_type = 'album' THEN 'album_metadata_or_tracklist'
              ELSE 'unknown'
            END AS inferred_target,
            CASE
              WHEN q.status = 'error' THEN 'stale_or_blocked'
              WHEN q.entity_type = 'track' AND ast.spotify_track_id IS NOT NULL THEN 'directly_relevant'
              WHEN q.entity_type = 'album' AND ca.spotify_album_id IS NOT NULL THEN 'directly_relevant'
              WHEN q.entity_type = 'track' AND sib.spotify_track_id IS NOT NULL THEN 'possibly_relevant'
              WHEN COALESCE(q.reason, '') LIKE '%identity_metadata%' THEN 'possibly_relevant'
              WHEN q.entity_type = 'album' AND COALESCE(q.reason, '') = 'album_lookup_visible_incomplete'
              THEN 'generic_catalog_backfill'
              WHEN COALESCE(q.reason, '') LIKE '%full_backfill%'
                OR COALESCE(q.reason, '') LIKE '%tracklist_completion%'
                OR COALESCE(q.reason, '') LIKE '%manual_priority%'
              THEN 'generic_catalog_backfill'
              ELSE 'unknown'
            END AS relevance_bucket,
            ast.spotify_track_id IS NOT NULL AS is_ambiguous_source_track,
            ca.spotify_album_id IS NOT NULL AS is_candidate_album,
            sib.spotify_track_id IS NOT NULL AS is_sibling_track,
            CASE
              WHEN q.entity_type NOT IN ('track', 'album') THEN 'unrecognized_entity_type'
              WHEN q.entity_type = 'album'
                AND COALESCE(q.reason, '') LIKE '%visible%' THEN 'legacy_album_lookup_visible_incomplete'
              WHEN q.status = 'done' THEN 'already_done_but_not_resolution_related'
              WHEN q.entity_type = 'album'
                AND ca.spotify_album_id IS NULL THEN 'not_candidate_album'
              WHEN q.entity_type = 'track'
                AND ast.spotify_track_id IS NULL
                AND sib.spotify_track_id IS NULL THEN 'not_ambiguous_source_track'
              WHEN COALESCE(q.reason, '') NOT IN ('', 'identity_metadata', 'manual_priority', 'tracklist_completion', 'full_backfill')
                AND COALESCE(q.reason, '') NOT LIKE '%identity_metadata%'
                AND COALESCE(q.reason, '') NOT LIKE '%manual_priority%'
                AND COALESCE(q.reason, '') NOT LIKE '%tracklist_completion%'
                AND COALESCE(q.reason, '') NOT LIKE '%full_backfill%'
              THEN 'unrecognized_reason'
              WHEN q.status = 'pending' THEN 'pending_but_not_resolution_related'
              ELSE 'insufficient_local_links'
            END AS unknown_reason
          FROM spotify_catalog_backfill_queue q
          LEFT JOIN ambiguous_sources ast
            ON q.entity_type = 'track'
           AND ast.spotify_track_id = q.spotify_id
          LEFT JOIN candidate_albums ca
            ON q.entity_type = 'album'
           AND ca.spotify_album_id = q.spotify_id
          LEFT JOIN sibling_tracks sib
            ON q.entity_type = 'track'
           AND sib.spotify_track_id = q.spotify_id
        )
        SELECT
          cq.*,
          stc.name AS track_name,
          stc.duration_ms,
          stc.explicit,
          stc.disc_number,
          stc.track_number,
          stc.album_id AS track_album_id,
          stc.artists_json AS track_artists_json,
          stc.raw_json AS track_raw_json,
          stc.fetched_at AS track_fetched_at,
          stc.last_status AS track_last_status,
          sac.name AS album_name,
          sac.release_date AS album_release_date,
          sac.total_tracks AS album_total_tracks,
          sac.images_json AS album_images_json,
          sac.raw_json AS album_raw_json,
          sac.fetched_at AS album_fetched_at,
          sac.last_status AS album_last_status,
          COALESCE(atc.album_track_count, 0) AS album_track_count
        FROM classified_queue cq
        LEFT JOIN spotify_track_catalog stc
          ON cq.entity_type = 'track'
         AND stc.spotify_track_id = cq.spotify_id
        LEFT JOIN spotify_album_catalog sac
          ON cq.entity_type = 'album'
         AND sac.spotify_album_id = cq.spotify_id
        LEFT JOIN (
          SELECT spotify_album_id, count(*) AS album_track_count
          FROM spotify_album_track
          GROUP BY spotify_album_id
        ) atc
          ON cq.entity_type = 'album'
         AND atc.spotify_album_id = cq.spotify_id
        ORDER BY cq.id ASC
    """
    delta_sql = f"""
        {ambiguous_ctes},
        duplicate_case_sources AS (
          SELECT sources.*
          FROM cases
          JOIN accepted_spotify_sources sources
            ON sources.spotify_track_id = cases.case_key
          WHERE cases.case_type = 'duplicate_spotify_track_id'
        ),
        split_case_sources AS (
          SELECT sources.*
          FROM cases
          JOIN accepted_spotify_sources sources
            ON sources.release_track_id = CAST(cases.case_key AS INTEGER)
          WHERE cases.case_type = 'release_track_source_split'
        ),
        ambiguous_sources AS (
          SELECT * FROM duplicate_case_sources
          UNION
          SELECT * FROM split_case_sources
        ),
        candidate_albums AS (
          SELECT DISTINCT catalog_album_id AS spotify_album_id
          FROM ambiguous_sources
          WHERE catalog_album_id IS NOT NULL
            AND trim(catalog_album_id) != ''
        ),
        candidate_album_status AS (
          SELECT
            ca.spotify_album_id,
            q.id AS queue_id,
            sac.fetched_at,
            sac.last_status,
            sac.release_date,
            sac.total_tracks,
            sac.images_json,
            sac.raw_json,
            COALESCE(atc.album_track_count, 0) AS album_track_count
          FROM candidate_albums ca
          LEFT JOIN spotify_catalog_backfill_queue q
            ON q.entity_type = 'album'
           AND q.spotify_id = ca.spotify_album_id
          LEFT JOIN spotify_album_catalog sac
            ON sac.spotify_album_id = ca.spotify_album_id
          LEFT JOIN (
            SELECT spotify_album_id, count(*) AS album_track_count
            FROM spotify_album_track
            GROUP BY spotify_album_id
          ) atc
            ON atc.spotify_album_id = ca.spotify_album_id
        ),
        sibling_track_status AS (
          SELECT
            sat.spotify_album_id,
            sat.spotify_track_id,
            q.id AS queue_id,
            stc.name,
            stc.duration_ms,
            stc.explicit,
            stc.disc_number,
            stc.track_number,
            stc.artists_json,
            stc.raw_json,
            stc.fetched_at,
            stc.last_status
          FROM spotify_album_track sat
          JOIN candidate_albums ca
            ON ca.spotify_album_id = sat.spotify_album_id
          LEFT JOIN spotify_catalog_backfill_queue q
            ON q.entity_type = 'track'
           AND q.spotify_id = sat.spotify_track_id
          LEFT JOIN spotify_track_catalog stc
            ON stc.spotify_track_id = sat.spotify_track_id
        ),
        ambiguous_track_status AS (
          SELECT DISTINCT
            sources.spotify_track_id,
            q.id AS queue_id
          FROM ambiguous_sources sources
          LEFT JOIN spotify_catalog_backfill_queue q
            ON q.entity_type = 'track'
           AND q.spotify_id = sources.spotify_track_id
        )
        SELECT 'candidate_album_missing_from_queue' AS bucket, spotify_album_id AS spotify_id, NULL AS parent_album_id
        FROM candidate_album_status
        WHERE queue_id IS NULL
        UNION ALL
        SELECT 'candidate_album_queued_missing_metadata', spotify_album_id, NULL
        FROM candidate_album_status
        WHERE queue_id IS NOT NULL
          AND (
            fetched_at IS NULL
            OR lower(COALESCE(last_status, '')) = 'error'
            OR release_date IS NULL
            OR trim(COALESCE(release_date, '')) = ''
            OR total_tracks IS NULL
            OR images_json IS NULL
            OR trim(COALESCE(images_json, '')) IN ('', '[]')
            OR json_extract(COALESCE(raw_json, '{{}}'), '$.label') IS NULL
            OR json_extract(COALESCE(raw_json, '{{}}'), '$.label') = ''
            OR json_array_length(COALESCE(json_extract(raw_json, '$.copyrights'), '[]')) = 0
          )
        UNION ALL
        SELECT 'candidate_album_queued_missing_tracklist', spotify_album_id, NULL
        FROM candidate_album_status
        WHERE queue_id IS NOT NULL
          AND (total_tracks IS NULL OR album_track_count < total_tracks)
        UNION ALL
        SELECT 'ambiguous_source_track_missing_from_queue', spotify_track_id, NULL
        FROM ambiguous_track_status
        WHERE queue_id IS NULL
        UNION ALL
        SELECT 'sibling_track_missing_from_queue', spotify_track_id, spotify_album_id
        FROM sibling_track_status
        WHERE queue_id IS NULL
        UNION ALL
        SELECT 'sibling_track_present_locally_not_queued', spotify_track_id, spotify_album_id
        FROM sibling_track_status
        WHERE queue_id IS NULL
          AND fetched_at IS NOT NULL
          AND lower(COALESCE(last_status, '')) != 'error'
        UNION ALL
        SELECT 'sibling_track_requiring_metadata', spotify_track_id, spotify_album_id
        FROM sibling_track_status
        WHERE fetched_at IS NULL
           OR lower(COALESCE(last_status, '')) = 'error'
           OR name IS NULL
           OR trim(COALESCE(name, '')) = ''
           OR duration_ms IS NULL
           OR explicit IS NULL
           OR disc_number IS NULL
           OR track_number IS NULL
           OR artists_json IS NULL
           OR trim(COALESCE(artists_json, '')) IN ('', '[]')
           OR json_extract(COALESCE(raw_json, '{{}}'), '$.external_ids.isrc') IS NULL
           OR json_extract(COALESCE(raw_json, '{{}}'), '$.external_ids.isrc') = ''
        UNION ALL
        SELECT 'tracklist_needed_before_sibling_tracks', spotify_album_id, NULL
        FROM candidate_album_status
        WHERE total_tracks IS NULL OR album_track_count < total_tracks
        ORDER BY bucket ASC, spotify_id ASC
    """
    plan_sql = f"""
        {ambiguous_ctes},
        duplicate_case_sources AS (
          SELECT sources.*
          FROM cases
          JOIN accepted_spotify_sources sources
            ON sources.spotify_track_id = cases.case_key
          WHERE cases.case_type = 'duplicate_spotify_track_id'
        ),
        split_case_sources AS (
          SELECT sources.*
          FROM cases
          JOIN accepted_spotify_sources sources
            ON sources.release_track_id = CAST(cases.case_key AS INTEGER)
          WHERE cases.case_type = 'release_track_source_split'
        ),
        ambiguous_sources AS (
          SELECT * FROM duplicate_case_sources
          UNION
          SELECT * FROM split_case_sources
        ),
        ambiguity_group_sources AS (
          SELECT
            case_type || ':' || case_key AS ambiguity_group_key,
            spotify_track_id,
            catalog_album_id
          FROM (
            SELECT cases.case_type, cases.case_key, sources.spotify_track_id, sources.catalog_album_id
            FROM cases
            JOIN accepted_spotify_sources sources
              ON sources.spotify_track_id = cases.case_key
            WHERE cases.case_type = 'duplicate_spotify_track_id'
            UNION
            SELECT cases.case_type, cases.case_key, sources.spotify_track_id, sources.catalog_album_id
            FROM cases
            JOIN accepted_spotify_sources sources
              ON sources.release_track_id = CAST(cases.case_key AS INTEGER)
            WHERE cases.case_type = 'release_track_source_split'
          )
        ),
        all_candidate_albums AS (
          SELECT DISTINCT ambiguity_group_key, catalog_album_id AS spotify_album_id
          FROM ambiguity_group_sources
          WHERE catalog_album_id IS NOT NULL
            AND trim(catalog_album_id) != ''
        ),
        focused_ambiguity_groups AS (
          SELECT DISTINCT ags.ambiguity_group_key
          FROM ambiguity_group_sources ags
          JOIN spotify_catalog_backfill_queue q
            ON q.entity_type = 'track'
           AND q.spotify_id = ags.spotify_track_id
          UNION
          SELECT DISTINCT aca.ambiguity_group_key
          FROM all_candidate_albums aca
          JOIN spotify_catalog_backfill_queue q
            ON q.entity_type = 'album'
           AND q.spotify_id = aca.spotify_album_id
        ),
        candidate_albums AS (
          SELECT DISTINCT aca.spotify_album_id
          FROM all_candidate_albums aca
          JOIN focused_ambiguity_groups fg
            ON fg.ambiguity_group_key = aca.ambiguity_group_key
        ),
        candidate_album_status AS (
          SELECT
            ca.spotify_album_id,
            q.id AS queue_id,
            q.status AS queue_status,
            sac.total_tracks,
            COALESCE(atc.album_track_count, 0) AS album_track_count,
            CASE
              WHEN sac.total_tracks IS NOT NULL
               AND COALESCE(atc.album_track_count, 0) > 0
               AND COALESCE(atc.album_track_count, 0) >= sac.total_tracks
              THEN 1
              ELSE 0
            END AS tracklist_verified
          FROM candidate_albums ca
          LEFT JOIN spotify_catalog_backfill_queue q
            ON q.entity_type = 'album'
           AND q.spotify_id = ca.spotify_album_id
          LEFT JOIN spotify_album_catalog sac
            ON sac.spotify_album_id = ca.spotify_album_id
          LEFT JOIN (
            SELECT spotify_album_id, count(*) AS album_track_count
            FROM spotify_album_track
            GROUP BY spotify_album_id
          ) atc
            ON atc.spotify_album_id = ca.spotify_album_id
        ),
        sibling_track_status AS (
          SELECT
            sat.spotify_album_id,
            sat.spotify_track_id,
            q.id AS queue_id,
            q.status AS queue_status,
            stc.name,
            stc.duration_ms,
            stc.explicit,
            stc.disc_number,
            stc.track_number,
            stc.artists_json,
            stc.raw_json,
            stc.fetched_at,
            stc.last_status
          FROM spotify_album_track sat
          JOIN candidate_albums ca
            ON ca.spotify_album_id = sat.spotify_album_id
          LEFT JOIN spotify_catalog_backfill_queue q
            ON q.entity_type = 'track'
           AND q.spotify_id = sat.spotify_track_id
          LEFT JOIN spotify_track_catalog stc
            ON stc.spotify_track_id = sat.spotify_track_id
        ),
        ambiguous_track_status AS (
          SELECT DISTINCT
            sources.spotify_track_id,
            q.id AS queue_id,
            q.status AS queue_status
          FROM ambiguous_sources sources
          LEFT JOIN spotify_catalog_backfill_queue q
            ON q.entity_type = 'track'
           AND q.spotify_id = sources.spotify_track_id
        )
        SELECT
          0 AS sort_order,
          'album_tracklist' AS planned_target,
          'album' AS entity_type,
          spotify_album_id AS spotify_id,
          NULL AS parent_album_id,
          CASE
            WHEN queue_status = 'pending' THEN 'already_queued_pending'
            WHEN queue_status = 'done' AND tracklist_verified = 1 THEN 'already_queued_done'
            WHEN queue_status = 'done' THEN 'done_but_tracklist_incomplete'
            WHEN queue_id IS NULL THEN 'should_append_later'
            ELSE 'should_append_later'
          END AS plan_status,
          'resolution_evidence' AS suggested_reason,
          'candidate album tracklist is needed before sibling-track evidence can be completed' AS rationale
        FROM candidate_album_status
        WHERE tracklist_verified = 0
        UNION ALL
        SELECT
          1,
          'track_metadata',
          'track',
          spotify_track_id,
          NULL,
          CASE
            WHEN queue_status = 'pending' THEN 'already_queued_pending'
            WHEN queue_status = 'done' THEN 'already_queued_done'
            WHEN queue_id IS NULL THEN 'should_append_later'
            ELSE 'should_append_later'
          END,
          'resolution_evidence',
          'ambiguous source track is missing from the current queue'
        FROM ambiguous_track_status
        WHERE queue_id IS NULL
        UNION ALL
        SELECT
          2,
          'track_metadata',
          'track',
          spotify_track_id,
          spotify_album_id,
          CASE
            WHEN queue_status = 'pending' THEN 'already_queued_pending'
            WHEN queue_status = 'done' THEN 'already_queued_done'
            WHEN queue_id IS NULL THEN 'should_append_later'
            ELSE 'should_append_later'
          END,
          'resolution_evidence',
          'sibling track from candidate album needs metadata for album-shape comparison'
        FROM sibling_track_status
        WHERE fetched_at IS NULL
           OR lower(COALESCE(last_status, '')) = 'error'
           OR name IS NULL
           OR trim(COALESCE(name, '')) = ''
           OR duration_ms IS NULL
           OR explicit IS NULL
           OR disc_number IS NULL
           OR track_number IS NULL
           OR artists_json IS NULL
           OR trim(COALESCE(artists_json, '')) IN ('', '[]')
           OR json_extract(COALESCE(raw_json, '{{}}'), '$.external_ids.isrc') IS NULL
           OR json_extract(COALESCE(raw_json, '{{}}'), '$.external_ids.isrc') = ''
        UNION ALL
        SELECT
          3,
          'album_tracklist',
          'album',
          spotify_album_id,
          NULL,
          CASE
            WHEN queue_status = 'pending' THEN 'tracklist_pending'
            WHEN queue_status = 'done' THEN 'done_but_tracklist_incomplete'
            WHEN queue_id IS NULL THEN 'tracklist_missing'
            ELSE 'blocked_until_tracklist_exists'
          END,
          'resolution_evidence',
          'candidate album tracklist must be fetched before sibling tracks can be enumerated'
        FROM candidate_album_status
        WHERE tracklist_verified = 0
        ORDER BY sort_order ASC, spotify_id ASC
    """
    plan_guardrail_sql = f"""
        {ambiguous_ctes},
        ambiguity_group_sources AS (
          SELECT
            case_type || ':' || case_key AS ambiguity_group_key,
            spotify_track_id,
            catalog_album_id
          FROM (
            SELECT cases.case_type, cases.case_key, sources.spotify_track_id, sources.catalog_album_id
            FROM cases
            JOIN accepted_spotify_sources sources
              ON sources.spotify_track_id = cases.case_key
            WHERE cases.case_type = 'duplicate_spotify_track_id'
            UNION
            SELECT cases.case_type, cases.case_key, sources.spotify_track_id, sources.catalog_album_id
            FROM cases
            JOIN accepted_spotify_sources sources
              ON sources.release_track_id = CAST(cases.case_key AS INTEGER)
            WHERE cases.case_type = 'release_track_source_split'
          )
        ),
        all_candidate_albums AS (
          SELECT DISTINCT ambiguity_group_key, catalog_album_id AS spotify_album_id
          FROM ambiguity_group_sources
          WHERE catalog_album_id IS NOT NULL
            AND trim(catalog_album_id) != ''
        ),
        focused_ambiguity_groups AS (
          SELECT DISTINCT ags.ambiguity_group_key
          FROM ambiguity_group_sources ags
          JOIN spotify_catalog_backfill_queue q
            ON q.entity_type = 'track'
           AND q.spotify_id = ags.spotify_track_id
          UNION
          SELECT DISTINCT aca.ambiguity_group_key
          FROM all_candidate_albums aca
          JOIN spotify_catalog_backfill_queue q
            ON q.entity_type = 'album'
           AND q.spotify_id = aca.spotify_album_id
        ),
        candidate_albums AS (
          SELECT DISTINCT aca.spotify_album_id
          FROM all_candidate_albums aca
          JOIN focused_ambiguity_groups fg
            ON fg.ambiguity_group_key = aca.ambiguity_group_key
        ),
        candidate_album_status AS (
          SELECT
            ca.spotify_album_id,
            sac.total_tracks,
            COALESCE(atc.album_track_count, 0) AS album_track_count,
            CASE
              WHEN sac.total_tracks IS NOT NULL
               AND COALESCE(atc.album_track_count, 0) > 0
               AND COALESCE(atc.album_track_count, 0) >= sac.total_tracks
              THEN 1
              ELSE 0
            END AS tracklist_verified
          FROM candidate_albums ca
          LEFT JOIN spotify_album_catalog sac
            ON sac.spotify_album_id = ca.spotify_album_id
          LEFT JOIN (
            SELECT spotify_album_id, count(*) AS album_track_count
            FROM spotify_album_track
            GROUP BY spotify_album_id
          ) atc
            ON atc.spotify_album_id = ca.spotify_album_id
        ),
        sibling_track_status AS (
          SELECT DISTINCT sat.spotify_track_id
          FROM spotify_album_track sat
          JOIN candidate_albums ca
            ON ca.spotify_album_id = sat.spotify_album_id
        ),
        broad_incomplete_albums AS (
          SELECT sac.spotify_album_id
          FROM spotify_album_catalog sac
          LEFT JOIN (
            SELECT spotify_album_id, count(*) AS album_track_count
            FROM spotify_album_track
            GROUP BY spotify_album_id
          ) atc
            ON atc.spotify_album_id = sac.spotify_album_id
          WHERE sac.total_tracks IS NULL OR COALESCE(atc.album_track_count, 0) < sac.total_tracks
        )
        SELECT
          (SELECT count(*) FROM focused_ambiguity_groups) AS ambiguity_group_count,
          (SELECT count(*) FROM candidate_albums) AS candidate_album_count,
          (SELECT count(*) FROM candidate_album_status WHERE tracklist_verified = 0)
            AS candidate_album_tracklist_missing_count,
          (SELECT count(*) FROM broad_incomplete_albums) AS broad_incomplete_album_tracklist_count,
          (SELECT count(*) FROM sibling_track_status) AS actual_sibling_track_count
    """

    with sqlite_connection() as connection:
        rows = connection.execute(report_sql).fetchall()
        delta_rows = connection.execute(delta_sql).fetchall()
        plan_rows = connection.execute(plan_sql).fetchall()
        plan_guardrail_row = connection.execute(plan_guardrail_sql).fetchone()

    snapshot = {
        "total_queued_items": len(rows),
        "oldest_requested_at": None,
        "newest_requested_at": None,
        "counts_by_status": {},
        "counts_by_reason": {},
        "counts_by_entity_type": {},
        "counts_by_inferred_target": {},
    }
    relevance_counts = {
        "directly_relevant": 0,
        "possibly_relevant": 0,
        "generic_catalog_backfill": 0,
        "stale_or_blocked": 0,
        "unknown": 0,
    }
    relevance_counts_by_status: dict[str, int] = {}
    unknown_reason_counts: dict[str, int] = {}
    evidence_coverage = {
        "queued_candidate_albums": 0,
        "queued_sibling_tracks": 0,
        "queued_ambiguous_source_tracks": 0,
        "album_metadata_gaps": 0,
        "album_tracklist_gaps": 0,
        "source_track_metadata_gaps": 0,
        "track_field_gaps": {
            "isrc": 0,
            "duration_ms": 0,
            "explicit": 0,
            "artists": 0,
            "track_name": 0,
            "disc_number": 0,
            "track_number": 0,
        },
        "album_field_gaps": {
            "images": 0,
            "copyrights": 0,
            "label": 0,
            "release_date": 0,
            "total_tracks": 0,
        },
    }
    sample_items: list[dict[str, Any]] = []
    queue_items: list[dict[str, Any]] = []
    directly_relevant_pending_samples: list[dict[str, Any]] = []
    unknown_pending_samples_by_reason: dict[str, list[dict[str, Any]]] = {}
    requested_values: list[str] = []

    for row in rows:
        (
            queue_id,
            entity_type,
            spotify_id,
            reason,
            priority,
            status,
            requested_at,
            last_attempted_at,
            attempts,
            last_error,
            inferred_target,
            relevance_bucket,
            is_ambiguous_source_track,
            is_candidate_album,
            is_sibling_track,
            unknown_reason,
            track_name,
            duration_ms,
            explicit,
            disc_number,
            track_number,
            track_album_id,
            track_artists_json,
            track_raw_json,
            track_fetched_at,
            track_last_status,
            album_name,
            album_release_date,
            album_total_tracks,
            album_images_json,
            album_raw_json,
            album_fetched_at,
            album_last_status,
            album_track_count,
        ) = row
        status_key = str(status or "unknown")
        reason_key = str(reason or "none")
        entity_key = str(entity_type or "unknown")
        target_key = str(inferred_target or "unknown")
        bucket_key = str(relevance_bucket or "unknown")
        if bucket_key not in relevance_counts:
            bucket_key = "unknown"

        snapshot["counts_by_status"][status_key] = snapshot["counts_by_status"].get(status_key, 0) + 1
        snapshot["counts_by_reason"][reason_key] = snapshot["counts_by_reason"].get(reason_key, 0) + 1
        snapshot["counts_by_entity_type"][entity_key] = snapshot["counts_by_entity_type"].get(entity_key, 0) + 1
        snapshot["counts_by_inferred_target"][target_key] = snapshot["counts_by_inferred_target"].get(target_key, 0) + 1
        relevance_counts[bucket_key] += 1
        relevance_counts_by_status[f"{bucket_key}_{status_key}"] = (
            relevance_counts_by_status.get(f"{bucket_key}_{status_key}", 0) + 1
        )
        if requested_at:
            requested_values.append(str(requested_at))

        if is_candidate_album:
            evidence_coverage["queued_candidate_albums"] += 1
        if is_sibling_track:
            evidence_coverage["queued_sibling_tracks"] += 1
        if is_ambiguous_source_track:
            evidence_coverage["queued_ambiguous_source_tracks"] += 1

        track_gaps: list[str] = []
        if entity_key == "track" and (is_ambiguous_source_track or is_sibling_track):
            track_raw = _json_object(track_raw_json)
            external_ids = track_raw.get("external_ids") if isinstance(track_raw.get("external_ids"), dict) else {}
            if not str(track_name or "").strip():
                track_gaps.append("track_name")
            if duration_ms is None:
                track_gaps.append("duration_ms")
            if explicit is None:
                track_gaps.append("explicit")
            if track_artists_json is None or str(track_artists_json).strip() in {"", "[]"}:
                track_gaps.append("artists")
            if disc_number is None:
                track_gaps.append("disc_number")
            if track_number is None:
                track_gaps.append("track_number")
            if not str(external_ids.get("isrc") or "").strip():
                track_gaps.append("isrc")
            if track_gaps:
                evidence_coverage["source_track_metadata_gaps"] += 1
                for gap in track_gaps:
                    evidence_coverage["track_field_gaps"][gap] += 1

        album_gaps: list[str] = []
        if entity_key == "album" and is_candidate_album:
            album_raw = _json_object(album_raw_json)
            copyrights = album_raw.get("copyrights")
            if not str(album_release_date or "").strip():
                album_gaps.append("release_date")
            if album_total_tracks is None:
                album_gaps.append("total_tracks")
            if album_images_json is None or str(album_images_json).strip() in {"", "[]"}:
                album_gaps.append("images")
            if not isinstance(copyrights, list) or len(copyrights) == 0:
                album_gaps.append("copyrights")
            if not str(album_raw.get("label") or "").strip():
                album_gaps.append("label")
            if album_gaps or album_fetched_at is None or str(album_last_status or "").lower() == "error":
                evidence_coverage["album_metadata_gaps"] += 1
                for gap in album_gaps:
                    evidence_coverage["album_field_gaps"][gap] += 1
            if album_total_tracks is None or int(album_track_count or 0) < int(album_total_tracks or 0):
                evidence_coverage["album_tracklist_gaps"] += 1

        unknown_reason_key = None
        if bucket_key == "unknown":
            unknown_reason_key = str(unknown_reason or "unknown")
            unknown_reason_counts[unknown_reason_key] = unknown_reason_counts.get(unknown_reason_key, 0) + 1

        compact_item = {
            "id": int(queue_id),
            "entity_type": entity_key,
            "spotify_id": str(spotify_id),
            "reason": reason,
            "status": status_key,
            "relevance_bucket": bucket_key,
            "inferred_target": target_key,
            "unknown_reason": unknown_reason_key,
            "is_ambiguous_source_track": bool(is_ambiguous_source_track),
            "is_candidate_album": bool(is_candidate_album),
            "is_sibling_track": bool(is_sibling_track),
            "track_gaps": track_gaps,
            "album_gaps": album_gaps,
            "last_error": last_error,
            "attempts": int(attempts or 0),
            "requested_at": requested_at,
            "last_attempted_at": last_attempted_at,
            "priority": int(priority or 0),
            "track_album_id": track_album_id,
            "album_track_count": int(album_track_count or 0) if entity_key == "album" else None,
            "album_total_tracks": int(album_total_tracks) if album_total_tracks is not None else None,
        }
        queue_items.append(compact_item)
        if bucket_key == "directly_relevant" and status_key == "pending" and len(directly_relevant_pending_samples) < 10:
            directly_relevant_pending_samples.append(compact_item)
        if bucket_key == "unknown" and status_key == "pending" and unknown_reason_key:
            reason_samples = unknown_pending_samples_by_reason.setdefault(unknown_reason_key, [])
            if len(reason_samples) < 5:
                reason_samples.append(compact_item)
        if len(sample_items) < 25 and bucket_key != "generic_catalog_backfill":
            sample_items.append(compact_item)

    if requested_values:
        snapshot["oldest_requested_at"] = min(requested_values)
        snapshot["newest_requested_at"] = max(requested_values)
    for bucket in relevance_counts:
        for status in ("pending", "done", "error"):
            relevance_counts_by_status.setdefault(f"{bucket}_{status}", 0)

    delta_counts = {
        "candidate_albums_missing_from_queue": 0,
        "candidate_albums_queued_but_missing_album_metadata": 0,
        "candidate_albums_queued_but_missing_tracklists": 0,
        "ambiguous_source_tracks_missing_from_queue": 0,
        "sibling_tracks_missing_from_queue": 0,
        "sibling_tracks_already_present_locally_but_not_queued": 0,
        "sibling_tracks_requiring_metadata": 0,
        "tracklists_needed_before_sibling_tracks_can_be_enumerated": 0,
    }
    delta_bucket_map = {
        "candidate_album_missing_from_queue": "candidate_albums_missing_from_queue",
        "candidate_album_queued_missing_metadata": "candidate_albums_queued_but_missing_album_metadata",
        "candidate_album_queued_missing_tracklist": "candidate_albums_queued_but_missing_tracklists",
        "ambiguous_source_track_missing_from_queue": "ambiguous_source_tracks_missing_from_queue",
        "sibling_track_missing_from_queue": "sibling_tracks_missing_from_queue",
        "sibling_track_present_locally_not_queued": "sibling_tracks_already_present_locally_but_not_queued",
        "sibling_track_requiring_metadata": "sibling_tracks_requiring_metadata",
        "tracklist_needed_before_sibling_tracks": "tracklists_needed_before_sibling_tracks_can_be_enumerated",
    }
    delta_samples: dict[str, list[dict[str, Any]]] = {value: [] for value in delta_counts}
    seen_delta_ids: dict[str, set[tuple[str, str | None]]] = {value: set() for value in delta_counts}
    for bucket, spotify_id, parent_album_id in delta_rows:
        target_bucket = delta_bucket_map.get(str(bucket))
        if target_bucket is None:
            continue
        key = (str(spotify_id), str(parent_album_id) if parent_album_id is not None else None)
        if key in seen_delta_ids[target_bucket]:
            continue
        seen_delta_ids[target_bucket].add(key)
        delta_counts[target_bucket] += 1
        if len(delta_samples[target_bucket]) < 10:
            delta_samples[target_bucket].append(
                {
                    "spotify_id": str(spotify_id),
                    "parent_album_id": str(parent_album_id) if parent_album_id is not None else None,
                }
            )

    plan_items: list[dict[str, Any]] = []
    candidate_album_tracklist_items: list[dict[str, Any]] = []
    actual_sibling_track_items: list[dict[str, Any]] = []
    blocked_sibling_collection_prerequisites: list[dict[str, Any]] = []
    plan_counts = {
        "already_queued_pending": 0,
        "already_queued_done": 0,
        "should_append_later": 0,
        "blocked_until_tracklist_exists": 0,
        "done_but_tracklist_incomplete": 0,
        "tracklist_complete": 0,
        "tracklist_pending": 0,
        "tracklist_missing": 0,
    }
    seen_plan_ids: set[tuple[int, str, str, str, str | None, str]] = set()
    blocked_prerequisite_statuses = {
        "blocked_until_tracklist_exists",
        "done_but_tracklist_incomplete",
        "tracklist_pending",
        "tracklist_missing",
    }
    for sort_order, planned_target, entity_type, spotify_id, parent_album_id, plan_status, suggested_reason, rationale in plan_rows:
        status_key = str(plan_status or "should_append_later")
        if status_key not in plan_counts:
            status_key = "should_append_later"
        key = (
            int(sort_order or 0),
            str(planned_target),
            str(entity_type),
            str(spotify_id),
            str(parent_album_id) if parent_album_id is not None else None,
            status_key,
        )
        if key in seen_plan_ids:
            continue
        seen_plan_ids.add(key)
        if str(entity_type) == "track" and parent_album_id is not None and str(spotify_id) == str(parent_album_id):
            raise AssertionError("Dry-run plan emitted a track item using an album Spotify ID.")
        if status_key in blocked_prerequisite_statuses and str(entity_type) == "track":
            raise AssertionError("Blocked sibling collection prerequisites must not be emitted as track items.")
        plan_counts[status_key] += 1
        plan_item = {
            "planned_target": str(planned_target),
            "entity_type": str(entity_type),
            "spotify_id": str(spotify_id),
            "parent_album_id": str(parent_album_id) if parent_album_id is not None else None,
            "plan_status": status_key,
            "suggested_reason": str(suggested_reason),
            "rationale": str(rationale),
        }
        plan_items.append(plan_item)
        if int(sort_order or 0) == 3:
            blocked_sibling_collection_prerequisites.append(plan_item)
        elif plan_item["planned_target"] == "album_tracklist":
            candidate_album_tracklist_items.append(plan_item)
        elif plan_item["planned_target"] == "track_metadata" and plan_item["parent_album_id"] is not None:
            actual_sibling_track_items.append(plan_item)

    plan_source_set_counts = {
        "ambiguity_group_count": int(plan_guardrail_row[0] or 0) if plan_guardrail_row else 0,
        "candidate_album_count": int(plan_guardrail_row[1] or 0) if plan_guardrail_row else 0,
        "candidate_album_tracklist_missing_count": int(plan_guardrail_row[2] or 0) if plan_guardrail_row else 0,
        "broad_incomplete_album_tracklist_count": int(plan_guardrail_row[3] or 0) if plan_guardrail_row else 0,
        "actual_sibling_track_count": int(plan_guardrail_row[4] or 0) if plan_guardrail_row else 0,
    }

    direct = relevance_counts["directly_relevant"]
    direct_pending = relevance_counts_by_status.get("directly_relevant_pending", 0)
    possible = relevance_counts["possibly_relevant"]
    generic = relevance_counts["generic_catalog_backfill"]
    generic_pending = relevance_counts_by_status.get("generic_catalog_backfill_pending", 0)
    stale = relevance_counts["stale_or_blocked"]
    unknown = relevance_counts["unknown"]
    unknown_pending = relevance_counts_by_status.get("unknown_pending", 0)

    recommendation_steps: list[str] = []
    if direct_pending:
        recommendation_steps.append("preserve_current_queue")
    if delta_counts["candidate_albums_queued_but_missing_tracklists"] or delta_counts[
        "tracklists_needed_before_sibling_tracks_can_be_enumerated"
    ]:
        recommendation_steps.append("let_directly_relevant_pending_candidate_albums_or_tracklists_complete_first")
        recommendation_steps.append("complete_candidate_album_tracklists_before_sibling_track_collection")
    elif evidence_coverage["queued_sibling_tracks"] == 0 and delta_counts["sibling_tracks_missing_from_queue"]:
        recommendation_steps.append("append_focused_sibling_track_resolution_evidence_later")
    if unknown_pending and unknown_pending > direct_pending and generic_pending + unknown_pending > direct_pending:
        recommendation_steps.append("review_legacy_or_non_resolution_pending_items_after_snapshot")

    if stale and direct_pending == 0 and generic + unknown > direct + possible:
        action = "needs_manual_review"
        rationale = f"{stale} queued item(s) are in error status; review failures before changing the queue."
    elif direct_pending:
        action = "preserve_current_queue"
        rationale = f"{direct_pending} directly relevant pending item(s) should be preserved for resolution evidence."
    elif direct + possible == 0 and generic > 0 and generic > unknown:
        action = "clear_and_replace_later"
        rationale = "No queued items currently map to resolution-evidence candidates; prepare a focused batch after review."
    elif generic_pending > direct + possible:
        action = "append_focused_resolution_evidence_items_later"
        rationale = (
            f"{direct + possible} relevant or possibly relevant item(s) exist, but {generic_pending} pending item(s) look generic."
        )
    elif unknown_pending:
        action = "needs_manual_review"
        rationale = f"{unknown_pending} pending queued item(s) need an unknown-reason review before queue changes."
    else:
        action = "preserve_current_queue"
        rationale = f"{direct + possible} queued item(s) are relevant or possibly relevant to resolution evidence."

    return {
        "ok": True,
        "mode": "read_only",
        "queue_snapshot": snapshot,
        "queue_items": queue_items,
        "resolution_relevance": {
            "bucket_counts": relevance_counts,
            "bucket_counts_by_status": relevance_counts_by_status,
            "unknown_reason_counts": unknown_reason_counts,
            "unknown_reason_definitions": {
                "not_candidate_album": "Album queue item is not one of the candidate albums connected to accepted source-track ambiguity groups.",
                "not_ambiguous_source_track": "Track queue item is not one of the ambiguous source tracks.",
                "not_sibling_track": "Track queue item is not currently known as a sibling track from a candidate album.",
                "legacy_album_lookup_visible_incomplete": "Album queue item appears to come from an older visible/incomplete lookup flow rather than resolution evidence.",
                "already_done_but_not_resolution_related": "Queue item is complete and does not appear tied to resolution evidence.",
                "pending_but_not_resolution_related": "Queue item is pending but does not match resolution-evidence links or known broad reasons.",
                "insufficient_local_links": "Local joins are not enough to explain the queue item.",
                "missing_join_metadata": "Expected local metadata was unavailable for classification.",
                "unrecognized_reason": "Queue reason does not match known reason patterns.",
                "unrecognized_entity_type": "Queue entity type is not recognized by this report.",
            },
            "bucket_definitions": {
                "directly_relevant": "Queued track/album is part of an accepted source-track ambiguity group or its candidate album set.",
                "possibly_relevant": "Queued item has identity metadata reason or is a sibling track from a candidate album.",
                "generic_catalog_backfill": "Queued item appears to be general catalog work, not specifically resolution evidence.",
                "stale_or_blocked": "Queued item is in error status.",
                "unknown": "Queued item lacks enough local metadata or recognized reason/type to classify.",
            },
        },
        "evidence_coverage_hints": evidence_coverage,
        "resolution_evidence_delta": {
            "counts": delta_counts,
            "samples": delta_samples,
        },
        "dry_run_resolution_evidence_plan": {
            "mode": "dry_run",
            "performed_action": "none",
            "suggested_reason": "resolution_evidence",
            "source_set_counts": plan_source_set_counts,
            "counts_by_plan_status": plan_counts,
            "candidate_album_tracklist_items": candidate_album_tracklist_items,
            "actual_sibling_track_items": actual_sibling_track_items,
            "blocked_sibling_collection_prerequisites": blocked_sibling_collection_prerequisites,
            "items": plan_items,
        },
        "sample_non_generic_items": sample_items,
        "samples": {
            "directly_relevant_pending_items": directly_relevant_pending_samples,
            "unknown_pending_items_by_unknown_reason": unknown_pending_samples_by_reason,
            "missing_candidate_album_tracklists": delta_samples[
                "candidate_albums_queued_but_missing_tracklists"
            ],
            "missing_sibling_track_evidence": delta_samples["sibling_tracks_requiring_metadata"],
        },
        "safety_recommendation": {
            "action": action,
            "rationale": rationale,
            "recommended_steps": recommendation_steps,
            "counts": {
                "directly_relevant": direct,
                "directly_relevant_pending": direct_pending,
                "possibly_relevant": possible,
                "generic_catalog_backfill": generic,
                "generic_catalog_backfill_pending": generic_pending,
                "stale_or_blocked": stale,
                "unknown": unknown,
                "unknown_pending": unknown_pending,
            },
            "allowed_next_actions": [
                "preserve current queue",
                "pause current queue",
                "clear and replace later",
                "append focused resolution_evidence items later",
                "manual review",
            ],
            "performed_action": "none",
        },
    }


def append_resolution_evidence_candidate_tracklists_from_report(
    *,
    report: dict[str, Any],
    apply: bool = False,
    priority: int = 80,
) -> dict[str, Any]:
    plan = report.get("dry_run_resolution_evidence_plan") if isinstance(report, dict) else None
    raw_items = plan.get("candidate_album_tracklist_items") if isinstance(plan, dict) else []
    selected_items: list[dict[str, Any]] = []
    seen_album_ids: set[str] = set()
    for raw_item in raw_items if isinstance(raw_items, list) else []:
        if not isinstance(raw_item, dict):
            continue
        spotify_id = str(raw_item.get("spotify_id") or "").strip()
        if (
            raw_item.get("planned_target") != "album_tracklist"
            or raw_item.get("plan_status") != "should_append_later"
            or raw_item.get("entity_type") != "album"
            or raw_item.get("suggested_reason") != "resolution_evidence"
            or not spotify_id
            or spotify_id in seen_album_ids
        ):
            continue
        seen_album_ids.add(spotify_id)
        selected_items.append(
            {
                "entity_type": "album",
                "spotify_id": spotify_id,
                "reason": "resolution_evidence",
                "priority": int(priority),
                "status": "pending",
                "planned_target": "album_tracklist",
                "rationale": raw_item.get("rationale"),
            }
        )

    result = {
        "ok": True,
        "mode": "apply" if apply else "dry_run",
        "performed_action": "inserted_queue_rows" if apply else "none",
        "snapshot_recommendation": (
            "Before applying, run the same report with "
            "--export-json data/queue-snapshots/spotify-catalog-queue-$(date +%Y%m%d-%H%M%S).json"
        ),
        "selection_rule": {
            "section": "candidate_album_tracklist_items",
            "plan_status": "should_append_later",
            "entity_type": "album",
            "planned_target": "album_tracklist",
            "suggested_reason": "resolution_evidence",
        },
        "priority": int(priority),
        "selected_count": len(selected_items),
        "selected_items": selected_items,
        "inserted": 0,
        "already_existing": 0,
        "skipped": 0,
    }
    if not apply or not selected_items:
        return result

    with sqlite_connection(write=True) as connection:
        for item in selected_items:
            before = connection.total_changes
            connection.execute(
                """
                INSERT OR IGNORE INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES ('album', ?, 'resolution_evidence', ?, 'pending', ?, 0)
                """,
                (item["spotify_id"], int(priority), _utc_now()),
            )
            if connection.total_changes > before:
                result["inserted"] += 1
            else:
                result["already_existing"] += 1
    return result


def append_resolution_evidence_sibling_tracks_from_report(
    *,
    report: dict[str, Any],
    apply: bool = False,
    priority: int = 80,
) -> dict[str, Any]:
    plan = report.get("dry_run_resolution_evidence_plan") if isinstance(report, dict) else None
    raw_items = plan.get("actual_sibling_track_items") if isinstance(plan, dict) else []
    selected_items: list[dict[str, Any]] = []
    seen_track_ids: set[str] = set()
    for raw_item in raw_items if isinstance(raw_items, list) else []:
        if not isinstance(raw_item, dict):
            continue
        spotify_id = str(raw_item.get("spotify_id") or "").strip()
        suggested_reason = raw_item.get("suggested_reason")
        if (
            raw_item.get("planned_target") != "track_metadata"
            or raw_item.get("plan_status") != "should_append_later"
            or raw_item.get("entity_type") != "track"
            or (suggested_reason is not None and suggested_reason != "resolution_evidence")
            or not spotify_id
            or spotify_id in seen_track_ids
        ):
            continue
        seen_track_ids.add(spotify_id)
        selected_items.append(
            {
                "entity_type": "track",
                "spotify_id": spotify_id,
                "reason": "resolution_evidence",
                "priority": int(priority),
                "status": "pending",
                "planned_target": "track_metadata",
                "parent_album_id": raw_item.get("parent_album_id"),
                "rationale": raw_item.get("rationale"),
            }
        )

    result = {
        "ok": True,
        "mode": "apply" if apply else "dry_run",
        "performed_action": "inserted_queue_rows" if apply else "none",
        "snapshot_recommendation": (
            "Before applying, run the same report with "
            "--export-json data/queue-snapshots/spotify-catalog-queue-$(date +%Y%m%d-%H%M%S).json"
        ),
        "selection_rule": {
            "section": "actual_sibling_track_items",
            "plan_status": "should_append_later",
            "entity_type": "track",
            "planned_target": "track_metadata",
            "suggested_reason": "resolution_evidence",
        },
        "priority": int(priority),
        "selected_count": len(selected_items),
        "selected_items": selected_items,
        "inserted": 0,
        "already_existing": 0,
        "skipped": 0,
    }
    result["appendability_diagnostic"] = _resolution_evidence_sibling_appendability_diagnostic(
        report=report,
        selected_items=selected_items,
    )
    if not apply or not selected_items:
        return result

    with sqlite_connection(write=True) as connection:
        for item in selected_items:
            before = connection.total_changes
            connection.execute(
                """
                INSERT OR IGNORE INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES ('track', ?, 'resolution_evidence', ?, 'pending', ?, 0)
                """,
                (item["spotify_id"], int(priority), _utc_now()),
            )
            if connection.total_changes > before:
                result["inserted"] += 1
            else:
                result["already_existing"] += 1
    return result


def _resolution_evidence_sibling_appendability_diagnostic(
    *,
    report: dict[str, Any],
    selected_items: list[dict[str, Any]],
    sample_limit: int = 10,
) -> dict[str, Any]:
    plan = report.get("dry_run_resolution_evidence_plan") if isinstance(report, dict) else {}
    delta = report.get("resolution_evidence_delta") if isinstance(report, dict) else {}
    delta_counts = delta.get("counts", {}) if isinstance(delta, dict) else {}
    delta_samples = delta.get("samples", {}) if isinstance(delta, dict) else {}
    raw_items = plan.get("actual_sibling_track_items") if isinstance(plan, dict) else []
    actual_items = [item for item in raw_items if isinstance(item, dict)]
    selected_keys = {
        (str(item.get("spotify_id") or ""), str(item.get("parent_album_id") or ""))
        for item in selected_items
        if isinstance(item, dict)
    }

    plan_status_counts: dict[str, int] = {}
    exclusion_counts: dict[str, int] = {}
    actual_item_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    seen_track_ids: set[str] = set()
    for item in actual_items:
        status_key = str(item.get("plan_status") or "unknown")
        plan_status_counts[status_key] = plan_status_counts.get(status_key, 0) + 1
        spotify_id = str(item.get("spotify_id") or "").strip()
        parent_album_id = str(item.get("parent_album_id") or "").strip()
        actual_item_by_key[(spotify_id, parent_album_id)] = item
        reason = _resolution_evidence_sibling_append_exclusion_reason(item, seen_track_ids=seen_track_ids)
        if reason == "appendable":
            seen_track_ids.add(spotify_id)
        exclusion_counts[reason] = exclusion_counts.get(reason, 0) + 1

    requiring_metadata_count = int(delta_counts.get("sibling_tracks_requiring_metadata") or 0)
    missing_from_queue_count = int(delta_counts.get("sibling_tracks_missing_from_queue") or 0)
    actual_count = len(actual_items)
    diagnostic: dict[str, Any] = {
        "source_counts": {
            "sibling_tracks_requiring_metadata_count": requiring_metadata_count,
            "sibling_tracks_missing_from_queue_count": missing_from_queue_count,
            "sibling_tracks_already_present_locally_but_not_queued_count": int(
                delta_counts.get("sibling_tracks_already_present_locally_but_not_queued") or 0
            ),
            "tracklists_needed_before_sibling_tracks_can_be_enumerated_count": int(
                delta_counts.get("tracklists_needed_before_sibling_tracks_can_be_enumerated") or 0
            ),
            "actual_sibling_track_items_count": actual_count,
            "append_selected_count": len(selected_items),
        },
        "actual_sibling_track_items_by_plan_status": plan_status_counts,
        "append_exclusion_counts": exclusion_counts,
        "broad_delta_not_in_focused_append_plan_count": max(0, requiring_metadata_count - actual_count),
        "selection_note": (
            "Sibling append selects only dry_run_resolution_evidence_plan.actual_sibling_track_items "
            "with plan_status=should_append_later. Broad delta counts can include sibling evidence outside "
            "that focused append plan, especially when candidate album tracklists still need to be enumerated."
        ),
    }

    samples: dict[str, list[dict[str, Any]]] = {}
    requiring_samples = delta_samples.get("sibling_tracks_requiring_metadata", []) if isinstance(delta_samples, dict) else []
    missing_samples = delta_samples.get("sibling_tracks_missing_from_queue", []) if isinstance(delta_samples, dict) else []
    samples["sibling_tracks_requiring_metadata"] = _annotate_sibling_appendability_samples(
        requiring_samples,
        actual_item_by_key=actual_item_by_key,
        selected_keys=selected_keys,
        default_reason="not_in_focused_append_plan",
        sample_limit=sample_limit,
    )
    samples["sibling_tracks_missing_from_queue"] = _annotate_sibling_appendability_samples(
        missing_samples,
        actual_item_by_key=actual_item_by_key,
        selected_keys=selected_keys,
        default_reason="not_in_focused_append_plan_or_already_has_metadata",
        sample_limit=sample_limit,
    )
    diagnostic["samples"] = samples
    return diagnostic


def _resolution_evidence_sibling_append_exclusion_reason(
    item: dict[str, Any],
    *,
    seen_track_ids: set[str],
) -> str:
    spotify_id = str(item.get("spotify_id") or "").strip()
    suggested_reason = item.get("suggested_reason")
    if not spotify_id:
        return "missing_spotify_track_id"
    if item.get("planned_target") != "track_metadata":
        return "planned_target_filter_mismatch"
    if item.get("entity_type") != "track":
        return "entity_type_filter_mismatch"
    if suggested_reason is not None and suggested_reason != "resolution_evidence":
        return "suggested_reason_filter_mismatch"
    plan_status = str(item.get("plan_status") or "")
    if plan_status != "should_append_later":
        return plan_status or "plan_status_filter_mismatch"
    if spotify_id in seen_track_ids:
        return "duplicate_spotify_track_id"
    return "appendable"


def _annotate_sibling_appendability_samples(
    raw_samples: Any,
    *,
    actual_item_by_key: dict[tuple[str, str], dict[str, Any]],
    selected_keys: set[tuple[str, str]],
    default_reason: str,
    sample_limit: int,
) -> list[dict[str, Any]]:
    sample_rows = [sample for sample in raw_samples if isinstance(sample, dict)][: max(0, int(sample_limit))]
    if not sample_rows:
        return []
    annotated: list[dict[str, Any]] = []
    with sqlite_connection() as connection:
        for sample in sample_rows:
            track_id = str(sample.get("spotify_id") or "").strip()
            album_id = str(sample.get("parent_album_id") or "").strip()
            key = (track_id, album_id)
            plan_item = actual_item_by_key.get(key)
            queue_row = connection.execute(
                """
                SELECT id, reason, status
                FROM spotify_catalog_backfill_queue
                WHERE entity_type = 'track' AND spotify_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (track_id,),
            ).fetchone() if track_id else None
            metadata_row = connection.execute(
                """
                SELECT fetched_at, last_status
                FROM spotify_track_catalog
                WHERE spotify_track_id = ?
                """,
                (track_id,),
            ).fetchone() if track_id else None
            source_rows = connection.execute(
                """
                SELECT st.id, stm.release_track_id
                FROM source_track st
                LEFT JOIN source_track_map stm
                  ON stm.source_track_id = st.id
                 AND stm.status = 'accepted'
                WHERE st.source_name = 'spotify'
                  AND st.external_id = ?
                ORDER BY st.id ASC
                LIMIT 10
                """,
                (track_id,),
            ).fetchall() if track_id else []
            album_row = connection.execute(
                """
                SELECT total_tracks, COALESCE(atc.album_track_count, 0)
                FROM spotify_album_catalog sac
                LEFT JOIN (
                  SELECT spotify_album_id, count(*) AS album_track_count
                  FROM spotify_album_track
                  GROUP BY spotify_album_id
                ) atc
                  ON atc.spotify_album_id = sac.spotify_album_id
                WHERE sac.spotify_album_id = ?
                """,
                (album_id,),
            ).fetchone() if album_id else None
            if plan_item is not None:
                appendability_reason = (
                    "appendable"
                    if key in selected_keys
                    else _resolution_evidence_sibling_append_exclusion_reason(plan_item, seen_track_ids=set())
                )
            elif metadata_row and _track_metadata_completion_state(connection=connection, spotify_track_id=track_id)["complete"]:
                appendability_reason = "already_has_metadata"
            else:
                appendability_reason = default_reason
            annotated.append(
                {
                    "spotify_track_id": track_id,
                    "spotify_album_id": album_id or None,
                    "source_track_ids": [int(row[0]) for row in source_rows],
                    "release_track_ids": sorted({int(row[1]) for row in source_rows if row[1] is not None}),
                    "already_queued": queue_row is not None,
                    "queue_id": int(queue_row[0]) if queue_row else None,
                    "queue_reason": queue_row[1] if queue_row else None,
                    "queue_status": queue_row[2] if queue_row else None,
                    "track_metadata_exists": metadata_row is not None,
                    "track_metadata_complete": bool(
                        metadata_row and _track_metadata_completion_state(connection=connection, spotify_track_id=track_id)["complete"]
                    ),
                    "track_metadata_last_status": metadata_row[1] if metadata_row else None,
                    "album_total_tracks": int(album_row[0]) if album_row and album_row[0] is not None else None,
                    "album_track_count": int(album_row[1]) if album_row else None,
                    "in_focused_append_plan": plan_item is not None,
                    "plan_status": plan_item.get("plan_status") if plan_item else None,
                    "appendability_reason": appendability_reason,
                }
            )
    return annotated


def inspect_spotify_album_metadata_display_gaps(*, sample_limit: int = 20) -> dict[str, Any]:
    bounded_sample_limit = max(1, min(int(sample_limit), 100))
    with sqlite_connection() as connection:
        tracks_with_album_id_missing_album_name = int(
            connection.execute(
                """
                SELECT count(*)
                FROM spotify_track_catalog stc
                LEFT JOIN spotify_album_catalog sac
                  ON sac.spotify_album_id = stc.album_id
                WHERE stc.album_id IS NOT NULL
                  AND trim(COALESCE(stc.album_id, '')) != ''
                  AND (
                    sac.spotify_album_id IS NULL
                    OR sac.name IS NULL
                    OR trim(COALESCE(sac.name, '')) = ''
                  )
                """
            ).fetchone()[0]
        )
        source_albums_with_spotify_id_missing_name = int(
            connection.execute(
                """
                SELECT count(*)
                FROM source_album
                WHERE source_name = 'spotify'
                  AND external_id IS NOT NULL
                  AND trim(COALESCE(external_id, '')) != ''
                  AND (
                    source_name_raw IS NULL
                    OR trim(COALESCE(source_name_raw, '')) = ''
                  )
                """
            ).fetchone()[0]
        )
        rows = connection.execute(
            """
            SELECT
              stc.spotify_track_id,
              stc.album_id,
              sac.name AS local_album_name,
              json_extract(COALESCE(stc.raw_json, '{}'), '$.album.name') AS embedded_album_name,
              json_extract(COALESCE(stc.raw_json, '{}'), '$.album.release_date') AS embedded_release_date,
              json_extract(COALESCE(stc.raw_json, '{}'), '$.album.total_tracks') AS embedded_total_tracks,
              json_extract(COALESCE(sac.raw_json, '{}'), '$.label') AS local_label,
              json_extract(COALESCE(sac.raw_json, '{}'), '$.copyrights[0].text') AS local_copyright,
              json_extract(COALESCE(sac.raw_json, '{}'), '$.external_ids.upc') AS local_upc
            FROM spotify_track_catalog stc
            LEFT JOIN spotify_album_catalog sac
              ON sac.spotify_album_id = stc.album_id
            WHERE stc.album_id IS NOT NULL
              AND trim(COALESCE(stc.album_id, '')) != ''
              AND (
                sac.spotify_album_id IS NULL
                OR sac.name IS NULL
                OR trim(COALESCE(sac.name, '')) = ''
              )
            ORDER BY stc.spotify_track_id ASC
            LIMIT ?
            """,
            (bounded_sample_limit,),
        ).fetchall()
    samples = [
        {
            "spotify_track_id": str(row[0]),
            "spotify_album_id": str(row[1]),
            "local_album_name": row[2],
            "embedded_album_name": row[3],
            "embedded_release_date": row[4],
            "embedded_total_tracks": int(row[5]) if row[5] is not None else None,
            "can_populate_basic_album_display_from_track_payload": bool(str(row[3] or "").strip()),
            "requires_full_album_fetch_for_label_copyright_external_ids": not (
                str(row[6] or "").strip() and str(row[7] or "").strip() and str(row[8] or "").strip()
            ),
        }
        for row in rows
    ]
    return {
        "ok": True,
        "mode": "read_only",
        "counts": {
            "tracks_with_album_spotify_id_missing_local_album_name": tracks_with_album_id_missing_album_name,
            "source_albums_with_spotify_id_missing_name": source_albums_with_spotify_id_missing_name,
            "samples_with_embedded_album_name": sum(
                1 for item in samples if item["can_populate_basic_album_display_from_track_payload"]
            ),
        },
        "samples": samples,
        "notes": [
            "Spotify track payload album objects can populate basic display fields.",
            "Full album fetch is still required for label, copyrights, external IDs, UPC/EAN, and complete album provenance.",
        ],
    }


_SOURCE_RELEASE_ALBUM_DIAGNOSTIC_ROWS_SQL = """
    WITH primary_albums AS (
      SELECT release_track_id, release_album_name
      FROM (
        SELECT
          album_track.release_track_id,
          release_album.primary_name AS release_album_name,
          row_number() OVER (
            PARTITION BY album_track.release_track_id
            ORDER BY album_track.id ASC, release_album.primary_name ASC, release_album.id ASC
          ) AS rn
        FROM album_track
        JOIN release_album
          ON release_album.id = album_track.release_album_id
      )
      WHERE rn = 1
    ),
    grouped AS (
      SELECT
        stm.release_track_id,
        count(DISTINCT stm.source_track_id) AS source_count
      FROM source_track_map stm
      WHERE stm.status = 'accepted'
      GROUP BY stm.release_track_id
      HAVING source_count > 1
    ),
    source_rows AS (
      SELECT
        stm.release_track_id,
        rt.primary_name AS release_track_name,
        pal.release_album_name,
        st.id AS source_track_id,
        st.source_name,
        st.external_id AS spotify_track_id,
        st.source_name_raw,
        stc.name AS spotify_track_name,
        NULLIF(TRIM(stc.album_id), '') AS catalog_album_id,
        NULLIF(TRIM(json_extract(COALESCE(stc.raw_json, '{}'), '$.album.id')), '') AS embedded_album_id,
        NULLIF(TRIM(json_extract(COALESCE(stc.raw_json, '{}'), '$.album.name')), '') AS embedded_album_name,
        NULLIF(TRIM(json_extract(COALESCE(stc.raw_json, '{}'), '$.album.release_date')), '') AS embedded_album_release_date,
        json_extract(COALESCE(stc.raw_json, '{}'), '$.album.total_tracks') AS embedded_album_total_tracks,
        sac.spotify_album_id AS spotify_album_catalog_id,
        NULLIF(TRIM(sac.name), '') AS spotify_album_catalog_name,
        NULLIF(TRIM(sac.release_date), '') AS spotify_album_catalog_release_date,
        sac.total_tracks AS spotify_album_catalog_total_tracks,
        salb.id AS source_album_id,
        NULLIF(TRIM(salb.source_name_raw), '') AS source_album_name
      FROM grouped g
      JOIN source_track_map stm
        ON stm.release_track_id = g.release_track_id
       AND stm.status = 'accepted'
      JOIN source_track st
        ON st.id = stm.source_track_id
      JOIN release_track rt
        ON rt.id = stm.release_track_id
      LEFT JOIN primary_albums pal
        ON pal.release_track_id = rt.id
      LEFT JOIN spotify_track_catalog stc
        ON stc.spotify_track_id = st.external_id
      LEFT JOIN spotify_album_catalog sac
        ON sac.spotify_album_id = COALESCE(
          NULLIF(TRIM(stc.album_id), ''),
          NULLIF(TRIM(json_extract(COALESCE(stc.raw_json, '{}'), '$.album.id')), '')
        )
      LEFT JOIN source_album salb
        ON salb.source_name = 'spotify'
       AND salb.external_id = COALESCE(
          NULLIF(TRIM(stc.album_id), ''),
          NULLIF(TRIM(json_extract(COALESCE(stc.raw_json, '{}'), '$.album.id')), '')
        )
    )
"""


def inspect_source_release_album_display_gaps(*, sample_limit: int = 10) -> dict[str, Any]:
    bounded_sample_limit = max(1, min(int(sample_limit), 50))

    def sample(
        connection: sqlite3.Connection,
        where_sql: str,
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            f"""
            {_SOURCE_RELEASE_ALBUM_DIAGNOSTIC_ROWS_SQL}
            SELECT
              release_track_id,
              release_track_name,
              release_album_name,
              source_track_id,
              source_name,
              spotify_track_id,
              source_name_raw,
              spotify_track_name,
              COALESCE(catalog_album_id, embedded_album_id) AS album_spotify_id,
              spotify_album_catalog_name,
              source_album_name,
              embedded_album_name
            FROM source_rows
            WHERE {where_sql}
            ORDER BY release_track_id ASC, source_track_id ASC
            LIMIT ?
            """,
            (bounded_sample_limit,),
        ).fetchall()
        return [
            {
                "release_track_id": int(row[0]),
                "release_track_name": row[1],
                "release_album_name": row[2],
                "source_track_id": int(row[3]),
                "source_name": row[4],
                "spotify_track_id": row[5],
                "source_name_raw": row[6],
                "spotify_track_name": row[7],
                "album_spotify_id": row[8],
                "spotify_album_catalog_name": row[9],
                "source_album_name": row[10],
                "embedded_album_name": row[11],
            }
            for row in rows
        ]

    with sqlite_connection() as connection:
        counts_row = connection.execute(
            f"""
            {_SOURCE_RELEASE_ALBUM_DIAGNOSTIC_ROWS_SQL}
            SELECT
              count(*) AS total_rows,
              sum(CASE WHEN NULLIF(TRIM(release_album_name), '') IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_release_album_display_info,
              sum(CASE WHEN COALESCE(spotify_album_catalog_name, source_album_name) IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_source_album_display_info,
              sum(CASE WHEN embedded_album_name IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_embedded_album_info,
              sum(CASE WHEN COALESCE(catalog_album_id, embedded_album_id) IS NOT NULL AND spotify_album_catalog_name IS NULL THEN 1 ELSE 0 END) AS rows_with_album_spotify_id_but_no_local_album_name,
              sum(CASE WHEN COALESCE(catalog_album_id, embedded_album_id) IS NULL THEN 1 ELSE 0 END) AS rows_with_no_album_spotify_id,
              sum(CASE WHEN COALESCE(spotify_album_catalog_name, source_album_name, embedded_album_name) IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_source_album_display_after_embedded_fallback,
              sum(CASE WHEN COALESCE(catalog_album_id, embedded_album_id, embedded_album_name) IS NULL THEN 1 ELSE 0 END) AS rows_with_no_spotify_album_evidence
            FROM source_rows
            """
        ).fetchone()
        samples = {
            "missing_release_album_display_info": sample(
                connection,
                "NULLIF(TRIM(release_album_name), '') IS NULL",
            ),
            "missing_source_album_display_info_before_embedded_fallback": sample(
                connection,
                "COALESCE(spotify_album_catalog_name, source_album_name) IS NULL",
            ),
            "embedded_album_available_but_not_local_album_name": sample(
                connection,
                "embedded_album_name IS NOT NULL AND spotify_album_catalog_name IS NULL",
            ),
            "album_spotify_id_but_no_local_album_name": sample(
                connection,
                "COALESCE(catalog_album_id, embedded_album_id) IS NOT NULL AND spotify_album_catalog_name IS NULL",
            ),
            "no_album_spotify_id": sample(
                connection,
                "COALESCE(catalog_album_id, embedded_album_id) IS NULL",
            ),
            "no_spotify_album_evidence": sample(
                connection,
                "COALESCE(catalog_album_id, embedded_album_id, embedded_album_name) IS NULL",
            ),
        }

    count_names = [
        "total_rows",
        "rows_with_release_album_display_info",
        "rows_with_source_album_display_info",
        "rows_with_embedded_album_info",
        "rows_with_album_spotify_id_but_no_local_album_name",
        "rows_with_no_album_spotify_id",
        "rows_with_source_album_display_after_embedded_fallback",
        "rows_with_no_spotify_album_evidence",
    ]
    return {
        "ok": True,
        "mode": "read_only",
        "counts": {
            name: int(counts_row[index] or 0)
            for index, name in enumerate(count_names)
        },
        "samples": samples,
        "notes": [
            "Rows are accepted Spotify source-track mappings under release tracks with more than one source track.",
            "Source album display info counts existing local spotify_album_catalog/source_album names before embedded track payload fallback.",
            "The fallback is display-only and does not mutate identity mappings or catalog data.",
        ],
    }


def plan_source_release_album_display_enrichment(*, sample_limit: int = 20) -> dict[str, Any]:
    bounded_sample_limit = max(1, min(int(sample_limit), 100))
    with sqlite_connection() as connection:
        counts_row = connection.execute(
            f"""
            {_SOURCE_RELEASE_ALBUM_DIAGNOSTIC_ROWS_SQL}
            SELECT
              count(*) AS total_source_release_rows,
              sum(CASE WHEN COALESCE(catalog_album_id, embedded_album_id, embedded_album_name) IS NULL THEN 1 ELSE 0 END)
                AS rows_with_no_spotify_album_evidence,
              count(DISTINCT CASE
                WHEN source_name = 'spotify'
                 AND NULLIF(TRIM(spotify_track_id), '') IS NOT NULL
                 AND COALESCE(spotify_album_catalog_name, source_album_name, embedded_album_name) IS NULL
                 AND catalog_album_id IS NULL
                 AND embedded_album_id IS NULL
                 AND embedded_album_name IS NULL
                THEN spotify_track_id
              END) AS distinct_track_spotify_ids_needing_metadata,
              count(DISTINCT CASE
                WHEN source_name = 'spotify'
                 AND NULLIF(TRIM(spotify_track_id), '') IS NOT NULL
                 AND COALESCE(spotify_album_catalog_name, source_album_name, embedded_album_name) IS NULL
                 AND catalog_album_id IS NULL
                 AND embedded_album_id IS NULL
                 AND embedded_album_name IS NULL
                THEN spotify_track_id
              END) AS eligible_to_fetch,
              sum(CASE
                WHEN COALESCE(spotify_album_catalog_name, source_album_name, embedded_album_name) IS NULL
                 AND catalog_album_id IS NULL
                 AND embedded_album_id IS NULL
                 AND embedded_album_name IS NULL
                 AND (source_name != 'spotify' OR NULLIF(TRIM(spotify_track_id), '') IS NULL)
                THEN 1 ELSE 0
              END) AS blocked_or_invalid
            FROM source_rows
            """
        ).fetchone()
        sample_rows = connection.execute(
            f"""
            {_SOURCE_RELEASE_ALBUM_DIAGNOSTIC_ROWS_SQL}
            SELECT DISTINCT spotify_track_id
            FROM source_rows
            WHERE source_name = 'spotify'
              AND NULLIF(TRIM(spotify_track_id), '') IS NOT NULL
              AND COALESCE(spotify_album_catalog_name, source_album_name, embedded_album_name) IS NULL
              AND catalog_album_id IS NULL
              AND embedded_album_id IS NULL
              AND embedded_album_name IS NULL
            ORDER BY spotify_track_id ASC
            LIMIT ?
            """,
            (bounded_sample_limit,),
        ).fetchall()
    return {
        "ok": True,
        "mode": "read_only",
        "performed_action": "none",
        "total_source_release_rows": int(counts_row[0] or 0),
        "rows_with_no_spotify_album_evidence": int(counts_row[1] or 0),
        "distinct_track_spotify_ids_needing_metadata": int(counts_row[2] or 0),
        "eligible_to_fetch": int(counts_row[3] or 0),
        "blocked_or_invalid": int(counts_row[4] or 0),
        "sample_track_spotify_ids": [str(row[0]) for row in sample_rows],
        "selection_rule": {
            "source": "Source -> Release accepted source-track rows under release tracks with multiple source tracks",
            "requires_missing": [
                "source album display",
                "spotify_track_catalog.album_id",
                "spotify_track_catalog.raw_json.album.id",
                "spotify_track_catalog.raw_json.album.name",
            ],
            "requires_source": "spotify source_track.external_id",
        },
    }


def _source_release_album_display_enrichment_track_ids(*, limit: int) -> list[str]:
    bounded_limit = max(1, min(int(limit), 500))
    with sqlite_connection() as connection:
        rows = connection.execute(
            f"""
            {_SOURCE_RELEASE_ALBUM_DIAGNOSTIC_ROWS_SQL}
            SELECT DISTINCT spotify_track_id
            FROM source_rows
            WHERE source_name = 'spotify'
              AND NULLIF(TRIM(spotify_track_id), '') IS NOT NULL
              AND COALESCE(spotify_album_catalog_name, source_album_name, embedded_album_name) IS NULL
              AND catalog_album_id IS NULL
              AND embedded_album_id IS NULL
              AND embedded_album_name IS NULL
            ORDER BY spotify_track_id ASC
            LIMIT ?
            """,
            (bounded_limit,),
        ).fetchall()
    return [str(row[0]) for row in rows]


def _track_has_album_evidence(*, spotify_track_id: str) -> bool:
    with sqlite_connection() as connection:
        row = connection.execute(
            """
            SELECT
              NULLIF(TRIM(album_id), '') AS album_id,
              NULLIF(TRIM(json_extract(COALESCE(raw_json, '{}'), '$.album.id')), '') AS embedded_album_id,
              NULLIF(TRIM(json_extract(COALESCE(raw_json, '{}'), '$.album.name')), '') AS embedded_album_name
            FROM spotify_track_catalog
            WHERE spotify_track_id = ?
            """,
            (str(spotify_track_id or "").strip(),),
        ).fetchone()
    return bool(row and (row[0] or row[1] or row[2]))


def _active_spotify_track_metadata_cooldown_until() -> str | None:
    with sqlite_connection() as connection:
        row = connection.execute(
            """
            SELECT cooldown_until
            FROM spotify_catalog_worker_state
            WHERE worker_name = 'spotify_track_metadata'
            """
        ).fetchone()
    cooldown_text = str(row[0] or "").strip() if row else ""
    if not cooldown_text:
        return None
    try:
        parsed = datetime.fromisoformat(cooldown_text.replace("Z", "+00:00"))
    except ValueError:
        return cooldown_text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return cooldown_text if parsed > datetime.now(UTC) else None


def run_source_release_album_display_enrichment_worker(
    *,
    access_token: str,
    limit: int = 25,
    market: str = "US",
    request_delay_seconds: float = 1.5,
    max_requests: int = 40,
    max_429: int = 1,
    dry_run: bool = False,
    sleeper: Callable[[float], None] | None = None,
    fetcher: Callable[[str, dict[str, Any], str], tuple[int, dict[str, str], dict[str, Any], str | None]] | None = None,
) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 100))
    bounded_max_requests = max(1, min(int(max_requests), 200))
    bounded_max_429 = max(1, min(int(max_429), 1))
    normalized_market = str(market or "US").strip() or "US"
    sleep_fn = sleeper or time.sleep
    fetch_fn = fetcher or _default_fetcher
    selected_track_ids = _source_release_album_display_enrichment_track_ids(limit=bounded_limit)
    summary: dict[str, Any] = {
        "ok": True,
        "worker_name": "source_release_album_display_enrichment",
        "status": "dry_run" if dry_run else "ok",
        "performed_action": "none" if dry_run else "fetched_single_track_metadata",
        "selection_rule": {
            "planner": "source_release_album_display_enrichment",
            "requires_missing": [
                "source album display",
                "spotify_track_catalog.album_id",
                "spotify_track_catalog.raw_json.album.id",
                "spotify_track_catalog.raw_json.album.name",
            ],
        },
        "selected_count": len(selected_track_ids),
        "selected_track_spotify_ids": selected_track_ids,
        "processed_count": 0,
        "fetched_track_metadata": 0,
        "fetched_and_album_evidence_added": 0,
        "fetched_but_still_missing_album_evidence": 0,
        "error_count": 0,
        "requests_total": 0,
        "requests_429": 0,
        "cooldown_until": None,
        "sample_outcomes": [],
        "warnings": [],
    }
    if dry_run:
        return summary
    cooldown_until = _active_spotify_track_metadata_cooldown_until()
    if cooldown_until:
        summary["status"] = "skipped_cooldown"
        summary["performed_action"] = "none"
        summary["cooldown_until"] = cooldown_until
        return summary

    telemetry: dict[str, Any] = {
        "requests_total": 0,
        "requests_success": 0,
        "requests_429": 0,
        "requests_failed": 0,
        "max_retry_after_seconds": 0.0,
        "warnings": [],
        "_request_delay_seconds": _normalize_delay_seconds(float(request_delay_seconds)),
        "_request_timestamps": [],
        "_peak_requests_last_30_seconds": 0,
    }
    fetched_at = _utc_now()
    current_track_id: str | None = None
    try:
        for track_id in selected_track_ids:
            if int(telemetry["requests_total"]) >= bounded_max_requests:
                summary["status"] = "partial"
                summary["warnings"].append("Stopped before next track because max_requests was reached.")
                break
            current_track_id = track_id
            summary["processed_count"] += 1
            payload = _request_json(
                access_token=access_token,
                url=f"https://api.spotify.com/v1/tracks/{track_id}",
                params={"market": normalized_market},
                endpoint_category="source_release_album_display_track",
                telemetry=telemetry,
                max_429=bounded_max_429,
                sleeper=sleep_fn,
                fetcher=fetch_fn,
            )
            if not isinstance(payload, dict) or not payload.get("id"):
                error_text = "source_release_album_display_track: Missing track payload."
                _upsert_track_catalog_error(
                    spotify_track_id=track_id,
                    market=normalized_market,
                    fetched_at=fetched_at,
                    last_error=error_text,
                )
                summary["error_count"] += 1
                summary["sample_outcomes"].append(
                    {"spotify_track_id": track_id, "outcome": "error", "error": error_text}
                )
                continue
            _upsert_track_catalog(
                track=payload,
                market=normalized_market,
                fetched_at=fetched_at,
                last_status="ok",
                last_error=None,
            )
            summary["fetched_track_metadata"] += 1
            if _track_has_album_evidence(spotify_track_id=track_id):
                summary["fetched_and_album_evidence_added"] += 1
                outcome = "fetched_and_album_evidence_added"
            else:
                summary["fetched_but_still_missing_album_evidence"] += 1
                outcome = "fetched_but_still_missing_album_evidence"
            if len(summary["sample_outcomes"]) < 20:
                summary["sample_outcomes"].append({"spotify_track_id": track_id, "outcome": outcome})
    except _PartialStop as exc:
        summary["status"] = "partial"
        if exc.reason == "rate_limited":
            retry_after = float(telemetry.get("max_retry_after_seconds") or 0.0)
            if retry_after > 0:
                summary["cooldown_until"] = (datetime.now(UTC) + timedelta(seconds=retry_after)).isoformat().replace("+00:00", "Z")
            if current_track_id and len(summary["sample_outcomes"]) < 20:
                summary["sample_outcomes"].append({"spotify_track_id": current_track_id, "outcome": "rate_limited"})
        summary["warnings"].extend([str(item) for item in telemetry.get("warnings", [])])
    except RuntimeError as exc:
        summary["status"] = "partial"
        summary["error_count"] += 1
        if current_track_id and len(summary["sample_outcomes"]) < 20:
            summary["sample_outcomes"].append(
                {"spotify_track_id": current_track_id, "outcome": "error", "error": str(exc)}
            )
        summary["warnings"].append(str(exc))

    summary["requests_total"] = int(telemetry.get("requests_total", 0))
    summary["requests_429"] = int(telemetry.get("requests_429", 0))
    summary["warnings"].extend([str(item) for item in telemetry.get("warnings", []) if str(item) not in summary["warnings"]])
    return summary


def _album_basic_repair_needs_update(existing: sqlite3.Row | None, album: dict[str, Any]) -> bool:
    if existing is None:
        return True
    field_pairs = [
        ("name", album.get("name")),
        ("album_type", album.get("album_type")),
        ("release_date", album.get("release_date")),
        ("release_date_precision", album.get("release_date_precision")),
    ]
    for column, value in field_pairs:
        if str(value or "").strip() and not str(existing[column] or "").strip():
            return True
    if isinstance(album.get("total_tracks"), int) and existing["total_tracks"] is None:
        return True
    if isinstance(album.get("artists"), list) and album.get("artists") and str(existing["artists_json"] or "").strip() in {"", "[]"}:
        return True
    if isinstance(album.get("images"), list) and album.get("images") and str(existing["images_json"] or "").strip() in {"", "[]"}:
        return True
    if str(existing["last_status"] or "").strip().lower() == "error":
        return True
    return False


def repair_spotify_album_basic_metadata_from_track_payloads(
    *,
    apply: bool = False,
    sample_limit: int = 20,
) -> dict[str, Any]:
    bounded_sample_limit = max(1, min(int(sample_limit), 100))
    candidates: dict[str, dict[str, Any]] = {}
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = connection.execute(
            """
            SELECT spotify_track_id, album_id, raw_json, market, fetched_at
            FROM spotify_track_catalog
            WHERE album_id IS NOT NULL
              AND trim(COALESCE(album_id, '')) != ''
              AND raw_json IS NOT NULL
              AND trim(COALESCE(raw_json, '')) != ''
            ORDER BY spotify_track_id ASC
            """
        ).fetchall()
        for row in rows:
            raw_payload = _json_object(row["raw_json"])
            album = raw_payload.get("album") if isinstance(raw_payload.get("album"), dict) else {}
            album_id = str(album.get("id") or row["album_id"] or "").strip()
            album_name = str(album.get("name") or "").strip()
            if not album_id or not album_name or album_id in candidates:
                continue
            existing = connection.execute(
                """
                SELECT name, album_type, release_date, release_date_precision, total_tracks,
                       artists_json, images_json, last_status
                FROM spotify_album_catalog
                WHERE spotify_album_id = ?
                """,
                (album_id,),
            ).fetchone()
            if not _album_basic_repair_needs_update(existing, album):
                continue
            candidates[album_id] = {
                "spotify_album_id": album_id,
                "album_name": album_name,
                "source_track_id": str(row["spotify_track_id"]),
                "market": str(row["market"] or "US") or "US",
                "fetched_at": str(row["fetched_at"] or _utc_now()),
                "album": album,
            }

    selected_items = [
        {
            "spotify_album_id": item["spotify_album_id"],
            "album_name": item["album_name"],
            "source_track_id": item["source_track_id"],
        }
        for item in list(candidates.values())[:bounded_sample_limit]
    ]
    result = {
        "ok": True,
        "mode": "apply" if apply else "dry_run",
        "performed_action": "populated_basic_album_metadata" if apply else "none",
        "candidate_count": len(candidates),
        "would_update_count": len(candidates),
        "updated_count": 0,
        "skipped_count": 0,
        "selected_items": selected_items,
        "notes": [
            "Uses only already-stored Spotify track payload embedded album objects.",
            "Populates missing basic album display fields without overwriting fuller album metadata.",
            "Full album fetch remains required for label, copyrights, UPC/EAN, and complete provenance.",
        ],
    }
    if not apply or not candidates:
        result["skipped_count"] = 0 if not apply else len(candidates)
        return result
    with sqlite_connection(write=True) as connection:
        for item in candidates.values():
            before = connection.total_changes
            _upsert_simplified_album_catalog(
                connection=connection,
                album=item["album"],
                market=item["market"],
                fetched_at=item["fetched_at"],
                last_status="ok",
                last_error=None,
            )
            if connection.total_changes > before:
                result["updated_count"] += 1
            else:
                result["skipped_count"] += 1
    return result


def _artist_json_has_id_without_name(value: Any) -> bool:
    for item in _json_list(value):
        if not isinstance(item, dict):
            continue
        if str(item.get("id") or "").strip() and not str(item.get("name") or "").strip():
            return True
    return False


def inspect_spotify_nested_metadata_integrity(*, sample_limit: int = 20) -> dict[str, Any]:
    bounded_sample_limit = max(1, min(int(sample_limit), 100))
    album_gap_report = inspect_spotify_album_metadata_display_gaps(sample_limit=bounded_sample_limit)
    track_artist_missing_count = 0
    album_artist_missing_count = 0
    incomplete_done_count = 0
    samples = {
        "tracks_with_album_spotify_id_missing_local_album_name": album_gap_report.get("samples", []),
        "tracks_with_artist_ids_missing_artist_names": [],
        "albums_with_artist_ids_missing_artist_names": [],
        "queue_rows_done_but_local_metadata_incomplete": [],
    }
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        track_artist_rows = connection.execute(
            """
            SELECT spotify_track_id, artists_json
            FROM spotify_track_catalog
            WHERE artists_json IS NOT NULL
              AND trim(COALESCE(artists_json, '')) NOT IN ('', '[]')
            ORDER BY spotify_track_id ASC
            """
        ).fetchall()
        for row in track_artist_rows:
            if _artist_json_has_id_without_name(row["artists_json"]):
                track_artist_missing_count += 1
                if len(samples["tracks_with_artist_ids_missing_artist_names"]) < bounded_sample_limit:
                    samples["tracks_with_artist_ids_missing_artist_names"].append(
                        {"spotify_track_id": str(row["spotify_track_id"])}
                    )
        album_artist_rows = connection.execute(
            """
            SELECT spotify_album_id, artists_json
            FROM spotify_album_catalog
            WHERE artists_json IS NOT NULL
              AND trim(COALESCE(artists_json, '')) NOT IN ('', '[]')
            ORDER BY spotify_album_id ASC
            """
        ).fetchall()
        for row in album_artist_rows:
            if _artist_json_has_id_without_name(row["artists_json"]):
                album_artist_missing_count += 1
                if len(samples["albums_with_artist_ids_missing_artist_names"]) < bounded_sample_limit:
                    samples["albums_with_artist_ids_missing_artist_names"].append(
                        {"spotify_album_id": str(row["spotify_album_id"])}
                    )
        done_rows = connection.execute(
            """
            SELECT id, spotify_id, reason
            FROM spotify_catalog_backfill_queue
            WHERE entity_type = 'track'
              AND status = 'done'
            ORDER BY id ASC
            """
        ).fetchall()
        for row in done_rows:
            completion = _track_metadata_completion_state(connection=connection, spotify_track_id=str(row["spotify_id"]))
            if not completion["complete"]:
                incomplete_done_count += 1
                if len(samples["queue_rows_done_but_local_metadata_incomplete"]) < bounded_sample_limit:
                    samples["queue_rows_done_but_local_metadata_incomplete"].append(
                        {
                            "queue_id": int(row["id"]),
                            "spotify_track_id": str(row["spotify_id"]),
                            "reason": row["reason"],
                            "gaps": completion["gaps"],
                        }
                    )
    return {
        "ok": True,
        "mode": "read_only",
        "counts": {
            "tracks_with_album_spotify_id_missing_local_album_name": int(
                album_gap_report["counts"]["tracks_with_album_spotify_id_missing_local_album_name"]
            ),
            "tracks_with_artist_ids_missing_artist_names": track_artist_missing_count,
            "albums_with_artist_ids_missing_artist_names": album_artist_missing_count,
            "queue_rows_done_but_local_metadata_incomplete": incomplete_done_count,
        },
        "samples": samples,
        "notes": [
            "Artist integrity is checked against stored JSON evidence because there is no dedicated Spotify artist catalog table.",
            "Full album fetch remains required for label, copyrights, UPC/EAN, and complete album provenance.",
        ],
    }


def _pending_resolution_evidence_album_tracklist_queue_rows(*, limit: int) -> list[dict[str, Any]]:
    report = inspect_spotify_catalog_queue_resolution_evidence()
    plan = report.get("dry_run_resolution_evidence_plan") if isinstance(report, dict) else {}
    items = plan.get("candidate_album_tracklist_items") if isinstance(plan, dict) else []
    planner_items: dict[str, dict[str, Any]] = {}
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        album_id = str(item.get("spotify_id") or "").strip()
        if (
            item.get("planned_target") == "album_tracklist"
            and item.get("entity_type") == "album"
            and item.get("plan_status") == "already_queued_pending"
            and album_id
            and album_id not in planner_items
        ):
            planner_items[album_id] = item
    deduped_album_ids = list(planner_items.keys())[: max(1, int(limit))]
    if not deduped_album_ids:
        return []
    placeholders = ",".join("?" for _ in deduped_album_ids)
    with sqlite_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT id, entity_type, spotify_id, reason, priority, status, requested_at, last_attempted_at, attempts, last_error
            FROM spotify_catalog_backfill_queue
            WHERE status = 'pending'
              AND entity_type = 'album'
              AND spotify_id IN ({placeholders})
            ORDER BY priority DESC, requested_at ASC, id ASC
            """,
            deduped_album_ids,
        ).fetchall()
    row_by_album_id = {str(row[2]): row for row in rows}
    ordered_rows = [row_by_album_id[album_id] for album_id in deduped_album_ids if album_id in row_by_album_id]
    return [
        {
            "id": int(row[0]),
            "entity_type": str(row[1]),
            "spotify_id": str(row[2]),
            "reason": row[3],
            "priority": int(row[4] or 0),
            "status": str(row[5]),
            "requested_at": row[6],
            "last_attempted_at": row[7],
            "attempts": int(row[8] or 0),
            "last_error": row[9],
            "planner_status": str(planner_items.get(str(row[2]), {}).get("plan_status") or ""),
            "planned_target": str(planner_items.get(str(row[2]), {}).get("planned_target") or ""),
        }
        for row in ordered_rows
    ]


def _pending_resolution_evidence_track_metadata_queue_rows(*, limit: int) -> list[dict[str, Any]]:
    report = inspect_spotify_catalog_queue_resolution_evidence()
    plan = report.get("dry_run_resolution_evidence_plan") if isinstance(report, dict) else {}
    items = plan.get("actual_sibling_track_items") if isinstance(plan, dict) else []
    planner_items: dict[str, dict[str, Any]] = {}
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        track_id = str(item.get("spotify_id") or "").strip()
        if (
            item.get("planned_target") == "track_metadata"
            and item.get("entity_type") == "track"
            and item.get("plan_status") == "already_queued_pending"
            and track_id
            and track_id not in planner_items
        ):
            planner_items[track_id] = item
    deduped_track_ids = list(planner_items.keys())[: max(1, int(limit))]
    if not deduped_track_ids:
        return []
    placeholders = ",".join("?" for _ in deduped_track_ids)
    with sqlite_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT id, entity_type, spotify_id, reason, priority, status, requested_at, last_attempted_at, attempts, last_error
            FROM spotify_catalog_backfill_queue
            WHERE status = 'pending'
              AND entity_type = 'track'
              AND reason = 'resolution_evidence'
              AND spotify_id IN ({placeholders})
            ORDER BY priority DESC, requested_at ASC, id ASC
            """,
            deduped_track_ids,
        ).fetchall()
    row_by_track_id = {str(row[2]): row for row in rows}
    ordered_rows = [row_by_track_id[track_id] for track_id in deduped_track_ids if track_id in row_by_track_id]
    return [
        {
            "id": int(row[0]),
            "entity_type": str(row[1]),
            "spotify_id": str(row[2]),
            "reason": row[3],
            "priority": int(row[4] or 0),
            "status": str(row[5]),
            "requested_at": row[6],
            "last_attempted_at": row[7],
            "attempts": int(row[8] or 0),
            "last_error": row[9],
            "planner_status": str(planner_items.get(str(row[2]), {}).get("plan_status") or ""),
            "planned_target": str(planner_items.get(str(row[2]), {}).get("planned_target") or ""),
            "parent_album_id": planner_items.get(str(row[2]), {}).get("parent_album_id"),
        }
        for row in ordered_rows
    ]


def _queue_mark_attempted(*, queue_id: int) -> None:
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            UPDATE spotify_catalog_backfill_queue
            SET
              last_attempted_at = ?,
              attempts = COALESCE(attempts, 0) + 1
            WHERE id = ?
            """,
            (_utc_now(), int(queue_id)),
        )


def _album_tracklist_completion_state(*, connection: sqlite3.Connection, album_id: str) -> dict[str, Any]:
    normalized_album_id = str(album_id or "").strip()
    if not normalized_album_id:
        return {"complete": False, "album_total_tracks": None, "album_track_count": 0, "error_count": 0}
    album_row = connection.execute(
        """
        SELECT total_tracks
        FROM spotify_album_catalog
        WHERE spotify_album_id = ?
        """,
        (normalized_album_id,),
    ).fetchone()
    total_tracks = int(album_row[0]) if album_row and album_row[0] is not None else None
    track_row = connection.execute(
        """
        SELECT
          count(*) AS track_count,
          sum(CASE WHEN lower(COALESCE(last_status, '')) = 'error' THEN 1 ELSE 0 END) AS error_count
        FROM spotify_album_track
        WHERE spotify_album_id = ?
        """,
        (normalized_album_id,),
    ).fetchone()
    track_count = int(track_row[0] or 0) if track_row else 0
    error_count = int(track_row[1] or 0) if track_row else 0
    complete = track_count > 0 and error_count == 0 and total_tracks is not None and track_count >= total_tracks
    return {
        "complete": complete,
        "album_total_tracks": total_tracks,
        "album_track_count": track_count,
        "error_count": error_count,
    }


def _resolution_album_metadata_needs_fetch(*, connection: sqlite3.Connection, album_id: str) -> bool:
    normalized_album_id = str(album_id or "").strip()
    if not normalized_album_id:
        return True
    row = connection.execute(
        """
        SELECT release_date, total_tracks, images_json, raw_json, last_status
        FROM spotify_album_catalog
        WHERE spotify_album_id = ?
        """,
        (normalized_album_id,),
    ).fetchone()
    if row is None:
        return True
    release_date, total_tracks, images_json, raw_json, last_status = row
    if str(last_status or "").strip().lower() == "error":
        return True
    if not str(release_date or "").strip() or total_tracks is None:
        return True
    if images_json is None or str(images_json).strip() in {"", "[]"}:
        return True
    raw_payload = _json_object(raw_json)
    copyrights = raw_payload.get("copyrights")
    if not isinstance(copyrights, list) or not copyrights:
        return True
    if not str(raw_payload.get("label") or "").strip():
        return True
    return False


def run_spotify_resolution_evidence_album_tracklist_worker(
    *,
    access_token: str,
    limit: int = 5,
    market: str = "US",
    request_delay_seconds: float = 1.5,
    max_requests: int = 20,
    max_429: int = 1,
    max_album_tracks_pages_per_album: int = DEFAULT_MAX_ALBUM_TRACKS_PAGES_PER_ALBUM,
    dry_run: bool = False,
    sleeper: Callable[[float], None] | None = None,
    fetcher: Callable[[str, dict[str, Any], str], tuple[int, dict[str, str], dict[str, Any], str | None]] | None = None,
) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 50))
    bounded_max_requests = max(1, min(int(max_requests), 100))
    bounded_max_429 = max(1, min(int(max_429), 1))
    bounded_max_album_tracks_pages_per_album = max(1, min(int(max_album_tracks_pages_per_album), 50))
    normalized_market = str(market or "US").strip() or "US"
    sleep_fn = sleeper or time.sleep
    fetch_fn = fetcher or _default_fetcher
    selected_rows = _pending_resolution_evidence_album_tracklist_queue_rows(limit=bounded_limit)
    selected_items = [
        {
            "queue_id": int(row["id"]),
            "spotify_album_id": str(row["spotify_id"]),
            "stored_reason": row.get("reason"),
            "planner_status": row.get("planner_status"),
        }
        for row in selected_rows
    ]
    selected_count_by_stored_reason: dict[str, int] = {}
    for item in selected_items:
        reason_key = str(item.get("stored_reason") or "none")
        selected_count_by_stored_reason[reason_key] = selected_count_by_stored_reason.get(reason_key, 0) + 1
    summary: dict[str, Any] = {
        "ok": True,
        "worker_name": "spotify_resolution_evidence_album_tracklists",
        "status": "dry_run" if dry_run else "ok",
        "performed_action": "none" if dry_run else "processed_queue_rows",
        "selection_rule": {
            "status": "pending",
            "entity_type": "album",
            "reason": "planner candidate album tracklist item; stored reason may be resolution_evidence or legacy visible",
            "planned_target": "album_tracklist",
        },
        "selected_count": len(selected_items),
        "selected_count_by_stored_reason": selected_count_by_stored_reason,
        "selected_items": selected_items,
        "processed_count": 0,
        "done_count": 0,
        "skipped_count": 0,
        "error_count": 0,
        "rate_limited_count": 0,
        "outcome_counts": {
            "fetched_album_metadata": 0,
            "fetched_tracklist": 0,
            "fetched_and_marked_done": 0,
            "already_complete_marked_done": 0,
            "fetched_but_not_complete": 0,
            "skipped_without_change": 0,
            "error": 0,
            "rate_limited": 0,
        },
        "outcomes": [],
        "requests_total": 0,
        "queue_ids_processed": [],
        "album_spotify_ids_processed": [],
        "remaining_pending_resolution_evidence_album_tracklists": 0,
        "cooldown_until": None,
        "warnings": [],
    }
    if dry_run:
        summary["remaining_pending_resolution_evidence_album_tracklists"] = len(
            _pending_resolution_evidence_album_tracklist_queue_rows(limit=10_000)
        )
        return summary

    telemetry: dict[str, Any] = {
        "requests_total": 0,
        "requests_success": 0,
        "requests_429": 0,
        "requests_failed": 0,
        "max_retry_after_seconds": 0.0,
        "warnings": [],
        "_request_delay_seconds": _normalize_delay_seconds(float(request_delay_seconds)),
        "_request_timestamps": [],
        "_peak_requests_last_30_seconds": 0,
    }
    fetched_at = _utc_now()
    try:
        for row in selected_rows:
            if int(telemetry["requests_total"]) >= bounded_max_requests:
                summary["status"] = "partial"
                summary["warnings"].append("Stopped before next album because max_requests was reached.")
                break
            queue_id = int(row["id"])
            album_id = str(row["spotify_id"])
            with sqlite_connection() as connection:
                completion_before = _album_tracklist_completion_state(connection=connection, album_id=album_id)
            if completion_before["complete"]:
                _queue_mark_done(queue_id=queue_id)
                summary["done_count"] += 1
                summary["skipped_count"] += 1
                summary["outcome_counts"]["already_complete_marked_done"] += 1
                summary["outcomes"].append(
                    {
                        "queue_id": queue_id,
                        "spotify_album_id": album_id,
                        "outcome": "already_complete_marked_done",
                        **completion_before,
                    }
                )
                continue
            _queue_mark_attempted(queue_id=queue_id)
            summary["processed_count"] += 1
            summary["queue_ids_processed"].append(queue_id)
            summary["album_spotify_ids_processed"].append(album_id)
            with sqlite_connection() as connection:
                needs_album_metadata = _resolution_album_metadata_needs_fetch(connection=connection, album_id=album_id)
            fetched_album_metadata = False
            fetched_tracklist = False
            if needs_album_metadata:
                if int(telemetry["requests_total"]) >= bounded_max_requests:
                    summary["status"] = "partial"
                    summary["warnings"].append("Stopped before album metadata fetch because max_requests was reached.")
                    break
                album_payload = _request_json(
                    access_token=access_token,
                    url=f"https://api.spotify.com/v1/albums/{album_id}",
                    params={"market": normalized_market},
                    endpoint_category="resolution_evidence_album",
                    telemetry=telemetry,
                    max_429=bounded_max_429,
                    sleeper=sleep_fn,
                    fetcher=fetch_fn,
                )
                if not isinstance(album_payload, dict) or not album_payload.get("id"):
                    raise RuntimeError("resolution_evidence_album: Missing album payload.")
                _upsert_album_catalog(
                    album=album_payload,
                    market=normalized_market,
                    fetched_at=fetched_at,
                    last_status="ok",
                    last_error=None,
                )
                fetched_album_metadata = True
                summary["outcome_counts"]["fetched_album_metadata"] += 1
            with sqlite_connection() as connection:
                resume_offset = _album_track_resume_offset(connection=connection, album_id=album_id, force_refresh=False)
            next_url: str | None = f"https://api.spotify.com/v1/albums/{album_id}/tracks"
            next_params: dict[str, Any] | None = {"limit": ALBUM_TRACK_PAGE_SIZE, "offset": resume_offset, "market": normalized_market}
            pages_fetched = 0
            while next_url is not None:
                if int(telemetry["requests_total"]) >= bounded_max_requests:
                    summary["status"] = "partial"
                    summary["warnings"].append("Stopped during album pagination because max_requests was reached.")
                    break
                if pages_fetched >= bounded_max_album_tracks_pages_per_album:
                    summary["status"] = "partial"
                    summary["warnings"].append(f"album track pagination capped for {album_id}")
                    break
                payload = _request_json(
                    access_token=access_token,
                    url=next_url,
                    params=next_params or {},
                    endpoint_category="resolution_evidence_album_tracks",
                    telemetry=telemetry,
                    max_429=bounded_max_429,
                    sleeper=sleep_fn,
                    fetcher=fetch_fn,
                )
                items = payload.get("items") if isinstance(payload.get("items"), list) else []
                fetched_tracklist = True
                for album_track in items:
                    if isinstance(album_track, dict) and album_track.get("id"):
                        _upsert_album_track(
                            album_id=album_id,
                            track=album_track,
                            market=normalized_market,
                            fetched_at=fetched_at,
                            last_status="ok",
                            last_error=None,
                        )
                    else:
                        summary["skipped_count"] += 1
                pages_fetched += 1
                next_value = payload.get("next")
                if isinstance(next_value, str) and next_value.strip():
                    next_url = next_value
                    next_params = {}
                else:
                    next_url = None
            if fetched_tracklist:
                summary["outcome_counts"]["fetched_tracklist"] += 1
            with sqlite_connection() as connection:
                completion_after = _album_tracklist_completion_state(connection=connection, album_id=album_id)
                if not completion_after["complete"]:
                    if summary["status"] == "ok":
                        summary["status"] = "partial"
                    summary["outcome_counts"]["fetched_but_not_complete"] += 1
                    summary["outcomes"].append(
                        {
                            "queue_id": queue_id,
                            "spotify_album_id": album_id,
                            "outcome": "fetched_but_not_complete",
                            "fetched_album_metadata": fetched_album_metadata,
                            "fetched_tracklist": fetched_tracklist,
                            **completion_after,
                        }
                    )
                else:
                    _queue_mark_done(queue_id=queue_id)
                    summary["done_count"] += 1
                    summary["outcome_counts"]["fetched_and_marked_done"] += 1
                    summary["outcomes"].append(
                        {
                            "queue_id": queue_id,
                            "spotify_album_id": album_id,
                            "outcome": "fetched_and_marked_done",
                            "fetched_album_metadata": fetched_album_metadata,
                            "fetched_tracklist": fetched_tracklist,
                            **completion_after,
                        }
                    )
    except _PartialStop as exc:
        summary["status"] = "partial"
        if exc.reason == "rate_limited":
            summary["rate_limited_count"] = int(telemetry.get("requests_429", 0))
            summary["outcome_counts"]["rate_limited"] = int(summary["rate_limited_count"])
            retry_after = float(telemetry.get("max_retry_after_seconds") or 0.0)
            if retry_after > 0:
                summary["cooldown_until"] = (datetime.now(UTC) + timedelta(seconds=retry_after)).isoformat().replace("+00:00", "Z")
        summary["warnings"].extend([str(item) for item in telemetry.get("warnings", [])])
    except RuntimeError as exc:
        summary["status"] = "partial"
        summary["error_count"] += 1
        summary["outcome_counts"]["error"] += 1
        if selected_rows:
            last_queue_id = int(selected_rows[min(int(summary["processed_count"]) - 1, len(selected_rows) - 1)]["id"])
            _queue_mark_error(queue_id=last_queue_id, error_message=str(exc))
        summary["warnings"].append(str(exc))

    summary["requests_total"] = int(telemetry.get("requests_total", 0))
    summary["rate_limited_count"] = max(int(summary["rate_limited_count"]), int(telemetry.get("requests_429", 0)))
    summary["warnings"].extend([str(item) for item in telemetry.get("warnings", []) if str(item) not in summary["warnings"]])
    summary["remaining_pending_resolution_evidence_album_tracklists"] = len(
        _pending_resolution_evidence_album_tracklist_queue_rows(limit=10_000)
    )
    return summary


def run_spotify_resolution_evidence_track_metadata_worker(
    *,
    access_token: str,
    limit: int = 25,
    market: str = "US",
    request_delay_seconds: float = 1.5,
    max_requests: int = 40,
    max_429: int = 1,
    dry_run: bool = False,
    sleeper: Callable[[float], None] | None = None,
    fetcher: Callable[[str, dict[str, Any], str], tuple[int, dict[str, str], dict[str, Any], str | None]] | None = None,
) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 100))
    bounded_max_requests = max(1, min(int(max_requests), 200))
    bounded_max_429 = max(1, min(int(max_429), 1))
    normalized_market = str(market or "US").strip() or "US"
    sleep_fn = sleeper or time.sleep
    fetch_fn = fetcher or _default_fetcher
    selected_rows = _pending_resolution_evidence_track_metadata_queue_rows(limit=bounded_limit)
    initial_integrity = inspect_spotify_nested_metadata_integrity(sample_limit=5)
    selected_items = [
        {
            "queue_id": int(row["id"]),
            "spotify_track_id": str(row["spotify_id"]),
            "stored_reason": row.get("reason"),
            "planner_status": row.get("planner_status"),
            "parent_album_id": row.get("parent_album_id"),
        }
        for row in selected_rows
    ]
    selected_count_by_stored_reason: dict[str, int] = {}
    for item in selected_items:
        reason_key = str(item.get("stored_reason") or "none")
        selected_count_by_stored_reason[reason_key] = selected_count_by_stored_reason.get(reason_key, 0) + 1
    summary: dict[str, Any] = {
        "ok": True,
        "worker_name": "spotify_resolution_evidence_track_metadata",
        "status": "dry_run" if dry_run else "ok",
        "performed_action": "none" if dry_run else "processed_queue_rows",
        "selection_rule": {
            "status": "pending",
            "entity_type": "track",
            "reason": "resolution_evidence",
            "planned_target": "track_metadata",
            "planner_section": "actual_sibling_track_items",
            "planner_status": "already_queued_pending",
        },
        "selected_count": len(selected_items),
        "selected_count_by_stored_reason": selected_count_by_stored_reason,
        "selected_items": selected_items,
        "processed_count": 0,
        "done_count": 0,
        "skipped_count": 0,
        "error_count": 0,
        "rate_limited_count": 0,
        "outcome_counts": {
            "fetched_track_metadata": 0,
            "fetched_and_marked_done": 0,
            "already_complete_marked_done": 0,
            "fetched_but_not_complete": 0,
            "skipped_without_change": 0,
            "error": 0,
            "rate_limited": 0,
        },
        "outcomes": [],
        "requests_total": 0,
        "queue_ids_processed": [],
        "track_spotify_ids_processed": [],
        "remaining_pending_resolution_evidence_tracks": 0,
        "cooldown_until": None,
        "warnings": [],
        "local_metadata_integrity": initial_integrity,
    }
    if dry_run:
        summary["remaining_pending_resolution_evidence_tracks"] = len(
            _pending_resolution_evidence_track_metadata_queue_rows(limit=10_000)
        )
        return summary

    telemetry: dict[str, Any] = {
        "requests_total": 0,
        "requests_success": 0,
        "requests_429": 0,
        "requests_failed": 0,
        "max_retry_after_seconds": 0.0,
        "warnings": [],
        "_request_delay_seconds": _normalize_delay_seconds(float(request_delay_seconds)),
        "_request_timestamps": [],
        "_peak_requests_last_30_seconds": 0,
    }
    fetched_at = _utc_now()
    current_queue_id: int | None = None
    try:
        for row in selected_rows:
            if int(telemetry["requests_total"]) >= bounded_max_requests:
                summary["status"] = "partial"
                summary["warnings"].append("Stopped before next track because max_requests was reached.")
                break
            queue_id = int(row["id"])
            current_queue_id = queue_id
            track_id = str(row["spotify_id"])
            with sqlite_connection() as connection:
                completion_before = _is_track_metadata_complete(connection=connection, spotify_track_id=track_id)
            if completion_before:
                _queue_mark_done(queue_id=queue_id)
                summary["done_count"] += 1
                summary["skipped_count"] += 1
                summary["outcome_counts"]["already_complete_marked_done"] += 1
                summary["outcomes"].append(
                    {
                        "queue_id": queue_id,
                        "spotify_track_id": track_id,
                        "outcome": "already_complete_marked_done",
                    }
                )
                continue
            _queue_mark_attempted(queue_id=queue_id)
            summary["processed_count"] += 1
            summary["queue_ids_processed"].append(queue_id)
            summary["track_spotify_ids_processed"].append(track_id)
            payload = _request_json(
                access_token=access_token,
                url=f"https://api.spotify.com/v1/tracks/{track_id}",
                params={"market": normalized_market},
                endpoint_category="resolution_evidence_track",
                telemetry=telemetry,
                max_429=bounded_max_429,
                sleeper=sleep_fn,
                fetcher=fetch_fn,
            )
            if not isinstance(payload, dict) or not payload.get("id"):
                error_text = "resolution_evidence_track: Missing track payload."
                _upsert_track_catalog_error(
                    spotify_track_id=track_id,
                    market=normalized_market,
                    fetched_at=fetched_at,
                    last_error=error_text,
                )
                _queue_mark_error(queue_id=queue_id, error_message=error_text)
                summary["error_count"] += 1
                summary["outcome_counts"]["error"] += 1
                summary["outcomes"].append(
                    {
                        "queue_id": queue_id,
                        "spotify_track_id": track_id,
                        "outcome": "error",
                        "error": error_text,
                    }
                )
                continue
            _upsert_track_catalog(
                track=payload,
                market=normalized_market,
                fetched_at=fetched_at,
                last_status="ok",
                last_error=None,
            )
            summary["outcome_counts"]["fetched_track_metadata"] += 1
            with sqlite_connection() as connection:
                completion_after = _is_track_metadata_complete(connection=connection, spotify_track_id=track_id)
            if completion_after:
                _queue_mark_done(queue_id=queue_id)
                summary["done_count"] += 1
                summary["outcome_counts"]["fetched_and_marked_done"] += 1
                summary["outcomes"].append(
                    {
                        "queue_id": queue_id,
                        "spotify_track_id": track_id,
                        "outcome": "fetched_and_marked_done",
                    }
                )
            else:
                if summary["status"] == "ok":
                    summary["status"] = "partial"
                summary["outcome_counts"]["fetched_but_not_complete"] += 1
                summary["outcomes"].append(
                    {
                        "queue_id": queue_id,
                        "spotify_track_id": track_id,
                        "outcome": "fetched_but_not_complete",
                    }
                )
    except _PartialStop as exc:
        summary["status"] = "partial"
        if exc.reason == "rate_limited":
            summary["rate_limited_count"] = int(telemetry.get("requests_429", 0))
            summary["outcome_counts"]["rate_limited"] = int(summary["rate_limited_count"])
            retry_after = float(telemetry.get("max_retry_after_seconds") or 0.0)
            if retry_after > 0:
                summary["cooldown_until"] = (datetime.now(UTC) + timedelta(seconds=retry_after)).isoformat().replace("+00:00", "Z")
        summary["warnings"].extend([str(item) for item in telemetry.get("warnings", [])])
    except RuntimeError as exc:
        summary["status"] = "partial"
        summary["error_count"] += 1
        summary["outcome_counts"]["error"] += 1
        if current_queue_id is not None:
            _queue_mark_error(queue_id=current_queue_id, error_message=str(exc))
        summary["warnings"].append(str(exc))

    summary["requests_total"] = int(telemetry.get("requests_total", 0))
    summary["rate_limited_count"] = max(int(summary["rate_limited_count"]), int(telemetry.get("requests_429", 0)))
    summary["warnings"].extend([str(item) for item in telemetry.get("warnings", []) if str(item) not in summary["warnings"]])
    summary["remaining_pending_resolution_evidence_tracks"] = len(
        _pending_resolution_evidence_track_metadata_queue_rows(limit=10_000)
    )
    final_integrity = inspect_spotify_nested_metadata_integrity(sample_limit=5)
    summary["local_metadata_integrity"] = final_integrity
    initial_album_gap_count = int(initial_integrity["counts"]["tracks_with_album_spotify_id_missing_local_album_name"])
    final_album_gap_count = int(final_integrity["counts"]["tracks_with_album_spotify_id_missing_local_album_name"])
    if final_album_gap_count > initial_album_gap_count and int(summary.get("done_count") or 0) > 0:
        warning = (
            "Local metadata integrity warning: track processing increased "
            "tracks_with_album_spotify_id_missing_local_album_name."
        )
        if warning not in summary["warnings"]:
            summary["warnings"].append(warning)
    return summary


def repair_incomplete_done_resolution_tracklist_queue_rows(*, apply: bool = False) -> dict[str, Any]:
    report = inspect_spotify_catalog_queue_resolution_evidence()
    plan = report.get("dry_run_resolution_evidence_plan") if isinstance(report, dict) else {}
    items = plan.get("candidate_album_tracklist_items") if isinstance(plan, dict) else []
    candidate_album_ids = [
        str(item.get("spotify_id") or "").strip()
        for item in items if isinstance(item, dict)
        and item.get("planned_target") == "album_tracklist"
        and item.get("entity_type") == "album"
        and item.get("plan_status") in {"already_queued_done", "done_but_tracklist_incomplete"}
        and str(item.get("spotify_id") or "").strip()
    ]
    deduped_album_ids = list(dict.fromkeys(candidate_album_ids))
    candidates: list[dict[str, Any]] = []
    if deduped_album_ids:
        placeholders = ",".join("?" for _ in deduped_album_ids)
        with sqlite_connection() as connection:
            rows = connection.execute(
                f"""
                SELECT q.id, q.spotify_id, q.reason, q.status
                FROM spotify_catalog_backfill_queue q
                WHERE q.entity_type = 'album'
                  AND q.status = 'done'
                  AND q.spotify_id IN ({placeholders})
                ORDER BY q.id ASC
                """,
                deduped_album_ids,
            ).fetchall()
            for row in rows:
                completion = _album_tracklist_completion_state(connection=connection, album_id=str(row[1]))
                if not completion["complete"]:
                    candidates.append(
                        {
                            "queue_id": int(row[0]),
                            "spotify_album_id": str(row[1]),
                            "stored_reason": row[2],
                            "status": str(row[3]),
                            **completion,
                        }
                    )
    result = {
        "ok": True,
        "mode": "apply" if apply else "dry_run",
        "performed_action": "reset_incomplete_done_rows" if apply else "none",
        "selected_count": len(candidates),
        "would_reset_count": len(candidates),
        "selected_items": candidates,
        "reset_count": 0,
    }
    if not apply or not candidates:
        return result
    with sqlite_connection(write=True) as connection:
        for candidate in candidates:
            before = connection.total_changes
            connection.execute(
                """
                UPDATE spotify_catalog_backfill_queue
                SET status = 'pending',
                    last_error = NULL
                WHERE id = ?
                  AND entity_type = 'album'
                  AND status = 'done'
                """,
                (int(candidate["queue_id"]),),
            )
            if connection.total_changes > before:
                result["reset_count"] += 1
    return result


def repair_spotify_catalog_backfill_queue_statuses() -> dict[str, Any]:
    repaired_to_pending = 0
    repaired_to_done = 0
    with sqlite_connection(write=True) as connection:
        rows = connection.execute(
            """
            SELECT id, entity_type, spotify_id, status
            FROM spotify_catalog_backfill_queue
            WHERE lower(COALESCE(entity_type, '')) IN ('track', 'album')
              AND lower(COALESCE(status, '')) IN ('pending', 'done')
            """
        ).fetchall()
        for row in rows:
            queue_id = int(row[0])
            entity_type = str(row[1] or "").strip().lower()
            spotify_id = str(row[2] or "").strip()
            status = str(row[3] or "").strip().lower()
            if not spotify_id:
                continue
            if entity_type == "track":
                is_complete = _is_track_catalog_complete(connection=connection, spotify_track_id=spotify_id)
            else:
                is_complete = _is_album_catalog_complete(connection=connection, spotify_album_id=spotify_id)
            if status == "done" and not is_complete:
                connection.execute(
                    "UPDATE spotify_catalog_backfill_queue SET status = 'pending', last_error = NULL WHERE id = ?",
                    (queue_id,),
                )
                repaired_to_pending += 1
            elif status == "pending" and is_complete:
                connection.execute(
                    "UPDATE spotify_catalog_backfill_queue SET status = 'done' WHERE id = ?",
                    (queue_id,),
                )
                repaired_to_done += 1
    return {"ok": True, "repaired_to_pending": repaired_to_pending, "repaired_to_done": repaired_to_done}


def search_album_catalog_lookup(
    *,
    q: str | None = None,
    catalog_status: str = "all",
    queue_status: str | None = "all",
    sort: str | None = "default",
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 200))
    bounded_offset = max(0, int(offset))
    normalized_status = str(catalog_status or "all").strip().lower()
    if normalized_status not in {"all", "backfilled", "not_backfilled", "tracklist_complete", "tracklist_incomplete", "error"}:
        normalized_status = "all"
    normalized_queue_status = str(queue_status or "all").strip().lower()
    if normalized_queue_status not in {"all", "not_queued", "pending", "done", "error"}:
        normalized_queue_status = "all"
    normalized_sort = str(sort or "default").strip().lower()
    if normalized_sort not in {"default", "recently_backfilled", "name", "incomplete_first"}:
        normalized_sort = "default"
    normalized_q = str(q or "").strip()
    like_q = f"%{normalized_q.lower()}%"

    where_clauses: list[str] = []
    params: list[Any] = []
    if normalized_q:
        where_clauses.append(
            "("
            "lower(COALESCE(base.release_album_name, '')) LIKE ? "
            "OR lower(COALESCE(base.artist_name, '')) LIKE ? "
            "OR lower(COALESCE(base.spotify_album_id, '')) LIKE ?"
            ")"
        )
        params.extend([like_q, like_q, like_q])

    if normalized_status == "backfilled":
        where_clauses.append("base.spotify_album_id IS NOT NULL")
    elif normalized_status == "not_backfilled":
        where_clauses.append("base.spotify_album_id IS NULL")
    elif normalized_status == "tracklist_complete":
        where_clauses.append("base.total_tracks IS NOT NULL AND base.album_track_rows >= base.total_tracks")
    elif normalized_status == "tracklist_incomplete":
        where_clauses.append("base.spotify_album_id IS NOT NULL AND (base.total_tracks IS NULL OR base.album_track_rows < base.total_tracks)")
    elif normalized_status == "error":
        where_clauses.append("(lower(COALESCE(base.catalog_last_status, '')) = 'error' OR base.catalog_last_error IS NOT NULL)")
    if normalized_queue_status != "all":
        where_clauses.append("base.queue_status = ?")
        params.append(normalized_queue_status)

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    base_sql = """
        WITH primary_artists AS (
          SELECT
            ordered.release_album_id,
            group_concat(ordered.artist_name, ', ') AS artist_name
          FROM (
            SELECT
              aa.release_album_id,
              a.canonical_name AS artist_name
            FROM album_artist aa
            JOIN artist a
              ON a.id = aa.artist_id
            WHERE aa.role = 'primary'
            ORDER BY aa.release_album_id, COALESCE(aa.billing_index, 999999), aa.id, a.canonical_name
          ) ordered
          GROUP BY ordered.release_album_id
        ),
        primary_artist_first AS (
          SELECT release_album_id, artist_name
          FROM (
            SELECT
              aa.release_album_id,
              a.canonical_name AS artist_name,
              row_number() OVER (
                PARTITION BY aa.release_album_id
                ORDER BY COALESCE(aa.billing_index, 999999), aa.id, a.canonical_name
              ) AS rn
            FROM album_artist aa
            JOIN artist a
              ON a.id = aa.artist_id
            WHERE aa.role = 'primary'
          )
          WHERE rn = 1
        ),
        mapped_album_candidates AS (
          SELECT
            sam.release_album_id,
            sa.external_id AS spotify_album_id,
            1 AS source_priority,
            0 AS listen_count
          FROM source_album sa
          JOIN source_album_map sam
            ON sam.source_album_id = sa.id
          WHERE sa.source_name = 'spotify'
            AND sa.external_id IS NOT NULL
            AND sa.external_id != ''
            AND sam.status = 'accepted'
          UNION ALL
          SELECT
            sam.release_album_id,
            sa.external_id AS spotify_album_id,
            2 AS source_priority,
            0 AS listen_count
          FROM source_album sa
          JOIN source_album_map sam
            ON sam.source_album_id = sa.id
          WHERE sa.source_name = 'spotify'
            AND sa.external_id IS NOT NULL
            AND sa.external_id != ''
        ),
        raw_album_candidates AS (
          SELECT
            at.release_album_id AS release_album_id,
            rpe.spotify_album_id AS spotify_album_id,
            3 AS source_priority,
            count(*) AS listen_count
          FROM raw_play_event rpe
          JOIN source_track st
            ON st.source_name = 'spotify'
           AND st.external_id = rpe.spotify_track_id
          JOIN source_track_map stm
            ON stm.source_track_id = st.id
           AND stm.status = 'accepted'
          JOIN album_track at
            ON at.release_track_id = stm.release_track_id
          WHERE rpe.spotify_album_id IS NOT NULL
            AND rpe.spotify_album_id != ''
          GROUP BY at.release_album_id, rpe.spotify_album_id
        ),
        fallback_local_album_candidates AS (
          SELECT
            ra.id AS release_album_id,
            sac.spotify_album_id AS spotify_album_id,
            4 AS source_priority,
            0 AS listen_count
          FROM release_album ra
          JOIN primary_artist_first paf
            ON paf.release_album_id = ra.id
          JOIN spotify_album_catalog sac
            ON lower(trim(COALESCE(sac.name, ''))) = lower(trim(COALESCE(ra.primary_name, '')))
          JOIN json_each(COALESCE(sac.artists_json, '[]')) artist_json
          WHERE lower(trim(COALESCE(json_extract(artist_json.value, '$.name'), '')))
            = lower(trim(COALESCE(paf.artist_name, '')))
        ),
        all_album_candidates AS (
          SELECT * FROM mapped_album_candidates
          UNION ALL
          SELECT * FROM raw_album_candidates
          UNION ALL
          SELECT * FROM fallback_local_album_candidates
        ),
        representative_spotify_album AS (
          SELECT release_album_id, spotify_album_id
          FROM (
            SELECT
              release_album_id,
              spotify_album_id,
              row_number() OVER (
                PARTITION BY release_album_id
                ORDER BY
                  source_priority ASC,
                  listen_count DESC,
                  spotify_album_id ASC
              ) AS rn
            FROM all_album_candidates
          )
          WHERE rn = 1
        ),
        album_track_counts AS (
          SELECT
            spotify_album_id,
            count(*) AS album_track_rows
          FROM spotify_album_track
          GROUP BY spotify_album_id
        ),
        base AS (
          SELECT
            ra.id AS release_album_id,
            ra.primary_name AS release_album_name,
            COALESCE(pa.artist_name, 'Unknown artist') AS artist_name,
            rsa.spotify_album_id AS spotify_album_id,
            sac.name AS spotify_album_name,
            sac.album_type AS album_type,
            sac.release_date AS release_date,
            sac.total_tracks AS total_tracks,
            COALESCE(atc.album_track_rows, 0) AS album_track_rows,
            CASE
              WHEN sac.total_tracks IS NOT NULL AND COALESCE(atc.album_track_rows, 0) >= sac.total_tracks THEN 1
              ELSE 0
            END AS tracklist_complete,
            sac.fetched_at AS catalog_fetched_at,
            sac.last_status AS catalog_last_status,
            sac.last_error AS catalog_last_error,
            CASE
              WHEN q.id IS NULL THEN 'not_queued'
              ELSE q.status
            END AS queue_status,
            q.priority AS queue_priority,
            q.requested_at AS queue_requested_at,
            q.attempts AS queue_attempts,
            q.last_error AS queue_last_error
          FROM release_album ra
          LEFT JOIN primary_artists pa
            ON pa.release_album_id = ra.id
          LEFT JOIN representative_spotify_album rsa
            ON rsa.release_album_id = ra.id
          LEFT JOIN spotify_album_catalog sac
            ON sac.spotify_album_id = rsa.spotify_album_id
          LEFT JOIN album_track_counts atc
            ON atc.spotify_album_id = rsa.spotify_album_id
          LEFT JOIN spotify_catalog_backfill_queue q
            ON q.entity_type = 'album'
           AND q.spotify_id = rsa.spotify_album_id
        )
    """

    total_query = f"{base_sql} SELECT count(*) FROM base {where_sql}"
    if normalized_sort == "recently_backfilled":
        order_sql = """
            ORDER BY
              CASE WHEN catalog_fetched_at IS NULL THEN 1 ELSE 0 END ASC,
              catalog_fetched_at DESC,
              release_album_name ASC,
              release_album_id ASC
        """
    elif normalized_sort == "name":
        order_sql = """
            ORDER BY
              release_album_name ASC,
              release_album_id ASC
        """
    else:
        order_sql = """
            ORDER BY
              CASE
                WHEN lower(COALESCE(catalog_last_status, '')) = 'error' OR catalog_last_error IS NOT NULL THEN 1
                WHEN spotify_album_id IS NULL OR total_tracks IS NULL OR album_track_rows < total_tracks THEN 2
                ELSE 3
              END ASC,
              release_album_name ASC,
              release_album_id ASC
        """

    items_query = f"""
        {base_sql}
        SELECT
          release_album_id,
          release_album_name,
          artist_name,
          spotify_album_id,
          spotify_album_name,
          album_type,
          release_date,
          total_tracks,
          album_track_rows,
          tracklist_complete,
          catalog_fetched_at,
          catalog_last_status,
          catalog_last_error,
          queue_status,
          queue_priority,
          queue_requested_at,
          queue_attempts,
          queue_last_error
        FROM base
        {where_sql}
        {order_sql}
        LIMIT ?
        OFFSET ?
    """
    with sqlite_connection() as connection:
        total = int(connection.execute(total_query, tuple(params)).fetchone()[0])
        rows = connection.execute(items_query, tuple(params + [bounded_limit, bounded_offset])).fetchall()

    items = [
        {
            "release_album_id": int(row[0]),
            "release_album_name": str(row[1] or ""),
            "artist_name": str(row[2] or "Unknown artist"),
            "spotify_album_id": row[3],
            "spotify_album_name": row[4],
            "album_type": row[5],
            "release_date": row[6],
            "total_tracks": int(row[7]) if row[7] is not None else None,
            "album_track_rows": int(row[8] or 0),
            "tracklist_complete": bool(row[9]),
            "catalog_fetched_at": row[10],
            "catalog_last_status": row[11],
            "catalog_last_error": row[12],
            "queue_status": str(row[13] or "not_queued"),
            "queue_priority": int(row[14]) if row[14] is not None else None,
            "queue_requested_at": row[15],
            "queue_attempts": int(row[16]) if row[16] is not None else None,
            "queue_last_error": row[17],
        }
        for row in rows
    ]
    return {"ok": True, "items": items, "total": total}


def search_album_catalog_duplicate_spotify_identities(
    *,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 500))
    bounded_offset = max(0, int(offset))
    base_sql = """
        WITH primary_artists AS (
          SELECT
            ordered.release_album_id,
            group_concat(ordered.artist_name, ', ') AS artist_name
          FROM (
            SELECT
              aa.release_album_id,
              a.canonical_name AS artist_name
            FROM album_artist aa
            JOIN artist a
              ON a.id = aa.artist_id
            WHERE aa.role = 'primary'
            ORDER BY aa.release_album_id, COALESCE(aa.billing_index, 999999), aa.id, a.canonical_name
          ) ordered
          GROUP BY ordered.release_album_id
        ),
        primary_artist_first AS (
          SELECT release_album_id, artist_name
          FROM (
            SELECT
              aa.release_album_id,
              a.canonical_name AS artist_name,
              row_number() OVER (
                PARTITION BY aa.release_album_id
                ORDER BY COALESCE(aa.billing_index, 999999), aa.id, a.canonical_name
              ) AS rn
            FROM album_artist aa
            JOIN artist a
              ON a.id = aa.artist_id
            WHERE aa.role = 'primary'
          )
          WHERE rn = 1
        ),
        mapped_album_candidates AS (
          SELECT
            sam.release_album_id,
            sa.external_id AS spotify_album_id,
            1 AS source_priority,
            0 AS listen_count
          FROM source_album sa
          JOIN source_album_map sam
            ON sam.source_album_id = sa.id
          WHERE sa.source_name = 'spotify'
            AND sa.external_id IS NOT NULL
            AND sa.external_id != ''
            AND sam.status = 'accepted'
          UNION ALL
          SELECT
            sam.release_album_id,
            sa.external_id AS spotify_album_id,
            2 AS source_priority,
            0 AS listen_count
          FROM source_album sa
          JOIN source_album_map sam
            ON sam.source_album_id = sa.id
          WHERE sa.source_name = 'spotify'
            AND sa.external_id IS NOT NULL
            AND sa.external_id != ''
        ),
        raw_album_candidates AS (
          SELECT
            at.release_album_id AS release_album_id,
            rpe.spotify_album_id AS spotify_album_id,
            3 AS source_priority,
            count(*) AS listen_count
          FROM raw_play_event rpe
          JOIN source_track st
            ON st.source_name = 'spotify'
           AND st.external_id = rpe.spotify_track_id
          JOIN source_track_map stm
            ON stm.source_track_id = st.id
           AND stm.status = 'accepted'
          JOIN album_track at
            ON at.release_track_id = stm.release_track_id
          WHERE rpe.spotify_album_id IS NOT NULL
            AND rpe.spotify_album_id != ''
          GROUP BY at.release_album_id, rpe.spotify_album_id
        ),
        fallback_local_album_candidates AS (
          SELECT
            ra.id AS release_album_id,
            sac.spotify_album_id AS spotify_album_id,
            4 AS source_priority,
            0 AS listen_count
          FROM release_album ra
          JOIN primary_artist_first paf
            ON paf.release_album_id = ra.id
          JOIN spotify_album_catalog sac
            ON lower(trim(COALESCE(sac.name, ''))) = lower(trim(COALESCE(ra.primary_name, '')))
          JOIN json_each(COALESCE(sac.artists_json, '[]')) artist_json
          WHERE lower(trim(COALESCE(json_extract(artist_json.value, '$.name'), '')))
            = lower(trim(COALESCE(paf.artist_name, '')))
        ),
        all_album_candidates AS (
          SELECT * FROM mapped_album_candidates
          UNION ALL
          SELECT * FROM raw_album_candidates
          UNION ALL
          SELECT * FROM fallback_local_album_candidates
        ),
        representative_spotify_album AS (
          SELECT release_album_id, spotify_album_id
          FROM (
            SELECT
              release_album_id,
              spotify_album_id,
              row_number() OVER (
                PARTITION BY release_album_id
                ORDER BY
                  source_priority ASC,
                  listen_count DESC,
                  spotify_album_id ASC
              ) AS rn
            FROM all_album_candidates
          )
          WHERE rn = 1
        ),
        album_track_counts AS (
          SELECT
            spotify_album_id,
            count(*) AS album_track_rows
          FROM spotify_album_track
          GROUP BY spotify_album_id
        ),
        base AS (
          SELECT
            ra.id AS release_album_id,
            ra.primary_name AS release_album_name,
            COALESCE(pa.artist_name, 'Unknown artist') AS artist_name,
            rsa.spotify_album_id AS spotify_album_id,
            sac.name AS spotify_album_name,
            sac.total_tracks AS total_tracks,
            COALESCE(atc.album_track_rows, 0) AS album_track_rows,
            CASE
              WHEN q.id IS NULL THEN 'not_queued'
              ELSE q.status
            END AS queue_status,
            sac.last_status AS catalog_status
          FROM release_album ra
          LEFT JOIN primary_artists pa
            ON pa.release_album_id = ra.id
          LEFT JOIN representative_spotify_album rsa
            ON rsa.release_album_id = ra.id
          LEFT JOIN spotify_album_catalog sac
            ON sac.spotify_album_id = rsa.spotify_album_id
          LEFT JOIN album_track_counts atc
            ON atc.spotify_album_id = rsa.spotify_album_id
          LEFT JOIN spotify_catalog_backfill_queue q
            ON q.entity_type = 'album'
           AND q.spotify_id = rsa.spotify_album_id
          WHERE rsa.spotify_album_id IS NOT NULL
            AND rsa.spotify_album_id != ''
        ),
        duplicate_groups AS (
          SELECT
            spotify_album_id,
            max(spotify_album_name) AS spotify_album_name,
            count(*) AS duplicate_count
          FROM base
          GROUP BY spotify_album_id
          HAVING count(*) > 1
        )
    """
    total_query = f"{base_sql} SELECT count(*) FROM duplicate_groups"
    items_query = f"""
        {base_sql}
        SELECT
          dg.spotify_album_id,
          dg.spotify_album_name,
          dg.duplicate_count,
          b.release_album_id,
          b.release_album_name,
          b.artist_name,
          b.album_track_rows,
          b.total_tracks,
          b.catalog_status,
          b.queue_status
        FROM duplicate_groups dg
        JOIN base b
          ON b.spotify_album_id = dg.spotify_album_id
        ORDER BY
          dg.duplicate_count DESC,
          dg.spotify_album_id ASC,
          b.release_album_name ASC,
          b.release_album_id ASC
        LIMIT ?
        OFFSET ?
    """
    with sqlite_connection() as connection:
        total = int(connection.execute(total_query).fetchone()[0])
        rows = connection.execute(items_query, (bounded_limit, bounded_offset)).fetchall()

    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        spotify_album_id = str(row[0] or "")
        if not spotify_album_id:
            continue
        if spotify_album_id not in grouped:
            grouped[spotify_album_id] = {
                "spotify_album_id": spotify_album_id,
                "spotify_album_name": row[1],
                "duplicate_count": int(row[2] or 0),
                "release_albums": [],
            }
        grouped[spotify_album_id]["release_albums"].append(
            {
                "release_album_id": int(row[3]),
                "release_album_name": str(row[4] or ""),
                "artist_name": str(row[5] or "Unknown artist"),
                "album_track_rows": int(row[6] or 0),
                "total_tracks": int(row[7]) if row[7] is not None else None,
                "catalog_status": row[8],
                "queue_status": str(row[9] or "not_queued"),
            }
        )

    items = list(grouped.values())
    return {"ok": True, "items": items, "total": total}


def search_album_catalog_duplicate_by_name_identities(
    *,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 500))
    bounded_offset = max(0, int(offset))
    base_sql = """
        WITH primary_artists AS (
          SELECT
            ordered.release_album_id,
            group_concat(ordered.artist_name, ', ') AS artist_name
          FROM (
            SELECT
              aa.release_album_id,
              a.canonical_name AS artist_name
            FROM album_artist aa
            JOIN artist a
              ON a.id = aa.artist_id
            WHERE aa.role = 'primary'
            ORDER BY aa.release_album_id, COALESCE(aa.billing_index, 999999), aa.id, a.canonical_name
          ) ordered
          GROUP BY ordered.release_album_id
        ),
        primary_artist_first AS (
          SELECT release_album_id, artist_name
          FROM (
            SELECT
              aa.release_album_id,
              a.canonical_name AS artist_name,
              row_number() OVER (
                PARTITION BY aa.release_album_id
                ORDER BY COALESCE(aa.billing_index, 999999), aa.id, a.canonical_name
              ) AS rn
            FROM album_artist aa
            JOIN artist a
              ON a.id = aa.artist_id
            WHERE aa.role = 'primary'
          )
          WHERE rn = 1
        ),
        mapped_album_candidates AS (
          SELECT
            sam.release_album_id,
            sa.external_id AS spotify_album_id,
            1 AS source_priority,
            0 AS listen_count
          FROM source_album sa
          JOIN source_album_map sam
            ON sam.source_album_id = sa.id
          WHERE sa.source_name = 'spotify'
            AND sa.external_id IS NOT NULL
            AND sa.external_id != ''
            AND sam.status = 'accepted'
          UNION ALL
          SELECT
            sam.release_album_id,
            sa.external_id AS spotify_album_id,
            2 AS source_priority,
            0 AS listen_count
          FROM source_album sa
          JOIN source_album_map sam
            ON sam.source_album_id = sa.id
          WHERE sa.source_name = 'spotify'
            AND sa.external_id IS NOT NULL
            AND sa.external_id != ''
        ),
        raw_album_candidates AS (
          SELECT
            at.release_album_id AS release_album_id,
            rpe.spotify_album_id AS spotify_album_id,
            3 AS source_priority,
            count(*) AS listen_count
          FROM raw_play_event rpe
          JOIN source_track st
            ON st.source_name = 'spotify'
           AND st.external_id = rpe.spotify_track_id
          JOIN source_track_map stm
            ON stm.source_track_id = st.id
           AND stm.status = 'accepted'
          JOIN album_track at
            ON at.release_track_id = stm.release_track_id
          WHERE rpe.spotify_album_id IS NOT NULL
            AND rpe.spotify_album_id != ''
          GROUP BY at.release_album_id, rpe.spotify_album_id
        ),
        fallback_local_album_candidates AS (
          SELECT
            ra.id AS release_album_id,
            sac.spotify_album_id AS spotify_album_id,
            4 AS source_priority,
            0 AS listen_count
          FROM release_album ra
          JOIN primary_artist_first paf
            ON paf.release_album_id = ra.id
          JOIN spotify_album_catalog sac
            ON lower(trim(COALESCE(sac.name, ''))) = lower(trim(COALESCE(ra.primary_name, '')))
          JOIN json_each(COALESCE(sac.artists_json, '[]')) artist_json
          WHERE lower(trim(COALESCE(json_extract(artist_json.value, '$.name'), '')))
            = lower(trim(COALESCE(paf.artist_name, '')))
        ),
        all_album_candidates AS (
          SELECT * FROM mapped_album_candidates
          UNION ALL
          SELECT * FROM raw_album_candidates
          UNION ALL
          SELECT * FROM fallback_local_album_candidates
        ),
        representative_spotify_album AS (
          SELECT release_album_id, spotify_album_id
          FROM (
            SELECT
              release_album_id,
              spotify_album_id,
              row_number() OVER (
                PARTITION BY release_album_id
                ORDER BY
                  source_priority ASC,
                  listen_count DESC,
                  spotify_album_id ASC
              ) AS rn
            FROM all_album_candidates
          )
          WHERE rn = 1
        ),
        album_track_counts AS (
          SELECT
            spotify_album_id,
            count(*) AS album_track_rows
          FROM spotify_album_track
          GROUP BY spotify_album_id
        ),
        base AS (
          SELECT
            ra.id AS release_album_id,
            ra.primary_name AS release_album_name,
            COALESCE(pa.artist_name, 'Unknown artist') AS artist_name,
            rsa.spotify_album_id AS spotify_album_id,
            sac.name AS spotify_album_name,
            sac.total_tracks AS total_tracks,
            COALESCE(atc.album_track_rows, 0) AS album_track_rows,
            CASE
              WHEN q.id IS NULL THEN 'not_queued'
              ELSE q.status
            END AS queue_status,
            sac.last_status AS catalog_status
          FROM release_album ra
          LEFT JOIN primary_artists pa
            ON pa.release_album_id = ra.id
          LEFT JOIN representative_spotify_album rsa
            ON rsa.release_album_id = ra.id
          LEFT JOIN spotify_album_catalog sac
            ON sac.spotify_album_id = rsa.spotify_album_id
          LEFT JOIN album_track_counts atc
            ON atc.spotify_album_id = rsa.spotify_album_id
          LEFT JOIN spotify_catalog_backfill_queue q
            ON q.entity_type = 'album'
           AND q.spotify_id = rsa.spotify_album_id
        )
        SELECT
          release_album_id,
          release_album_name,
          artist_name,
          spotify_album_id,
          spotify_album_name,
          album_track_rows,
          total_tracks,
          catalog_status,
          queue_status
        FROM base
        ORDER BY release_album_name ASC, release_album_id ASC
    """
    with sqlite_connection() as connection:
        rows = connection.execute(base_sql).fetchall()

    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        normalized_album_name = _normalize_identity_text(row[1])
        normalized_primary_artist = _primary_artist_key(row[2])
        if not normalized_album_name or not normalized_primary_artist:
            continue
        group_key = (normalized_album_name, normalized_primary_artist)
        if group_key not in groups:
            groups[group_key] = {
                "normalized_album_name": normalized_album_name,
                "normalized_primary_artist": normalized_primary_artist,
                "spotify_album_ids": [],
                "release_albums": [],
            }
        group = groups[group_key]
        spotify_album_id = str(row[3] or "").strip()
        if spotify_album_id and spotify_album_id not in group["spotify_album_ids"]:
            group["spotify_album_ids"].append(spotify_album_id)
        group["release_albums"].append(
            {
                "release_album_id": int(row[0]),
                "release_album_name": str(row[1] or ""),
                "artist_name": str(row[2] or "Unknown artist"),
                "spotify_album_id": row[3],
                "spotify_album_name": row[4],
                "album_track_rows": int(row[5] or 0),
                "total_tracks": int(row[6]) if row[6] is not None else None,
                "catalog_status": row[7],
                "queue_status": str(row[8] or "not_queued"),
            }
        )

    duplicate_groups = []
    for group in groups.values():
        duplicate_count = len(group["release_albums"])
        if duplicate_count <= 1:
            continue
        group["duplicate_count"] = duplicate_count
        duplicate_groups.append(group)

    duplicate_groups.sort(
        key=lambda item: (
            -int(item["duplicate_count"]),
            str(item["normalized_primary_artist"]),
            str(item["normalized_album_name"]),
        )
    )
    total = len(duplicate_groups)
    items = duplicate_groups[bounded_offset : bounded_offset + bounded_limit]
    return {"ok": True, "items": items, "total": total}


def preview_release_album_merge(release_album_ids: list[int]) -> dict[str, Any]:
    requested_ids = []
    seen_requested_ids: set[int] = set()
    for raw_id in release_album_ids:
        try:
            release_album_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        if release_album_id <= 0 or release_album_id in seen_requested_ids:
            continue
        seen_requested_ids.add(release_album_id)
        requested_ids.append(release_album_id)

    if len(requested_ids) < 2:
        return {
            "ok": False,
            "survivor_release_album_id": None,
            "merge_release_album_ids": [],
            "merge_readiness": "unsafe",
            "readiness_reasons": ["At least two release album IDs are required."],
            "warnings": ["Select at least two release albums to preview a merge."],
            "affected": {
                "source_album_map_rows": 0,
                "album_artist_rows": 0,
                "release_track_rows": 0,
                "album_track_rows": 0,
                "album_track_conflicts": 0,
                "raw_play_event_rows": 0,
            },
            "proposed_operations": [],
        }

    placeholders = ",".join("?" for _ in requested_ids)
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        album_rows = connection.execute(
            f"""
            WITH track_counts AS (
              SELECT release_album_id, count(*) AS album_track_rows
              FROM album_track
              WHERE release_album_id IN ({placeholders})
              GROUP BY release_album_id
            ),
            direct_spotify AS (
              SELECT
                sam.release_album_id,
                min(sa.external_id) AS spotify_album_id,
                max(CASE WHEN sam.status = 'accepted' THEN 1 ELSE 0 END) AS has_accepted_spotify_map
              FROM source_album_map sam
              JOIN source_album sa
                ON sa.id = sam.source_album_id
              WHERE sam.release_album_id IN ({placeholders})
                AND sa.source_name = 'spotify'
                AND sa.external_id IS NOT NULL
                AND sa.external_id != ''
              GROUP BY sam.release_album_id
            ),
            catalog_matches AS (
              SELECT
                ds.release_album_id,
                max(CASE WHEN sac.spotify_album_id IS NOT NULL THEN 1 ELSE 0 END) AS has_catalog_match
              FROM direct_spotify ds
              LEFT JOIN spotify_album_catalog sac
                ON sac.spotify_album_id = ds.spotify_album_id
              GROUP BY ds.release_album_id
            ),
            raw_listens AS (
              SELECT
                at.release_album_id,
                count(DISTINCT rpe.id) AS raw_play_event_rows
              FROM album_track at
              JOIN source_track_map stm
                ON stm.release_track_id = at.release_track_id
              JOIN source_track st
                ON st.id = stm.source_track_id
              JOIN raw_play_event rpe
                ON rpe.spotify_track_id = st.external_id
              WHERE at.release_album_id IN ({placeholders})
                AND st.source_name IN ('spotify', 'spotify_uri')
              GROUP BY at.release_album_id
            ),
            primary_artists AS (
              SELECT
                ordered.release_album_id,
                group_concat(ordered.artist_name, ', ') AS artist_name
              FROM (
                SELECT
                  aa.release_album_id,
                  a.canonical_name AS artist_name
                FROM album_artist aa
                JOIN artist a
                  ON a.id = aa.artist_id
                WHERE aa.release_album_id IN ({placeholders})
                  AND aa.role = 'primary'
                ORDER BY aa.release_album_id, COALESCE(aa.billing_index, 999999), aa.id
              ) ordered
              GROUP BY ordered.release_album_id
            )
            SELECT
              ra.id AS release_album_id,
              ra.primary_name AS release_album_name,
              ra.normalized_name AS normalized_album_name,
              COALESCE(pa.artist_name, '') AS artist_name,
              COALESCE(ds.has_accepted_spotify_map, 0) AS has_accepted_spotify_map,
              COALESCE(cm.has_catalog_match, 0) AS has_catalog_match,
              COALESCE(tc.album_track_rows, 0) AS album_track_rows,
              COALESCE(rl.raw_play_event_rows, 0) AS raw_play_event_rows,
              ds.spotify_album_id AS spotify_album_id
            FROM release_album ra
            LEFT JOIN primary_artists pa
              ON pa.release_album_id = ra.id
            LEFT JOIN direct_spotify ds
              ON ds.release_album_id = ra.id
            LEFT JOIN catalog_matches cm
              ON cm.release_album_id = ra.id
            LEFT JOIN track_counts tc
              ON tc.release_album_id = ra.id
            LEFT JOIN raw_listens rl
              ON rl.release_album_id = ra.id
            WHERE ra.id IN ({placeholders})
            """,
            requested_ids * 5,
        ).fetchall()

        found_ids = {int(row["release_album_id"]) for row in album_rows}
        missing_ids = [release_album_id for release_album_id in requested_ids if release_album_id not in found_ids]

        if len(album_rows) < 2:
            return {
                "ok": False,
                "survivor_release_album_id": int(album_rows[0]["release_album_id"]) if album_rows else None,
                "merge_release_album_ids": [],
                "merge_readiness": "unsafe",
                "readiness_reasons": ["At least two requested release albums must exist."],
                "warnings": ["At least two requested release albums must exist."],
                "affected": {
                    "source_album_map_rows": 0,
                    "album_artist_rows": 0,
                    "release_track_rows": 0,
                    "album_track_rows": 0,
                    "album_track_conflicts": 0,
                    "raw_play_event_rows": 0,
                },
                "proposed_operations": [],
            }

        survivor_row = sorted(
            album_rows,
            key=lambda row: (
                -int(row["has_accepted_spotify_map"] or 0),
                -int(row["has_catalog_match"] or 0),
                -(int(row["album_track_rows"] or 0) + int(row["raw_play_event_rows"] or 0)),
                int(row["release_album_id"]),
            ),
        )[0]
        survivor_release_album_id = int(survivor_row["release_album_id"])
        merge_release_album_ids = [
            int(row["release_album_id"])
            for row in album_rows
            if int(row["release_album_id"]) != survivor_release_album_id
        ]
        merge_placeholders = ",".join("?" for _ in merge_release_album_ids)

        if merge_release_album_ids:
            source_album_map_rows = int(
                connection.execute(
                    f"SELECT count(*) FROM source_album_map WHERE release_album_id IN ({merge_placeholders})",
                    merge_release_album_ids,
                ).fetchone()[0]
            )
            album_artist_rows = int(
                connection.execute(
                    f"SELECT count(*) FROM album_artist WHERE release_album_id IN ({merge_placeholders})",
                    merge_release_album_ids,
                ).fetchone()[0]
            )
            album_track_rows = int(
                connection.execute(
                    f"SELECT count(*) FROM album_track WHERE release_album_id IN ({merge_placeholders})",
                    merge_release_album_ids,
                ).fetchone()[0]
            )
            release_track_rows = int(
                connection.execute(
                    f"SELECT count(DISTINCT release_track_id) FROM album_track WHERE release_album_id IN ({merge_placeholders})",
                    merge_release_album_ids,
                ).fetchone()[0]
            )
            album_track_conflicts = int(
                connection.execute(
                    f"""
                    SELECT count(*)
                    FROM album_track duplicate_at
                    JOIN album_track survivor_at
                      ON survivor_at.release_track_id = duplicate_at.release_track_id
                     AND survivor_at.release_album_id = ?
                    WHERE duplicate_at.release_album_id IN ({merge_placeholders})
                    """,
                    [survivor_release_album_id] + merge_release_album_ids,
                ).fetchone()[0]
            )
        else:
            source_album_map_rows = 0
            album_artist_rows = 0
            album_track_rows = 0
            release_track_rows = 0
            album_track_conflicts = 0

        spotify_album_ids = sorted({str(row["spotify_album_id"]) for row in album_rows if row["spotify_album_id"]})
        raw_play_event_rows = 0
        if spotify_album_ids:
            spotify_placeholders = ",".join("?" for _ in spotify_album_ids)
            raw_play_event_rows = int(
                connection.execute(
                    f"SELECT count(*) FROM raw_play_event WHERE spotify_album_id IN ({spotify_placeholders})",
                    spotify_album_ids,
                ).fetchone()[0]
            )

    warnings = []
    if missing_ids:
        warnings.append(f"Requested release album IDs not found: {', '.join(str(value) for value in missing_ids)}.")
    normalized_album_names = {
        _normalize_identity_text(row["release_album_name"] or row["normalized_album_name"])
        for row in album_rows
        if _normalize_identity_text(row["release_album_name"] or row["normalized_album_name"])
    }
    primary_artist_keys = {
        _primary_artist_key(row["artist_name"])
        for row in album_rows
        if _primary_artist_key(row["artist_name"])
    }
    if len(spotify_album_ids) > 1:
        warnings.append(f"Multiple Spotify album IDs are involved: {', '.join(spotify_album_ids)}.")
    if len(normalized_album_names) > 1:
        warnings.append("Requested albums have different normalized album names.")
    if len(primary_artist_keys) > 1:
        warnings.append("Requested albums have different normalized primary artists.")

    readiness_reasons: list[str] = []
    has_missing_ids = bool(missing_ids)
    has_name_mismatch = len(normalized_album_names) > 1
    has_artist_mismatch = len(primary_artist_keys) > 1
    has_multiple_spotify_ids = len(spotify_album_ids) > 1
    has_strong_spotify_evidence = len(spotify_album_ids) == 1 and any(
        int(row["has_accepted_spotify_map"] or 0) == 1 or int(row["has_catalog_match"] or 0) == 1
        for row in album_rows
    )

    if has_missing_ids:
        readiness_reasons.append("One or more requested release album IDs were not found.")
    if has_name_mismatch:
        readiness_reasons.append("Requested albums have different normalized album names.")
    if has_artist_mismatch:
        readiness_reasons.append("Requested albums have different normalized primary artists.")
    if has_multiple_spotify_ids:
        readiness_reasons.append("Multiple distinct Spotify album IDs are involved.")
    if album_track_conflicts > 0:
        readiness_reasons.append("Some album-track rows would collide and need deduping.")
    if not has_strong_spotify_evidence:
        readiness_reasons.append("No strong single Spotify album evidence was found.")

    if has_missing_ids or has_name_mismatch or has_artist_mismatch:
        merge_readiness = "unsafe"
    elif has_multiple_spotify_ids or album_track_conflicts > 0 or not has_strong_spotify_evidence:
        merge_readiness = "needs_review"
    else:
        merge_readiness = "safe_candidate"
        readiness_reasons.append("Same album name and primary artist with strong single Spotify evidence and no album-track conflicts.")

    proposed_operations = [
        f"Would keep release_album {survivor_release_album_id} as the recommended survivor.",
        f"Would repoint source_album_map rows from {len(merge_release_album_ids)} duplicate album(s) to the survivor, deduping conflicts.",
        "Would repoint album_track.release_album_id rows to the survivor and dedupe duplicate album-track pairs.",
        "Would dedupe album_artist rows against the survivor by artist and role.",
        "Would not change release_track rows directly; album membership lives in album_track.",
        "Would not mutate Spotify catalog tables.",
        "Would not mutate analysis mappings in preview.",
    ]

    return {
        "ok": True,
        "survivor_release_album_id": survivor_release_album_id,
        "merge_release_album_ids": merge_release_album_ids,
        "merge_readiness": merge_readiness,
        "readiness_reasons": readiness_reasons,
        "warnings": warnings,
        "affected": {
            "source_album_map_rows": source_album_map_rows,
            "album_artist_rows": album_artist_rows,
            "release_track_rows": release_track_rows,
            "album_track_rows": album_track_rows,
            "album_track_conflicts": album_track_conflicts,
            "raw_play_event_rows": raw_play_event_rows,
        },
        "proposed_operations": proposed_operations,
    }


def dry_run_release_album_merge(
    release_album_ids: list[int],
    *,
    survivor_release_album_id: int | None,
) -> dict[str, Any]:
    preview = preview_release_album_merge(release_album_ids)
    readiness = str(preview.get("merge_readiness") or "unsafe")
    recommended_survivor_id = preview.get("survivor_release_album_id")
    requested_survivor_id = int(survivor_release_album_id) if survivor_release_album_id is not None else None
    blocked_reasons: list[str] = []
    if readiness == "unsafe":
        blocked_reasons.extend(str(reason) for reason in preview.get("readiness_reasons", []))
    if requested_survivor_id != recommended_survivor_id:
        blocked_reasons.append("Requested survivor does not match merge-preview recommendation.")

    base_response: dict[str, Any] = {
        "ok": not blocked_reasons,
        "blocked": bool(blocked_reasons),
        "blocked_reasons": blocked_reasons,
        "merge_readiness": readiness,
        "readiness_reasons": preview.get("readiness_reasons", []),
        "survivor_release_album_id": recommended_survivor_id,
        "merge_release_album_ids": preview.get("merge_release_album_ids", []),
        "rows_affected": {
            "source_album_map": 0,
            "album_artist_insert": 0,
            "album_artist_delete": 0,
            "album_track_repoint": 0,
            "album_track_conflict_delete": 0,
            "release_album_retire": 0,
        },
        "plan": {
            "source_album_map_repoints": [],
            "album_artist_inserts": [],
            "album_artist_deletes": [],
            "album_track_repoints": [],
            "album_track_conflicts": [],
            "release_album_retirements": [],
        },
        "statements": [
            "release_track rows are not changed directly.",
            "spotify catalog tables are not changed.",
            "analysis_track_map is not changed.",
        ],
    }
    if blocked_reasons:
        return base_response

    merge_ids = [int(value) for value in preview.get("merge_release_album_ids", [])]
    if not merge_ids or recommended_survivor_id is None:
        return base_response

    merge_placeholders = ",".join("?" for _ in merge_ids)
    survivor_id = int(recommended_survivor_id)
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        source_album_map_repoints = [
            {
                "source_album_map_id": int(row["id"]),
                "source_album_id": int(row["source_album_id"]),
                "source_name": row["source_name"],
                "external_id": row["external_id"],
                "from_release_album_id": int(row["release_album_id"]),
                "to_release_album_id": survivor_id,
                "would_conflict": bool(row["would_conflict"]),
            }
            for row in connection.execute(
                f"""
                SELECT
                  sam.id,
                  sam.source_album_id,
                  sa.source_name,
                  sa.external_id,
                  sam.release_album_id,
                  EXISTS (
                    SELECT 1
                    FROM source_album_map existing
                    WHERE existing.source_album_id = sam.source_album_id
                      AND existing.release_album_id = ?
                  ) AS would_conflict
                FROM source_album_map sam
                JOIN source_album sa
                  ON sa.id = sam.source_album_id
                WHERE sam.release_album_id IN ({merge_placeholders})
                ORDER BY sam.release_album_id, sam.id
                """,
                [survivor_id] + merge_ids,
            ).fetchall()
        ]
        album_artist_rows = connection.execute(
            f"""
            SELECT
              aa.id,
              aa.release_album_id,
              aa.artist_id,
              a.canonical_name AS artist_name,
              aa.role,
              aa.billing_index,
              aa.credited_as,
              EXISTS (
                SELECT 1
                FROM album_artist survivor_aa
                WHERE survivor_aa.release_album_id = ?
                  AND survivor_aa.artist_id = aa.artist_id
                  AND survivor_aa.role = aa.role
              ) AS would_conflict
            FROM album_artist aa
            JOIN artist a
              ON a.id = aa.artist_id
            WHERE aa.release_album_id IN ({merge_placeholders})
            ORDER BY aa.release_album_id, COALESCE(aa.billing_index, 999999), aa.id
            """,
            [survivor_id] + merge_ids,
        ).fetchall()
        album_artist_inserts = [
            {
                "from_album_artist_id": int(row["id"]),
                "to_release_album_id": survivor_id,
                "artist_id": int(row["artist_id"]),
                "artist_name": row["artist_name"],
                "role": row["role"],
                "billing_index": row["billing_index"],
                "credited_as": row["credited_as"],
            }
            for row in album_artist_rows
            if not bool(row["would_conflict"])
        ]
        album_artist_deletes = [
            {
                "album_artist_id": int(row["id"]),
                "release_album_id": int(row["release_album_id"]),
                "artist_id": int(row["artist_id"]),
                "artist_name": row["artist_name"],
                "role": row["role"],
                "reason": "duplicate album artist row would be retired after survivor insert/dedupe",
            }
            for row in album_artist_rows
        ]
        album_track_rows = connection.execute(
            f"""
            SELECT
              duplicate_at.id,
              duplicate_at.release_album_id,
              duplicate_at.release_track_id,
              rt.primary_name AS release_track_name,
              survivor_at.id AS survivor_album_track_id
            FROM album_track duplicate_at
            JOIN release_track rt
              ON rt.id = duplicate_at.release_track_id
            LEFT JOIN album_track survivor_at
              ON survivor_at.release_album_id = ?
             AND survivor_at.release_track_id = duplicate_at.release_track_id
            WHERE duplicate_at.release_album_id IN ({merge_placeholders})
            ORDER BY duplicate_at.release_album_id, duplicate_at.id
            """,
            [survivor_id] + merge_ids,
        ).fetchall()
        album_track_repoints = [
            {
                "album_track_id": int(row["id"]),
                "from_release_album_id": int(row["release_album_id"]),
                "to_release_album_id": survivor_id,
                "release_track_id": int(row["release_track_id"]),
                "release_track_name": row["release_track_name"],
            }
            for row in album_track_rows
            if row["survivor_album_track_id"] is None
        ]
        album_track_conflicts = [
            {
                "album_track_id": int(row["id"]),
                "conflicts_with_album_track_id": int(row["survivor_album_track_id"]),
                "from_release_album_id": int(row["release_album_id"]),
                "survivor_release_album_id": survivor_id,
                "release_track_id": int(row["release_track_id"]),
                "release_track_name": row["release_track_name"],
                "resolution": "delete or skip duplicate album_track row",
            }
            for row in album_track_rows
            if row["survivor_album_track_id"] is not None
        ]
        release_album_retirements = [
            {
                "release_album_id": int(row["id"]),
                "release_album_name": row["primary_name"],
                "reason": "duplicate release_album would be retired after references move to survivor",
            }
            for row in connection.execute(
                f"""
                SELECT id, primary_name
                FROM release_album
                WHERE id IN ({merge_placeholders})
                ORDER BY id
                """,
                merge_ids,
            ).fetchall()
        ]

    base_response["rows_affected"] = {
        "source_album_map": len(source_album_map_repoints),
        "album_artist_insert": len(album_artist_inserts),
        "album_artist_delete": len(album_artist_deletes),
        "album_track_repoint": len(album_track_repoints),
        "album_track_conflict_delete": len(album_track_conflicts),
        "release_album_retire": len(release_album_retirements),
    }
    base_response["plan"] = {
        "source_album_map_repoints": source_album_map_repoints,
        "album_artist_inserts": album_artist_inserts,
        "album_artist_deletes": album_artist_deletes,
        "album_track_repoints": album_track_repoints,
        "album_track_conflicts": album_track_conflicts,
        "release_album_retirements": release_album_retirements,
    }
    return base_response


def search_track_catalog_lookup(
    *,
    q: str | None = None,
    catalog_status: str = "all",
    queue_status: str | None = "all",
    sort: str | None = "default",
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 200))
    bounded_offset = max(0, int(offset))
    normalized_status = str(catalog_status or "all").strip().lower()
    if normalized_status not in {"all", "backfilled", "not_backfilled", "duration_missing", "error"}:
        normalized_status = "all"
    normalized_queue_status = str(queue_status or "all").strip().lower()
    if normalized_queue_status not in {"all", "not_queued", "pending", "done", "error"}:
        normalized_queue_status = "all"
    normalized_sort = str(sort or "default").strip().lower()
    if normalized_sort not in {"default", "recently_backfilled", "name", "incomplete_first"}:
        normalized_sort = "default"
    normalized_q = str(q or "").strip()
    like_q = f"%{normalized_q.lower()}%"

    where_clauses: list[str] = []
    params: list[Any] = []
    if normalized_q:
        where_clauses.append(
            "("
            "lower(COALESCE(base.release_track_name, '')) LIKE ? "
            "OR lower(COALESCE(base.artist_name, '')) LIKE ? "
            "OR lower(COALESCE(base.release_album_name, '')) LIKE ? "
            "OR lower(COALESCE(base.spotify_track_id, '')) LIKE ?"
            ")"
        )
        params.extend([like_q, like_q, like_q, like_q])

    if normalized_status == "backfilled":
        where_clauses.append("base.has_catalog_row = 1")
    elif normalized_status == "not_backfilled":
        where_clauses.append("base.has_catalog_row = 0")
    elif normalized_status == "duration_missing":
        where_clauses.append("(base.has_catalog_row = 0 OR base.duration_ms IS NULL)")
    elif normalized_status == "error":
        where_clauses.append("(lower(COALESCE(base.catalog_last_status, '')) = 'error' OR base.catalog_last_error IS NOT NULL)")
    if normalized_queue_status != "all":
        where_clauses.append("base.queue_status = ?")
        params.append(normalized_queue_status)

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    base_sql = """
        WITH raw_track_listens AS (
          SELECT
            spotify_track_id,
            count(*) AS listen_count
          FROM raw_play_event
          WHERE spotify_track_id IS NOT NULL
            AND spotify_track_id != ''
          GROUP BY spotify_track_id
        ),
        spotify_track_candidates AS (
          SELECT
            stm.release_track_id,
            st.external_id AS spotify_track_id,
            COALESCE(rtl.listen_count, 0) AS listen_count,
            st.id AS source_track_row_id,
            stm.id AS source_track_map_row_id
          FROM source_track st
          JOIN source_track_map stm
            ON stm.source_track_id = st.id
          LEFT JOIN raw_track_listens rtl
            ON rtl.spotify_track_id = st.external_id
          WHERE st.source_name = 'spotify'
            AND st.external_id IS NOT NULL
            AND st.external_id != ''
            AND stm.status = 'accepted'
        ),
        representative_spotify_track AS (
          SELECT release_track_id, spotify_track_id
          FROM (
            SELECT
              release_track_id,
              spotify_track_id,
              row_number() OVER (
                PARTITION BY release_track_id
                ORDER BY
                  listen_count DESC,
                  spotify_track_id ASC,
                  source_track_map_row_id ASC,
                  source_track_row_id ASC
              ) AS rn
            FROM spotify_track_candidates
          )
          WHERE rn = 1
        ),
        primary_artists AS (
          SELECT
            ordered.release_track_id,
            group_concat(ordered.artist_name, ', ') AS artist_name
          FROM (
            SELECT
              ta.release_track_id,
              a.canonical_name AS artist_name
            FROM track_artist ta
            JOIN artist a
              ON a.id = ta.artist_id
            WHERE ta.role = 'primary'
            ORDER BY ta.release_track_id, COALESCE(ta.billing_index, 999999), ta.id, a.canonical_name
          ) ordered
          GROUP BY ordered.release_track_id
        ),
        primary_albums AS (
          SELECT release_track_id, release_album_name
          FROM (
            SELECT
              at.release_track_id,
              ra.primary_name AS release_album_name,
              row_number() OVER (
                PARTITION BY at.release_track_id
                ORDER BY at.id ASC, ra.primary_name ASC, ra.id ASC
              ) AS rn
            FROM album_track at
            JOIN release_album ra
              ON ra.id = at.release_album_id
          )
          WHERE rn = 1
        ),
        base AS (
          SELECT
            rt.id AS release_track_id,
            rt.primary_name AS release_track_name,
            COALESCE(pa.artist_name, 'Unknown artist') AS artist_name,
            COALESCE(pal.release_album_name, 'Unknown album') AS release_album_name,
            rst.spotify_track_id AS spotify_track_id,
            stc.name AS spotify_track_name,
            stc.duration_ms AS duration_ms,
            stc.album_id AS album_id,
            stc.fetched_at AS catalog_fetched_at,
            stc.last_status AS catalog_last_status,
            stc.last_error AS catalog_last_error,
            CASE WHEN stc.spotify_track_id IS NULL THEN 0 ELSE 1 END AS has_catalog_row,
            CASE
              WHEN q.id IS NULL THEN 'not_queued'
              ELSE q.status
            END AS queue_status,
            q.priority AS queue_priority,
            q.requested_at AS queue_requested_at,
            q.attempts AS queue_attempts,
            q.last_error AS queue_last_error
          FROM release_track rt
          LEFT JOIN primary_artists pa
            ON pa.release_track_id = rt.id
          LEFT JOIN primary_albums pal
            ON pal.release_track_id = rt.id
          LEFT JOIN representative_spotify_track rst
            ON rst.release_track_id = rt.id
          LEFT JOIN spotify_track_catalog stc
            ON stc.spotify_track_id = rst.spotify_track_id
          LEFT JOIN spotify_catalog_backfill_queue q
            ON q.entity_type = 'track'
           AND q.spotify_id = rst.spotify_track_id
        )
    """

    total_query = f"{base_sql} SELECT count(*) FROM base {where_sql}"
    if normalized_sort == "recently_backfilled":
        order_sql = """
            ORDER BY
              CASE WHEN catalog_fetched_at IS NULL THEN 1 ELSE 0 END ASC,
              catalog_fetched_at DESC,
              release_track_name ASC,
              release_track_id ASC
        """
    elif normalized_sort == "name":
        order_sql = """
            ORDER BY
              release_track_name ASC,
              release_track_id ASC
        """
    else:
        order_sql = """
            ORDER BY
              CASE
                WHEN lower(COALESCE(catalog_last_status, '')) = 'error' OR catalog_last_error IS NOT NULL THEN 1
                WHEN has_catalog_row = 0 OR duration_ms IS NULL THEN 2
                ELSE 3
              END ASC,
              release_track_name ASC,
              release_track_id ASC
        """

    items_query = f"""
        {base_sql}
        SELECT
          release_track_id,
          release_track_name,
          artist_name,
          release_album_name,
          spotify_track_id,
          spotify_track_name,
          duration_ms,
          album_id,
          catalog_fetched_at,
          catalog_last_status,
          catalog_last_error,
          queue_status,
          queue_priority,
          queue_requested_at,
          queue_attempts,
          queue_last_error
        FROM base
        {where_sql}
        {order_sql}
        LIMIT ?
        OFFSET ?
    """
    with sqlite_connection() as connection:
        total = int(connection.execute(total_query, tuple(params)).fetchone()[0])
        rows = connection.execute(items_query, tuple(params + [bounded_limit, bounded_offset])).fetchall()

    items = [
        {
            "release_track_id": int(row[0]),
            "release_track_name": str(row[1] or ""),
            "artist_name": str(row[2] or "Unknown artist"),
            "release_album_name": str(row[3] or "Unknown album"),
            "spotify_track_id": row[4],
            "spotify_track_name": row[5],
            "duration_ms": int(row[6]) if row[6] is not None else None,
            "duration_display": _duration_display(row[6]),
            "album_id": row[7],
            "catalog_fetched_at": row[8],
            "catalog_last_status": row[9],
            "catalog_last_error": row[10],
            "queue_status": str(row[11] or "not_queued"),
            "queue_priority": int(row[12]) if row[12] is not None else None,
            "queue_requested_at": row[13],
            "queue_attempts": int(row[14]) if row[14] is not None else None,
            "queue_last_error": row[15],
        }
        for row in rows
    ]
    return {"ok": True, "items": items, "total": total}


def search_track_mapping_lineage(
    *,
    q: str | None = None,
    limit: int = 50,
    offset: int = 0,
    source_metadata: str = "all",
    confirmation_certainty: str = "all",
    mapping_kind: str = "source_release",
) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 100))
    bounded_offset = max(0, int(offset))
    normalized_q = str(q or "").strip()
    normalized_source_metadata = str(source_metadata or "all").strip().lower()
    if normalized_source_metadata not in {"all", "complete", "incomplete"}:
        normalized_source_metadata = "all"
    normalized_confirmation_certainty = str(confirmation_certainty or "all").strip().lower()
    if normalized_confirmation_certainty not in {"all", "certain", "uncertain"}:
        normalized_confirmation_certainty = "all"
    normalized_mapping_kind = str(mapping_kind or "all").strip().lower()
    if normalized_mapping_kind not in {"all", "source_release", "release_family"}:
        normalized_mapping_kind = "all"
    like_q = f"%{normalized_q.lower()}%"
    source_search_clause = ""
    source_q_params: tuple[Any, ...] = ()
    if normalized_q:
        source_search_clause = """
          WHERE (
            lower(COALESCE(rt.primary_name, '')) LIKE ?
            OR EXISTS (
              SELECT 1
              FROM track_artist q_ta
              JOIN artist q_a
                ON q_a.id = q_ta.artist_id
              WHERE q_ta.release_track_id = g.release_track_id
                AND q_ta.role = 'primary'
                AND lower(COALESCE(q_a.canonical_name, '')) LIKE ?
            )
            OR EXISTS (
              SELECT 1
              FROM album_track q_alt
              JOIN release_album q_ra
                ON q_ra.id = q_alt.release_album_id
              WHERE q_alt.release_track_id = g.release_track_id
                AND lower(COALESCE(q_ra.primary_name, '')) LIKE ?
            )
            OR EXISTS (
              SELECT 1
              FROM source_track_map detail_stm
              JOIN source_track detail_st
                ON detail_st.id = detail_stm.source_track_id
              LEFT JOIN spotify_track_catalog detail_stc
                ON detail_stc.spotify_track_id = detail_st.external_id
              WHERE detail_stm.release_track_id = g.release_track_id
                AND detail_stm.status = 'accepted'
                AND (
                  lower(COALESCE(detail_st.external_id, '')) LIKE ?
                  OR lower(COALESCE(detail_st.source_name_raw, '')) LIKE ?
                  OR lower(COALESCE(detail_stc.name, '')) LIKE ?
                )
            )
          )
        """
        source_q_params = (like_q, like_q, like_q, like_q, like_q, like_q)
    release_search_clause = ""
    release_q_params: tuple[Any, ...] = ()
    if normalized_q:
        release_search_clause = """
          WHERE (
            lower(COALESCE(at.primary_name, '')) LIKE ?
            OR lower(COALESCE(at.grouping_note, '')) LIKE ?
            OR EXISTS (
              SELECT 1
              FROM analysis_track_map detail_atm
              JOIN release_track detail_rt
                ON detail_rt.id = detail_atm.release_track_id
              WHERE detail_atm.analysis_track_id = g.analysis_track_id
                AND detail_atm.status IN ('accepted', 'suggested')
                AND lower(COALESCE(detail_rt.primary_name, '')) LIKE ?
            )
            OR EXISTS (
              SELECT 1
              FROM analysis_track_map detail_atm
              JOIN track_artist detail_ta
                ON detail_ta.release_track_id = detail_atm.release_track_id
              JOIN artist detail_a
                ON detail_a.id = detail_ta.artist_id
              WHERE detail_atm.analysis_track_id = g.analysis_track_id
                AND detail_atm.status IN ('accepted', 'suggested')
                AND detail_ta.role = 'primary'
                AND lower(COALESCE(detail_a.canonical_name, '')) LIKE ?
            )
            OR EXISTS (
              SELECT 1
              FROM analysis_track_map detail_atm
              JOIN album_track detail_alt
                ON detail_alt.release_track_id = detail_atm.release_track_id
              JOIN release_album detail_ra
                ON detail_ra.id = detail_alt.release_album_id
              WHERE detail_atm.analysis_track_id = g.analysis_track_id
                AND detail_atm.status IN ('accepted', 'suggested')
                AND lower(COALESCE(detail_ra.primary_name, '')) LIKE ?
            )
          )
        """
        release_q_params = (like_q, like_q, like_q, like_q, like_q)

    source_release_base_sql = f"""
        WITH grouped AS (
          SELECT
            stm.release_track_id,
            count(DISTINCT stm.source_track_id) AS source_count
          FROM source_track_map stm
          JOIN source_track st
            ON st.id = stm.source_track_id
          WHERE stm.status = 'accepted'
          GROUP BY stm.release_track_id
          HAVING source_count > 1
        ),
        filtered AS (
          SELECT
            g.release_track_id,
            rt.primary_name AS release_track_name,
            COALESCE(
              (
                SELECT group_concat(artist_rows.artist_name, ', ')
                FROM (
                  SELECT a.canonical_name AS artist_name
                  FROM track_artist ta
                  JOIN artist a
                    ON a.id = ta.artist_id
                  WHERE ta.release_track_id = g.release_track_id
                    AND ta.role = 'primary'
                  ORDER BY COALESCE(ta.billing_index, 999999), ta.id, a.canonical_name
                ) artist_rows
              ),
              'Unknown artist'
            ) AS artist_name,
            COALESCE(
              (
                SELECT ra.primary_name
                FROM album_track alt
                JOIN release_album ra
                  ON ra.id = alt.release_album_id
                WHERE alt.release_track_id = g.release_track_id
                ORDER BY alt.id ASC, ra.primary_name ASC, ra.id ASC
                LIMIT 1
              ),
              'Unknown album'
            ) AS release_album_name,
            g.source_count
          FROM grouped g
          JOIN release_track rt
            ON rt.id = g.release_track_id
          {source_search_clause}
        )
    """
    source_group_query = f"""
        {source_release_base_sql}
        SELECT
          release_track_id,
          release_track_name,
          artist_name,
          release_album_name,
          source_count
        FROM filtered
        ORDER BY source_count DESC, release_track_name ASC, release_track_id ASC
        LIMIT ?
        OFFSET ?
    """
    source_detail_query = """
        SELECT
          st.id AS source_track_id,
          st.source_name,
          st.external_id,
          st.source_name_raw,
          stc.name AS spotify_track_name,
          stc.duration_ms,
          COALESCE(
            NULLIF(TRIM(stc.album_id), ''),
            NULLIF(TRIM(json_extract(COALESCE(stc.raw_json, '{}'), '$.album.id')), '')
          ) AS album_id,
          sac.name AS album_name,
          json_extract(COALESCE(stc.raw_json, '{}'), '$.album.name') AS embedded_album_name,
          salb.source_name_raw AS source_album_name,
          COALESCE(
            NULLIF(TRIM(sac.name), ''),
            NULLIF(TRIM(json_extract(COALESCE(stc.raw_json, '{}'), '$.album.name')), ''),
            NULLIF(TRIM(salb.source_name_raw), '')
          ) AS album_name_display,
          CASE
            WHEN NULLIF(TRIM(sac.name), '') IS NOT NULL THEN 'spotify_album_catalog'
            WHEN NULLIF(TRIM(json_extract(COALESCE(stc.raw_json, '{}'), '$.album.name')), '') IS NOT NULL THEN 'embedded_track_album'
            WHEN NULLIF(TRIM(salb.source_name_raw), '') IS NOT NULL THEN 'source_album'
            ELSE NULL
          END AS album_name_display_source,
          COALESCE(
            NULLIF(TRIM(sac.release_date), ''),
            NULLIF(TRIM(json_extract(COALESCE(stc.raw_json, '{}'), '$.album.release_date')), '')
          ) AS album_release_date,
          COALESCE(sac.total_tracks, json_extract(COALESCE(stc.raw_json, '{}'), '$.album.total_tracks')) AS album_total_tracks,
          json_extract(COALESCE(sac.raw_json, '{}'), '$.copyrights[0].text') AS album_copyright,
          stc.disc_number,
          stc.track_number,
          stc.fetched_at,
          stc.last_status,
          CASE
            WHEN EXISTS (
              SELECT 1
              FROM json_each(CASE WHEN json_valid(COALESCE(stc.artists_json, '[]')) THEN COALESCE(stc.artists_json, '[]') ELSE '[]' END)
              WHERE NULLIF(TRIM(json_extract(value, '$.name')), '') IS NOT NULL
            ) THEN 1 ELSE 0
          END AS has_artist_name,
          NULLIF(TRIM(json_extract(COALESCE(stc.raw_json, '{}'), '$.external_ids.isrc')), '') AS isrc,
          (
            SELECT count(*)
            FROM raw_play_event rpe
            WHERE rpe.spotify_track_id = st.external_id
          ) AS play_count,
          stm.match_method,
          stm.confidence,
          stm.status,
          stm.is_user_confirmed
        FROM source_track_map stm
        JOIN source_track st
          ON st.id = stm.source_track_id
        LEFT JOIN spotify_track_catalog stc
          ON stc.spotify_track_id = st.external_id
        LEFT JOIN spotify_album_catalog sac
          ON sac.spotify_album_id = COALESCE(
            NULLIF(TRIM(stc.album_id), ''),
            NULLIF(TRIM(json_extract(COALESCE(stc.raw_json, '{}'), '$.album.id')), '')
          )
        LEFT JOIN source_album salb
          ON salb.source_name = 'spotify'
         AND salb.external_id = COALESCE(
            NULLIF(TRIM(stc.album_id), ''),
            NULLIF(TRIM(json_extract(COALESCE(stc.raw_json, '{}'), '$.album.id')), '')
          )
        WHERE stm.release_track_id = ?
          AND stm.status = 'accepted'
        ORDER BY st.source_name ASC, st.external_id ASC, st.id ASC
    """

    release_family_base_sql = f"""
        WITH grouped AS (
          SELECT
            atm.analysis_track_id,
            count(DISTINCT atm.release_track_id) AS release_count
          FROM analysis_track_map atm
          WHERE atm.status IN ('accepted', 'suggested')
          GROUP BY atm.analysis_track_id
          HAVING release_count > 1
        ),
        filtered AS (
          SELECT
            g.analysis_track_id,
            at.primary_name AS track_family_name,
            at.grouping_note,
            g.release_count
          FROM grouped g
          JOIN analysis_track at
            ON at.id = g.analysis_track_id
          {release_search_clause}
        )
    """
    release_group_query = f"""
        {release_family_base_sql}
        SELECT analysis_track_id, track_family_name, grouping_note, release_count
        FROM filtered
        ORDER BY release_count DESC, track_family_name ASC, analysis_track_id ASC
        LIMIT ?
        OFFSET ?
    """
    release_detail_query = """
        SELECT
          rt.id AS release_track_id,
          rt.primary_name AS release_track_name,
          COALESCE(
            (
              SELECT group_concat(artist_rows.artist_name, ', ')
              FROM (
                SELECT a.canonical_name AS artist_name
                FROM track_artist ta
                JOIN artist a
                  ON a.id = ta.artist_id
                WHERE ta.release_track_id = rt.id
                  AND ta.role = 'primary'
                ORDER BY COALESCE(ta.billing_index, 999999), ta.id, a.canonical_name
              ) artist_rows
            ),
            'Unknown artist'
          ) AS artist_name,
          COALESCE(
            (
              SELECT ra.primary_name
              FROM album_track alt
              JOIN release_album ra
                ON ra.id = alt.release_album_id
              WHERE alt.release_track_id = rt.id
              ORDER BY alt.id ASC, ra.primary_name ASC, ra.id ASC
              LIMIT 1
            ),
            'Unknown album'
          ) AS release_album_name,
          (
            SELECT count(DISTINCT sc_stm.source_track_id)
            FROM source_track_map sc_stm
            WHERE sc_stm.release_track_id = rt.id
              AND sc_stm.status = 'accepted'
          ) AS source_count,
          (
            SELECT count(rpe.id)
            FROM source_track_map rl_stm
            JOIN source_track rl_st
              ON rl_st.id = rl_stm.source_track_id
            JOIN raw_play_event rpe
              ON rpe.spotify_track_id = rl_st.external_id
            WHERE rl_stm.release_track_id = rt.id
              AND rl_stm.status = 'accepted'
              AND rl_st.source_name = 'spotify'
          ) AS play_count,
          atm.match_method,
          atm.confidence,
          atm.status,
          atm.is_user_confirmed
        FROM analysis_track_map atm
        JOIN release_track rt
          ON rt.id = atm.release_track_id
        WHERE atm.analysis_track_id = ?
          AND atm.status IN ('accepted', 'suggested')
        ORDER BY rt.primary_name ASC, rt.id ASC
    """

    with sqlite_connection() as connection:
        can_use_fast_source_page = (
            normalized_source_metadata == "all"
            and normalized_confirmation_certainty == "all"
        )
        source_total = 0
        source_status_rows = connection.execute(
            """
            SELECT status, is_user_confirmed, count(*) AS row_count
            FROM source_track_map
            GROUP BY status, is_user_confirmed
            ORDER BY status ASC, is_user_confirmed ASC
            """
        ).fetchall()
        release_status_rows = connection.execute(
            """
            SELECT status, is_user_confirmed, count(*) AS row_count
            FROM analysis_track_map
            GROUP BY status, is_user_confirmed
            ORDER BY status ASC, is_user_confirmed ASC
            """
        ).fetchall()
        def build_source_group(row: Any) -> dict[str, Any]:
            release_track_id = int(row[0])
            detail_rows = connection.execute(source_detail_query, (release_track_id,)).fetchall()
            source_items = []
            source_metadata_complete_count = 0
            source_metadata_incomplete_count = 0
            for detail in detail_rows:
                source_name = str(detail[1] or "")
                external_id = str(detail[2] or "").strip() if detail[2] is not None else ""
                metadata_gaps: list[str] = []
                if source_name != "spotify":
                    metadata_gaps.append("non_spotify_source")
                if not external_id:
                    metadata_gaps.append("spotify_track_id")
                if not str(detail[4] or "").strip():
                    metadata_gaps.append("track_name")
                if detail[5] is None:
                    metadata_gaps.append("duration_ms")
                if not str(detail[6] or "").strip():
                    metadata_gaps.append("album_id")
                if detail[15] is None:
                    metadata_gaps.append("disc_number")
                if detail[16] is None:
                    metadata_gaps.append("track_number")
                if str(detail[18] or "").strip().lower() == "error":
                    metadata_gaps.append("last_status")
                if not bool(detail[19]):
                    metadata_gaps.append("artists")
                if not str(detail[20] or "").strip():
                    metadata_gaps.append("isrc")
                if not str(detail[10] or "").strip():
                    metadata_gaps.append("album_display_name")
                metadata_complete = not metadata_gaps
                if metadata_complete:
                    source_metadata_complete_count += 1
                else:
                    source_metadata_incomplete_count += 1
                source_items.append(
                    {
                        "source_track_id": int(detail[0]),
                        "source_name": source_name,
                        "external_id": detail[2],
                        "source_name_raw": detail[3],
                        "spotify_track_name": detail[4],
                        "duration_ms": int(detail[5]) if detail[5] is not None else None,
                        "duration_display": _duration_display(detail[5]),
                        "album_id": detail[6],
                        "album_name": detail[7],
                        "embedded_album_name": detail[8],
                        "source_album_name": detail[9],
                        "album_name_display": detail[10],
                        "album_name_display_source": detail[11],
                        "album_release_date": detail[12],
                        "album_total_tracks": int(detail[13]) if detail[13] is not None else None,
                        "album_copyright": detail[14],
                        "disc_number": int(detail[15]) if detail[15] is not None else None,
                        "track_number": int(detail[16]) if detail[16] is not None else None,
                        "catalog_fetched_at": detail[17],
                        "metadata_complete": metadata_complete,
                        "metadata_gaps": metadata_gaps,
                        "play_count": int(detail[21] or 0),
                        "match_method": str(detail[22] or ""),
                        "confidence": float(detail[23]) if detail[23] is not None else None,
                        "status": str(detail[24] or ""),
                        "is_user_confirmed": bool(detail[25]),
                        "isrc": detail[20],
                    }
                )
            source_group = {
                "release_track_id": release_track_id,
                "release_track_name": str(row[1] or ""),
                "artist_name": str(row[2] or "Unknown artist"),
                "release_album_name": str(row[3] or "Unknown album"),
                "source_count": int(row[4]),
                "source_metadata_complete_count": source_metadata_complete_count,
                "source_metadata_incomplete_count": source_metadata_incomplete_count,
                "all_source_metadata_complete": source_metadata_incomplete_count == 0,
                "sources": source_items,
            }
            source_group["confirmation_preview"] = _source_release_confirmation_preview(source_group)
            return source_group

        def source_group_matches_active_filters(source_group: dict[str, Any]) -> bool:
            metadata_filter = (
                "complete"
                if normalized_confirmation_certainty == "certain"
                else normalized_source_metadata
            )
            if metadata_filter == "complete" and not source_group["all_source_metadata_complete"]:
                return False
            if metadata_filter == "incomplete" and source_group["all_source_metadata_complete"]:
                return False
            readiness = source_group["confirmation_preview"]["readiness"]
            if normalized_confirmation_certainty == "certain":
                return readiness == "safe_candidate"
            if normalized_confirmation_certainty == "uncertain":
                return readiness != "safe_candidate"
            return True

        source_groups: list[dict[str, Any]] = []
        if normalized_mapping_kind == "release_family":
            source_has_more = False
            source_total_is_exact = True
        elif can_use_fast_source_page:
            source_group_rows = connection.execute(
                source_group_query,
                source_q_params + (bounded_limit + 1, bounded_offset),
            ).fetchall()
            source_has_more = len(source_group_rows) > bounded_limit
            source_groups = [build_source_group(row) for row in source_group_rows[:bounded_limit]]
            source_total = bounded_offset + len(source_groups) + (1 if source_has_more else 0)
            source_total_is_exact = False
        else:
            scan_offset = 0
            matched_count = 0
            source_has_more = False
            scanned_count = 0
            max_scan_count = max(500, bounded_offset + bounded_limit + 100)
            while len(source_groups) <= bounded_limit and scanned_count < max_scan_count:
                source_group_rows = connection.execute(
                    source_group_query,
                    source_q_params + (100, scan_offset),
                ).fetchall()
                if not source_group_rows:
                    break
                for row in source_group_rows:
                    scanned_count += 1
                    source_group = build_source_group(row)
                    if not source_group_matches_active_filters(source_group):
                        continue
                    if matched_count >= bounded_offset:
                        source_groups.append(source_group)
                    matched_count += 1
                    if len(source_groups) > bounded_limit:
                        source_has_more = True
                        break
                if source_has_more:
                    break
                scan_offset += 100
            if source_has_more:
                source_groups = source_groups[:bounded_limit]
            source_total = bounded_offset + len(source_groups) + (1 if source_has_more else 0)
            source_total_is_exact = (
                not source_has_more
                and scanned_count < max_scan_count
            )

        release_groups = []
        release_total = 0
        release_has_more = False
        release_total_is_exact = True
        if normalized_mapping_kind in {"all", "release_family"}:
            release_group_rows = connection.execute(
                release_group_query,
                release_q_params + (bounded_limit + 1, bounded_offset),
            ).fetchall()
            release_has_more = len(release_group_rows) > bounded_limit
            release_total = bounded_offset + min(len(release_group_rows), bounded_limit) + (1 if release_has_more else 0)
            release_total_is_exact = False
            for row in release_group_rows[:bounded_limit]:
                analysis_track_id = int(row[0])
                detail_rows = connection.execute(release_detail_query, (analysis_track_id,)).fetchall()
                release_groups.append(
                    {
                        "analysis_track_id": analysis_track_id,
                        "track_family_name": str(row[1] or ""),
                        "grouping_note": row[2],
                        "release_count": int(row[3]),
                        "release_tracks": [
                            {
                                "release_track_id": int(detail[0]),
                                "release_track_name": str(detail[1] or ""),
                                "artist_name": str(detail[2] or "Unknown artist"),
                                "release_album_name": str(detail[3] or "Unknown album"),
                                "source_count": int(detail[4] or 0),
                                "play_count": int(detail[5] or 0),
                                "match_method": str(detail[6] or ""),
                                "confidence": float(detail[7]) if detail[7] is not None else None,
                                "status": str(detail[8] or ""),
                                "is_user_confirmed": bool(detail[9]),
                            }
                            for detail in detail_rows
                        ],
                    }
                )

    return {
        "ok": True,
        "source_release": {
            "total": source_total,
            "groups": source_groups,
            "included_statuses": ["accepted"],
            "source_metadata_filter": normalized_source_metadata,
            "confirmation_certainty_filter": normalized_confirmation_certainty,
            "has_more": source_has_more,
            "total_is_exact": source_total_is_exact,
            "map_counts": [
                {
                    "status": str(row[0] or ""),
                    "is_user_confirmed": bool(row[1]),
                    "count": int(row[2] or 0),
                }
                for row in source_status_rows
            ],
        },
        "release_family": {
            "total": release_total,
            "groups": release_groups,
            "included_statuses": ["accepted", "suggested"],
            "has_more": release_has_more,
            "total_is_exact": release_total_is_exact,
            "map_counts": [
                {
                    "status": str(row[0] or ""),
                    "is_user_confirmed": bool(row[1]),
                    "count": int(row[2] or 0),
                }
                for row in release_status_rows
            ],
        },
        "limit": bounded_limit,
        "offset": bounded_offset,
        "mapping_kind_filter": normalized_mapping_kind,
    }


def search_track_catalog_duplicate_spotify_identities(
    *,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 500))
    bounded_offset = max(0, int(offset))
    base_sql = """
        WITH raw_track_listens AS (
          SELECT
            spotify_track_id,
            count(*) AS listen_count
          FROM raw_play_event
          WHERE spotify_track_id IS NOT NULL
            AND spotify_track_id != ''
          GROUP BY spotify_track_id
        ),
        spotify_track_candidates AS (
          SELECT
            stm.release_track_id,
            st.external_id AS spotify_track_id,
            COALESCE(rtl.listen_count, 0) AS listen_count,
            st.id AS source_track_row_id,
            stm.id AS source_track_map_row_id
          FROM source_track st
          JOIN source_track_map stm
            ON stm.source_track_id = st.id
          LEFT JOIN raw_track_listens rtl
            ON rtl.spotify_track_id = st.external_id
          WHERE st.source_name = 'spotify'
            AND st.external_id IS NOT NULL
            AND st.external_id != ''
            AND stm.status = 'accepted'
        ),
        representative_spotify_track AS (
          SELECT release_track_id, spotify_track_id
          FROM (
            SELECT
              release_track_id,
              spotify_track_id,
              row_number() OVER (
                PARTITION BY release_track_id
                ORDER BY
                  listen_count DESC,
                  spotify_track_id ASC,
                  source_track_map_row_id ASC,
                  source_track_row_id ASC
              ) AS rn
            FROM spotify_track_candidates
          )
          WHERE rn = 1
        ),
        primary_artists AS (
          SELECT
            ordered.release_track_id,
            group_concat(ordered.artist_name, ', ') AS artist_name
          FROM (
            SELECT
              ta.release_track_id,
              a.canonical_name AS artist_name
            FROM track_artist ta
            JOIN artist a
              ON a.id = ta.artist_id
            WHERE ta.role = 'primary'
            ORDER BY ta.release_track_id, COALESCE(ta.billing_index, 999999), ta.id, a.canonical_name
          ) ordered
          GROUP BY ordered.release_track_id
        ),
        primary_albums AS (
          SELECT release_track_id, release_album_name
          FROM (
            SELECT
              at.release_track_id,
              ra.primary_name AS release_album_name,
              row_number() OVER (
                PARTITION BY at.release_track_id
                ORDER BY at.id ASC, ra.primary_name ASC, ra.id ASC
              ) AS rn
            FROM album_track at
            JOIN release_album ra
              ON ra.id = at.release_album_id
          )
          WHERE rn = 1
        ),
        base AS (
          SELECT
            rt.id AS release_track_id,
            rt.primary_name AS release_track_name,
            COALESCE(pa.artist_name, 'Unknown artist') AS artist_name,
            COALESCE(pal.release_album_name, 'Unknown album') AS release_album_name,
            rst.spotify_track_id AS spotify_track_id,
            stc.name AS spotify_track_name,
            stc.duration_ms AS duration_ms,
            stc.album_id AS spotify_album_id,
            stc.last_status AS catalog_status,
            CASE
              WHEN q.id IS NULL THEN 'not_queued'
              ELSE q.status
            END AS queue_status
          FROM release_track rt
          LEFT JOIN primary_artists pa
            ON pa.release_track_id = rt.id
          LEFT JOIN primary_albums pal
            ON pal.release_track_id = rt.id
          LEFT JOIN representative_spotify_track rst
            ON rst.release_track_id = rt.id
          LEFT JOIN spotify_track_catalog stc
            ON stc.spotify_track_id = rst.spotify_track_id
          LEFT JOIN spotify_catalog_backfill_queue q
            ON q.entity_type = 'track'
           AND q.spotify_id = rst.spotify_track_id
          WHERE rst.spotify_track_id IS NOT NULL
            AND rst.spotify_track_id != ''
        ),
        duplicate_groups AS (
          SELECT
            spotify_track_id,
            max(spotify_track_name) AS spotify_track_name,
            max(duration_ms) AS duration_ms,
            count(*) AS duplicate_count
          FROM base
          GROUP BY spotify_track_id
          HAVING count(*) > 1
        )
    """
    total_query = f"{base_sql} SELECT count(*) FROM duplicate_groups"
    items_query = f"""
        {base_sql}
        SELECT
          dg.spotify_track_id,
          dg.spotify_track_name,
          dg.duration_ms,
          dg.duplicate_count,
          b.release_track_id,
          b.release_track_name,
          b.artist_name,
          b.release_album_name,
          b.spotify_album_id,
          b.catalog_status,
          b.queue_status
        FROM duplicate_groups dg
        JOIN base b
          ON b.spotify_track_id = dg.spotify_track_id
        ORDER BY
          dg.duplicate_count DESC,
          dg.spotify_track_id ASC,
          b.release_track_name ASC,
          b.release_track_id ASC
        LIMIT ?
        OFFSET ?
    """
    with sqlite_connection() as connection:
        total = int(connection.execute(total_query).fetchone()[0])
        rows = connection.execute(items_query, (bounded_limit, bounded_offset)).fetchall()

    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        spotify_track_id = str(row[0] or "")
        if not spotify_track_id:
            continue
        if spotify_track_id not in grouped:
            grouped[spotify_track_id] = {
                "spotify_track_id": spotify_track_id,
                "spotify_track_name": row[1],
                "duration_ms": int(row[2]) if row[2] is not None else None,
                "duration_display": _duration_display(row[2]),
                "duplicate_count": int(row[3] or 0),
                "release_tracks": [],
            }
        grouped[spotify_track_id]["release_tracks"].append(
            {
                "release_track_id": int(row[4]),
                "release_track_name": str(row[5] or ""),
                "artist_name": str(row[6] or "Unknown artist"),
                "release_album_name": str(row[7] or "Unknown album"),
                "spotify_album_id": row[8],
                "catalog_status": row[9],
                "queue_status": str(row[10] or "not_queued"),
            }
        )

    items = list(grouped.values())
    return {"ok": True, "items": items, "total": total}


def discover_known_spotify_track_id(*, offset: int = 0) -> str | None:
    ids, _ = _known_track_ids(limit=1, offset=offset)
    return ids[0] if ids else None


def discover_known_spotify_track_ids(*, limit: int = 5, offset: int = 0) -> list[str]:
    ids, _ = _known_track_ids(limit=max(1, min(int(limit), 50)), offset=offset)
    return ids


def list_spotify_catalog_backfill_runs(*, limit: int = 20, offset: int = 0) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 100))
    bounded_offset = max(0, int(offset))

    with sqlite_connection() as connection:
        total = int(connection.execute("SELECT count(*) FROM spotify_catalog_backfill_run").fetchone()[0])
        rows = connection.execute(
            """
            SELECT
              id,
              started_at,
              completed_at,
              market,
              status,
              tracks_seen,
              tracks_fetched,
              tracks_upserted,
              albums_seen,
              albums_fetched,
              album_tracks_upserted,
              skipped,
              errors,
              requests_total,
              requests_success,
              requests_429,
              requests_failed,
              initial_request_delay_seconds,
              final_request_delay_seconds,
              effective_requests_per_minute,
              peak_requests_last_30_seconds,
              max_retry_after_seconds,
              has_more,
              last_error,
              warnings_json,
              run_mode,
              run_reason,
              album_tracklist_policy
            FROM spotify_catalog_backfill_run
            ORDER BY started_at DESC, id DESC
            LIMIT ?
            OFFSET ?
            """,
            (bounded_limit, bounded_offset),
        ).fetchall()

    items = []
    for row in rows:
        warnings = _warnings_from_json_text(row[24])
        items.append(
            {
                "id": int(row[0]),
                "started_at": row[1],
                "completed_at": row[2],
                "market": row[3],
                "status": row[4],
                "tracks_seen": int(row[5] or 0),
                "tracks_fetched": int(row[6] or 0),
                "tracks_upserted": int(row[7] or 0),
                "albums_seen": int(row[8] or 0),
                "albums_fetched": int(row[9] or 0),
                "album_tracks_upserted": int(row[10] or 0),
                "skipped": int(row[11] or 0),
                "errors": int(row[12] or 0),
                "requests_total": int(row[13] or 0),
                "requests_success": int(row[14] or 0),
                "requests_429": int(row[15] or 0),
                "requests_failed": int(row[16] or 0),
                "initial_request_delay_seconds": float(row[17] or 0.0),
                "final_request_delay_seconds": float(row[18] or 0.0),
                "effective_requests_per_minute": float(row[19] or 0.0),
                "peak_requests_last_30_seconds": int(row[20] or 0),
                "max_retry_after_seconds": float(row[21] or 0.0),
                "has_more": bool(row[22]),
                "last_error": row[23],
                "warnings": warnings,
                "warnings_count": len(warnings),
                "run_mode": row[25] or "full_catalog",
                "run_reason": row[26],
                "album_tracklist_policy": row[27] or "all",
            }
        )
    return {"ok": True, "items": items, "total": total}


def get_spotify_catalog_backfill_coverage() -> dict[str, Any]:
    with sqlite_connection() as connection:
        known_release_tracks = int(
            connection.execute(
                """
                SELECT count(DISTINCT stm.release_track_id)
                FROM source_track st
                JOIN source_track_map stm
                  ON stm.source_track_id = st.id
                WHERE st.source_name = 'spotify'
                  AND stm.status = 'accepted'
                """
            ).fetchone()[0]
        )
        track_catalog_rows = int(connection.execute("SELECT count(*) FROM spotify_track_catalog").fetchone()[0])
        track_duration_coverage_count = int(
            connection.execute(
                """
                SELECT count(DISTINCT stm.release_track_id)
                FROM source_track st
                JOIN source_track_map stm
                  ON stm.source_track_id = st.id
                JOIN spotify_track_catalog stc
                  ON stc.spotify_track_id = st.external_id
                WHERE st.source_name = 'spotify'
                  AND stm.status = 'accepted'
                  AND stc.duration_ms IS NOT NULL
                """
            ).fetchone()[0]
        )
        known_release_albums = int(
            connection.execute(
                """
                SELECT count(DISTINCT sam.release_album_id)
                FROM source_album sa
                JOIN source_album_map sam
                  ON sam.source_album_id = sa.id
                WHERE sa.source_name = 'spotify'
                  AND sam.status = 'accepted'
                """
            ).fetchone()[0]
        )
        album_catalog_rows = int(connection.execute("SELECT count(*) FROM spotify_album_catalog").fetchone()[0])
        album_track_rows = int(connection.execute("SELECT count(*) FROM spotify_album_track").fetchone()[0])
        missing_source_track_metadata = int(
            connection.execute(
                """
                WITH source_tracks AS (
                  SELECT DISTINCT st.external_id AS spotify_track_id
                  FROM source_track st
                  JOIN source_track_map stm
                    ON stm.source_track_id = st.id
                  WHERE st.source_name = 'spotify'
                    AND st.external_id IS NOT NULL
                    AND st.external_id != ''
                    AND stm.status = 'accepted'
                )
                SELECT count(*)
                FROM source_tracks st
                LEFT JOIN spotify_track_catalog stc
                  ON stc.spotify_track_id = st.spotify_track_id
                WHERE stc.spotify_track_id IS NULL
                   OR lower(COALESCE(stc.last_status, '')) = 'error'
                """
            ).fetchone()[0]
        )
        missing_track_duration_ms = int(
            connection.execute(
                """
                WITH source_tracks AS (
                  SELECT DISTINCT st.external_id AS spotify_track_id
                  FROM source_track st
                  JOIN source_track_map stm
                    ON stm.source_track_id = st.id
                  WHERE st.source_name = 'spotify'
                    AND st.external_id IS NOT NULL
                    AND st.external_id != ''
                    AND stm.status = 'accepted'
                )
                SELECT count(*)
                FROM source_tracks st
                LEFT JOIN spotify_track_catalog stc
                  ON stc.spotify_track_id = st.spotify_track_id
                WHERE stc.duration_ms IS NULL
                   OR lower(COALESCE(stc.last_status, '')) = 'error'
                """
            ).fetchone()[0]
        )
        missing_track_isrc = int(
            connection.execute(
                """
                WITH source_tracks AS (
                  SELECT DISTINCT st.external_id AS spotify_track_id
                  FROM source_track st
                  JOIN source_track_map stm
                    ON stm.source_track_id = st.id
                  WHERE st.source_name = 'spotify'
                    AND st.external_id IS NOT NULL
                    AND st.external_id != ''
                    AND stm.status = 'accepted'
                )
                SELECT count(*)
                FROM source_tracks st
                LEFT JOIN spotify_track_catalog stc
                  ON stc.spotify_track_id = st.spotify_track_id
                WHERE json_extract(COALESCE(stc.raw_json, '{}'), '$.external_ids.isrc') IS NULL
                   OR json_extract(COALESCE(stc.raw_json, '{}'), '$.external_ids.isrc') = ''
                   OR lower(COALESCE(stc.last_status, '')) = 'error'
                """
            ).fetchone()[0]
        )
        track_metadata_priority = get_spotify_track_metadata_priority_debug(sample_limit=5)
        missing_source_album_metadata = int(
            connection.execute(
                """
                WITH source_albums AS (
                  SELECT DISTINCT sa.external_id AS spotify_album_id
                  FROM source_album sa
                  JOIN source_album_map sam
                    ON sam.source_album_id = sa.id
                  WHERE sa.source_name = 'spotify'
                    AND sa.external_id IS NOT NULL
                    AND sa.external_id != ''
                    AND sam.status = 'accepted'
                  UNION
                  SELECT DISTINCT spotify_album_id
                  FROM raw_play_event
                  WHERE spotify_album_id IS NOT NULL
                    AND spotify_album_id != ''
                )
                SELECT count(*)
                FROM source_albums sa
                LEFT JOIN spotify_album_catalog sac
                  ON sac.spotify_album_id = sa.spotify_album_id
                WHERE sac.spotify_album_id IS NULL
                   OR lower(COALESCE(sac.last_status, '')) = 'error'
                """
            ).fetchone()[0]
        )
        missing_album_release_date = int(
            connection.execute(
                """
                WITH source_albums AS (
                  SELECT DISTINCT sa.external_id AS spotify_album_id
                  FROM source_album sa
                  JOIN source_album_map sam
                    ON sam.source_album_id = sa.id
                  WHERE sa.source_name = 'spotify'
                    AND sa.external_id IS NOT NULL
                    AND sa.external_id != ''
                    AND sam.status = 'accepted'
                  UNION
                  SELECT DISTINCT spotify_album_id
                  FROM raw_play_event
                  WHERE spotify_album_id IS NOT NULL
                    AND spotify_album_id != ''
                )
                SELECT count(*)
                FROM source_albums sa
                LEFT JOIN spotify_album_catalog sac
                  ON sac.spotify_album_id = sa.spotify_album_id
                WHERE sac.release_date IS NULL
                   OR sac.release_date = ''
                   OR lower(COALESCE(sac.last_status, '')) = 'error'
                """
            ).fetchone()[0]
        )
        missing_album_external_ids = int(
            connection.execute(
                """
                WITH source_albums AS (
                  SELECT DISTINCT sa.external_id AS spotify_album_id
                  FROM source_album sa
                  JOIN source_album_map sam
                    ON sam.source_album_id = sa.id
                  WHERE sa.source_name = 'spotify'
                    AND sa.external_id IS NOT NULL
                    AND sa.external_id != ''
                    AND sam.status = 'accepted'
                  UNION
                  SELECT DISTINCT spotify_album_id
                  FROM raw_play_event
                  WHERE spotify_album_id IS NOT NULL
                    AND spotify_album_id != ''
                )
                SELECT count(*)
                FROM source_albums sa
                LEFT JOIN spotify_album_catalog sac
                  ON sac.spotify_album_id = sa.spotify_album_id
                WHERE (
                    json_extract(COALESCE(sac.raw_json, '{}'), '$.external_ids.upc') IS NULL
                    AND json_extract(COALESCE(sac.raw_json, '{}'), '$.external_ids.ean') IS NULL
                  )
                   OR lower(COALESCE(sac.last_status, '')) = 'error'
                """
            ).fetchone()[0]
        )
        missing_album_tracklists = int(
            connection.execute(
                """
                SELECT count(*)
                FROM spotify_album_catalog sac
                LEFT JOIN (
                  SELECT spotify_album_id, count(*) AS track_rows
                  FROM spotify_album_track
                  GROUP BY spotify_album_id
                ) sat
                  ON sat.spotify_album_id = sac.spotify_album_id
                WHERE sac.total_tracks IS NOT NULL
                  AND COALESCE(sat.track_rows, 0) < sac.total_tracks
                  AND lower(COALESCE(sac.last_status, '')) != 'error'
                """
            ).fetchone()[0]
        )
        relevant_album_tracklist_backlog = int(
            connection.execute(
                """
                WITH album_stats AS (
                  SELECT
                    sac.spotify_album_id,
                    sac.total_tracks,
                    COALESCE(count(DISTINCT sat.spotify_track_id), 0) AS track_rows,
                    count(DISTINCT rpe.id) AS raw_play_events
                  FROM spotify_album_catalog sac
                  LEFT JOIN spotify_album_track sat
                    ON sat.spotify_album_id = sac.spotify_album_id
                  LEFT JOIN raw_play_event rpe
                    ON rpe.spotify_album_id = sac.spotify_album_id
                  WHERE sac.total_tracks IS NOT NULL
                    AND lower(COALESCE(sac.last_status, '')) != 'error'
                  GROUP BY sac.spotify_album_id, sac.total_tracks
                )
                SELECT count(*)
                FROM album_stats
                WHERE track_rows < total_tracks
                  AND raw_play_events >= 3
                """
            ).fetchone()[0]
        )
        unlistened_tracklist_rows = int(
            connection.execute(
                """
                SELECT count(*)
                FROM spotify_album_track sat
                LEFT JOIN raw_play_event rpe
                  ON rpe.spotify_track_id = sat.spotify_track_id
                WHERE rpe.id IS NULL
                """
            ).fetchone()[0]
        )
        latest_run_row = connection.execute(
            """
            SELECT
              id,
              started_at,
              completed_at,
              market,
              status,
              tracks_seen,
              tracks_fetched,
              tracks_upserted,
              albums_seen,
              albums_fetched,
              album_tracks_upserted,
              skipped,
              errors,
              requests_total,
              requests_success,
              requests_429,
              requests_failed,
              final_request_delay_seconds,
              has_more,
              last_error,
              warnings_json,
              run_mode,
              run_reason,
              album_tracklist_policy
            FROM spotify_catalog_backfill_run
            ORDER BY started_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        recent_errors_count = int(
            connection.execute(
                """
                WITH recent_runs AS (
                  SELECT status, errors
                  FROM spotify_catalog_backfill_run
                  ORDER BY started_at DESC, id DESC
                  LIMIT 20
                )
                SELECT count(*)
                FROM recent_runs
                WHERE COALESCE(errors, 0) > 0 OR status != 'ok'
                """
            ).fetchone()[0]
        )

    track_duration_coverage_percent = 0.0
    if known_release_tracks > 0:
        track_duration_coverage_percent = round((track_duration_coverage_count * 100.0) / known_release_tracks, 2)
    latest_run = None
    if latest_run_row is not None:
        latest_warnings = _warnings_from_json_text(latest_run_row[20])
        latest_run = {
            "id": int(latest_run_row[0]),
            "started_at": latest_run_row[1],
            "completed_at": latest_run_row[2],
            "market": latest_run_row[3],
            "status": latest_run_row[4],
            "tracks_seen": int(latest_run_row[5] or 0),
            "tracks_fetched": int(latest_run_row[6] or 0),
            "tracks_upserted": int(latest_run_row[7] or 0),
            "albums_seen": int(latest_run_row[8] or 0),
            "albums_fetched": int(latest_run_row[9] or 0),
            "album_tracks_upserted": int(latest_run_row[10] or 0),
            "skipped": int(latest_run_row[11] or 0),
            "errors": int(latest_run_row[12] or 0),
            "requests_total": int(latest_run_row[13] or 0),
            "requests_success": int(latest_run_row[14] or 0),
            "requests_429": int(latest_run_row[15] or 0),
            "requests_failed": int(latest_run_row[16] or 0),
            "final_request_delay_seconds": float(latest_run_row[17] or 0.0),
            "has_more": bool(latest_run_row[18]),
            "last_error": latest_run_row[19],
            "warnings": latest_warnings,
            "warnings_count": len(latest_warnings),
            "run_mode": latest_run_row[21] or "full_catalog",
            "run_reason": latest_run_row[22],
            "album_tracklist_policy": latest_run_row[23] or "all",
        }

    return {
        "ok": True,
        "known_release_tracks": known_release_tracks,
        "track_catalog_rows": track_catalog_rows,
        "track_duration_coverage_count": track_duration_coverage_count,
        "track_duration_coverage_percent": track_duration_coverage_percent,
        "known_release_albums": known_release_albums,
        "album_catalog_rows": album_catalog_rows,
        "album_track_rows": album_track_rows,
        "latest_run": latest_run,
        "recent_errors_count": recent_errors_count,
        "identity_critical": {
            "missing_source_track_metadata": missing_source_track_metadata,
            "missing_priority_track_metadata": track_metadata_priority["counts"]["missing_priority_track_metadata"],
            "missing_identity_ambiguous_track_metadata": track_metadata_priority["counts"][
                "missing_identity_ambiguous_track_metadata"
            ],
            "missing_top_track_metadata": track_metadata_priority["counts"]["missing_top_track_metadata"],
            "missing_source_album_metadata": missing_source_album_metadata,
            "missing_track_isrc": missing_track_isrc,
            "missing_track_duration_ms": missing_track_duration_ms,
            "missing_album_release_date": missing_album_release_date,
            "missing_album_external_ids": missing_album_external_ids,
        },
        "catalog_expansion": {
            "missing_deferred_track_metadata": track_metadata_priority["counts"]["missing_deferred_track_metadata"],
            "missing_album_tracklists": missing_album_tracklists,
            "relevant_album_tracklist_backlog": relevant_album_tracklist_backlog,
            "unlistened_tracklist_rows": unlistened_tracklist_rows,
        },
        "track_metadata_priority": track_metadata_priority,
    }


def _run_insert(*, market: str, delay: float, run_mode: str, run_reason: str | None, album_tracklist_policy: str) -> int:
    with sqlite_connection(write=True) as connection:
        cursor = connection.execute(
            """
            INSERT INTO spotify_catalog_backfill_run (
              started_at,
              market,
              status,
              tracks_seen,
              tracks_fetched,
              tracks_upserted,
              albums_seen,
              albums_fetched,
              album_tracks_upserted,
              skipped,
              errors,
              requests_total,
              requests_success,
              requests_429,
              requests_failed,
              initial_request_delay_seconds,
              final_request_delay_seconds,
              effective_requests_per_minute,
              peak_requests_last_30_seconds,
              max_retry_after_seconds,
              has_more,
              last_error,
              warnings_json,
              run_mode,
              run_reason,
              album_tracklist_policy
            ) VALUES (?, ?, 'running', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, ?, 0, 0, 0, 0, NULL, NULL, ?, ?, ?)
            """,
            (_utc_now(), market, delay, delay, run_mode, run_reason, album_tracklist_policy),
        )
    return int(cursor.lastrowid)


def _run_finish(*, run_id: int, payload: dict[str, Any], status_text: str, last_error: str | None) -> None:
    elapsed_seconds = max(0.001, float(payload.get("_elapsed_seconds", 0.0)))
    requests_success = int(payload.get("requests_success", 0))
    peak_requests_last_30_seconds = int(payload.get("_peak_requests_last_30_seconds", 0))
    effective_requests_per_minute = round((requests_success * 60.0) / elapsed_seconds, 3)

    warnings = [str(item) for item in (payload.get("warnings") or []) if str(item).strip()]

    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            UPDATE spotify_catalog_backfill_run
            SET
              completed_at = ?,
              status = ?,
              tracks_seen = ?,
              tracks_fetched = ?,
              tracks_upserted = ?,
              albums_seen = ?,
              albums_fetched = ?,
              album_tracks_upserted = ?,
              skipped = ?,
              errors = ?,
              requests_total = ?,
              requests_success = ?,
              requests_429 = ?,
              requests_failed = ?,
              final_request_delay_seconds = ?,
              effective_requests_per_minute = ?,
              peak_requests_last_30_seconds = ?,
              max_retry_after_seconds = ?,
              has_more = ?,
              last_error = ?,
              warnings_json = ?
            WHERE id = ?
            """,
            (
                _utc_now(),
                status_text,
                int(payload.get("tracks_seen", 0)),
                int(payload.get("tracks_fetched", 0)),
                int(payload.get("tracks_upserted", 0)),
                int(payload.get("albums_seen", 0)),
                int(payload.get("albums_fetched", 0)),
                int(payload.get("album_tracks_upserted", 0)),
                int(payload.get("skipped", 0)),
                int(payload.get("errors", 0)),
                int(payload.get("requests_total", 0)),
                int(payload.get("requests_success", 0)),
                int(payload.get("requests_429", 0)),
                int(payload.get("requests_failed", 0)),
                float(payload.get("_request_delay_seconds", DEFAULT_REQUEST_DELAY_SECONDS)),
                effective_requests_per_minute,
                peak_requests_last_30_seconds,
                float(payload.get("max_retry_after_seconds", 0.0)),
                1 if bool(payload.get("has_more", False)) else 0,
                last_error,
                _json_dump(warnings),
                run_id,
            ),
        )


def _upsert_simplified_album_catalog(
    *,
    connection: sqlite3.Connection,
    album: dict[str, Any],
    market: str,
    fetched_at: str,
    last_status: str,
    last_error: str | None,
) -> None:
    album_id = str(album.get("id") or "").strip()
    if not album_id:
        return
    album_artists = album.get("artists") if isinstance(album.get("artists"), list) else []
    album_images = album.get("images") if isinstance(album.get("images"), list) else []
    connection.execute(
        """
        INSERT INTO spotify_album_catalog (
          spotify_album_id, name, album_type, release_date, release_date_precision, total_tracks,
          artists_json, images_json, raw_json, market, fetched_at, last_status, last_error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(spotify_album_id) DO UPDATE SET
          name = CASE
            WHEN excluded.name IS NOT NULL AND trim(excluded.name) != ''
             AND (spotify_album_catalog.name IS NULL OR trim(COALESCE(spotify_album_catalog.name, '')) = '')
            THEN excluded.name ELSE spotify_album_catalog.name END,
          album_type = CASE
            WHEN excluded.album_type IS NOT NULL AND trim(excluded.album_type) != ''
             AND (spotify_album_catalog.album_type IS NULL OR trim(COALESCE(spotify_album_catalog.album_type, '')) = '')
            THEN excluded.album_type ELSE spotify_album_catalog.album_type END,
          release_date = CASE
            WHEN excluded.release_date IS NOT NULL AND trim(excluded.release_date) != ''
             AND (spotify_album_catalog.release_date IS NULL OR trim(COALESCE(spotify_album_catalog.release_date, '')) = '')
            THEN excluded.release_date ELSE spotify_album_catalog.release_date END,
          release_date_precision = CASE
            WHEN excluded.release_date_precision IS NOT NULL AND trim(excluded.release_date_precision) != ''
             AND (
               spotify_album_catalog.release_date_precision IS NULL
               OR trim(COALESCE(spotify_album_catalog.release_date_precision, '')) = ''
             )
            THEN excluded.release_date_precision ELSE spotify_album_catalog.release_date_precision END,
          total_tracks = COALESCE(spotify_album_catalog.total_tracks, excluded.total_tracks),
          artists_json = CASE
            WHEN excluded.artists_json IS NOT NULL AND trim(excluded.artists_json) NOT IN ('', '[]')
             AND (
               spotify_album_catalog.artists_json IS NULL
               OR trim(COALESCE(spotify_album_catalog.artists_json, '')) IN ('', '[]')
             )
            THEN excluded.artists_json ELSE spotify_album_catalog.artists_json END,
          images_json = CASE
            WHEN excluded.images_json IS NOT NULL AND trim(excluded.images_json) NOT IN ('', '[]')
             AND (
               spotify_album_catalog.images_json IS NULL
               OR trim(COALESCE(spotify_album_catalog.images_json, '')) IN ('', '[]')
             )
            THEN excluded.images_json ELSE spotify_album_catalog.images_json END,
          raw_json = CASE
            WHEN spotify_album_catalog.raw_json IS NULL
              OR trim(COALESCE(spotify_album_catalog.raw_json, '')) IN ('', '{}')
              OR lower(COALESCE(spotify_album_catalog.last_status, '')) = 'error'
            THEN excluded.raw_json ELSE spotify_album_catalog.raw_json END,
          market = COALESCE(spotify_album_catalog.market, excluded.market),
          fetched_at = CASE
            WHEN spotify_album_catalog.fetched_at IS NULL
              OR lower(COALESCE(spotify_album_catalog.last_status, '')) = 'error'
            THEN excluded.fetched_at ELSE spotify_album_catalog.fetched_at END,
          last_status = CASE
            WHEN spotify_album_catalog.last_status IS NULL
              OR lower(COALESCE(spotify_album_catalog.last_status, '')) = 'error'
            THEN excluded.last_status ELSE spotify_album_catalog.last_status END,
          last_error = CASE
            WHEN lower(COALESCE(spotify_album_catalog.last_status, '')) = 'error'
            THEN excluded.last_error ELSE spotify_album_catalog.last_error END
        """,
        (
            album_id,
            str(album.get("name") or "") or None,
            str(album.get("album_type") or "") or None,
            str(album.get("release_date") or "") or None,
            str(album.get("release_date_precision") or "") or None,
            int(album["total_tracks"]) if isinstance(album.get("total_tracks"), int) else None,
            _json_dump(album_artists),
            _json_dump(album_images),
            _json_dump(album),
            market,
            fetched_at,
            last_status,
            last_error,
        ),
    )


def _upsert_track_catalog(*, track: dict[str, Any], market: str, fetched_at: str, last_status: str, last_error: str | None) -> None:
    artists = track.get("artists") if isinstance(track.get("artists"), list) else []
    album = track.get("album") if isinstance(track.get("album"), dict) else {}
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            INSERT INTO spotify_track_catalog (
              spotify_track_id, name, duration_ms, explicit, disc_number, track_number,
              album_id, artists_json, raw_json, market, fetched_at, last_status, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(spotify_track_id) DO UPDATE SET
              name = excluded.name,
              duration_ms = excluded.duration_ms,
              explicit = excluded.explicit,
              disc_number = excluded.disc_number,
              track_number = excluded.track_number,
              album_id = excluded.album_id,
              artists_json = excluded.artists_json,
              raw_json = excluded.raw_json,
              market = excluded.market,
              fetched_at = excluded.fetched_at,
              last_status = excluded.last_status,
              last_error = excluded.last_error
            """,
            (
                str(track.get("id") or ""),
                str(track.get("name") or "") or None,
                int(track["duration_ms"]) if isinstance(track.get("duration_ms"), int) else None,
                _to_int_bool(track.get("explicit")),
                int(track["disc_number"]) if isinstance(track.get("disc_number"), int) else None,
                int(track["track_number"]) if isinstance(track.get("track_number"), int) else None,
                str(album.get("id") or "") or None,
                _json_dump(artists),
                _json_dump(track),
                market,
                fetched_at,
                last_status,
                last_error,
            ),
        )
        _upsert_simplified_album_catalog(
            connection=connection,
            album=album,
            market=market,
            fetched_at=fetched_at,
            last_status=last_status,
            last_error=last_error,
        )


def _upsert_track_catalog_error(*, spotify_track_id: str, market: str, fetched_at: str, last_error: str) -> None:
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            INSERT INTO spotify_track_catalog (
              spotify_track_id, market, fetched_at, last_status, last_error
            ) VALUES (?, ?, ?, 'error', ?)
            ON CONFLICT(spotify_track_id) DO UPDATE SET
              market = excluded.market,
              fetched_at = excluded.fetched_at,
              last_status = excluded.last_status,
              last_error = excluded.last_error
            """,
            (spotify_track_id, market, fetched_at, last_error),
        )


def _upsert_album_catalog(*, album: dict[str, Any], market: str, fetched_at: str, last_status: str, last_error: str | None) -> None:
    artists = album.get("artists") if isinstance(album.get("artists"), list) else []
    images = album.get("images") if isinstance(album.get("images"), list) else []
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            INSERT INTO spotify_album_catalog (
              spotify_album_id, name, album_type, release_date, release_date_precision, total_tracks,
              artists_json, images_json, raw_json, market, fetched_at, last_status, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(spotify_album_id) DO UPDATE SET
              name = excluded.name,
              album_type = excluded.album_type,
              release_date = excluded.release_date,
              release_date_precision = excluded.release_date_precision,
              total_tracks = excluded.total_tracks,
              artists_json = excluded.artists_json,
              images_json = excluded.images_json,
              raw_json = excluded.raw_json,
              market = excluded.market,
              fetched_at = excluded.fetched_at,
              last_status = excluded.last_status,
              last_error = excluded.last_error
            """,
            (
                str(album.get("id") or ""),
                str(album.get("name") or "") or None,
                str(album.get("album_type") or "") or None,
                str(album.get("release_date") or "") or None,
                str(album.get("release_date_precision") or "") or None,
                int(album["total_tracks"]) if isinstance(album.get("total_tracks"), int) else None,
                _json_dump(artists),
                _json_dump(images),
                _json_dump(album),
                market,
                fetched_at,
                last_status,
                last_error,
            ),
        )


def _upsert_album_catalog_error(*, spotify_album_id: str, market: str, fetched_at: str, last_error: str) -> None:
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            INSERT INTO spotify_album_catalog (
              spotify_album_id, market, fetched_at, last_status, last_error
            ) VALUES (?, ?, ?, 'error', ?)
            ON CONFLICT(spotify_album_id) DO UPDATE SET
              market = excluded.market,
              fetched_at = excluded.fetched_at,
              last_status = excluded.last_status,
              last_error = excluded.last_error
            """,
            (spotify_album_id, market, fetched_at, last_error),
        )


def _source_track_id_for_spotify_track(*, spotify_track_id: str) -> int | None:
    with sqlite_connection() as connection:
        row = connection.execute(
            """
            SELECT st.id
            FROM source_track st
            JOIN source_track_map stm
              ON stm.source_track_id = st.id
            WHERE st.source_name = 'spotify'
              AND st.external_id = ?
              AND stm.status = 'accepted'
            ORDER BY st.id ASC
            LIMIT 1
            """,
            (spotify_track_id,),
        ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def run_spotify_track_metadata_canary(
    *,
    access_token: str,
    market: str = "US",
    fetcher: (
        Callable[[str, dict[str, Any], str], tuple[int, dict[str, str], dict[str, Any], str | None]] | None
    ) = None,
) -> dict[str, Any]:
    track_ids, _ = _known_track_ids_missing_metadata_for_scope(
        limit=1,
        offset=0,
        priority_scope=DEFAULT_TRACK_METADATA_PRIORITY_SCOPE,
    )
    spotify_track_id = track_ids[0] if track_ids else None
    if not spotify_track_id:
        return {"status": "skipped_no_candidate", "requests_total": 0, "requests_429": 0}

    request_fetcher = fetcher or _default_fetcher
    status_code, headers, payload, raw_text = request_fetcher(
        f"https://api.spotify.com/v1/tracks/{spotify_track_id}",
        {"market": market},
        access_token,
    )
    source_track_id = _source_track_id_for_spotify_track(spotify_track_id=spotify_track_id)
    retry_after_raw = headers.get("Retry-After")
    retry_after_seconds = 0.0
    if retry_after_raw:
        try:
            retry_after_seconds = max(0.0, float(retry_after_raw))
        except ValueError:
            retry_after_seconds = 0.0
    base = {
        "source_track_id": source_track_id,
        "spotify_track_id": spotify_track_id,
        "status_code": int(status_code),
        "retry_after_seconds": retry_after_seconds,
        "requests_total": 1,
        "requests_429": 1 if int(status_code) == 429 else 0,
        "max_retry_after_seconds": retry_after_seconds,
    }
    if int(status_code) == 429:
        return {
            **base,
            "status": "rate_limited",
            "stop_reason": "post_cooldown_canary_429",
            "last_error": "Post-cooldown canary hit Spotify 429.",
        }
    if int(status_code) >= 400:
        body = _compact_error_body(payload, raw_text)
        detail = f"Post-cooldown canary failed with status {status_code}"
        if body:
            detail = f"{detail}: {body}"
        return {**base, "status": "failed_non_429", "last_error": detail}

    _upsert_track_catalog(track=payload, market=market, fetched_at=_utc_now(), last_status="ok", last_error=None)
    return {**base, "status": "success", "requests_success": 1}


def _upsert_album_track(*, album_id: str, track: dict[str, Any], market: str, fetched_at: str, last_status: str, last_error: str | None) -> None:
    artists = track.get("artists") if isinstance(track.get("artists"), list) else []
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            INSERT INTO spotify_album_track (
              spotify_album_id, spotify_track_id, disc_number, track_number, name, duration_ms,
              artists_json, raw_json, market, fetched_at, last_status, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(spotify_album_id, spotify_track_id) DO UPDATE SET
              disc_number = excluded.disc_number,
              track_number = excluded.track_number,
              name = excluded.name,
              duration_ms = excluded.duration_ms,
              artists_json = excluded.artists_json,
              raw_json = excluded.raw_json,
              market = excluded.market,
              fetched_at = excluded.fetched_at,
              last_status = excluded.last_status,
              last_error = excluded.last_error
            """,
            (
                album_id,
                str(track.get("id") or ""),
                int(track["disc_number"]) if isinstance(track.get("disc_number"), int) else None,
                int(track["track_number"]) if isinstance(track.get("track_number"), int) else None,
                str(track.get("name") or "") or None,
                int(track["duration_ms"]) if isinstance(track.get("duration_ms"), int) else None,
                _json_dump(artists),
                _json_dump(track),
                market,
                fetched_at,
                last_status,
                last_error,
            ),
        )


def _compact_error_body(payload: dict[str, Any], raw_text: str | None) -> str:
    if isinstance(payload, dict) and payload:
        error_payload = payload.get("error")
        if isinstance(error_payload, dict) and error_payload:
            return _json_dump(error_payload)
        return _json_dump(payload)
    if raw_text:
        text = str(raw_text).strip()
        if len(text) > 400:
            return text[:400] + "...(truncated)"
        return text
    return ""


def _request_json(
    *,
    access_token: str,
    url: str,
    params: dict[str, Any],
    endpoint_category: str,
    telemetry: dict[str, Any],
    max_429: int,
    sleeper: Callable[[float], None],
    fetcher: Callable[[str, dict[str, Any], str], tuple[int, dict[str, str], dict[str, Any], str | None]],
) -> dict[str, Any]:
    for attempt in range(4):
        if telemetry["requests_total"] > 0:
            sleeper(float(telemetry["_request_delay_seconds"]))
        telemetry["requests_total"] += 1
        now_ts = time.monotonic()
        telemetry["_request_timestamps"].append(now_ts)
        recent_count = len([ts for ts in telemetry["_request_timestamps"] if (now_ts - ts) <= 30.0])
        telemetry["_peak_requests_last_30_seconds"] = max(telemetry["_peak_requests_last_30_seconds"], recent_count)

        status_code, headers, payload, raw_text = fetcher(url, params, access_token)
        if status_code == 429:
            telemetry["requests_429"] += 1
            retry_after_raw = headers.get("Retry-After")
            retry_after_seconds: float | None = None
            if retry_after_raw:
                try:
                    retry_after_seconds = max(0.0, float(retry_after_raw))
                except ValueError:
                    retry_after_seconds = None
            if retry_after_seconds is not None:
                telemetry["last_retry_after_seconds"] = retry_after_seconds
                telemetry["max_retry_after_seconds"] = max(float(telemetry["max_retry_after_seconds"]), retry_after_seconds)
                cooldown_seconds = retry_after_seconds + 0.25
            else:
                current_delay_seconds = float(telemetry["_request_delay_seconds"])
                cooldown_seconds = max(current_delay_seconds * 2.0, 5.0)
                warning_text = "429 without valid Retry-After; used fallback cooldown"
                if warning_text not in telemetry["warnings"]:
                    telemetry["warnings"].append(warning_text)
            telemetry["_request_delay_seconds"] = min(float(telemetry["_request_delay_seconds"]) * 1.75, MAX_REQUEST_DELAY_SECONDS)
            sleeper(cooldown_seconds)
            if int(telemetry.get("requests_429", 0)) >= int(max_429):
                if FIRST_429_STOP_WARNING not in telemetry["warnings"]:
                    telemetry["warnings"].append(FIRST_429_STOP_WARNING)
                raise _PartialStop("rate_limited")
            if attempt < 3:
                continue
            telemetry["requests_failed"] += 1
            raise RuntimeError(f"{endpoint_category}: Spotify rate limit persisted after retries.")
        if status_code >= 400:
            telemetry["requests_failed"] += 1
            body = _compact_error_body(payload, raw_text)
            detail = f"{endpoint_category}: Spotify request failed with status {status_code}"
            if body:
                detail = f"{detail}: {body}"
            raise RuntimeError(detail)

        telemetry["requests_success"] += 1
        if telemetry["requests_success"] % 25 == 0:
            telemetry["_request_delay_seconds"] = max(float(telemetry["_request_delay_seconds"]) * 0.90, MIN_REQUEST_DELAY_SECONDS)
        return payload

    telemetry["requests_failed"] += 1
    raise RuntimeError("Spotify request failed after retries.")


def _default_fetcher(
    url: str, params: dict[str, Any], access_token: str
) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, headers={"Authorization": f"Bearer {access_token}"}, params=params)
    raw_text = response.text
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    return int(response.status_code), dict(response.headers), payload if isinstance(payload, dict) else {}, raw_text


def _check_stop_reason(
    *,
    telemetry: dict[str, Any],
    started_monotonic: float,
    max_runtime_seconds: float,
    max_requests: int,
    max_errors: int,
    max_429: int,
) -> str | None:
    elapsed_seconds = max(0.0, time.monotonic() - started_monotonic)
    if elapsed_seconds >= float(max_runtime_seconds):
        return "max_runtime_seconds"
    if int(telemetry.get("requests_total", 0)) >= int(max_requests):
        return "max_requests"
    if int(telemetry.get("errors", 0)) >= int(max_errors):
        return "max_errors"
    if int(telemetry.get("requests_429", 0)) >= int(max_429):
        return "rate_limited"
    return None


def run_spotify_catalog_backfill(
    *,
    access_token: str,
    run_mode: str = "full_catalog",
    reason: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    market: str = "US",
    include_albums: bool = True,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
    max_runtime_seconds: float = DEFAULT_MAX_RUNTIME_SECONDS,
    max_requests: int = DEFAULT_MAX_REQUESTS,
    max_errors: int = DEFAULT_MAX_ERRORS,
    max_album_tracks_pages_per_album: int = DEFAULT_MAX_ALBUM_TRACKS_PAGES_PER_ALBUM,
    max_429: int = DEFAULT_MAX_429,
    force_refresh: bool = False,
    album_tracklist_policy: str = "all",
    target: str | None = None,
    priority_scope: str | None = None,
    sleeper: Callable[[float], None] | None = None,
    fetcher: Callable[[str, dict[str, Any], str], tuple[int, dict[str, str], dict[str, Any], str | None]] | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), MAX_LIMIT))
    bounded_offset = max(0, int(offset))
    normalized_market = str(market or "US").strip() or "US"
    effective_delay = _normalize_delay_seconds(float(request_delay_seconds))
    bounded_max_runtime_seconds = max(5.0, min(float(max_runtime_seconds), float(MAX_RUNTIME_SECONDS)))
    bounded_max_requests = max(1, min(int(max_requests), 1000))
    bounded_max_errors = max(1, min(int(max_errors), 100))
    bounded_max_album_tracks_pages_per_album = max(1, min(int(max_album_tracks_pages_per_album), 50))
    bounded_max_429 = 1
    normalized_run_mode = str(run_mode or "full_catalog").strip().lower()
    if normalized_run_mode not in CATALOG_BACKFILL_RUN_MODES:
        normalized_run_mode = "full_catalog"
    normalized_reason = str(reason or "").strip().lower() or None
    if normalized_reason is not None and normalized_reason not in CATALOG_BACKFILL_REASONS:
        normalized_reason = None
    normalized_album_tracklist_policy = str(album_tracklist_policy or "all").strip().lower()
    if normalized_album_tracklist_policy not in ALBUM_TRACKLIST_POLICIES:
        normalized_album_tracklist_policy = "all"
    requested_album_tracklist_policy = normalized_album_tracklist_policy
    target_was_provided = target is not None and str(target).strip() != ""
    normalized_target = str(target or "all").strip().lower()
    if normalized_target not in CATALOG_BACKFILL_TARGETS:
        normalized_target = "all"
    normalized_priority_scope = str(priority_scope or "").strip().lower() or None
    if normalized_priority_scope is not None and normalized_priority_scope not in TRACK_METADATA_PRIORITY_SCOPES:
        normalized_priority_scope = None
    if normalized_run_mode == "metadata_only":
        if not target_was_provided:
            include_albums = True
        normalized_album_tracklist_policy = "none"
        normalized_reason = normalized_reason or "identity_metadata"
        if normalized_reason == "identity_metadata" and normalized_priority_scope is None:
            normalized_priority_scope = DEFAULT_TRACK_METADATA_PRIORITY_SCOPE
    elif normalized_run_mode == "tracklists_relevant":
        include_albums = True
        if normalized_album_tracklist_policy in {"all", "none"}:
            normalized_album_tracklist_policy = "relevant_albums"
        normalized_reason = normalized_reason or "tracklist_completion"
    elif normalized_run_mode == "full_catalog":
        normalized_reason = normalized_reason or "full_backfill"
    if target_was_provided:
        if normalized_target == "tracks":
            include_albums = False
            normalized_album_tracklist_policy = "none"
        elif normalized_target == "albums":
            include_albums = True
            normalized_album_tracklist_policy = "none"
        elif normalized_target == "album_tracklists":
            include_albums = True
        else:
            include_albums = True
    run_tracks = normalized_target in {"tracks", "all"}
    run_album_metadata = normalized_target in {"albums", "all"}
    run_album_tracklists = normalized_target in {"album_tracklists", "all"} and include_albums and normalized_album_tracklist_policy != "none"
    sleep_fn = sleeper or time.sleep
    fetch_fn = fetcher or _default_fetcher

    run_id = _run_insert(
        market=normalized_market,
        delay=effective_delay,
        run_mode=normalized_run_mode,
        run_reason=normalized_reason,
        album_tracklist_policy=normalized_album_tracklist_policy,
    )
    started_monotonic = time.monotonic()
    telemetry: dict[str, Any] = {
        "tracks_seen": 0,
        "tracks_fetched": 0,
        "tracks_upserted": 0,
        "albums_seen": 0,
        "albums_fetched": 0,
        "album_tracks_upserted": 0,
        "album_tracklists_capped": 0,
        "album_tracklists_seen": 0,
        "album_tracklists_skipped_by_policy": 0,
        "album_tracklists_fetched": 0,
        "skipped": 0,
        "errors": 0,
        "requests_total": 0,
        "requests_success": 0,
        "requests_429": 0,
        "requests_failed": 0,
        "last_retry_after_seconds": 0.0,
        "max_retry_after_seconds": 0.0,
        "has_more": False,
        "warnings": [],
        "_request_delay_seconds": effective_delay,
        "_request_timestamps": [],
        "_peak_requests_last_30_seconds": 0,
        "_progress_track_candidates": 0,
    }
    initial_integrity = inspect_spotify_nested_metadata_integrity(sample_limit=5)
    final_integrity: dict[str, Any] = initial_integrity

    last_error: str | None = None
    status_text = "ok"
    stop_reason: str | None = None
    partial = False
    track_batch_unavailable = False
    album_batch_unavailable = False
    last_progress_tracks_fetched = 0
    last_progress_monotonic = started_monotonic

    def _result_payload() -> dict[str, Any]:
        return {
            "run_id": run_id,
            "status": status_text,
            "tracks_seen": int(telemetry["tracks_seen"]),
            "tracks_fetched": int(telemetry["tracks_fetched"]),
            "tracks_upserted": int(telemetry["tracks_upserted"]),
            "albums_seen": int(telemetry["albums_seen"]),
            "albums_fetched": int(telemetry["albums_fetched"]),
            "album_tracks_upserted": int(telemetry["album_tracks_upserted"]),
            "album_tracklists_capped": int(telemetry["album_tracklists_capped"]),
            "album_tracklists_seen": int(telemetry["album_tracklists_seen"]),
            "album_tracklists_skipped_by_policy": int(telemetry["album_tracklists_skipped_by_policy"]),
            "album_tracklists_fetched": int(telemetry["album_tracklists_fetched"]),
            "skipped": int(telemetry["skipped"]),
            "errors": int(telemetry["errors"]),
            "requests_total": int(telemetry["requests_total"]),
            "requests_success": int(telemetry["requests_success"]),
            "requests_429": int(telemetry["requests_429"]),
            "requests_failed": int(telemetry["requests_failed"]),
            "initial_request_delay_seconds": effective_delay,
            "final_request_delay_seconds": float(telemetry["_request_delay_seconds"]),
            "effective_requests_per_minute": round(
                (int(telemetry["requests_success"]) * 60.0) / max(0.001, float(telemetry.get("_elapsed_seconds", 0.0))),
                3,
            ),
            "peak_requests_last_30_seconds": int(telemetry["_peak_requests_last_30_seconds"]),
            "last_retry_after_seconds": float(telemetry["last_retry_after_seconds"]),
            "max_retry_after_seconds": float(telemetry["max_retry_after_seconds"]),
            "has_more": bool(telemetry["has_more"]),
            "warnings": list(telemetry["warnings"]),
            "stop_reason": stop_reason,
            "partial": bool(partial),
            "market": normalized_market,
            "run_mode": normalized_run_mode,
            "run_reason": normalized_reason,
            "priority_scope": normalized_priority_scope,
            "target": normalized_target,
            "limit": bounded_limit,
            "offset": bounded_offset,
            "include_albums": bool(include_albums),
            "force_refresh": bool(force_refresh),
            "album_tracklist_policy": normalized_album_tracklist_policy,
            "max_runtime_seconds": bounded_max_runtime_seconds,
            "max_requests": bounded_max_requests,
            "max_errors": bounded_max_errors,
            "max_album_tracks_pages_per_album": bounded_max_album_tracks_pages_per_album,
            "max_429": bounded_max_429,
            "last_error": last_error,
            "local_metadata_integrity": final_integrity,
        }

    def _emit_progress(event: str, extra: dict[str, Any] | None = None) -> None:
        if progress_callback is None:
            return
        elapsed_seconds = max(0.0, time.monotonic() - started_monotonic)
        candidate_count = int(telemetry.get("_progress_track_candidates", 0) or 0)
        tracks_fetched = int(telemetry["tracks_fetched"])
        requests_total = int(telemetry["requests_total"])
        requests_429 = int(telemetry["requests_429"])
        if event == "start":
            message = f"Run {run_id} started: {candidate_count} track candidates, has_more={bool(telemetry['has_more'])}"
        else:
            total_text = str(candidate_count) if candidate_count > 0 else "?"
            message = (
                f"Run {run_id}: {tracks_fetched}/{total_text} tracks fetched, "
                f"{requests_total} requests, {requests_429} rate limits, {round(elapsed_seconds)}s elapsed"
            )
        payload: dict[str, Any] = {
            "event": event,
            "run_id": run_id,
            "status": status_text,
            "stop_reason": stop_reason,
            "tracks_fetched": tracks_fetched,
            "requests_total": requests_total,
            "requests_429": requests_429,
            "elapsed_seconds": round(elapsed_seconds, 3),
            "message": message,
        }
        if extra:
            payload.update(extra)
        try:
            progress_callback(payload)
        except Exception:
            return

    def _emit_progress_if_due() -> None:
        nonlocal last_progress_monotonic, last_progress_tracks_fetched
        if progress_callback is None:
            return
        tracks_fetched = int(telemetry["tracks_fetched"])
        now_monotonic = time.monotonic()
        if tracks_fetched >= last_progress_tracks_fetched + 10 or (now_monotonic - last_progress_monotonic) >= 30.0:
            _emit_progress("progress")
            last_progress_tracks_fetched = tracks_fetched
            last_progress_monotonic = now_monotonic

    if normalized_run_mode == "metadata_only" and normalized_target == "album_tracklists":
        error_text = "metadata_only target=album_tracklists is invalid; album tracklists require an explicit tracklist run mode."
        telemetry["warnings"].append(error_text)
        telemetry["errors"] = 1
        telemetry["_elapsed_seconds"] = max(0.0, time.monotonic() - started_monotonic)
        _run_finish(run_id=run_id, payload=telemetry, status_text="failed", last_error=error_text)
        status_text = "failed"
        last_error = error_text
        return _result_payload()
    if normalized_target == "album_tracklists" and requested_album_tracklist_policy == "none":
        error_text = "target=album_tracklists requires album_tracklist_policy other than none."
        telemetry["warnings"].append(error_text)
        telemetry["errors"] = 1
        telemetry["_elapsed_seconds"] = max(0.0, time.monotonic() - started_monotonic)
        _run_finish(run_id=run_id, payload=telemetry, status_text="failed", last_error=error_text)
        status_text = "failed"
        last_error = error_text
        return _result_payload()

    def _raise_if_should_stop() -> None:
        reason = _check_stop_reason(
            telemetry=telemetry,
            started_monotonic=started_monotonic,
            max_runtime_seconds=bounded_max_runtime_seconds,
            max_requests=bounded_max_requests,
            max_errors=bounded_max_errors,
            max_429=bounded_max_429,
        )
        if reason is not None:
            raise _PartialStop(reason)

    def _warn_once(text: str) -> None:
        if text not in telemetry["warnings"]:
            telemetry["warnings"].append(text)

    def _fetch_single_tracks(track_ids: list[str]) -> None:
        for track_id in track_ids:
            try:
                _raise_if_should_stop()
                single_payload = _request_json(
                    access_token=access_token,
                    url=f"https://api.spotify.com/v1/tracks/{track_id}",
                    params={"market": normalized_market},
                    endpoint_category="tracks_single_fallback",
                    telemetry=telemetry,
                    max_429=bounded_max_429,
                    sleeper=sleep_fn,
                    fetcher=fetch_fn,
                )
                if not isinstance(single_payload, dict) or not single_payload.get("id"):
                    telemetry["skipped"] += 1
                    _upsert_track_catalog_error(
                        spotify_track_id=track_id,
                        market=normalized_market,
                        fetched_at=fetched_at,
                        last_error="tracks_single_fallback: Missing track payload.",
                    )
                    continue
                _upsert_track_catalog(
                    track=single_payload,
                    market=normalized_market,
                    fetched_at=fetched_at,
                    last_status="ok",
                    last_error=None,
                )
                telemetry["tracks_fetched"] += 1
                telemetry["tracks_upserted"] += 1
                _emit_progress_if_due()
                album = single_payload.get("album") if isinstance(single_payload.get("album"), dict) else {}
                if album.get("id"):
                    album_ids.add(str(album["id"]))
            except RuntimeError as single_exc:
                telemetry["errors"] += 1
                _upsert_track_catalog_error(
                    spotify_track_id=track_id,
                    market=normalized_market,
                    fetched_at=fetched_at,
                    last_error=str(single_exc),
                )

    def _fetch_single_albums(album_ids_for_fetch: list[str]) -> list[dict[str, Any]]:
        album_payloads: list[dict[str, Any]] = []
        for album_id in album_ids_for_fetch:
            try:
                try:
                    _raise_if_should_stop()
                except _PartialStop:
                    return album_payloads
                single_album = _request_json(
                    access_token=access_token,
                    url=f"https://api.spotify.com/v1/albums/{album_id}",
                    params={"market": normalized_market},
                    endpoint_category="album_single_fallback",
                    telemetry=telemetry,
                    max_429=bounded_max_429,
                    sleeper=sleep_fn,
                    fetcher=fetch_fn,
                )
                if not isinstance(single_album, dict) or not single_album.get("id"):
                    telemetry["skipped"] += 1
                    _upsert_album_catalog_error(
                        spotify_album_id=album_id,
                        market=normalized_market,
                        fetched_at=fetched_at,
                        last_error="album_single_fallback: Missing album payload.",
                    )
                    continue
                album_payloads.append(single_album)
            except RuntimeError as single_exc:
                telemetry["errors"] += 1
                _upsert_album_catalog_error(
                    spotify_album_id=album_id,
                    market=normalized_market,
                    fetched_at=fetched_at,
                    last_error=str(single_exc),
                )
        return album_payloads

    try:
        queue_items = _pending_queue_items(limit=bounded_limit)
        if normalized_run_mode == "metadata_only":
            queue_items = [
                item
                for item in queue_items
                if str(item.get("reason") or "").strip().lower() in METADATA_ONLY_QUEUE_REASONS
            ]
        queue_slots_used = 0
        queued_track_ids_processed: set[str] = set()
        queued_album_ids_processed: set[str] = set()
        album_ids: set[str] = set()
        album_track_fetch_ids: set[str] = set()
        queued_album_track_fetch_queue_ids: dict[str, list[int]] = {}
        if normalized_run_mode == "metadata_only" and run_tracks:
            seeded_track_ids, has_more = _known_track_ids_missing_metadata_for_scope(
                limit=bounded_limit,
                offset=bounded_offset,
                priority_scope=normalized_priority_scope or "all",
            )
        elif run_tracks:
            seeded_track_ids, has_more = _known_track_ids(limit=bounded_limit, offset=bounded_offset)
        else:
            seeded_track_ids, has_more = [], False
        if normalized_run_mode == "metadata_only" and normalized_target == "albums":
            seeded_source_album_ids, album_has_more = _known_album_ids_missing_metadata(
                limit=bounded_limit,
                offset=bounded_offset,
            )
        elif run_album_metadata:
            seeded_source_album_ids, album_has_more = _known_source_album_ids(limit=bounded_limit, offset=bounded_offset)
        else:
            seeded_source_album_ids, album_has_more = [], False
        deduped_track_ids = list(dict.fromkeys(str(track_id) for track_id in seeded_track_ids if str(track_id).strip()))
        telemetry["tracks_seen"] = len(deduped_track_ids)
        telemetry["has_more"] = has_more or (normalized_run_mode == "metadata_only" and album_has_more)
        telemetry["_progress_track_candidates"] = len(deduped_track_ids)
        _emit_progress(
            "start",
            {
                "worker_config": {
                    "target": normalized_target,
                    "run_mode": normalized_run_mode,
                    "reason": normalized_reason,
                    "priority_scope": normalized_priority_scope,
                    "album_tracklist_policy": normalized_album_tracklist_policy,
                    "include_albums": bool(include_albums),
                    "limit": bounded_limit,
                    "max_requests": bounded_max_requests,
                    "max_runtime_seconds": bounded_max_runtime_seconds,
                    "request_delay_seconds": effective_delay,
                    "market": normalized_market,
                },
                "candidate_counts": {
                    "tracks_seen": int(telemetry["tracks_seen"]),
                    "source_albums_seen": len(seeded_source_album_ids),
                    "queue_items": len(queue_items),
                    "has_more": bool(telemetry["has_more"]),
                },
            },
        )

        fetched_at = _utc_now()

        # Process explicit queue requests before bulk backlog.
        for queue_item in queue_items:
            _raise_if_should_stop()
            if queue_slots_used >= bounded_limit:
                break
            queue_id = int(queue_item["id"])
            entity_type = str(queue_item["entity_type"])
            spotify_id = str(queue_item["spotify_id"])
            if entity_type == "track" and not run_tracks:
                continue
            if entity_type == "album" and not (run_album_metadata or run_album_tracklists):
                continue
            queue_slots_used += 1
            if entity_type == "track":
                queued_track_ids_processed.add(spotify_id)
                telemetry["tracks_seen"] += 1
                is_complete, known_album_id = _track_catalog_completion_info(spotify_track_id=spotify_id)
                if normalized_run_mode == "metadata_only":
                    with sqlite_connection() as connection:
                        is_complete = _is_track_metadata_complete(connection=connection, spotify_track_id=spotify_id)
                if is_complete and not force_refresh:
                    telemetry["skipped"] += 1
                    if known_album_id:
                        album_ids.add(known_album_id)
                    _queue_mark_done(queue_id=queue_id)
                    continue
                try:
                    payload = _request_json(
                        access_token=access_token,
                        url=f"https://api.spotify.com/v1/tracks/{spotify_id}",
                        params={"market": normalized_market},
                        endpoint_category="queue_track",
                        telemetry=telemetry,
                        max_429=bounded_max_429,
                        sleeper=sleep_fn,
                        fetcher=fetch_fn,
                    )
                    if not isinstance(payload, dict) or not payload.get("id"):
                        telemetry["errors"] += 1
                        error_text = "queue_track: Missing track payload."
                        _upsert_track_catalog_error(
                            spotify_track_id=spotify_id,
                            market=normalized_market,
                            fetched_at=fetched_at,
                            last_error=error_text,
                        )
                        _queue_mark_error(queue_id=queue_id, error_message=error_text)
                        continue
                    _upsert_track_catalog(
                        track=payload,
                        market=normalized_market,
                        fetched_at=fetched_at,
                        last_status="ok",
                        last_error=None,
                    )
                    telemetry["tracks_fetched"] += 1
                    telemetry["tracks_upserted"] += 1
                    _emit_progress_if_due()
                    album = payload.get("album") if isinstance(payload.get("album"), dict) else {}
                    if album.get("id"):
                        album_ids.add(str(album["id"]))
                    with sqlite_connection() as connection:
                        if normalized_run_mode == "metadata_only":
                            now_complete = _is_track_metadata_complete(connection=connection, spotify_track_id=spotify_id)
                        else:
                            now_complete = _is_track_catalog_complete(connection=connection, spotify_track_id=spotify_id)
                    if now_complete:
                        _queue_mark_done(queue_id=queue_id)
                except RuntimeError as exc:
                    telemetry["errors"] += 1
                    _upsert_track_catalog_error(
                        spotify_track_id=spotify_id,
                        market=normalized_market,
                        fetched_at=fetched_at,
                        last_error=str(exc),
                    )
                    _queue_mark_error(queue_id=queue_id, error_message=str(exc))
                continue

            if entity_type == "album":
                queued_album_ids_processed.add(spotify_id)
                telemetry["albums_seen"] += 1
                if run_album_tracklists and not run_album_metadata:
                    with sqlite_connection() as connection:
                        metadata_complete = _is_album_metadata_complete(connection=connection, spotify_album_id=spotify_id)
                        needs_track_fetch = (
                            _album_tracklist_needs_fetch(connection=connection, album_id=spotify_id)
                            if metadata_complete or force_refresh
                            else False
                        )
                        if needs_track_fetch:
                            album_track_fetch_ids.add(spotify_id)
                            queued_album_track_fetch_queue_ids.setdefault(spotify_id, []).append(queue_id)
                        else:
                            telemetry["skipped"] += 1
                            _queue_mark_done(queue_id=queue_id)
                    continue
                if normalized_run_mode == "metadata_only":
                    with sqlite_connection() as connection:
                        metadata_complete = _is_album_metadata_complete(connection=connection, spotify_album_id=spotify_id)
                    if metadata_complete and not force_refresh:
                        telemetry["skipped"] += 1
                        _queue_mark_done(queue_id=queue_id)
                        continue
                if _album_catalog_is_complete(spotify_album_id=spotify_id) and not force_refresh:
                    album_ids.add(spotify_id)
                    if include_albums:
                        with sqlite_connection() as connection:
                            needs_track_fetch = _album_tracklist_needs_fetch(connection=connection, album_id=spotify_id)
                        if needs_track_fetch:
                            album_track_fetch_ids.add(spotify_id)
                            queued_album_track_fetch_queue_ids.setdefault(spotify_id, []).append(queue_id)
                        else:
                            telemetry["skipped"] += 1
                            _queue_mark_done(queue_id=queue_id)
                    else:
                        # Metadata may be complete while tracklist remains incomplete; keep pending if not complete.
                        telemetry["skipped"] += 1
                        with sqlite_connection() as connection:
                            now_complete = _is_album_catalog_complete(connection=connection, spotify_album_id=spotify_id)
                        if now_complete:
                            _queue_mark_done(queue_id=queue_id)
                    continue
                try:
                    album_payload = _request_json(
                        access_token=access_token,
                        url=f"https://api.spotify.com/v1/albums/{spotify_id}",
                        params={"market": normalized_market},
                        endpoint_category="queue_album",
                        telemetry=telemetry,
                        max_429=bounded_max_429,
                        sleeper=sleep_fn,
                        fetcher=fetch_fn,
                    )
                    if not isinstance(album_payload, dict) or not album_payload.get("id"):
                        telemetry["errors"] += 1
                        error_text = "queue_album: Missing album payload."
                        _upsert_album_catalog_error(
                            spotify_album_id=spotify_id,
                            market=normalized_market,
                            fetched_at=fetched_at,
                            last_error=error_text,
                        )
                        _queue_mark_error(queue_id=queue_id, error_message=error_text)
                        continue
                    _upsert_album_catalog(
                        album=album_payload,
                        market=normalized_market,
                        fetched_at=fetched_at,
                        last_status="ok",
                        last_error=None,
                    )
                    telemetry["albums_fetched"] += 1
                    album_id = str(album_payload.get("id") or spotify_id)
                    album_ids.add(album_id)
                    if normalized_run_mode == "metadata_only":
                        with sqlite_connection() as connection:
                            now_metadata_complete = _is_album_metadata_complete(connection=connection, spotify_album_id=album_id)
                        if now_metadata_complete:
                            _queue_mark_done(queue_id=queue_id)
                    elif include_albums:
                        album_track_fetch_ids.add(album_id)
                        queued_album_track_fetch_queue_ids.setdefault(album_id, []).append(queue_id)
                    else:
                        with sqlite_connection() as connection:
                            now_complete = _is_album_catalog_complete(connection=connection, spotify_album_id=album_id)
                        if now_complete:
                            _queue_mark_done(queue_id=queue_id)
                except RuntimeError as exc:
                    telemetry["errors"] += 1
                    _upsert_album_catalog_error(
                        spotify_album_id=spotify_id,
                        market=normalized_market,
                        fetched_at=fetched_at,
                        last_error=str(exc),
                    )
                    _queue_mark_error(queue_id=queue_id, error_message=str(exc))
                continue

            # Defensive handling if legacy invalid rows exist.
            telemetry["errors"] += 1
            _queue_mark_error(queue_id=queue_id, error_message=f"Unsupported entity_type '{entity_type}'")

        remaining_bulk_capacity = max(0, bounded_limit - queue_slots_used)
        track_ids_to_fetch = [track_id for track_id in deduped_track_ids if track_id not in queued_track_ids_processed]
        if remaining_bulk_capacity == 0:
            track_ids_to_fetch = []
        elif len(track_ids_to_fetch) > remaining_bulk_capacity:
            track_ids_to_fetch = track_ids_to_fetch[:remaining_bulk_capacity]

        if not force_refresh and deduped_track_ids:
            track_ids_to_fetch, known_album_ids = _split_track_ids_for_fetch(
                track_ids=deduped_track_ids,
                require_identity_metadata=normalized_run_mode == "metadata_only",
            )
            track_ids_to_fetch = [track_id for track_id in track_ids_to_fetch if track_id not in queued_track_ids_processed]
            if remaining_bulk_capacity == 0:
                track_ids_to_fetch = []
            elif len(track_ids_to_fetch) > remaining_bulk_capacity:
                track_ids_to_fetch = track_ids_to_fetch[:remaining_bulk_capacity]
            telemetry["skipped"] += max(0, len(deduped_track_ids) - len(track_ids_to_fetch) - len(queued_track_ids_processed))
            album_ids.update(known_album_ids)

        for id_chunk in _chunked(track_ids_to_fetch, TRACK_BATCH_SIZE):
            if track_batch_unavailable:
                _fetch_single_tracks(id_chunk)
                continue
            try:
                _raise_if_should_stop()
                payload = _request_json(
                    access_token=access_token,
                    url="https://api.spotify.com/v1/tracks",
                    params={"ids": ",".join(id_chunk), "market": normalized_market},
                    endpoint_category="tracks_batch",
                    telemetry=telemetry,
                    max_429=bounded_max_429,
                    sleeper=sleep_fn,
                    fetcher=fetch_fn,
                )
                tracks = payload.get("tracks") if isinstance(payload.get("tracks"), list) else []
                for track in tracks:
                    if not isinstance(track, dict):
                        telemetry["skipped"] += 1
                        continue
                    if not track.get("id"):
                        telemetry["skipped"] += 1
                        continue
                    _upsert_track_catalog(
                        track=track,
                        market=normalized_market,
                        fetched_at=fetched_at,
                        last_status="ok",
                        last_error=None,
                    )
                    telemetry["tracks_fetched"] += 1
                    telemetry["tracks_upserted"] += 1
                    _emit_progress_if_due()
                    album = track.get("album") if isinstance(track.get("album"), dict) else {}
                    if album.get("id"):
                        album_ids.add(str(album["id"]))
            except RuntimeError as exc:
                error_text = str(exc)
                if "tracks_batch: Spotify request failed with status 403" not in error_text:
                    raise

                track_batch_unavailable = True
                _warn_once(TRACK_BATCH_FORBIDDEN_WARNING)
                _fetch_single_tracks(id_chunk)

        if normalized_run_mode == "metadata_only" and run_album_metadata:
            album_ids.update(str(album_id) for album_id in seeded_source_album_ids if str(album_id).strip())
        representative_album_ids = list(dict.fromkeys(_representative_album_ids(album_ids)))
        representative_album_ids = [album_id for album_id in representative_album_ids if album_id not in queued_album_ids_processed]
        if run_album_metadata:
            telemetry["albums_seen"] += len(representative_album_ids)
        if run_album_metadata and include_albums and representative_album_ids:
            album_ids_to_fetch = representative_album_ids
            if not force_refresh:
                if normalized_run_mode == "metadata_only":
                    album_ids_to_fetch = _split_album_metadata_ids_for_fetch(album_ids=representative_album_ids)
                else:
                    album_ids_to_fetch = _split_album_ids_for_fetch(album_ids=representative_album_ids)
                telemetry["skipped"] += max(0, len(representative_album_ids) - len(album_ids_to_fetch))
                metadata_skipped_album_ids = [album_id for album_id in representative_album_ids if album_id not in set(album_ids_to_fetch)]
                if normalized_run_mode != "metadata_only" and metadata_skipped_album_ids:
                    with sqlite_connection() as connection:
                        for album_id in metadata_skipped_album_ids:
                            if _album_tracklist_needs_fetch(connection=connection, album_id=album_id):
                                album_track_fetch_ids.add(album_id)
            for album_chunk in _chunked(album_ids_to_fetch, ALBUM_BATCH_SIZE):
                album_payloads: list[dict[str, Any]] = []
                if album_batch_unavailable:
                    album_payloads = _fetch_single_albums(album_chunk)
                else:
                    try:
                        _raise_if_should_stop()
                        payload = _request_json(
                            access_token=access_token,
                            url="https://api.spotify.com/v1/albums",
                            params={"ids": ",".join(album_chunk), "market": normalized_market},
                            endpoint_category="album_batch",
                            telemetry=telemetry,
                            max_429=bounded_max_429,
                            sleeper=sleep_fn,
                            fetcher=fetch_fn,
                        )
                        albums = payload.get("albums") if isinstance(payload.get("albums"), list) else []
                        album_payloads = [album for album in albums if isinstance(album, dict)]
                    except RuntimeError as exc:
                        error_text = str(exc)
                        if "album_batch: Spotify request failed with status 403" not in error_text:
                            raise
                        album_batch_unavailable = True
                        _warn_once(ALBUM_BATCH_FORBIDDEN_WARNING)
                        album_payloads = _fetch_single_albums(album_chunk)

                for album in album_payloads:
                    if not isinstance(album, dict) or not album.get("id"):
                        telemetry["skipped"] += 1
                        continue
                    album_id = str(album["id"])
                    _upsert_album_catalog(
                        album=album,
                        market=normalized_market,
                        fetched_at=fetched_at,
                        last_status="ok",
                        last_error=None,
                    )
                    telemetry["albums_fetched"] += 1
                    if run_album_tracklists or normalized_target == "all":
                        album_track_fetch_ids.add(album_id)
                _raise_if_should_stop()

        if (run_album_tracklists or normalized_target == "all") and include_albums and album_track_fetch_ids:
            sorted_album_track_fetch_ids = sorted(album_track_fetch_ids)
            telemetry["album_tracklists_seen"] += len(sorted_album_track_fetch_ids)
            eligible_album_track_fetch_ids: list[str] = []
            if normalized_album_tracklist_policy == "none":
                telemetry["album_tracklists_skipped_by_policy"] += len(sorted_album_track_fetch_ids)
            elif normalized_album_tracklist_policy == "priority_only":
                queued_album_ids = set(queued_album_track_fetch_queue_ids.keys())
                for album_id in sorted_album_track_fetch_ids:
                    if album_id in queued_album_ids:
                        eligible_album_track_fetch_ids.append(album_id)
                    else:
                        telemetry["album_tracklists_skipped_by_policy"] += 1
            elif normalized_album_tracklist_policy == "relevant_albums":
                queued_album_ids = set(queued_album_track_fetch_queue_ids.keys())
                relevance_stats = _album_relevance_stats(album_ids=sorted_album_track_fetch_ids)
                for album_id in sorted_album_track_fetch_ids:
                    if album_id in queued_album_ids:
                        eligible_album_track_fetch_ids.append(album_id)
                        continue
                    listened_track_count, total_album_play_count = relevance_stats.get(album_id, (0, 0))
                    if listened_track_count >= 2 or total_album_play_count >= 3:
                        eligible_album_track_fetch_ids.append(album_id)
                    else:
                        telemetry["album_tracklists_skipped_by_policy"] += 1
            else:
                eligible_album_track_fetch_ids = list(sorted_album_track_fetch_ids)

            for album_id in eligible_album_track_fetch_ids:
                _raise_if_should_stop()
                if not force_refresh and album_id in _existing_complete_album_tracklist_ids(album_ids=[album_id]):
                    telemetry["skipped"] += 1
                    for queued_id in queued_album_track_fetch_queue_ids.get(album_id, []):
                        _queue_mark_done(queue_id=queued_id)
                    continue
                telemetry["album_tracklists_fetched"] += 1

                with sqlite_connection() as connection:
                    resume_offset = _album_track_resume_offset(
                        connection=connection,
                        album_id=album_id,
                        force_refresh=force_refresh,
                    )

                next_url: str | None = f"https://api.spotify.com/v1/albums/{album_id}/tracks"
                next_params: dict[str, Any] | None = {
                    "limit": ALBUM_TRACK_PAGE_SIZE,
                    "offset": resume_offset,
                    "market": normalized_market,
                }
                album_tracks_pages_fetched = 0
                seen_page_requests: set[str] = set()
                while next_url is not None:
                    _raise_if_should_stop()
                    if album_tracks_pages_fetched >= bounded_max_album_tracks_pages_per_album:
                        telemetry["album_tracklists_capped"] += 1
                        telemetry["skipped"] += 1
                        warning_text = f"album track pagination capped for {album_id}"
                        if warning_text not in telemetry["warnings"]:
                            telemetry["warnings"].append(warning_text)
                        break

                    page_request_key = f"{next_url}|{_json_dump(next_params or {})}"
                    if page_request_key in seen_page_requests:
                        warning_text = f"album track pagination loop detected for {album_id}; stopped pagination"
                        telemetry["skipped"] += 1
                        if warning_text not in telemetry["warnings"]:
                            telemetry["warnings"].append(warning_text)
                        break
                    seen_page_requests.add(page_request_key)

                    track_payload = _request_json(
                        access_token=access_token,
                        url=next_url,
                        params=next_params or {},
                        endpoint_category="album_tracks",
                        telemetry=telemetry,
                        max_429=bounded_max_429,
                        sleeper=sleep_fn,
                        fetcher=fetch_fn,
                    )
                    items = track_payload.get("items") if isinstance(track_payload.get("items"), list) else []
                    for album_track in items:
                        if not isinstance(album_track, dict):
                            telemetry["skipped"] += 1
                            continue
                        if not album_track.get("id"):
                            telemetry["skipped"] += 1
                            continue
                        _upsert_album_track(
                            album_id=album_id,
                            track=album_track,
                            market=normalized_market,
                            fetched_at=fetched_at,
                            last_status="ok",
                            last_error=None,
                        )
                        telemetry["album_tracks_upserted"] += 1

                    album_tracks_pages_fetched += 1
                    next_value = track_payload.get("next")
                    if not items and next_value:
                        warning_text = f"album track pagination returned empty page for {album_id}; stopped pagination"
                        telemetry["skipped"] += 1
                        if warning_text not in telemetry["warnings"]:
                            telemetry["warnings"].append(warning_text)
                        break
                    if isinstance(next_value, str) and next_value.strip():
                        next_url = next_value
                        next_params = {}
                    else:
                        next_url = None

                for queued_id in queued_album_track_fetch_queue_ids.get(album_id, []):
                    with sqlite_connection() as connection:
                        if _is_album_catalog_complete(connection=connection, spotify_album_id=album_id):
                            _queue_mark_done(queue_id=queued_id)

    except _PartialStop as exc:
        status_text = "partial"
        partial = True
        stop_reason = exc.reason
        stop_text = f"Stopped early due to {exc.reason}"
        if exc.reason == "rate_limited":
            telemetry["has_more"] = True
        if stop_text not in telemetry["warnings"]:
            telemetry["warnings"].append(stop_text)
        last_error = stop_text
    except Exception as exc:
        telemetry["errors"] += 1
        has_progress = (
            int(telemetry.get("requests_success", 0)) > 0
            or int(telemetry.get("tracks_upserted", 0)) > 0
            or int(telemetry.get("albums_fetched", 0)) > 0
            or int(telemetry.get("album_tracks_upserted", 0)) > 0
        )
        if has_progress:
            status_text = "partial"
            partial = True
        else:
            status_text = "failed"
        last_error = str(exc)
    finally:
        final_integrity = inspect_spotify_nested_metadata_integrity(sample_limit=5)
        initial_album_gap_count = int(initial_integrity["counts"]["tracks_with_album_spotify_id_missing_local_album_name"])
        final_album_gap_count = int(final_integrity["counts"]["tracks_with_album_spotify_id_missing_local_album_name"])
        if final_album_gap_count > initial_album_gap_count and int(telemetry.get("tracks_upserted", 0) or 0) > 0:
            warning = (
                "Local metadata integrity warning: track processing increased "
                "tracks_with_album_spotify_id_missing_local_album_name."
            )
            if warning not in telemetry["warnings"]:
                telemetry["warnings"].append(warning)
        telemetry["_elapsed_seconds"] = max(0.0, time.monotonic() - started_monotonic)
        _run_finish(run_id=run_id, payload=telemetry, status_text=status_text, last_error=last_error)

    return _result_payload()
