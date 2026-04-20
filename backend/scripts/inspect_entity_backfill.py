from __future__ import annotations

import json

from backend.app.db import ensure_sqlite_db, sqlite_connection


EXCLUDED_SOURCE_TYPES = ("manual_test",)


def _rows(query: str, params: tuple[object, ...]) -> list[dict[str, object]]:
    with sqlite_connection(row_factory=None) as connection:
        connection.row_factory = None
        cursor = connection.execute(query, params)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def inspect_entity_backfill(*, limit: int = 20) -> dict[str, object]:
    placeholders = ",".join("?" for _ in EXCLUDED_SOURCE_TYPES)

    album_coverage = _rows(
        """
        SELECT
          ra.id AS release_album_id,
          ra.primary_name AS album_name,
          count(DISTINCT at.release_track_id) AS linked_track_count,
          count(DISTINCT aa.artist_id) AS linked_artist_count,
          count(DISTINCT sam.source_album_id) AS source_album_count
        FROM release_album ra
        LEFT JOIN album_track at
          ON at.release_album_id = ra.id
        LEFT JOIN album_artist aa
          ON aa.release_album_id = ra.id
        LEFT JOIN source_album_map sam
          ON sam.release_album_id = ra.id
        GROUP BY ra.id, ra.primary_name
        ORDER BY linked_track_count DESC, linked_artist_count DESC, source_album_count DESC, ra.primary_name ASC
        LIMIT ?
        """,
        (limit,),
    )

    artist_coverage = _rows(
        """
        SELECT
          a.id AS artist_id,
          a.canonical_name AS artist_name,
          count(DISTINCT ta.release_track_id) AS linked_track_count,
          count(DISTINCT aa.release_album_id) AS linked_album_count,
          count(DISTINCT sam.source_artist_id) AS source_artist_count
        FROM artist a
        LEFT JOIN track_artist ta
          ON ta.artist_id = a.id
        LEFT JOIN album_artist aa
          ON aa.artist_id = a.id
        LEFT JOIN source_artist_map sam
          ON sam.artist_id = a.id
        GROUP BY a.id, a.canonical_name
        ORDER BY linked_track_count DESC, linked_album_count DESC, source_artist_count DESC, a.canonical_name ASC
        LIMIT ?
        """,
        (limit,),
    )

    artist_string_spread = _rows(
        f"""
        SELECT
          artist_name_raw,
          count(DISTINCT album_name_raw) AS distinct_album_names,
          count(DISTINCT track_name_raw) AS distinct_track_names,
          count(*) AS raw_event_count
        FROM raw_play_event
        WHERE source_type NOT IN ({placeholders})
          AND artist_name_raw IS NOT NULL
          AND trim(artist_name_raw) != ''
        GROUP BY artist_name_raw
        HAVING count(DISTINCT album_name_raw) >= 5
        ORDER BY distinct_album_names DESC, distinct_track_names DESC, raw_event_count DESC, artist_name_raw ASC
        LIMIT ?
        """,
        (*EXCLUDED_SOURCE_TYPES, limit),
    )

    album_name_spread = _rows(
        f"""
        SELECT
          album_name_raw,
          count(DISTINCT artist_name_raw) AS distinct_artist_strings,
          count(DISTINCT track_name_raw) AS distinct_track_names,
          count(*) AS raw_event_count
        FROM raw_play_event
        WHERE source_type NOT IN ({placeholders})
          AND album_name_raw IS NOT NULL
          AND trim(album_name_raw) != ''
        GROUP BY album_name_raw
        HAVING count(DISTINCT artist_name_raw) >= 2
        ORDER BY distinct_artist_strings DESC, distinct_track_names DESC, raw_event_count DESC, album_name_raw ASC
        LIMIT ?
        """,
        (*EXCLUDED_SOURCE_TYPES, limit),
    )

    suspicious_album_merge_candidates = _rows(
        f"""
        SELECT
          album_name_raw,
          count(DISTINCT artist_name_raw) AS distinct_artist_strings,
          count(DISTINCT track_name_raw) AS distinct_track_names,
          count(*) AS raw_event_count
        FROM raw_play_event
        WHERE source_type NOT IN ({placeholders})
          AND album_name_raw IS NOT NULL
          AND trim(album_name_raw) != ''
        GROUP BY album_name_raw
        HAVING count(DISTINCT artist_name_raw) >= 3
        ORDER BY distinct_artist_strings DESC, raw_event_count DESC, distinct_track_names DESC, album_name_raw ASC
        LIMIT ?
        """,
        (*EXCLUDED_SOURCE_TYPES, limit),
    )

    suspicious_artist_merge_candidates = _rows(
        f"""
        SELECT
          artist_name_raw,
          count(DISTINCT album_name_raw) AS distinct_album_names,
          count(DISTINCT track_name_raw) AS distinct_track_names,
          count(*) AS raw_event_count
        FROM raw_play_event
        WHERE source_type NOT IN ({placeholders})
          AND artist_name_raw IS NOT NULL
          AND trim(artist_name_raw) != ''
        GROUP BY artist_name_raw
        HAVING count(DISTINCT album_name_raw) >= 8
        ORDER BY distinct_album_names DESC, raw_event_count DESC, distinct_track_names DESC, artist_name_raw ASC
        LIMIT ?
        """,
        (*EXCLUDED_SOURCE_TYPES, limit),
    )

    suspicious_track_merge_candidates = _rows(
        f"""
        SELECT
          track_name_raw,
          count(DISTINCT artist_name_raw) AS distinct_artist_strings,
          count(DISTINCT album_name_raw) AS distinct_album_names,
          count(*) AS raw_event_count
        FROM raw_play_event
        WHERE source_type NOT IN ({placeholders})
          AND track_name_raw IS NOT NULL
          AND trim(track_name_raw) != ''
        GROUP BY track_name_raw
        HAVING count(DISTINCT artist_name_raw) >= 3
        ORDER BY distinct_artist_strings DESC, distinct_album_names DESC, raw_event_count DESC, track_name_raw ASC
        LIMIT ?
        """,
        (*EXCLUDED_SOURCE_TYPES, limit),
    )

    multi_artist_string_examples = _rows(
        f"""
        SELECT
          artist_name_raw,
          count(*) AS raw_event_count,
          count(DISTINCT track_name_raw) AS distinct_track_names,
          count(DISTINCT album_name_raw) AS distinct_album_names
        FROM raw_play_event
        WHERE source_type NOT IN ({placeholders})
          AND artist_name_raw IS NOT NULL
          AND trim(artist_name_raw) != ''
          AND (
            artist_name_raw LIKE '%,%'
            OR artist_name_raw LIKE '% & %'
            OR artist_name_raw LIKE '% feat.%'
            OR artist_name_raw LIKE '% featuring %'
            OR artist_name_raw LIKE '% with %'
            OR artist_name_raw LIKE '% x %'
          )
        GROUP BY artist_name_raw
        ORDER BY raw_event_count DESC, distinct_track_names DESC, artist_name_raw ASC
        LIMIT ?
        """,
        (*EXCLUDED_SOURCE_TYPES, limit),
    )

    multi_artist_track_rows = _rows(
        f"""
        SELECT
          r.track_name_raw,
          r.artist_name_raw,
          r.album_name_raw,
          count(*) AS raw_event_count,
          count(DISTINCT ta.artist_id) AS linked_artist_rows
        FROM raw_play_event r
        JOIN source_track st
          ON (
            (st.source_name = 'spotify' AND st.external_id = r.spotify_track_id)
            OR (st.source_name = 'spotify_uri' AND st.external_id = r.spotify_track_uri)
          )
        JOIN source_track_map stm
          ON stm.source_track_id = st.id
        LEFT JOIN track_artist ta
          ON ta.release_track_id = stm.release_track_id
        WHERE r.source_type NOT IN ({placeholders})
          AND r.artist_name_raw IS NOT NULL
          AND trim(r.artist_name_raw) != ''
          AND (
            r.artist_name_raw LIKE '%,%'
            OR r.artist_name_raw LIKE '% & %'
            OR r.artist_name_raw LIKE '% feat.%'
            OR r.artist_name_raw LIKE '% featuring %'
            OR r.artist_name_raw LIKE '% with %'
            OR r.artist_name_raw LIKE '% x %'
          )
        GROUP BY r.track_name_raw, r.artist_name_raw, r.album_name_raw
        ORDER BY linked_artist_rows DESC, raw_event_count DESC, r.artist_name_raw ASC, r.track_name_raw ASC
        LIMIT ?
        """,
        (*EXCLUDED_SOURCE_TYPES, limit),
    )

    multi_artist_album_rows = _rows(
        f"""
        SELECT
          r.album_name_raw,
          r.artist_name_raw,
          count(*) AS raw_event_count,
          count(DISTINCT aa.artist_id) AS linked_artist_rows,
          count(DISTINCT r.track_name_raw) AS distinct_track_names
        FROM raw_play_event r
        LEFT JOIN source_album sa
          ON (sa.source_name = 'spotify' AND sa.external_id = r.spotify_album_id)
        LEFT JOIN source_album_map sam
          ON sam.source_album_id = sa.id
        LEFT JOIN album_artist aa
          ON aa.release_album_id = sam.release_album_id
        WHERE r.source_type NOT IN ({placeholders})
          AND r.artist_name_raw IS NOT NULL
          AND trim(r.artist_name_raw) != ''
          AND r.album_name_raw IS NOT NULL
          AND trim(r.album_name_raw) != ''
          AND (
            r.artist_name_raw LIKE '%,%'
            OR r.artist_name_raw LIKE '% & %'
            OR r.artist_name_raw LIKE '% feat.%'
            OR r.artist_name_raw LIKE '% featuring %'
            OR r.artist_name_raw LIKE '% with %'
            OR r.artist_name_raw LIKE '% x %'
          )
        GROUP BY r.album_name_raw, r.artist_name_raw
        ORDER BY linked_artist_rows DESC, raw_event_count DESC, r.album_name_raw ASC, r.artist_name_raw ASC
        LIMIT ?
        """,
        (*EXCLUDED_SOURCE_TYPES, limit),
    )

    multi_artist_handling_summary = _rows(
        f"""
        WITH multi_artist_raw AS (
          SELECT
            r.id,
            r.track_name_raw,
            r.artist_name_raw,
            r.album_name_raw,
            coalesce(stm.release_track_id, 0) AS release_track_id,
            coalesce(sam.release_album_id, 0) AS release_album_id
          FROM raw_play_event r
          LEFT JOIN source_track st
            ON (
              (st.source_name = 'spotify' AND st.external_id = r.spotify_track_id)
              OR (st.source_name = 'spotify_uri' AND st.external_id = r.spotify_track_uri)
            )
          LEFT JOIN source_track_map stm
            ON stm.source_track_id = st.id
          LEFT JOIN source_album sa
            ON (sa.source_name = 'spotify' AND sa.external_id = r.spotify_album_id)
          LEFT JOIN source_album_map sam
            ON sam.source_album_id = sa.id
          WHERE r.source_type NOT IN ({placeholders})
            AND r.artist_name_raw IS NOT NULL
            AND trim(r.artist_name_raw) != ''
            AND (
              r.artist_name_raw LIKE '%,%'
              OR r.artist_name_raw LIKE '% & %'
              OR r.artist_name_raw LIKE '% feat.%'
              OR r.artist_name_raw LIKE '% featuring %'
              OR r.artist_name_raw LIKE '% with %'
              OR r.artist_name_raw LIKE '% x %'
            )
        )
        SELECT
          count(*) AS raw_multi_artist_rows,
          count(DISTINCT track_name_raw || '|' || artist_name_raw || '|' || coalesce(album_name_raw, '')) AS distinct_multi_artist_row_shapes,
          sum(
            CASE
              WHEN release_track_id != 0 THEN 1
              ELSE 0
            END
          ) AS rows_with_track_mapping,
          sum(
            CASE
              WHEN release_album_id != 0 THEN 1
              ELSE 0
            END
          ) AS rows_with_album_mapping
        FROM multi_artist_raw
        """,
        EXCLUDED_SOURCE_TYPES,
    )

    multi_artist_linked_distribution = _rows(
        f"""
        WITH multi_artist_tracks AS (
          SELECT
            r.track_name_raw,
            r.artist_name_raw,
            r.album_name_raw,
            coalesce(stm.release_track_id, 0) AS release_track_id
          FROM raw_play_event r
          LEFT JOIN source_track st
            ON (
              (st.source_name = 'spotify' AND st.external_id = r.spotify_track_id)
              OR (st.source_name = 'spotify_uri' AND st.external_id = r.spotify_track_uri)
            )
          LEFT JOIN source_track_map stm
            ON stm.source_track_id = st.id
          WHERE r.source_type NOT IN ({placeholders})
            AND r.artist_name_raw IS NOT NULL
            AND trim(r.artist_name_raw) != ''
            AND (
              r.artist_name_raw LIKE '%,%'
              OR r.artist_name_raw LIKE '% & %'
              OR r.artist_name_raw LIKE '% feat.%'
              OR r.artist_name_raw LIKE '% featuring %'
              OR r.artist_name_raw LIKE '% with %'
              OR r.artist_name_raw LIKE '% x %'
            )
        ),
        grouped AS (
          SELECT
            mat.track_name_raw,
            mat.artist_name_raw,
            mat.album_name_raw,
            count(DISTINCT ta.artist_id) AS linked_artist_rows
          FROM multi_artist_tracks mat
          LEFT JOIN track_artist ta
            ON ta.release_track_id = mat.release_track_id
          GROUP BY mat.track_name_raw, mat.artist_name_raw, mat.album_name_raw
        )
        SELECT
          linked_artist_rows,
          count(*) AS row_shape_count
        FROM grouped
        GROUP BY linked_artist_rows
        ORDER BY linked_artist_rows ASC
        """,
        EXCLUDED_SOURCE_TYPES,
    )

    spotify_structured_artist_string_evidence = _rows(
        f"""
        WITH spotify_structured AS (
          SELECT
            r.artist_name_raw,
            r.track_name_raw,
            r.album_name_raw,
            count(DISTINCT sa.external_id) AS structured_artist_count,
            group_concat(DISTINCT a.canonical_name) AS linked_artist_names,
            count(*) AS raw_event_count
          FROM raw_play_event r
          JOIN source_track st
            ON st.source_name = 'spotify'
           AND st.external_id = r.spotify_track_id
          JOIN source_track_map stm
            ON stm.source_track_id = st.id
          JOIN track_artist ta
            ON ta.release_track_id = stm.release_track_id
          JOIN artist a
            ON a.id = ta.artist_id
          LEFT JOIN source_artist_map sam
            ON sam.artist_id = a.id
          LEFT JOIN source_artist sa
            ON sa.id = sam.source_artist_id
           AND sa.source_name = 'spotify'
          WHERE r.source_type NOT IN ({placeholders})
            AND r.artist_name_raw IS NOT NULL
            AND trim(r.artist_name_raw) != ''
            AND (
              r.artist_name_raw LIKE '%,%'
              OR r.artist_name_raw LIKE '% & %'
              OR r.artist_name_raw LIKE '% feat.%'
              OR r.artist_name_raw LIKE '% featuring %'
              OR r.artist_name_raw LIKE '% with %'
              OR r.artist_name_raw LIKE '% x %'
            )
          GROUP BY r.artist_name_raw, r.track_name_raw, r.album_name_raw
        )
        SELECT
          artist_name_raw,
          track_name_raw,
          album_name_raw,
          structured_artist_count,
          linked_artist_names,
          raw_event_count
        FROM spotify_structured
        ORDER BY structured_artist_count DESC, raw_event_count DESC, artist_name_raw ASC, track_name_raw ASC
        LIMIT ?
        """,
        (*EXCLUDED_SOURCE_TYPES, limit),
    )

    artist_string_split_candidates = _rows(
        f"""
        WITH structured AS (
          SELECT
            r.artist_name_raw,
            count(DISTINCT sa.external_id) AS structured_artist_count,
            count(*) AS raw_event_count
          FROM raw_play_event r
          JOIN source_track st
            ON st.source_name = 'spotify'
           AND st.external_id = r.spotify_track_id
          JOIN source_track_map stm
            ON stm.source_track_id = st.id
          JOIN track_artist ta
            ON ta.release_track_id = stm.release_track_id
          JOIN artist a
            ON a.id = ta.artist_id
          LEFT JOIN source_artist_map sam
            ON sam.artist_id = a.id
          LEFT JOIN source_artist sa
            ON sa.id = sam.source_artist_id
           AND sa.source_name = 'spotify'
          WHERE r.source_type NOT IN ({placeholders})
            AND r.artist_name_raw IS NOT NULL
            AND trim(r.artist_name_raw) != ''
            AND (
              r.artist_name_raw LIKE '%,%'
              OR r.artist_name_raw LIKE '% & %'
              OR r.artist_name_raw LIKE '% feat.%'
              OR r.artist_name_raw LIKE '% featuring %'
              OR r.artist_name_raw LIKE '% with %'
              OR r.artist_name_raw LIKE '% x %'
            )
          GROUP BY r.artist_name_raw
        )
        SELECT
          artist_name_raw,
          structured_artist_count,
          raw_event_count,
          CASE
            WHEN structured_artist_count >= 2 THEN 'candidate_split'
            ELSE 'likely_single_entity'
          END AS suggested_handling
        FROM structured
        ORDER BY structured_artist_count DESC, raw_event_count DESC, artist_name_raw ASC
        LIMIT ?
        """,
        (*EXCLUDED_SOURCE_TYPES, limit),
    )

    unmapped_history_text_rows = _rows(
        f"""
        SELECT
          r.source_type,
          r.track_name_raw,
          r.artist_name_raw,
          r.album_name_raw,
          count(*) AS raw_event_count
        FROM raw_play_event r
        LEFT JOIN source_track st
          ON (
            (st.source_name = 'spotify' AND st.external_id = r.spotify_track_id)
            OR (st.source_name = 'spotify_uri' AND st.external_id = r.spotify_track_uri)
          )
        LEFT JOIN source_track_map stm
          ON stm.source_track_id = st.id
        WHERE r.source_type NOT IN ({placeholders})
          AND stm.release_track_id IS NULL
          AND r.track_name_raw IS NOT NULL
          AND trim(r.track_name_raw) != ''
        GROUP BY r.source_type, r.track_name_raw, r.artist_name_raw, r.album_name_raw
        ORDER BY raw_event_count DESC, r.source_type ASC, r.artist_name_raw ASC, r.track_name_raw ASC
        LIMIT ?
        """,
        (*EXCLUDED_SOURCE_TYPES, limit),
    )

    return {
        "excluded_source_types": list(EXCLUDED_SOURCE_TYPES),
        "album_coverage": album_coverage,
        "artist_coverage": artist_coverage,
        "artist_string_spread": artist_string_spread,
        "album_name_spread": album_name_spread,
        "suspicious_album_merge_candidates": suspicious_album_merge_candidates,
        "suspicious_artist_merge_candidates": suspicious_artist_merge_candidates,
        "suspicious_track_merge_candidates": suspicious_track_merge_candidates,
        "multi_artist_string_examples": multi_artist_string_examples,
        "multi_artist_handling_summary": multi_artist_handling_summary,
        "multi_artist_linked_distribution": multi_artist_linked_distribution,
        "spotify_structured_artist_string_evidence": spotify_structured_artist_string_evidence,
        "artist_string_split_candidates": artist_string_split_candidates,
        "multi_artist_track_rows": multi_artist_track_rows,
        "multi_artist_album_rows": multi_artist_album_rows,
        "unmapped_history_text_rows": unmapped_history_text_rows,
    }


def main() -> None:
    ensure_sqlite_db()
    result = inspect_entity_backfill()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
