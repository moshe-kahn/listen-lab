from __future__ import annotations

import sys

from backend.app.config import get_settings
from backend.app.db import _parse_grouping_note, ensure_sqlite_db, sqlite_connection


def _scalar(query: str, params: tuple[object, ...] = ()) -> int:
    with sqlite_connection() as connection:
        row = connection.execute(query, params).fetchone()
    assert row is not None
    return int(row[0])


def _rows(query: str, params: tuple[object, ...] = ()) -> list[tuple[object, ...]]:
    with sqlite_connection() as connection:
        return connection.execute(query, params).fetchall()


def _print_section(title: str) -> None:
    print()
    print(title)
    print("-" * len(title))


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ensure_sqlite_db()
    settings = get_settings()

    schema_version = _scalar("SELECT version FROM schema_version LIMIT 1")
    release_track_count = _scalar("SELECT count(*) FROM release_track")
    source_track_count = _scalar("SELECT count(*) FROM source_track")
    track_map_count = _scalar("SELECT count(*) FROM source_track_map")
    spotify_uri_source_track_count = _scalar(
        "SELECT count(*) FROM source_track WHERE source_name = 'spotify_uri'"
    )
    history_raw_source_track_count = _scalar(
        "SELECT count(*) FROM source_track WHERE source_name = 'history_raw'"
    )
    spotify_id_uri_equivalent_count = _scalar(
        "SELECT count(*) FROM source_track_map WHERE match_method = 'spotify_id_uri_equivalent'"
    )
    multi_source_release_track_count = _scalar(
        """
        SELECT count(*)
        FROM (
          SELECT release_track_id
          FROM source_track_map
          GROUP BY release_track_id
          HAVING count(*) > 1
        )
        """
    )
    analysis_track_count = _scalar("SELECT count(*) FROM analysis_track")
    analysis_track_map_count = _scalar("SELECT count(*) FROM analysis_track_map")
    suggested_analysis_track_map_count = _scalar(
        "SELECT count(*) FROM analysis_track_map WHERE status = 'suggested'"
    )
    release_track_merge_log_count = _scalar("SELECT count(*) FROM release_track_merge_log")
    track_relationship_count = _scalar("SELECT count(*) FROM track_relationship")

    multi_source_examples = _rows(
        """
        SELECT
          rt.id,
          rt.primary_name,
          count(*) AS source_count,
          group_concat(st.source_name || ':' || st.external_id, ' | ') AS source_refs,
          group_concat(stm.match_method || '@' || printf('%.2f', stm.confidence), ' | ') AS mapping_methods
        FROM release_track rt
        JOIN source_track_map stm
          ON stm.release_track_id = rt.id
        JOIN source_track st
          ON st.id = stm.source_track_id
        GROUP BY rt.id, rt.primary_name
        HAVING count(*) > 1
        ORDER BY source_count DESC, rt.id ASC
        LIMIT 10
        """
    )
    history_raw_examples = _rows(
        """
        SELECT
          st.id,
          st.source_name_raw,
          rt.id,
          rt.primary_name,
          stm.match_method,
          printf('%.2f', stm.confidence),
          stm.explanation
        FROM source_track st
        JOIN source_track_map stm
          ON stm.source_track_id = st.id
        JOIN release_track rt
          ON rt.id = stm.release_track_id
        WHERE st.source_name = 'history_raw'
        ORDER BY st.id ASC
        LIMIT 10
        """
    )
    suggested_analysis_examples = _rows(
        """
        SELECT
          at.id,
          at.primary_name,
          at.grouping_note,
          count(atm.release_track_id) AS mapped_release_tracks,
          group_concat(rt.id, ', ') AS release_track_ids
        FROM analysis_track at
        JOIN analysis_track_map atm
          ON atm.analysis_track_id = at.id
        JOIN release_track rt
          ON rt.id = atm.release_track_id
        WHERE atm.status = 'suggested'
        GROUP BY at.id, at.primary_name, at.grouping_note
        ORDER BY mapped_release_tracks DESC, at.id ASC
        LIMIT 10
        """
    )
    duplicate_title_examples = _rows(
        """
        SELECT
          primary_name,
          normalized_name,
          count(*) AS release_track_count,
          group_concat(id, ', ') AS release_track_ids
        FROM release_track
        GROUP BY primary_name, normalized_name
        HAVING count(*) > 1
        ORDER BY release_track_count DESC, primary_name ASC
        LIMIT 10
        """
    )

    print("Track Entity Report")
    print("===================")
    print(f"DB path: {settings.sqlite_db_path}")
    print(f"Schema version: {schema_version}")

    _print_section("Counts")
    print(f"release_tracks: {release_track_count}")
    print(f"source_tracks: {source_track_count}")
    print(f"source_track_map rows: {track_map_count}")
    print(f"spotify_uri source tracks: {spotify_uri_source_track_count}")
    print(f"history_raw source tracks: {history_raw_source_track_count}")
    print(f"spotify_id_uri_equivalent mappings: {spotify_id_uri_equivalent_count}")
    print(f"release tracks with multiple source mappings: {multi_source_release_track_count}")
    print(f"analysis_track rows: {analysis_track_count}")
    print(f"analysis_track_map rows: {analysis_track_map_count}")
    print(f"suggested analysis_track_map rows: {suggested_analysis_track_map_count}")
    print(f"release_track_merge_log rows: {release_track_merge_log_count}")
    print(f"track_relationship rows: {track_relationship_count}")

    _print_section("Interpretation")
    if multi_source_release_track_count == 0:
        print("No release_track rows currently have more than one source_track mapped to them.")
    else:
        print("Some release_track rows are now shared by multiple source_track rows after release-level dedupe.")

    if spotify_id_uri_equivalent_count == 0:
        print("No Spotify ID <-> spotify:track URI equivalence mappings are present yet.")
    else:
        print("Spotify ID <-> spotify:track URI equivalence mappings are present.")

    if history_raw_source_track_count > 0:
        print("Text-derived history_raw tracks remain isolated unless stronger evidence exists.")
    else:
        print("No history_raw-only source_track rows are present.")

    if suggested_analysis_track_map_count > 0:
        print("Song-family analysis grouping suggestions are present for review.")
    else:
        print("No song-family analysis grouping suggestions are present yet.")

    _print_section("Release-Level Dedupe Examples")
    if not multi_source_examples:
        print("None")
    else:
        for release_track_id, primary_name, source_count, source_refs, mapping_methods in multi_source_examples:
            print(f"[release_track {release_track_id}] {primary_name} ({source_count} sources)")
            print(f"  sources: {source_refs}")
            print(f"  methods: {mapping_methods}")

    _print_section("Text-Only history_raw Examples")
    if not history_raw_examples:
        print("None")
    else:
        for source_track_id, source_name_raw, release_track_id, primary_name, match_method, confidence, explanation in history_raw_examples:
            print(
                f"[source_track {source_track_id} -> release_track {release_track_id}] "
                f"{source_name_raw} -> {primary_name}"
            )
            print(f"  method: {match_method} @ {confidence}")
            print(f"  explanation: {explanation}")

    _print_section("Song-Family Analysis Groups")
    if not suggested_analysis_examples:
        print("None")
    else:
        for analysis_track_id, primary_name, grouping_note, mapped_release_tracks, release_track_ids in suggested_analysis_examples:
            grouping_hash, grouping_title_key = _parse_grouping_note(grouping_note)
            print(
                f"[analysis_track {analysis_track_id}] {primary_name} "
                f"({mapped_release_tracks} linked release tracks)"
            )
            print(f"  linked release_tracks: {release_track_ids}")
            print(f"  song-family key: {grouping_title_key or '(unknown)'}")
            print(f"  grouping hash: {grouping_hash or grouping_note}")

    _print_section("Duplicate Title Examples Kept Separate")
    if not duplicate_title_examples:
        print("None")
    else:
        for primary_name, normalized_name, duplicate_count, release_track_ids in duplicate_title_examples:
            print(f"{primary_name} [{normalized_name}] -> {duplicate_count} release tracks ({release_track_ids})")


if __name__ == "__main__":
    main()
