from __future__ import annotations

import sys
from datetime import UTC, datetime

from backend.app.config import BACKEND_DIR, get_settings
from backend.app.db import _parse_grouping_note, ensure_sqlite_db, sqlite_connection


def _rows() -> list[tuple[object, ...]]:
    with sqlite_connection() as connection:
        return connection.execute(
            """
            WITH primary_artists AS (
              SELECT ordered.release_track_id, group_concat(ordered.artist_name, ' | ') AS artist_signature
              FROM (
                SELECT ta.release_track_id, a.canonical_name AS artist_name
                FROM track_artist ta
                JOIN artist a ON a.id = ta.artist_id
                WHERE ta.role = 'primary'
                ORDER BY ta.release_track_id, COALESCE(ta.billing_index, 999999), ta.id, a.canonical_name
              ) ordered
              GROUP BY ordered.release_track_id
            ),
            release_albums AS (
              SELECT
                at.release_track_id,
                group_concat(ra.primary_name, ' | ') AS album_names,
                count(DISTINCT ra.primary_name) AS distinct_album_names
              FROM album_track at
              JOIN release_album ra ON ra.id = at.release_album_id
              GROUP BY at.release_track_id
            ),
            analysis_group_album_counts AS (
              SELECT
                at.id AS analysis_track_id,
                count(DISTINCT ra.primary_name) AS distinct_album_names,
                count(DISTINCT atm.release_track_id) AS linked_release_tracks
              FROM analysis_track at
              JOIN analysis_track_map atm ON atm.analysis_track_id = at.id
              JOIN album_track alt ON alt.release_track_id = atm.release_track_id
              JOIN release_album ra ON ra.id = alt.release_album_id
              GROUP BY at.id
              HAVING count(DISTINCT ra.primary_name) = 1
                 AND count(DISTINCT atm.release_track_id) >= 2
            ),
            source_refs AS (
              SELECT
                stm.release_track_id,
                group_concat(st.source_name || ':' || st.external_id, ' | ') AS source_refs,
                group_concat(stm.match_method || '@' || printf('%.2f', stm.confidence), ' | ') AS source_map_methods
              FROM source_track_map stm
              JOIN source_track st ON st.id = stm.source_track_id
              GROUP BY stm.release_track_id
            )
            SELECT
              at.id AS analysis_track_id,
              at.primary_name AS analysis_name,
              at.grouping_note,
              atm.match_method,
              atm.confidence,
              atm.status,
              rt.id AS release_track_id,
              rt.primary_name AS release_track_name,
              rt.normalized_name,
              coalesce(pa.artist_signature, '') AS artist_signature,
              coalesce(ral.album_names, '') AS album_names,
              agac.linked_release_tracks,
              coalesce(sr.source_refs, '') AS source_refs,
              coalesce(sr.source_map_methods, '') AS source_map_methods
            FROM analysis_group_album_counts agac
            JOIN analysis_track at ON at.id = agac.analysis_track_id
            JOIN analysis_track_map atm ON atm.analysis_track_id = at.id
            JOIN release_track rt ON rt.id = atm.release_track_id
            LEFT JOIN primary_artists pa ON pa.release_track_id = rt.id
            LEFT JOIN release_albums ral ON ral.release_track_id = rt.id
            LEFT JOIN source_refs sr ON sr.release_track_id = rt.id
            ORDER BY agac.linked_release_tracks DESC, at.id ASC, rt.id ASC
            """
        ).fetchall()


def _build_output(rows: list[tuple[object, ...]]) -> str:
    settings = get_settings()
    lines = [
        "Same-Album Analysis Groups",
        "==========================",
        f"DB path: {settings.sqlite_db_path}",
        f"Generated at: {datetime.now(UTC).isoformat().replace('+00:00', 'Z')}",
        "",
    ]

    if not rows:
        lines.append("No same-album-only analysis groups found.")
        return "\n".join(lines) + "\n"

    grouped: dict[int, list[tuple[object, ...]]] = {}
    for row in rows:
        grouped.setdefault(int(row[0]), []).append(row)

    lines.append(f"Groups found: {len(grouped)}")
    lines.append("")

    for analysis_track_id, group_rows in grouped.items():
        first_row = group_rows[0]
        analysis_name = str(first_row[1])
        grouping_note = first_row[2]
        match_method = str(first_row[3])
        confidence = float(first_row[4])
        status = str(first_row[5])
        linked_release_tracks = int(first_row[11])
        grouping_hash, song_family_key = _parse_grouping_note(grouping_note)

        lines.append(f"[analysis_track {analysis_track_id}] {analysis_name}")
        lines.append(f"  song-family key: {song_family_key or '(unknown)'}")
        lines.append(f"  grouping hash: {grouping_hash or grouping_note}")
        lines.append(f"  analysis match method: {match_method}")
        lines.append(f"  confidence: {confidence:.2f}")
        lines.append(f"  status: {status}")
        lines.append(f"  linked release_tracks: {linked_release_tracks}")
        lines.append("")

        for row in group_rows:
            release_track_id = int(row[6])
            release_track_name = str(row[7])
            normalized_name = str(row[8]) if row[8] is not None else ""
            artist_signature = str(row[9])
            album_names = str(row[10])
            source_refs = str(row[12])
            source_map_methods = str(row[13])

            lines.append(f"  - release_track {release_track_id}: {release_track_name}")
            lines.append(f"    normalized_name: {normalized_name}")
            lines.append(f"    primary_artists: {artist_signature}")
            lines.append(f"    albums: {album_names}")
            lines.append(f"    source_refs: {source_refs}")
            lines.append(f"    source_map_methods: {source_map_methods}")
            lines.append("")

    return "\n".join(lines)


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ensure_sqlite_db()
    rows = _rows()

    logs_dir = BACKEND_DIR / "data" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_path = logs_dir / f"same_album_analysis_groups_{timestamp}.txt"
    output_path.write_text(_build_output(rows), encoding="utf-8")
    print(output_path)


if __name__ == "__main__":
    main()
