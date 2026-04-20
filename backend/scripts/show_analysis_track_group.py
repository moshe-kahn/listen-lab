from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime

from backend.app.config import BACKEND_DIR, get_settings
from backend.app.db import _parse_grouping_note, ensure_sqlite_db, sqlite_connection


def _resolve_analysis_track_ids(*, analysis_track_id: int | None, title_query: str | None) -> list[int]:
    with sqlite_connection() as connection:
        if analysis_track_id is not None:
            row = connection.execute(
                "SELECT id FROM analysis_track WHERE id = ? LIMIT 1",
                (analysis_track_id,),
            ).fetchone()
            return [int(row[0])] if row is not None else []

        if title_query is None or not title_query.strip():
            return []

        pattern = f"%{title_query.strip()}%"
        rows = connection.execute(
            """
            SELECT id
            FROM analysis_track
            WHERE primary_name LIKE ?
            ORDER BY id ASC
            LIMIT 20
            """,
            (pattern,),
        ).fetchall()
        return [int(row[0]) for row in rows]


def _group_rows(analysis_track_ids: list[int]) -> list[tuple[object, ...]]:
    if not analysis_track_ids:
        return []

    placeholders = ",".join("?" for _ in analysis_track_ids)
    query = f"""
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
          SELECT at.release_track_id, group_concat(ra.primary_name, ' | ') AS album_names
          FROM album_track at
          JOIN release_album ra ON ra.id = at.release_album_id
          GROUP BY at.release_track_id
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
          coalesce(sr.source_refs, '') AS source_refs,
          coalesce(sr.source_map_methods, '') AS source_map_methods
        FROM analysis_track at
        JOIN analysis_track_map atm ON atm.analysis_track_id = at.id
        JOIN release_track rt ON rt.id = atm.release_track_id
        LEFT JOIN primary_artists pa ON pa.release_track_id = rt.id
        LEFT JOIN release_albums ral ON ral.release_track_id = rt.id
        LEFT JOIN source_refs sr ON sr.release_track_id = rt.id
        WHERE at.id IN ({placeholders})
        ORDER BY at.id ASC, rt.id ASC
    """

    with sqlite_connection() as connection:
        return connection.execute(query, tuple(analysis_track_ids)).fetchall()


def _build_output(rows: list[tuple[object, ...]], *, selector: str) -> str:
    settings = get_settings()
    lines = [
        "Analysis Track Group Report",
        "===========================",
        f"DB path: {settings.sqlite_db_path}",
        f"Generated at: {datetime.now(UTC).isoformat().replace('+00:00', 'Z')}",
        f"Selector: {selector}",
        "",
    ]

    if not rows:
        lines.append("No matching analysis_track rows found.")
        return "\n".join(lines) + "\n"

    current_analysis_track_id: int | None = None
    for row in rows:
        analysis_track_id = int(row[0])
        analysis_name = str(row[1])
        grouping_note = row[2]
        match_method = str(row[3])
        confidence = float(row[4])
        status = str(row[5])
        release_track_id = int(row[6])
        release_track_name = str(row[7])
        normalized_name = str(row[8]) if row[8] is not None else ""
        artist_signature = str(row[9])
        album_names = str(row[10])
        source_refs = str(row[11])
        source_map_methods = str(row[12])
        grouping_hash, grouping_title_key = _parse_grouping_note(grouping_note)

        if current_analysis_track_id != analysis_track_id:
            current_analysis_track_id = analysis_track_id
            lines.append(f"[analysis_track {analysis_track_id}] {analysis_name}")
            lines.append(f"  song-family key: {grouping_title_key or '(unknown)'}")
            lines.append(f"  grouping hash: {grouping_hash or grouping_note}")
            lines.append(f"  analysis match method: {match_method}")
            lines.append(f"  confidence: {confidence:.2f}")
            lines.append(f"  status: {status}")
            lines.append("")

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

    parser = argparse.ArgumentParser(description="Write a readable analysis_track group report.")
    parser.add_argument("--id", type=int, dest="analysis_track_id", help="Exact analysis_track id to inspect.")
    parser.add_argument("--title", type=str, help="Partial analysis track title to search.")
    args = parser.parse_args()

    if args.analysis_track_id is None and not args.title:
        parser.error("Provide either --id or --title.")

    selector = f"id={args.analysis_track_id}" if args.analysis_track_id is not None else f"title={args.title}"
    analysis_track_ids = _resolve_analysis_track_ids(
        analysis_track_id=args.analysis_track_id,
        title_query=args.title,
    )
    rows = _group_rows(analysis_track_ids)

    logs_dir = BACKEND_DIR / "data" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_path = logs_dir / f"analysis_track_group_{timestamp}.txt"
    output_path.write_text(_build_output(rows, selector=selector), encoding="utf-8")
    print(output_path)


if __name__ == "__main__":
    main()
