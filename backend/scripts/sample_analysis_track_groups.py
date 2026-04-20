from __future__ import annotations

import argparse
import random
import re
import sys
from datetime import UTC, datetime

from backend.app.config import BACKEND_DIR, get_settings
from backend.app.db import _parse_grouping_note, ensure_sqlite_db, sqlite_connection


RISKY_VARIANT_PATTERN = re.compile(
    r"(\blive\b|\bacoustic\b|\bdemo\b|\bremaster(?:ed)?\b|\bversion\b|\bedit\b|\bexplicit\b|\bclean\b)",
    re.IGNORECASE,
)


def _candidate_groups() -> list[dict[str, object]]:
    with sqlite_connection() as connection:
        rows = connection.execute(
            """
            SELECT
              at.id,
              at.primary_name,
              at.grouping_note,
              count(atm.release_track_id) AS release_track_count,
              min(atm.match_method) AS match_method,
              min(atm.confidence) AS confidence
            FROM analysis_track at
            JOIN analysis_track_map atm
              ON atm.analysis_track_id = at.id
            WHERE atm.status = 'suggested'
            GROUP BY at.id, at.primary_name, at.grouping_note
            ORDER BY at.id ASC
            """
        ).fetchall()

    groups: list[dict[str, object]] = []
    for row in rows:
        grouping_hash, song_family_key = _parse_grouping_note(row[2])
        groups.append(
            {
                "analysis_track_id": int(row[0]),
                "analysis_name": str(row[1]),
                "grouping_note": str(row[2]),
                "grouping_hash": grouping_hash,
                "song_family_key": song_family_key,
                "release_track_count": int(row[3]),
                "match_method": str(row[4]),
                "confidence": float(row[5]),
            }
        )
    return groups


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


def _risk_score(group_rows: list[tuple[object, ...]]) -> tuple[int, int]:
    risky_titles = 0
    distinct_album_sets: set[str] = set()
    for row in group_rows:
        release_track_name = str(row[7])
        album_names = str(row[10])
        if RISKY_VARIANT_PATTERN.search(release_track_name):
            risky_titles += 1
        distinct_album_sets.add(album_names)
    return risky_titles, len(distinct_album_sets)


def _build_output(
    rows: list[tuple[object, ...]],
    *,
    selector: str,
    total_groups: int,
) -> str:
    settings = get_settings()
    lines = [
        "Sample Analysis Track Group Report",
        "==================================",
        f"DB path: {settings.sqlite_db_path}",
        f"Generated at: {datetime.now(UTC).isoformat().replace('+00:00', 'Z')}",
        f"Selector: {selector}",
        f"Included groups: {len({int(row[0]) for row in rows})}",
        f"Total suggested analysis groups available: {total_groups}",
        "",
    ]

    if not rows:
        lines.append("No matching analysis_track rows found.")
        return "\n".join(lines) + "\n"

    grouped: dict[int, list[tuple[object, ...]]] = {}
    for row in rows:
        grouped.setdefault(int(row[0]), []).append(row)

    for analysis_track_id, group_rows in grouped.items():
        first_row = group_rows[0]
        analysis_name = str(first_row[1])
        grouping_note = first_row[2]
        match_method = str(first_row[3])
        confidence = float(first_row[4])
        status = str(first_row[5])
        grouping_hash, song_family_key = _parse_grouping_note(grouping_note)
        risky_titles, distinct_album_sets = _risk_score(group_rows)

        lines.append(f"[analysis_track {analysis_track_id}] {analysis_name}")
        lines.append(f"  song-family key: {song_family_key or '(unknown)'}")
        lines.append(f"  grouping hash: {grouping_hash or grouping_note}")
        lines.append(f"  analysis match method: {match_method}")
        lines.append(f"  confidence: {confidence:.2f}")
        lines.append(f"  status: {status}")
        lines.append(f"  linked release_tracks: {len(group_rows)}")
        lines.append(f"  risky variant-title count: {risky_titles}")
        lines.append(f"  distinct album-name sets: {distinct_album_sets}")
        lines.append("")

        for row in group_rows:
            release_track_id = int(row[6])
            release_track_name = str(row[7])
            normalized_name = str(row[8]) if row[8] is not None else ""
            artist_signature = str(row[9])
            album_names = str(row[10])
            source_refs = str(row[11])
            source_map_methods = str(row[12])

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

    parser = argparse.ArgumentParser(description="Write random or higher-risk samples of analysis_track groups.")
    parser.add_argument("--count", type=int, default=10, help="Number of analysis groups to include.")
    parser.add_argument(
        "--mode",
        choices=("random", "risky"),
        default="random",
        help="random = random sample, risky = lowest-confidence groups first",
    )
    parser.add_argument("--seed", type=int, default=7, help="Random seed for repeatable random samples.")
    args = parser.parse_args()

    groups = _candidate_groups()
    count = max(1, int(args.count))

    if args.mode == "random":
        rng = random.Random(args.seed)
        selected = groups[:]
        rng.shuffle(selected)
        selected = selected[:count]
        selector = f"mode=random count={count} seed={args.seed}"
    else:
        group_rows = _group_rows([int(group["analysis_track_id"]) for group in groups])
        grouped_rows: dict[int, list[tuple[object, ...]]] = {}
        for row in group_rows:
            grouped_rows.setdefault(int(row[0]), []).append(row)

        ranked = sorted(
            groups,
            key=lambda group: (
                -float(group["confidence"]),
                _risk_score(grouped_rows.get(int(group["analysis_track_id"]), []))[0],
                _risk_score(grouped_rows.get(int(group["analysis_track_id"]), []))[1],
                -int(group["release_track_count"]),
                -int(group["analysis_track_id"]),
            ),
        )
        selected = ranked[:count]
        selector = f"mode=risky count={count} ordered_by=confidence_asc"

    selected_ids = [int(group["analysis_track_id"]) for group in selected]
    rows = _group_rows(selected_ids)

    logs_dir = BACKEND_DIR / "data" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_path = logs_dir / f"analysis_track_group_sample_{args.mode}_{timestamp}.txt"
    output_path.write_text(
        _build_output(rows, selector=selector, total_groups=len(groups)),
        encoding="utf-8",
    )
    print(output_path)


if __name__ == "__main__":
    main()
