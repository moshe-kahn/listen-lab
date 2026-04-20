from __future__ import annotations

import json
import sys
from datetime import UTC, datetime

from backend.app.config import BACKEND_DIR, get_settings
from backend.app.db import (
    _parse_grouping_note,
    apply_pending_migrations,
    ensure_sqlite_db,
    refresh_conservative_analysis_track_links,
    sqlite_connection,
)
from backend.app.track_variant_policy import interpret_track_variant_title, load_track_variant_policy


PRIMARY_ARTISTS_CTE = """
WITH ordered_primary_artists AS (
  SELECT
    ta.release_track_id,
    ta.billing_index,
    ta.id,
    coalesce(a.canonical_name, ta.credited_as, ta.source_basis, '') AS artist_name
  FROM track_artist ta
  LEFT JOIN artist a
    ON a.id = ta.artist_id
  WHERE ta.role = 'primary'
),
primary_artists AS (
  SELECT
    ordered.release_track_id,
    group_concat(ordered.artist_name, ' | ') AS artist_names
  FROM (
    SELECT
      release_track_id,
      artist_name
    FROM ordered_primary_artists
    ORDER BY
      release_track_id ASC,
      coalesce(billing_index, 999999) ASC,
      id ASC,
      artist_name ASC
  ) ordered
  GROUP BY ordered.release_track_id
)
"""


def _component_summary(release_track_name: str) -> tuple[str, str]:
    interpretation = interpret_track_variant_title(release_track_name)
    components = ", ".join(
        f"{component.normalized_label}:{component.family}:{component.semantic_category}:{component.groupable_by_default}"
        for component in interpretation.components
    ) or "(none)"
    base_summary = (
        f"base='{interpretation.base_title_anchor}', dominant='{interpretation.dominant_family}', "
        f"components={components}"
    )
    return base_summary, components


def _snapshot_suggested_analysis_maps() -> dict[int, dict[str, object]]:
    with sqlite_connection() as connection:
        rows = connection.execute(
            PRIMARY_ARTISTS_CTE
            + """
            SELECT
              atm.release_track_id,
              at.id AS analysis_track_id,
              at.primary_name AS analysis_name,
              at.grouping_note,
              atm.match_method,
              atm.confidence,
              atm.status,
              rt.primary_name AS release_track_name,
              coalesce(pa.artist_names, '') AS primary_artists
            FROM analysis_track_map atm
            JOIN analysis_track at ON at.id = atm.analysis_track_id
            JOIN release_track rt ON rt.id = atm.release_track_id
            LEFT JOIN primary_artists pa ON pa.release_track_id = rt.id
            WHERE atm.status = 'suggested'
            ORDER BY atm.release_track_id ASC, at.id ASC
            """
        ).fetchall()

    snapshot: dict[int, dict[str, object]] = {}
    for row in rows:
        grouping_hash, song_family_key = _parse_grouping_note(row[3])
        snapshot[int(row[0])] = {
            "release_track_id": int(row[0]),
            "analysis_track_id": int(row[1]),
            "analysis_name": str(row[2]),
            "grouping_hash": grouping_hash,
            "song_family_key": song_family_key,
            "match_method": str(row[4]),
            "confidence": float(row[5]),
            "status": str(row[6]),
            "release_track_name": str(row[7]),
            "primary_artists": str(row[8]),
        }
    return snapshot


def _review_rows() -> list[dict[str, object]]:
    with sqlite_connection() as connection:
        rows = connection.execute(
            PRIMARY_ARTISTS_CTE
            + """
            SELECT
              rt.id AS release_track_id,
              rt.primary_name AS release_track_name,
              coalesce(pa.artist_names, '') AS primary_artists,
              at.primary_name AS analysis_name,
              at.grouping_note,
              atm.confidence,
              atm.status
            FROM release_track rt
            LEFT JOIN primary_artists pa
              ON pa.release_track_id = rt.id
            LEFT JOIN analysis_track_map atm
              ON atm.release_track_id = rt.id
             AND atm.status = 'suggested'
            LEFT JOIN analysis_track at
              ON at.id = atm.analysis_track_id
            ORDER BY rt.id ASC
            """
        ).fetchall()

    review_rows: list[dict[str, object]] = []
    for row in rows:
        grouping_hash = None
        song_family_key = None
        if row[4] is not None:
            grouping_hash, song_family_key = _parse_grouping_note(str(row[4]))
        review_rows.append(
            {
                "release_track_id": int(row[0]),
                "release_track_name": str(row[1]),
                "primary_artists": str(row[2]),
                "analysis_name": None if row[3] is None else str(row[3]),
                "grouping_hash": grouping_hash,
                "song_family_key": song_family_key,
                "confidence": None if row[5] is None else float(row[5]),
                "status": None if row[6] is None else str(row[6]),
            }
        )
    return review_rows


def _reason_hint(
    release_track_name: str,
    before_row: dict[str, object] | None,
    after_row: dict[str, object] | None,
) -> str:
    base_summary, _ = _component_summary(release_track_name)
    if before_row is not None and after_row is not None:
        return f"regrouped from '{before_row['analysis_name']}' to '{after_row['analysis_name']}'; {base_summary}"
    if before_row is None and after_row is not None:
        return f"newly grouped under current policy; {base_summary}"
    return f"no longer grouped after refresh; {base_summary}"


def _build_diff_log(
    refresh_result: dict[str, int],
    before: dict[int, dict[str, object]],
    after: dict[int, dict[str, object]],
) -> str:
    settings = get_settings()
    removed_ids = sorted(set(before) - set(after))
    added_ids = sorted(set(after) - set(before))
    shared_ids = sorted(set(before) & set(after))
    regrouped_ids = [
        release_track_id
        for release_track_id in shared_ids
        if before[release_track_id]["song_family_key"] != after[release_track_id]["song_family_key"]
        or before[release_track_id]["analysis_name"] != after[release_track_id]["analysis_name"]
    ]

    lines = [
        "Analysis Track Refresh Diff",
        "===========================",
        f"DB path: {settings.sqlite_db_path}",
        f"Generated at: {datetime.now(UTC).isoformat().replace('+00:00', 'Z')}",
        "",
        "Refresh Result",
        "--------------",
        json.dumps(refresh_result, indent=2, sort_keys=True),
        "",
        "Diff Summary",
        "------------",
        f"removed_mappings: {len(removed_ids)}",
        f"added_mappings: {len(added_ids)}",
        f"regrouped_mappings: {len(regrouped_ids)}",
        "",
        "Removed",
        "-------",
    ]

    if not removed_ids:
        lines.append("(none)")
    for release_track_id in removed_ids:
        before_row = before[release_track_id]
        lines.append(
            f"[release_track {release_track_id}] {before_row['release_track_name']} | artist={before_row['primary_artists']} | "
            f"old_analysis={before_row['analysis_name']} | old_song_family_key={before_row['song_family_key']}"
        )
        lines.append(f"  why: {_reason_hint(str(before_row['release_track_name']), before_row, None)}")

    lines.extend(["", "Added", "-----"])
    if not added_ids:
        lines.append("(none)")
    for release_track_id in added_ids:
        after_row = after[release_track_id]
        lines.append(
            f"[release_track {release_track_id}] {after_row['release_track_name']} | artist={after_row['primary_artists']} | "
            f"new_analysis={after_row['analysis_name']} | new_song_family_key={after_row['song_family_key']}"
        )
        lines.append(f"  why: {_reason_hint(str(after_row['release_track_name']), None, after_row)}")

    lines.extend(["", "Regrouped", "---------"])
    if not regrouped_ids:
        lines.append("(none)")
    for release_track_id in regrouped_ids:
        before_row = before[release_track_id]
        after_row = after[release_track_id]
        lines.append(
            f"[release_track {release_track_id}] {after_row['release_track_name']} | artist={after_row['primary_artists']} | "
            f"old_analysis={before_row['analysis_name']} -> new_analysis={after_row['analysis_name']}"
        )
        lines.append(
            f"  old_song_family_key={before_row['song_family_key']} -> new_song_family_key={after_row['song_family_key']}"
        )
        lines.append(f"  why: {_reason_hint(str(after_row['release_track_name']), before_row, after_row)}")

    return "\n".join(lines) + "\n"


def _build_ambiguous_review_log(review_rows: list[dict[str, object]]) -> str:
    policy = load_track_variant_policy()
    grouped_entries: list[dict[str, object]] = []
    ungrouped_entries: list[dict[str, object]] = []
    family_counts: dict[str, int] = {}

    for row in review_rows:
        interpretation = interpret_track_variant_title(str(row["release_track_name"]))
        if not interpretation.components:
            continue
        review_components = [
            component
            for component in interpretation.components
            if (policy.get_family(component.family) is not None and policy.get_family(component.family).needs_review)
        ]
        if not review_components:
            continue

        for component in review_components:
            family_counts[component.family] = family_counts.get(component.family, 0) + 1

        component_summary = ", ".join(
            f"{component.normalized_label}:{component.family}:{component.semantic_category}:{component.groupable_by_default}"
            for component in interpretation.components
        )
        entry = {
            "release_track_id": int(row["release_track_id"]),
            "release_track_name": str(row["release_track_name"]),
            "primary_artists": str(row["primary_artists"]),
            "analysis_name": row["analysis_name"],
            "song_family_key": row["song_family_key"],
            "confidence": row["confidence"],
            "dominant_family": interpretation.dominant_family,
            "base_title_anchor": interpretation.base_title_anchor,
            "component_summary": component_summary,
            "review_families": ", ".join(sorted({component.family for component in review_components})),
        }
        if row["analysis_name"] is None:
            ungrouped_entries.append(entry)
        else:
            grouped_entries.append(entry)

    grouped_entries.sort(key=lambda item: (str(item["review_families"]), str(item["primary_artists"]), str(item["release_track_name"])))
    ungrouped_entries.sort(key=lambda item: (str(item["review_families"]), str(item["primary_artists"]), str(item["release_track_name"])))

    lines = [
        "Ambiguous Analysis Review Queue",
        "===============================",
        f"DB path: {get_settings().sqlite_db_path}",
        f"Generated at: {datetime.now(UTC).isoformat().replace('+00:00', 'Z')}",
        "",
        "Summary",
        "-------",
        f"grouped_review_entries: {len(grouped_entries)}",
        f"ungrouped_review_entries: {len(ungrouped_entries)}",
        "",
        "Review Family Counts",
        "--------------------",
    ]
    if not family_counts:
        lines.append("(none)")
    else:
        for family, count in sorted(family_counts.items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"{family}: {count}")

    lines.extend(["", "Grouped Review Entries", "----------------------"])
    if not grouped_entries:
        lines.append("(none)")
    for entry in grouped_entries:
        lines.append(
            f"[release_track {entry['release_track_id']}] {entry['release_track_name']} | artist={entry['primary_artists']} | "
            f"analysis={entry['analysis_name']} | song_family_key={entry['song_family_key']} | confidence={entry['confidence']:.2f}"
        )
        lines.append(
            f"  review_families={entry['review_families']} | base='{entry['base_title_anchor']}' | dominant='{entry['dominant_family']}'"
        )
        lines.append(f"  components={entry['component_summary']}")

    lines.extend(["", "Ungrouped Review Entries", "------------------------"])
    if not ungrouped_entries:
        lines.append("(none)")
    for entry in ungrouped_entries:
        lines.append(
            f"[release_track {entry['release_track_id']}] {entry['release_track_name']} | artist={entry['primary_artists']} | analysis=(none)"
        )
        lines.append(
            f"  review_families={entry['review_families']} | base='{entry['base_title_anchor']}' | dominant='{entry['dominant_family']}'"
        )
        lines.append(f"  components={entry['component_summary']}")

    return "\n".join(lines) + "\n"


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    ensure_sqlite_db()
    apply_pending_migrations()
    before = _snapshot_suggested_analysis_maps()
    refresh_result = refresh_conservative_analysis_track_links()
    after = _snapshot_suggested_analysis_maps()
    review_rows = _review_rows()

    logs_dir = BACKEND_DIR / "data" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    diff_output_path = logs_dir / f"analysis_track_refresh_diff_{timestamp}.txt"
    diff_output_path.write_text(_build_diff_log(refresh_result, before, after), encoding="utf-8")

    review_output_path = logs_dir / f"analysis_ambiguous_review_{timestamp}.txt"
    review_output_path.write_text(_build_ambiguous_review_log(review_rows), encoding="utf-8")

    print(json.dumps(refresh_result, indent=2, sort_keys=True))
    print(diff_output_path)
    print(review_output_path)


if __name__ == "__main__":
    main()
