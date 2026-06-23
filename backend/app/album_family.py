from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

from backend.app.db import sqlite_connection
from backend.app.recording_track_candidates import (
    _base_title,
    recording_release_album_ids_for_release_track_ids,
    recording_representatives_for_release_track_ids,
)
from backend.app.release_track_metadata import enrich_album_track_rows_with_release_metadata


_EDITION_SUFFIX = re.compile(
    r"\s*[\[(]\s*(expanded\s+deluxe(?:\s+edition)?|deluxe(?:\s+edition)?|expanded(?:\s+edition)?|"
    r"(?:\d+(?:st|nd|rd|th)\s+)?anniversary\s+remaster(?:ed)?(?:\s+edition)?|"
    r"anniversary(?:\s+edition)?|remaster(?:ed)?(?:\s+edition)?|mono|stereo|rework|"
    r"(?:bonus\s+)?(?:disc|disk)\s+\d+)\s*[\])]\s*$",
    re.IGNORECASE,
)
_COMPANION_DISC = re.compile(r"\b(?:bonus\s+)?(?:disc|disk)\s+\d+\b", re.IGNORECASE)
_FORBIDDEN_ALBUM_CONTEXT = re.compile(r"\b(single|soundtrack|score|compilation|greatest hits|best of)\b", re.IGNORECASE)
_COMBINED_EDITION_GROUPS = (
    {
        "combined_spotify_album_id": "6ofEQubaL265rIW6WnCU8y",
        "versions": (
            ("6GjwtEZcfenmOf6l18N7T7", "Kid A"),
            ("1HrMmB5useeZ0F5lHrMvl0", "Amnesiac"),
            ("6ofEQubaL265rIW6WnCU8y", "Extended Edition"),
        ),
        "disc_labels": {1: "Kid A", 2: "Amnesiac", 3: "Extra Content"},
        "version_disc_numbers": {
            "6GjwtEZcfenmOf6l18N7T7": 1,
            "1HrMmB5useeZ0F5lHrMvl0": 2,
        },
    },
)


def _core_album_name(name: str) -> str:
    stripped = _EDITION_SUFFIX.sub("", str(name or "")).strip()
    return stripped or str(name or "").strip()


def _normalize(value: str | None) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).split())


def _edition_label(name: str, core_name: str) -> str:
    normalized = _normalize(name)
    companion_match = _COMPANION_DISC.search(str(name or ""))
    if companion_match:
        return companion_match.group(0).title()
    if "expanded deluxe" in normalized:
        return "Expanded Deluxe Edition"
    if "deluxe" in normalized:
        return "Deluxe Edition"
    if "remaster" in normalized:
        return "Remaster"
    if "anniversary" in normalized:
        return "Anniversary Edition"
    if re.search(r"\bmono\b", normalized):
        return "Mono"
    if re.search(r"\bstereo\b", normalized):
        return "Stereo"
    if "rework" in normalized:
        return "Rework"
    normalized_core = _normalize(core_name)
    if normalized_core and normalized != normalized_core and normalized.startswith(f"{normalized_core} "):
        return "Expanded Edition"
    return "Original" if _normalize(name) == _normalize(core_name) else name


def _edition_menu_label(name: str, label: str) -> str:
    match = re.search(r"[\[(]\s*([^\])]+)\s*[\])]\s*$", str(name or ""))
    if match:
        return match.group(1).strip()
    if label == "Expanded Edition" and str(name or "").strip():
        return str(name).strip()
    return label


def _combined_edition_group(spotify_album_id: str) -> dict[str, Any] | None:
    for group in _COMBINED_EDITION_GROUPS:
        if spotify_album_id in {version_id for version_id, _label in group["versions"]}:
            return group
    return None


def _album_rows(connection: sqlite3.Connection, release_album_ids: list[int]) -> list[sqlite3.Row]:
    placeholders = ",".join("?" for _ in release_album_ids)
    return connection.execute(
        f"""
        WITH primary_artists AS (
          SELECT
            distinct_artists.release_album_id,
            group_concat(distinct_artists.artist_name, '|') AS artist_signature
          FROM (
            SELECT DISTINCT
              aa.release_album_id,
              lower(trim(a.canonical_name)) AS artist_name
            FROM album_artist aa
            JOIN artist a ON a.id = aa.artist_id
            WHERE aa.role = 'primary'
              AND aa.release_album_id IN ({placeholders})
            ORDER BY aa.release_album_id, artist_name
          ) distinct_artists
          GROUP BY distinct_artists.release_album_id
        ),
        track_counts AS (
          SELECT release_album_id, count(DISTINCT release_track_id) AS track_count
          FROM album_track
          WHERE release_album_id IN ({placeholders})
          GROUP BY release_album_id
        ),
        catalog_track_counts AS (
          SELECT
            sam.release_album_id,
            max(sac.total_tracks) AS catalog_total_tracks
          FROM source_album_map sam
          JOIN source_album sa ON sa.id = sam.source_album_id AND sa.source_name = 'spotify'
          JOIN spotify_album_catalog sac ON sac.spotify_album_id = sa.external_id
          WHERE sam.status = 'accepted'
            AND sam.release_album_id IN ({placeholders})
          GROUP BY sam.release_album_id
        )
        SELECT
          ra.id,
          ra.primary_name,
          ra.release_year,
          COALESCE(pa.artist_signature, '') AS artist_signature,
          COALESCE(tc.track_count, 0) AS track_count,
          ctc.catalog_total_tracks
        FROM release_album ra
        LEFT JOIN primary_artists pa ON pa.release_album_id = ra.id
        LEFT JOIN track_counts tc ON tc.release_album_id = ra.id
        LEFT JOIN catalog_track_counts ctc ON ctc.release_album_id = ra.id
        WHERE ra.id IN ({placeholders})
        ORDER BY ra.id
        """,
        (*release_album_ids, *release_album_ids, *release_album_ids, *release_album_ids),
    ).fetchall()


def _album_track_base_titles(connection: sqlite3.Connection, release_album_id: int) -> set[str]:
    rows = connection.execute(
        """
        SELECT rt.primary_name
        FROM album_track at
        JOIN release_track rt ON rt.id = at.release_track_id
        WHERE at.release_album_id = ?
        UNION
        SELECT sat.name AS primary_name
        FROM source_album_map sam
        JOIN source_album sa
          ON sa.id = sam.source_album_id
         AND sa.source_name = 'spotify'
        JOIN spotify_album_track sat
          ON sat.spotify_album_id = sa.external_id
         AND lower(COALESCE(sat.last_status, '')) != 'error'
        WHERE sam.release_album_id = ?
          AND sam.status = 'accepted'
        """,
        (release_album_id, release_album_id),
    ).fetchall()
    return {_base_title(str(row["primary_name"] or "")) for row in rows if str(row["primary_name"] or "").strip()}


def _equivalent_reviewed_family_id(
    connection: sqlite3.Connection,
    *,
    release_album_id: int,
    album_name: str,
) -> int | None:
    """Resolve duplicate album identity to one unambiguous reviewed family for display only."""
    selected_artist_names = {
        _normalize(row["canonical_name"])
        for row in connection.execute(
            """
            SELECT a.canonical_name
            FROM album_artist aa
            JOIN artist a ON a.id = aa.artist_id
            WHERE aa.release_album_id = ? AND aa.role = 'primary'
            """,
            (release_album_id,),
        ).fetchall()
        if _normalize(row["canonical_name"])
    }
    if not selected_artist_names:
        return None
    rows = connection.execute(
        """
        SELECT afm.album_family_id, ra.id AS release_album_id, ra.primary_name, a.canonical_name
        FROM album_family_map afm
        JOIN release_album ra ON ra.id = afm.release_album_id
        JOIN album_artist aa ON aa.release_album_id = ra.id AND aa.role = 'primary'
        JOIN artist a ON a.id = aa.artist_id
        WHERE afm.status = 'accepted' AND ra.id != ?
        """,
        (release_album_id,),
    ).fetchall()
    matching_family_ids = {
        int(row["album_family_id"])
        for row in rows
        if _normalize(row["primary_name"]) == _normalize(album_name)
        and _normalize(row["canonical_name"]) in selected_artist_names
    }
    return next(iter(matching_family_ids)) if len(matching_family_ids) == 1 else None


def preview_album_family_grouping(
    *,
    release_album_ids: list[int],
    canonical_release_album_id: int,
) -> dict[str, Any]:
    normalized_ids = sorted({int(value) for value in release_album_ids if int(value) > 0})
    reasons: list[str] = []
    blockers: list[str] = []
    if len(normalized_ids) < 2:
        blockers.append("at_least_two_release_albums_required")
    if canonical_release_album_id not in normalized_ids:
        blockers.append("canonical_album_must_be_in_group")
    albums: list[dict[str, Any]] = []
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = _album_rows(connection, normalized_ids) if normalized_ids else []
        if len(rows) != len(normalized_ids):
            blockers.append("one_or_more_release_albums_missing")
        for row in rows:
            albums.append(dict(row))
        canonical_row = next((row for row in rows if int(row["id"]) == canonical_release_album_id), None)
        canonical_core_name = _core_album_name(str(canonical_row["primary_name"] or "")) if canonical_row else ""
        canonical_core_normalized = _normalize(canonical_core_name)
        title_relations: dict[int, str] = {}
        for row in rows:
            release_album_id = int(row["id"])
            album_name = str(row["primary_name"] or "")
            normalized_name = _normalize(album_name)
            normalized_core = _normalize(_core_album_name(album_name))
            if normalized_name == canonical_core_normalized or normalized_core == canonical_core_normalized:
                title_relations[release_album_id] = "same_core_title"
            elif canonical_core_normalized and normalized_name.startswith(f"{canonical_core_normalized} "):
                title_relations[release_album_id] = "core_title_prefix_extension"
            else:
                blockers.append(f"core_album_title_mismatch:{release_album_id}")
        if len(title_relations) == len(rows):
            reasons.append("core_album_title_matches_or_prefixes")
        artist_signatures = {str(row["artist_signature"] or "") for row in rows if str(row["artist_signature"] or "")}
        if len(artist_signatures) != 1:
            blockers.append("primary_artist_signatures_do_not_match")
        else:
            reasons.append("primary_artist_matches")
        if any(_FORBIDDEN_ALBUM_CONTEXT.search(str(row["primary_name"] or "")) for row in rows):
            blockers.append("single_compilation_or_soundtrack_not_allowed")
        canonical_titles = _album_track_base_titles(connection, canonical_release_album_id) if canonical_release_album_id > 0 else set()
        overlap: dict[int, float] = {}
        for row in rows:
            release_album_id = int(row["id"])
            titles = _album_track_base_titles(connection, release_album_id)
            ratio = len(canonical_titles & titles) / max(1, len(canonical_titles))
            overlap[release_album_id] = round(ratio, 4)
            is_companion = bool(_COMPANION_DISC.search(str(row["primary_name"] or "")))
            complete_same_title_catalog_shell = bool(
                title_relations.get(release_album_id) == "same_core_title"
                and row["catalog_total_tracks"] is not None
                and int(row["catalog_total_tracks"]) >= len(canonical_titles)
            )
            if (
                release_album_id != canonical_release_album_id
                and not is_companion
                and not complete_same_title_catalog_shell
                and ratio < 1.0
            ):
                blockers.append(f"incomplete_core_track_coverage:{release_album_id}")
            if release_album_id != canonical_release_album_id and is_companion:
                reasons.append(f"explicit_companion_disc:{release_album_id}")
            if release_album_id != canonical_release_album_id and complete_same_title_catalog_shell:
                reasons.append(f"same_title_catalog_track_count_matches:{release_album_id}")
        if canonical_titles and all(
            value >= 1.0
            or bool(_COMPANION_DISC.search(str(next(row for row in rows if int(row['id']) == album_id)["primary_name"] or "")))
            or bool(
                title_relations.get(album_id) == "same_core_title"
                and next(row for row in rows if int(row["id"]) == album_id)["catalog_total_tracks"] is not None
                and int(next(row for row in rows if int(row["id"]) == album_id)["catalog_total_tracks"]) >= len(canonical_titles)
            )
            for album_id, value in overlap.items()
            if album_id != canonical_release_album_id
        ):
            reasons.append("complete_core_coverage_or_companion_disc")
    return {
        "dry_run": True,
        "safe": not blockers,
        "release_album_ids": normalized_ids,
        "canonical_release_album_id": canonical_release_album_id,
        "albums": albums,
        "track_overlap": overlap if 'overlap' in locals() else {},
        "title_relations": title_relations if 'title_relations' in locals() else {},
        "reasons": reasons,
        "blockers": sorted(set(blockers)),
    }


def apply_reviewed_album_family_grouping(
    *,
    release_album_ids: list[int],
    canonical_release_album_id: int,
    rationale: str,
    apply: bool = False,
) -> dict[str, Any]:
    preview = preview_album_family_grouping(
        release_album_ids=release_album_ids,
        canonical_release_album_id=canonical_release_album_id,
    )
    if not apply or not preview["safe"]:
        return {**preview, "applied": False}
    rationale_text = str(rationale or "").strip()
    if not rationale_text:
        return {**preview, "applied": False, "safe": False, "blockers": ["review_rationale_required"]}
    album_by_id = {int(album["id"]): album for album in preview["albums"]}
    canonical = album_by_id[canonical_release_album_id]
    core_name = _core_album_name(str(canonical["primary_name"] or ""))
    with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
        family_row = connection.execute(
            "SELECT album_family_id FROM album_family_map WHERE release_album_id = ?",
            (canonical_release_album_id,),
        ).fetchone()
        if family_row is None:
            album_family_id = int(connection.execute(
                """
                INSERT INTO album_family (
                  primary_name, normalized_name, release_year, canonical_release_album_id
                ) VALUES (?, ?, ?, ?)
                """,
                (core_name, _normalize(core_name), canonical["release_year"], canonical_release_album_id),
            ).lastrowid)
        else:
            album_family_id = int(family_row["album_family_id"])
        connection.execute(
            """
            UPDATE album_family
            SET primary_name = ?, normalized_name = ?, release_year = ?,
                canonical_release_album_id = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE id = ?
            """,
            (core_name, _normalize(core_name), canonical["release_year"], canonical_release_album_id, album_family_id),
        )
        for release_album_id in preview["release_album_ids"]:
            connection.execute(
                """
                INSERT INTO album_family_map (
                  release_album_id, album_family_id, match_method, confidence, status,
                  is_user_confirmed, explanation
                ) VALUES (?, ?, 'reviewed_album_family', 1.0, 'accepted', 1, ?)
                ON CONFLICT(release_album_id) DO UPDATE SET
                  album_family_id = excluded.album_family_id,
                  match_method = excluded.match_method,
                  confidence = excluded.confidence,
                  status = excluded.status,
                  is_user_confirmed = excluded.is_user_confirmed,
                  explanation = excluded.explanation,
                  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                """,
                (release_album_id, album_family_id, rationale_text),
            )
        connection.execute(
            """
            INSERT INTO album_family_review (
              album_family_id, canonical_release_album_id, release_album_ids_json,
              decision, rationale
            ) VALUES (?, ?, ?, 'accept', ?)
            """,
            (album_family_id, canonical_release_album_id, json.dumps(preview["release_album_ids"]), rationale_text),
        )
    return {**preview, "dry_run": False, "applied": True, "album_family_id": album_family_id, "core_name": core_name}


def _cached_album_items(connection: sqlite3.Connection, spotify_album_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT spotify_track_id, disc_number, track_number, name, duration_ms, artists_json
        FROM spotify_album_track
        WHERE spotify_album_id = ? AND lower(COALESCE(last_status, '')) != 'error'
        ORDER BY COALESCE(disc_number, 0), COALESCE(track_number, 0), name, spotify_track_id
        """,
        (spotify_album_id,),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        try:
            artists = json.loads(str(row["artists_json"] or "[]"))
        except json.JSONDecodeError:
            artists = []
        items.append({
            "id": str(row["spotify_track_id"] or "") or None,
            "name": row["name"],
            "uri": f"spotify:track:{row['spotify_track_id']}" if row["spotify_track_id"] else None,
            "duration_ms": row["duration_ms"],
            "artists": artists if isinstance(artists, list) else [],
            "disc_number": row["disc_number"],
            "track_number": row["track_number"],
        })
    return items


def build_album_family_context(
    *,
    selected_spotify_album_id: str,
    selected_items: list[dict[str, Any]],
) -> dict[str, Any] | None:
    combined_edition_group = _combined_edition_group(selected_spotify_album_id)
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        selected = connection.execute(
            """
            SELECT sam.release_album_id, afm.album_family_id, ra.primary_name, ra.release_year
            FROM source_album sa
            JOIN source_album_map sam ON sam.source_album_id = sa.id AND sam.status = 'accepted'
            JOIN release_album ra ON ra.id = sam.release_album_id
            LEFT JOIN album_family_map afm ON afm.release_album_id = sam.release_album_id AND afm.status = 'accepted'
            WHERE sa.source_name = 'spotify' AND sa.external_id = ?
            LIMIT 1
            """,
            (selected_spotify_album_id,),
        ).fetchone()
        if selected is None:
            return None
        selected_name = str(selected["primary_name"] or "")
        selected_core_name = _core_album_name(selected_name)
        selected_label = _edition_label(selected_name, selected_core_name)
        selected_release_album_id = int(selected["release_album_id"])
        selected_family_id = int(selected["album_family_id"]) if selected["album_family_id"] is not None else None
        inferred_equivalent_family = False
        if combined_edition_group is not None:
            selected_family_id = -1
        elif selected_family_id is None:
            selected_family_id = _equivalent_reviewed_family_id(
                connection,
                release_album_id=selected_release_album_id,
                album_name=selected_name,
            )
            inferred_equivalent_family = selected_family_id is not None
        if selected_family_id is None:
            if selected_label == "Original":
                return None
            catalog = connection.execute(
                """
                SELECT
                  COALESCE(
                    (SELECT total_tracks FROM spotify_album_catalog WHERE spotify_album_id = ?),
                    (SELECT count(*) FROM spotify_album_track WHERE spotify_album_id = ? AND lower(COALESCE(last_status, '')) != 'error')
                  ) AS total_tracks,
                  (SELECT release_date FROM spotify_album_catalog WHERE spotify_album_id = ?) AS release_date,
                  (SELECT sum(COALESCE(duration_ms, 0)) FROM spotify_album_track WHERE spotify_album_id = ? AND lower(COALESCE(last_status, '')) != 'error') AS total_duration_ms,
                  (SELECT json_extract(images_json, '$[0].url') FROM spotify_album_catalog WHERE spotify_album_id = ?) AS image_url
                """,
                (selected_spotify_album_id,) * 5,
            ).fetchone()
            track_remaster_year = next((
                int(match.group(1))
                for item in selected_items
                if (match := re.search(r"\b(\d{4})\s+remaster(?:ed)?\b", str(item.get("name") or ""), re.IGNORECASE))
            ), None)
            version = {
                "release_album_id": int(selected["release_album_id"]),
                "spotify_album_id": selected_spotify_album_id,
                "name": selected_name,
                "label": selected_label,
                "menu_label": _edition_menu_label(selected_name, selected_label),
                "release_year": (
                    int(selected["release_year"])
                    if selected["release_year"] is not None
                    else int(str(catalog["release_date"])[:4])
                    if catalog and re.match(r"^\d{4}", str(catalog["release_date"] or ""))
                    else track_remaster_year
                ),
                "total_tracks": int(catalog["total_tracks"]) if catalog and catalog["total_tracks"] is not None else None,
                "total_duration_ms": int(catalog["total_duration_ms"]) if catalog and catalog["total_duration_ms"] is not None else None,
                "image_url": str(catalog["image_url"]) if catalog and catalog["image_url"] else None,
                "is_selected": True,
                "is_canonical": True,
            }
            items = [dict(item) for item in selected_items]
            for item in items:
                item["family_exclusive"] = False
                item["family_available_versions"] = [version]
                item["family_has_edition_relation"] = False
                item["family_has_external_recording_relation"] = False
            return {
                "album_family_id": int(selected["release_album_id"]),
                "core_name": selected_core_name,
                "selected_spotify_album_id": selected_spotify_album_id,
                "release_album_ids": [int(selected["release_album_id"])],
                "versions": [version],
                "items": items,
            }
        if combined_edition_group is not None:
            configured_album_ids = [version_id for version_id, _label in combined_edition_group["versions"]]
            placeholders = ",".join("?" for _ in configured_album_ids)
            raw_version_rows = connection.execute(
                f"""
                SELECT
                  sam.release_album_id,
                  ra.primary_name,
                  ra.release_year,
                  sa.external_id AS spotify_album_id,
                  sac.total_tracks,
                  durations.total_duration_ms,
                  json_extract(sac.images_json, '$[0].url') AS image_url
                FROM source_album sa
                JOIN source_album_map sam ON sam.source_album_id = sa.id AND sam.status = 'accepted'
                JOIN release_album ra ON ra.id = sam.release_album_id
                LEFT JOIN spotify_album_catalog sac ON sac.spotify_album_id = sa.external_id
                LEFT JOIN (
                  SELECT spotify_album_id, sum(COALESCE(duration_ms, 0)) AS total_duration_ms
                  FROM spotify_album_track
                  WHERE lower(COALESCE(last_status, '')) != 'error'
                  GROUP BY spotify_album_id
                ) durations ON durations.spotify_album_id = sa.external_id
                WHERE sa.source_name = 'spotify' AND sa.external_id IN ({placeholders})
                """,
                tuple(configured_album_ids),
            ).fetchall()
            version_order = {spotify_album_id: index for index, spotify_album_id in enumerate(configured_album_ids)}
            version_rows = sorted(raw_version_rows, key=lambda row: version_order.get(str(row["spotify_album_id"]), 999))
            combined_spotify_album_id = str(combined_edition_group["combined_spotify_album_id"])
            if selected_spotify_album_id != combined_spotify_album_id:
                version_rows = [
                    row for row in version_rows
                    if str(row["spotify_album_id"]) in {selected_spotify_album_id, combined_spotify_album_id}
                ]
            combined_row = next(
                row for row in version_rows
                if str(row["spotify_album_id"]) == combined_spotify_album_id
            )
            family_id = int(combined_row["release_album_id"])
            family: sqlite3.Row | dict[str, Any] | None = {
                "primary_name": str(combined_row["primary_name"] or ""),
                "canonical_release_album_id": int(combined_row["release_album_id"]),
            }
        else:
            family_id = selected_family_id
            family = connection.execute(
                "SELECT primary_name, canonical_release_album_id FROM album_family WHERE id = ?",
                (family_id,),
            ).fetchone()
            version_rows = connection.execute(
                """
                SELECT
                  afm.release_album_id,
                  ra.primary_name,
                  ra.release_year,
                  sa.external_id AS spotify_album_id,
                  sac.total_tracks,
                  durations.total_duration_ms,
                  json_extract(sac.images_json, '$[0].url') AS image_url
                FROM album_family_map afm
                JOIN release_album ra ON ra.id = afm.release_album_id
                JOIN source_album_map sam ON sam.release_album_id = ra.id AND sam.status = 'accepted'
                JOIN source_album sa ON sa.id = sam.source_album_id AND sa.source_name = 'spotify'
                LEFT JOIN spotify_album_catalog sac ON sac.spotify_album_id = sa.external_id
                LEFT JOIN (
                  SELECT spotify_album_id, sum(COALESCE(duration_ms, 0)) AS total_duration_ms
                  FROM spotify_album_track
                  WHERE lower(COALESCE(last_status, '')) != 'error'
                  GROUP BY spotify_album_id
                ) durations ON durations.spotify_album_id = sa.external_id
                WHERE afm.album_family_id = ? AND afm.status = 'accepted'
                ORDER BY COALESCE(sac.total_tracks, 999999), ra.release_year, ra.id
                """,
                (family_id,),
            ).fetchall()
        if not version_rows:
            return None
        core_name = _core_album_name(
            str(family["primary_name"] or "") if family else str(version_rows[0]["primary_name"] or "")
        )
        if len(version_rows) == 1 and _edition_label(str(version_rows[0]["primary_name"] or ""), core_name) == "Original":
            return None
        versions = [
            {
                "release_album_id": int(row["release_album_id"]),
                "spotify_album_id": str(row["spotify_album_id"]),
                "name": str(row["primary_name"] or ""),
                "label": (
                    dict(combined_edition_group["versions"]).get(str(row["spotify_album_id"]))
                    if combined_edition_group is not None
                    else _edition_label(str(row["primary_name"] or ""), core_name)
                ),
                "menu_label": (
                    dict(combined_edition_group["versions"]).get(str(row["spotify_album_id"]))
                    if combined_edition_group is not None
                    else _edition_menu_label(
                        str(row["primary_name"] or ""),
                        _edition_label(str(row["primary_name"] or ""), core_name),
                    )
                ),
                "release_year": int(row["release_year"]) if row["release_year"] is not None else None,
                "total_tracks": int(row["total_tracks"]) if row["total_tracks"] is not None else None,
                "total_duration_ms": int(row["total_duration_ms"]) if row["total_duration_ms"] is not None else None,
                "image_url": str(row["image_url"]) if row["image_url"] else None,
                "is_selected": str(row["spotify_album_id"]) == selected_spotify_album_id,
                "is_canonical": family is not None and int(row["release_album_id"]) == int(family["canonical_release_album_id"]),
            }
            for row in version_rows
        ]
        if inferred_equivalent_family and not any(version["is_selected"] for version in versions):
            equivalent_version = next((
                version
                for version in versions
                if _normalize(version["name"]) == _normalize(selected_name)
            ), None)
            if equivalent_version is not None:
                equivalent_version["release_album_id"] = selected_release_album_id
                equivalent_version["spotify_album_id"] = selected_spotify_album_id
                equivalent_version["name"] = selected_name
                equivalent_version["label"] = selected_label
                equivalent_version["menu_label"] = _edition_menu_label(selected_name, selected_label)
                equivalent_version["is_selected"] = True
        items_by_album = {
            version["spotify_album_id"]: enrich_album_track_rows_with_release_metadata(
                selected_items if version["spotify_album_id"] == selected_spotify_album_id else _cached_album_items(connection, version["spotify_album_id"])
            )
            for version in versions
        }
        if combined_edition_group is not None:
            component_disc_numbers = {
                spotify_album_id: index
                for index, (spotify_album_id, _label) in enumerate(combined_edition_group["versions"], start=1)
                if spotify_album_id != combined_edition_group["combined_spotify_album_id"]
            }
            for spotify_album_id, disc_number in component_disc_numbers.items():
                for item in items_by_album.get(spotify_album_id, []):
                    item["disc_number"] = disc_number
            for spotify_album_id, items in items_by_album.items():
                for item in items:
                    disc_number = item.get("disc_number")
                    track_number = item.get("track_number")
                    if isinstance(disc_number, int) and isinstance(track_number, int):
                        item["_combined_edition_position_key"] = (
                            f"{combined_edition_group['combined_spotify_album_id']}:{disc_number}:{track_number}"
                        )
        for version in versions:
            companion_match = _COMPANION_DISC.search(str(version["label"] or ""))
            if not companion_match:
                continue
            disc_match = re.search(r"\d+", companion_match.group(0))
            if not disc_match:
                continue
            family_disc_number = int(disc_match.group(0))
            for item in items_by_album.get(version["spotify_album_id"], []):
                # Spotify models a separately released companion disc as disc 1.
                # Preserve source metadata in cache; only project its family position here.
                item["disc_number"] = family_disc_number
    release_track_ids = [
        int(item["release_track_id"])
        for items in items_by_album.values()
        for item in items
        if isinstance(item.get("release_track_id"), int)
    ]
    representatives = recording_representatives_for_release_track_ids(release_track_ids)
    recording_album_ids = recording_release_album_ids_for_release_track_ids(release_track_ids)
    family_release_album_ids = {int(version["release_album_id"]) for version in versions}

    def identity_key(item: dict[str, Any]) -> str:
        combined_position_key = item.get("_combined_edition_position_key")
        if combined_position_key:
            return f"combined-edition:{combined_position_key}"
        release_track_id = item.get("release_track_id")
        if isinstance(release_track_id, int):
            return f"recording:{representatives.get(release_track_id, release_track_id)}"
        return f"spotify:{item.get('id') or ''}"

    selected_rows = [dict(item) for item in items_by_album.get(selected_spotify_album_id, [])]
    selected_keys = {identity_key(item) for item in selected_rows}
    availability: dict[str, list[dict[str, Any]]] = {}
    item_by_key: dict[str, dict[str, Any]] = {}
    for version in versions:
        for item in items_by_album.get(version["spotify_album_id"], []):
            key = identity_key(item)
            availability.setdefault(key, []).append(version)
            item_by_key.setdefault(key, item)
    for item in selected_rows:
        key = identity_key(item)
        item["family_exclusive"] = False
        item["family_available_versions"] = availability.get(key, [])
        release_track_id = item.get("release_track_id")
        item["family_has_edition_relation"] = len(availability.get(key, [])) > 1
        item["family_has_external_recording_relation"] = isinstance(release_track_id, int) and bool(
            recording_album_ids.get(release_track_id, set()) - family_release_album_ids
        )
    ghost_rows: list[dict[str, Any]] = []
    for key, available_versions in availability.items():
        if key in selected_keys:
            continue
        switch_version = sorted(
            available_versions,
            key=lambda version: (version.get("total_tracks") or 999999, version.get("release_year") or 999999),
        )[0]
        ghost = dict(item_by_key[key])
        ghost["family_exclusive"] = True
        ghost["family_available_versions"] = available_versions
        ghost["family_switch_album_id"] = switch_version["spotify_album_id"]
        ghost["family_switch_label"] = switch_version["label"]
        release_track_id = ghost.get("release_track_id")
        ghost["family_has_edition_relation"] = True
        ghost["family_has_external_recording_relation"] = isinstance(release_track_id, int) and bool(
            recording_album_ids.get(release_track_id, set()) - family_release_album_ids
        )
        ghost_rows.append(ghost)
    combined_rows = selected_rows + ghost_rows
    if combined_edition_group is not None or any(_COMPANION_DISC.search(str(version.get("label") or "")) for version in versions):
        combined_rows.sort(key=lambda item: (
            int(item.get("disc_number") or 1),
            int(item.get("track_number") or 999999),
            str(item.get("name") or ""),
        ))
    for item in combined_rows:
        item.pop("_combined_edition_position_key", None)
    return {
        "album_family_id": family_id,
        "core_name": core_name,
        "selected_spotify_album_id": selected_spotify_album_id,
        "release_album_ids": [version["release_album_id"] for version in versions],
        "versions": versions,
        "items": combined_rows,
        "disc_labels": combined_edition_group.get("disc_labels", {}) if combined_edition_group is not None else {},
        "version_disc_numbers": combined_edition_group.get("version_disc_numbers", {}) if combined_edition_group is not None else {},
    }
