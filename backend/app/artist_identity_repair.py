from __future__ import annotations

import sqlite3
import unicodedata
import re
from collections import defaultdict
from difflib import SequenceMatcher
from typing import Any

from backend.app.db import _normalize_name, sqlite_connection
from backend.app.artist_resolution import (
    EVIDENCE_RECONCILED_SOURCE_ALBUM,
    EVIDENCE_RECONCILED_SOURCE_TRACK,
    EVIDENCE_SHARED_NORMALIZED_ALBUM_TITLE_WITH_PROVIDER_CONTEXT,
    EVIDENCE_SHARED_RELEASE_ALBUM_ID,
    EVIDENCE_SHARED_RELEASE_TRACK_ID,
    IDENTITY_EVIDENCE_TYPES,
)


TEXT_ONLY_SOURCE_NAMES = {"history_raw"}
MIN_SIMILAR_NAME_RATIO = 0.55
ARTIST_INVENTORY_LIMIT = 25
SAFE_REPAIR_CATEGORIES = {
    "exact_name_identity_evidence_safe_repair",
    "exact_name_album_title_provider_context_safe_repair",
}


def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _artist_source_maps(connection: sqlite3.Connection, artist_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    if not artist_ids:
        return {}
    placeholders = ",".join("?" for _ in artist_ids)
    rows = connection.execute(
        f"""
        SELECT
          sam.id,
          sam.artist_id,
          sam.source_artist_id,
          sa.source_name,
          sa.external_id,
          sa.external_uri,
          sa.source_name_raw,
          sam.match_method,
          sam.confidence,
          sam.status,
          sam.is_user_confirmed,
          sam.explanation
        FROM source_artist_map sam
        JOIN source_artist sa
          ON sa.id = sam.source_artist_id
        WHERE sam.artist_id IN ({placeholders})
        ORDER BY sam.artist_id ASC, sa.source_name ASC, sa.external_id ASC, sam.id ASC
        """,
        artist_ids,
    ).fetchall()
    by_artist: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_artist[int(row["artist_id"])].append(_row_dict(row))
    return by_artist


def _link_counts(connection: sqlite3.Connection, table: str, artist_ids: list[int]) -> dict[int, int]:
    if not artist_ids:
        return {}
    placeholders = ",".join("?" for _ in artist_ids)
    rows = connection.execute(
        f"""
        SELECT artist_id, count(*) AS link_count
        FROM {table}
        WHERE artist_id IN ({placeholders})
        GROUP BY artist_id
        """,
        artist_ids,
    ).fetchall()
    return {int(row["artist_id"]): int(row["link_count"]) for row in rows}


def _provider_source_keys(source_maps: list[dict[str, Any]]) -> set[tuple[str, str]]:
    return {
        (str(row["source_name"]), str(row["external_id"]))
        for row in source_maps
        if str(row["source_name"]) not in TEXT_ONLY_SOURCE_NAMES
    }


def _is_text_only(source_maps: list[dict[str, Any]]) -> bool:
    return bool(source_maps) and all(str(row["source_name"]) in TEXT_ONLY_SOURCE_NAMES for row in source_maps)


def _is_composite_credit_name(value: str | None) -> bool:
    return "," in (value or "")


def _stylization_key(value: str | None) -> str | None:
    normalized = unicodedata.normalize("NFKD", value or "")
    without_marks = "".join(char for char in normalized if not unicodedata.combining(char))
    folded = "".join(char.lower() for char in without_marks if char.isalnum())
    return folded or None


def _uniform_artist_match_text(value: str | None) -> str | None:
    normalized = unicodedata.normalize("NFKD", value or "")
    without_marks = "".join(char for char in normalized if not unicodedata.combining(char))
    folded = "".join(char.lower() if char.isalnum() else " " for char in without_marks)
    uniform = " ".join(folded.split())
    return uniform or None


def _split_composite_credit_parts(value: str | None) -> list[dict[str, str]]:
    if not _is_composite_credit_name(value):
        return []
    seen: set[str] = set()
    parts: list[dict[str, str]] = []
    raw_parts = str(value or "").split(",")
    for index, raw_part in enumerate(raw_parts):
        display = " ".join(raw_part.strip().split())
        if index == len(raw_parts) - 1:
            display = re.sub(r"\s+(?:&|and)\s+friends\.?$", "", display, flags=re.IGNORECASE).strip()
        normalized = _normalize_name(display)
        if not display or not normalized or normalized in seen:
            continue
        seen.add(normalized)
        parts.append({"display_name": display, "normalized_name": normalized})
    return parts if len(parts) > 1 else []


def _has_non_latin_text(value: str | None) -> bool:
    for char in value or "":
        if char.isalpha():
            try:
                name = unicodedata.name(char)
            except ValueError:
                continue
            if "LATIN" not in name:
                return True
    return False


def _name_similarity(left: str | None, right: str | None) -> float:
    left_key = _stylization_key(left) or ""
    right_key = _stylization_key(right) or ""
    if not left_key or not right_key:
        return 0.0
    return SequenceMatcher(None, left_key, right_key).ratio()


def build_duplicate_artist_audit() -> dict[str, Any]:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        return _build_duplicate_artist_audit_with_connection(connection)


def _build_duplicate_artist_audit_with_connection(connection: sqlite3.Connection) -> dict[str, Any]:
    artist_rows = _artist_rows(connection)
    groups: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in artist_rows:
        normalized_name = _normalize_name(str(row["canonical_name"] or row["sort_name"] or ""))
        if normalized_name:
            groups[normalized_name].append(row)

    duplicate_groups: list[dict[str, Any]] = []
    exact_name_identity_evidence_safe_repair: list[dict[str, Any]] = []
    exact_name_album_title_provider_context_safe_repair: list[dict[str, Any]] = []
    exact_name_album_title_evidence_safe_repair: list[dict[str, Any]] = []
    exact_name_only_review: list[dict[str, Any]] = []
    exact_name_orphan_placeholder_review: list[dict[str, Any]] = []
    ambiguous_provider_review_only: list[dict[str, Any]] = []
    exact_name_no_provider_review_only: list[dict[str, Any]] = []
    for normalized_name, rows in sorted(groups.items()):
        if len(rows) < 2:
            continue
        artist_ids = [int(row["id"]) for row in rows]
        source_maps_by_artist = _artist_source_maps(connection, artist_ids)
        album_counts = _link_counts(connection, "album_artist", artist_ids)
        track_counts = _link_counts(connection, "track_artist", artist_ids)

        artist_payloads: list[dict[str, Any]] = []
        provider_backed_ids: list[int] = []
        for row in rows:
            artist_id = int(row["id"])
            source_maps = source_maps_by_artist.get(artist_id, [])
            provider_source_keys = sorted(_provider_source_keys(source_maps))
            if provider_source_keys:
                provider_backed_ids.append(artist_id)
            artist_payloads.append(
                {
                    "artist_id": artist_id,
                    "display_name": row["canonical_name"],
                    "sort_name": row["sort_name"],
                    "provider_backed": bool(provider_source_keys),
                    "provider_source_ids": [
                        {"source_name": source_name, "external_id": external_id}
                        for source_name, external_id in provider_source_keys
                    ],
                    "text_only": _is_text_only(source_maps),
                    "source_artist_maps": source_maps,
                    "album_artist_link_count": album_counts.get(artist_id, 0),
                    "track_artist_link_count": track_counts.get(artist_id, 0),
                    "albums": _artist_albums(connection, artist_id),
                    "tracks": _artist_tracks(connection, artist_id),
                }
            )

        recommended_canonical_artist_id: int | None = None
        duplicate_candidate_artist_ids: list[int] = []
        recommendation_reason: str
        if len(provider_backed_ids) == 1:
            recommended_canonical_artist_id = provider_backed_ids[0]
            duplicate_candidate_artist_ids = [artist_id for artist_id in artist_ids if artist_id != provider_backed_ids[0]]
            recommendation_reason = "single_provider_backed_artist"
        elif len(provider_backed_ids) > 1:
            recommendation_reason = "multiple_provider_backed_artists"
        else:
            recommendation_reason = "no_provider_backed_artist"

        group_payload = {
            "normalized_name": normalized_name,
            "canonical_name": normalized_name,
            "artists": artist_payloads,
            "recommended_canonical_artist_id": recommended_canonical_artist_id,
            "duplicate_candidate_artist_ids": duplicate_candidate_artist_ids,
            "recommendation_reason": recommendation_reason,
        }
        classified_group = _classify_exact_name_group(connection, group_payload)
        group_payload = {**group_payload, **classified_group}
        duplicate_groups.append(group_payload)
        category = str(group_payload["category"])
        if category == "exact_name_identity_evidence_safe_repair":
            exact_name_identity_evidence_safe_repair.append(group_payload)
        elif category == "exact_name_album_title_provider_context_safe_repair":
            exact_name_album_title_provider_context_safe_repair.append(group_payload)
        elif category == "exact_name_album_title_evidence_safe_repair":
            exact_name_album_title_evidence_safe_repair.append(group_payload)
        elif category == "exact_name_orphan_placeholder_review":
            exact_name_orphan_placeholder_review.append(group_payload)
        elif category == "ambiguous_provider_review_only":
            ambiguous_provider_review_only.append(group_payload)
        elif category == "exact_name_no_provider_review_only":
            exact_name_no_provider_review_only.append(group_payload)
        else:
            exact_name_only_review.append(group_payload)

    stylization_groups = _build_stylization_groups(connection, artist_rows)
    similar_same_album_groups, composite_credit_groups = _build_same_album_review_groups(connection, artist_rows)

    return {
        "groups_found": len(duplicate_groups),
        "groups": duplicate_groups,
        "candidate_categories": {
            "exact_name": {
                "label": "Exact normalized name",
                "repairable": True,
                "groups": duplicate_groups,
            },
            "exact_name_identity_evidence_safe_repair": {
                "label": "Exact name + internal identity evidence",
                "repairable": True,
                "groups": exact_name_identity_evidence_safe_repair,
            },
            "exact_name_album_title_provider_context_safe_repair": {
                "label": "Exact name + album title with provider context",
                "repairable": True,
                "groups": exact_name_album_title_provider_context_safe_repair,
            },
            "exact_name_album_title_evidence_safe_repair": {
                "label": "Exact name + album title evidence",
                "repairable": True,
                "groups": exact_name_album_title_evidence_safe_repair,
            },
            "exact_name_only_review": {
                "label": "Exact name only",
                "repairable": False,
                "groups": exact_name_only_review,
            },
            "exact_name_orphan_placeholder_review": {
                "label": "Exact name orphan placeholder",
                "repairable": False,
                "groups": exact_name_orphan_placeholder_review,
            },
            "ambiguous_provider_review_only": {
                "label": "Ambiguous provider-backed exact name",
                "repairable": False,
                "groups": ambiguous_provider_review_only,
            },
            "exact_name_no_provider_review_only": {
                "label": "Exact name with no provider-backed artist",
                "repairable": False,
                "groups": exact_name_no_provider_review_only,
            },
            "stylization": {
                "label": "Different stylization",
                "repairable": False,
                "groups": stylization_groups,
            },
            "similar_same_album": {
                "label": "Similar name + same album",
                "repairable": False,
                "groups": similar_same_album_groups,
            },
            "composite_credit": {
                "label": "Composite text credit",
                "repairable": False,
                "groups": composite_credit_groups,
            },
        },
        "summary": {
            "exact_name_groups": len(duplicate_groups),
            "exact_name_identity_evidence_safe_repair_groups": len(exact_name_identity_evidence_safe_repair),
            "exact_name_album_title_provider_context_safe_repair_groups": len(exact_name_album_title_provider_context_safe_repair),
            "exact_name_album_title_evidence_safe_repair_groups": len(exact_name_album_title_evidence_safe_repair),
            "exact_name_only_review_groups": len(exact_name_only_review),
            "exact_name_orphan_placeholder_review_groups": len(exact_name_orphan_placeholder_review),
            "ambiguous_provider_review_only_groups": len(ambiguous_provider_review_only),
            "exact_name_no_provider_review_only_groups": len(exact_name_no_provider_review_only),
            "stylization_groups": len(stylization_groups),
            "similar_same_album_groups": len(similar_same_album_groups),
            "composite_credit_groups": len(composite_credit_groups),
        },
    }


def _artist_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT id, canonical_name, sort_name, created_at, updated_at
        FROM artist
        ORDER BY id ASC
        """
    ).fetchall()


def _artist_albums(connection: sqlite3.Connection, artist_id: int) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
          aa.release_album_id,
          ra.primary_name,
          ra.normalized_name,
          ra.release_year,
          COALESCE(aa.role, 'primary') AS role,
          aa.billing_index,
          aa.credited_as,
          aa.match_method,
          aa.confidence,
          aa.source_basis
        FROM album_artist aa
        JOIN release_album ra
          ON ra.id = aa.release_album_id
        WHERE aa.artist_id = ?
        ORDER BY aa.billing_index IS NULL ASC, aa.billing_index ASC, ra.primary_name ASC, aa.release_album_id ASC
        LIMIT ?
        """,
        (artist_id, ARTIST_INVENTORY_LIMIT),
    ).fetchall()
    return [
        {
            "release_album_id": int(row["release_album_id"]),
            "album_name": row["primary_name"],
            "normalized_name": row["normalized_name"],
            "release_year": row["release_year"],
            "role": row["role"],
            "billing_index": row["billing_index"],
            "credited_as": row["credited_as"],
            "match_method": row["match_method"],
            "confidence": row["confidence"],
            "source_basis": row["source_basis"],
        }
        for row in rows
    ]


def _artist_tracks(connection: sqlite3.Connection, artist_id: int) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
          ta.release_track_id,
          rt.primary_name,
          rt.normalized_name,
          rt.duration_ms,
          COALESCE(ta.role, 'primary') AS role,
          ta.billing_index,
          ta.credited_as,
          ta.match_method,
          ta.confidence,
          ta.source_basis,
          group_concat(DISTINCT ra.primary_name) AS album_names
        FROM track_artist ta
        JOIN release_track rt
          ON rt.id = ta.release_track_id
        LEFT JOIN album_track at
          ON at.release_track_id = ta.release_track_id
        LEFT JOIN release_album ra
          ON ra.id = at.release_album_id
        WHERE ta.artist_id = ?
        GROUP BY ta.id
        ORDER BY ta.billing_index IS NULL ASC, ta.billing_index ASC, rt.primary_name ASC, ta.release_track_id ASC
        LIMIT ?
        """,
        (artist_id, ARTIST_INVENTORY_LIMIT),
    ).fetchall()
    return [
        {
            "release_track_id": int(row["release_track_id"]),
            "track_name": row["primary_name"],
            "normalized_name": row["normalized_name"],
            "duration_ms": row["duration_ms"],
            "role": row["role"],
            "billing_index": row["billing_index"],
            "credited_as": row["credited_as"],
            "match_method": row["match_method"],
            "confidence": row["confidence"],
            "source_basis": row["source_basis"],
            "album_names": [name for name in str(row["album_names"] or "").split(",") if name],
        }
        for row in rows
    ]


def _classify_exact_name_group(connection: sqlite3.Connection, group: dict[str, Any]) -> dict[str, Any]:
    artists = list(group["artists"])
    provider_artists = [artist for artist in artists if artist["provider_backed"]]
    text_only_artists = [artist for artist in artists if artist["text_only"]]
    artist_ids = [int(artist["artist_id"]) for artist in artists]
    if len(provider_artists) != 1:
        if len(provider_artists) == 0:
            return {
                "category": "exact_name_no_provider_review_only",
                "repairable": False,
                "evidence_by_duplicate_artist_id": {},
                "evidence_types": [],
                "review_reason": "no_provider_backed_artist",
            }
        return {
            "category": "ambiguous_provider_review_only",
            "repairable": False,
            "evidence_by_duplicate_artist_id": {},
            "evidence_types": [],
            "review_reason": "multiple_provider_backed_artists",
        }

    canonical_artist_id = int(provider_artists[0]["artist_id"])
    duplicate_artist_ids = [
        int(artist["artist_id"])
        for artist in text_only_artists
        if int(artist["artist_id"]) != canonical_artist_id
    ]
    unsafe_artist_ids = [
        artist_id
        for artist_id in artist_ids
        if artist_id != canonical_artist_id and artist_id not in duplicate_artist_ids
    ]
    if unsafe_artist_ids:
        return {
            "category": "exact_name_only_review",
            "repairable": False,
            "evidence_by_duplicate_artist_id": {},
            "evidence_types": [],
            "review_reason": "non_text_only_duplicate_artist",
            "unsafe_artist_ids": unsafe_artist_ids,
        }
    if not duplicate_artist_ids:
        return {
            "category": "exact_name_only_review",
            "repairable": False,
            "evidence_by_duplicate_artist_id": {},
            "evidence_types": [],
            "review_reason": "no_text_only_duplicate_artist",
        }

    evidence_by_duplicate: dict[int, list[dict[str, Any]]] = {}
    missing_evidence_artist_ids: list[int] = []
    orphan_artist_ids: list[int] = []
    for duplicate_artist_id in duplicate_artist_ids:
        evidence = _safe_repair_evidence_for_duplicate(
            connection,
            canonical_artist_id=canonical_artist_id,
            duplicate_artist_id=duplicate_artist_id,
        )
        evidence_by_duplicate[duplicate_artist_id] = evidence
        if not evidence:
            if _is_orphan_placeholder_artist(connection, duplicate_artist_id):
                orphan_artist_ids.append(duplicate_artist_id)
            else:
                missing_evidence_artist_ids.append(duplicate_artist_id)

    if missing_evidence_artist_ids or orphan_artist_ids:
        return {
            "category": "exact_name_orphan_placeholder_review" if orphan_artist_ids and not missing_evidence_artist_ids else "exact_name_only_review",
            "repairable": False,
            "evidence_by_duplicate_artist_id": evidence_by_duplicate,
            "evidence_types": sorted({item["type"] for items in evidence_by_duplicate.values() for item in items}),
            "review_reason": "orphan_placeholder_without_identity_evidence" if orphan_artist_ids and not missing_evidence_artist_ids else "same_name_without_identity_evidence",
            "missing_evidence_artist_ids": missing_evidence_artist_ids,
            "orphan_artist_ids": orphan_artist_ids,
        }

    evidence_types = sorted({item["type"] for items in evidence_by_duplicate.values() for item in items})
    has_identity_evidence = any(
        evidence_type in IDENTITY_EVIDENCE_TYPES
        for evidence_type in evidence_types
    )
    category = (
        "exact_name_identity_evidence_safe_repair"
        if has_identity_evidence
        else "exact_name_album_title_provider_context_safe_repair"
    )
    return {
        "category": category,
        "repairable": True,
        "canonical_artist_id": canonical_artist_id,
        "duplicate_artist_ids": duplicate_artist_ids,
        "evidence_by_duplicate_artist_id": evidence_by_duplicate,
        "evidence_types": evidence_types,
        "review_reason": "safe_repair_evidence",
    }


def _safe_repair_evidence_for_duplicate(
    connection: sqlite3.Connection,
    *,
    canonical_artist_id: int,
    duplicate_artist_id: int,
) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    evidence.extend(_shared_release_album_evidence(connection, canonical_artist_id, duplicate_artist_id))
    evidence.extend(_shared_release_track_evidence(connection, canonical_artist_id, duplicate_artist_id))
    evidence.extend(_reconciled_source_album_evidence(connection, canonical_artist_id, duplicate_artist_id))
    evidence.extend(_reconciled_source_track_evidence(connection, canonical_artist_id, duplicate_artist_id))
    if evidence:
        return _dedupe_evidence(evidence)
    evidence.extend(_shared_normalized_album_title_provider_context_evidence(connection, canonical_artist_id, duplicate_artist_id))
    return _dedupe_evidence(evidence)


def _dedupe_evidence(evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for item in evidence:
        key = (str(item.get("type")), repr(sorted((key, value) for key, value in item.items() if key != "type")))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _shared_release_album_evidence(connection: sqlite3.Connection, canonical_artist_id: int, duplicate_artist_id: int) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
          aa_text.release_album_id,
          ra.primary_name,
          ra.normalized_name
        FROM album_artist aa_text
        JOIN album_artist aa_provider
          ON aa_provider.release_album_id = aa_text.release_album_id
         AND COALESCE(aa_provider.role, 'primary') = COALESCE(aa_text.role, 'primary')
        JOIN release_album ra
          ON ra.id = aa_text.release_album_id
        WHERE aa_text.artist_id = ?
          AND aa_provider.artist_id = ?
        ORDER BY aa_text.release_album_id ASC
        """,
        (duplicate_artist_id, canonical_artist_id),
    ).fetchall()
    return [
        {
            "type": EVIDENCE_SHARED_RELEASE_ALBUM_ID,
            "release_album_id": int(row["release_album_id"]),
            "album_name": row["primary_name"],
            "normalized_album_title": row["normalized_name"],
        }
        for row in rows
    ]


def _shared_release_track_evidence(connection: sqlite3.Connection, canonical_artist_id: int, duplicate_artist_id: int) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
          ta_text.release_track_id,
          rt.primary_name,
          rt.normalized_name
        FROM track_artist ta_text
        JOIN track_artist ta_provider
          ON ta_provider.release_track_id = ta_text.release_track_id
         AND COALESCE(ta_provider.role, 'primary') = COALESCE(ta_text.role, 'primary')
        JOIN release_track rt
          ON rt.id = ta_text.release_track_id
        WHERE ta_text.artist_id = ?
          AND ta_provider.artist_id = ?
        ORDER BY ta_text.release_track_id ASC
        """,
        (duplicate_artist_id, canonical_artist_id),
    ).fetchall()
    return [
        {
            "type": EVIDENCE_SHARED_RELEASE_TRACK_ID,
            "release_track_id": int(row["release_track_id"]),
            "track_name": row["primary_name"],
            "normalized_track_title": row["normalized_name"],
        }
        for row in rows
    ]


def _reconciled_source_album_evidence(connection: sqlite3.Connection, canonical_artist_id: int, duplicate_artist_id: int) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT DISTINCT
          aa_text.release_album_id,
          ra.primary_name,
          history_source.external_id AS text_source_album_id,
          provider_source.source_name AS provider_source_name,
          provider_source.external_id AS provider_source_album_id
        FROM album_artist aa_text
        JOIN album_artist aa_provider
          ON aa_provider.release_album_id = aa_text.release_album_id
         AND COALESCE(aa_provider.role, 'primary') = COALESCE(aa_text.role, 'primary')
        JOIN release_album ra
          ON ra.id = aa_text.release_album_id
        JOIN source_album_map history_map
          ON history_map.release_album_id = aa_text.release_album_id
        JOIN source_album history_source
          ON history_source.id = history_map.source_album_id
         AND history_source.source_name IN ('history_raw')
        JOIN source_album_map provider_map
          ON provider_map.release_album_id = aa_text.release_album_id
        JOIN source_album provider_source
          ON provider_source.id = provider_map.source_album_id
         AND provider_source.source_name NOT IN ('history_raw')
        WHERE aa_text.artist_id = ?
          AND aa_provider.artist_id = ?
        ORDER BY aa_text.release_album_id ASC
        """,
        (duplicate_artist_id, canonical_artist_id),
    ).fetchall()
    return [
        {
            "type": EVIDENCE_RECONCILED_SOURCE_ALBUM,
            "release_album_id": int(row["release_album_id"]),
            "album_name": row["primary_name"],
            "text_source_album_id": row["text_source_album_id"],
            "provider_source_name": row["provider_source_name"],
            "provider_source_album_id": row["provider_source_album_id"],
        }
        for row in rows
    ]


def _reconciled_source_track_evidence(connection: sqlite3.Connection, canonical_artist_id: int, duplicate_artist_id: int) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT DISTINCT
          ta_text.release_track_id,
          rt.primary_name,
          history_source.external_id AS text_source_track_id,
          provider_source.source_name AS provider_source_name,
          provider_source.external_id AS provider_source_track_id
        FROM track_artist ta_text
        JOIN track_artist ta_provider
          ON ta_provider.release_track_id = ta_text.release_track_id
         AND COALESCE(ta_provider.role, 'primary') = COALESCE(ta_text.role, 'primary')
        JOIN release_track rt
          ON rt.id = ta_text.release_track_id
        JOIN source_track_map history_map
          ON history_map.release_track_id = ta_text.release_track_id
        JOIN source_track history_source
          ON history_source.id = history_map.source_track_id
         AND history_source.source_name IN ('history_raw')
        JOIN source_track_map provider_map
          ON provider_map.release_track_id = ta_text.release_track_id
        JOIN source_track provider_source
          ON provider_source.id = provider_map.source_track_id
         AND provider_source.source_name NOT IN ('history_raw')
        WHERE ta_text.artist_id = ?
          AND ta_provider.artist_id = ?
        ORDER BY ta_text.release_track_id ASC
        """,
        (duplicate_artist_id, canonical_artist_id),
    ).fetchall()
    return [
        {
            "type": EVIDENCE_RECONCILED_SOURCE_TRACK,
            "release_track_id": int(row["release_track_id"]),
            "track_name": row["primary_name"],
            "text_source_track_id": row["text_source_track_id"],
            "provider_source_name": row["provider_source_name"],
            "provider_source_track_id": row["provider_source_track_id"],
        }
        for row in rows
    ]


def _shared_normalized_album_title_provider_context_evidence(
    connection: sqlite3.Connection,
    canonical_artist_id: int,
    duplicate_artist_id: int,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT DISTINCT
          text_album.id AS text_release_album_id,
          text_album.primary_name AS text_album_name,
          provider_album.id AS provider_release_album_id,
          provider_album.primary_name AS provider_album_name,
          COALESCE(text_album.normalized_name, lower(trim(text_album.primary_name))) AS normalized_album_title
        FROM album_artist text_link
        JOIN release_album text_album
          ON text_album.id = text_link.release_album_id
        JOIN album_artist provider_link
          ON provider_link.artist_id = ?
        JOIN release_album provider_album
          ON provider_album.id = provider_link.release_album_id
         AND COALESCE(provider_album.normalized_name, lower(trim(provider_album.primary_name))) =
             COALESCE(text_album.normalized_name, lower(trim(text_album.primary_name)))
        WHERE text_link.artist_id = ?
          AND text_link.release_album_id != provider_link.release_album_id
          AND (
            EXISTS (
              SELECT 1
              FROM source_album_map provider_album_map
              JOIN source_album provider_source_album
                ON provider_source_album.id = provider_album_map.source_album_id
               AND provider_source_album.source_name NOT IN ('history_raw')
              WHERE provider_album_map.release_album_id = provider_album.id
            )
            OR EXISTS (
              SELECT 1
              FROM album_track provider_album_track
              JOIN source_track_map provider_track_map
                ON provider_track_map.release_track_id = provider_album_track.release_track_id
              JOIN source_track provider_source_track
                ON provider_source_track.id = provider_track_map.source_track_id
               AND provider_source_track.source_name NOT IN ('history_raw')
              WHERE provider_album_track.release_album_id = provider_album.id
            )
          )
        ORDER BY normalized_album_title ASC, text_album.id ASC, provider_album.id ASC
        """,
        (canonical_artist_id, duplicate_artist_id),
    ).fetchall()
    return [
        {
            "type": EVIDENCE_SHARED_NORMALIZED_ALBUM_TITLE_WITH_PROVIDER_CONTEXT,
            "normalized_album_title": row["normalized_album_title"],
            "text_release_album_id": int(row["text_release_album_id"]),
            "text_album_name": row["text_album_name"],
            "provider_release_album_id": int(row["provider_release_album_id"]),
            "provider_album_name": row["provider_album_name"],
        }
        for row in rows
    ]


def _is_orphan_placeholder_artist(connection: sqlite3.Connection, artist_id: int) -> bool:
    counts = _artist_reference_counts(connection, artist_id)
    return counts["source_artist_map"] > 0 and counts["album_artist"] == 0 and counts["track_artist"] == 0


def _build_stylization_groups(connection: sqlite3.Connection, artist_rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    by_key: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in artist_rows:
        key = _stylization_key(str(row["canonical_name"] or row["sort_name"] or ""))
        if key:
            by_key[key].append(row)

    groups: list[dict[str, Any]] = []
    for key, rows in sorted(by_key.items()):
        normalized_names = {
            _normalize_name(str(row["canonical_name"] or row["sort_name"] or ""))
            for row in rows
        }
        normalized_names.discard(None)
        if len(rows) < 2 or len(normalized_names) < 2:
            continue
        artist_ids = [int(row["id"]) for row in rows]
        source_maps_by_artist = _artist_source_maps(connection, artist_ids)
        album_counts = _link_counts(connection, "album_artist", artist_ids)
        track_counts = _link_counts(connection, "track_artist", artist_ids)
        groups.append(
            {
                "category": "stylization",
                "stylization_key": key,
                "matching_key": key,
                "uniform_matching_text": _uniform_artist_match_text(str(rows[0]["canonical_name"] or rows[0]["sort_name"] or "")),
                "normalized_names": sorted(str(name) for name in normalized_names),
                "repairable": False,
                "reason": "accent_punctuation_or_spacing_variant",
                "artists": [
                    _artist_candidate_payload(connection, row, source_maps_by_artist, album_counts, track_counts)
                    for row in rows
                ],
            }
        )
    return groups


def _build_composite_credit_cleanup_plan(
    connection: sqlite3.Connection,
    *,
    composite_artist_id: int,
    release_album_id: int | None,
    role: str,
    artist_ids: set[int],
    row_by_id: dict[int, sqlite3.Row],
) -> dict[str, Any]:
    composite_name = str(row_by_id[composite_artist_id]["canonical_name"] or "")
    credit_parts = _split_composite_credit_parts(composite_name)
    candidate_artist_ids = [artist_id for artist_id in sorted(artist_ids) if artist_id != composite_artist_id]
    matched_parts: list[dict[str, Any]] = []
    ambiguous_parts: list[dict[str, Any]] = []
    missing_parts: list[dict[str, Any]] = []
    matched_artist_ids: list[int] = []

    for part in credit_parts:
        matching_artist_ids = [
            artist_id
            for artist_id in candidate_artist_ids
            if _normalize_name(str(row_by_id[artist_id]["canonical_name"] or row_by_id[artist_id]["sort_name"] or ""))
            == part["normalized_name"]
        ]
        part_payload = {**part, "matched_artist_ids": matching_artist_ids}
        if len(matching_artist_ids) == 1:
            matched_parts.append(part_payload)
            matched_artist_ids.append(matching_artist_ids[0])
        elif len(matching_artist_ids) > 1:
            ambiguous_parts.append(part_payload)
        else:
            missing_parts.append(part_payload)

    album_links_to_delete: list[dict[str, Any]] = []
    album_links_to_insert: list[dict[str, Any]] = []
    track_links_to_delete: list[dict[str, Any]] = []
    track_links_review_only: list[dict[str, Any]] = []
    all_parts_matched = bool(credit_parts) and len(matched_parts) == len(credit_parts)

    if all_parts_matched and release_album_id is not None:
        composite_album_links = connection.execute(
            """
            SELECT id
            FROM album_artist
            WHERE release_album_id = ?
              AND artist_id = ?
              AND COALESCE(role, 'primary') = ?
            ORDER BY id ASC
            """,
            (release_album_id, composite_artist_id, role),
        ).fetchall()
        for row in composite_album_links:
            album_links_to_delete.append(
                {
                    "link_id": int(row["id"]),
                    "release_album_id": release_album_id,
                    "artist_id": composite_artist_id,
                    "role": role,
                }
            )

        for artist_id in matched_artist_ids:
            existing = connection.execute(
                """
                SELECT id
                FROM album_artist
                WHERE release_album_id = ?
                  AND artist_id = ?
                  AND COALESCE(role, 'primary') = ?
                LIMIT 1
                """,
                (release_album_id, artist_id, role),
            ).fetchone()
            if existing is None:
                album_links_to_insert.append(
                    {
                        "release_album_id": release_album_id,
                        "artist_id": artist_id,
                        "role": role,
                    }
                )

        composite_track_links = connection.execute(
            """
            SELECT ta.id, ta.release_track_id
            FROM track_artist ta
            JOIN album_track at
              ON at.release_track_id = ta.release_track_id
            WHERE at.release_album_id = ?
              AND ta.artist_id = ?
              AND COALESCE(ta.role, 'primary') = ?
            ORDER BY ta.release_track_id ASC, ta.id ASC
            """,
            (release_album_id, composite_artist_id, role),
        ).fetchall()
        for row in composite_track_links:
            release_track_id = int(row["release_track_id"])
            missing_track_artist_ids = [
                artist_id
                for artist_id in matched_artist_ids
                if connection.execute(
                    """
                    SELECT 1
                    FROM track_artist
                    WHERE release_track_id = ?
                      AND artist_id = ?
                      AND COALESCE(role, 'primary') = ?
                    LIMIT 1
                    """,
                    (release_track_id, artist_id, role),
                ).fetchone()
                is None
            ]
            payload = {
                "link_id": int(row["id"]),
                "release_track_id": release_track_id,
                "artist_id": composite_artist_id,
                "role": role,
                "matched_artist_ids": matched_artist_ids,
            }
            if missing_track_artist_ids:
                track_links_review_only.append({**payload, "missing_artist_ids": missing_track_artist_ids})
            else:
                track_links_to_delete.append(payload)

    return {
        "composite_artist_id": composite_artist_id,
        "composite_display_name": composite_name,
        "credit_parts": credit_parts,
        "matched_parts": matched_parts,
        "missing_parts": missing_parts,
        "ambiguous_parts": ambiguous_parts,
        "matched_artist_ids": sorted(set(matched_artist_ids)),
        "all_parts_matched": all_parts_matched,
        "album_links_to_delete": album_links_to_delete,
        "album_links_to_insert": album_links_to_insert,
        "track_links_to_delete": track_links_to_delete,
        "track_links_review_only": track_links_review_only,
        "ready_for_cleanup": all_parts_matched
        and not ambiguous_parts
        and not missing_parts
        and release_album_id is not None
        and not album_links_to_insert
        and not track_links_review_only,
    }


def _artist_candidate_payload(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    source_maps_by_artist: dict[int, list[dict[str, Any]]],
    album_counts: dict[int, int],
    track_counts: dict[int, int],
) -> dict[str, Any]:
    artist_id = int(row["id"])
    source_maps = source_maps_by_artist.get(artist_id, [])
    provider_source_keys = sorted(_provider_source_keys(source_maps))
    return {
        "artist_id": artist_id,
        "display_name": row["canonical_name"],
        "sort_name": row["sort_name"],
        "normalized_name": _normalize_name(str(row["canonical_name"] or row["sort_name"] or "")),
        "provider_backed": bool(provider_source_keys),
        "provider_source_ids": [
            {"source_name": source_name, "external_id": external_id}
            for source_name, external_id in provider_source_keys
        ],
        "text_only": _is_text_only(source_maps),
        "source_artist_maps": source_maps,
        "album_artist_link_count": album_counts.get(artist_id, 0),
        "track_artist_link_count": track_counts.get(artist_id, 0),
        "albums": _artist_albums(connection, artist_id),
        "tracks": _artist_tracks(connection, artist_id),
    }


def _build_same_album_review_groups(
    connection: sqlite3.Connection,
    artist_rows: list[sqlite3.Row],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = connection.execute(
        """
        SELECT
          left_artist.id AS left_artist_id,
          left_artist.canonical_name AS left_name,
          right_artist.id AS right_artist_id,
          right_artist.canonical_name AS right_name,
          aa_left.release_album_id,
          ra.primary_name AS album_name,
          COALESCE(aa_left.role, 'primary') AS role
        FROM album_artist aa_left
        JOIN album_artist aa_right
          ON aa_right.release_album_id = aa_left.release_album_id
         AND aa_right.artist_id > aa_left.artist_id
         AND COALESCE(aa_right.role, 'primary') = COALESCE(aa_left.role, 'primary')
        JOIN artist left_artist
          ON left_artist.id = aa_left.artist_id
        JOIN artist right_artist
          ON right_artist.id = aa_right.artist_id
        JOIN release_album ra
          ON ra.id = aa_left.release_album_id
        WHERE COALESCE(left_artist.sort_name, lower(trim(left_artist.canonical_name))) !=
              COALESCE(right_artist.sort_name, lower(trim(right_artist.canonical_name)))
        ORDER BY left_artist.id ASC, right_artist.id ASC, aa_left.release_album_id ASC
        """
    ).fetchall()
    albums_by_pair: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    names_by_pair: dict[tuple[int, int], tuple[str, str]] = {}
    for row in rows:
        pair = (int(row["left_artist_id"]), int(row["right_artist_id"]))
        names_by_pair[pair] = (str(row["left_name"] or ""), str(row["right_name"] or ""))
        albums_by_pair[pair].append(
            {
                "release_album_id": int(row["release_album_id"]),
                "album_name": row["album_name"],
                "role": row["role"],
            }
        )

    row_by_id = {int(row["id"]): row for row in artist_rows}
    similar_groups: list[dict[str, Any]] = []
    composite_group_members: dict[tuple[int, int, str], set[int]] = defaultdict(set)
    composite_group_albums: dict[tuple[int, int, str], dict[str, Any]] = {}
    for pair, albums in albums_by_pair.items():
        left_name, right_name = names_by_pair[pair]
        if _stylization_key(left_name) == _stylization_key(right_name):
            continue
        artist_ids = list(pair)
        composite_artist_ids = [
            artist_id
            for artist_id, name in zip(artist_ids, (left_name, right_name), strict=True)
            if _is_composite_credit_name(name)
        ]
        if composite_artist_ids:
            for composite_artist_id in composite_artist_ids:
                for album in albums:
                    key = (composite_artist_id, int(album["release_album_id"]), str(album["role"]))
                    composite_group_members[key].update(artist_ids)
                    composite_group_albums[key] = album
            continue
        similarity = _name_similarity(left_name, right_name)
        has_script_difference = _has_non_latin_text(left_name) or _has_non_latin_text(right_name)
        if similarity < MIN_SIMILAR_NAME_RATIO and not has_script_difference:
            continue
        source_maps_by_artist = _artist_source_maps(connection, artist_ids)
        album_counts = _link_counts(connection, "album_artist", artist_ids)
        track_counts = _link_counts(connection, "track_artist", artist_ids)
        artist_payloads = [
            _artist_candidate_payload(connection, row_by_id[artist_id], source_maps_by_artist, album_counts, track_counts)
            for artist_id in artist_ids
            if artist_id in row_by_id
        ]
        similar_groups.append(
            {
                "category": "similar_same_album",
                "repairable": False,
                "reason": "shared_album_with_name_similarity" if similarity >= MIN_SIMILAR_NAME_RATIO else "shared_album_with_non_latin_name",
                "name_similarity": round(similarity, 3),
                "shared_album_count": len(albums),
                "shared_albums": albums[:10],
                "artists": artist_payloads,
            }
        )
    composite_groups: list[dict[str, Any]] = []
    composite_artist_ids_with_same_album_context: set[int] = set()
    for key, artist_ids in composite_group_members.items():
        composite_artist_id, release_album_id, role = key
        composite_artist_ids_with_same_album_context.add(composite_artist_id)
        sorted_artist_ids = sorted(
            artist_ids,
            key=lambda artist_id: (artist_id == composite_artist_id, str(row_by_id[artist_id]["canonical_name"]).lower()),
        )
        source_maps_by_artist = _artist_source_maps(connection, sorted_artist_ids)
        album_counts = _link_counts(connection, "album_artist", sorted_artist_ids)
        track_counts = _link_counts(connection, "track_artist", sorted_artist_ids)
        cleanup_plan = _build_composite_credit_cleanup_plan(
            connection,
            composite_artist_id=composite_artist_id,
            release_album_id=release_album_id,
            role=role,
            artist_ids=artist_ids,
            row_by_id=row_by_id,
        )
        composite_groups.append(
            {
                "category": "composite_credit",
                "repairable": False,
                "reason": "comma_separated_history_credit_on_same_album",
                "shared_album_count": 1,
                "shared_albums": [composite_group_albums[key]],
                "composite_artist_id": composite_artist_id,
                "cleanup_plan": cleanup_plan,
                "artists": [
                    _artist_candidate_payload(connection, row_by_id[artist_id], source_maps_by_artist, album_counts, track_counts)
                    for artist_id in sorted_artist_ids
                    if artist_id in row_by_id
                ],
            }
        )
    composite_groups.extend(
        _build_matched_comma_credit_review_groups(
            connection,
            artist_rows=artist_rows,
            row_by_id=row_by_id,
            excluded_composite_artist_ids=composite_artist_ids_with_same_album_context,
        )
    )
    similar_groups.sort(key=lambda group: (-int(group["shared_album_count"]), -float(group["name_similarity"])))
    composite_groups.sort(key=lambda group: -int(group["shared_album_count"]))
    return similar_groups, composite_groups


def _build_matched_comma_credit_review_groups(
    connection: sqlite3.Connection,
    *,
    artist_rows: list[sqlite3.Row],
    row_by_id: dict[int, sqlite3.Row],
    excluded_composite_artist_ids: set[int],
) -> list[dict[str, Any]]:
    artist_ids_by_normalized_name: dict[str, list[int]] = defaultdict(list)
    for row in artist_rows:
        artist_id = int(row["id"])
        normalized_name = _normalize_name(str(row["canonical_name"] or row["sort_name"] or ""))
        if normalized_name:
            artist_ids_by_normalized_name[normalized_name].append(artist_id)

    groups: list[dict[str, Any]] = []
    for row in artist_rows:
        composite_artist_id = int(row["id"])
        if composite_artist_id in excluded_composite_artist_ids:
            continue
        display_name = str(row["canonical_name"] or "")
        parts = _split_composite_credit_parts(display_name)
        if len(parts) <= 1:
            continue
        if _provider_source_keys(_artist_source_maps(connection, [composite_artist_id]).get(composite_artist_id, [])):
            continue

        matched_artist_ids: set[int] = set()
        ambiguous = False
        for part in parts:
            matches = [
                artist_id
                for artist_id in artist_ids_by_normalized_name.get(part["normalized_name"], [])
                if artist_id != composite_artist_id
            ]
            if len(matches) != 1:
                ambiguous = True
                break
            matched_artist_ids.add(matches[0])
        if ambiguous or len(matched_artist_ids) != len(parts):
            continue

        group_artist_ids = sorted(
            [*matched_artist_ids, composite_artist_id],
            key=lambda artist_id: (artist_id == composite_artist_id, str(row_by_id[artist_id]["canonical_name"]).lower()),
        )
        source_maps_by_artist = _artist_source_maps(connection, group_artist_ids)
        album_counts = _link_counts(connection, "album_artist", group_artist_ids)
        track_counts = _link_counts(connection, "track_artist", group_artist_ids)
        cleanup_plan = _build_composite_credit_cleanup_plan(
            connection,
            composite_artist_id=composite_artist_id,
            release_album_id=None,
            role="primary",
            artist_ids=set(group_artist_ids),
            row_by_id=row_by_id,
        )
        groups.append(
            {
                "category": "composite_credit",
                "repairable": False,
                "reason": "comma_credit_parts_match_existing_artists_review_only",
                "shared_album_count": 0,
                "shared_albums": [],
                "composite_artist_id": composite_artist_id,
                "cleanup_plan": cleanup_plan,
                "artists": [
                    _artist_candidate_payload(connection, row_by_id[artist_id], source_maps_by_artist, album_counts, track_counts)
                    for artist_id in group_artist_ids
                    if artist_id in row_by_id
                ],
            }
        )
    return groups


def plan_duplicate_artist_repair(connection: sqlite3.Connection) -> dict[str, Any]:
    audit = _build_duplicate_artist_audit_with_connection(connection)
    safe_groups: list[dict[str, Any]] = []
    skipped_groups: list[dict[str, Any]] = []

    source_mappings_to_move: list[dict[str, Any]] = []
    source_mappings_to_delete: list[dict[str, Any]] = []
    album_links_to_move: list[dict[str, Any]] = []
    album_links_to_delete: list[dict[str, Any]] = []
    track_links_to_move: list[dict[str, Any]] = []
    track_links_to_delete: list[dict[str, Any]] = []
    artist_rows_to_delete: list[int] = []
    evidence_type_counts: dict[str, int] = defaultdict(int)

    for group in audit["groups"]:
        artists = group["artists"]
        provider_artists = [artist for artist in artists if artist["provider_backed"]]
        text_only_artists = [artist for artist in artists if artist["text_only"]]
        if len(provider_artists) != 1:
            skipped_groups.append(
                {
                    "normalized_name": group["normalized_name"],
                    "reason": "multiple_provider_backed_artists" if len(provider_artists) > 1 else "no_provider_backed_artist",
                    "artist_ids": [artist["artist_id"] for artist in artists],
                }
            )
            continue
        canonical_artist_id = int(provider_artists[0]["artist_id"])
        duplicate_artist_ids = [
            int(artist["artist_id"])
            for artist in text_only_artists
            if int(artist["artist_id"]) != canonical_artist_id
        ]
        unsafe_artist_ids = [
            int(artist["artist_id"])
            for artist in artists
            if int(artist["artist_id"]) != canonical_artist_id and int(artist["artist_id"]) not in duplicate_artist_ids
        ]
        if not duplicate_artist_ids or unsafe_artist_ids:
            skipped_groups.append(
                {
                    "normalized_name": group["normalized_name"],
                    "reason": "non_text_only_duplicate_artist",
                    "artist_ids": [artist["artist_id"] for artist in artists],
                    "unsafe_artist_ids": unsafe_artist_ids,
                }
            )
            continue
        if str(group.get("category")) not in SAFE_REPAIR_CATEGORIES:
            skipped_groups.append(
                {
                    "normalized_name": group["normalized_name"],
                    "reason": str(group.get("review_reason") or group.get("category") or "not_safe_for_auto_repair"),
                    "category": group.get("category"),
                    "artist_ids": [artist["artist_id"] for artist in artists],
                    "duplicate_artist_ids": duplicate_artist_ids,
                    "evidence_by_duplicate_artist_id": group.get("evidence_by_duplicate_artist_id", {}),
                }
            )
            continue

        safe_groups.append(
            {
                "normalized_name": group["normalized_name"],
                "category": group["category"],
                "canonical_artist_id": canonical_artist_id,
                "duplicate_artist_ids": duplicate_artist_ids,
                "evidence_by_duplicate_artist_id": group.get("evidence_by_duplicate_artist_id", {}),
                "evidence_types": group.get("evidence_types", []),
            }
        )
        for evidence_type in group.get("evidence_types", []):
            evidence_type_counts[str(evidence_type)] += 1

        for duplicate_artist_id in duplicate_artist_ids:
            source_rows = connection.execute(
                """
                SELECT id, source_artist_id
                FROM source_artist_map
                WHERE artist_id = ?
                ORDER BY id ASC
                """,
                (duplicate_artist_id,),
            ).fetchall()
            for row in source_rows:
                existing = connection.execute(
                    """
                    SELECT id
                    FROM source_artist_map
                    WHERE source_artist_id = ?
                      AND artist_id = ?
                    LIMIT 1
                    """,
                    (row["source_artist_id"], canonical_artist_id),
                ).fetchone()
                payload = {
                    "source_artist_map_id": int(row["id"]),
                    "source_artist_id": int(row["source_artist_id"]),
                    "from_artist_id": duplicate_artist_id,
                    "to_artist_id": canonical_artist_id,
                }
                if existing is not None:
                    source_mappings_to_delete.append({**payload, "existing_source_artist_map_id": int(existing["id"])})
                else:
                    source_mappings_to_move.append(payload)

            _plan_link_moves(
                connection,
                table="album_artist",
                owner_column="release_album_id",
                duplicate_artist_id=duplicate_artist_id,
                canonical_artist_id=canonical_artist_id,
                moves=album_links_to_move,
                deletes=album_links_to_delete,
            )
            _plan_link_moves(
                connection,
                table="track_artist",
                owner_column="release_track_id",
                duplicate_artist_id=duplicate_artist_id,
                canonical_artist_id=canonical_artist_id,
                moves=track_links_to_move,
                deletes=track_links_to_delete,
            )
            artist_rows_to_delete.append(duplicate_artist_id)

    return {
        "groups_found": audit["groups_found"],
        "safe_groups": safe_groups,
        "skipped_groups": skipped_groups,
        "source_mappings_to_move": source_mappings_to_move,
        "source_mappings_to_delete": source_mappings_to_delete,
        "album_links_to_move": album_links_to_move,
        "album_links_to_delete": album_links_to_delete,
        "track_links_to_move": track_links_to_move,
        "track_links_to_delete": track_links_to_delete,
        "artist_rows_to_delete": sorted(set(artist_rows_to_delete)),
        "evidence_type_counts": dict(sorted(evidence_type_counts.items())),
    }


def plan_composite_artist_credit_cleanup(connection: sqlite3.Connection) -> dict[str, Any]:
    audit = _build_duplicate_artist_audit_with_connection(connection)
    safe_groups: list[dict[str, Any]] = []
    skipped_groups: list[dict[str, Any]] = []
    album_links_to_delete: list[dict[str, Any]] = []
    track_links_to_delete: list[dict[str, Any]] = []
    artist_rows_to_delete: list[int] = []

    for group in audit["candidate_categories"]["composite_credit"]["groups"]:
        cleanup_plan = group.get("cleanup_plan") or {}
        composite_artist_id = int(group.get("composite_artist_id") or cleanup_plan.get("composite_artist_id") or 0)
        if not bool(cleanup_plan.get("ready_for_cleanup")):
            skipped_groups.append(
                {
                    "composite_artist_id": composite_artist_id,
                    "reason": group.get("reason") or "not_ready_for_cleanup",
                    "ready_for_cleanup": False,
                    "credit_parts": cleanup_plan.get("credit_parts", []),
                }
            )
            continue

        album_deletes = [
            {
                **item,
                "composite_artist_id": composite_artist_id,
                "composite_display_name": cleanup_plan.get("composite_display_name"),
            }
            for item in cleanup_plan.get("album_links_to_delete", [])
        ]
        track_deletes = [
            {
                **item,
                "composite_artist_id": composite_artist_id,
                "composite_display_name": cleanup_plan.get("composite_display_name"),
            }
            for item in cleanup_plan.get("track_links_to_delete", [])
        ]
        if not album_deletes and not track_deletes:
            skipped_groups.append(
                {
                    "composite_artist_id": composite_artist_id,
                    "reason": "no_composite_links_to_delete",
                    "ready_for_cleanup": True,
                    "credit_parts": cleanup_plan.get("credit_parts", []),
                }
            )
            continue

        safe_groups.append(
            {
                "composite_artist_id": composite_artist_id,
                "composite_display_name": cleanup_plan.get("composite_display_name"),
                "matched_artist_ids": cleanup_plan.get("matched_artist_ids", []),
                "credit_parts": cleanup_plan.get("credit_parts", []),
                "album_links_to_delete": album_deletes,
                "track_links_to_delete": track_deletes,
            }
        )
        album_links_to_delete.extend(album_deletes)
        track_links_to_delete.extend(track_deletes)
        artist_rows_to_delete.append(composite_artist_id)

    return {
        "groups_found": len(audit["candidate_categories"]["composite_credit"]["groups"]),
        "safe_groups": safe_groups,
        "skipped_groups": skipped_groups,
        "album_links_to_delete": album_links_to_delete,
        "track_links_to_delete": track_links_to_delete,
        "artist_rows_to_delete": sorted(set(artist_rows_to_delete)),
    }


def repair_composite_artist_credits(*, dry_run: bool = True) -> dict[str, Any]:
    with sqlite_connection(write=not dry_run, row_factory=sqlite3.Row) as connection:
        plan = plan_composite_artist_credit_cleanup(connection)
        if dry_run:
            return {"dry_run": True, **plan}

        for item in plan["track_links_to_delete"]:
            connection.execute("DELETE FROM track_artist WHERE id = ?", (item["link_id"],))
        for item in plan["album_links_to_delete"]:
            connection.execute("DELETE FROM album_artist WHERE id = ?", (item["link_id"],))

        deleted_artist_ids: list[int] = []
        skipped_artist_deletes: list[dict[str, Any]] = []
        for artist_id in plan["artist_rows_to_delete"]:
            references = _artist_reference_counts(connection, artist_id)
            if all(count == 0 for count in references.values()):
                connection.execute("DELETE FROM artist WHERE id = ?", (artist_id,))
                deleted_artist_ids.append(artist_id)
            else:
                skipped_artist_deletes.append({"artist_id": artist_id, "remaining_references": references})

        return {
            "dry_run": False,
            **plan,
            "artist_rows_deleted": deleted_artist_ids,
            "artist_row_deletes_skipped": skipped_artist_deletes,
        }


def _plan_link_moves(
    connection: sqlite3.Connection,
    *,
    table: str,
    owner_column: str,
    duplicate_artist_id: int,
    canonical_artist_id: int,
    moves: list[dict[str, Any]],
    deletes: list[dict[str, Any]],
) -> None:
    rows = connection.execute(
        f"""
        SELECT id, {owner_column}, COALESCE(role, 'primary') AS role
        FROM {table}
        WHERE artist_id = ?
        ORDER BY id ASC
        """,
        (duplicate_artist_id,),
    ).fetchall()
    for row in rows:
        existing = connection.execute(
            f"""
            SELECT id
            FROM {table}
            WHERE {owner_column} = ?
              AND artist_id = ?
              AND COALESCE(role, 'primary') = ?
            LIMIT 1
            """,
            (row[owner_column], canonical_artist_id, row["role"]),
        ).fetchone()
        payload = {
            "link_id": int(row["id"]),
            owner_column: int(row[owner_column]),
            "role": row["role"],
            "from_artist_id": duplicate_artist_id,
            "to_artist_id": canonical_artist_id,
        }
        if existing is not None:
            deletes.append({**payload, "existing_link_id": int(existing["id"])})
        else:
            moves.append(payload)


def repair_duplicate_artists(*, dry_run: bool = True) -> dict[str, Any]:
    with sqlite_connection(write=not dry_run, row_factory=sqlite3.Row) as connection:
        plan = plan_duplicate_artist_repair(connection)
        if dry_run:
            return {"dry_run": True, **plan}

        for item in plan["source_mappings_to_delete"]:
            connection.execute("DELETE FROM source_artist_map WHERE id = ?", (item["source_artist_map_id"],))
        for item in plan["album_links_to_delete"]:
            connection.execute("DELETE FROM album_artist WHERE id = ?", (item["link_id"],))
        for item in plan["track_links_to_delete"]:
            connection.execute("DELETE FROM track_artist WHERE id = ?", (item["link_id"],))

        for item in plan["source_mappings_to_move"]:
            connection.execute(
                """
                UPDATE source_artist_map
                SET
                  artist_id = ?,
                  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE id = ?
                """,
                (item["to_artist_id"], item["source_artist_map_id"]),
            )
        for item in plan["album_links_to_move"]:
            connection.execute(
                """
                UPDATE album_artist
                SET artist_id = ?
                WHERE id = ?
                """,
                (item["to_artist_id"], item["link_id"]),
            )
        for item in plan["track_links_to_move"]:
            connection.execute(
                """
                UPDATE track_artist
                SET artist_id = ?
                WHERE id = ?
                """,
                (item["to_artist_id"], item["link_id"]),
            )

        deleted_artist_ids: list[int] = []
        skipped_artist_deletes: list[dict[str, Any]] = []
        for artist_id in plan["artist_rows_to_delete"]:
            references = _artist_reference_counts(connection, artist_id)
            if all(count == 0 for count in references.values()):
                connection.execute("DELETE FROM artist WHERE id = ?", (artist_id,))
                deleted_artist_ids.append(artist_id)
            else:
                skipped_artist_deletes.append({"artist_id": artist_id, "remaining_references": references})

        return {
            "dry_run": False,
            **plan,
            "artist_rows_deleted": deleted_artist_ids,
            "artist_row_deletes_skipped": skipped_artist_deletes,
        }


def _artist_reference_counts(connection: sqlite3.Connection, artist_id: int) -> dict[str, int]:
    return {
        "source_artist_map": int(
            connection.execute("SELECT count(*) FROM source_artist_map WHERE artist_id = ?", (artist_id,)).fetchone()[0]
        ),
        "album_artist": int(
            connection.execute("SELECT count(*) FROM album_artist WHERE artist_id = ?", (artist_id,)).fetchone()[0]
        ),
        "track_artist": int(
            connection.execute("SELECT count(*) FROM track_artist WHERE artist_id = ?", (artist_id,)).fetchone()[0]
        ),
    }
