from __future__ import annotations

import re
import sqlite3
from typing import Any, Literal, TypedDict

from backend.app.db import sqlite_connection
from backend.app.track_variant_policy import TrackVariantComponent, interpret_track_variant_title


CandidateType = Literal["recording_track_candidate", "track_family_candidate"]
SafetyStatus = Literal["safe_candidate", "needs_review", "unsafe"]
RelationshipStrength = Literal["exact", "minor_technical", "probable_same_song", "review_required"]
EvidenceBucket = Literal[
    "same_isrc",
    "conflicting_isrc_but_compatible_metadata",
    "missing_isrc_but_compatible_metadata",
    "partial_isrc_match",
    "variant_flag_excluded",
    "metadata_review_required",
]

NEAR_DURATION_MS = 2_000
REVIEW_DURATION_MS = 10_000
RECORDING_VERSION_FAMILIES = {"remaster", "format", "packaging", "score_soundtrack", "featured_credit"}
FAMILY_VERSION_FAMILIES = {
    "live",
    "demo",
    "acoustic",
    "remix",
    "rework",
    "cover",
    "session",
    "recording_context",
    "performance_style",
}
RERECORDING_SEMANTIC_CATEGORIES = {"rerecorded_version", "recording_lineage_change"}
RADIO_EDIT_SEMANTIC_CATEGORIES = {"broadcast_length_or_content_edit"}


class RecordingTrackCandidateMember(TypedDict):
    release_track_id: int
    title: str
    artist: str
    album: str
    release_album_ids: list[int]
    spotify_album_ids: list[str]
    album_release_dates: list[str]
    album_types: list[str]
    source_track_ids: list[str]
    source_track_db_ids: list[int]
    source_track_uris: list[str]
    isrc: str | None
    isrc_values: list[str]
    duration_ms: int | None
    duration_values_ms: list[int]
    evidence: dict[str, Any]


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _split_aggregate(value: Any) -> list[str]:
    if value is None:
        return []
    return [item for item in (str(part).strip() for part in str(value).split("|")) if item]


def _split_int_aggregate(value: Any) -> list[int]:
    items: list[int] = []
    for part in _split_aggregate(value):
        try:
            items.append(int(part))
        except ValueError:
            continue
    return items


def _unique_values(values: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        clean = str(value or "").strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        unique.append(clean)
    return unique


def _unique_ints(values: list[int]) -> list[int]:
    unique: list[int] = []
    seen: set[int] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique.append(value)
    return unique


def _variant_components(title: str) -> tuple[TrackVariantComponent, ...]:
    return interpret_track_variant_title(title).components


def _base_title(title: str) -> str:
    interpretation = interpret_track_variant_title(title)
    return _normalize_text(interpretation.base_title_anchor or title)


def _component_labels(components: tuple[TrackVariantComponent, ...]) -> list[str]:
    return [component.normalized_label for component in components]


def _component_families(components: tuple[TrackVariantComponent, ...]) -> set[str]:
    return {component.family for component in components}


def _component_semantic_categories(components: tuple[TrackVariantComponent, ...]) -> set[str]:
    return {component.semantic_category for component in components}


def _album_context(album_names: list[str]) -> str:
    normalized = " | ".join(_normalize_text(album_name) for album_name in album_names)
    if "soundtrack" in normalized or "motion picture" in normalized or "original score" in normalized:
        return "soundtrack"
    if "compilation" in normalized or "collection" in normalized or "best of" in normalized or "greatest hits" in normalized:
        return "compilation"
    if re.search(r"\bsingle\b", normalized):
        return "single"
    if "remaster" in normalized or "remastered" in normalized or "reissue" in normalized:
        return "rerelease"
    return "album"


def _relationship_kind(
    *,
    components: tuple[TrackVariantComponent, ...],
    album_contexts: set[str],
    same_isrc: bool,
    duration_delta_ms: int | None,
) -> str:
    families = _component_families(components)
    semantics = _component_semantic_categories(components)
    if "remix" in families:
        return "remix"
    if "rework" in families:
        return "rework"
    if "live" in families:
        return "live"
    if "demo" in families:
        return "demo"
    if "acoustic" in families:
        return "acoustic"
    if "session" in families or "recording_context" in families:
        return "alternate_take"
    if RERECORDING_SEMANTIC_CATEGORIES & semantics:
        return "rerecording"
    if RADIO_EDIT_SEMANTIC_CATEGORIES & semantics:
        return "radio_edit"
    if "soundtrack" in album_contexts:
        return "soundtrack_appearance"
    if "compilation" in album_contexts:
        return "compilation_appearance"
    if "single" in album_contexts:
        return "single_release"
    if "remaster" in families:
        return "remaster"
    if "rerelease" in album_contexts or "packaging" in families:
        return "rerelease"
    if same_isrc:
        return "same_isrc"
    if duration_delta_ms is not None and duration_delta_ms <= NEAR_DURATION_MS:
        return "near_match"
    return "near_match"


def _relationship_strength(
    *,
    same_isrc: bool,
    duration_delta_ms: int | None,
    has_recording_variant: bool,
    needs_review: bool,
) -> RelationshipStrength:
    if needs_review:
        return "review_required"
    if same_isrc and duration_delta_ms is not None and duration_delta_ms <= NEAR_DURATION_MS:
        return "exact"
    if same_isrc:
        return "minor_technical" if has_recording_variant else "exact"
    if duration_delta_ms is not None and duration_delta_ms <= NEAR_DURATION_MS:
        return "probable_same_song"
    return "review_required"


def _member_metadata_score(member: RecordingTrackCandidateMember) -> int:
    score = 0
    if member["source_track_ids"]:
        score += 3
    if member["source_track_uris"]:
        score += 3
    if member["isrc"]:
        score += 2
    if member["duration_ms"] is not None:
        score += 1
    if member["album"]:
        score += 1
    return score


def _member_album_context(member: RecordingTrackCandidateMember) -> str:
    return _album_context(_split_aggregate(member["album"].replace(", ", "|")))


def _representative_member(members: list[RecordingTrackCandidateMember]) -> tuple[RecordingTrackCandidateMember, int | None, str]:
    def sort_key(member: RecordingTrackCandidateMember) -> tuple[int, int, int, int]:
        playable = 1 if member["source_track_uris"] or member["source_track_ids"] else 0
        context = _member_album_context(member)
        preferred_context = 1 if context in {"album", "single"} else 0
        return (
            -playable,
            -_member_metadata_score(member),
            -preferred_context,
            member["release_track_id"],
        )

    representative = sorted(members, key=sort_key)[0]
    source_track_id = representative["source_track_db_ids"][0] if representative["source_track_db_ids"] else None
    reasons: list[str] = []
    if representative["source_track_uris"] or representative["source_track_ids"]:
        reasons.append("playable/source-backed candidate")
    if representative["isrc"]:
        reasons.append("has ISRC evidence")
    if representative["album"]:
        reasons.append("has album context")
    if not reasons:
        reasons.append("deterministic lowest release_track_id fallback")
    return representative, source_track_id, "; ".join(reasons)


def classify_recording_track_candidate_group(
    members: list[RecordingTrackCandidateMember],
    *,
    candidate_key: str | None = None,
) -> dict[str, Any]:
    sorted_members = sorted(members, key=lambda item: item["release_track_id"])
    all_components: list[TrackVariantComponent] = []
    for member in sorted_members:
        all_components.extend(_variant_components(member["title"]))
    components = tuple(all_components)
    families = _component_families(components)
    semantics = _component_semantic_categories(components)

    isrcs = sorted({isrc for member in sorted_members for isrc in member.get("isrc_values", []) if isrc})
    durations = [
        duration
        for member in sorted_members
        for duration in member.get("duration_values_ms", [])
        if duration is not None
    ]
    duration_delta_ms = max(durations) - min(durations) if len(durations) > 1 else None
    normalized_titles = {_base_title(member["title"]) for member in sorted_members}
    artists = {_normalize_text(member["artist"]) for member in sorted_members if _normalize_text(member["artist"])}
    album_contexts = {_album_context(_split_aggregate(member["album"].replace(", ", "|"))) for member in sorted_members if member["album"]}
    album_count = len({album for member in sorted_members for album in _split_aggregate(member["album"].replace(", ", "|"))})

    all_members_have_isrc = all(bool(member.get("isrc_values")) for member in sorted_members)
    same_isrc = len(isrcs) == 1 and all_members_have_isrc
    near_duration = duration_delta_ms is not None and duration_delta_ms <= NEAR_DURATION_MS
    review_duration = duration_delta_ms is None or duration_delta_ms <= REVIEW_DURATION_MS
    same_base_title = len(normalized_titles) == 1
    same_artist = len(artists) <= 1
    has_family_variant = bool(FAMILY_VERSION_FAMILIES & families or RERECORDING_SEMANTIC_CATEGORIES & semantics)
    has_radio_edit = bool(RADIO_EDIT_SEMANTIC_CATEGORIES & semantics)
    has_recording_variant = bool(RECORDING_VERSION_FAMILIES & families or album_contexts & {"single", "compilation", "soundtrack", "rerelease"})
    has_any_isrc = bool(isrcs)
    has_partial_isrc = has_any_isrc and not all_members_have_isrc
    has_conflicting_isrc = len(isrcs) > 1
    compatible_metadata = same_base_title and same_artist and near_duration
    strong_recording_metadata = compatible_metadata and has_recording_variant

    why_grouped: list[str] = []
    why_review: list[str] = []
    if same_base_title:
        why_grouped.append("normalized base title matches")
    if same_artist:
        why_grouped.append("primary artist matches")
    if same_isrc:
        why_grouped.append("same ISRC")
    if near_duration:
        why_grouped.append(f"duration delta <= {NEAR_DURATION_MS}ms")
    if has_recording_variant:
        why_grouped.append("release/remaster/appearance evidence is compatible with recording-track grouping")
    if compatible_metadata and not same_isrc:
        why_grouped.append("compatible title, artist, and duration metadata")

    if not same_base_title:
        why_review.append("normalized base titles differ")
    if not same_artist:
        why_review.append("primary artists differ")
    if not isrcs:
        why_review.append("missing ISRC support")
    elif has_partial_isrc:
        why_review.append("partial ISRC support")
    elif not same_isrc:
        why_review.append("multiple ISRC values require review, but can be expected for remasters/reissues")
    if duration_delta_ms is None:
        why_review.append("missing duration comparison")
    elif not near_duration:
        why_review.append(f"duration delta is {duration_delta_ms}ms")
    if album_count > 2 and not (same_isrc or near_duration):
        why_review.append("same title appears across many albums without strong ISRC or duration support")

    if has_family_variant or has_radio_edit:
        candidate_type: CandidateType = "track_family_candidate"
        safety_status: SafetyStatus = "needs_review"
        evidence_bucket: EvidenceBucket = "variant_flag_excluded"
        if has_radio_edit:
            why_review.append("radio/edit variant should not silently collapse into recording_track")
        else:
            why_review.append("meaningful variant belongs at Track Family layer")
    else:
        candidate_type = "recording_track_candidate"
        if same_isrc:
            evidence_bucket = "same_isrc"
        elif has_conflicting_isrc and compatible_metadata:
            evidence_bucket = "conflicting_isrc_but_compatible_metadata"
        elif has_partial_isrc and compatible_metadata:
            evidence_bucket = "partial_isrc_match"
        elif not has_any_isrc and compatible_metadata:
            evidence_bucket = "missing_isrc_but_compatible_metadata"
        else:
            evidence_bucket = "metadata_review_required"
        if not same_base_title or not same_artist:
            safety_status = "needs_review" if same_isrc else "unsafe"
        elif same_isrc and review_duration:
            safety_status = "safe_candidate"
        elif has_conflicting_isrc and strong_recording_metadata:
            safety_status = "needs_review"
        elif has_partial_isrc and strong_recording_metadata:
            safety_status = "needs_review"
        elif near_duration and (has_recording_variant or len(sorted_members) == 2):
            safety_status = "safe_candidate"
        else:
            safety_status = "needs_review"

    relationship_kind = _relationship_kind(
        components=components,
        album_contexts=album_contexts,
        same_isrc=same_isrc,
        duration_delta_ms=duration_delta_ms,
    )
    relationship_strength = _relationship_strength(
        same_isrc=same_isrc,
        duration_delta_ms=duration_delta_ms,
        has_recording_variant=has_recording_variant,
        needs_review=safety_status != "safe_candidate",
    )
    confidence = 0.5
    if safety_status == "safe_candidate":
        confidence = 0.94 if same_isrc else 0.84
        if has_recording_variant and not same_isrc:
            confidence -= 0.04
    elif safety_status == "needs_review":
        if evidence_bucket == "conflicting_isrc_but_compatible_metadata":
            confidence = 0.8
        elif evidence_bucket == "partial_isrc_match":
            confidence = 0.78
        elif evidence_bucket == "missing_isrc_but_compatible_metadata":
            confidence = 0.76
        else:
            confidence = 0.68 if candidate_type == "recording_track_candidate" else 0.58
    elif safety_status == "unsafe":
        confidence = 0.2

    representative, representative_source_track_id, representative_reason = _representative_member(sorted_members)
    display_name = representative["title"] or sorted_members[0]["title"]

    return {
        "candidate_key": candidate_key or f"{next(iter(artists), 'unknown')}|{next(iter(normalized_titles), 'unknown')}",
        "display_name": display_name,
        "candidate_type": candidate_type,
        "safety_status": safety_status,
        "confidence": round(confidence, 2),
        "relationship_kind": relationship_kind,
        "relationship_strength": relationship_strength,
        "evidence_bucket": evidence_bucket,
        "representative": {
            "release_track_id": representative["release_track_id"],
            "source_track_id": representative_source_track_id,
            "reason": representative_reason,
        },
        "members": sorted_members,
        "why_grouped": why_grouped,
        "why_review": sorted(set(why_review)),
    }


def _candidate_member_from_row(row: sqlite3.Row) -> RecordingTrackCandidateMember:
    title = str(row["title"] or "")
    album_names = _split_aggregate(row["album_names"])
    release_album_ids = _split_int_aggregate(row["release_album_ids"])
    spotify_album_ids = _unique_values(_split_aggregate(row["spotify_album_ids"]))
    album_release_dates = _unique_values(_split_aggregate(row["album_release_dates"]))
    album_types = _unique_values(_split_aggregate(row["album_types"]))
    source_track_ids = _split_aggregate(row["source_track_ids"])
    isrcs = _unique_values([isrc.upper() for isrc in _split_aggregate(row["isrcs"])])
    duration_values = _split_int_aggregate(row["duration_values_ms"])
    release_duration_ms = int(row["duration_ms"]) if row["duration_ms"] is not None else None
    duration_ms = release_duration_ms if release_duration_ms is not None else (duration_values[0] if duration_values else None)
    components = _variant_components(title)
    return {
        "release_track_id": int(row["release_track_id"]),
        "title": title,
        "artist": str(row["artist"] or ""),
        "album": ", ".join(album_names),
        "release_album_ids": release_album_ids,
        "spotify_album_ids": spotify_album_ids,
        "album_release_dates": album_release_dates,
        "album_types": album_types,
        "source_track_ids": source_track_ids,
        "source_track_db_ids": _split_int_aggregate(row["source_track_db_ids"]),
        "source_track_uris": _split_aggregate(row["source_track_uris"]),
        "isrc": isrcs[0] if len(isrcs) == 1 else None,
        "isrc_values": isrcs,
        "duration_ms": duration_ms,
        "duration_values_ms": _unique_ints([duration for duration in ([release_duration_ms] if release_duration_ms is not None else []) + duration_values]),
        "evidence": {
            "normalized_title": _base_title(title),
            "version_tokens": _component_labels(components),
            "album_context": _album_context(album_names),
            "duration_delta_ms": 0,
        },
    }


def _candidate_source_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        WITH primary_artists AS (
          SELECT
            ordered.release_track_id,
            group_concat(ordered.artist_name, ' | ') AS artist_signature
          FROM (
            SELECT
              ta.release_track_id,
              a.canonical_name AS artist_name
            FROM track_artist ta
            JOIN artist a ON a.id = ta.artist_id
            WHERE ta.role = 'primary'
            ORDER BY ta.release_track_id, COALESCE(ta.billing_index, 999999), ta.id, a.canonical_name
          ) ordered
          GROUP BY ordered.release_track_id
        ),
        release_albums AS (
          SELECT
            ordered.release_track_id,
            group_concat(ordered.release_album_id, '|') AS release_album_ids,
            group_concat(ordered.spotify_album_id, '|') AS spotify_album_ids,
            group_concat(ordered.album_release_date, '|') AS album_release_dates,
            group_concat(ordered.album_type, '|') AS album_types,
            group_concat(ordered.album_name, '|') AS album_names
          FROM (
            SELECT
              at.release_track_id,
              at.release_album_id,
              ra.primary_name AS album_name,
              sam_source.external_id AS spotify_album_id,
              sac.release_date AS album_release_date,
              sac.album_type AS album_type
            FROM album_track at
            JOIN release_album ra ON ra.id = at.release_album_id
            LEFT JOIN source_album_map sam ON sam.release_album_id = ra.id AND sam.status = 'accepted'
            LEFT JOIN source_album sam_source ON sam_source.id = sam.source_album_id AND sam_source.source_name = 'spotify'
            LEFT JOIN spotify_album_catalog sac ON sac.spotify_album_id = sam_source.external_id
            ORDER BY at.release_track_id, ra.release_year, ra.id
          ) ordered
          GROUP BY ordered.release_track_id
        ),
        source_refs AS (
          SELECT
            ordered.release_track_id,
            group_concat(ordered.source_track_db_id, '|') AS source_track_db_ids,
            group_concat(ordered.spotify_track_id, '|') AS source_track_ids,
            group_concat(ordered.external_uri, '|') AS source_track_uris,
            group_concat(ordered.isrc, '|') AS isrcs,
            group_concat(ordered.duration_ms, '|') AS duration_values_ms,
            group_concat(ordered.catalog_album_id, '|') AS catalog_album_ids,
            group_concat(ordered.catalog_release_date, '|') AS catalog_release_dates,
            group_concat(ordered.catalog_album_type, '|') AS catalog_album_types
          FROM (
            SELECT
              stm.release_track_id,
              st.id AS source_track_db_id,
              st.external_id AS spotify_track_id,
              COALESCE(st.external_uri, CASE WHEN st.source_name = 'spotify' THEN 'spotify:track:' || st.external_id ELSE NULL END) AS external_uri,
              NULLIF(TRIM(COALESCE(st.isrc, json_extract(COALESCE(stc.raw_json, '{}'), '$.external_ids.isrc'))), '') AS isrc,
              stc.duration_ms AS duration_ms,
              stc.album_id AS catalog_album_id,
              stc_album.release_date AS catalog_release_date,
              stc_album.album_type AS catalog_album_type
            FROM source_track_map stm
            JOIN source_track st ON st.id = stm.source_track_id
            LEFT JOIN spotify_track_catalog stc ON stc.spotify_track_id = st.external_id
            LEFT JOIN spotify_album_catalog stc_album ON stc_album.spotify_album_id = stc.album_id
            WHERE stm.status = 'accepted'
              AND st.source_name IN ('spotify', 'spotify_uri')
            ORDER BY stm.release_track_id, st.id
          ) ordered
          GROUP BY ordered.release_track_id
        )
        SELECT
          rt.id AS release_track_id,
          rt.primary_name AS title,
          rt.normalized_name AS normalized_title,
          rt.duration_ms AS duration_ms,
          COALESCE(pa.artist_signature, '') AS artist,
          COALESCE(ral.release_album_ids, '') AS release_album_ids,
          COALESCE(ral.spotify_album_ids, sr.catalog_album_ids, '') AS spotify_album_ids,
          COALESCE(ral.album_release_dates, sr.catalog_release_dates, '') AS album_release_dates,
          COALESCE(ral.album_types, sr.catalog_album_types, '') AS album_types,
          COALESCE(ral.album_names, '') AS album_names,
          COALESCE(sr.source_track_db_ids, '') AS source_track_db_ids,
          COALESCE(sr.source_track_ids, '') AS source_track_ids,
          COALESCE(sr.source_track_uris, '') AS source_track_uris,
          COALESCE(sr.isrcs, '') AS isrcs,
          COALESCE(sr.duration_values_ms, '') AS duration_values_ms
        FROM release_track rt
        LEFT JOIN primary_artists pa ON pa.release_track_id = rt.id
        LEFT JOIN release_albums ral ON ral.release_track_id = rt.id
        LEFT JOIN source_refs sr ON sr.release_track_id = rt.id
        ORDER BY rt.id ASC
        """
    ).fetchall()


def _item_isrc_values(item: dict[str, Any]) -> set[str]:
    return {
        str(isrc)
        for member in item["members"]
        for isrc in member.get("isrc_values", [])
        if isrc
    }


def _item_has_same_isrc(item: dict[str, Any]) -> bool:
    members = item.get("members", [])
    return bool(members) and all(member.get("isrc_values") for member in members) and len(_item_isrc_values(item)) == 1


def _item_evidence_bucket(item: dict[str, Any]) -> str:
    bucket = item.get("evidence_bucket")
    return str(bucket) if bucket else "metadata_review_required"


def _item_duration_delta_ms(item: dict[str, Any]) -> int | None:
    durations = [
        int(duration)
        for member in item["members"]
        for duration in (member.get("duration_values_ms") or [])
        if duration is not None
    ]
    return max(durations) - min(durations) if len(durations) > 1 else None


def _candidate_items() -> list[dict[str, Any]]:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = _candidate_source_rows(connection)

    grouped: dict[str, list[RecordingTrackCandidateMember]] = {}
    isrc_grouped: dict[str, list[RecordingTrackCandidateMember]] = {}
    for row in rows:
        member = _candidate_member_from_row(row)
        base_title = member["evidence"]["normalized_title"]
        artist_key = _normalize_text(member["artist"])
        if not base_title or not artist_key:
            continue
        grouped.setdefault(f"{artist_key}|{base_title}", []).append(member)
        for isrc in member["isrc_values"]:
            isrc_grouped.setdefault(f"isrc:{isrc}", []).append(member)

    items: list[dict[str, Any]] = []
    seen_member_sets: set[tuple[int, ...]] = set()

    def add_candidate(key: str, members: list[RecordingTrackCandidateMember]) -> None:
        if len(members) < 2:
            return
        member_key = tuple(sorted(member["release_track_id"] for member in members))
        if member_key in seen_member_sets:
            return
        seen_member_sets.add(member_key)
        item = classify_recording_track_candidate_group(members, candidate_key=key)
        duration_delta_ms = _item_duration_delta_ms(item)
        for member in item["members"]:
            member["evidence"]["duration_delta_ms"] = duration_delta_ms
        items.append(item)

    for key, members in isrc_grouped.items():
        add_candidate(key, members)

    for key, members in grouped.items():
        add_candidate(key, members)

    items.sort(key=lambda item: (item["candidate_type"], item["safety_status"], item["candidate_key"]))
    return items


def _filtered_candidate_items(
    *,
    safety_status: str | None = None,
    candidate_type: str | None = None,
    relationship_kind: str | None = None,
    min_confidence: float | None = None,
    include_track_family_candidates: bool = True,
    same_isrc_only: bool = False,
    q: str | None = None,
    artist: str | None = None,
) -> list[dict[str, Any]]:
    items = _candidate_items()
    normalized_q = _normalize_text(q)
    normalized_artist = _normalize_text(artist)
    filtered: list[dict[str, Any]] = []
    for item in items:
        if safety_status and item["safety_status"] != safety_status:
            continue
        if candidate_type and item["candidate_type"] != candidate_type:
            continue
        if relationship_kind and item["relationship_kind"] != relationship_kind:
            continue
        if min_confidence is not None and float(item["confidence"]) < float(min_confidence):
            continue
        if not include_track_family_candidates and item["candidate_type"] == "track_family_candidate":
            continue
        if same_isrc_only and not _item_has_same_isrc(item):
            continue
        if normalized_q:
            title_haystack = " ".join(_normalize_text(member["title"]) for member in item["members"])
            if normalized_q not in _normalize_text(item["display_name"]) and normalized_q not in title_haystack:
                continue
        if normalized_artist:
            artist_haystack = " ".join(_normalize_text(member["artist"]) for member in item["members"])
            if normalized_artist not in artist_haystack:
                continue
        filtered.append(item)
    return filtered


def query_recording_track_candidates(
    *,
    limit: int = 50,
    offset: int = 0,
    safety_status: str | None = None,
    candidate_type: str | None = None,
    relationship_kind: str | None = None,
    min_confidence: float | None = None,
    include_track_family_candidates: bool = True,
    same_isrc_only: bool = False,
    q: str | None = None,
    artist: str | None = None,
) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 500))
    bounded_offset = max(0, int(offset))
    items = _filtered_candidate_items(
        safety_status=safety_status,
        candidate_type=candidate_type,
        relationship_kind=relationship_kind,
        min_confidence=min_confidence,
        include_track_family_candidates=include_track_family_candidates,
        same_isrc_only=same_isrc_only,
        q=q,
        artist=artist,
    )
    paged_items = items[bounded_offset : bounded_offset + bounded_limit]
    return {
        "items": paged_items,
        "limit": bounded_limit,
        "offset": bounded_offset,
        "total": len(items),
        "returned": len(paged_items),
        "has_more": bounded_offset + len(paged_items) < len(items),
        "source": {
            "kind": "sqlite",
            "uses_spotify_api": False,
            "mutates_identity": False,
        },
        "filters": {
            "safety_status": safety_status,
            "candidate_type": candidate_type,
            "relationship_kind": relationship_kind,
            "min_confidence": min_confidence,
            "include_track_family_candidates": include_track_family_candidates,
            "same_isrc_only": same_isrc_only,
            "q": q,
            "artist": artist,
        },
    }


def _count_by(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = str(item.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _metadata_availability_summary() -> dict[str, int]:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        source_tracks_with_isrc = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM source_track st
                LEFT JOIN spotify_track_catalog stc ON stc.spotify_track_id = st.external_id
                WHERE st.source_name = 'spotify'
                  AND NULLIF(TRIM(COALESCE(st.isrc, json_extract(COALESCE(stc.raw_json, '{}'), '$.external_ids.isrc'))), '') IS NOT NULL
                """
            ).fetchone()[0]
        )
        release_tracks_with_isrc = int(
            connection.execute(
                """
                SELECT COUNT(DISTINCT stm.release_track_id)
                FROM source_track_map stm
                JOIN source_track st ON st.id = stm.source_track_id
                LEFT JOIN spotify_track_catalog stc ON stc.spotify_track_id = st.external_id
                WHERE stm.status = 'accepted'
                  AND st.source_name = 'spotify'
                  AND NULLIF(TRIM(COALESCE(st.isrc, json_extract(COALESCE(stc.raw_json, '{}'), '$.external_ids.isrc'))), '') IS NOT NULL
                """
            ).fetchone()[0]
        )
        release_tracks_with_multiple_isrcs = int(
            connection.execute(
                """
                WITH mapped AS (
                  SELECT
                    stm.release_track_id,
                    NULLIF(TRIM(COALESCE(st.isrc, json_extract(COALESCE(stc.raw_json, '{}'), '$.external_ids.isrc'))), '') AS isrc
                  FROM source_track_map stm
                  JOIN source_track st ON st.id = stm.source_track_id
                  LEFT JOIN spotify_track_catalog stc ON stc.spotify_track_id = st.external_id
                  WHERE stm.status = 'accepted'
                    AND st.source_name = 'spotify'
                )
                SELECT COUNT(*)
                FROM (
                  SELECT release_track_id
                  FROM mapped
                  WHERE isrc IS NOT NULL
                  GROUP BY release_track_id
                  HAVING COUNT(DISTINCT isrc) > 1
                )
                """
            ).fetchone()[0]
        )
    return {
        "source_tracks_with_isrc_available": source_tracks_with_isrc,
        "release_tracks_with_isrc_available": release_tracks_with_isrc,
        "release_tracks_with_multiple_isrcs": release_tracks_with_multiple_isrcs,
    }


def _sample_items(items: list[dict[str, Any]], *, safety_status: str | None = None, candidate_type: str | None = None, limit: int = 5) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for item in items:
        if safety_status and item["safety_status"] != safety_status:
            continue
        if candidate_type and item["candidate_type"] != candidate_type:
            continue
        samples.append(item)
        if len(samples) >= limit:
            break
    return samples


def summarize_recording_track_candidates(*, sample_limit: int = 5) -> dict[str, Any]:
    items = _candidate_items()
    duration_over_near = 0
    same_isrc = 0
    missing_isrc = 0
    for item in items:
        isrcs = _item_isrc_values(item)
        if _item_has_same_isrc(item):
            same_isrc += 1
        if not isrcs:
            missing_isrc += 1
        duration_delta_ms = _item_duration_delta_ms(item)
        if duration_delta_ms is not None and duration_delta_ms > NEAR_DURATION_MS:
            duration_over_near += 1

    bounded_sample_limit = max(1, min(int(sample_limit), 20))
    same_isrc_items = [item for item in items if len(_item_isrc_values(item)) == 1]
    metadata_missing_items = [item for item in items if not _item_isrc_values(item)]
    top_review_reasons: dict[str, int] = {}
    for item in items:
        for reason in item.get("why_review", []):
            top_review_reasons[str(reason)] = top_review_reasons.get(str(reason), 0) + 1

    return {
        "total_candidate_groups": len(items),
        **_metadata_availability_summary(),
        "count_by_candidate_type": _count_by(items, "candidate_type"),
        "count_by_safety_status": _count_by(items, "safety_status"),
        "count_by_relationship_kind": _count_by(items, "relationship_kind"),
        "count_by_relationship_strength": _count_by(items, "relationship_strength"),
        "count_by_evidence_bucket": _count_by(items, "evidence_bucket"),
        "count_with_same_isrc_evidence": same_isrc,
        "count_with_missing_isrc": missing_isrc,
        "count_by_has_isrc_evidence": {
            "true": len(items) - missing_isrc,
            "false": missing_isrc,
        },
        "count_by_same_isrc": {
            "true": same_isrc,
            "false": len(items) - same_isrc,
        },
        "count_with_duration_delta_over_threshold": duration_over_near,
        "duration_delta_threshold_ms": NEAR_DURATION_MS,
        "top_needs_review_reasons": dict(sorted(top_review_reasons.items(), key=lambda item: (-item[1], item[0]))[:10]),
        "sample_safe_candidate_groups": _sample_items(items, safety_status="safe_candidate", limit=bounded_sample_limit),
        "sample_needs_review_groups": _sample_items(items, safety_status="needs_review", limit=bounded_sample_limit),
        "sample_track_family_candidate_groups": _sample_items(items, candidate_type="track_family_candidate", limit=bounded_sample_limit),
        "sample_same_isrc_groups": same_isrc_items[:bounded_sample_limit],
        "sample_metadata_missing_groups": metadata_missing_items[:bounded_sample_limit],
        "sample_conflicting_isrc_compatible_metadata_groups": [
            item for item in items if _item_evidence_bucket(item) == "conflicting_isrc_but_compatible_metadata"
        ][:bounded_sample_limit],
        "sample_missing_isrc_compatible_metadata_groups": [
            item for item in items if _item_evidence_bucket(item) == "missing_isrc_but_compatible_metadata"
        ][:bounded_sample_limit],
        "sample_partial_isrc_match_groups": [
            item for item in items if _item_evidence_bucket(item) == "partial_isrc_match"
        ][:bounded_sample_limit],
        "sample_variant_flag_excluded_groups": [
            item for item in items if _item_evidence_bucket(item) == "variant_flag_excluded"
        ][:bounded_sample_limit],
        "source": {
            "kind": "sqlite",
            "uses_spotify_api": False,
            "mutates_identity": False,
        },
    }
