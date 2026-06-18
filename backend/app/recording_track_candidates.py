from __future__ import annotations

import json
import re
import sqlite3
import time
from datetime import UTC, datetime
from typing import Any, Literal, TypedDict

from backend.app.db import (
    clear_generated_recording_track_cluster_dirty_with_connection,
    dirty_generated_recording_track_cluster_ids,
    get_sqlite_db_path,
    sqlite_connection,
)
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
    "instrumental",
    "remix",
    "rework",
    "cover",
    "session",
    "recording_context",
    "performance_style",
    "structural",
    "commentary",
    "wellness_frequency",
}

_CANDIDATE_ITEMS_CACHE_TTL_SECONDS = 60.0
_candidate_items_cache: dict[str, Any] = {
    "db_path": None,
    "db_inode": None,
    "cached_at": 0.0,
    "items": None,
}
RERECORDING_SEMANTIC_CATEGORIES = {"rerecorded_version", "recording_lineage_change"}
RADIO_EDIT_SEMANTIC_CATEGORIES = {"broadcast_length_or_content_edit"}
RECORDING_COMPATIBLE_SEMANTIC_CATEGORIES = {
    "mastering_or_reissue_label",
    "packaging_version",
    "content_or_format_version",
    "generic_originality_label",
    "format_or_presentation_change",
    "release_packaging",
    "credit_annotation_or_collab_delta",
    "inline_credit_annotation",
    "placement_or_context_label",
}
RECORDING_DISTINCT_SEMANTIC_CATEGORIES = {
    "arrangement_change",
    "arrangement_or_vocal_subtraction",
    "attributed_derived_version",
    "broadcast_length_or_content_edit",
    "recording_lineage_change",
    "alternate_take_or_arrangement",
    "dated_revision",
    "ambiguous_version_misc",
    "ambiguous_edit_misc",
    "mix_treatment",
    "ambiguous_mix_misc",
    "special_recording_context",
    "capture_context",
    "distinct_track_form",
    "spoken_context_track",
    "wellness_or_frequency_program",
}


class RecordingTrackCandidateMember(TypedDict):
    release_track_id: int
    title: str
    artist: str
    artists: list[dict[str, Any]]
    album: str
    release_album_ids: list[int]
    spotify_album_ids: list[str]
    album_image_urls: list[str]
    album_release_dates: list[str]
    album_types: list[str]
    source_track_ids: list[str]
    source_track_db_ids: list[int]
    source_track_uris: list[str]
    play_count: int
    first_played_at: str | None
    last_played_at: str | None
    isrc: str | None
    isrc_values: list[str]
    duration_ms: int | None
    duration_values_ms: list[int]
    evidence: dict[str, Any]


def _member_play_count(member: dict[str, Any]) -> int:
    try:
        return max(0, int(member.get("play_count") or 0))
    except (TypeError, ValueError):
        return 0


def _candidate_listen_counts(members: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "recording_total_play_count": sum(_member_play_count(member) for member in members),
        "recording_member_count": len(members),
    }


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _split_aggregate(value: Any) -> list[str]:
    if value is None:
        return []
    return [item for item in (str(part).strip() for part in str(value).split("|")) if item]


def _json_list(value: Any) -> list[Any]:
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _artist_entries_from_json(value: Any) -> list[dict[str, Any]]:
    artists: list[dict[str, Any]] = []
    for item in _json_list(value):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        artists.append(
            {
                "artist_id": str(item["artist_id"]) if item.get("artist_id") is not None else None,
                "name": name,
                "role": item.get("role"),
                "billing_index": item.get("billing_index"),
            }
        )
    return artists


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


def _unique_display_values(values: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        clean = str(value or "").strip()
        key = _normalize_text(clean)
        if not clean or not key or key in seen:
            continue
        seen.add(key)
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


def _component_signature(components: tuple[TrackVariantComponent, ...]) -> tuple[tuple[str, str, str], ...]:
    return tuple(sorted((component.family, component.semantic_category, component.normalized_label) for component in components))


def _component_is_recording_distinct(component: TrackVariantComponent) -> bool:
    if component.family in FAMILY_VERSION_FAMILIES:
        return True
    if component.semantic_category in RECORDING_DISTINCT_SEMANTIC_CATEGORIES:
        return True
    if component.groupable_by_default:
        return False
    return component.semantic_category not in RECORDING_COMPATIBLE_SEMANTIC_CATEGORIES


def _has_recording_distinct_variant_mismatch(members: list[RecordingTrackCandidateMember]) -> bool:
    signatures: set[tuple[tuple[str, str, str], ...]] = set()
    has_distinct_component = False
    for member in members:
        components = _variant_components(member["title"])
        signatures.add(_component_signature(components))
        if any(_component_is_recording_distinct(component) for component in components):
            has_distinct_component = True
    return has_distinct_component and len(signatures) > 1


def _member_has_family_component(member: RecordingTrackCandidateMember, family: str) -> bool:
    return any(component.family == family for component in _variant_components(member["title"]))


def _has_mixed_family_variant(members: list[RecordingTrackCandidateMember], family: str) -> bool:
    states = {_member_has_family_component(member, family) for member in members}
    return len(states) > 1


def _album_context(album_names: list[str]) -> str:
    normalized = " | ".join(_normalize_text(album_name) for album_name in album_names)
    if "soundtrack" in normalized or "motion picture" in normalized or "original score" in normalized:
        return "soundtrack"
    if (
        "compilation" in normalized
        or "collection" in normalized
        or "best of" in normalized
        or "greatest hits" in normalized
        or "very best" in normalized
        or "essential" in normalized
        or "anthology" in normalized
        or "golden age" in normalized
        or "soundway presents" in normalized
    ):
        return "compilation"
    if re.search(r"\bsingle\b", normalized):
        return "single"
    if (
        "remaster" in normalized
        or "remastered" in normalized
        or "reissue" in normalized
        or "expanded" in normalized
        or "deluxe" in normalized
        or "anniversary" in normalized
    ):
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
    if "instrumental" in families:
        return "instrumental"
    if "structural" in families:
        return "structural_segment"
    if "session" in families or "recording_context" in families:
        return "alternate_take"
    if "mix" in families:
        return "mix"
    if "version" in families and "attributed_derived_version" in semantics:
        return "derived_version"
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
    album_types = {_normalize_text(album_type) for album_type in member.get("album_types", [])}
    if "compilation" in album_types:
        return "compilation"
    if "single" in album_types:
        return "single"
    context = _album_context(_split_aggregate(member["album"].replace(", ", "|")))
    if context == "album" and _normalize_text(member["album"]) == _base_title(member["title"]):
        return "single"
    return context


def _has_duplicate_display_values(value: Any) -> bool:
    parts = [part.strip() for part in str(value or "").split(",") if part.strip()]
    normalized = [_normalize_text(part) for part in parts]
    normalized = [part for part in normalized if part]
    return len(normalized) > len(set(normalized))


def _generated_item_needs_display_refresh(item: dict[str, Any]) -> bool:
    for member in item.get("members") or []:
        if not isinstance(member, dict):
            continue
        if _has_duplicate_display_values(member.get("album")):
            return True
    return False


def _earliest_release_year(member: RecordingTrackCandidateMember) -> int | None:
    years: list[int] = []
    for raw_date in member.get("album_release_dates", []):
        match = re.match(r"^\s*(\d{4})", str(raw_date or ""))
        if not match:
            continue
        years.append(int(match.group(1)))
    return min(years) if years else None


def _representative_context_rank(context: str) -> int:
    ranks = {
        "album": 0,
        "rerelease": 1,
        "single": 2,
        "soundtrack": 3,
        "compilation": 4,
    }
    return ranks.get(context, 5)


def _representative_variant_rank(member: RecordingTrackCandidateMember) -> int:
    components = _variant_components(member["title"])
    if not components:
        return 0
    families = _component_families(components)
    if families <= {"content_rating"}:
        return 1
    if families & {"format", "remaster", "packaging"}:
        return 2
    return 3


def _representative_member(members: list[RecordingTrackCandidateMember]) -> tuple[RecordingTrackCandidateMember, int | None, str]:
    def sort_key(member: RecordingTrackCandidateMember) -> tuple[int, int, int, int, int, int]:
        playable = 1 if member["source_track_uris"] or member["source_track_ids"] else 0
        context = _member_album_context(member)
        release_year = _earliest_release_year(member)
        return (
            -playable,
            _representative_context_rank(context),
            _representative_variant_rank(member),
            9999 if release_year is None else release_year,
            -_member_metadata_score(member),
            member["release_track_id"],
        )

    representative = sorted(members, key=sort_key)[0]
    source_track_id = representative["source_track_db_ids"][0] if representative["source_track_db_ids"] else None
    reasons: list[str] = []
    if representative["source_track_uris"] or representative["source_track_ids"]:
        reasons.append("playable/source-backed candidate")
    context = _member_album_context(representative)
    if context == "album":
        reasons.append("preferred original album context")
    elif context == "single":
        reasons.append("single release preferred over compilation/soundtrack fallback")
    elif context == "rerelease":
        reasons.append("rerelease/remaster preferred over compilation fallback")
    elif context == "soundtrack":
        reasons.append("soundtrack fallback; no stronger album/single source-backed representative")
    elif context == "compilation":
        reasons.append("compilation fallback; no stronger source-backed representative")
    release_year = _earliest_release_year(representative)
    if release_year is not None:
        reasons.append(f"earliest release year {release_year}")
    if _representative_variant_rank(representative) == 0:
        reasons.append("clean base title preferred for display")
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
    has_recording_distinct_variant_mismatch = _has_recording_distinct_variant_mismatch(sorted_members)
    has_mixed_live_variant = _has_mixed_family_variant(sorted_members, "live")
    has_recording_variant = bool(RECORDING_VERSION_FAMILIES & families or album_contexts & {"single", "compilation", "soundtrack", "rerelease"})
    has_any_isrc = bool(isrcs)
    has_partial_isrc = has_any_isrc and not all_members_have_isrc
    has_conflicting_isrc = len(isrcs) > 1
    compatible_metadata = same_base_title and same_artist
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
        if near_duration:
            why_grouped.append("compatible title, artist, and duration metadata")
        else:
            why_grouped.append("compatible title and artist metadata")

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

    if has_mixed_live_variant:
        candidate_type = "track_family_candidate"
        safety_status = "needs_review"
        evidence_bucket = "variant_flag_excluded"
        why_review.append("live/studio variants belong at Track Family layer")
    elif has_recording_distinct_variant_mismatch:
        candidate_type: CandidateType = "track_family_candidate"
        safety_status: SafetyStatus = "needs_review"
        evidence_bucket: EvidenceBucket = "variant_flag_excluded"
        if RADIO_EDIT_SEMANTIC_CATEGORIES & semantics:
            why_review.append("radio/edit variant should not silently collapse into recording_track")
        else:
            why_review.append("recording-distinct variant labels belong at Track Family layer")
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
    for member in sorted_members:
        member["evidence"]["album_context"] = _member_album_context(member)

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
        "listen_counts": _candidate_listen_counts(sorted_members),
        "members": sorted_members,
        "why_grouped": why_grouped,
        "why_review": sorted(set(why_review)),
    }


def _candidate_member_from_row(row: sqlite3.Row) -> RecordingTrackCandidateMember:
    title = str(row["title"] or "")
    album_names = _unique_display_values(_split_aggregate(row["album_names"]))
    release_album_ids = _split_int_aggregate(row["release_album_ids"])
    spotify_album_ids = _unique_values(_split_aggregate(row["spotify_album_ids"]))
    album_image_urls = _unique_values(_split_aggregate(row["album_image_urls"]))
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
        "artists": _artist_entries_from_json(row["artists_json"]),
        "album": ", ".join(album_names),
        "release_album_ids": release_album_ids,
        "spotify_album_ids": spotify_album_ids,
        "album_image_urls": album_image_urls,
        "album_release_dates": album_release_dates,
        "album_types": album_types,
        "source_track_ids": source_track_ids,
        "source_track_db_ids": _split_int_aggregate(row["source_track_db_ids"]),
        "source_track_uris": _split_aggregate(row["source_track_uris"]),
        "play_count": int(row["play_count"] or 0),
        "first_played_at": str(row["first_played_at"]) if row["first_played_at"] else None,
        "last_played_at": str(row["last_played_at"]) if row["last_played_at"] else None,
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


def _candidate_source_rows(
    connection: sqlite3.Connection,
    *,
    release_track_ids: set[int] | None = None,
    artist_ids: set[int] | None = None,
) -> list[sqlite3.Row]:
    params: list[Any] = []
    where_parts: list[str] = []
    if release_track_ids is not None:
        target_ids = sorted({int(release_track_id) for release_track_id in release_track_ids if int(release_track_id) > 0})
        if not target_ids:
            return []
        where_parts.append(f"rt.id IN ({','.join('?' for _ in target_ids)})")
        params.extend(target_ids)
    if artist_ids is not None:
        target_artist_ids = sorted({int(artist_id) for artist_id in artist_ids if int(artist_id) > 0})
        if not target_artist_ids:
            return []
        where_parts.append(
            "EXISTS ("
            "SELECT 1 FROM track_artist scoped_ta "
            "WHERE scoped_ta.release_track_id = rt.id "
            "AND scoped_ta.role = 'primary' "
            f"AND scoped_ta.artist_id IN ({','.join('?' for _ in target_artist_ids)})"
            ")"
        )
        params.extend(target_artist_ids)
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    return connection.execute(
        f"""
        WITH primary_artists AS (
          SELECT
            ordered.release_track_id,
            group_concat(ordered.artist_name, ' | ') AS artist_signature,
            json_group_array(
              json_object(
                'artist_id', ordered.artist_id,
                'name', ordered.artist_name,
                'role', ordered.role,
                'billing_index', ordered.billing_index
              )
            ) AS artists_json
          FROM (
            SELECT
              ta.release_track_id,
              ta.artist_id,
              a.canonical_name AS artist_name,
              ta.role,
              ta.billing_index
            FROM track_artist ta
            JOIN artist a ON a.id = ta.artist_id
            WHERE ta.role = 'primary'
            ORDER BY ta.release_track_id, COALESCE(ta.billing_index, 999999), ta.id, a.canonical_name
          ) ordered
          GROUP BY ordered.release_track_id
        ),
        unique_catalog_albums_by_name AS (
          SELECT
            lower(trim(name)) AS normalized_name,
            max(spotify_album_id) AS spotify_album_id
          FROM spotify_album_catalog
          WHERE NULLIF(trim(name), '') IS NOT NULL
          GROUP BY lower(trim(name))
          HAVING count(*) = 1
        ),
        release_albums AS (
          SELECT
            ordered.release_track_id,
            group_concat(ordered.release_album_id, '|') AS release_album_ids,
            group_concat(ordered.spotify_album_id, '|') AS spotify_album_ids,
            group_concat(ordered.album_image_url, '|') AS album_image_urls,
            group_concat(ordered.album_release_date, '|') AS album_release_dates,
            group_concat(ordered.album_type, '|') AS album_types,
            group_concat(ordered.album_name, '|') AS album_names
          FROM (
            SELECT
              at.release_track_id,
              at.release_album_id,
              ra.primary_name AS album_name,
              COALESCE(sam_source.external_id, name_catalog.spotify_album_id) AS spotify_album_id,
              json_extract(COALESCE(sac.images_json, name_catalog.images_json), '$[0].url') AS album_image_url,
              COALESCE(sac.release_date, name_catalog.release_date) AS album_release_date,
              COALESCE(sac.album_type, name_catalog.album_type) AS album_type
            FROM album_track at
            JOIN release_album ra ON ra.id = at.release_album_id
            LEFT JOIN source_album_map sam ON sam.release_album_id = ra.id AND sam.status = 'accepted'
            LEFT JOIN source_album sam_source ON sam_source.id = sam.source_album_id AND sam_source.source_name = 'spotify'
            LEFT JOIN spotify_album_catalog sac ON sac.spotify_album_id = sam_source.external_id
            LEFT JOIN unique_catalog_albums_by_name unique_name
              ON unique_name.normalized_name = lower(trim(ra.primary_name))
            LEFT JOIN spotify_album_catalog name_catalog
              ON name_catalog.spotify_album_id = unique_name.spotify_album_id
            ORDER BY at.release_track_id, ra.release_year, ra.id
          ) ordered
          GROUP BY ordered.release_track_id
        ),
        source_play_counts AS (
          SELECT
            spotify_track_id,
            play_count,
            first_played_at,
            last_played_at
          FROM source_track_play_count_cache
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
            group_concat(ordered.catalog_album_image_url, '|') AS catalog_album_image_urls,
            group_concat(ordered.catalog_release_date, '|') AS catalog_release_dates,
            group_concat(ordered.catalog_album_type, '|') AS catalog_album_types,
            sum(ordered.play_count) AS play_count,
            min(ordered.first_played_at) AS first_played_at,
            max(ordered.last_played_at) AS last_played_at
          FROM (
            SELECT
              stm.release_track_id,
              st.id AS source_track_db_id,
              CASE
                WHEN st.source_name = 'spotify' THEN st.external_id
                WHEN st.source_name = 'spotify_uri' THEN replace(st.external_id, 'spotify:track:', '')
                WHEN st.external_uri LIKE 'spotify:track:%' THEN replace(st.external_uri, 'spotify:track:', '')
                ELSE st.external_id
              END AS spotify_track_id,
              COALESCE(
                st.external_uri,
                CASE
                  WHEN st.source_name = 'spotify' THEN 'spotify:track:' || st.external_id
                  WHEN st.source_name = 'spotify_uri' THEN st.external_id
                  ELSE NULL
                END
              ) AS external_uri,
              NULLIF(TRIM(COALESCE(
                st.isrc,
                json_extract(COALESCE(stc.raw_json, '{{}}'), '$.external_ids.isrc'),
                CASE
                  WHEN json_valid(st.raw_payload_json)
                  THEN json_extract(st.raw_payload_json, '$.track.external_ids.isrc')
                  ELSE NULL
                END
              )), '') AS isrc,
              COALESCE(
                stc.duration_ms,
                CASE
                  WHEN json_valid(st.raw_payload_json)
                  THEN json_extract(st.raw_payload_json, '$.track.duration_ms')
                  ELSE NULL
                END
              ) AS duration_ms,
              COALESCE(
                stc.album_id,
                CASE
                  WHEN json_valid(st.raw_payload_json)
                  THEN json_extract(st.raw_payload_json, '$.track.album.id')
                  ELSE NULL
                END
              ) AS catalog_album_id,
              COALESCE(
                json_extract(stc_album.images_json, '$[0].url'),
                CASE
                  WHEN json_valid(st.raw_payload_json)
                  THEN json_extract(st.raw_payload_json, '$.track.album.images[0].url')
                  ELSE NULL
                END
              ) AS catalog_album_image_url,
              COALESCE(
                stc_album.release_date,
                CASE
                  WHEN json_valid(st.raw_payload_json)
                  THEN json_extract(st.raw_payload_json, '$.track.album.release_date')
                  ELSE NULL
                END
              ) AS catalog_release_date,
              COALESCE(
                stc_album.album_type,
                CASE
                  WHEN json_valid(st.raw_payload_json)
                  THEN json_extract(st.raw_payload_json, '$.track.album.album_type')
                  ELSE NULL
                END
              ) AS catalog_album_type,
              COALESCE(spc.play_count, 0) AS play_count,
              spc.first_played_at AS first_played_at,
              spc.last_played_at AS last_played_at
            FROM source_track_map stm
            JOIN source_track st ON st.id = stm.source_track_id
            LEFT JOIN spotify_track_catalog stc ON stc.spotify_track_id = CASE
              WHEN st.source_name = 'spotify' THEN st.external_id
              WHEN st.source_name = 'spotify_uri' THEN replace(st.external_id, 'spotify:track:', '')
              WHEN st.external_uri LIKE 'spotify:track:%' THEN replace(st.external_uri, 'spotify:track:', '')
              ELSE st.external_id
            END
            LEFT JOIN spotify_album_catalog stc_album ON stc_album.spotify_album_id = COALESCE(
              stc.album_id,
              CASE
                WHEN json_valid(st.raw_payload_json)
                THEN json_extract(st.raw_payload_json, '$.track.album.id')
                ELSE NULL
              END
            )
            LEFT JOIN source_play_counts spc ON spc.spotify_track_id = CASE
              WHEN st.source_name = 'spotify' THEN st.external_id
              WHEN st.source_name = 'spotify_uri' THEN replace(st.external_id, 'spotify:track:', '')
              WHEN st.external_uri LIKE 'spotify:track:%' THEN replace(st.external_uri, 'spotify:track:', '')
              ELSE st.external_id
            END
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
          COALESCE(pa.artists_json, '[]') AS artists_json,
          COALESCE(ral.release_album_ids, '') AS release_album_ids,
          COALESCE(ral.spotify_album_ids, sr.catalog_album_ids, '') AS spotify_album_ids,
          COALESCE(ral.album_image_urls, sr.catalog_album_image_urls, '') AS album_image_urls,
          COALESCE(ral.album_release_dates, sr.catalog_release_dates, '') AS album_release_dates,
          COALESCE(ral.album_types, sr.catalog_album_types, '') AS album_types,
          COALESCE(ral.album_names, '') AS album_names,
          COALESCE(sr.source_track_db_ids, '') AS source_track_db_ids,
          COALESCE(sr.source_track_ids, '') AS source_track_ids,
          COALESCE(sr.source_track_uris, '') AS source_track_uris,
          COALESCE(sr.play_count, 0) AS play_count,
          sr.first_played_at AS first_played_at,
          sr.last_played_at AS last_played_at,
          COALESCE(sr.isrcs, '') AS isrcs,
          COALESCE(sr.duration_values_ms, '') AS duration_values_ms
        FROM release_track rt
        LEFT JOIN primary_artists pa ON pa.release_track_id = rt.id
        LEFT JOIN release_albums ral ON ral.release_track_id = rt.id
        LEFT JOIN source_refs sr ON sr.release_track_id = rt.id
        {where_sql}
        ORDER BY rt.id ASC
        """,
        tuple(params),
    ).fetchall()


def _hydrate_candidate_items_with_current_member_metadata(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    release_track_ids = {
        int(member["release_track_id"])
        for item in items
        for member in item.get("members", [])
        if isinstance(member, dict) and isinstance(member.get("release_track_id"), int)
    }
    if not release_track_ids:
        return items
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        current_members = {
            member["release_track_id"]: member
            for member in (_candidate_member_from_row(row) for row in _candidate_source_rows(connection, release_track_ids=release_track_ids))
        }
    display_fields = (
        "title",
        "artist",
        "artists",
        "album",
        "release_album_ids",
        "spotify_album_ids",
        "album_image_urls",
        "album_release_dates",
        "album_types",
        "source_track_ids",
        "source_track_db_ids",
        "source_track_uris",
        "play_count",
        "first_played_at",
        "last_played_at",
        "isrc",
        "isrc_values",
        "duration_ms",
        "duration_values_ms",
    )
    for item in items:
        for member in item.get("members", []):
            if not isinstance(member, dict):
                continue
            current_member = current_members.get(int(member.get("release_track_id") or 0))
            if not current_member:
                continue
            for field in display_fields:
                member[field] = current_member[field]
        members = item.get("members", [])
        if isinstance(members, list):
            item["listen_counts"] = _candidate_listen_counts([member for member in members if isinstance(member, dict)])
    return items


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


def _build_candidate_items_from_rows(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    grouped: dict[str, list[RecordingTrackCandidateMember]] = {}
    isrc_grouped: dict[str, list[RecordingTrackCandidateMember]] = {}
    base_title_grouped: dict[str, list[RecordingTrackCandidateMember]] = {}
    for row in rows:
        member = _candidate_member_from_row(row)
        base_title = member["evidence"]["normalized_title"]
        artist_key = _normalize_text(member["artist"])
        if not base_title or not artist_key:
            continue
        grouped.setdefault(f"{artist_key}|{base_title}", []).append(member)
        base_title_grouped.setdefault(base_title, []).append(member)
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

    for base_title, members in base_title_grouped.items():
        remaining = list(members)
        while remaining:
            component = [remaining.pop(0)]
            component_artist_ids = {
                str(artist.get("artist_id") or "")
                for artist in component[0].get("artists", [])
                if artist.get("artist_id")
            }
            component_artist_names = {
                _normalize_text(artist.get("name"))
                for artist in component[0].get("artists", [])
                if _normalize_text(artist.get("name"))
            }
            changed = True
            while changed:
                changed = False
                for member in list(remaining):
                    member_artist_ids = {
                        str(artist.get("artist_id") or "")
                        for artist in member.get("artists", [])
                        if artist.get("artist_id")
                    }
                    member_artist_names = {
                        _normalize_text(artist.get("name"))
                        for artist in member.get("artists", [])
                        if _normalize_text(artist.get("name"))
                    }
                    if not (component_artist_ids & member_artist_ids or component_artist_names & member_artist_names):
                        continue
                    remaining.remove(member)
                    component.append(member)
                    component_artist_ids.update(member_artist_ids)
                    component_artist_names.update(member_artist_names)
                    changed = True
            artist_signatures = {_normalize_text(member["artist"]) for member in component}
            variant_signatures = {_component_signature(_variant_components(member["title"])) for member in component}
            if len(artist_signatures) > 1 and len(variant_signatures) > 1:
                family_key = f"family:{base_title}:{'|'.join(sorted(component_artist_names))}"
                add_candidate(family_key, component)

    items.sort(key=lambda item: (item["candidate_type"], item["safety_status"], item["candidate_key"]))
    return items


def _build_candidate_items() -> list[dict[str, Any]]:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        rows = _candidate_source_rows(connection)
    return _build_candidate_items_from_rows(rows)


def _candidate_items() -> list[dict[str, Any]]:
    db_path = get_sqlite_db_path()
    try:
        db_inode = db_path.stat().st_ino
    except OSError:
        db_inode = None
    now = time.monotonic()
    cached_items = _candidate_items_cache.get("items")
    if (
        cached_items is not None
        and _candidate_items_cache.get("db_path") == str(db_path)
        and _candidate_items_cache.get("db_inode") == db_inode
        and now - float(_candidate_items_cache.get("cached_at") or 0.0) < _CANDIDATE_ITEMS_CACHE_TTL_SECONDS
    ):
        return cached_items

    items = _build_candidate_items()
    _candidate_items_cache.update(
        {
            "db_path": str(db_path),
            "db_inode": db_inode,
            "cached_at": now,
            "items": items,
        }
    )
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


def _generated_cluster_tables_available(connection: sqlite3.Connection) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
          AND name = 'generated_recording_track_cluster'
        LIMIT 1
        """
    ).fetchone()
    return row is not None


def _generated_cluster_count(connection: sqlite3.Connection) -> int:
    if not _generated_cluster_tables_available(connection):
        return 0
    return int(connection.execute("SELECT count(*) FROM generated_recording_track_cluster").fetchone()[0])


def _invalidate_candidate_items_cache() -> None:
    _candidate_items_cache.update(
        {
            "db_path": None,
            "db_inode": None,
            "cached_at": 0.0,
            "items": None,
        }
    )


def _insert_generated_candidate_item(
    connection: sqlite3.Connection,
    *,
    item: dict[str, Any],
    generated_at: str,
) -> int:
    members = item.get("members") or []
    representative = item.get("representative") or {}
    cluster_id = int(
        connection.execute(
            """
            INSERT INTO generated_recording_track_cluster (
              candidate_key,
              candidate_type,
              safety_status,
              relationship_kind,
              relationship_strength,
              evidence_bucket,
              confidence,
              representative_release_track_id,
              representative_reason,
              member_count,
              candidate_snapshot_json,
              generated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(item.get("candidate_key") or ""),
                str(item.get("candidate_type") or ""),
                str(item.get("safety_status") or ""),
                str(item.get("relationship_kind") or ""),
                str(item.get("relationship_strength") or ""),
                item.get("evidence_bucket"),
                float(item.get("confidence") or 0.0),
                int(representative["release_track_id"]) if representative.get("release_track_id") is not None else None,
                representative.get("reason"),
                len(members),
                json.dumps(item, sort_keys=True),
                generated_at,
            ),
        ).lastrowid
    )
    for index, member in enumerate(members):
        release_track_id = int(member["release_track_id"])
        connection.execute(
            """
            INSERT INTO generated_recording_track_cluster_member (
              cluster_id,
              release_track_id,
              member_index,
              is_representative
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                cluster_id,
                release_track_id,
                index,
                1 if representative.get("release_track_id") == release_track_id else 0,
            ),
        )
    return len(members)


def rebuild_generated_recording_track_clusters() -> dict[str, Any]:
    items = _candidate_items()
    generated_at = datetime.now(UTC).isoformat()
    with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
        if not _generated_cluster_tables_available(connection):
            return {"rebuilt": False, "cluster_count": 0, "member_count": 0, "reason": "missing_tables"}
        connection.execute("DELETE FROM generated_recording_track_cluster_member")
        connection.execute("DELETE FROM generated_recording_track_cluster")
        member_count = 0
        for item in items:
            member_count += _insert_generated_candidate_item(connection, item=item, generated_at=generated_at)
        clear_generated_recording_track_cluster_dirty_with_connection(
            connection,
            [
                int(member["release_track_id"])
                for item in items
                for member in (item.get("members") or [])
                if int(member.get("release_track_id") or 0) > 0
            ],
        )
    _invalidate_candidate_items_cache()
    return {"rebuilt": True, "cluster_count": len(items), "member_count": member_count, "generated_at": generated_at}


def refresh_generated_recording_track_clusters_for_release_tracks(
    release_track_ids: list[int] | set[int] | tuple[int, ...],
) -> dict[str, Any]:
    target_ids = sorted({int(release_track_id) for release_track_id in release_track_ids if int(release_track_id) > 0})
    if not target_ids:
        return {"refreshed": False, "cluster_count": 0, "member_count": 0, "reason": "no_release_track_ids"}

    generated_at = datetime.now(UTC).isoformat()
    with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
        if not _generated_cluster_tables_available(connection):
            return {"refreshed": False, "cluster_count": 0, "member_count": 0, "reason": "missing_tables"}

        target_rows = _candidate_source_rows(connection, release_track_ids=set(target_ids))
        artist_ids = {
            int(artist["artist_id"])
            for row in target_rows
            for artist in _artist_entries_from_json(row["artists_json"])
            if str(artist.get("artist_id") or "").isdigit()
        }
        if not artist_ids:
            clear_generated_recording_track_cluster_dirty_with_connection(connection, target_ids)
            return {"refreshed": True, "cluster_count": 0, "member_count": 0, "generated_at": generated_at}

        rows = _candidate_source_rows(connection, artist_ids=artist_ids)
        scoped_release_track_ids = {
            int(row["release_track_id"])
            for row in rows
            if int(row["release_track_id"] or 0) > 0
        }
        items = _build_candidate_items_from_rows(rows)
        candidate_keys = [str(item.get("candidate_key") or "") for item in items if str(item.get("candidate_key") or "")]

        delete_cluster_ids: set[int] = set()
        if scoped_release_track_ids:
            placeholders = ",".join("?" for _ in scoped_release_track_ids)
            delete_cluster_ids.update(
                int(row["cluster_id"])
                for row in connection.execute(
                    f"""
                    SELECT DISTINCT cluster_id
                    FROM generated_recording_track_cluster_member
                    WHERE release_track_id IN ({placeholders})
                    """,
                    tuple(sorted(scoped_release_track_ids)),
                ).fetchall()
            )
        if candidate_keys:
            placeholders = ",".join("?" for _ in candidate_keys)
            delete_cluster_ids.update(
                int(row["id"])
                for row in connection.execute(
                    f"""
                    SELECT id
                    FROM generated_recording_track_cluster
                    WHERE candidate_key IN ({placeholders})
                    """,
                    tuple(candidate_keys),
                ).fetchall()
            )

        if delete_cluster_ids:
            placeholders = ",".join("?" for _ in delete_cluster_ids)
            params = tuple(sorted(delete_cluster_ids))
            connection.execute(
                f"DELETE FROM generated_recording_track_cluster_member WHERE cluster_id IN ({placeholders})",
                params,
            )
            connection.execute(
                f"DELETE FROM generated_recording_track_cluster WHERE id IN ({placeholders})",
                params,
            )

        member_count = 0
        for item in items:
            member_count += _insert_generated_candidate_item(connection, item=item, generated_at=generated_at)
        clear_generated_recording_track_cluster_dirty_with_connection(connection, scoped_release_track_ids | set(target_ids))

    _invalidate_candidate_items_cache()
    return {
        "refreshed": True,
        "cluster_count": len(items),
        "member_count": member_count,
        "release_track_count": len(scoped_release_track_ids),
        "generated_at": generated_at,
    }


def drain_generated_recording_track_cluster_dirty(*, limit: int = 50) -> dict[str, Any]:
    dirty_ids = dirty_generated_recording_track_cluster_ids(limit=max(1, int(limit)))
    if not dirty_ids:
        return {"refreshed": False, "dirty_count": 0}
    result = refresh_generated_recording_track_clusters_for_release_tracks(dirty_ids)
    result["dirty_count"] = len(dirty_ids)
    return result


def _ensure_generated_recording_track_clusters() -> None:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        if _generated_cluster_count(connection) > 0:
            return
    rebuild_generated_recording_track_clusters()


def _generated_candidate_items_for_release_track(release_track_id: int) -> list[dict[str, Any]]:
    _ensure_generated_recording_track_clusters()
    dirty_ids = dirty_generated_recording_track_cluster_ids([release_track_id], limit=1)
    if dirty_ids:
        refresh_generated_recording_track_clusters_for_release_tracks(dirty_ids)
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        if not _generated_cluster_tables_available(connection):
            return []
        rows = connection.execute(
            """
            SELECT c.candidate_snapshot_json
            FROM generated_recording_track_cluster_member m
            JOIN generated_recording_track_cluster c
              ON c.id = m.cluster_id
            WHERE m.release_track_id = ?
            """,
            (release_track_id,),
        ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        try:
            value = json.loads(str(row["candidate_snapshot_json"] or "{}"))
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            items.append(value)
    items = _hydrate_candidate_items_with_current_member_metadata(items)
    if any(_generated_item_needs_display_refresh(item) for item in items):
        refresh_generated_recording_track_clusters_for_release_tracks([release_track_id])
        with sqlite_connection(row_factory=sqlite3.Row) as connection:
            rows = connection.execute(
                """
                SELECT c.candidate_snapshot_json
                FROM generated_recording_track_cluster_member m
                JOIN generated_recording_track_cluster c
                  ON c.id = m.cluster_id
                WHERE m.release_track_id = ?
                """,
                (release_track_id,),
            ).fetchall()
        items = []
        for row in rows:
            try:
                value = json.loads(str(row["candidate_snapshot_json"] or "{}"))
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                items.append(value)
        items = _hydrate_candidate_items_with_current_member_metadata(items)
    return items


def get_recording_track_candidate_for_release_track(release_track_id: int) -> dict[str, Any] | None:
    target_id = int(release_track_id)
    if target_id <= 0:
        return None
    matches = get_recording_track_candidates_for_release_track(target_id)
    if not matches:
        return None
    return matches[0]


def get_recording_track_candidates_for_release_track(release_track_id: int) -> list[dict[str, Any]]:
    target_id = int(release_track_id)
    if target_id <= 0:
        return []
    matches = _generated_candidate_items_for_release_track(target_id)
    if not matches:
        matches = [
            item
            for item in _candidate_items()
            if any(int(member["release_track_id"]) == target_id for member in item["members"])
        ]
    if not matches:
        return []
    return sorted(
        matches,
        key=lambda item: (
            0 if item["candidate_type"] == "recording_track_candidate" else 1,
            0 if int(item["representative"]["release_track_id"] or 0) == target_id else 1,
            -float(item["confidence"]),
            item["candidate_key"],
        ),
    )


def candidate_cluster_metadata_for_release_track_ids(release_track_ids: list[int]) -> dict[int, dict[str, Any]]:
    target_ids = {int(release_track_id) for release_track_id in release_track_ids if int(release_track_id) > 0}
    if not target_ids:
        return {}
    _ensure_generated_recording_track_clusters()
    dirty_ids = dirty_generated_recording_track_cluster_ids(target_ids, limit=len(target_ids))
    if dirty_ids:
        refresh_generated_recording_track_clusters_for_release_tracks(dirty_ids)
    placeholders = ",".join("?" for _ in target_ids)
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        if _generated_cluster_tables_available(connection):
            rows = connection.execute(
                f"""
                SELECT
                  m.release_track_id,
                  c.candidate_type,
                  c.relationship_kind,
                  max(c.member_count) AS cluster_member_count
                FROM generated_recording_track_cluster_member m
                JOIN generated_recording_track_cluster c
                  ON c.id = m.cluster_id
                WHERE m.release_track_id IN ({placeholders})
                  AND c.candidate_type IN ('recording_track_candidate', 'track_family_candidate')
                GROUP BY m.release_track_id, c.candidate_type, c.relationship_kind
                """,
                tuple(sorted(target_ids)),
            ).fetchall()
            metadata: dict[int, dict[str, Any]] = {}
            for row in rows:
                release_track_id = int(row["release_track_id"])
                cluster_member_count = int(row["cluster_member_count"] or 0)
                candidate_type = str(row["candidate_type"] or "")
                current = metadata.get(release_track_id)
                current_is_recording = current and current.get("cluster_candidate_type") == "recording_track_candidate"
                next_is_recording = candidate_type == "recording_track_candidate"
                if current_is_recording and not next_is_recording:
                    continue
                if current and current_is_recording == next_is_recording and int(current["cluster_member_count"]) >= cluster_member_count:
                    continue
                metadata[release_track_id] = {
                    "cluster_member_count": cluster_member_count,
                    "cluster_candidate_type": candidate_type,
                    "cluster_relationship_kind": str(row["relationship_kind"] or ""),
                }
            return metadata

    metadata: dict[int, dict[str, Any]] = {}
    for item in _candidate_items():
        members = item.get("members") or []
        member_ids = {
            int(member["release_track_id"])
            for member in members
            if int(member.get("release_track_id") or 0) > 0
        }
        matched_ids = member_ids & target_ids
        if not matched_ids or len(member_ids) < 2:
            continue
        candidate_type = str(item.get("candidate_type") or "")
        if candidate_type not in {"recording_track_candidate", "track_family_candidate"}:
            continue
        for release_track_id in matched_ids:
            current = metadata.get(release_track_id)
            current_is_recording = current and current.get("cluster_candidate_type") == "recording_track_candidate"
            next_is_recording = candidate_type == "recording_track_candidate"
            if current_is_recording and not next_is_recording:
                continue
            if current and current_is_recording == next_is_recording and int(current["cluster_member_count"]) >= len(member_ids):
                continue
            metadata[release_track_id] = {
                "cluster_member_count": len(member_ids),
                "cluster_candidate_type": candidate_type,
                "cluster_relationship_kind": str(item.get("relationship_kind") or ""),
            }
    return metadata


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
