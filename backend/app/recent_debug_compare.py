from __future__ import annotations

from collections import Counter
from typing import Any


SPARSE_FIELDS = (
    "image_url",
    "album_name",
    "album_id",
    "album_url",
    "artist_name",
    "artists",
    "preview_url",
    "url",
)


def _is_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict, tuple, set)):
        return bool(value)
    return True


def _track_identity(item: dict[str, Any]) -> str | None:
    track_id = item.get("track_id")
    if isinstance(track_id, str) and track_id.strip():
        return track_id.strip()
    uri = item.get("uri")
    if isinstance(uri, str) and uri.strip():
        return uri.strip()
    track_name = " ".join(str(item.get("track_name") or "").strip().lower().split())
    artist_name = " ".join(str(item.get("artist_name") or "").strip().lower().split())
    if track_name and artist_name:
        return f"{track_name}|||{artist_name}"
    return None


def _duplicate_summary(items: list[dict[str, Any]], inspect_limit: int) -> dict[str, Any]:
    inspected = items[:inspect_limit]
    keys = [_track_identity(item) for item in inspected]
    counts = Counter(key for key in keys if key)
    duplicates = {key: count for key, count in counts.items() if count > 1}
    return {
        "duplicate_identity_count": len(duplicates),
        "duplicate_row_count": sum(count - 1 for count in duplicates.values()),
        "examples": [
            {"track_identity": key, "count": count}
            for key, count in list(sorted(duplicates.items(), key=lambda pair: (-pair[1], pair[0])))[:5]
        ],
    }


def _field_presence_summary(items: list[dict[str, Any]], inspect_limit: int) -> dict[str, int]:
    inspected = items[:inspect_limit]
    return {
        field: sum(1 for item in inspected if _is_present(item.get(field)))
        for field in SPARSE_FIELDS
    }


def _played_at_order_summary(items: list[dict[str, Any]], inspect_limit: int) -> dict[str, Any]:
    inspected = items[:inspect_limit]
    violations: list[dict[str, Any]] = []
    for index in range(len(inspected) - 1):
        current_ms = inspected[index].get("spotify_played_at_unix_ms")
        next_ms = inspected[index + 1].get("spotify_played_at_unix_ms")
        if current_ms is None or next_ms is None:
            continue
        if int(current_ms) < int(next_ms):
            violations.append(
                {
                    "index": index,
                    "current_played_at": inspected[index].get("spotify_played_at"),
                    "next_played_at": inspected[index + 1].get("spotify_played_at"),
                }
            )
    return {
        "descending_by_played_at_unix_ms": not violations,
        "violations": violations[:5],
    }


def _recent_tracks_db_order_summary(db_rows: list[dict[str, Any]], inspect_limit: int) -> dict[str, Any]:
    inspected = db_rows[:inspect_limit]
    violations: list[dict[str, Any]] = []
    tie_count = 0
    for index in range(len(inspected) - 1):
        current = inspected[index]
        nxt = inspected[index + 1]
        current_played_at = str(current.get("played_at") or "")
        next_played_at = str(nxt.get("played_at") or "")
        current_id = current.get("raw_row_id")
        next_id = nxt.get("raw_row_id")
        if current_played_at < next_played_at:
            violations.append(
                {
                    "index": index,
                    "reason": "played_at_ascending",
                    "current_played_at": current.get("played_at"),
                    "next_played_at": nxt.get("played_at"),
                }
            )
            continue
        if current_played_at == next_played_at:
            tie_count += 1
            if isinstance(current_id, int) and isinstance(next_id, int) and current_id < next_id:
                violations.append(
                    {
                        "index": index,
                        "reason": "raw_row_id_ascending_on_played_at_tie",
                        "current_played_at": current.get("played_at"),
                        "current_raw_row_id": current_id,
                        "next_raw_row_id": next_id,
                    }
                )
    return {
        "ordered_by_played_at_desc_id_desc": not violations,
        "played_at_tie_count": tie_count,
        "violations": violations[:5],
    }


def _recent_top_tracks_db_order_summary(items: list[dict[str, Any]], inspect_limit: int) -> dict[str, Any]:
    inspected = items[:inspect_limit]
    violations: list[dict[str, Any]] = []
    for index in range(len(inspected) - 1):
        current = inspected[index]
        nxt = inspected[index + 1]
        current_tuple = (
            int(current.get("recent_play_count") or 0),
            str(current.get("last_played_at") or ""),
            int(current.get("all_time_play_count") or 0),
            str(_track_identity(current) or ""),
        )
        next_tuple = (
            int(nxt.get("recent_play_count") or 0),
            str(nxt.get("last_played_at") or ""),
            int(nxt.get("all_time_play_count") or 0),
            str(_track_identity(nxt) or ""),
        )
        if current_tuple < next_tuple:
            violations.append(
                {
                    "index": index,
                    "current": {
                        "track_id": current.get("track_id"),
                        "recent_play_count": current.get("recent_play_count"),
                        "last_played_at": current.get("last_played_at"),
                        "all_time_play_count": current.get("all_time_play_count"),
                    },
                    "next": {
                        "track_id": nxt.get("track_id"),
                        "recent_play_count": nxt.get("recent_play_count"),
                        "last_played_at": nxt.get("last_played_at"),
                        "all_time_play_count": nxt.get("all_time_play_count"),
                    },
                }
            )
    return {
        "ordered_by_recent_play_count_then_recency": not violations,
        "violations": violations[:5],
    }


def _index_diff_examples(
    *,
    legacy_items: list[dict[str, Any]],
    db_items: list[dict[str, Any]],
    inspect_limit: int,
    fields: tuple[str, ...],
) -> dict[str, Any]:
    examples: list[dict[str, Any]] = []
    mismatch_counts = {field: 0 for field in fields}
    compared = min(inspect_limit, len(legacy_items), len(db_items))
    for index in range(compared):
        legacy_item = legacy_items[index]
        db_item = db_items[index]
        differing_fields: list[str] = []
        for field in fields:
            if legacy_item.get(field) != db_item.get(field):
                mismatch_counts[field] += 1
                differing_fields.append(field)
        if differing_fields:
            examples.append(
                {
                    "index": index,
                    "legacy_track_id": legacy_item.get("track_id"),
                    "db_track_id": db_item.get("track_id"),
                    "differing_fields": differing_fields,
                    "legacy": {field: legacy_item.get(field) for field in differing_fields},
                    "db": {field: db_item.get(field) for field in differing_fields},
                }
            )
        if len(examples) >= 8:
            break
    return {
        "compared_items": compared,
        "mismatch_counts": mismatch_counts,
        "examples": examples,
    }


def build_recent_comparison_summary(
    *,
    legacy_payload: dict[str, Any],
    db_recent_tracks_payload: dict[str, Any],
    db_recent_top_tracks_payload: dict[str, Any],
    db_recent_track_rows: list[dict[str, Any]],
    inspect_limit: int,
) -> dict[str, Any]:
    bounded_inspect_limit = max(1, int(inspect_limit))
    legacy_recent_tracks = list(legacy_payload.get("recent_tracks") or [])
    legacy_recent_top_tracks = list(legacy_payload.get("recent_top_tracks") or [])
    db_recent_tracks = list(db_recent_tracks_payload.get("items") or [])
    db_recent_top_tracks = list(db_recent_top_tracks_payload.get("items") or [])

    legacy_top_identities = {_track_identity(item) for item in legacy_recent_top_tracks[:bounded_inspect_limit]}
    db_top_identities = {_track_identity(item) for item in db_recent_top_tracks[:bounded_inspect_limit]}
    legacy_top_identities.discard(None)
    db_top_identities.discard(None)

    return {
        "inspect_limit": bounded_inspect_limit,
        "recent_tracks": {
            "legacy_count": len(legacy_recent_tracks),
            "db_count": len(db_recent_tracks),
            "legacy_order_check": _played_at_order_summary(legacy_recent_tracks, bounded_inspect_limit),
            "db_order_check": _recent_tracks_db_order_summary(db_recent_track_rows, bounded_inspect_limit),
            "legacy_duplicates": _duplicate_summary(legacy_recent_tracks, bounded_inspect_limit),
            "db_duplicates": _duplicate_summary(db_recent_tracks, bounded_inspect_limit),
            "field_presence": {
                "legacy": _field_presence_summary(legacy_recent_tracks, bounded_inspect_limit),
                "db": _field_presence_summary(db_recent_tracks, bounded_inspect_limit),
            },
            "index_diffs": _index_diff_examples(
                legacy_items=legacy_recent_tracks,
                db_items=db_recent_tracks,
                inspect_limit=bounded_inspect_limit,
                fields=(
                    "track_id",
                    "track_name",
                    "artist_name",
                    "album_name",
                    "album_id",
                    "image_url",
                    "spotify_played_at",
                    "artists",
                ),
            ),
        },
        "recent_top_tracks": {
            "legacy_count": len(legacy_recent_top_tracks),
            "db_count": len(db_recent_top_tracks),
            "legacy_duplicates": _duplicate_summary(legacy_recent_top_tracks, bounded_inspect_limit),
            "db_duplicates": _duplicate_summary(db_recent_top_tracks, bounded_inspect_limit),
            "db_order_check": _recent_top_tracks_db_order_summary(db_recent_top_tracks, bounded_inspect_limit),
            "field_presence": {
                "legacy": _field_presence_summary(legacy_recent_top_tracks, bounded_inspect_limit),
                "db": _field_presence_summary(db_recent_top_tracks, bounded_inspect_limit),
            },
            "overlap": {
                "shared_identity_count": len(legacy_top_identities & db_top_identities),
                "legacy_only_count": len(legacy_top_identities - db_top_identities),
                "db_only_count": len(db_top_identities - legacy_top_identities),
            },
            "index_diffs": _index_diff_examples(
                legacy_items=legacy_recent_top_tracks,
                db_items=db_recent_top_tracks,
                inspect_limit=bounded_inspect_limit,
                fields=(
                    "track_id",
                    "track_name",
                    "artist_name",
                    "album_name",
                    "album_id",
                    "image_url",
                    "play_count",
                    "recent_play_count",
                    "all_time_play_count",
                    "last_played_at",
                ),
            ),
            "notes": [
                "Legacy recent_top_tracks currently comes from Spotify top-tracks API output, not the new DB event-window aggregate.",
                "Expect ordering and membership mismatches here until route rewiring intentionally changes semantics.",
            ],
        },
    }
