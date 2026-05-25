from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any, Literal

from backend.app.db import sqlite_connection

RecordingTrackReviewDecision = Literal[
    "accepted",
    "rejected",
    "unsure",
    "needs_more_metadata",
    "wrong_representative",
    "maybe_split",
    "maybe_merge_more",
]

VALID_DECISIONS: set[str] = {
    "accepted",
    "rejected",
    "unsure",
    "needs_more_metadata",
    "wrong_representative",
    "maybe_split",
    "maybe_merge_more",
}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _load_json_object(raw_json: str | None) -> dict[str, Any]:
    if not raw_json:
        return {}
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _review_row_to_item(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "candidate_key": str(row["candidate_key"] or ""),
        "decision": str(row["decision"] or ""),
        "reviewer_note": row["reviewer_note"] if row["reviewer_note"] is None else str(row["reviewer_note"]),
        "preferred_representative_release_track_id": (
            None if row["preferred_representative_release_track_id"] is None else int(row["preferred_representative_release_track_id"])
        ),
        "preferred_playback_source_track_id": (
            None if row["preferred_playback_source_track_id"] is None else int(row["preferred_playback_source_track_id"])
        ),
        "candidate_snapshot": _load_json_object(str(row["candidate_snapshot_json"] or "")),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Expected integer or null.") from exc


def save_recording_track_candidate_review(payload: dict[str, Any]) -> dict[str, Any]:
    candidate_key = str(payload.get("candidate_key") or "").strip()
    if not candidate_key:
        raise ValueError("candidate_key is required.")

    decision = str(payload.get("decision") or "").strip()
    if decision not in VALID_DECISIONS:
        raise ValueError("decision is invalid.")

    candidate_snapshot = payload.get("candidate_snapshot")
    if not isinstance(candidate_snapshot, dict):
        raise ValueError("candidate_snapshot must be a JSON object.")

    reviewer_note_raw = payload.get("reviewer_note")
    reviewer_note = None if reviewer_note_raw is None else str(reviewer_note_raw).strip()
    preferred_representative_release_track_id = _optional_int(payload.get("preferred_representative_release_track_id"))
    preferred_playback_source_track_id = _optional_int(payload.get("preferred_playback_source_track_id"))
    snapshot_json = json.dumps(candidate_snapshot, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    now = _utc_now()

    with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
        existing = connection.execute(
            """
            SELECT id, created_at
            FROM recording_track_candidate_review
            WHERE candidate_key = ?
            LIMIT 1
            """,
            (candidate_key,),
        ).fetchone()
        if existing is None:
            cursor = connection.execute(
                """
                INSERT INTO recording_track_candidate_review (
                  candidate_key,
                  decision,
                  reviewer_note,
                  preferred_representative_release_track_id,
                  preferred_playback_source_track_id,
                  candidate_snapshot_json,
                  created_at,
                  updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate_key,
                    decision,
                    reviewer_note,
                    preferred_representative_release_track_id,
                    preferred_playback_source_track_id,
                    snapshot_json,
                    now,
                    now,
                ),
            )
            review_id = int(cursor.lastrowid)
        else:
            review_id = int(existing["id"])
            connection.execute(
                """
                UPDATE recording_track_candidate_review
                SET decision = ?,
                    reviewer_note = ?,
                    preferred_representative_release_track_id = ?,
                    preferred_playback_source_track_id = ?,
                    candidate_snapshot_json = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    decision,
                    reviewer_note,
                    preferred_representative_release_track_id,
                    preferred_playback_source_track_id,
                    snapshot_json,
                    now,
                    review_id,
                ),
            )
        row = connection.execute(
            """
            SELECT *
            FROM recording_track_candidate_review
            WHERE id = ?
            LIMIT 1
            """,
            (review_id,),
        ).fetchone()

    return {
        "ok": True,
        "item": _review_row_to_item(row),
        "source": {
            "kind": "sqlite",
            "uses_spotify_api": False,
            "mutates_identity": False,
        },
    }


def list_recording_track_candidate_reviews(
    *,
    limit: int = 500,
    offset: int = 0,
    decision: str | None = None,
) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit), 2000))
    bounded_offset = max(0, int(offset))
    decision_filter = str(decision or "").strip()
    params: list[Any] = []
    where_sql = ""
    if decision_filter:
        where_sql = "WHERE decision = ?"
        params.append(decision_filter)

    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        total = int(connection.execute(f"SELECT count(*) FROM recording_track_candidate_review {where_sql}", params).fetchone()[0])
        rows = connection.execute(
            f"""
            SELECT *
            FROM recording_track_candidate_review
            {where_sql}
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            OFFSET ?
            """,
            [*params, bounded_limit, bounded_offset],
        ).fetchall()

    return {
        "ok": True,
        "items": [_review_row_to_item(row) for row in rows],
        "total": total,
        "limit": bounded_limit,
        "offset": bounded_offset,
        "source": {
            "kind": "sqlite",
            "uses_spotify_api": False,
            "mutates_identity": False,
        },
    }


def get_recording_track_candidate_review(review_id: int) -> dict[str, Any] | None:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        row = connection.execute(
            """
            SELECT *
            FROM recording_track_candidate_review
            WHERE id = ?
            LIMIT 1
            """,
            (int(review_id),),
        ).fetchone()
    if row is None:
        return None
    return {
        "ok": True,
        "item": _review_row_to_item(row),
        "source": {
            "kind": "sqlite",
            "uses_spotify_api": False,
            "mutates_identity": False,
        },
    }
