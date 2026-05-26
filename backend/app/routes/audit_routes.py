from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, HTTPException, Request, status
from fastapi.responses import JSONResponse

from backend.app.auth.session import _require_local_data_session
from backend.app.merged_track_aggregate import _merged_track_aggregate_payload
from backend.app.recording_track_candidates import query_recording_track_candidates, summarize_recording_track_candidates
from backend.app.recording_track_candidate_reviews import (
    get_recording_track_candidate_review,
    list_recording_track_candidate_reviews,
    save_recording_track_candidate_review,
)
from backend.app.spotify_catalog_backfill import query_release_track_duration_conflicts
from backend.app.track_identity_audit import (
    build_track_identity_audit,
    build_track_identity_readiness_report,
    query_ambiguous_review_queue,
    query_suggested_analysis_groups,
)
from backend.app.track_identity_audit_submission import (
    dry_run_identity_audit_submission,
    get_identity_audit_submission,
    list_identity_audit_submissions,
    save_identity_audit_submission,
    validate_identity_audit_submission_preview,
)

router = APIRouter(tags=["identity-audit"])


@router.get("/tracks/merged-aggregate")
async def tracks_merged_aggregate(
    request: Request,
    limit: int = 200,
    recent_window_days: int = 28,
    source_filter: str = "all",
) -> dict[str, Any]:
    _require_local_data_session(request)
    return _merged_track_aggregate_payload(
        limit=limit,
        recent_window_days=recent_window_days,
        source_filter=source_filter,
    )


@router.get("/debug/tracks/merged-aggregate")
async def debug_tracks_merged_aggregate(
    request: Request,
    limit: int = 200,
    recent_window_days: int = 28,
    source_filter: str = "all",
) -> dict[str, Any]:
    _require_local_data_session(request)
    return _merged_track_aggregate_payload(
        limit=limit,
        recent_window_days=recent_window_days,
        source_filter=source_filter,
    )


@router.get("/debug/tracks/identity-audit")
async def debug_tracks_identity_audit(
    request: Request,
    limit: int = 5,
) -> dict[str, Any]:
    _require_local_data_session(request)
    return build_track_identity_audit(limit=limit)


@router.get("/debug/tracks/identity-audit/ambiguous-review")
async def debug_tracks_identity_audit_ambiguous_review(
    request: Request,
    limit: int = 200,
    offset: int = 0,
    family: str | None = None,
    bucket: str | None = None,
    log_path: str | None = None,
) -> dict[str, Any]:
    _require_local_data_session(request)
    return query_ambiguous_review_queue(
        log_path=log_path,
        limit=limit,
        offset=offset,
        family=family,
        bucket=bucket,
    )


@router.get("/debug/tracks/identity-audit/suggested-groups")
async def debug_tracks_identity_audit_suggested_groups(
    request: Request,
    limit: int = 50,
    offset: int = 0,
    status_filter: str = "suggested",
) -> dict[str, Any]:
    _require_local_data_session(request)
    return query_suggested_analysis_groups(
        limit=limit,
        offset=offset,
        status=status_filter,
    )


@router.get("/debug/tracks/recording-track-candidates")
async def debug_tracks_recording_track_candidates(
    request: Request,
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
    _require_local_data_session(request)
    return query_recording_track_candidates(
        limit=limit,
        offset=offset,
        safety_status=safety_status,
        candidate_type=candidate_type,
        relationship_kind=relationship_kind,
        min_confidence=min_confidence,
        include_track_family_candidates=include_track_family_candidates,
        same_isrc_only=same_isrc_only,
        q=q,
        artist=artist,
    )


@router.get("/debug/tracks/recording-track-candidates/summary")
async def debug_tracks_recording_track_candidates_summary(
    request: Request,
    sample_limit: int = 5,
) -> dict[str, Any]:
    _require_local_data_session(request)
    return summarize_recording_track_candidates(sample_limit=sample_limit)


@router.get("/debug/tracks/release-track-duration-conflicts")
async def debug_tracks_release_track_duration_conflicts(
    request: Request,
    limit: int = 100,
    offset: int = 0,
    min_duration_delta_ms: int = 2_000,
) -> dict[str, Any]:
    _require_local_data_session(request)
    return query_release_track_duration_conflicts(
        limit=limit,
        offset=offset,
        min_duration_delta_ms=min_duration_delta_ms,
    )


@router.post("/debug/tracks/recording-track-candidate-reviews")
async def debug_tracks_recording_track_candidate_reviews_create(
    request: Request,
    payload: Any = Body(...),
) -> dict[str, Any]:
    _require_local_data_session(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Payload must be a JSON object.",
        )
    try:
        return save_recording_track_candidate_review(payload)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/debug/tracks/recording-track-candidate-reviews")
async def debug_tracks_recording_track_candidate_reviews_list(
    request: Request,
    limit: int = 500,
    offset: int = 0,
    decision: str | None = None,
) -> dict[str, Any]:
    _require_local_data_session(request)
    return list_recording_track_candidate_reviews(limit=limit, offset=offset, decision=decision)


@router.get("/debug/tracks/recording-track-candidate-reviews/{review_id}")
async def debug_tracks_recording_track_candidate_reviews_read(
    request: Request,
    review_id: int,
) -> Any:
    _require_local_data_session(request)
    payload = get_recording_track_candidate_review(review_id)
    if payload is None:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={
                "ok": False,
                "error": {
                    "code": "recording_track_candidate_review_not_found",
                    "message": f"Recording track candidate review {review_id} was not found.",
                },
            },
        )
    return payload


@router.post("/debug/tracks/identity-audit/submission-preview/validate")
async def debug_tracks_identity_audit_submission_preview_validate(
    request: Request,
    payload: Any = Body(...),
) -> dict[str, Any]:
    _require_local_data_session(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Payload must be a JSON object.",
    )
    return validate_identity_audit_submission_preview(payload)


@router.post("/debug/tracks/identity-audit/submissions")
async def debug_tracks_identity_audit_submissions_create(
    request: Request,
    payload: Any = Body(...),
) -> dict[str, Any]:
    _require_local_data_session(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Payload must be a JSON object.",
    )
    return save_identity_audit_submission(payload)


@router.get("/debug/tracks/identity-audit/submissions")
async def debug_tracks_identity_audit_submissions_list(
    request: Request,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    _require_local_data_session(request)
    return list_identity_audit_submissions(limit=limit, offset=offset)


@router.get("/debug/tracks/identity-audit/submissions/{submission_id}")
async def debug_tracks_identity_audit_submissions_read(
    request: Request,
    submission_id: int,
) -> Any:
    _require_local_data_session(request)
    payload = get_identity_audit_submission(submission_id)
    if payload is None:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={
                "ok": False,
                "error": {
                    "code": "submission_not_found",
                    "message": f"Submission {submission_id} was not found.",
                },
            },
        )
    return payload


@router.post("/debug/tracks/identity-audit/submissions/{submission_id}/dry-run")
async def debug_tracks_identity_audit_submissions_dry_run(
    request: Request,
    submission_id: int,
) -> Any:
    _require_local_data_session(request)
    payload = dry_run_identity_audit_submission(submission_id)
    if payload is None:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={
                "ok": False,
                "error": {
                    "code": "submission_not_found",
                    "message": f"Submission {submission_id} was not found.",
                },
            },
        )
    return payload


@router.get("/debug/tracks/identity-audit/readiness")
async def debug_track_identity_readiness(
    request: Request,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    _require_local_data_session(request)
    return build_track_identity_readiness_report(limit=limit, offset=offset)
