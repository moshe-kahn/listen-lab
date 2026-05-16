from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, HTTPException, Request, status
from fastapi.responses import JSONResponse

from backend.app.auth.session import _require_local_data_session
from backend.app.merged_track_aggregate import _merged_track_aggregate_payload
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
