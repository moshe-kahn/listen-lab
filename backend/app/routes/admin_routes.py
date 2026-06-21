from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import APIRouter, Request

from backend.app.activity_release_track_audit import build_activity_release_track_coverage_audit
from backend.app.auth.session import _require_local_data_session
from backend.app.cache.history_cache import (
    HISTORY_TRACKS_DISPLAY_LIMIT,
    SECTION_PREVIEW_LIMIT,
    _clear_dashboard_caches,
    _store_local_history_insights_cache,
    _store_persistent_history_cache,
)
from backend.app.config import get_settings
from backend.app.history_analysis import get_history_signature, load_history_insights
from backend.app.listening_log import query_listening_log
from backend.app.spotify_recent_sync import maybe_sync_spotify_recent
from backend.app.spotify_token_store import refresh_access_token_if_needed

router = APIRouter(tags=["admin"])
logger = logging.getLogger("listenlabs.admin")


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.post("/cache/rebuild")
async def cache_rebuild(request: Request) -> dict[str, str]:
    settings = get_settings()
    _clear_dashboard_caches()
    history_signature = get_history_signature(settings.spotify_history_dir)
    if history_signature:
        for recent_window_days in (28, 180):
            history_insights = load_history_insights(
                settings.spotify_history_dir,
                max(SECTION_PREVIEW_LIMIT, 50),
                recent_window_days=recent_window_days,
            )
            if not history_insights:
                continue
            _store_local_history_insights_cache(
                history_signature,
                recent_window_days,
                max(SECTION_PREVIEW_LIMIT, 50),
                history_insights,
            )
            history_sections_with_tracks = {
                "tracks_all_time": history_insights.get("tracks_all_time", [])[:HISTORY_TRACKS_DISPLAY_LIMIT],
                "tracks_recent": history_insights.get("tracks_recent", [])[:SECTION_PREVIEW_LIMIT],
                "artists_all_time": history_insights.get("artists_all_time", [])[:SECTION_PREVIEW_LIMIT],
                "artists_recent": history_insights.get("artists_recent", [])[:SECTION_PREVIEW_LIMIT],
                "albums_all_time": history_insights.get("albums_all_time", [])[:SECTION_PREVIEW_LIMIT],
                "albums_recent": history_insights.get("albums_recent", [])[:SECTION_PREVIEW_LIMIT],
            }
            _store_persistent_history_cache(
                history_signature,
                recent_window_days,
                history_sections_with_tracks,
            )
    return {"status": "cache_rebuilt"}


@router.get("/debug/listening-log")
async def debug_listening_log(
    request: Request,
    limit: int = 50,
    offset: int = 0,
    source_filter: str = "all",
    liked_only: bool = False,
    force_recent_sync: bool = False,
) -> dict[str, Any]:
    route_started_at = time.perf_counter()
    user_id = _require_local_data_session(request)
    if force_recent_sync:
        sync_started_at = time.perf_counter()
        token_row = refresh_access_token_if_needed(user_id)
        recent_sync_summary = await maybe_sync_spotify_recent(
            str(token_row["access_token"]),
            source_ref="listen_log_reload",
            force=True,
            limit=50,
            raise_on_error=False,
        )
        logger.info(
            "event=listening_log_phase_timing phase=recent_sync elapsed_ms=%.1f",
            (time.perf_counter() - sync_started_at) * 1_000,
        )
    query_started_at = time.perf_counter()
    payload = query_listening_log(
        limit=limit,
        offset=offset,
        source_filter=source_filter if source_filter in {"all", "api", "history", "both"} else "all",
        user_id=str(user_id),
        liked_only=liked_only,
    )
    logger.info(
        "event=listening_log_phase_timing phase=query elapsed_ms=%.1f total_ms=%.1f",
        (time.perf_counter() - query_started_at) * 1_000,
        (time.perf_counter() - route_started_at) * 1_000,
    )
    result = dict(payload)
    if force_recent_sync:
        result["recent_sync_summary"] = recent_sync_summary
    return result


@router.get("/debug/activity/release-track-coverage")
async def debug_activity_release_track_coverage(
    request: Request,
    activity_limit: int = 50,
    backing_limit: int = 1000,
    sample_limit: int = 5,
) -> dict[str, Any]:
    _require_local_data_session(request)
    return build_activity_release_track_coverage_audit(
        activity_limit=activity_limit,
        backing_limit=backing_limit,
        sample_limit=sample_limit,
    )
