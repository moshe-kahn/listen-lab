from __future__ import annotations

import json
import logging
import time
from typing import Any

from backend.app.cache import static_metadata_cache
from backend.app.cache.file_cache import _cache_dir, _read_json_file, _write_json_file
from backend.app.cache.short_cache import SECTION_CACHE
from backend.app.cache.static_metadata_cache import _static_metadata_cache_path
from backend.app.history_analysis import clear_history_insights_cache

logger = logging.getLogger("listenlabs.auth")

SECTION_PREVIEW_LIMIT = 10
CACHE_VERSION = 1
PERSISTENT_HISTORY_CACHE_SCHEMA = "history_sections.v1"
PERSISTENT_HISTORY_CACHE_FILE = "history_sections.json"
LOCAL_HISTORY_INSIGHTS_CACHE_FILE = "local_history_insights.json"
LOCAL_HISTORY_INSIGHTS_CACHE_SCHEMA = "local_history_insights.v1"
HISTORY_TRACKS_DISPLAY_LIMIT = 40
LOCAL_HISTORY_INSIGHTS_CACHE_VERSION = 1
USER_RECENT_CACHE_FILE = "user_recent_sections.json"
USER_PROFILE_SNAPSHOT_CACHE_FILE = "user_profile_snapshots.json"


def _persistent_history_cache_path():
    return _cache_dir() / PERSISTENT_HISTORY_CACHE_FILE


def _local_history_insights_cache_path():
    return _cache_dir() / LOCAL_HISTORY_INSIGHTS_CACHE_FILE


def _user_recent_cache_path():
    return _cache_dir() / USER_RECENT_CACHE_FILE


def _user_profile_snapshot_cache_path():
    return _cache_dir() / USER_PROFILE_SNAPSHOT_CACHE_FILE


def _store_persistent_history_cache(
    history_signature: tuple[tuple[str, int, int], ...] | None,
    recent_window_days: int,
    sections: dict[str, Any],
) -> None:
    if not history_signature:
        return

    payload = {
        "cache_version": CACHE_VERSION,
        "schema": PERSISTENT_HISTORY_CACHE_SCHEMA,
        "history_signature": [list(item) for item in history_signature],
        "recent_window_days": recent_window_days,
        "stored_at": time.time(),
        "sections": sections,
    }

    cache_path = _persistent_history_cache_path()
    cache_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _store_local_history_insights_cache(
    history_signature: tuple[tuple[str, int, int], ...] | None,
    recent_window_days: int,
    track_limit: int,
    insights: dict[str, Any],
) -> None:
    if not history_signature or not insights:
        return
    path = _local_history_insights_cache_path()
    payload = _read_json_file(path) or {}
    existing_signature = payload.get("history_signature")
    next_signature = [list(item) for item in history_signature]
    if existing_signature != next_signature:
        payload = {
            "cache_version": LOCAL_HISTORY_INSIGHTS_CACHE_VERSION,
            "schema": LOCAL_HISTORY_INSIGHTS_CACHE_SCHEMA,
            "history_signature": next_signature,
            "entries": {},
        }
    payload["cache_version"] = LOCAL_HISTORY_INSIGHTS_CACHE_VERSION
    payload["schema"] = LOCAL_HISTORY_INSIGHTS_CACHE_SCHEMA
    payload["history_signature"] = next_signature
    entries = payload.get("entries") or {}
    entries[str(recent_window_days)] = {
        "track_limit": int(track_limit),
        "stored_at": time.time(),
        "insights": insights,
    }
    payload["entries"] = entries
    _write_json_file(path, payload)


def _clear_dashboard_caches() -> None:
    SECTION_CACHE.clear()
    static_metadata_cache.STATIC_METADATA_CACHE = None
    static_metadata_cache.STATIC_METADATA_DIRTY_ACCESS = False
    static_metadata_cache.STATIC_METADATA_DIRTY_CONTENT = False
    clear_history_insights_cache()
    try:
        _persistent_history_cache_path().unlink(missing_ok=True)
    except OSError:
        logger.exception("Failed to remove persistent history cache.")
    try:
        _local_history_insights_cache_path().unlink(missing_ok=True)
    except OSError:
        logger.exception("Failed to remove local history insights cache.")
    try:
        _static_metadata_cache_path().unlink(missing_ok=True)
    except OSError:
        logger.exception("Failed to remove static metadata cache.")
    try:
        _user_recent_cache_path().unlink(missing_ok=True)
    except OSError:
        logger.exception("Failed to remove user recent cache.")
    try:
        _user_profile_snapshot_cache_path().unlink(missing_ok=True)
    except OSError:
        logger.exception("Failed to remove user profile snapshot cache.")
