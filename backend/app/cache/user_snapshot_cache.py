from __future__ import annotations

import time
from typing import Any

from backend.app.cache.file_cache import _read_json_file, _write_json_file
from backend.app.cache.history_cache import _user_profile_snapshot_cache_path, _user_recent_cache_path

USER_RECENT_CACHE_VERSION = 1
USER_RECENT_CACHE_SCHEMA = "user_recent_sections.v1"
USER_PROFILE_SNAPSHOT_CACHE_VERSION = 1
USER_PROFILE_SNAPSHOT_CACHE_SCHEMA = "user_profile_snapshots.v1"
USER_RECENT_CACHE_MAX_USERS = 50
USER_RECENT_CACHE_MAX_AGE_SECONDS = 60 * 60 * 12
USER_PROFILE_SNAPSHOT_MAX_AGE_SECONDS = 60 * 60 * 24 * 14


def _snapshot_user_entry(users: dict[str, Any], user_id: str | None) -> dict[str, Any]:
    if user_id:
        return users.get(str(user_id)) or {}
    if len(users) == 1:
        return next(iter(users.values())) or {}
    return {}


def _load_user_recent_cache() -> dict[str, Any]:
    payload = _read_json_file(_user_recent_cache_path()) or {}
    if payload.get("cache_version") != USER_RECENT_CACHE_VERSION:
        return {"cache_version": USER_RECENT_CACHE_VERSION, "schema": USER_RECENT_CACHE_SCHEMA, "users": {}}
    if payload.get("schema") != USER_RECENT_CACHE_SCHEMA:
        return {"cache_version": USER_RECENT_CACHE_VERSION, "schema": USER_RECENT_CACHE_SCHEMA, "users": {}}
    return {
        "cache_version": USER_RECENT_CACHE_VERSION,
        "schema": USER_RECENT_CACHE_SCHEMA,
        "users": payload.get("users") or {},
    }


def _save_user_recent_cache(payload: dict[str, Any]) -> None:
    payload["cache_version"] = USER_RECENT_CACHE_VERSION
    payload["schema"] = USER_RECENT_CACHE_SCHEMA
    _write_json_file(_user_recent_cache_path(), payload)


def _load_user_profile_snapshot_cache() -> dict[str, Any]:
    payload = _read_json_file(_user_profile_snapshot_cache_path()) or {}
    if payload.get("cache_version") != USER_PROFILE_SNAPSHOT_CACHE_VERSION:
        return {"cache_version": USER_PROFILE_SNAPSHOT_CACHE_VERSION, "schema": USER_PROFILE_SNAPSHOT_CACHE_SCHEMA, "users": {}}
    if payload.get("schema") != USER_PROFILE_SNAPSHOT_CACHE_SCHEMA:
        return {"cache_version": USER_PROFILE_SNAPSHOT_CACHE_VERSION, "schema": USER_PROFILE_SNAPSHOT_CACHE_SCHEMA, "users": {}}
    return {
        "cache_version": USER_PROFILE_SNAPSHOT_CACHE_VERSION,
        "schema": USER_PROFILE_SNAPSHOT_CACHE_SCHEMA,
        "users": payload.get("users") or {},
    }


def _save_user_profile_snapshot_cache(payload: dict[str, Any]) -> None:
    payload["cache_version"] = USER_PROFILE_SNAPSHOT_CACHE_VERSION
    payload["schema"] = USER_PROFILE_SNAPSHOT_CACHE_SCHEMA
    _write_json_file(_user_profile_snapshot_cache_path(), payload)


def _store_user_profile_snapshot(user_id: str | None, snapshot: dict[str, Any]) -> None:
    if not user_id:
        return
    payload = _load_user_profile_snapshot_cache()
    users = payload.get("users") or {}
    existing_snapshot = ((users.get(str(user_id)) or {}).get("snapshot")) or {}
    users[str(user_id)] = {
        "stored_at": time.time(),
        "snapshot": {
            **existing_snapshot,
            **snapshot,
        },
    }
    payload["users"] = users
    _save_user_profile_snapshot_cache(payload)


def _load_user_profile_snapshot(user_id: str | None) -> dict[str, Any] | None:
    payload = _load_user_profile_snapshot_cache()
    users = payload.get("users") or {}
    entry = _snapshot_user_entry(users, user_id)
    if not entry:
        return None
    stored_at = float(entry.get("stored_at", 0.0))
    if time.time() - stored_at > USER_PROFILE_SNAPSHOT_MAX_AGE_SECONDS:
        if user_id:
            users.pop(str(user_id), None)
            payload["users"] = users
            _save_user_profile_snapshot_cache(payload)
        return None
    snapshot = entry.get("snapshot")
    if not isinstance(snapshot, dict):
        return None
    return {
        **snapshot,
        "_stored_at": stored_at,
    }


def _store_user_recent_snapshot(
    user_id: str | None,
    recent_range: str,
    snapshot: dict[str, Any],
) -> None:
    if not user_id:
        return
    payload = _load_user_recent_cache()
    users = payload.get("users") or {}
    now = time.time()
    users[str(user_id)] = {
        "stored_at": now,
        "recent_range": recent_range,
        "snapshot": snapshot,
    }
    if len(users) > USER_RECENT_CACHE_MAX_USERS:
        ranked = sorted(
            users.items(),
            key=lambda item: float((item[1] or {}).get("stored_at", 0.0)),
            reverse=True,
        )[:USER_RECENT_CACHE_MAX_USERS]
        users = dict(ranked)
    payload["users"] = users
    _save_user_recent_cache(payload)


def _load_user_recent_snapshot(
    user_id: str | None,
    recent_range: str,
    *,
    allow_stale: bool = False,
) -> dict[str, Any] | None:
    payload = _load_user_recent_cache()
    users = payload.get("users") or {}
    entry = _snapshot_user_entry(users, user_id)
    if not entry:
        return None
    stored_at = float(entry.get("stored_at", 0.0))
    if time.time() - stored_at > USER_RECENT_CACHE_MAX_AGE_SECONDS and not allow_stale:
        if user_id:
            users.pop(str(user_id), None)
            payload["users"] = users
            _save_user_recent_cache(payload)
        return None
    if entry.get("recent_range") != recent_range:
        return None
    snapshot = entry.get("snapshot")
    return {**snapshot, "_stored_at": stored_at} if isinstance(snapshot, dict) else None


def _invalidate_user_recent_snapshot(user_id: str | None) -> bool:
    if not user_id:
        return False
    payload = _load_user_recent_cache()
    users = payload.get("users") or {}
    removed = users.pop(str(user_id), None) is not None
    if removed:
        payload["users"] = users
        _save_user_recent_cache(payload)
    return removed
