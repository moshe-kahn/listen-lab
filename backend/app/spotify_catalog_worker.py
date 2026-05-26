from __future__ import annotations

import json
import os
import socket
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from backend.app.db import list_spotify_auth_users, sqlite_connection
from backend.app.spotify_catalog_backfill import (
    repair_release_track_durations_from_spotify_catalog,
    run_spotify_catalog_backfill,
    run_spotify_track_metadata_canary,
)
from backend.app.spotify_token_store import SpotifyTokenStoreError, refresh_access_token_if_needed


SPOTIFY_TRACK_METADATA_WORKER = "spotify_track_metadata"
STALE_LOCK_SECONDS = 2 * 60 * 60
FALLBACK_RATE_LIMIT_COOLDOWN_SECONDS = 60 * 60
POST_COOLDOWN_CANARY_FALLBACK_BASE_SECONDS = 6 * 60 * 60
POST_COOLDOWN_CANARY_FALLBACK_CAP_SECONDS = 24 * 60 * 60
REQUEST_BUDGET_WINDOW_SECONDS = 60 * 60
REQUEST_BUDGET_SOFT_LIMIT = 550
REQUEST_BUDGET_HARD_LIMIT = 650
REQUEST_BUDGET_SOFT_COOLDOWN_SECONDS = 15 * 60
REQUEST_BUDGET_HARD_COOLDOWN_SECONDS = 30 * 60
REQUEST_BUDGET_MIN_RUN_REQUESTS = 2

TRACK_METADATA_WORKER_CONFIG: dict[str, Any] = {
    "target": "tracks",
    "run_mode": "metadata_only",
    "reason": "identity_metadata",
    "album_tracklist_policy": "none",
    "include_albums": False,
    "limit": 50,
    "max_requests": 60,
    "max_runtime_seconds": 360,
    "request_delay_seconds": 5.0,
    "market": "US",
    "priority_scope": "identity_and_top_listened",
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_iso_utc(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _json_dump(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _owner_id() -> str:
    host = socket.gethostname() or "unknown-host"
    return f"{host}:{os.getpid()}:{uuid.uuid4().hex}"


def _insert_invocation(
    *,
    worker_name: str,
    started_at: str,
    status: str,
    skip_reason: str | None = None,
) -> int:
    with sqlite_connection(write=True) as connection:
        cursor = connection.execute(
            """
            INSERT INTO spotify_catalog_worker_invocation (
              worker_name,
              started_at,
              status,
              skip_reason
            ) VALUES (?, ?, ?, ?)
            """,
            (worker_name, started_at, status, skip_reason),
        )
        connection.commit()
        return int(cursor.lastrowid)


def _finish_invocation(
    *,
    invocation_id: int,
    completed_at: str,
    status: str,
    skip_reason: str | None = None,
    result: dict[str, Any] | None = None,
    cooldown_until: str | None = None,
    error: str | None = None,
) -> None:
    result = result or {}
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            UPDATE spotify_catalog_worker_invocation
            SET
              completed_at = ?,
              status = ?,
              skip_reason = ?,
              backfill_run_id = ?,
              requests_total = ?,
              requests_429 = ?,
              tracks_fetched = ?,
              tracks_upserted = ?,
              cooldown_until = ?,
              result_json = ?,
              error = ?
            WHERE id = ?
            """,
            (
                completed_at,
                status,
                skip_reason,
                result.get("run_id"),
                result.get("requests_total"),
                result.get("requests_429"),
                result.get("tracks_fetched"),
                result.get("tracks_upserted"),
                cooldown_until,
                _json_dump(result) if result else None,
                error,
                invocation_id,
            ),
        )
        connection.commit()


def _upsert_worker_state(
    *,
    worker_name: str,
    updated_at: str,
    cooldown_until: str | None,
    last_started_at: str,
    last_completed_at: str,
    last_status: str,
    last_run_id: int | None,
    last_result: dict[str, Any] | None,
    last_error: str | None,
    consecutive_post_cooldown_canary_429s: int | None = None,
) -> None:
    counter_update_sql = ""
    if consecutive_post_cooldown_canary_429s is not None:
        counter_update_sql = ", consecutive_post_cooldown_canary_429s = excluded.consecutive_post_cooldown_canary_429s"
    with sqlite_connection(write=True) as connection:
        connection.execute(
            f"""
            INSERT INTO spotify_catalog_worker_state (
              worker_name,
              cooldown_until,
              last_started_at,
              last_completed_at,
              last_status,
              last_run_id,
              last_result_json,
              last_error,
              updated_at,
              consecutive_post_cooldown_canary_429s
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(worker_name) DO UPDATE SET
              cooldown_until = excluded.cooldown_until,
              last_started_at = excluded.last_started_at,
              last_completed_at = excluded.last_completed_at,
              last_status = excluded.last_status,
              last_run_id = excluded.last_run_id,
              last_result_json = excluded.last_result_json,
              last_error = excluded.last_error,
              updated_at = excluded.updated_at
              {counter_update_sql}
            """,
            (
                worker_name,
                cooldown_until,
                last_started_at,
                last_completed_at,
                last_status,
                last_run_id,
                _json_dump(last_result) if last_result else None,
                last_error,
                updated_at,
                int(consecutive_post_cooldown_canary_429s or 0),
            ),
        )
        connection.commit()


def _cooldown_until_from_result(*, result: dict[str, Any], now: datetime) -> str | None:
    stop_reason = str(result.get("stop_reason") or "")
    if stop_reason not in {"rate_limited", "post_cooldown_canary_429"}:
        return None
    retry_after_seconds = float(result.get("max_retry_after_seconds") or 0.0)
    if stop_reason != "post_cooldown_canary_429":
        cooldown_seconds = retry_after_seconds if retry_after_seconds > 0 else FALLBACK_RATE_LIMIT_COOLDOWN_SECONDS
        return _iso_utc(now + timedelta(seconds=cooldown_seconds))

    fallback_seconds = float(result.get("fallback_cooldown_seconds") or 0.0)
    if fallback_seconds <= 0:
        fallback_seconds = _post_cooldown_canary_fallback_seconds(1)
    cooldown_seconds = max(retry_after_seconds, fallback_seconds) if retry_after_seconds > 0 else fallback_seconds
    return _iso_utc(now + timedelta(seconds=cooldown_seconds))


def _post_cooldown_canary_fallback_seconds(consecutive_429s: int) -> int:
    bounded_count = max(1, int(consecutive_429s))
    multiplier = 2 ** (bounded_count - 1)
    return min(POST_COOLDOWN_CANARY_FALLBACK_BASE_SECONDS * multiplier, POST_COOLDOWN_CANARY_FALLBACK_CAP_SECONDS)


def _current_cooldown_until(worker_name: str) -> str | None:
    with sqlite_connection() as connection:
        row = connection.execute(
            "SELECT cooldown_until FROM spotify_catalog_worker_state WHERE worker_name = ?",
            (worker_name,),
        ).fetchone()
    return str(row[0]) if row and row[0] else None


def reset_spotify_track_metadata_worker_cooldown(
    *,
    apply: bool = True,
    now: datetime | None = None,
) -> dict[str, Any]:
    worker_name = SPOTIFY_TRACK_METADATA_WORKER
    checked_at = now or _utc_now()
    previous_cooldown_until = _current_cooldown_until(worker_name)
    parsed_previous = _parse_iso_utc(previous_cooldown_until)
    active = parsed_previous is not None and parsed_previous > checked_at
    result = {
        "ok": True,
        "mode": "apply" if apply else "dry_run",
        "performed_action": "reset_cooldown" if apply and previous_cooldown_until else "none",
        "worker_name": worker_name,
        "previous_cooldown_until": previous_cooldown_until,
        "active_cooldown": bool(active),
        "cooldown_until": None if apply else previous_cooldown_until,
        "reset_count": 0,
        "would_reset_count": 1 if previous_cooldown_until else 0,
    }
    if not apply or not previous_cooldown_until:
        return result
    with sqlite_connection(write=True) as connection:
        before = connection.total_changes
        connection.execute(
            """
            UPDATE spotify_catalog_worker_state
            SET cooldown_until = NULL,
                updated_at = ?
            WHERE worker_name = ?
            """,
            (_iso_utc(checked_at), worker_name),
        )
        if connection.total_changes > before:
            result["reset_count"] = 1
    return result


def _worker_state(worker_name: str) -> dict[str, Any] | None:
    with sqlite_connection() as connection:
        row = connection.execute(
            """
            SELECT
              cooldown_until,
              last_status,
              last_result_json,
              consecutive_post_cooldown_canary_429s
            FROM spotify_catalog_worker_state
            WHERE worker_name = ?
            """,
            (worker_name,),
        ).fetchone()
    if not row:
        return None
    last_result: dict[str, Any] = {}
    if row[2]:
        try:
            parsed = json.loads(str(row[2]))
            if isinstance(parsed, dict):
                last_result = parsed
        except (TypeError, ValueError):
            last_result = {}
    return {
        "cooldown_until": str(row[0]) if row[0] else None,
        "last_status": str(row[1] or ""),
        "last_result": last_result,
        "consecutive_post_cooldown_canary_429s": int(row[3] or 0),
    }


def _needs_post_cooldown_canary(*, state: dict[str, Any] | None, now: datetime) -> bool:
    if not state:
        return False
    cooldown_until = _parse_iso_utc(state.get("cooldown_until"))
    if cooldown_until is None or cooldown_until > now:
        return False
    last_result = state.get("last_result") if isinstance(state.get("last_result"), dict) else {}
    return str(last_result.get("stop_reason") or "") in {"rate_limited", "post_cooldown_canary_429"}


def _recent_spotify_request_count(*, now: datetime) -> int:
    since = _iso_utc(now - timedelta(seconds=REQUEST_BUDGET_WINDOW_SECONDS))
    with sqlite_connection() as connection:
        row = connection.execute(
            """
            SELECT COALESCE(sum(requests_total), 0)
            FROM spotify_catalog_backfill_run
            WHERE started_at >= ?
            """,
            (since,),
        ).fetchone()
    return int(row[0] or 0) if row else 0


def _request_budget_cooldown(*, now: datetime) -> tuple[str | None, int]:
    recent_requests = _recent_spotify_request_count(now=now)
    if recent_requests >= REQUEST_BUDGET_HARD_LIMIT:
        return _iso_utc(now + timedelta(seconds=REQUEST_BUDGET_HARD_COOLDOWN_SECONDS)), recent_requests
    if recent_requests >= REQUEST_BUDGET_SOFT_LIMIT:
        return _iso_utc(now + timedelta(seconds=REQUEST_BUDGET_SOFT_COOLDOWN_SECONDS)), recent_requests
    return None, recent_requests


def _request_budget_adjusted_config(*, now: datetime, config: dict[str, Any]) -> tuple[str | None, int, dict[str, Any]]:
    cooldown_until, recent_requests = _request_budget_cooldown(now=now)
    adjusted_config = dict(config)
    if cooldown_until is not None:
        return cooldown_until, recent_requests, adjusted_config

    remaining_requests = REQUEST_BUDGET_SOFT_LIMIT - recent_requests
    if remaining_requests < REQUEST_BUDGET_MIN_RUN_REQUESTS:
        return _iso_utc(now + timedelta(seconds=REQUEST_BUDGET_SOFT_COOLDOWN_SECONDS)), recent_requests, adjusted_config

    configured_max_requests = int(adjusted_config.get("max_requests") or 0)
    if configured_max_requests > 0:
        adjusted_config["max_requests"] = min(configured_max_requests, remaining_requests)
    adjusted_max_requests = int(adjusted_config.get("max_requests") or 0)
    configured_limit = int(adjusted_config.get("limit") or 0)
    if adjusted_max_requests > 0 and configured_limit > 0:
        adjusted_config["limit"] = min(configured_limit, max(1, adjusted_max_requests - 1))
    return None, recent_requests, adjusted_config


def _acquire_worker_lock(*, worker_name: str, owner: str, now: datetime) -> bool:
    locked_at = _iso_utc(now)
    stale_before = now - timedelta(seconds=STALE_LOCK_SECONDS)
    with sqlite_connection(write=True) as connection:
        try:
            connection.execute(
                """
                INSERT INTO spotify_catalog_worker_lock (worker_name, locked_at, owner)
                VALUES (?, ?, ?)
                """,
                (worker_name, locked_at, owner),
            )
            connection.commit()
            return True
        except Exception:
            connection.rollback()

        row = connection.execute(
            "SELECT locked_at FROM spotify_catalog_worker_lock WHERE worker_name = ?",
            (worker_name,),
        ).fetchone()
        existing_locked_at = _parse_iso_utc(str(row[0])) if row and row[0] else None
        if existing_locked_at is None or existing_locked_at > stale_before:
            return False
        connection.execute(
            """
            UPDATE spotify_catalog_worker_lock
            SET locked_at = ?, owner = ?
            WHERE worker_name = ?
            """,
            (locked_at, owner, worker_name),
        )
        connection.commit()
        return True


def _release_worker_lock(*, worker_name: str, owner: str) -> None:
    with sqlite_connection(write=True) as connection:
        connection.execute(
            "DELETE FROM spotify_catalog_worker_lock WHERE worker_name = ? AND owner = ?",
            (worker_name, owner),
        )
        connection.commit()


def _load_local_access_token(
    *,
    user_lister: Callable[..., list[dict[str, Any]]] = list_spotify_auth_users,
    token_refresher: Callable[..., dict[str, Any]] = refresh_access_token_if_needed,
) -> tuple[str | None, str | None]:
    users = user_lister(active_only=True, limit=1)
    if not users:
        return None, "No active Spotify auth user found."
    user_id = str(users[0].get("user_id") or "").strip()
    if not user_id:
        return None, "Active Spotify auth user has no user_id."
    try:
        token_row = token_refresher(user_id)
    except SpotifyTokenStoreError as exc:
        return None, str(exc)
    access_token = str(token_row.get("access_token") or "").strip()
    if not access_token:
        return None, "Spotify access token is unavailable."
    return access_token, None


def run_spotify_track_metadata_worker(
    *,
    now: datetime | None = None,
    owner: str | None = None,
    backfill_runner: Callable[..., dict[str, Any]] = run_spotify_catalog_backfill,
    user_lister: Callable[..., list[dict[str, Any]]] = list_spotify_auth_users,
    token_refresher: Callable[..., dict[str, Any]] = refresh_access_token_if_needed,
    canary_runner: Callable[..., dict[str, Any]] = run_spotify_track_metadata_canary,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    worker_name = SPOTIFY_TRACK_METADATA_WORKER
    started = now or _utc_now()
    started_at = _iso_utc(started)

    state = _worker_state(worker_name)
    cooldown_until_text = str(state.get("cooldown_until") or "") if state else _current_cooldown_until(worker_name)
    cooldown_until = _parse_iso_utc(cooldown_until_text)
    if cooldown_until is not None and cooldown_until > started:
        invocation_id = _insert_invocation(
            worker_name=worker_name,
            started_at=started_at,
            status="skipped_cooldown",
            skip_reason="cooldown_active",
        )
        completed_at = _iso_utc(_utc_now())
        _finish_invocation(
            invocation_id=invocation_id,
            completed_at=completed_at,
            status="skipped_cooldown",
            skip_reason="cooldown_active",
            cooldown_until=cooldown_until_text,
        )
        return {
            "worker_name": worker_name,
            "status": "skipped_cooldown",
            "skip_reason": "cooldown_active",
            "cooldown_until": cooldown_until_text,
            "invocation_id": invocation_id,
        }

    request_budget_cooldown_until, recent_requests, backfill_kwargs = _request_budget_adjusted_config(
        now=started,
        config=TRACK_METADATA_WORKER_CONFIG,
    )
    if request_budget_cooldown_until is not None:
        invocation_id = _insert_invocation(
            worker_name=worker_name,
            started_at=started_at,
            status="skipped_request_budget",
            skip_reason="request_budget_cooldown",
        )
        completed_at = _iso_utc(_utc_now())
        result = {
            "recent_requests_60m": recent_requests,
            "request_budget_soft_limit": REQUEST_BUDGET_SOFT_LIMIT,
            "request_budget_hard_limit": REQUEST_BUDGET_HARD_LIMIT,
        }
        error_text = (
            f"Skipped: {recent_requests} Spotify requests in the last 60 minutes; "
            f"cooldown until {request_budget_cooldown_until}."
        )
        _finish_invocation(
            invocation_id=invocation_id,
            completed_at=completed_at,
            status="skipped_request_budget",
            skip_reason="request_budget_cooldown",
            result=result,
            cooldown_until=request_budget_cooldown_until,
            error=error_text,
        )
        _upsert_worker_state(
            worker_name=worker_name,
            updated_at=completed_at,
            cooldown_until=request_budget_cooldown_until,
            last_started_at=started_at,
            last_completed_at=completed_at,
            last_status="skipped_request_budget",
            last_run_id=None,
            last_result=result,
            last_error=error_text,
        )
        return {
            "worker_name": worker_name,
            "status": "skipped_request_budget",
            "skip_reason": "request_budget_cooldown",
            "cooldown_until": request_budget_cooldown_until,
            "recent_requests_60m": recent_requests,
            "invocation_id": invocation_id,
            "message": error_text,
        }

    should_canary = _needs_post_cooldown_canary(state=state, now=started)
    lock_owner = owner or _owner_id()
    if not _acquire_worker_lock(worker_name=worker_name, owner=lock_owner, now=started):
        invocation_id = _insert_invocation(
            worker_name=worker_name,
            started_at=started_at,
            status="skipped_overlap",
            skip_reason="worker_lock_active",
        )
        completed_at = _iso_utc(_utc_now())
        _finish_invocation(
            invocation_id=invocation_id,
            completed_at=completed_at,
            status="skipped_overlap",
            skip_reason="worker_lock_active",
        )
        return {
            "worker_name": worker_name,
            "status": "skipped_overlap",
            "skip_reason": "worker_lock_active",
            "invocation_id": invocation_id,
        }

    invocation_id = _insert_invocation(worker_name=worker_name, started_at=started_at, status="running")
    try:
        access_token, token_error = _load_local_access_token(user_lister=user_lister, token_refresher=token_refresher)
        if not access_token:
            completed_at = _iso_utc(_utc_now())
            _finish_invocation(
                invocation_id=invocation_id,
                completed_at=completed_at,
                status="skipped_no_token",
                skip_reason="no_token",
                error=token_error,
            )
            _upsert_worker_state(
                worker_name=worker_name,
                updated_at=completed_at,
                cooldown_until=None,
                last_started_at=started_at,
                last_completed_at=completed_at,
                last_status="skipped_no_token",
                last_run_id=None,
                last_result=None,
                last_error=token_error,
            )
            return {
                "worker_name": worker_name,
                "status": "skipped_no_token",
                "skip_reason": "no_token",
                "error": token_error,
                "invocation_id": invocation_id,
            }

        canary_succeeded = False
        if should_canary:
            canary_attempt = {
                "event": "canary_attempt",
                "status": "started",
                "worker_name": worker_name,
            }
            if progress_callback is not None:
                progress_callback(canary_attempt)
            canary_result = canary_runner(
                access_token=access_token,
                market=str(backfill_kwargs.get("market") or "US"),
            )
            canary_event_base = {
                "source_track_id": canary_result.get("source_track_id"),
                "spotify_track_id": canary_result.get("spotify_track_id"),
                "status_code": canary_result.get("status_code"),
                "retry_after": canary_result.get("retry_after_seconds"),
                "retry_after_seconds": canary_result.get("retry_after_seconds"),
                "requests_total": int(canary_result.get("requests_total") or 0),
                "requests_429": int(canary_result.get("requests_429") or 0),
            }
            canary_status = str(canary_result.get("status") or "")
            if canary_status == "skipped_no_candidate":
                if progress_callback is not None:
                    progress_callback({"event": "canary_skipped_no_candidate", "status": "skipped", **canary_event_base})
            elif canary_status == "success":
                canary_succeeded = True
                if progress_callback is not None:
                    progress_callback({"event": "canary_success", "status": "success", **canary_event_base})
            elif canary_status == "rate_limited":
                completed = _utc_now()
                completed_at = _iso_utc(completed)
                consecutive_canary_429s = int(
                    (state or {}).get("consecutive_post_cooldown_canary_429s") or 0
                ) + 1
                fallback_cooldown_seconds = _post_cooldown_canary_fallback_seconds(consecutive_canary_429s)
                retry_after_seconds = float(canary_result.get("max_retry_after_seconds") or 0.0)
                result = {
                    "status": "skipped_canary_rate_limited",
                    "stop_reason": "post_cooldown_canary_429",
                    "requests_total": int(canary_result.get("requests_total") or 1),
                    "requests_429": int(canary_result.get("requests_429") or 1),
                    "tracks_fetched": 0,
                    "tracks_upserted": 0,
                    "max_retry_after_seconds": retry_after_seconds,
                    "retry_after_seconds": retry_after_seconds,
                    "fallback_cooldown_seconds": fallback_cooldown_seconds,
                    "consecutive_post_cooldown_canary_429s": consecutive_canary_429s,
                    "last_error": str(canary_result.get("last_error") or "Post-cooldown canary hit Spotify 429."),
                }
                cooldown_until = _cooldown_until_from_result(result=result, now=completed)
                if progress_callback is not None:
                    progress_callback(
                        {
                            "event": "canary_rate_limited",
                            "status": "rate_limited",
                            "stop_reason": "post_cooldown_canary_429",
                            "cooldown_until": cooldown_until,
                            "consecutive_post_cooldown_canary_429s": consecutive_canary_429s,
                            "fallback_cooldown_seconds": fallback_cooldown_seconds,
                            **canary_event_base,
                        }
                    )
                _finish_invocation(
                    invocation_id=invocation_id,
                    completed_at=completed_at,
                    status="skipped_canary_rate_limited",
                    result=result,
                    cooldown_until=cooldown_until,
                    error=str(result["last_error"]),
                )
                _upsert_worker_state(
                    worker_name=worker_name,
                    updated_at=completed_at,
                    cooldown_until=cooldown_until,
                    last_started_at=started_at,
                    last_completed_at=completed_at,
                    last_status="skipped_canary_rate_limited",
                    last_run_id=None,
                    last_result=result,
                    last_error=str(result["last_error"]),
                    consecutive_post_cooldown_canary_429s=consecutive_canary_429s,
                )
                return {
                    "worker_name": worker_name,
                    "status": "skipped_canary_rate_limited",
                    "stop_reason": "post_cooldown_canary_429",
                    "requests_total": result["requests_total"],
                    "requests_429": result["requests_429"],
                    "retry_after_seconds": retry_after_seconds,
                    "fallback_cooldown_seconds": fallback_cooldown_seconds,
                    "consecutive_post_cooldown_canary_429s": consecutive_canary_429s,
                    "tracks_fetched": 0,
                    "tracks_upserted": 0,
                    "cooldown_until": cooldown_until,
                    "invocation_id": invocation_id,
                }
            else:
                completed_at = _iso_utc(_utc_now())
                error_text = str(canary_result.get("last_error") or "Post-cooldown canary failed.")
                result = {
                    "status": "skipped_canary_failed",
                    "stop_reason": "post_cooldown_canary_failed",
                    "requests_total": int(canary_result.get("requests_total") or 1),
                    "requests_429": 0,
                    "tracks_fetched": 0,
                    "tracks_upserted": 0,
                    "last_error": error_text,
                }
                if progress_callback is not None:
                    progress_callback({"event": "canary_failed_non_429", "status": "failed", **canary_event_base})
                _finish_invocation(
                    invocation_id=invocation_id,
                    completed_at=completed_at,
                    status="skipped_canary_failed",
                    result=result,
                    error=error_text,
                )
                _upsert_worker_state(
                    worker_name=worker_name,
                    updated_at=completed_at,
                    cooldown_until=None,
                    last_started_at=started_at,
                    last_completed_at=completed_at,
                    last_status="skipped_canary_failed",
                    last_run_id=None,
                    last_result=result,
                    last_error=error_text,
                )
                return {
                    "worker_name": worker_name,
                    "status": "skipped_canary_failed",
                    "stop_reason": "post_cooldown_canary_failed",
                    "requests_total": result["requests_total"],
                    "requests_429": 0,
                    "tracks_fetched": 0,
                    "tracks_upserted": 0,
                    "error": error_text,
                    "invocation_id": invocation_id,
                }

        if progress_callback is not None:
            backfill_kwargs["progress_callback"] = progress_callback
        result = backfill_runner(access_token=access_token, **backfill_kwargs)
        try:
            result["release_track_duration_repair"] = repair_release_track_durations_from_spotify_catalog(apply=True)
        except Exception as exc:
            result["release_track_duration_repair"] = {
                "ok": False,
                "error": str(exc),
                "performed_action": "none",
            }
        completed = _utc_now()
        completed_at = _iso_utc(completed)
        status = str(result.get("status") or "failed")
        cooldown_until = _cooldown_until_from_result(result=result, now=completed)
        reset_canary_429s = canary_succeeded and status in {"ok", "partial"} and str(result.get("stop_reason") or "") != "rate_limited"
        _finish_invocation(
            invocation_id=invocation_id,
            completed_at=completed_at,
            status=status,
            result=result,
            cooldown_until=cooldown_until,
            error=str(result.get("last_error") or "") or None,
        )
        _upsert_worker_state(
            worker_name=worker_name,
            updated_at=completed_at,
            cooldown_until=cooldown_until,
            last_started_at=started_at,
            last_completed_at=completed_at,
            last_status=status,
            last_run_id=int(result["run_id"]) if result.get("run_id") is not None else None,
            last_result=result,
            last_error=str(result.get("last_error") or "") or None,
            consecutive_post_cooldown_canary_429s=0 if reset_canary_429s else None,
        )
        return {
            "worker_name": worker_name,
            "status": status,
            "cooldown_until": cooldown_until,
            "invocation_id": invocation_id,
            "backfill_run_id": result.get("run_id"),
            "requests_total": result.get("requests_total"),
            "requests_429": result.get("requests_429"),
            "tracks_fetched": result.get("tracks_fetched"),
            "tracks_upserted": result.get("tracks_upserted"),
            "stop_reason": result.get("stop_reason"),
        }
    except Exception as exc:
        completed_at = _iso_utc(_utc_now())
        _finish_invocation(
            invocation_id=invocation_id,
            completed_at=completed_at,
            status="failed",
            error=str(exc),
        )
        _upsert_worker_state(
            worker_name=worker_name,
            updated_at=completed_at,
            cooldown_until=None,
            last_started_at=started_at,
            last_completed_at=completed_at,
            last_status="failed",
            last_run_id=None,
            last_result=None,
            last_error=str(exc),
        )
        return {
            "worker_name": worker_name,
            "status": "failed",
            "error": str(exc),
            "invocation_id": invocation_id,
        }
    finally:
        _release_worker_lock(worker_name=worker_name, owner=lock_owner)
