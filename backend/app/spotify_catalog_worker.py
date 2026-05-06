from __future__ import annotations

import json
import os
import socket
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from backend.app.db import list_spotify_auth_users, sqlite_connection
from backend.app.spotify_catalog_backfill import run_spotify_catalog_backfill
from backend.app.spotify_token_store import SpotifyTokenStoreError, refresh_access_token_if_needed


SPOTIFY_TRACK_METADATA_WORKER = "spotify_track_metadata"
STALE_LOCK_SECONDS = 2 * 60 * 60
FALLBACK_RATE_LIMIT_COOLDOWN_SECONDS = 60 * 60
REQUEST_BUDGET_WINDOW_SECONDS = 60 * 60
REQUEST_BUDGET_SOFT_LIMIT = 550
REQUEST_BUDGET_HARD_LIMIT = 650
REQUEST_BUDGET_SOFT_COOLDOWN_SECONDS = 15 * 60
REQUEST_BUDGET_HARD_COOLDOWN_SECONDS = 30 * 60

TRACK_METADATA_WORKER_CONFIG: dict[str, Any] = {
    "target": "tracks",
    "run_mode": "metadata_only",
    "reason": "identity_metadata",
    "album_tracklist_policy": "none",
    "include_albums": False,
    "limit": 250,
    "max_requests": 275,
    "max_runtime_seconds": 900,
    "request_delay_seconds": 2.0,
    "market": "US",
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
) -> None:
    with sqlite_connection(write=True) as connection:
        connection.execute(
            """
            INSERT INTO spotify_catalog_worker_state (
              worker_name,
              cooldown_until,
              last_started_at,
              last_completed_at,
              last_status,
              last_run_id,
              last_result_json,
              last_error,
              updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(worker_name) DO UPDATE SET
              cooldown_until = excluded.cooldown_until,
              last_started_at = excluded.last_started_at,
              last_completed_at = excluded.last_completed_at,
              last_status = excluded.last_status,
              last_run_id = excluded.last_run_id,
              last_result_json = excluded.last_result_json,
              last_error = excluded.last_error,
              updated_at = excluded.updated_at
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
            ),
        )
        connection.commit()


def _cooldown_until_from_result(*, result: dict[str, Any], now: datetime) -> str | None:
    if str(result.get("stop_reason") or "") != "rate_limited":
        return None
    retry_after_seconds = float(result.get("max_retry_after_seconds") or 0.0)
    cooldown_seconds = retry_after_seconds if retry_after_seconds > 0 else FALLBACK_RATE_LIMIT_COOLDOWN_SECONDS
    return _iso_utc(now + timedelta(seconds=cooldown_seconds))


def _current_cooldown_until(worker_name: str) -> str | None:
    with sqlite_connection() as connection:
        row = connection.execute(
            "SELECT cooldown_until FROM spotify_catalog_worker_state WHERE worker_name = ?",
            (worker_name,),
        ).fetchone()
    return str(row[0]) if row and row[0] else None


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
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    worker_name = SPOTIFY_TRACK_METADATA_WORKER
    started = now or _utc_now()
    started_at = _iso_utc(started)

    cooldown_until_text = _current_cooldown_until(worker_name)
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

    request_budget_cooldown_until, recent_requests = _request_budget_cooldown(now=started)
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

        backfill_kwargs = dict(TRACK_METADATA_WORKER_CONFIG)
        if progress_callback is not None:
            backfill_kwargs["progress_callback"] = progress_callback
        result = backfill_runner(access_token=access_token, **backfill_kwargs)
        completed = _utc_now()
        completed_at = _iso_utc(completed)
        status = str(result.get("status") or "failed")
        cooldown_until = _cooldown_until_from_result(result=result, now=completed)
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
