from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

from fastapi import Request

from backend.app.config import get_settings

logger = logging.getLogger("listenlabs.auth")
LOAD_PROGRESS: dict[str, dict[str, Any]] = {}
PROGRESS_LOG_FILE = "dashboard-progress.log"
PROGRESS_LOG_MAX_BYTES = 1_000_000
PROGRESS_LOG_KEEP_TAIL_BYTES = 400_000


def _progress_key(request: Request) -> str | None:
    user = request.session.get("spotify_user") or {}
    if user.get("id"):
        return str(user["id"])
    user_id = request.session.get("user_id")
    if user_id:
        return f"user:{user_id}"
    return None


def _set_load_progress(request: Request, phase: str, mode: str | None = None) -> None:
    key = _progress_key(request)
    if not key:
        return
    current = LOAD_PROGRESS.get(key)
    current_mode = mode or (str(current.get("mode")) if current and current.get("mode") else None)
    mode_prefix = f"mode={current_mode} " if current_mode else ""
    if current is None:
        LOAD_PROGRESS[key] = {
            "phase": phase,
            "mode": current_mode,
            "started_at": time.perf_counter(),
            "last_at_seconds": 0.0,
            "events": [{"phase": phase, "at_seconds": 0.0}],
        }
        _append_progress_log(key, f"total=0.0s delta=0.0s {mode_prefix}{phase}")
        return
    if current.get("phase") == phase:
        return
    if current_mode:
        current["mode"] = current_mode
    current["phase"] = phase
    started_at = float(current.get("started_at", time.perf_counter()))
    elapsed_seconds = round(time.perf_counter() - started_at, 1)
    previous_elapsed = float(current.get("last_at_seconds", 0.0))
    delta_seconds = round(max(0.0, elapsed_seconds - previous_elapsed), 1)
    current["last_at_seconds"] = elapsed_seconds
    current.setdefault("events", []).append(
        {"phase": phase, "at_seconds": elapsed_seconds}
    )
    _append_progress_log(key, f"total={elapsed_seconds:.1f}s delta={delta_seconds:.1f}s {mode_prefix}{phase}")


def _clear_load_progress(request: Request) -> None:
    key = _progress_key(request)
    if key:
        progress = LOAD_PROGRESS.get(key)
        if progress:
            elapsed_seconds = round(
                time.perf_counter() - float(progress.get("started_at", time.perf_counter())),
                1,
            )
            previous_elapsed = float(progress.get("last_at_seconds", 0.0))
            delta_seconds = round(max(0.0, elapsed_seconds - previous_elapsed), 1)
            mode = progress.get("mode")
            mode_prefix = f"mode={mode} " if mode else ""
            _append_progress_log(key, f"total={elapsed_seconds:.1f}s delta={delta_seconds:.1f}s {mode_prefix}complete")
        LOAD_PROGRESS.pop(key, None)


def _progress_log_path() -> Path:
    path = Path(get_settings().cache_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path / PROGRESS_LOG_FILE


def _append_progress_log(key: str, message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_path = _progress_log_path()
    try:
        if log_path.exists() and log_path.stat().st_size > PROGRESS_LOG_MAX_BYTES:
            raw = log_path.read_bytes()
            tail = raw[-PROGRESS_LOG_KEEP_TAIL_BYTES:]
            first_newline = tail.find(b"\n")
            if first_newline != -1:
                tail = tail[first_newline + 1:]
            header = f"[{timestamp}] [system] progress-log truncated keep_tail_bytes={PROGRESS_LOG_KEEP_TAIL_BYTES}\n".encode("utf-8")
            log_path.write_bytes(header + tail)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"[{timestamp}] [{key}] {message}\n")
    except OSError:
        logger.exception("Failed to append dashboard progress log.")
