from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.db import apply_pending_migrations, ensure_sqlite_db
from backend.app.spotify_catalog_worker import run_spotify_track_metadata_worker


def _print_json_line(payload: dict[str, object]) -> None:
    print(json.dumps(payload, ensure_ascii=True, sort_keys=True), flush=True)


def _print_terminal_line(text: str) -> None:
    print(text, flush=True)


def _json_line_writer(path: str | None) -> Callable[[dict[str, object]], None] | None:
    if not path:
        return None
    output_path = Path(path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    def _write(payload: dict[str, object]) -> None:
        with output_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True))
            handle.write("\n")

    return _write


def _parse_iso_utc(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _seconds_until(value: object, *, now: Callable[[], datetime]) -> float:
    cooldown_until = _parse_iso_utc(value)
    if cooldown_until is None:
        return 0.0
    return max(0.0, (cooldown_until - now().astimezone(UTC)).total_seconds())


def _human_sleep(seconds: float) -> str:
    rounded = max(0, int(round(seconds)))
    if rounded >= 60:
        return f"{int(round(rounded / 60.0))}m"
    return f"{rounded}s"


def _final_result_with_message(result: dict[str, object]) -> dict[str, object]:
    enriched = dict(result)
    run_id = enriched.get("backfill_run_id")
    status = str(enriched.get("status") or "unknown")
    tracks_fetched = int(enriched.get("tracks_fetched") or 0)
    requests_total = int(enriched.get("requests_total") or 0)
    requests_429 = int(enriched.get("requests_429") or 0)
    stop_reason = enriched.get("stop_reason")
    if run_id is None:
        message = f"Worker finished {status}: {tracks_fetched} tracks fetched, {requests_total} requests, {requests_429} rate limits"
    else:
        message = f"Run {run_id} finished {status}: {tracks_fetched} tracks fetched, {requests_total} requests, {requests_429} rate limits"
    if stop_reason:
        message = f"{message}, stop_reason={stop_reason}"
    enriched["message"] = message
    return enriched


def _terminal_summary(payload: dict[str, object]) -> str | None:
    event = str(payload.get("event") or "")
    if event == "start":
        run_id = payload.get("run_id")
        worker_config = payload.get("worker_config") if isinstance(payload.get("worker_config"), dict) else {}
        limit = worker_config.get("limit") if isinstance(worker_config, dict) else None
        return f"run {run_id} start limit={limit}"
    if event == "progress":
        run_id = payload.get("run_id")
        return (
            f"run {run_id} progress fetched={int(payload.get('tracks_fetched') or 0)} "
            f"req={int(payload.get('requests_total') or 0)} 429={int(payload.get('requests_429') or 0)}"
        )
    status = str(payload.get("status") or "")
    if status:
        run_id = payload.get("backfill_run_id") or payload.get("run_id")
        return (
            f"run {run_id} {status} fetched={int(payload.get('tracks_fetched') or 0)} "
            f"req={int(payload.get('requests_total') or 0)} 429={int(payload.get('requests_429') or 0)}"
        )
    return None


def _build_progress_writer(
    *,
    jsonl_writer: Callable[[dict[str, object]], None] | None,
    terminal_writer: Callable[[str], None],
    condensed_terminal: bool,
    default_json_writer: Callable[[dict[str, object]], None],
) -> Callable[[dict[str, object]], None]:
    def _write(payload: dict[str, object]) -> None:
        if jsonl_writer is not None:
            jsonl_writer(payload)
        if condensed_terminal:
            summary = _terminal_summary(payload)
            if summary:
                terminal_writer(summary)
        elif jsonl_writer is None:
            default_json_writer(payload)

    return _write


def _emit_loop_event(
    *,
    event: str,
    jsonl_writer: Callable[[dict[str, object]], None] | None,
    terminal_writer: Callable[[str], None],
    message: str,
    **extra: object,
) -> None:
    payload = {"event": event, "message": message, **extra}
    if jsonl_writer is not None:
        jsonl_writer(payload)
    terminal_writer(message)


def _parse_args(args: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Spotify track metadata worker.")
    parser.add_argument("--loop", action="store_true", help="Run batches until stopped or max runtime is reached.")
    parser.add_argument("--max-runtime-minutes", type=float, default=None, help="Total loop wall-clock bound.")
    parser.add_argument("--between-runs-seconds", type=float, default=300.0, help="Sleep after a clean run.")
    parser.add_argument("--jsonl-output", type=str, default=None, help="Append full JSON-line event stream to this file.")
    return parser.parse_args(args)


def main(
    args: Sequence[str] | None = (),
    *,
    progress_writer: Callable[[dict[str, object]], None] = _print_json_line,
    terminal_writer: Callable[[str], None] = _print_terminal_line,
    worker_runner: Callable[..., dict[str, object]] = run_spotify_track_metadata_worker,
    sleeper: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> None:
    parsed = _parse_args(args)
    ensure_sqlite_db()
    apply_pending_migrations()

    jsonl_writer = _json_line_writer(parsed.jsonl_output)
    condensed_terminal = bool(parsed.loop or parsed.jsonl_output)
    emit_progress = _build_progress_writer(
        jsonl_writer=jsonl_writer,
        terminal_writer=terminal_writer,
        condensed_terminal=condensed_terminal,
        default_json_writer=progress_writer,
    )

    started_monotonic = monotonic()
    max_runtime_seconds = None
    if parsed.max_runtime_minutes is not None:
        max_runtime_seconds = max(0.0, float(parsed.max_runtime_minutes) * 60.0)

    def _remaining_runtime() -> float | None:
        if max_runtime_seconds is None:
            return None
        return max(0.0, max_runtime_seconds - (monotonic() - started_monotonic))

    def _sleep_or_stop(seconds: float, *, cooldown: bool = False) -> bool:
        remaining = _remaining_runtime()
        if remaining is not None and remaining <= 0:
            _emit_loop_event(
                event="stopped",
                jsonl_writer=jsonl_writer,
                terminal_writer=terminal_writer,
                message="stopped max_runtime",
                reason="max_runtime",
            )
            return False
        sleep_seconds = max(0.0, float(seconds))
        if remaining is not None and sleep_seconds > remaining:
            _emit_loop_event(
                event="stopped",
                jsonl_writer=jsonl_writer,
                terminal_writer=terminal_writer,
                message="stopped max_runtime",
                reason="max_runtime",
            )
            return False
        if cooldown:
            message = f"cooldown {_human_sleep(sleep_seconds)}"
            event = "cooldown_sleep"
        else:
            message = f"sleep {int(round(sleep_seconds))}s"
            event = "between_runs_sleep"
        _emit_loop_event(
            event=event,
            jsonl_writer=jsonl_writer,
            terminal_writer=terminal_writer,
            message=message,
            sleep_seconds=round(sleep_seconds, 3),
        )
        if sleep_seconds > 0:
            sleeper(sleep_seconds)
        return True

    try:
        while True:
            remaining = _remaining_runtime()
            if remaining is not None and remaining <= 0:
                _emit_loop_event(
                    event="stopped",
                    jsonl_writer=jsonl_writer,
                    terminal_writer=terminal_writer,
                    message="stopped max_runtime",
                    reason="max_runtime",
                )
                return

            result = _final_result_with_message(worker_runner(progress_callback=emit_progress))
            emit_progress(result)

            if not parsed.loop:
                return

            status = str(result.get("status") or "")
            stop_reason = str(result.get("stop_reason") or "")
            if status == "ok":
                if not _sleep_or_stop(float(parsed.between_runs_seconds), cooldown=False):
                    return
                continue

            if status in {"skipped_request_budget", "skipped_cooldown"} or (
                status == "partial" and stop_reason == "rate_limited"
            ):
                cooldown_seconds = _seconds_until(result.get("cooldown_until"), now=now)
                if not _sleep_or_stop(cooldown_seconds, cooldown=True):
                    return
                continue

            return
    except KeyboardInterrupt:
        _emit_loop_event(
            event="stopped",
            jsonl_writer=jsonl_writer,
            terminal_writer=terminal_writer,
            message="stopped ctrl_c",
            reason="ctrl_c",
        )


if __name__ == "__main__":
    main(args=None)
