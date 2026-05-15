from __future__ import annotations

import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from fastapi import HTTPException, status

SPOTIFY_RATE_LIMIT_STATE: dict[str, float | None] = {"cooldown_until": None}
SPOTIFY_RATE_LIMIT_COOLDOWN_SECONDS = 60
SPOTIFY_MAX_RETRY_AFTER_SECONDS = 600


def _spotify_cooldown_seconds_remaining() -> int:
    cooldown_until = SPOTIFY_RATE_LIMIT_STATE.get("cooldown_until")
    if cooldown_until is None:
        return 0
    return max(0, min(int(cooldown_until - time.time()), SPOTIFY_MAX_RETRY_AFTER_SECONDS))


def _enforce_spotify_cooldown() -> None:
    remaining = _spotify_cooldown_seconds_remaining()
    if remaining > 0:
        raise HTTPException(
            status_code=429,
            detail=_spotify_rate_limit_detail("Spotify is rate-limiting requests right now."),
        )


def _note_spotify_rate_limit(retry_after_seconds: int | None = None) -> None:
    candidate_seconds = retry_after_seconds or SPOTIFY_RATE_LIMIT_COOLDOWN_SECONDS
    cooldown_seconds = min(
        SPOTIFY_MAX_RETRY_AFTER_SECONDS,
        max(1, int(candidate_seconds)),
    )
    cooldown_until = time.time() + cooldown_seconds
    previous = SPOTIFY_RATE_LIMIT_STATE.get("cooldown_until")
    previous_until = float(previous or 0)
    max_allowed_until = time.time() + SPOTIFY_MAX_RETRY_AFTER_SECONDS
    if previous_until > max_allowed_until:
        previous_until = max_allowed_until
    SPOTIFY_RATE_LIMIT_STATE["cooldown_until"] = max(previous_until, cooldown_until)


def _parse_retry_after_seconds(retry_after_header: str | None) -> int | None:
    if not retry_after_header:
        return None
    value = retry_after_header.strip()
    if not value:
        return None
    if value.isdigit():
        return int(value)
    try:
        retry_after_date = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if retry_after_date.tzinfo is None:
        retry_after_date = retry_after_date.replace(tzinfo=timezone.utc)
    delta_seconds = int((retry_after_date - datetime.now(timezone.utc)).total_seconds())
    return max(1, delta_seconds)


def _spotify_rate_limit_detail(prefix: str) -> str:
    remaining = max(1, _spotify_cooldown_seconds_remaining())
    return f"{prefix} Try again in about {remaining} seconds."
