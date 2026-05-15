from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any


def _expires_at_from_expires_in(expires_in: int | str | None) -> str:
    seconds = int(expires_in or 0)
    if seconds <= 0:
        seconds = 3600
    return (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def _parse_iso_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _format_recent_track_played_at_for_route(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = _parse_iso_utc(value)
    except ValueError:
        return value
    milliseconds = int(parsed.microsecond / 1000)
    return parsed.strftime("%Y-%m-%dT%H:%M:%S.") + f"{milliseconds:03d}Z"
