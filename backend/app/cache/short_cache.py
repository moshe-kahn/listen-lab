from __future__ import annotations

import time
from typing import Any

SECTION_CACHE: dict[str, dict[str, Any]] = {}
SHORT_CACHE_TTL_SECONDS = 180


def _cache_key(section: str, user_id: str | None, limit: int) -> str:
    return f"{section}:{user_id or 'anonymous'}:{limit}"


def _get_short_cache(section: str, user_id: str | None, limit: int) -> Any | None:
    entry = SECTION_CACHE.get(_cache_key(section, user_id, limit))
    if not entry:
        return None
    if time.time() - float(entry.get("stored_at", 0)) > SHORT_CACHE_TTL_SECONDS:
        SECTION_CACHE.pop(_cache_key(section, user_id, limit), None)
        return None
    return entry.get("value")


def _set_short_cache(section: str, user_id: str | None, limit: int, value: Any) -> Any:
    SECTION_CACHE[_cache_key(section, user_id, limit)] = {
        "stored_at": time.time(),
        "value": value,
    }
    return value
