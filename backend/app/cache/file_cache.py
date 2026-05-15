from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from backend.app.config import get_settings

logger = logging.getLogger("listenlabs.auth")


def _cache_dir() -> Path:
    path = Path(get_settings().cache_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _read_json_file(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_json_file(path: Path, payload: dict[str, Any]) -> None:
    try:
        path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    except OSError:
        logger.exception("Failed to write cache file: %s", path)
