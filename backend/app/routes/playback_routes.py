from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Request, status

from backend.app.auth.session import _require_user_id
from backend.app.auth.token import _require_token
from backend.app.db import (
    complete_ingest_run,
    insert_ingest_run,
    insert_listenlab_player_play,
    update_listenlab_player_play_progress,
)
from backend.app.play_event_projector import reconcile_fact_play_events_for_ingest_run
from backend.app.spotify_current_playback import get_current_playback_for_user
from backend.app.spotify_http import _fetch_spotify_profile
from backend.app.spotify_preview import (
    _fetch_album_representative_track,
    _fetch_artist_representative_track,
)
from backend.app.spotify_token_store import get_spotify_tokens

router = APIRouter(tags=["playback"])


@router.get("/auth/current-playback")
async def auth_current_playback(request: Request) -> dict[str, Any]:
    user_id = _require_user_id(request)
    return await get_current_playback_for_user(user_id)


@router.post("/auth/player-listen-event")
async def auth_player_listen_event(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    user_id = _require_user_id(request)
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    event_id = str(payload.get("event_id") or uuid.uuid4())
    track_uri = str(payload.get("track_uri") or "") or None
    track_id = str(payload.get("track_id") or "") or None
    if track_id is None and track_uri and track_uri.startswith("spotify:track:"):
        track_id = track_uri.split(":")[-1] or None
    if not track_id and not track_uri:
        raise HTTPException(status_code=400, detail="A Spotify track id or URI is required.")

    duration_ms_raw = payload.get("duration_ms")
    duration_ms = int(duration_ms_raw) if isinstance(duration_ms_raw, (int, float)) and duration_ms_raw >= 0 else None
    progress_ms_raw = payload.get("progress_ms")
    progress_ms = int(progress_ms_raw) if isinstance(progress_ms_raw, (int, float)) and progress_ms_raw >= 0 else 0
    if duration_ms is not None:
        progress_ms = min(progress_ms, duration_ms)
    confidence = str(payload.get("ms_played_confidence") or ("complete" if duration_ms and progress_ms >= duration_ms * 0.98 else "in_progress"))

    if payload.get("row_id") is not None:
        updated = update_listenlab_player_play_progress(
            row_id=int(payload["row_id"]),
            user_id=str(user_id),
            ms_played=progress_ms,
            ms_played_confidence=confidence,
        )
        if not updated:
            raise HTTPException(status_code=404, detail="ListenLab player event was not found.")
        return {"ok": True, "row_id": int(payload["row_id"]), "action": "updated"}

    run_id = f"listenlab-player-{uuid.uuid4()}"
    insert_ingest_run(
        run_id=run_id,
        source_type="listenlab_player",
        source_ref="web_player",
        started_at=now,
    )
    source_row_key = f"listenlab_player:{user_id}:{event_id}"
    auth_row = get_spotify_tokens(str(user_id))
    result = insert_listenlab_player_play(
        ingest_run_id=run_id,
        source_row_key=source_row_key,
        source_event_id=event_id,
        user_id=str(user_id),
        spotify_user_id=str(auth_row.get("spotify_user_id")) if auth_row else None,
        played_at=str(payload.get("played_at") or now),
        raw_payload_json=json.dumps(payload, separators=(",", ":")),
        spotify_track_id=track_id,
        spotify_track_uri=track_uri,
        spotify_album_id=str(payload.get("album_id") or "") or None,
        spotify_artist_ids_json=json.dumps(payload.get("artist_ids") or []),
        track_name_raw=str(payload.get("track_name") or "") or None,
        artist_name_raw=str(payload.get("artist_name") or "") or None,
        album_name_raw=str(payload.get("album_name") or "") or None,
        track_duration_ms=duration_ms,
        ms_played=progress_ms,
        ms_played_confidence=confidence,
    )
    complete_ingest_run(
        run_id=run_id,
        completed_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        row_count=1,
        inserted_count=1 if result.get("action") == "inserted" else 0,
        duplicate_count=0 if result.get("action") == "inserted" else 1,
    )
    projection = reconcile_fact_play_events_for_ingest_run(source_type="listenlab_player", run_id=run_id)
    return {
        "ok": True,
        "row_id": int(result["row_id"]),
        "event_id": event_id,
        "action": result.get("action"),
        "projection": projection,
    }


@router.get("/preview/representative")
async def preview_representative(
    request: Request,
    kind: str,
    spotify_id: str,
) -> dict[str, Any]:
    token = _require_token(request)

    market: str | None = None
    try:
        profile = await _fetch_spotify_profile(token)
        market = profile.get("country")
    except HTTPException:
        market = None

    try:
        if kind == "artist":
            track = await _fetch_artist_representative_track(token, spotify_id, market=market)
        elif kind == "album":
            track = await _fetch_album_representative_track(token, spotify_id, market=market)
        else:
            raise HTTPException(status_code=400, detail="Unsupported preview kind.")
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return {"track": None, "reason": "spotify_rejected_lookup"}
        if exc.status_code == status.HTTP_404_NOT_FOUND:
            return {"track": None, "reason": "item_not_found"}
        if exc.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
            return {"track": None, "reason": "rate_limited"}
        if exc.status_code == status.HTTP_502_BAD_GATEWAY:
            return {"track": None, "reason": "spotify_lookup_failed"}
        raise

    if not track:
        return {"track": None, "reason": "no_representative_track"}

    return {"track": track, "reason": "ok"}
