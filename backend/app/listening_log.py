from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Literal, TypedDict

from backend.app.db import sqlite_connection


ListeningLogSourceFilter = Literal["all", "api", "history", "both"]
ListeningLogSourceLabel = Literal["api", "history", "both"]


class ListeningLogItem(TypedDict, total=False):
    event_id: int
    track_id: str | None
    track_name: str | None
    artist_name: str | None
    album_name: str | None
    album_id: str | None
    album_url: str | None
    duration_ms: int | None
    duration_seconds: float | None
    uri: str | None
    url: str | None
    image_url: str | None
    spotify_played_at: str | None
    spotify_played_at_unix_ms: int | None
    played_at_gap_ms: int | None
    estimated_played_ms: int | None
    estimated_played_seconds: float | None
    estimated_completion_ratio: float | None
    spotify_context_type: str | None
    spotify_context_uri: str | None
    source_label: ListeningLogSourceLabel
    has_recent_source: bool
    has_history_source: bool
    raw_spotify_recent_id: int | None
    raw_spotify_history_id: int | None
    raw_listenlab_player_play_id: int | None
    timing_source: str | None
    matched_state: str | None
    spotify_skipped: bool | None
    spotify_shuffle: bool | None
    spotify_offline: bool | None


class ListeningLogPayload(TypedDict):
    items: list[ListeningLogItem]
    limit: int
    offset: int
    has_more: bool
    source_filter: ListeningLogSourceFilter
    liked_only: bool


def _parse_payload(raw_payload_json: str | None) -> dict[str, Any]:
    if not raw_payload_json:
        return {}
    try:
        payload = json.loads(raw_payload_json)
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _positive_int(value: Any) -> int | None:
    return int(value) if isinstance(value, int) and value >= 0 else None


def _iso_to_unix_ms(value: str | None) -> int | None:
    if not isinstance(value, str):
        return None
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def _spotify_entity_url(kind: Literal["track", "album"], entity_id: str | None) -> str | None:
    return f"https://open.spotify.com/{kind}/{entity_id}" if entity_id else None


def _spotify_url_from_payload(payload: dict[str, Any], *, kind: Literal["track", "album"]) -> str | None:
    track_payload = payload.get("track") if isinstance(payload.get("track"), dict) else {}
    if kind == "track":
        external_urls = track_payload.get("external_urls") if isinstance(track_payload.get("external_urls"), dict) else {}
        url = external_urls.get("spotify")
        return str(url) if url else None

    album_payload = track_payload.get("album") if isinstance(track_payload.get("album"), dict) else {}
    external_urls = album_payload.get("external_urls") if isinstance(album_payload.get("external_urls"), dict) else {}
    url = external_urls.get("spotify")
    return str(url) if url else None


def _first_album_image_url(*payloads: dict[str, Any], album_images_json: str | None) -> str | None:
    for payload in payloads:
        track_payload = payload.get("track") if isinstance(payload.get("track"), dict) else {}
        album_payload = track_payload.get("album") if isinstance(track_payload.get("album"), dict) else {}
        images = album_payload.get("images") if isinstance(album_payload.get("images"), list) else []
        for image in images:
            if isinstance(image, dict) and image.get("url"):
                return str(image["url"])
        if payload.get("image_url"):
            return str(payload["image_url"])

    if album_images_json:
        try:
            images = json.loads(album_images_json)
        except ValueError:
            images = []
        if isinstance(images, list):
            for image in images:
                if isinstance(image, dict) and image.get("url"):
                    return str(image["url"])
    return None


def query_listening_log(
    *,
    limit: int = 50,
    offset: int = 0,
    source_filter: ListeningLogSourceFilter = "all",
    user_id: str | None = None,
    liked_only: bool = False,
) -> ListeningLogPayload:
    bounded_limit = max(1, min(int(limit), 200))
    bounded_offset = max(0, int(offset))
    normalized_source_filter: ListeningLogSourceFilter = (
        source_filter if source_filter in {"all", "api", "history", "both"} else "all"
    )

    where_clauses: list[str] = []
    query_parameters: list[Any] = []
    if normalized_source_filter == "api":
        where_clauses.append("v.raw_spotify_recent_id IS NOT NULL AND v.raw_spotify_history_id IS NULL")
    elif normalized_source_filter == "history":
        where_clauses.append("v.raw_spotify_history_id IS NOT NULL AND v.raw_spotify_recent_id IS NULL")
    elif normalized_source_filter == "both":
        where_clauses.append("v.raw_spotify_recent_id IS NOT NULL AND v.raw_spotify_history_id IS NOT NULL")
    apply_liked_filter = bool(liked_only and user_id)
    if apply_liked_filter:
        where_clauses.append(
            """
            EXISTS (
              SELECT 1
              FROM spotify_liked_track_cache liked
              WHERE liked.user_id = ?
                AND liked.spotify_track_id = v.spotify_track_id
                AND liked.is_liked = 1
            )
            """
        )
        query_parameters.append(str(user_id))
    where_clause = "".join(f" AND ({clause})" for clause in where_clauses)

    with sqlite_connection(row_factory=None) as connection:
        rows = connection.execute(
            f"""
            SELECT
              v.id,
              v.canonical_ended_at,
              v.canonical_ms_played,
              v.canonical_context_type,
              v.canonical_context_uri,
              v.spotify_track_id,
              v.spotify_track_uri,
              v.spotify_album_id,
              v.track_name_canonical,
              v.artist_name_canonical,
              v.album_name_canonical,
              v.timing_source,
              v.matched_state,
              v.canonical_skipped,
              v.canonical_shuffle,
              v.canonical_offline,
              v.raw_spotify_recent_id,
              v.raw_spotify_history_id,
              v.raw_listenlab_player_play_id,
              rr.track_duration_ms,
              rr.raw_payload_json,
              rh.raw_payload_json,
              rp.track_duration_ms,
              rp.raw_payload_json,
              stc.duration_ms,
              COALESCE(v.spotify_album_id, stc.album_id),
              sac.images_json
            FROM v_fact_play_event_with_sources v
            LEFT JOIN raw_spotify_recent rr
              ON rr.id = v.raw_spotify_recent_id
            LEFT JOIN raw_spotify_history rh
              ON rh.id = v.raw_spotify_history_id
            LEFT JOIN raw_listenlab_player_play rp
              ON rp.id = v.raw_listenlab_player_play_id
            LEFT JOIN spotify_track_catalog stc
              ON stc.spotify_track_id = v.spotify_track_id
            LEFT JOIN spotify_album_catalog sac
              ON sac.spotify_album_id = COALESCE(v.spotify_album_id, stc.album_id)
            WHERE v.canonical_ended_at IS NOT NULL
              {where_clause}
            ORDER BY v.canonical_ended_at DESC, v.id DESC
            LIMIT ?
            OFFSET ?
            """,
            (*query_parameters, bounded_limit + 1, bounded_offset),
        ).fetchall()

    has_more = len(rows) > bounded_limit
    items: list[ListeningLogItem] = []
    for index, row in enumerate(rows[:bounded_limit]):
        (
            event_id,
            canonical_ended_at,
            canonical_ms_played,
            canonical_context_type,
            canonical_context_uri,
            spotify_track_id,
            spotify_track_uri,
            spotify_album_id,
            track_name_canonical,
            artist_name_canonical,
            album_name_canonical,
            timing_source,
            matched_state,
            canonical_skipped,
            canonical_shuffle,
            canonical_offline,
            raw_spotify_recent_id,
            raw_spotify_history_id,
            raw_listenlab_player_play_id,
            recent_duration_ms,
            recent_payload_json,
            history_payload_json,
            player_duration_ms,
            player_payload_json,
            catalog_duration_ms,
            effective_album_id,
            album_images_json,
        ) = row

        has_recent_source = raw_spotify_recent_id is not None
        has_history_source = raw_spotify_history_id is not None
        source_label: ListeningLogSourceLabel = (
            "both" if has_recent_source and has_history_source else "history" if has_history_source else "api"
        )

        played_at_unix_ms = _iso_to_unix_ms(canonical_ended_at)
        next_older_ended_at = rows[index + 1][1] if index + 1 < len(rows) else None
        next_older_played_at_unix_ms = _iso_to_unix_ms(next_older_ended_at)
        played_at_gap_ms = (
            max(0, played_at_unix_ms - next_older_played_at_unix_ms)
            if played_at_unix_ms is not None and next_older_played_at_unix_ms is not None
            else None
        )

        estimated_played_ms = int(canonical_ms_played) if isinstance(canonical_ms_played, int) else None
        duration_ms = (
            _positive_int(recent_duration_ms)
            or _positive_int(player_duration_ms)
            or _positive_int(catalog_duration_ms)
        )
        estimated_completion_ratio = (
            round(min(1.0, estimated_played_ms / duration_ms), 4)
            if isinstance(estimated_played_ms, int) and isinstance(duration_ms, int) and duration_ms > 0
            else None
        )
        recent_payload = _parse_payload(recent_payload_json)
        history_payload = _parse_payload(history_payload_json)
        player_payload = _parse_payload(player_payload_json)
        album_id = effective_album_id or spotify_album_id

        items.append(
            {
                "event_id": int(event_id),
                "track_id": spotify_track_id,
                "track_name": track_name_canonical,
                "artist_name": artist_name_canonical,
                "album_name": album_name_canonical,
                "album_id": album_id,
                "album_url": _spotify_url_from_payload(recent_payload, kind="album") or _spotify_entity_url("album", album_id),
                "duration_ms": duration_ms,
                "duration_seconds": round(duration_ms / 1000.0, 3) if isinstance(duration_ms, int) and duration_ms >= 0 else None,
                "uri": spotify_track_uri,
                "url": _spotify_url_from_payload(recent_payload, kind="track") or _spotify_entity_url("track", spotify_track_id),
                "image_url": _first_album_image_url(
                    recent_payload,
                    player_payload,
                    history_payload,
                    album_images_json=album_images_json,
                ),
                "spotify_played_at": canonical_ended_at,
                "spotify_played_at_unix_ms": played_at_unix_ms,
                "played_at_gap_ms": played_at_gap_ms,
                "estimated_played_ms": estimated_played_ms,
                "estimated_played_seconds": round(estimated_played_ms / 1000.0, 3) if isinstance(estimated_played_ms, int) and estimated_played_ms >= 0 else None,
                "estimated_completion_ratio": estimated_completion_ratio,
                "spotify_context_type": canonical_context_type,
                "spotify_context_uri": canonical_context_uri,
                "source_label": source_label,
                "has_recent_source": has_recent_source,
                "has_history_source": has_history_source,
                "raw_spotify_recent_id": int(raw_spotify_recent_id) if isinstance(raw_spotify_recent_id, int) else None,
                "raw_spotify_history_id": int(raw_spotify_history_id) if isinstance(raw_spotify_history_id, int) else None,
                "raw_listenlab_player_play_id": int(raw_listenlab_player_play_id) if isinstance(raw_listenlab_player_play_id, int) else None,
                "timing_source": timing_source,
                "matched_state": matched_state,
                "spotify_skipped": bool(canonical_skipped) if canonical_skipped is not None else None,
                "spotify_shuffle": bool(canonical_shuffle) if canonical_shuffle is not None else None,
                "spotify_offline": bool(canonical_offline) if canonical_offline is not None else None,
            }
        )

    return {
        "items": items,
        "limit": bounded_limit,
        "offset": bounded_offset,
        "has_more": has_more,
        "source_filter": normalized_source_filter,
        "liked_only": apply_liked_filter,
    }
