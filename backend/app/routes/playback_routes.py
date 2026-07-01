from __future__ import annotations

import json
import logging
import re
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
from fastapi import APIRouter, Body, HTTPException, Query, Request, status

from backend.app.artist_album_evidence import list_artist_album_evidence, list_artist_tracks
from backend.app.album_family import build_album_family_context
from backend.app.artwork import resolve_artist_artwork
from backend.app.catalog_identity_promotion import promote_catalog_album_tracks_to_identity
from backend.app.auth.session import _require_local_data_session, _require_user_id
from backend.app.auth.token import _require_token
from backend.app.db import (
    complete_ingest_run,
    get_spotify_auth_record,
    insert_ingest_run,
    insert_listenlab_player_play,
    refresh_source_track_play_count_cache,
    sqlite_connection,
    update_listenlab_player_play_progress,
)
from backend.app.play_event_projector import reconcile_fact_play_events_for_ingest_run
from backend.app.playlist_index import (
    cache_playlist_track_page,
    cached_playlist_tracks,
    enrich_rows_with_playlist_membership_counts,
    fetch_playlist_track_page_from_spotify,
)
from backend.app.release_track_detail import ReleaseTrackDetailNotFound, get_release_track_detail
from backend.app.release_track_metadata import enrich_album_track_rows_with_release_metadata
from backend.app.spotify_catalog_backfill import (
    _upsert_album_track,
    _upsert_track_catalog,
    enqueue_spotify_catalog_backfill_items,
)
from backend.app.spotify_current_playback import get_current_playback_for_user
from backend.app.spotify_http import _fetch_spotify_profile, _spotify_client_credentials_token, _spotify_get
from backend.app.spotify_preview import (
    _fetch_album_representative_track,
    _fetch_artist_representative_track,
)
from backend.app.spotify_queue_playlist import (
    sync_queue_playlist,
    validate_queue_playlist_uris,
)
from backend.app.spotify_token_store import get_spotify_tokens

router = APIRouter(tags=["playback"])
logger = logging.getLogger("listenlabs.playback")


def _scope_set(scope_text: str | None) -> set[str]:
    return {scope.strip() for scope in str(scope_text or "").split() if scope.strip()}


def _json_list(value: Any) -> list[Any]:
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _spotify_track_id_from_uri(track_uri: str | None) -> str | None:
    if not track_uri or not track_uri.startswith("spotify:track:"):
        return None
    return track_uri.split(":")[-1] or None


def _require_playlist_modify_scope(user_id: str) -> None:
    granted_scopes = _scope_set((get_spotify_auth_record(str(user_id)) or {}).get("scopes"))
    if "playlist-modify-private" in granted_scopes or "playlist-modify-public" in granted_scopes:
        return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Spotify permission missing. Log out and log back in so ListenLab can request playlist modify access.",
    )


async def _spotify_post_json(access_token: str, url: str, payload: dict[str, Any]) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.post(
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
    if response.status_code == status.HTTP_204_NO_CONTENT:
        return {}
    if response.status_code >= 400:
        try:
            error_payload = response.json()
        except ValueError:
            error_payload = {"error": response.text[:160]}
        spotify_message = error_payload.get("error")
        if isinstance(spotify_message, dict):
            spotify_message = spotify_message.get("message") or spotify_message.get("reason")
        detail = str(spotify_message or response.text or "Spotify request failed.").strip()
        raise HTTPException(status_code=response.status_code, detail=detail)
    try:
        parsed = response.json()
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


async def _spotify_delete_json(access_token: str, url: str, payload: dict[str, Any]) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.request(
            "DELETE",
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
    if response.status_code >= 400:
        try:
            error_payload = response.json()
        except ValueError:
            error_payload = {"error": response.text[:160]}
        spotify_message = error_payload.get("error")
        if isinstance(spotify_message, dict):
            spotify_message = spotify_message.get("message") or spotify_message.get("reason")
        raise HTTPException(status_code=response.status_code, detail=str(spotify_message or response.text or "Spotify request failed.").strip())
    try:
        parsed = response.json()
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _artist_entries_for_album_evidence(artist_names: list[str], artist_ids: list[str] | None = None) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    normalized_ids = [str(artist_id or "").strip() for artist_id in (artist_ids or [])]
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        for index, artist_name in enumerate(artist_names):
            clean_name = str(artist_name or "").strip()
            if not clean_name:
                continue
            artist_id = normalized_ids[index] if index < len(normalized_ids) and normalized_ids[index] else None
            if not artist_id:
                row = connection.execute(
                    """
                    SELECT sa.external_id
                    FROM artist a
                    JOIN source_artist_map sam
                      ON sam.artist_id = a.id
                     AND sam.status = 'accepted'
                    JOIN source_artist sa
                      ON sa.id = sam.source_artist_id
                     AND sa.source_name = 'spotify'
                    WHERE lower(trim(a.canonical_name)) = lower(trim(?))
                      AND sa.external_id IS NOT NULL
                      AND trim(sa.external_id) != ''
                    ORDER BY sam.is_user_confirmed DESC, sam.confidence DESC, sam.id
                    LIMIT 1
                    """,
                    (clean_name,),
                ).fetchone()
                artist_id = str(row["external_id"]).strip() if row and row["external_id"] else None
            entries.append(
                {
                    "artist_id": artist_id,
                    "id": artist_id,
                    "name": clean_name,
                    "uri": f"spotify:artist:{artist_id}" if artist_id else None,
                    "url": f"https://open.spotify.com/artist/{artist_id}" if artist_id else None,
                    "image_url": None,
                }
            )
    return entries


def _enqueue_incomplete_artist_album_tracklists(items: list[dict[str, Any]]) -> dict[str, Any]:
    queue_items: list[dict[str, Any]] = []
    seen_album_ids: set[str] = set()
    for item in items:
        album_id = str(item.get("album_id") or "").strip()
        if not album_id or album_id in seen_album_ids:
            continue
        seen_album_ids.add(album_id)
        if item.get("tracklist_complete") is True:
            continue
        queue_items.append(
            {
                "entity_type": "album",
                "spotify_id": album_id,
                "reason": "tracklist_completion",
                "priority": 70,
            }
        )
    return enqueue_spotify_catalog_backfill_items(items=queue_items)


@router.get("/tracks/release-track/{release_track_id}")
async def tracks_release_track_detail(
    request: Request,
    release_track_id: int,
    context_spotify_track_id: str | None = Query(default=None),
) -> dict[str, Any]:
    _require_local_data_session(request)
    if release_track_id <= 0:
        raise HTTPException(status_code=400, detail="release_track_id must be a positive integer.")
    try:
        return get_release_track_detail(
            release_track_id,
            context_spotify_track_id=context_spotify_track_id,
        )
    except ReleaseTrackDetailNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


def _cached_album_id_for_track(spotify_track_id: str) -> str | None:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        row = connection.execute(
            """
            SELECT album_id
            FROM spotify_track_catalog
            WHERE spotify_track_id = ?
              AND album_id IS NOT NULL
              AND trim(album_id) != ''
              AND lower(COALESCE(last_status, '')) != 'error'
            """,
            (spotify_track_id,),
        ).fetchone()
    return str(row["album_id"]) if row and row["album_id"] else None


def _cached_album_track_rows(album_id: str) -> tuple[list[dict[str, Any]], bool]:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        album_row = connection.execute(
            """
            SELECT total_tracks
            FROM spotify_album_catalog
            WHERE spotify_album_id = ?
              AND lower(COALESCE(last_status, '')) != 'error'
            """,
            (album_id,),
        ).fetchone()
        rows = connection.execute(
            """
            SELECT spotify_track_id, disc_number, track_number, name, duration_ms, artists_json
            FROM spotify_album_track
            WHERE spotify_album_id = ?
              AND lower(COALESCE(last_status, '')) != 'error'
            ORDER BY COALESCE(disc_number, 0), COALESCE(track_number, 0), name, spotify_track_id
            """,
            (album_id,),
        ).fetchall()
    total_tracks = int(album_row["total_tracks"] or 0) if album_row and album_row["total_tracks"] is not None else None
    complete = bool(rows) and (total_tracks is None or len(rows) >= total_tracks)
    return [
        {
            "id": str(row["spotify_track_id"] or "") or None,
            "name": row["name"],
            "uri": f"spotify:track:{row['spotify_track_id']}" if row["spotify_track_id"] else None,
            "duration_ms": row["duration_ms"],
            "artists": _json_list(row["artists_json"]),
            "disc_number": row["disc_number"],
            "track_number": row["track_number"],
        }
        for row in rows
    ], complete


def _cached_album_track_rows_from_track_catalog(album_id: str) -> tuple[list[dict[str, Any]], bool]:
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        album_row = connection.execute(
            """
            SELECT total_tracks
            FROM spotify_album_catalog
            WHERE spotify_album_id = ?
              AND lower(COALESCE(last_status, '')) != 'error'
            """,
            (album_id,),
        ).fetchone()
        rows = connection.execute(
            """
            SELECT spotify_track_id, disc_number, track_number, name, duration_ms, artists_json
            FROM spotify_track_catalog
            WHERE album_id = ?
              AND lower(COALESCE(last_status, '')) != 'error'
            ORDER BY COALESCE(disc_number, 0), COALESCE(track_number, 0), name, spotify_track_id
            """,
            (album_id,),
        ).fetchall()
    total_tracks = int(album_row["total_tracks"] or 0) if album_row and album_row["total_tracks"] is not None else None
    complete = bool(rows) and (total_tracks is None or len(rows) >= total_tracks)
    return [
        {
            "id": str(row["spotify_track_id"] or "") or None,
            "name": row["name"],
            "uri": f"spotify:track:{row['spotify_track_id']}" if row["spotify_track_id"] else None,
            "duration_ms": row["duration_ms"],
            "artists": _json_list(row["artists_json"]),
            "disc_number": row["disc_number"],
            "track_number": row["track_number"],
        }
        for row in rows
    ], complete


def _enriched_album_items_with_family(
    *,
    user_id: str,
    album_id: str,
    items: list[dict[str, Any]],
    include_family: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    enriched = enrich_album_track_rows_with_release_metadata(
        items,
        refresh_dirty_clusters=False,
    )
    with sqlite_connection(row_factory=sqlite3.Row) as connection:
        album_row = connection.execute(
            """
            SELECT COALESCE(sac.name, ra.primary_name, sa.source_name_raw) AS album_name
            FROM source_album sa
            LEFT JOIN source_album_map sam
              ON sam.source_album_id = sa.id
             AND sam.status = 'accepted'
            LEFT JOIN release_album ra ON ra.id = sam.release_album_id
            LEFT JOIN spotify_album_catalog sac ON sac.spotify_album_id = sa.external_id
            WHERE sa.source_name = 'spotify' AND sa.external_id = ?
            LIMIT 1
            """,
            (album_id,),
        ).fetchone()
    album_name = str(album_row["album_name"] or "") if album_row else ""
    if re.search(r"\b(?:remix|rmx)\b", album_name, re.IGNORECASE):
        for item in enriched:
            if not item.get("release_track_cluster_candidate_type"):
                # The release itself proves remix semantics even when no single
                # original recording can be linked safely (for example a medley).
                item["release_track_cluster_candidate_type"] = "track_family_candidate"
                item["release_track_cluster_relationship_kind"] = "remix"
    if not include_family:
        return enrich_rows_with_playlist_membership_counts(user_id, enriched), None
    family_context = build_album_family_context(
        selected_spotify_album_id=album_id,
        selected_items=enriched,
    )
    if family_context is None:
        return enrich_rows_with_playlist_membership_counts(user_id, enriched), None
    family_payload = {key: value for key, value in family_context.items() if key != "items"}
    return enrich_rows_with_playlist_membership_counts(user_id, list(family_context["items"])), family_payload


async def _fetch_and_cache_album_tracks(access_token: str, album_id: str, market: str) -> list[dict[str, Any]]:
    fetched_at = _utc_now()
    items: list[dict[str, Any]] = []
    offset = 0
    limit = 50
    while True:
        payload = await _spotify_get(
            access_token,
            f"https://api.spotify.com/v1/albums/{album_id}/tracks",
            {"limit": limit, "offset": offset},
        )
        page_items = payload.get("items") if isinstance(payload.get("items"), list) else []
        for item in page_items:
            if isinstance(item, dict):
                items.append(item)
                _upsert_album_track(
                    album_id=album_id,
                    track=item,
                    market=market,
                    fetched_at=fetched_at,
                    last_status="ok",
                    last_error=None,
                )
        next_url = str(payload.get("next") or "")
        if not next_url or len(page_items) < limit:
            break
        offset += limit
    return items


@router.get("/auth/current-playback")
async def auth_current_playback(request: Request) -> dict[str, Any]:
    user_id = _require_user_id(request)
    return await get_current_playback_for_user(user_id)


@router.get("/auth/artist-albums")
async def auth_artist_albums(
    request: Request,
    artist_names: list[str] | None = Query(default=None),
    artist_ids: list[str] | None = Query(default=None),
    source_album_id: str | None = None,
    source_album_name: str | None = None,
) -> dict[str, Any]:
    user_id = _require_user_id(request)
    token = _require_token(request)
    try:
        normalized_artist_names = [str(name or "").strip() for name in (artist_names or []) if str(name or "").strip()]
        items = list_artist_album_evidence(
            artist_names=normalized_artist_names,
            source_album_id=source_album_id,
            source_album_name=source_album_name,
        )
        try:
            backfill_queue = _enqueue_incomplete_artist_album_tracklists(items)
        except Exception:
            backfill_queue = {"ok": False, "error": "artist_album_tracklist_enqueue_failed"}
        refresh_source_track_play_count_cache()
        tracks = list_artist_tracks(normalized_artist_names, artist_ids=artist_ids)
        artists = await resolve_artist_artwork(
            _artist_entries_for_album_evidence(normalized_artist_names, artist_ids),
            access_token=token,
            allow_spotify_fetch=True,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Artist album evidence could not be loaded.") from exc
    return {"items": items, "tracks": tracks, "artists": artists, "backfill_queue": backfill_queue}


@router.post("/auth/playback/queue-playlist/sync")
async def auth_sync_queue_playlist(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    user_id = _require_user_id(request)
    token = _require_token(request)
    uris = validate_queue_playlist_uris(payload.get("uris") if isinstance(payload, dict) else None)
    return await sync_queue_playlist(
        access_token=token,
        spotify_user_id=str(user_id),
        uris=uris,
    )


@router.get("/auth/playback/playlist-tracks")
async def auth_playback_playlist_tracks(
    request: Request,
    playlist_id: str,
    limit: int = Query(default=500, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    user_id = _require_user_id(request)
    token = _require_token(request)
    normalized_playlist_id = str(playlist_id or "").strip()
    if not normalized_playlist_id:
        raise HTTPException(status_code=400, detail="playlist_id is required.")

    cached = cached_playlist_tracks(str(user_id), normalized_playlist_id, limit=limit, offset=offset)
    if cached and (cached["items"] or cached.get("complete")):
        return {
            "playlist_id": normalized_playlist_id,
            "items": cached["items"],
            "total": cached["total"],
            "limit": limit,
            "offset": cached["offset"],
            "next_offset": cached["next_offset"],
            "has_more": cached["has_more"],
            "source": "cache",
        }

    try:
        payload = await fetch_playlist_track_page_from_spotify(
            token,
            normalized_playlist_id,
            limit=limit,
            offset=offset,
        )
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            public_payload: dict[str, Any] | None = None
            public_error: HTTPException | None = None
            try:
                public_token = await _spotify_client_credentials_token()
                public_payload = await fetch_playlist_track_page_from_spotify(
                    public_token,
                    normalized_playlist_id,
                    limit=limit,
                    offset=offset,
                )
            except HTTPException as fallback_exc:
                public_error = fallback_exc
            if public_payload is not None:
                cache_playlist_track_page(
                    str(user_id),
                    normalized_playlist_id,
                    public_payload["items"],
                    offset=offset,
                    total=public_payload.get("total"),
                )
                return {
                    "playlist_id": normalized_playlist_id,
                    "items": public_payload["items"],
                    "total": public_payload.get("total", len(public_payload["items"])),
                    "limit": limit,
                    "offset": offset,
                    "next_offset": public_payload.get("next_offset", offset + len(public_payload["items"])),
                    "has_more": public_payload.get("has_more", False),
                    "source": "spotify_public_read",
                }
            granted_scopes = _scope_set((get_spotify_auth_record(str(user_id)) or {}).get("scopes"))
            missing_scopes = [
                scope
                for scope in ("playlist-read-private", "playlist-read-collaborative")
                if scope not in granted_scopes
            ]
            spotify_detail = str(exc.detail or "").strip().rstrip(".")
            public_detail = str(public_error.detail or "").strip().rstrip(".") if public_error else ""
            if missing_scopes:
                detail = (
                    "Spotify denied access to this playlist's tracks because the current token is missing "
                    f"{', '.join(missing_scopes)}. Log out and log back in to grant the updated playlist permissions."
                )
            else:
                detail = (
                    "Spotify denied access to this playlist's tracks even though ListenLab has the playlist scopes. "
                    "ListenLab also tried a read-only public playlist request. This usually means the playlist is private to another account, "
                    "no longer accessible to your Spotify user, or Spotify is blocking this specific playlist."
                )
                if spotify_detail:
                    detail = f"{detail} Spotify said: {spotify_detail}."
                if public_detail and public_detail != spotify_detail:
                    detail = f"{detail} Public read said: {public_detail}."
            logger.warning(
                "event=playlist_tracks_spotify_forbidden playlist_id=%s user_id=%s missing_scopes=%s spotify_detail=%s public_detail=%s",
                normalized_playlist_id,
                user_id,
                ",".join(missing_scopes),
                spotify_detail or "",
                public_detail or "",
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=detail,
            ) from exc
        raise
    cache_playlist_track_page(
        str(user_id),
        normalized_playlist_id,
        payload["items"],
        offset=offset,
        total=payload.get("total"),
    )

    return {
        "playlist_id": normalized_playlist_id,
        "items": payload["items"],
        "total": payload.get("total", len(payload["items"])),
        "limit": limit,
        "offset": offset,
        "next_offset": payload.get("next_offset", offset + len(payload["items"])),
        "has_more": payload.get("has_more", False),
        "source": "spotify",
    }


@router.post("/auth/playback/playlists")
async def auth_playback_create_playlist(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    user_id = _require_user_id(request)
    token = _require_token(request)
    _require_playlist_modify_scope(str(user_id))
    name = str(payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Playlist name is required.")
    track_uri = str(payload.get("track_uri") or "").strip()
    if track_uri and not track_uri.startswith("spotify:track:"):
        raise HTTPException(status_code=400, detail="track_uri must be a Spotify track URI.")
    profile = await _fetch_spotify_profile(token)
    spotify_user_id = str(profile.get("id") or user_id).strip()
    if not spotify_user_id:
        raise HTTPException(status_code=400, detail="Spotify user id is unavailable.")
    playlist_payload = await _spotify_post_json(
        token,
        f"https://api.spotify.com/v1/users/{spotify_user_id}/playlists",
        {
            "name": name,
            "public": False,
            "collaborative": False,
            "description": "Created by ListenLab",
        },
    )
    playlist_id = str(playlist_payload.get("id") or "").strip()
    if not playlist_id:
        raise HTTPException(status_code=502, detail="Spotify created the playlist without returning an id.")
    if track_uri:
        await _spotify_post_json(
            token,
            f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
            {"uris": [track_uri]},
        )
    owner = playlist_payload.get("owner") if isinstance(playlist_payload.get("owner"), dict) else {}
    images = playlist_payload.get("images") if isinstance(playlist_payload.get("images"), list) else []
    external_urls = playlist_payload.get("external_urls") if isinstance(playlist_payload.get("external_urls"), dict) else {}
    return {
        "playlist": {
            "playlist_id": playlist_id,
            "name": playlist_payload.get("name") or name,
            "track_count": 1 if track_uri else 0,
            "description": playlist_payload.get("description"),
            "is_public": playlist_payload.get("public"),
            "is_collaborative": bool(playlist_payload.get("collaborative")),
            "is_owned": True,
            "owner_id": owner.get("id") or spotify_user_id,
            "owner_name": owner.get("display_name") or profile.get("display_name"),
            "hidden_by_user": False,
            "playlist_category": "private",
            "snapshot_id": playlist_payload.get("snapshot_id"),
            "url": external_urls.get("spotify"),
            "image_url": images[0].get("url") if images and isinstance(images[0], dict) else None,
        }
    }


@router.delete("/auth/playback/playlists/{playlist_id}")
async def auth_playback_delete_playlist(request: Request, playlist_id: str) -> dict[str, Any]:
    user_id = _require_user_id(request)
    token = _require_token(request)
    _require_playlist_modify_scope(str(user_id))
    normalized_playlist_id = str(playlist_id or "").strip()
    if not normalized_playlist_id:
        raise HTTPException(status_code=400, detail="playlist_id is required.")
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.delete(
            f"https://api.spotify.com/v1/playlists/{normalized_playlist_id}/followers",
            headers={"Authorization": f"Bearer {token}"},
        )
    if response.status_code >= 400:
        try:
            error_payload = response.json()
        except ValueError:
            error_payload = {"error": response.text[:160]}
        spotify_message = error_payload.get("error")
        if isinstance(spotify_message, dict):
            spotify_message = spotify_message.get("message") or spotify_message.get("reason")
        raise HTTPException(status_code=response.status_code, detail=str(spotify_message or "Spotify could not delete this playlist."))
    return {"playlist_id": normalized_playlist_id, "deleted": True}


@router.post("/auth/playback/playlist-tracks")
async def auth_playback_add_playlist_tracks(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    user_id = _require_user_id(request)
    token = _require_token(request)
    _require_playlist_modify_scope(str(user_id))
    track_uri = str(payload.get("track_uri") or "").strip()
    if not track_uri.startswith("spotify:track:"):
        raise HTTPException(status_code=400, detail="track_uri must be a Spotify track URI.")
    playlist_ids_raw = payload.get("playlist_ids")
    if not isinstance(playlist_ids_raw, list):
        raise HTTPException(status_code=400, detail="playlist_ids must be a list.")
    playlist_ids = [
        str(playlist_id or "").strip()
        for playlist_id in playlist_ids_raw
        if str(playlist_id or "").strip()
    ]
    if not playlist_ids:
        raise HTTPException(status_code=400, detail="At least one playlist is required.")
    added: list[str] = []
    errors: list[dict[str, str]] = []
    for playlist_id in dict.fromkeys(playlist_ids):
        try:
            await _spotify_post_json(
                token,
                f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
                {"uris": [track_uri]},
            )
            added.append(playlist_id)
        except HTTPException as exc:
            errors.append({"playlist_id": playlist_id, "error": str(exc.detail or "Spotify rejected this playlist.")})
    if not added and errors:
        detail = errors[0]["error"]
        if "Insufficient client scope" in detail or "scope" in detail.lower():
            detail = "Spotify permission missing. Log out and log back in so ListenLab can request playlist modify access."
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=detail)
    return {
        "added_playlist_ids": added,
        "errors": errors,
    }


@router.delete("/auth/playback/playlist-tracks")
async def auth_playback_remove_playlist_tracks(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    user_id = _require_user_id(request)
    token = _require_token(request)
    _require_playlist_modify_scope(str(user_id))
    track_uri = str(payload.get("track_uri") or "").strip()
    if not track_uri.startswith("spotify:track:"):
        raise HTTPException(status_code=400, detail="track_uri must be a Spotify track URI.")
    playlist_ids_raw = payload.get("playlist_ids")
    if not isinstance(playlist_ids_raw, list):
        raise HTTPException(status_code=400, detail="playlist_ids must be a list.")
    playlist_ids = [
        str(playlist_id or "").strip()
        for playlist_id in playlist_ids_raw
        if str(playlist_id or "").strip()
    ]
    if not playlist_ids:
        raise HTTPException(status_code=400, detail="At least one playlist is required.")
    removed: list[str] = []
    errors: list[dict[str, str]] = []
    for playlist_id in dict.fromkeys(playlist_ids):
        try:
            await _spotify_delete_json(
                token,
                f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
                {"tracks": [{"uri": track_uri}]},
            )
            removed.append(playlist_id)
        except HTTPException as exc:
            errors.append({"playlist_id": playlist_id, "error": str(exc.detail or "Spotify rejected this playlist.")})
    if not removed and errors:
        detail = errors[0]["error"]
        if "Insufficient client scope" in detail or "scope" in detail.lower():
            detail = "Spotify permission missing. Log out and log back in so ListenLab can request playlist modify access."
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=detail)
    return {
        "removed_playlist_ids": removed,
        "errors": errors,
    }


@router.get("/auth/playback/album-tracks")
async def auth_playback_album_tracks(
    request: Request,
    track_id: str | None = None,
    track_uri: str | None = None,
    album_id: str | None = None,
    force_spotify: bool = False,
    local_only: bool = False,
    promote_identity: bool = False,
    include_family: bool = False,
) -> dict[str, Any]:
    _require_user_id(request)
    track_id_candidate = track_id or _spotify_track_id_from_uri(track_uri)
    normalized_track_id = str(track_id_candidate or "").strip() or None
    normalized_album_id = str(album_id or "").strip() or None
    if not normalized_album_id and normalized_track_id:
        normalized_album_id = _cached_album_id_for_track(normalized_track_id)

    market = "US"
    source = "cache"
    local_fallback_items: list[dict[str, Any]] = []
    if normalized_album_id:
        cached_items, cached_complete = _cached_album_track_rows(normalized_album_id)
        local_fallback_items = cached_items
        if cached_complete:
            promotion = promote_catalog_album_tracks_to_identity(
                album_ids=[normalized_album_id],
                apply=True,
                refresh_clusters=False,
            ) if promote_identity else None
            response_items, album_family = _enriched_album_items_with_family(
                user_id=str(user_id),
                album_id=normalized_album_id,
                items=cached_items,
                include_family=include_family,
            )
            return {
                "album_id": normalized_album_id,
                "track_id": normalized_track_id,
                "items": response_items,
                "source": source,
                "cached": True,
                "partial": False,
                "identity_promotion": promotion,
                "album_family": album_family,
            }

        catalog_items, catalog_complete = _cached_album_track_rows_from_track_catalog(normalized_album_id)
        if len(catalog_items) > len(local_fallback_items):
            local_fallback_items = catalog_items
        if not force_spotify and (catalog_complete or (catalog_items and not cached_items)):
            promotion = promote_catalog_album_tracks_to_identity(
                album_ids=[normalized_album_id],
                apply=True,
                refresh_clusters=False,
            ) if promote_identity else None
            return {
                "album_id": normalized_album_id,
                "track_id": normalized_track_id,
                "items": enrich_rows_with_playlist_membership_counts(
                    str(user_id),
                    enrich_album_track_rows_with_release_metadata(
                        catalog_items,
                        refresh_dirty_clusters=False,
                    ),
                ),
                "source": "track_catalog",
                "cached": True,
                "partial": not catalog_complete,
                "identity_promotion": promotion,
            }
        if not force_spotify and local_fallback_items:
            promotion = promote_catalog_album_tracks_to_identity(
                album_ids=[normalized_album_id],
                apply=True,
                refresh_clusters=False,
            ) if promote_identity else None
            return {
                "album_id": normalized_album_id,
                "track_id": normalized_track_id,
                "items": enrich_rows_with_playlist_membership_counts(
                    str(user_id),
                    enrich_album_track_rows_with_release_metadata(
                        local_fallback_items,
                        refresh_dirty_clusters=False,
                    ),
                ),
                "source": "local_partial",
                "cached": True,
                "partial": True,
                "identity_promotion": promotion,
            }

    if local_only:
        if normalized_album_id and local_fallback_items:
            return {
                "album_id": normalized_album_id,
                "track_id": normalized_track_id,
                "items": enrich_rows_with_playlist_membership_counts(
                    str(user_id),
                    enrich_album_track_rows_with_release_metadata(
                        local_fallback_items,
                        refresh_dirty_clusters=False,
                    ),
                ),
                "source": "local_partial",
                "cached": True,
                "partial": True,
            }
        raise HTTPException(status_code=404, detail="Album track list is unavailable for this item.")

    try:
        token = _require_token(request)
    except HTTPException:
        if normalized_album_id and local_fallback_items:
            return {
                "album_id": normalized_album_id,
                "track_id": normalized_track_id,
                "items": enrich_rows_with_playlist_membership_counts(
                    str(user_id),
                    enrich_album_track_rows_with_release_metadata(
                        local_fallback_items,
                        refresh_dirty_clusters=False,
                    ),
                ),
                "source": "local_partial",
                "cached": True,
                "partial": True,
            }
        raise
    if not normalized_album_id and normalized_track_id:
        track_payload = await _spotify_get(
            token,
            f"https://api.spotify.com/v1/tracks/{normalized_track_id}",
            {},
        )
        album_payload = track_payload.get("album") if isinstance(track_payload.get("album"), dict) else {}
        normalized_album_id = str(album_payload.get("id") or "").strip() or None
        if normalized_album_id:
            _upsert_track_catalog(
                track=track_payload,
                market=market,
                fetched_at=_utc_now(),
                last_status="ok",
                last_error=None,
            )
            source = "spotify_track"

    if not normalized_album_id:
        raise HTTPException(status_code=404, detail="Album track list is unavailable for this item.")

    cached_items, cached_complete = _cached_album_track_rows(normalized_album_id)
    if cached_complete:
        promotion = promote_catalog_album_tracks_to_identity(
            album_ids=[normalized_album_id],
            apply=True,
            refresh_clusters=False,
        ) if promote_identity else None
        response_items, album_family = _enriched_album_items_with_family(
            user_id=str(user_id),
            album_id=normalized_album_id,
            items=cached_items,
            include_family=include_family,
        )
        return {
            "album_id": normalized_album_id,
            "track_id": normalized_track_id,
            "items": response_items,
            "source": source,
            "cached": True,
            "partial": False,
            "identity_promotion": promotion,
            "album_family": album_family,
        }

    try:
        items = await _fetch_and_cache_album_tracks(token, normalized_album_id, market)
    except HTTPException:
        if local_fallback_items:
            return {
                "album_id": normalized_album_id,
                "track_id": normalized_track_id,
                "items": enrich_rows_with_playlist_membership_counts(
                    str(user_id),
                    enrich_album_track_rows_with_release_metadata(
                        local_fallback_items,
                        refresh_dirty_clusters=False,
                    ),
                ),
                "source": "local_partial",
                "cached": True,
                "partial": True,
            }
        raise
    promotion = promote_catalog_album_tracks_to_identity(
        album_ids=[normalized_album_id],
        apply=True,
        refresh_clusters=False,
    ) if promote_identity else None
    response_items, album_family = _enriched_album_items_with_family(
        user_id=str(user_id),
        album_id=normalized_album_id,
        items=items,
        include_family=include_family,
    )
    return {
        "album_id": normalized_album_id,
        "track_id": normalized_track_id,
        "items": response_items,
        "source": "spotify_album_tracks",
        "cached": False,
        "partial": False,
        "identity_promotion": promotion,
        "album_family": album_family,
    }


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
        update_result = update_listenlab_player_play_progress(
            row_id=int(payload["row_id"]),
            user_id=str(user_id),
            ms_played=progress_ms,
            ms_played_confidence=confidence,
        )
        if not update_result["updated"]:
            raise HTTPException(status_code=404, detail="ListenLab player event was not found.")
        if update_result["crossed_listen_threshold"] or update_result.get("cache_last_played_may_change"):
            refresh_source_track_play_count_cache()
        return {
            "ok": True,
            "row_id": int(payload["row_id"]),
            "action": "updated",
            "listen_qualified": update_result["crossed_listen_threshold"],
        }

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
        "listen_qualified": progress_ms >= (int(duration_ms * 0.65) if duration_ms and duration_ms > 0 else 30_000),
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
