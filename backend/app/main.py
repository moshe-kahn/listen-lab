from __future__ import annotations

import base64
import json
import logging
import secrets
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from starlette.middleware.sessions import SessionMiddleware

from app.config import get_settings
from app.history_analysis import clear_history_insights_cache, get_history_signature, load_history_insights

settings = get_settings()
logger = logging.getLogger("listenlab.auth")
SECTION_PREVIEW_LIMIT = 10
ALBUM_ANALYSIS_LIMIT = 10
PLAYLIST_ANALYSIS_LIMIT = 10
LOAD_PROGRESS: dict[str, dict[str, Any]] = {}
SECTION_CACHE: dict[str, dict[str, Any]] = {}
INITIAL_DASHBOARD_LIMIT = 5
SHORT_CACHE_TTL_SECONDS = 180
CACHE_VERSION = 1
PERSISTENT_HISTORY_CACHE_FILE = "history_sections.json"
PROGRESS_LOG_FILE = "dashboard-progress.log"

app = FastAPI(title="ListenLab API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.allowed_origin],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.session_secret,
    same_site="lax",
    https_only=False,
)


def _is_configured() -> bool:
    return bool(
        settings.spotify_client_id
        and settings.spotify_client_secret
        and settings.spotify_redirect_uri
        and settings.session_secret
    )


def _require_token(request: Request) -> str:
    token = request.session.get("access_token")
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated with Spotify.",
        )
    return token


def _callback_redirect_url(reason: str, detail: str | None = None) -> str:
    query = {"status": reason}
    if detail:
        query["detail"] = detail
    return f"{settings.frontend_url}/auth/callback?{urlencode(query)}"


def _progress_key(request: Request) -> str | None:
    user = request.session.get("spotify_user") or {}
    if user.get("id"):
        return str(user["id"])
    token = request.session.get("access_token")
    if token:
        return f"token:{str(token)[:12]}"
    return None


def _set_load_progress(request: Request, phase: str) -> None:
    key = _progress_key(request)
    if not key:
        return
    current = LOAD_PROGRESS.get(key)
    if current is None:
        LOAD_PROGRESS[key] = {
            "phase": phase,
            "started_at": time.perf_counter(),
            "last_at_seconds": 0.0,
            "events": [{"phase": phase, "at_seconds": 0.0}],
        }
        _append_progress_log(key, f"total=0.0s delta=0.0s {phase}")
        return
    if current.get("phase") == phase:
        return
    current["phase"] = phase
    started_at = float(current.get("started_at", time.perf_counter()))
    elapsed_seconds = round(time.perf_counter() - started_at, 1)
    previous_elapsed = float(current.get("last_at_seconds", 0.0))
    delta_seconds = round(max(0.0, elapsed_seconds - previous_elapsed), 1)
    current["last_at_seconds"] = elapsed_seconds
    current.setdefault("events", []).append(
        {"phase": phase, "at_seconds": elapsed_seconds}
    )
    _append_progress_log(key, f"total={elapsed_seconds:.1f}s delta={delta_seconds:.1f}s {phase}")


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
            _append_progress_log(key, f"total={elapsed_seconds:.1f}s delta={delta_seconds:.1f}s complete")
        LOAD_PROGRESS.pop(key, None)


def _cache_dir() -> Path:
    path = Path(settings.cache_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _persistent_history_cache_path() -> Path:
    return _cache_dir() / PERSISTENT_HISTORY_CACHE_FILE


def _progress_log_path() -> Path:
    return _cache_dir() / PROGRESS_LOG_FILE


def _append_progress_log(key: str, message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    try:
        with _progress_log_path().open("a", encoding="utf-8") as handle:
            handle.write(f"[{timestamp}] [{key}] {message}\n")
    except OSError:
        logger.exception("Failed to append dashboard progress log.")


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


def _load_persistent_history_cache(history_signature: tuple[tuple[str, int, int], ...] | None) -> dict[str, Any] | None:
    if not history_signature:
        return None

    cache_path = _persistent_history_cache_path()
    if not cache_path.exists():
        return None

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    if payload.get("cache_version") != CACHE_VERSION:
        return None
    if payload.get("history_signature") != [list(item) for item in history_signature]:
        return None

    return payload.get("sections")


def _store_persistent_history_cache(
    history_signature: tuple[tuple[str, int, int], ...] | None,
    sections: dict[str, Any],
) -> None:
    if not history_signature:
        return

    payload = {
        "cache_version": CACHE_VERSION,
        "history_signature": [list(item) for item in history_signature],
        "stored_at": time.time(),
        "sections": sections,
    }

    cache_path = _persistent_history_cache_path()
    cache_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _clear_dashboard_caches() -> None:
    SECTION_CACHE.clear()
    clear_history_insights_cache()
    try:
        _persistent_history_cache_path().unlink(missing_ok=True)
    except OSError:
        logger.exception("Failed to remove persistent history cache.")


async def _fetch_spotify_profile(access_token: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            settings.spotify_me_url,
            headers={"Authorization": f"Bearer {access_token}"},
        )

    if response.status_code == status.HTTP_401_UNAUTHORIZED:
        raise HTTPException(status_code=401, detail="Spotify access token is no longer valid.")
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail="Failed to fetch Spotify profile.")

    return response.json()


async def _spotify_get(access_token: str, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {access_token}"},
        )

    if response.status_code == status.HTTP_401_UNAUTHORIZED:
        raise HTTPException(status_code=401, detail="Spotify access token is no longer valid.")
    if response.status_code == status.HTTP_403_FORBIDDEN:
        raise HTTPException(status_code=403, detail="Spotify scope is missing for this resource.")
    if response.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
        raise HTTPException(status_code=429, detail="Spotify rate limit reached for this resource.")
    if response.status_code >= 400:
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("error_description") or payload.get("error", {}).get("message") or ""
        except ValueError:
            detail = response.text[:160]
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch Spotify data from {url} (status {response.status_code}){f': {detail}' if detail else ''}",
        )

    return response.json()


async def _spotify_get_many(access_token: str, url: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {access_token}"},
        )

    if response.status_code == status.HTTP_401_UNAUTHORIZED:
        raise HTTPException(status_code=401, detail="Spotify access token is no longer valid.")
    if response.status_code == status.HTTP_403_FORBIDDEN:
        raise HTTPException(status_code=403, detail="Spotify scope is missing for this resource.")
    if response.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
        raise HTTPException(status_code=429, detail="Spotify rate limit reached for this resource.")
    if response.status_code >= 400:
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("error_description") or payload.get("error", {}).get("message") or ""
        except ValueError:
            detail = response.text[:160]
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch Spotify data from {url} (status {response.status_code}){f': {detail}' if detail else ''}",
        )

    payload = response.json()
    return payload.get("artists") or []


async def _fetch_recent_tracks(access_token: str, limit: int) -> tuple[list[dict[str, Any]], bool]:
    try:
        payload = await _spotify_get(
            access_token,
            "https://api.spotify.com/v1/me/player/recently-played",
            {"limit": limit},
        )
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return [], False
        raise

    items = payload.get("items") or []
    results: list[dict[str, Any]] = []
    last_track_id: str | None = None

    for item in items:
        track = item.get("track") or {}
        track_id = track.get("id")
        if not track_id:
            continue
        if track_id == last_track_id:
            continue
        results.append(_normalize_track(track))
        last_track_id = track_id

    return results, True


async def _fetch_owned_playlists(
    access_token: str,
    spotify_user_id: str | None,
    max_items: int | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    if not spotify_user_id:
        return [], False

    results: list[dict[str, Any]] = []
    offset = 0
    limit = 50

    while True:
        try:
            payload = await _spotify_get(
                access_token,
                "https://api.spotify.com/v1/me/playlists",
                {"limit": limit, "offset": offset},
            )
        except HTTPException as exc:
            if exc.status_code == status.HTTP_403_FORBIDDEN:
                return [], False
            raise

        items = payload.get("items") or []
        if not items:
            break

        for item in items:
            owner = item.get("owner") or {}
            if owner.get("id") != spotify_user_id:
                continue

            external_urls = item.get("external_urls") or {}
            items_info = item.get("items") or {}
            tracks = item.get("tracks") or {}
            images = item.get("images") or []

            if not item.get("public"):
                continue

            results.append(
                {
                    "playlist_id": item.get("id"),
                    "name": item.get("name"),
                    "track_count": items_info.get("total", tracks.get("total")),
                    "description": item.get("description"),
                    "is_public": item.get("public"),
                    "url": external_urls.get("spotify"),
                    "image_url": images[0].get("url") if images else None,
                }
            )

        offset += len(items)
        if len(items) < limit:
            break
        if max_items is not None and len(results) >= max_items:
            break

    if max_items is not None:
        results = results[:max_items]

    return results, True


async def _fetch_playlist_tracks(
    access_token: str,
    playlist_id: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    payload = await _spotify_get(
        access_token,
        f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
        {"limit": limit},
    )

    items = payload.get("items") or []
    results: list[dict[str, Any]] = []

    for item in items:
        track = item.get("track") or {}
        if not track.get("id"):
            continue
        results.append(_normalize_track(track))

    return results


async def _fetch_recent_liked_tracks(access_token: str, limit: int) -> tuple[list[dict[str, Any]], bool]:
    try:
        payload = await _spotify_get(
            access_token,
            "https://api.spotify.com/v1/me/tracks",
            {"limit": limit},
        )
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return [], False
        raise

    items = payload.get("items") or []
    results: list[dict[str, Any]] = []

    for item in items:
        track = item.get("track") or {}
        album = track.get("album") or {}
        artists = track.get("artists") or []
        external_urls = track.get("external_urls") or {}

        if not track.get("id"):
            continue

        results.append(
            {
                "track_id": track.get("id"),
                "track_name": track.get("name"),
                "artist_name": ", ".join(artist.get("name", "") for artist in artists if artist.get("name")),
                "album_name": album.get("name"),
                "url": external_urls.get("spotify"),
                "image_url": ((album.get("images") or [{}])[0]).get("url"),
            }
        )

    return results, True


async def _fetch_followed_artists_total(access_token: str) -> tuple[int | None, bool]:
    try:
        payload = await _spotify_get(
            access_token,
            "https://api.spotify.com/v1/me/following",
            {"type": "artist", "limit": 1},
        )
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return None, False
        raise

    artists = payload.get("artists") or {}
    return artists.get("total"), True


def _normalize_artist(artist: dict[str, Any]) -> dict[str, Any]:
    images = artist.get("images") or []
    external_urls = artist.get("external_urls") or {}
    followers = artist.get("followers") or {}
    genres = artist.get("genres") or []
    return {
        "artist_id": artist.get("id"),
        "name": artist.get("name"),
        "followers_total": followers.get("total"),
        "genres": genres[:2],
        "popularity": artist.get("popularity"),
        "url": external_urls.get("spotify"),
        "image_url": images[0].get("url") if images else None,
    }


def _artist_enrichment_lookup(artists: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        (artist.get("name") or "").strip().lower(): artist
        for artist in artists
        if artist.get("name")
    }


def _normalize_track(track: dict[str, Any]) -> dict[str, Any]:
    album = track.get("album") or {}
    artists = track.get("artists") or []
    external_urls = track.get("external_urls") or {}
    album_external_urls = album.get("external_urls") or {}
    return {
        "track_id": track.get("id"),
        "track_name": track.get("name"),
        "artist_name": ", ".join(artist.get("name", "") for artist in artists if artist.get("name")),
        "album_name": album.get("name"),
        "url": external_urls.get("spotify"),
        "album_url": album_external_urls.get("spotify"),
        "image_url": ((album.get("images") or [{}])[0]).get("url"),
        "album_id": album.get("id"),
        "artists": [
            {
                "artist_id": artist.get("id"),
                "name": artist.get("name"),
            }
            for artist in artists
            if artist.get("name")
        ],
    }


def _album_enrichment_lookup(tracks: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for track in tracks:
        album_name = (track.get("album_name") or "").strip()
        artist_name = (track.get("artist_name") or "").strip()
        if not album_name or not artist_name:
            continue
        lookup[(album_name.lower(), artist_name.lower())] = {
            "album_id": track.get("album_id"),
            "url": track.get("album_url") or track.get("url"),
            "image_url": track.get("image_url"),
        }
    return lookup


def _normalized_max(value: float, max_value: float) -> float:
    if max_value <= 0:
        return 0.0
    return value / max_value


def _merge_history_artists(
    history_artists: list[dict[str, Any]],
    enrichment_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for artist in history_artists:
        enriched = enrichment_lookup.get((artist.get("name") or "").strip().lower(), {})
        results.append(
            {
                **artist,
                "artist_id": enriched.get("artist_id", artist.get("artist_id")),
                "followers_total": enriched.get("followers_total", artist.get("followers_total")),
                "genres": enriched.get("genres", artist.get("genres") or []),
                "popularity": enriched.get("popularity", artist.get("popularity")),
                "url": enriched.get("url", artist.get("url")),
                "image_url": enriched.get("image_url", artist.get("image_url")),
            }
        )
    return results


def _merge_history_albums(
    history_albums: list[dict[str, Any]],
    enrichment_lookup: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for album in history_albums:
        key = (
            (album.get("name") or "").strip().lower(),
            (album.get("artist_name") or "").strip().lower(),
        )
        enriched = enrichment_lookup.get(key, {})
        results.append(
            {
                **album,
                "album_id": enriched.get("album_id", album.get("album_id")),
                "url": enriched.get("url", album.get("url")),
                "image_url": enriched.get("image_url", album.get("image_url")),
            }
        )
    return results


async def _enrich_history_artists_from_search(
    access_token: str,
    artists: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for artist in artists:
        if artist.get("image_url") and artist.get("url"):
            results.append(artist)
            continue

        artist_name = (artist.get("name") or "").strip()
        if not artist_name:
            results.append(artist)
            continue

        try:
            payload = await _spotify_get(
                access_token,
                "https://api.spotify.com/v1/search",
                {"q": f'artist:"{artist_name}"', "type": "artist", "limit": 1},
            )
        except HTTPException as exc:
            if exc.status_code in {status.HTTP_403_FORBIDDEN, status.HTTP_429_TOO_MANY_REQUESTS}:
                results.append(artist)
                continue
            raise

        items = ((payload.get("artists") or {}).get("items")) or []
        if not items:
            results.append(artist)
            continue

        match = items[0]
        images = match.get("images") or []
        external_urls = match.get("external_urls") or {}
        followers = match.get("followers") or {}
        results.append(
            {
                **artist,
                "artist_id": match.get("id") or artist.get("artist_id"),
                "followers_total": followers.get("total") or artist.get("followers_total"),
                "genres": (match.get("genres") or [])[:2] or artist.get("genres") or [],
                "popularity": match.get("popularity") if match.get("popularity") is not None else artist.get("popularity"),
                "url": external_urls.get("spotify") or artist.get("url"),
                "image_url": images[0].get("url") if images else artist.get("image_url"),
            }
        )

    return results


async def _enrich_history_albums_from_search(
    access_token: str,
    albums: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for album in albums:
        if album.get("image_url") and album.get("url"):
            results.append(album)
            continue

        album_name = (album.get("name") or "").strip()
        artist_name = (album.get("artist_name") or "").strip()
        if not album_name:
            results.append(album)
            continue

        query = f'album:"{album_name}"'
        if artist_name:
            query += f' artist:"{artist_name}"'

        try:
            payload = await _spotify_get(
                access_token,
                "https://api.spotify.com/v1/search",
                {"q": query, "type": "album", "limit": 1},
            )
        except HTTPException as exc:
            if exc.status_code in {status.HTTP_403_FORBIDDEN, status.HTTP_429_TOO_MANY_REQUESTS}:
                results.append(album)
                continue
            raise

        items = ((payload.get("albums") or {}).get("items")) or []
        if not items:
            results.append(album)
            continue

        match = items[0]
        images = match.get("images") or []
        external_urls = match.get("external_urls") or {}
        results.append(
            {
                **album,
                "album_id": match.get("id") or album.get("album_id"),
                "url": external_urls.get("spotify") or album.get("url"),
                "image_url": images[0].get("url") if images else album.get("image_url"),
            }
        )

    return results


def _normalize_live_top_artists(
    long_term_top_tracks: list[dict[str, Any]],
    recent_top_tracks: list[dict[str, Any]],
    recent_tracks: list[dict[str, Any]],
    liked_tracks: list[dict[str, Any]],
    enrichment_lookup: dict[str, dict[str, Any]],
    mode: str,
) -> list[dict[str, Any]]:
    aggregates: dict[str, dict[str, Any]] = {}

    def ensure_artist(artist_id: str | None, artist_name: str | None) -> dict[str, Any] | None:
        if not artist_name:
            return None
        key = (artist_id or artist_name).strip().lower()
        entry = aggregates.get(key)
        if entry is None:
            entry = {
                "artist_id": artist_id,
                "name": artist_name,
                "long_top_track_ids": set(),
                "recent_top_track_ids": set(),
                "liked_track_ids": set(),
                "recent_track_ids": set(),
                "recent_play_count": 0,
                "long_rank_weight": 0.0,
                "recent_rank_weight": 0.0,
            }
            aggregates[key] = entry
        return entry

    def track_artists(track: dict[str, Any]) -> list[dict[str, Any]]:
        return track.get("artists") or []

    def apply_top_tracks(tracks: list[dict[str, Any]], target_key: str, weight_key: str) -> None:
        total = len(tracks) or 1
        for index, track in enumerate(tracks):
            track_id = track.get("track_id")
            rank_weight = (total - index) / total
            for artist in track_artists(track):
                entry = ensure_artist(artist.get("artist_id"), artist.get("name"))
                if not entry or not track_id:
                    continue
                entry[target_key].add(track_id)
                entry[weight_key] += rank_weight

    apply_top_tracks(long_term_top_tracks, "long_top_track_ids", "long_rank_weight")
    apply_top_tracks(recent_top_tracks, "recent_top_track_ids", "recent_rank_weight")

    for track in liked_tracks:
        track_id = track.get("track_id")
        if not track_id:
            continue
        for artist in track_artists(track):
            entry = ensure_artist(artist.get("artist_id"), artist.get("name"))
            if entry:
                entry["liked_track_ids"].add(track_id)

    for track in recent_tracks:
        track_id = track.get("track_id")
        if not track_id:
            continue
        for artist in track_artists(track):
            entry = ensure_artist(artist.get("artist_id"), artist.get("name"))
            if not entry:
                continue
            entry["recent_track_ids"].add(track_id)
            entry["recent_play_count"] += 1

    if not aggregates:
        return []

    max_long_top = max(len(item["long_top_track_ids"]) for item in aggregates.values())
    max_recent_top = max(len(item["recent_top_track_ids"]) for item in aggregates.values())
    max_liked = max(len(item["liked_track_ids"]) for item in aggregates.values())
    max_recent_distinct = max(len(item["recent_track_ids"]) for item in aggregates.values())
    max_recent_play_count = max(item["recent_play_count"] for item in aggregates.values())
    max_long_rank_weight = max(item["long_rank_weight"] for item in aggregates.values())
    max_recent_rank_weight = max(item["recent_rank_weight"] for item in aggregates.values())

    results: list[dict[str, Any]] = []
    for item in aggregates.values():
        long_top_track_count_norm = _normalized_max(len(item["long_top_track_ids"]), max_long_top)
        recent_top_track_count_norm = _normalized_max(len(item["recent_top_track_ids"]), max_recent_top)
        liked_track_count_norm = _normalized_max(len(item["liked_track_ids"]), max_liked)
        recent_distinct_tracks_norm = _normalized_max(len(item["recent_track_ids"]), max_recent_distinct)
        recent_play_count_norm = _normalized_max(item["recent_play_count"], max_recent_play_count)
        long_rank_weight_norm = _normalized_max(item["long_rank_weight"], max_long_rank_weight)
        recent_rank_weight_norm = _normalized_max(item["recent_rank_weight"], max_recent_rank_weight)

        if mode == "recent":
            score = (
                recent_rank_weight_norm * 0.30
                + recent_distinct_tracks_norm * 0.25
                + recent_play_count_norm * 0.20
                + recent_top_track_count_norm * 0.15
                + liked_track_count_norm * 0.10
            )
        else:
            score = (
                long_rank_weight_norm * 0.32
                + long_top_track_count_norm * 0.23
                + liked_track_count_norm * 0.18
                + recent_rank_weight_norm * 0.12
                + recent_distinct_tracks_norm * 0.10
                + recent_play_count_norm * 0.05
            )

        enriched = enrichment_lookup.get((item["name"] or "").strip().lower(), {})
        results.append(
            {
                "artist_id": enriched.get("artist_id", item["artist_id"]),
                "name": item["name"],
                "followers_total": enriched.get("followers_total"),
                "genres": enriched.get("genres") or [],
                "popularity": enriched.get("popularity"),
                "url": enriched.get("url"),
                "image_url": enriched.get("image_url"),
                "debug": {
                    "source": "live_formula",
                    "score": round(score, 4),
                    "long_top_track_count_norm": round(long_top_track_count_norm, 4),
                    "recent_top_track_count_norm": round(recent_top_track_count_norm, 4),
                    "liked_track_count_norm": round(liked_track_count_norm, 4),
                    "recent_distinct_tracks_norm": round(recent_distinct_tracks_norm, 4),
                    "recent_play_count_norm": round(recent_play_count_norm, 4),
                    "long_rank_weight_norm": round(long_rank_weight_norm, 4),
                    "recent_rank_weight_norm": round(recent_rank_weight_norm, 4),
                },
            }
        )

    return sorted(
        results,
        key=lambda artist: (
            -artist["debug"]["score"],
            -artist["debug"]["recent_distinct_tracks_norm"],
            -artist["debug"]["recent_play_count_norm"],
            artist["name"] or "",
        ),
    )[:SECTION_PREVIEW_LIMIT]


def _normalize_live_top_albums(
    long_term_top_tracks: list[dict[str, Any]],
    recent_top_tracks: list[dict[str, Any]],
    recent_tracks: list[dict[str, Any]],
    liked_tracks: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    aggregates: dict[str, dict[str, Any]] = {}

    def album_key(track: dict[str, Any]) -> str | None:
        album_id = track.get("album_id")
        album_name = track.get("album_name")
        artist_name = track.get("artist_name")
        if album_id:
            return album_id
        if album_name and artist_name:
            return f"{album_name.strip().lower()}::{artist_name.strip().lower()}"
        return None

    def ensure_album(track: dict[str, Any]) -> dict[str, Any] | None:
        key = album_key(track)
        if not key:
            return None
        entry = aggregates.get(key)
        if entry is None:
            entry = {
                "album_id": track.get("album_id"),
                "name": track.get("album_name"),
                "artist_name": track.get("artist_name"),
                "url": track.get("album_url") or track.get("url"),
                "image_url": track.get("image_url"),
                "long_track_ids": set(),
                "recent_track_ids": set(),
                "liked_track_ids": set(),
                "recent_play_count": 0,
                "long_rank_weight": 0.0,
                "recent_rank_weight": 0.0,
                "track_name_scores": {},
            }
            aggregates[key] = entry
        return entry

    def add_top_tracks(tracks: list[dict[str, Any]], target_key: str, weight_key: str) -> None:
        total = len(tracks) or 1
        for index, track in enumerate(tracks):
            track_id = track.get("track_id")
            entry = ensure_album(track)
            if not entry or not track_id:
                continue
            rank_weight = (total - index) / total
            entry[target_key].add(track_id)
            entry[weight_key] += rank_weight
            track_name = track.get("track_name")
            if track_name:
                entry["track_name_scores"][track_name] = entry["track_name_scores"].get(track_name, 0.0) + rank_weight

    add_top_tracks(long_term_top_tracks, "long_track_ids", "long_rank_weight")
    add_top_tracks(recent_top_tracks, "recent_track_ids", "recent_rank_weight")

    for track in liked_tracks:
        track_id = track.get("track_id")
        entry = ensure_album(track)
        if not entry or not track_id:
            continue
        entry["liked_track_ids"].add(track_id)
        track_name = track.get("track_name")
        if track_name:
            entry["track_name_scores"][track_name] = entry["track_name_scores"].get(track_name, 0.0) + 0.5

    for track in recent_tracks:
        track_id = track.get("track_id")
        entry = ensure_album(track)
        if not entry or not track_id:
            continue
        entry["recent_track_ids"].add(track_id)
        entry["recent_play_count"] += 1
        track_name = track.get("track_name")
        if track_name:
            entry["track_name_scores"][track_name] = entry["track_name_scores"].get(track_name, 0.0) + 0.25

    if not aggregates:
        return [], []

    max_long_distinct = max(len(item["long_track_ids"]) for item in aggregates.values())
    max_recent_distinct = max(len(item["recent_track_ids"]) for item in aggregates.values())
    max_liked = max(len(item["liked_track_ids"]) for item in aggregates.values())
    max_long_rank_weight = max(item["long_rank_weight"] for item in aggregates.values())
    max_recent_rank_weight = max(item["recent_rank_weight"] for item in aggregates.values())
    max_recent_play_count = max(item["recent_play_count"] for item in aggregates.values())

    def build_results(mode: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for item in aggregates.values():
            long_distinct_norm = _normalized_max(len(item["long_track_ids"]), max_long_distinct)
            recent_distinct_norm = _normalized_max(len(item["recent_track_ids"]), max_recent_distinct)
            liked_norm = _normalized_max(len(item["liked_track_ids"]), max_liked)
            long_rank_norm = _normalized_max(item["long_rank_weight"], max_long_rank_weight)
            recent_rank_norm = _normalized_max(item["recent_rank_weight"], max_recent_rank_weight)
            recent_play_count_norm = _normalized_max(item["recent_play_count"], max_recent_play_count)

            if mode == "recent":
                score = (
                    recent_distinct_norm * 0.34
                    + recent_play_count_norm * 0.24
                    + recent_rank_norm * 0.18
                    + liked_norm * 0.14
                    + long_distinct_norm * 0.10
                )
                track_representation_count = len(item["recent_track_ids"])
            else:
                score = (
                    long_distinct_norm * 0.34
                    + long_rank_norm * 0.24
                    + liked_norm * 0.18
                    + recent_distinct_norm * 0.14
                    + recent_play_count_norm * 0.10
                )
                track_representation_count = len(item["long_track_ids"])

            represented_track_names = [
                name
                for name, _weight in sorted(
                    item["track_name_scores"].items(),
                    key=lambda pair: (-pair[1], pair[0].lower()),
                )[:3]
            ]
            results.append(
                {
                    "album_id": item["album_id"],
                    "name": item["name"],
                    "artist_name": item["artist_name"],
                    "url": item["url"],
                    "image_url": item["image_url"],
                    "track_representation_count": track_representation_count,
                    "rank_score": round(recent_rank_norm if mode == "recent" else long_rank_norm, 4),
                    "album_score": round(score, 4),
                    "represented_track_names": represented_track_names,
                    "debug": {
                        "source": "live_formula",
                        "score": round(score, 4),
                        "long_distinct_tracks_norm": round(long_distinct_norm, 4),
                        "recent_distinct_tracks_norm": round(recent_distinct_norm, 4),
                        "liked_tracks_on_album_norm": round(liked_norm, 4),
                        "long_rank_weight_on_album_norm": round(long_rank_norm, 4),
                        "recent_rank_weight_on_album_norm": round(recent_rank_norm, 4),
                        "recent_play_count_on_album_norm": round(recent_play_count_norm, 4),
                    },
                }
            )

        return sorted(
            results,
            key=lambda album: (
                -album["debug"]["score"],
                -album["track_representation_count"],
                album["name"] or "",
            ),
        )[:SECTION_PREVIEW_LIMIT]

    return build_results("long_term"), build_results("recent")


async def _fetch_album_track_ids(
    access_token: str,
    album_id: str,
    max_tracks: int = 300,
) -> set[str]:
    offset = 0
    limit = 50
    track_ids: set[str] = set()

    while offset < max_tracks:
        payload = await _spotify_get(
            access_token,
            f"https://api.spotify.com/v1/albums/{album_id}/tracks",
            {"limit": limit, "offset": offset},
        )
        items = payload.get("items") or []
        if not items:
            break

        for item in items:
            track_id = item.get("id")
            if track_id:
                track_ids.add(track_id)

        offset += len(items)
        if len(items) < limit:
            break

    return track_ids


def _track_weight_map(tracks: list[dict[str, Any]]) -> dict[str, float]:
    total = len(tracks)
    if total == 0:
        return {}

    weights: dict[str, float] = {}
    for index, track in enumerate(tracks):
        track_id = track.get("track_id")
        if not track_id:
            continue
        weights[track_id] = max(weights.get(track_id, 0.0), (total - index) / total)
    return weights


def _album_track_name_map(tracks: list[dict[str, Any]]) -> dict[str, list[str]]:
    names_by_album: dict[str, list[str]] = {}
    for track in tracks:
        album_id = track.get("album_id")
        track_name = track.get("track_name")
        if not album_id or not track_name:
            continue
        album_names = names_by_album.setdefault(album_id, [])
        if track_name not in album_names:
            album_names.append(track_name)
    return names_by_album


def _normalize_top_albums_fallback(
    short_term_top_tracks: list[dict[str, Any]],
    long_term_top_tracks: list[dict[str, Any]],
    recent_tracks: list[dict[str, Any]],
    liked_tracks: list[dict[str, Any]],
    mode: str,
) -> list[dict[str, Any]]:
    albums: dict[str, dict[str, Any]] = {}

    weighted_sources: list[tuple[list[dict[str, Any]], float]] = (
        [
            (recent_tracks, 4.0),
            (short_term_top_tracks, 3.0),
            (liked_tracks, 1.5),
            (long_term_top_tracks, 1.0),
        ]
        if mode == "short_term"
        else [
            (long_term_top_tracks, 4.0),
            (liked_tracks, 2.5),
            (short_term_top_tracks, 1.5),
            (recent_tracks, 1.0),
        ]
    )

    for tracks, source_weight in weighted_sources:
        for index, track in enumerate(tracks):
            album_id = track.get("album_id")
            if not album_id:
                continue

            entry = albums.get(album_id)
            rank_weight = source_weight * max(1, len(tracks) - index)

            if entry is None:
                entry = {
                    "album_id": album_id,
                    "name": track.get("album_name"),
                    "artist_name": track.get("artist_name"),
                    "url": track.get("album_url") or track.get("url"),
                    "image_url": track.get("image_url"),
                    "track_representation_count": 0,
                    "rank_score": 0,
                    "album_score": 0,
                    "represented_track_names": [],
                    "debug": {
                        "fallback": True,
                        "total_album_tracks": None,
                    },
                }
                albums[album_id] = entry

            entry["track_representation_count"] += 1
            entry["rank_score"] += rank_weight
            entry["album_score"] = entry["track_representation_count"] * 1000 + entry["rank_score"]
            track_name = track.get("track_name")
            if track_name and track_name not in entry["represented_track_names"]:
                entry["represented_track_names"].append(track_name)

    return sorted(
        albums.values(),
        key=lambda album: (
            -album["album_score"],
            -album["track_representation_count"],
            -album["rank_score"],
            album["name"] or "",
        ),
    )[:SECTION_PREVIEW_LIMIT]


def _normalize_top_playlists_fallback(playlists: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    def to_result(playlist: dict[str, Any]) -> dict[str, Any]:
        return {
            "playlist_id": playlist.get("playlist_id"),
            "playlist_name": playlist.get("name"),
            "playlist_url": playlist.get("url"),
            "image_url": playlist.get("image_url"),
            "track_count": playlist.get("track_count"),
            "score": 0.0,
            "match_counts": {
                "short_term_top": 0,
                "long_term_top": 0,
                "recently_played": 0,
                "liked": 0,
                "playlist_size": playlist.get("track_count") or 0,
            },
            "fallback": True,
        }

    all_time_results = [
        to_result(playlist)
        for playlist in sorted(
            playlists,
            key=lambda item: (-(item.get("track_count") or 0), item.get("name") or ""),
        )[:SECTION_PREVIEW_LIMIT]
    ]
    recent_results = [
        to_result(playlist)
        for playlist in playlists[:SECTION_PREVIEW_LIMIT]
    ]
    return recent_results, all_time_results


def _rank_album_candidates(
    short_term_top_tracks: list[dict[str, Any]],
    long_term_top_tracks: list[dict[str, Any]],
    recent_tracks: list[dict[str, Any]],
    liked_tracks: list[dict[str, Any]],
) -> list[str]:
    scores: dict[str, float] = {}

    def add_tracks(tracks: list[dict[str, Any]], weight: float) -> None:
        for index, track in enumerate(tracks):
            album_id = track.get("album_id")
            if not album_id:
                continue
            scores[album_id] = scores.get(album_id, 0.0) + weight / (index + 1)

    add_tracks(short_term_top_tracks, 4.0)
    add_tracks(long_term_top_tracks, 3.0)
    add_tracks(recent_tracks, 2.0)
    add_tracks(liked_tracks, 1.5)

    return [
        album_id
        for album_id, _score in sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    ]


def _normalize_top_albums(
    album_metadata: dict[str, dict[str, Any]],
    album_track_ids: dict[str, set[str]],
    short_term_top_tracks: list[dict[str, Any]],
    long_term_top_tracks: list[dict[str, Any]],
    recent_tracks: list[dict[str, Any]],
    liked_tracks: list[dict[str, Any]],
    mode: str,
) -> list[dict[str, Any]]:
    short_top_ids = {track.get("track_id") for track in short_term_top_tracks if track.get("track_id")}
    long_top_ids = {track.get("track_id") for track in long_term_top_tracks if track.get("track_id")}
    recent_ids = [track.get("track_id") for track in recent_tracks if track.get("track_id")]
    recent_id_set = set(recent_ids)
    liked_ids = {track.get("track_id") for track in liked_tracks if track.get("track_id")}
    short_weight_map = _track_weight_map(short_term_top_tracks)
    long_weight_map = _track_weight_map(long_term_top_tracks)
    represented_track_names = _album_track_name_map(short_term_top_tracks + long_term_top_tracks)

    album_stats: list[dict[str, Any]] = []
    max_recent_play_count = 1
    max_short_intensity = 1.0
    max_long_intensity = 1.0

    for album_id, track_ids in album_track_ids.items():
        if not track_ids:
            continue

        metadata = album_metadata.get(album_id) or {}
        total_album_tracks = len(track_ids)
        short_matches = track_ids & short_top_ids
        long_matches = track_ids & long_top_ids
        recent_matches = track_ids & recent_id_set
        liked_matches = track_ids & liked_ids
        combined_long_breadth_ids = track_ids & (long_top_ids | liked_ids)
        recent_play_count = sum(1 for track_id in recent_ids if track_id in track_ids)
        weighted_short_term_top_presence = sum(short_weight_map.get(track_id, 0.0) for track_id in short_matches)
        weighted_long_term_top_presence = sum(long_weight_map.get(track_id, 0.0) for track_id in long_matches)

        max_recent_play_count = max(max_recent_play_count, recent_play_count)
        max_short_intensity = max(max_short_intensity, weighted_short_term_top_presence)
        max_long_intensity = max(max_long_intensity, weighted_long_term_top_presence)

        album_stats.append(
            {
                "album_id": album_id,
                "name": metadata.get("name"),
                "artist_name": metadata.get("artist_name"),
                "url": metadata.get("url"),
                "image_url": metadata.get("image_url"),
                "track_representation_count": len(long_matches if mode == "long_term" else short_matches),
                "represented_track_names": represented_track_names.get(album_id, []),
                "total_album_tracks": total_album_tracks,
                "recent_breadth": len(recent_matches) / total_album_tracks,
                "liked_breadth": len(liked_matches) / total_album_tracks,
                "top_track_breadth_short": len(short_matches) / total_album_tracks,
                "top_track_breadth_long": len(long_matches) / total_album_tracks,
                "recent_play_count_for_album": recent_play_count,
                "weighted_short_term_top_presence": weighted_short_term_top_presence,
                "weighted_long_term_top_presence": weighted_long_term_top_presence,
                "album_completion_bonus": 1.0 if (len(combined_long_breadth_ids) / total_album_tracks) >= 0.8 else 0.0,
                "debug": {
                    "recent_track_matches": len(recent_matches),
                    "liked_track_matches": len(liked_matches),
                    "short_top_track_matches": len(short_matches),
                    "long_top_track_matches": len(long_matches),
                    "total_album_tracks": total_album_tracks,
                },
            }
        )

    results: list[dict[str, Any]] = []
    for album in album_stats:
        recent_play_count_normalized = album["recent_play_count_for_album"] / max_recent_play_count
        weighted_short_normalized = album["weighted_short_term_top_presence"] / max_short_intensity
        weighted_long_normalized = album["weighted_long_term_top_presence"] / max_long_intensity

        if mode == "short_term":
            album_score = (
                album["recent_breadth"] * 0.45
                + album["top_track_breadth_short"] * 0.20
                + recent_play_count_normalized * 0.25
                + album["liked_breadth"] * 0.10
            )
        else:
            album_score = (
                album["top_track_breadth_long"] * 0.40
                + album["liked_breadth"] * 0.30
                + weighted_long_normalized * 0.20
                + album["album_completion_bonus"] * 0.10
            )

        results.append(
            {
                **album,
                "album_score": round(album_score, 4),
                "rank_score": round(
                    weighted_short_normalized if mode == "short_term" else weighted_long_normalized,
                    4,
                ),
                "debug": {
                    **album["debug"],
                    "recent_play_count_normalized": round(recent_play_count_normalized, 4),
                    "weighted_short_term_top_presence_normalized": round(weighted_short_normalized, 4),
                    "weighted_long_term_top_presence_normalized": round(weighted_long_normalized, 4),
                    "recent_breadth": round(album["recent_breadth"], 4),
                    "liked_breadth": round(album["liked_breadth"], 4),
                    "top_track_breadth_short": round(album["top_track_breadth_short"], 4),
                    "top_track_breadth_long": round(album["top_track_breadth_long"], 4),
                    "album_completion_bonus": album["album_completion_bonus"],
                },
            }
        )

    return sorted(
        results,
        key=lambda album: (
            -album["album_score"],
            -album["track_representation_count"],
            album["name"] or "",
        ),
    )[:SECTION_PREVIEW_LIMIT]


async def _fetch_top_artists(access_token: str, time_range: str, limit: int) -> tuple[list[dict[str, Any]], bool]:
    try:
        payload = await _spotify_get(
            access_token,
            "https://api.spotify.com/v1/me/top/artists",
            {"limit": limit, "time_range": time_range},
        )
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return [], False
        raise

    artists_payload = payload.get("items") or []
    return [_normalize_artist(artist) for artist in artists_payload], True


async def _fetch_top_tracks(access_token: str, time_range: str, limit: int) -> tuple[list[dict[str, Any]], bool]:
    try:
        payload = await _spotify_get(
            access_token,
            "https://api.spotify.com/v1/me/top/tracks",
            {"limit": limit, "time_range": time_range},
        )
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return [], False
        raise

    tracks = payload.get("items") or []
    return [_normalize_track(track) for track in tracks], True


async def _fetch_playlist_track_ids(
    access_token: str,
    playlist_id: str,
    max_tracks: int = 500,
) -> set[str]:
    offset = 0
    limit = 100
    track_ids: set[str] = set()

    while offset < max_tracks:
        payload = await _spotify_get(
            access_token,
            f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
            {"limit": limit, "offset": offset},
        )
        items = payload.get("items") or []
        if not items:
            break

        for item in items:
            track = item.get("track") or {}
            track_id = track.get("id")
            if track_id:
                track_ids.add(track_id)

        offset += len(items)
        if len(items) < limit:
            break

    return track_ids


def _normalize_top_playlists(
    playlists: list[dict[str, Any]],
    playlist_track_ids: dict[str, set[str]],
    short_term_top_track_ids: set[str],
    long_term_top_track_ids: set[str],
    recent_track_ids: set[str],
    liked_track_ids: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    recent_results: list[dict[str, Any]] = []
    long_term_results: list[dict[str, Any]] = []

    candidate_sizes = [
        len(track_ids)
        for playlist in playlists
        if playlist.get("playlist_id") and len(playlist_track_ids.get(playlist["playlist_id"], set())) >= 5
        for track_ids in [playlist_track_ids.get(playlist["playlist_id"], set())]
    ]
    max_playlist_size = max(candidate_sizes, default=1)

    for playlist in playlists:
        playlist_id = playlist.get("playlist_id")
        if not playlist_id:
            continue

        track_ids = playlist_track_ids.get(playlist_id, set())
        playlist_size = len(track_ids)
        if playlist_size < 5:
            continue

        short_matches = len(track_ids & short_term_top_track_ids)
        long_matches = len(track_ids & long_term_top_track_ids)
        recent_matches = len(track_ids & recent_track_ids)
        liked_matches = len(track_ids & liked_track_ids)

        normalized_short = short_matches / playlist_size
        normalized_long = long_matches / playlist_size
        normalized_recent = recent_matches / playlist_size
        normalized_liked = liked_matches / playlist_size
        playlist_size_normalized = min(1.0, playlist_size / max_playlist_size)

        recent_score = (
            normalized_short * 0.5
            + normalized_recent * 0.3
            + normalized_liked * 0.2
        )
        long_term_score = (
            normalized_long * 0.6
            + normalized_liked * 0.3
            + playlist_size_normalized * 0.1
        )

        base_result = {
            "playlist_id": playlist_id,
            "playlist_name": playlist.get("name"),
            "playlist_url": playlist.get("url"),
            "image_url": playlist.get("image_url"),
            "track_count": playlist.get("track_count") or playlist_size,
            "match_counts": {
                "short_term_top": short_matches,
                "long_term_top": long_matches,
                "recently_played": recent_matches,
                "liked": liked_matches,
                "playlist_size": playlist_size,
            },
        }
        recent_results.append({**base_result, "score": round(recent_score, 4)})
        long_term_results.append({**base_result, "score": round(long_term_score, 4)})

    recent_results.sort(key=lambda playlist: (-playlist["score"], -(playlist["match_counts"]["short_term_top"]), playlist["playlist_name"] or ""))
    long_term_results.sort(key=lambda playlist: (-playlist["score"], -(playlist["match_counts"]["long_term_top"]), playlist["playlist_name"] or ""))
    return recent_results[:SECTION_PREVIEW_LIMIT], long_term_results[:SECTION_PREVIEW_LIMIT]


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/auth/login")
async def auth_login(request: Request) -> RedirectResponse:
    if not _is_configured():
        raise HTTPException(status_code=500, detail="Spotify OAuth is not configured.")

    state = secrets.token_urlsafe(32)
    request.session["oauth_state"] = state

    query = urlencode(
        {
            "client_id": settings.spotify_client_id,
            "response_type": "code",
            "redirect_uri": settings.spotify_redirect_uri,
            "scope": settings.spotify_scope,
            "state": state,
            "show_dialog": "true",
        }
    )

    return RedirectResponse(url=f"{settings.spotify_authorize_url}?{query}", status_code=302)


@app.get("/auth/callback")
async def auth_callback(request: Request, code: str | None = None, state: str | None = None) -> RedirectResponse:
    expected_state = request.session.get("oauth_state")
    if not code or not state or state != expected_state:
        logger.warning("Spotify callback state validation failed.")
        return RedirectResponse(url=_callback_redirect_url("state_error"), status_code=302)

    credentials = f"{settings.spotify_client_id}:{settings.spotify_client_secret}".encode("utf-8")
    basic_auth = base64.b64encode(credentials).decode("utf-8")

    async with httpx.AsyncClient(timeout=15.0) as client:
        token_response = await client.post(
            settings.spotify_token_url,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": settings.spotify_redirect_uri,
            },
            headers={
                "Authorization": f"Basic {basic_auth}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

    if token_response.status_code >= 400:
        detail = ""
        try:
            payload = token_response.json()
            detail = payload.get("error_description") or payload.get("error") or ""
        except ValueError:
            detail = token_response.text[:120]

        logger.warning(
            "Spotify token exchange failed with status %s: %s",
            token_response.status_code,
            detail or "<no detail>",
        )
        return RedirectResponse(
            url=_callback_redirect_url("token_error", detail or f"http_{token_response.status_code}"),
            status_code=302,
        )

    token_data = token_response.json()
    access_token = token_data.get("access_token")
    if not access_token:
        logger.warning("Spotify token exchange succeeded without an access token.")
        return RedirectResponse(url=_callback_redirect_url("token_missing"), status_code=302)

    request.session.pop("oauth_state", None)
    request.session["access_token"] = access_token
    request.session["refresh_token"] = token_data.get("refresh_token")
    request.session["token_type"] = token_data.get("token_type")
    request.session["expires_in"] = token_data.get("expires_in")

    try:
        profile = await _fetch_spotify_profile(access_token)
    except HTTPException:
        logger.warning("Spotify profile fetch failed after token exchange.")
        return RedirectResponse(url=_callback_redirect_url("profile_error"), status_code=302)

    request.session["spotify_user"] = {
        "id": profile.get("id"),
        "display_name": profile.get("display_name"),
        "email": profile.get("email"),
    }

    return RedirectResponse(url=_callback_redirect_url("success"), status_code=302)


@app.get("/auth/session")
async def auth_session(request: Request) -> dict[str, Any]:
    user = request.session.get("spotify_user") or {}
    authenticated = bool(request.session.get("access_token"))

    if authenticated:
        try:
            await _fetch_spotify_profile(request.session["access_token"])
        except HTTPException as exc:
            if exc.status_code == status.HTTP_401_UNAUTHORIZED:
                request.session.clear()
                user = {}
                authenticated = False
            else:
                raise

    return {
        "authenticated": authenticated,
        "display_name": user.get("display_name"),
        "spotify_user_id": user.get("id"),
        "email": user.get("email"),
    }


@app.post("/auth/logout")
async def auth_logout(request: Request) -> dict[str, str]:
    request.session.clear()
    return {"status": "logged_out"}


@app.post("/cache/rebuild")
async def cache_rebuild(request: Request) -> dict[str, str]:
    _require_token(request)
    _clear_dashboard_caches()
    return {"status": "cache_cleared"}


@app.get("/me/progress")
async def me_progress(request: Request) -> dict[str, Any]:
    key = _progress_key(request)
    if not key:
        return {"active": False, "phase": None, "elapsed_seconds": 0.0}

    progress = LOAD_PROGRESS.get(key)
    if not progress:
        return {"active": False, "phase": None, "elapsed_seconds": 0.0}

    return {
        "active": True,
        "phase": progress.get("phase"),
        "elapsed_seconds": round(time.perf_counter() - float(progress.get("started_at", time.perf_counter())), 1),
        "events": progress.get("events", []),
    }


@app.get("/me")
async def me(request: Request, mode: str = "initial") -> dict[str, Any]:
    token = _require_token(request)
    is_extended = mode == "extended"
    item_limit = SECTION_PREVIEW_LIMIT if is_extended else INITIAL_DASHBOARD_LIMIT
    playlist_limit = None if is_extended else INITIAL_DASHBOARD_LIMIT
    playlist_cache_limit = playlist_limit if playlist_limit is not None else -1
    _set_load_progress(request, "profile")
    try:
        profile = await _fetch_spotify_profile(token)
        user_id = profile.get("id")
        _set_load_progress(request, "recent listening")
        recent_tracks, recent_tracks_available = await _fetch_recent_tracks(token, item_limit)
        cached_playlists = _get_short_cache("owned_playlists", user_id, playlist_cache_limit)
        _set_load_progress(
            request,
            "playlists (cache hit)" if cached_playlists is not None else "playlists (fresh)",
        )
        if cached_playlists is not None:
            playlists, owned_playlists_available = cached_playlists
        else:
            playlists, owned_playlists_available = await _fetch_owned_playlists(token, user_id, playlist_limit)
            _set_short_cache(
                "owned_playlists",
                user_id,
                playlist_cache_limit,
                (playlists, owned_playlists_available),
            )
        _set_load_progress(request, "liked tracks")
        recent_likes_tracks, recent_likes_available = await _fetch_recent_liked_tracks(token, item_limit)
        cached_followed_total = _get_short_cache("followed_artists_total", user_id, 1)
        _set_load_progress(
            request,
            "followed artist count (cache hit)" if cached_followed_total is not None else "followed artist count (fresh)",
        )
        if cached_followed_total is not None:
            followed_artists_total, followed_artists_available = cached_followed_total
        else:
            followed_artists_total, followed_artists_available = await _fetch_followed_artists_total(token)
            _set_short_cache(
                "followed_artists_total",
                user_id,
                1,
                (followed_artists_total, followed_artists_available),
            )
        cached_top_artists_all_time = _get_short_cache("top_artists_long_term", user_id, item_limit)
        _set_load_progress(
            request,
            "top artists all time (cache hit)" if cached_top_artists_all_time is not None else "top artists all time (fresh)",
        )
        if cached_top_artists_all_time is not None:
            top_artists_all_time, top_artists_all_time_available = cached_top_artists_all_time
        else:
            top_artists_all_time, top_artists_all_time_available = await _fetch_top_artists(token, "long_term", item_limit)
            _set_short_cache(
                "top_artists_long_term",
                user_id,
                item_limit,
                (top_artists_all_time, top_artists_all_time_available),
            )
        cached_top_artists_recent = _get_short_cache("top_artists_short_term", user_id, item_limit)
        _set_load_progress(
            request,
            "top artists recent (cache hit)" if cached_top_artists_recent is not None else "top artists recent (fresh)",
        )
        if cached_top_artists_recent is not None:
            top_artists_recent, top_artists_recent_available = cached_top_artists_recent
        else:
            top_artists_recent, top_artists_recent_available = await _fetch_top_artists(token, "short_term", item_limit)
            _set_short_cache(
                "top_artists_short_term",
                user_id,
                item_limit,
                (top_artists_recent, top_artists_recent_available),
            )
        cached_top_tracks_all_time = _get_short_cache("top_tracks_long_term", user_id, item_limit)
        _set_load_progress(
            request,
            "top tracks all time (cache hit)" if cached_top_tracks_all_time is not None else "top tracks all time (fresh)",
        )
        if cached_top_tracks_all_time is not None:
            top_tracks_all_time, top_tracks_all_time_available = cached_top_tracks_all_time
        else:
            top_tracks_all_time, top_tracks_all_time_available = await _fetch_top_tracks(token, "long_term", item_limit)
            _set_short_cache(
                "top_tracks_long_term",
                user_id,
                item_limit,
                (top_tracks_all_time, top_tracks_all_time_available),
            )
        cached_top_tracks_recent = _get_short_cache("top_tracks_short_term", user_id, item_limit)
        _set_load_progress(
            request,
            "top tracks recent (cache hit)" if cached_top_tracks_recent is not None else "top tracks recent (fresh)",
        )
        if cached_top_tracks_recent is not None:
            top_tracks_recent, top_tracks_recent_available = cached_top_tracks_recent
        else:
            top_tracks_recent, top_tracks_recent_available = await _fetch_top_tracks(token, "short_term", item_limit)
            _set_short_cache(
                "top_tracks_short_term",
                user_id,
                item_limit,
                (top_tracks_recent, top_tracks_recent_available),
            )
        top_albums_all_time: list[dict[str, Any]] = []
        top_albums_recent: list[dict[str, Any]] = []
        live_formula_available = any(
            [
                top_tracks_all_time_available,
                top_tracks_recent_available,
                recent_tracks_available,
                recent_likes_available,
            ]
        )
        top_albums_all_time_available = live_formula_available
        top_albums_recent_available = top_albums_all_time_available
        top_playlists_recent: list[dict[str, Any]] = []
        top_playlists_all_time: list[dict[str, Any]] = []
        top_playlists_available = owned_playlists_available
        history_signature = get_history_signature(settings.spotify_history_dir)
        persistent_history_sections = _load_persistent_history_cache(history_signature)
        history_insights_available = False

        if persistent_history_sections:
            _set_load_progress(request, "history favorites (persistent cache hit)")
            top_artists_all_time = persistent_history_sections.get("artists_all_time", [])[:item_limit]
            top_artists_recent = persistent_history_sections.get("artists_recent", [])[:item_limit]
            top_albums_all_time = persistent_history_sections.get("albums_all_time", [])[:item_limit]
            top_albums_recent = persistent_history_sections.get("albums_recent", [])[:item_limit]
            top_artists_all_time_available = bool(top_artists_all_time)
            top_artists_recent_available = bool(top_artists_recent)
            top_albums_all_time_available = bool(top_albums_all_time)
            top_albums_recent_available = bool(top_albums_recent)
            history_insights_available = True
        elif live_formula_available:
            cached_live_favorites = _get_short_cache("live_favorites", user_id, item_limit)
            _set_load_progress(
                request,
                "live artist and album formulas (cache hit)"
                if cached_live_favorites is not None
                else "live artist and album formulas (fresh)",
            )
            if cached_live_favorites is not None:
                (
                    top_artists_all_time,
                    top_artists_recent,
                    top_albums_all_time,
                    top_albums_recent,
                ) = cached_live_favorites
            else:
                artist_lookup = _artist_enrichment_lookup(top_artists_all_time + top_artists_recent)
                top_artists_all_time = _normalize_live_top_artists(
                    long_term_top_tracks=top_tracks_all_time,
                    recent_top_tracks=top_tracks_recent,
                    recent_tracks=recent_tracks,
                    liked_tracks=recent_likes_tracks,
                    enrichment_lookup=artist_lookup,
                    mode="all_time",
                )
                top_artists_recent = _normalize_live_top_artists(
                    long_term_top_tracks=top_tracks_all_time,
                    recent_top_tracks=top_tracks_recent,
                    recent_tracks=recent_tracks,
                    liked_tracks=recent_likes_tracks,
                    enrichment_lookup=artist_lookup,
                    mode="recent",
                )
                top_albums_all_time, top_albums_recent = _normalize_live_top_albums(
                    long_term_top_tracks=top_tracks_all_time,
                    recent_top_tracks=top_tracks_recent,
                    recent_tracks=recent_tracks,
                    liked_tracks=recent_likes_tracks,
                )
                _set_short_cache(
                    "live_favorites",
                    user_id,
                    item_limit,
                    (
                        top_artists_all_time,
                        top_artists_recent,
                        top_albums_all_time,
                        top_albums_recent,
                    ),
                )
            top_artists_all_time_available = True
            top_artists_recent_available = True
            top_albums_all_time_available = True
            top_albums_recent_available = True

        if is_extended and history_signature and not persistent_history_sections:
            _set_load_progress(request, "history calibration (rebuild)")
            history_insights = load_history_insights(settings.spotify_history_dir, SECTION_PREVIEW_LIMIT)
            if history_insights:
                _set_load_progress(request, "history favorites rebuild (search enrichment)")
                artist_lookup = _artist_enrichment_lookup(top_artists_all_time + top_artists_recent)
                album_lookup = _album_enrichment_lookup(
                    top_tracks_all_time + top_tracks_recent + recent_tracks + recent_likes_tracks
                )
                history_sections = {
                    "artists_all_time": await _enrich_history_artists_from_search(
                        token,
                        _merge_history_artists(history_insights["artists_all_time"], artist_lookup),
                    ),
                    "artists_recent": await _enrich_history_artists_from_search(
                        token,
                        _merge_history_artists(history_insights["artists_recent"], artist_lookup),
                    ),
                    "albums_all_time": await _enrich_history_albums_from_search(
                        token,
                        _merge_history_albums(history_insights["albums_all_time"], album_lookup),
                    ),
                    "albums_recent": await _enrich_history_albums_from_search(
                        token,
                        _merge_history_albums(history_insights["albums_recent"], album_lookup),
                    ),
                }
                _store_persistent_history_cache(history_signature, history_sections)
                top_artists_all_time = history_sections["artists_all_time"][:item_limit]
                top_artists_recent = history_sections["artists_recent"][:item_limit]
                top_albums_all_time = history_sections["albums_all_time"][:item_limit]
                top_albums_recent = history_sections["albums_recent"][:item_limit]
                top_artists_all_time_available = True
                top_artists_recent_available = True
                top_albums_all_time_available = True
                top_albums_recent_available = True
                history_insights_available = True

        _set_load_progress(request, "finishing")
        images = profile.get("images") or []
        external_urls = profile.get("external_urls") or {}
        followers = profile.get("followers") or {}

        return {
            "id": profile.get("id"),
            "display_name": profile.get("display_name"),
            "email": profile.get("email"),
            "product": profile.get("product"),
            "country": profile.get("country"),
            "username": profile.get("id"),
            "followers_total": followers.get("total"),
            "followed_artists_total": followed_artists_total,
            "followed_artists_available": followed_artists_available,
            "followed_artists": top_artists_all_time,
            "followed_artists_list_available": top_artists_all_time_available,
            "recent_top_artists": top_artists_recent,
            "recent_top_artists_available": top_artists_recent_available,
            "top_tracks": top_tracks_all_time,
            "top_tracks_available": top_tracks_all_time_available,
            "recent_top_tracks": top_tracks_recent,
            "recent_top_tracks_available": top_tracks_recent_available,
            "top_albums": top_albums_all_time,
            "top_albums_available": top_albums_all_time_available,
            "recent_top_albums": top_albums_recent,
            "recent_top_albums_available": top_albums_recent_available,
            "history_insights_available": history_insights_available,
            "extended_loaded": is_extended,
            "top_playlists_recent": top_playlists_recent,
            "top_playlists_all_time": top_playlists_all_time,
            "top_playlists_available": top_playlists_available,
            "profile_url": external_urls.get("spotify"),
            "image_url": images[0].get("url") if images else None,
            "recent_tracks": recent_tracks,
            "recent_tracks_available": recent_tracks_available,
            "owned_playlists": playlists,
            "owned_playlists_available": owned_playlists_available,
            "recent_likes_tracks": recent_likes_tracks,
            "recent_likes_available": recent_likes_available,
        }
    finally:
        _clear_load_progress(request)
