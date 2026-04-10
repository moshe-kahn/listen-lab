from __future__ import annotations

import base64
import logging
import secrets
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from starlette.middleware.sessions import SessionMiddleware

from app.config import get_settings
from app.history_analysis import load_history_insights

settings = get_settings()
logger = logging.getLogger("listenlab.auth")
SECTION_PREVIEW_LIMIT = 10
ALBUM_ANALYSIS_LIMIT = 10
PLAYLIST_ANALYSIS_LIMIT = 10

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


async def _fetch_recent_tracks(access_token: str) -> tuple[list[dict[str, Any]], bool]:
    try:
        payload = await _spotify_get(
            access_token,
            "https://api.spotify.com/v1/me/player/recently-played",
            {"limit": SECTION_PREVIEW_LIMIT},
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


async def _fetch_recent_liked_tracks(access_token: str) -> tuple[list[dict[str, Any]], bool]:
    try:
        payload = await _spotify_get(
            access_token,
            "https://api.spotify.com/v1/me/tracks",
            {"limit": SECTION_PREVIEW_LIMIT},
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


async def _fetch_top_artists(access_token: str, time_range: str) -> tuple[list[dict[str, Any]], bool]:
    try:
        payload = await _spotify_get(
            access_token,
            "https://api.spotify.com/v1/me/top/artists",
            {"limit": SECTION_PREVIEW_LIMIT, "time_range": time_range},
        )
    except HTTPException as exc:
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return [], False
        raise

    artists_payload = payload.get("items") or []
    return [_normalize_artist(artist) for artist in artists_payload], True


async def _fetch_top_tracks(access_token: str, time_range: str) -> tuple[list[dict[str, Any]], bool]:
    try:
        payload = await _spotify_get(
            access_token,
            "https://api.spotify.com/v1/me/top/tracks",
            {"limit": SECTION_PREVIEW_LIMIT, "time_range": time_range},
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


@app.get("/me")
async def me(request: Request) -> dict[str, Any]:
    token = _require_token(request)
    profile = await _fetch_spotify_profile(token)
    recent_tracks, recent_tracks_available = await _fetch_recent_tracks(token)
    playlists, owned_playlists_available = await _fetch_owned_playlists(token, profile.get("id"))
    recent_likes_tracks, recent_likes_available = await _fetch_recent_liked_tracks(token)
    followed_artists_total, followed_artists_available = await _fetch_followed_artists_total(token)
    top_artists_all_time, top_artists_all_time_available = await _fetch_top_artists(token, "long_term")
    top_artists_recent, top_artists_recent_available = await _fetch_top_artists(token, "short_term")
    top_tracks_all_time, top_tracks_all_time_available = await _fetch_top_tracks(token, "long_term")
    top_tracks_recent, top_tracks_recent_available = await _fetch_top_tracks(token, "short_term")
    top_albums_all_time: list[dict[str, Any]] = []
    top_albums_recent: list[dict[str, Any]] = []
    top_albums_all_time_available = (
        top_tracks_all_time_available and top_tracks_recent_available and recent_tracks_available and recent_likes_available
    )
    top_albums_recent_available = top_albums_all_time_available
    top_playlists_recent: list[dict[str, Any]] = []
    top_playlists_all_time: list[dict[str, Any]] = []
    top_playlists_available = owned_playlists_available
    history_insights = load_history_insights(settings.spotify_history_dir, SECTION_PREVIEW_LIMIT)

    if history_insights:
        artist_lookup = _artist_enrichment_lookup(top_artists_all_time + top_artists_recent)
        album_lookup = _album_enrichment_lookup(
            top_tracks_all_time + top_tracks_recent + recent_tracks + recent_likes_tracks
        )
        top_artists_all_time = _merge_history_artists(history_insights["artists_all_time"], artist_lookup)
        top_artists_recent = _merge_history_artists(history_insights["artists_recent"], artist_lookup)
        top_artists_all_time_available = True
        top_artists_recent_available = True
        top_albums_all_time = await _enrich_history_albums_from_search(
            token,
            _merge_history_albums(history_insights["albums_all_time"], album_lookup),
        )
        top_albums_recent = await _enrich_history_albums_from_search(
            token,
            _merge_history_albums(history_insights["albums_recent"], album_lookup),
        )
        top_albums_all_time_available = True
        top_albums_recent_available = True

    if top_albums_all_time_available and not history_insights:
        candidate_album_tracks = top_tracks_all_time + top_tracks_recent + recent_tracks + recent_likes_tracks
        ranked_candidate_album_ids = _rank_album_candidates(
            short_term_top_tracks=top_tracks_recent,
            long_term_top_tracks=top_tracks_all_time,
            recent_tracks=recent_tracks,
            liked_tracks=recent_likes_tracks,
        )
        candidate_album_ids = ranked_candidate_album_ids[:ALBUM_ANALYSIS_LIMIT]
        album_metadata: dict[str, dict[str, Any]] = {}
        album_track_ids: dict[str, set[str]] = {}

        for track in candidate_album_tracks:
            album_id = track.get("album_id")
            if not album_id or album_id in album_metadata:
                continue

            album_metadata[album_id] = {
                "name": track.get("album_name"),
                "artist_name": track.get("artist_name"),
                "url": track.get("album_url") or track.get("url"),
                "image_url": track.get("image_url"),
            }

        try:
            for album_id in candidate_album_ids:
                try:
                    album_track_ids[album_id] = await _fetch_album_track_ids(token, album_id)
                except HTTPException as exc:
                    if exc.status_code in {status.HTTP_403_FORBIDDEN, status.HTTP_429_TOO_MANY_REQUESTS}:
                        raise
                    logger.warning("Skipping album %s during album analysis: %s", album_id, exc.detail)
            if not album_track_ids:
                top_albums_all_time_available = False
                top_albums_recent_available = False
            else:
                top_albums_recent = _normalize_top_albums(
                    album_metadata=album_metadata,
                    album_track_ids=album_track_ids,
                    short_term_top_tracks=top_tracks_recent,
                    long_term_top_tracks=top_tracks_all_time,
                    recent_tracks=recent_tracks,
                    liked_tracks=recent_likes_tracks,
                    mode="short_term",
                )
                top_albums_all_time = _normalize_top_albums(
                    album_metadata=album_metadata,
                    album_track_ids=album_track_ids,
                    short_term_top_tracks=top_tracks_recent,
                    long_term_top_tracks=top_tracks_all_time,
                    recent_tracks=recent_tracks,
                    liked_tracks=recent_likes_tracks,
                    mode="long_term",
                )
        except HTTPException as exc:
            if exc.status_code == status.HTTP_403_FORBIDDEN:
                top_albums_all_time_available = False
                top_albums_recent_available = False
            elif exc.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
                top_albums_recent = _normalize_top_albums_fallback(
                    short_term_top_tracks=top_tracks_recent,
                    long_term_top_tracks=top_tracks_all_time,
                    recent_tracks=recent_tracks,
                    liked_tracks=recent_likes_tracks,
                    mode="short_term",
                )
                top_albums_all_time = _normalize_top_albums_fallback(
                    short_term_top_tracks=top_tracks_recent,
                    long_term_top_tracks=top_tracks_all_time,
                    recent_tracks=recent_tracks,
                    liked_tracks=recent_likes_tracks,
                    mode="long_term",
                )
                top_albums_all_time_available = True
                top_albums_recent_available = True
            else:
                raise

    if (
        owned_playlists_available
        and playlists
        and top_tracks_all_time_available
        and top_tracks_recent_available
        and recent_tracks_available
        and recent_likes_available
    ):
        try:
            playlist_track_ids: dict[str, set[str]] = {}
            for playlist in playlists[:PLAYLIST_ANALYSIS_LIMIT]:
                playlist_id = playlist.get("playlist_id")
                if not playlist_id:
                    continue
                try:
                    playlist_track_ids[playlist_id] = await _fetch_playlist_track_ids(token, playlist_id)
                except HTTPException as exc:
                    if exc.status_code in {status.HTTP_403_FORBIDDEN, status.HTTP_429_TOO_MANY_REQUESTS}:
                        raise
                    logger.warning("Skipping playlist %s during playlist analysis: %s", playlist_id, exc.detail)
            top_playlists_recent, top_playlists_all_time = _normalize_top_playlists(
                playlists=playlists[:PLAYLIST_ANALYSIS_LIMIT],
                playlist_track_ids=playlist_track_ids,
                short_term_top_track_ids={
                    track["track_id"] for track in top_tracks_recent if track.get("track_id")
                },
                long_term_top_track_ids={
                    track["track_id"] for track in top_tracks_all_time if track.get("track_id")
                },
                recent_track_ids={track["track_id"] for track in recent_tracks if track.get("track_id")},
                liked_track_ids={
                    track["track_id"] for track in recent_likes_tracks if track.get("track_id")
                },
            )
            if not top_playlists_recent and not top_playlists_all_time:
                top_playlists_recent, top_playlists_all_time = _normalize_top_playlists_fallback(
                    playlists[:PLAYLIST_ANALYSIS_LIMIT]
                )
        except HTTPException as exc:
            if exc.status_code == status.HTTP_403_FORBIDDEN:
                top_playlists_available = False
            elif exc.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
                top_playlists_recent, top_playlists_all_time = _normalize_top_playlists_fallback(
                    playlists[:PLAYLIST_ANALYSIS_LIMIT]
                )
                top_playlists_available = True
            else:
                raise
    elif not (
        top_tracks_all_time_available
        and top_tracks_recent_available
        and recent_tracks_available
        and recent_likes_available
    ):
        top_playlists_available = False

    if (
        owned_playlists_available
        and playlists
        and not top_playlists_recent
        and not top_playlists_all_time
    ):
        top_playlists_recent, top_playlists_all_time = _normalize_top_playlists_fallback(
            playlists[:PLAYLIST_ANALYSIS_LIMIT]
        )
        top_playlists_available = True

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
        "history_insights_available": bool(history_insights),
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
