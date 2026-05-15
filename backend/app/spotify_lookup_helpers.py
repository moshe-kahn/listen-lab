from __future__ import annotations

from typing import Any

from backend.app.spotify_normalization import _track_identity_key


def _artist_enrichment_lookup(artists: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        (artist.get("name") or "").strip().lower(): artist
        for artist in artists
        if artist.get("name")
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
            "release_year": track.get("album_release_year"),
        }
    return lookup


def _track_enrichment_lookup(tracks: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for track in tracks:
        key = _track_identity_key(track.get("track_name"), track.get("artist_name"))
        if not key:
            continue
        lookup[key] = track
    return lookup


def _merge_history_tracks(
    history_tracks: list[dict[str, Any]],
    enrichment_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for track in history_tracks:
        key = _track_identity_key(track.get("track_name"), track.get("artist_name"))
        enriched = enrichment_lookup.get(key, {}) if key else {}
        results.append(
            {
                **track,
                "track_id": track.get("track_id") or enriched.get("track_id"),
                "album_release_year": track.get("album_release_year") or enriched.get("album_release_year"),
                "url": track.get("url") or enriched.get("url"),
                "album_url": track.get("album_url") or enriched.get("album_url"),
                "image_url": track.get("image_url") or enriched.get("image_url"),
            }
        )
    return results


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
                "release_year": enriched.get("release_year", album.get("release_year")),
            }
        )
    return results
