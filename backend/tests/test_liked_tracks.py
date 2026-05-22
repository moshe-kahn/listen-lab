from __future__ import annotations

import os
import time
import unittest
from typing import Any
from unittest.mock import patch

from fastapi import HTTPException, status
from fastapi.testclient import TestClient

from backend.app.artwork import resolve_track_artwork
from backend.app.db import apply_pending_migrations, ensure_sqlite_db, sqlite_connection
from backend.app.liked_tracks import (
    is_liked_track_cached,
    list_cached_liked_tracks,
    mark_missing_liked_tracks_unliked,
    sync_spotify_liked_tracks,
    upsert_liked_tracks,
)
from backend.app.main import app


def _saved_item(track_id: str, added_at: str) -> dict[str, Any]:
    return {
        "added_at": added_at,
        "track": {
            "id": track_id,
            "uri": f"spotify:track:{track_id}",
            "name": f"Track {track_id}",
            "artists": [{"id": f"artist-{track_id}", "name": "Artist"}],
            "album": {
                "id": f"album-{track_id}",
                "name": f"Album {track_id}",
                "images": [{"url": f"https://images.example/{track_id}.jpg"}],
            },
            "duration_ms": 180000,
            "popularity": 50,
            "explicit": False,
        },
    }


class LikedTracksSyncTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(os.getcwd(), "backend", "tests", "_tmp_liked_tracks.sqlite3")
        for suffix in ("", "-wal", "-shm"):
            path = f"{self.db_path}{suffix}"
            if os.path.exists(path):
                os.remove(path)
        os.environ["SQLITE_DB_PATH"] = self.db_path
        ensure_sqlite_db()
        apply_pending_migrations()

    def tearDown(self) -> None:
        for suffix in ("", "-wal", "-shm"):
            path = f"{self.db_path}{suffix}"
            if os.path.exists(path):
                for _ in range(5):
                    try:
                        os.remove(path)
                        break
                    except PermissionError:
                        time.sleep(0.1)

    async def test_multi_page_quick_sync_upserts_rows_and_orders_by_liked_at(self) -> None:
        pages = {
            0: {
                "items": [_saved_item("a", "2026-05-01T00:00:00Z")],
                "limit": 50,
                "offset": 0,
                "total": 2,
                "next": "next-page",
            },
            50: {
                "items": [_saved_item("b", "2026-05-02T00:00:00Z")],
                "limit": 50,
                "offset": 50,
                "total": 2,
                "next": None,
            },
        }

        async def spotify_get(_token: str, _url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            return pages[int((params or {}).get("offset", 0))]

        result = await sync_spotify_liked_tracks(
            user_id="user-1",
            access_token="token",
            mode="quick",
            spotify_get=spotify_get,
        )

        self.assertEqual("natural_end", result["stopped_reason"])
        self.assertEqual(2, result["pages_seen"])
        self.assertEqual(2, result["tracks_upserted"])
        cached = list_cached_liked_tracks("user-1")
        self.assertEqual(["b", "a"], [item["track_id"] for item in cached["items"]])
        self.assertEqual("https://images.example/b.jpg", cached["items"][0]["image_url"])
        self.assertEqual("artist-b", cached["items"][0]["artists"][0]["artist_id"])

    async def test_artwork_resolver_uses_catalog_before_spotify_fetch(self) -> None:
        with sqlite_connection(write=True) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, images_json, raw_json, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "album-a",
                    '[{"url": "https://images.example/catalog-a.jpg"}]',
                    "{}",
                    "2026-05-01T00:00:00Z",
                    "ok",
                ),
            )

        with patch(
            "backend.app.artwork._spotify_get",
            side_effect=AssertionError("catalog artwork should avoid Spotify fetch"),
        ):
            resolved = await resolve_track_artwork(
                [
                    {
                        "track_id": "track-a",
                        "track_name": "Track A",
                        "artist_name": "Artist",
                        "album_name": "Album A",
                        "album_id": "album-a",
                        "image_url": None,
                    }
                ],
                access_token="token",
            )

        self.assertEqual("https://images.example/catalog-a.jpg", resolved[0]["image_url"])

    async def test_artwork_resolver_fetches_missing_album_art_from_spotify(self) -> None:
        async def spotify_get(_token: str, url: str, _params: dict[str, Any] | None = None) -> dict[str, Any]:
            self.assertEqual("https://api.spotify.com/v1/albums/album-spotify", url)
            return {
                "id": "album-spotify",
                "name": "Album Spotify",
                "images": [{"url": "https://images.example/spotify-a.jpg"}],
            }

        with patch("backend.app.artwork._spotify_get", side_effect=spotify_get):
            resolved = await resolve_track_artwork(
                [
                    {
                        "track_id": "track-spotify",
                        "track_name": "Track Spotify",
                        "artist_name": "Artist",
                        "album_name": "Album Spotify",
                        "album_id": "album-spotify",
                        "image_url": None,
                    }
                ],
                access_token="token",
            )

        self.assertEqual("https://images.example/spotify-a.jpg", resolved[0]["image_url"])
        with sqlite_connection() as connection:
            row = connection.execute(
                "SELECT images_json FROM spotify_album_catalog WHERE spotify_album_id = ?",
                ("album-spotify",),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertIn("https://images.example/spotify-a.jpg", row[0])

    async def test_artwork_resolver_fetches_missing_artist_art_from_spotify(self) -> None:
        async def spotify_get(_token: str, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            self.assertEqual("https://api.spotify.com/v1/artists", url)
            self.assertEqual({"ids": "artist-spotify"}, params)
            return {
                "artists": [
                    {
                        "id": "artist-spotify",
                        "name": "Artist Spotify",
                        "external_urls": {"spotify": "https://open.spotify.com/artist/artist-spotify"},
                        "images": [{"url": "https://images.example/artist-spotify.jpg"}],
                    }
                ]
            }

        with patch("backend.app.artwork._spotify_get", side_effect=spotify_get):
            resolved = await resolve_track_artwork(
                [
                    {
                        "track_id": "track-spotify",
                        "track_name": "Track Spotify",
                        "artist_name": "Artist Spotify",
                        "album_name": "Album Spotify",
                        "album_id": "album-spotify",
                        "image_url": "https://images.example/album-present.jpg",
                        "artists": [
                            {
                                "artist_id": "artist-spotify",
                                "id": "artist-spotify",
                                "name": "Artist Spotify",
                            }
                        ],
                    }
                ],
                access_token="token",
            )

        artist = resolved[0]["artists"][0]
        self.assertEqual("https://images.example/artist-spotify.jpg", artist["image_url"])
        self.assertEqual("https://open.spotify.com/artist/artist-spotify", artist["url"])

    async def test_liked_tracks_endpoint_fetches_artist_art_when_album_art_exists(self) -> None:
        upsert_liked_tracks(
            "user-1",
            [
                {
                    "spotify_track_id": "track-route",
                    "uri": "spotify:track:track-route",
                    "name": "Track Route",
                    "artist_names": ["Route Artist"],
                    "artist_ids": ["route-artist"],
                    "album_name": "Route Album",
                    "album_spotify_id": "route-album",
                    "album_image_url": "https://images.example/route-album.jpg",
                    "duration_ms": 120000,
                    "popularity": None,
                    "explicit": None,
                    "liked_at": "2026-05-01T00:00:00Z",
                }
            ],
            "2026-05-01T00:00:00Z",
        )

        async def spotify_get(_token: str, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            self.assertEqual("https://api.spotify.com/v1/artists", url)
            self.assertEqual({"ids": "route-artist"}, params)
            return {
                "artists": [
                    {
                        "id": "route-artist",
                        "name": "Route Artist",
                        "external_urls": {"spotify": "https://open.spotify.com/artist/route-artist"},
                        "images": [{"url": "https://images.example/route-artist.jpg"}],
                    }
                ]
            }

        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main._require_token",
            return_value="token",
        ) as require_token, patch("backend.app.artwork._spotify_get", side_effect=spotify_get):
            client = TestClient(app)
            response = client.get("/me/liked-tracks")

        self.assertEqual(200, response.status_code)
        require_token.assert_called_once()
        item = response.json()["items"][0]
        self.assertEqual("https://images.example/route-album.jpg", item["image_url"])
        self.assertEqual("https://images.example/route-artist.jpg", item["artists"][0]["image_url"])

    async def test_quick_sync_does_not_mark_missing_existing_rows_unliked(self) -> None:
        upsert_liked_tracks(
            "user-1",
            [
                {
                    "spotify_track_id": "old",
                    "uri": "spotify:track:old",
                    "name": "Old",
                    "artist_names": ["Artist"],
                    "album_name": "Old Album",
                    "album_spotify_id": "old-album",
                    "duration_ms": 120000,
                    "popularity": None,
                    "explicit": None,
                    "liked_at": "2026-04-01T00:00:00Z",
                }
            ],
            "2026-05-01T00:00:00Z",
        )

        async def spotify_get(_token: str, _url: str, _params: dict[str, Any] | None = None) -> dict[str, Any]:
            return {
                "items": [_saved_item("new", "2026-05-02T00:00:00Z")],
                "limit": 50,
                "offset": 0,
                "total": 1,
                "next": None,
            }

        await sync_spotify_liked_tracks(user_id="user-1", access_token="token", mode="quick", spotify_get=spotify_get)

        cached = list_cached_liked_tracks("user-1", limit=10)
        self.assertEqual({"new", "old"}, {item["track_id"] for item in cached["items"]})

    async def test_completed_full_sync_marks_missing_active_rows_unliked(self) -> None:
        upsert_liked_tracks(
            "user-1",
            [
                {
                    "spotify_track_id": "old",
                    "uri": "spotify:track:old",
                    "name": "Old",
                    "artist_names": ["Artist"],
                    "album_name": "Old Album",
                    "album_spotify_id": "old-album",
                    "duration_ms": 120000,
                    "popularity": None,
                    "explicit": None,
                    "liked_at": "2026-04-01T00:00:00Z",
                }
            ],
            "2026-05-01T00:00:00Z",
        )

        async def spotify_get(_token: str, _url: str, _params: dict[str, Any] | None = None) -> dict[str, Any]:
            return {
                "items": [_saved_item("new", "2026-05-02T00:00:00Z")],
                "limit": 50,
                "offset": 0,
                "total": 1,
                "next": None,
            }

        result = await sync_spotify_liked_tracks(user_id="user-1", access_token="token", mode="full", spotify_get=spotify_get)

        self.assertTrue(result["full_completed"])
        self.assertEqual(1, result["marked_unliked"])
        active = list_cached_liked_tracks("user-1", limit=10)
        self.assertEqual(["new"], [item["track_id"] for item in active["items"]])
        all_rows = list_cached_liked_tracks("user-1", limit=10, active_only=False)
        old = next(item for item in all_rows["items"] if item["track_id"] == "old")
        self.assertFalse(old["is_liked"])
        self.assertIsNotNone(old["unliked_at"])

    async def test_partial_full_sync_from_cap_or_rate_limit_does_not_mark_unliked(self) -> None:
        upsert_liked_tracks(
            "user-1",
            [
                {
                    "spotify_track_id": "old",
                    "uri": "spotify:track:old",
                    "name": "Old",
                    "artist_names": ["Artist"],
                    "album_name": "Old Album",
                    "album_spotify_id": "old-album",
                    "duration_ms": 120000,
                    "popularity": None,
                    "explicit": None,
                    "liked_at": "2026-04-01T00:00:00Z",
                }
            ],
            "2026-05-01T00:00:00Z",
        )

        async def capped_get(_token: str, _url: str, _params: dict[str, Any] | None = None) -> dict[str, Any]:
            return {
                "items": [_saved_item("new", "2026-05-02T00:00:00Z")],
                "limit": 50,
                "offset": 0,
                "total": 2,
                "next": "next-page",
            }

        cap_result = await sync_spotify_liked_tracks(
            user_id="user-1",
            access_token="token",
            mode="full",
            spotify_get=capped_get,
            full_page_cap=1,
        )
        self.assertEqual("cap_reached", cap_result["stopped_reason"])
        self.assertFalse(cap_result["full_completed"])
        self.assertEqual(0, cap_result["marked_unliked"])
        self.assertEqual({"new", "old"}, {item["track_id"] for item in list_cached_liked_tracks("user-1", limit=10)["items"]})

        async def rate_limited_get(_token: str, _url: str, _params: dict[str, Any] | None = None) -> dict[str, Any]:
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="rate limited")

        rate_result = await sync_spotify_liked_tracks(
            user_id="user-1",
            access_token="token",
            mode="full",
            spotify_get=rate_limited_get,
        )
        self.assertEqual("rate_limited", rate_result["stopped_reason"])
        self.assertFalse(rate_result["full_completed"])
        self.assertEqual(0, rate_result["marked_unliked"])
        self.assertEqual({"new", "old"}, {item["track_id"] for item in list_cached_liked_tracks("user-1", limit=10)["items"]})

    async def test_unexpected_page_shape_is_partial_and_updates_metadata(self) -> None:
        async def spotify_get(_token: str, _url: str, _params: dict[str, Any] | None = None) -> dict[str, Any]:
            return {"limit": 50, "offset": 0, "total": 0, "next": None}

        result = await sync_spotify_liked_tracks(user_id="user-1", access_token="token", mode="full", spotify_get=spotify_get)

        self.assertEqual("unexpected_response", result["stopped_reason"])
        self.assertFalse(result["full_completed"])
        self.assertEqual("full", result["metadata"]["last_sync_mode"])
        self.assertEqual("unexpected_response", result["metadata"]["last_stopped_reason"])
        self.assertIsNone(result["metadata"]["last_completed_full_sync_at"])

    async def test_cached_contains_returns_true_only_for_active_user_track(self) -> None:
        upsert_liked_tracks(
            "user-1",
            [
                {
                    "spotify_track_id": "track-a",
                    "uri": "spotify:track:track-a",
                    "name": "Track A",
                    "artist_names": ["Artist"],
                    "album_name": "Album A",
                    "album_spotify_id": "album-a",
                    "duration_ms": 120000,
                    "popularity": None,
                    "explicit": None,
                    "liked_at": "2026-05-01T00:00:00Z",
                }
            ],
            "2026-05-01T00:00:00Z",
        )
        upsert_liked_tracks(
            "user-2",
            [
                {
                    "spotify_track_id": "track-b",
                    "uri": "spotify:track:track-b",
                    "name": "Track B",
                    "artist_names": ["Artist"],
                    "album_name": "Album B",
                    "album_spotify_id": "album-b",
                    "duration_ms": 120000,
                    "popularity": None,
                    "explicit": None,
                    "liked_at": "2026-05-01T00:00:00Z",
                }
            ],
            "2026-05-01T00:00:00Z",
        )

        self.assertTrue(is_liked_track_cached("user-1", "track-a"))
        self.assertFalse(is_liked_track_cached("user-1", "track-b"))
        self.assertFalse(is_liked_track_cached("user-1", "missing"))
        self.assertFalse(is_liked_track_cached("user-1", "   "))

        mark_missing_liked_tracks_unliked("user-1", set(), "2026-05-02T00:00:00Z")
        self.assertFalse(is_liked_track_cached("user-1", "track-a"))

    async def test_contains_endpoint_reads_cache_without_spotify_profile_call(self) -> None:
        upsert_liked_tracks(
            "user-1",
            [
                {
                    "spotify_track_id": "track-a",
                    "uri": "spotify:track:track-a",
                    "name": "Track A",
                    "artist_names": ["Artist"],
                    "album_name": "Album A",
                    "album_spotify_id": "album-a",
                    "duration_ms": 120000,
                    "popularity": None,
                    "explicit": None,
                    "liked_at": "2026-05-01T00:00:00Z",
                }
            ],
            "2026-05-01T00:00:00Z",
        )

        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main._fetch_spotify_profile",
            side_effect=AssertionError("contains endpoint must not fetch Spotify profile"),
        ):
            client = TestClient(app)
            response = client.get("/me/liked-tracks/contains?spotify_track_id=track-a")
            absent_response = client.get("/me/liked-tracks/contains?spotify_track_id=missing")
            blank_response = client.get("/me/liked-tracks/contains?spotify_track_id=%20%20")

        self.assertEqual(200, response.status_code)
        self.assertEqual({"spotify_track_id": "track-a", "is_liked": True}, response.json())
        self.assertEqual(200, absent_response.status_code)
        self.assertEqual({"spotify_track_id": "missing", "is_liked": False}, absent_response.json())
        self.assertEqual(400, blank_response.status_code)

    async def test_sync_failure_simulation_requires_env_gate_and_debug_header(self) -> None:
        upsert_liked_tracks(
            "user-1",
            [
                {
                    "spotify_track_id": "track-a",
                    "uri": "spotify:track:track-a",
                    "name": "Track A",
                    "artist_names": ["Artist"],
                    "album_name": "Album A",
                    "album_spotify_id": "album-a",
                    "duration_ms": 120000,
                    "popularity": None,
                    "explicit": None,
                    "liked_at": "2026-05-01T00:00:00Z",
                }
            ],
            "2026-05-01T00:00:00Z",
        )
        before = list_cached_liked_tracks("user-1")

        with patch("backend.app.main._require_user_id", return_value="user-1"):
            client = TestClient(app)
            with patch.dict(os.environ, {"LISTENLAB_ENABLE_DEBUG_SYNC_FAILURE": "0"}), patch(
                "backend.app.main._require_token",
                side_effect=AssertionError("disabled simulation must not fall through to Spotify token handling"),
            ):
                disabled_response = client.post(
                    "/me/liked-tracks/sync",
                    headers={"X-ListenLab-Debug-Sync-Failure": "1"},
                    json={"mode": "quick", "simulate_failure_reason": "missing_scope"},
                )

            with patch.dict(os.environ, {"LISTENLAB_ENABLE_DEBUG_SYNC_FAILURE": "1"}), patch(
                "backend.app.main._require_token",
                side_effect=AssertionError("simulation must not require a Spotify token"),
            ):
                missing_header_response = client.post(
                    "/me/liked-tracks/sync",
                    json={"mode": "quick", "simulate_failure_reason": "missing_scope"},
                )
                simulated_response = client.post(
                    "/me/liked-tracks/sync",
                    headers={"X-ListenLab-Debug-Sync-Failure": "1"},
                    json={"mode": "quick", "simulate_failure_reason": "missing_scope"},
                )
                unsupported_response = client.post(
                    "/me/liked-tracks/sync",
                    headers={"X-ListenLab-Debug-Sync-Failure": "1"},
                    json={"mode": "quick", "simulate_failure_reason": "natural_end"},
                )

        after = list_cached_liked_tracks("user-1")
        payload = simulated_response.json()

        self.assertEqual(400, disabled_response.status_code)
        self.assertEqual("Liked tracks sync failure simulation is disabled.", disabled_response.json()["detail"])
        self.assertEqual(400, missing_header_response.status_code)
        self.assertEqual(200, simulated_response.status_code)
        self.assertEqual(400, unsupported_response.status_code)
        self.assertEqual("quick", payload["sync_mode"])
        self.assertEqual("missing_scope", payload["stopped_reason"])
        self.assertEqual(0, payload["tracks_upserted"])
        self.assertEqual(0, payload["pages_seen"])
        self.assertEqual(1, payload["active_likes"])
        self.assertEqual([item["track_id"] for item in before["items"]], [item["track_id"] for item in after["items"]])
        self.assertEqual(before["metadata"], after["metadata"])
