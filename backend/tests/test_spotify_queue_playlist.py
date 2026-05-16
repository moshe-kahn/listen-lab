from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from backend.app.spotify_queue_playlist import (
    MAX_QUEUE_PLAYLIST_URIS,
    sync_queue_playlist,
    validate_queue_playlist_uris,
)


class SpotifyQueuePlaylistTests(unittest.TestCase):
    def test_validate_queue_playlist_uris_requires_non_empty_list(self) -> None:
        with self.assertRaises(HTTPException) as exc:
            validate_queue_playlist_uris([])

        self.assertEqual(400, exc.exception.status_code)

    def test_validate_queue_playlist_uris_rejects_non_track_uri(self) -> None:
        with self.assertRaises(HTTPException) as exc:
            validate_queue_playlist_uris(["spotify:album:album-1"])

        self.assertEqual(400, exc.exception.status_code)

    def test_validate_queue_playlist_uris_caps_to_spotify_limit(self) -> None:
        uris = [f"spotify:track:{index}" for index in range(MAX_QUEUE_PLAYLIST_URIS + 5)]

        result = validate_queue_playlist_uris(uris)

        self.assertEqual(MAX_QUEUE_PLAYLIST_URIS, len(result))

    def test_sync_queue_playlist_reuses_existing_private_owned_playlist(self) -> None:
        existing_playlist = {
            "id": "playlist-1",
            "uri": "spotify:playlist:playlist-1",
            "name": "ListenLab Queue",
            "public": False,
            "owner": {"id": "user-1"},
            "external_urls": {"spotify": "https://open.spotify.com/playlist/playlist-1"},
        }
        spotify_get = AsyncMock(return_value={"items": [existing_playlist]})
        spotify_post = AsyncMock()
        spotify_put = AsyncMock(return_value={"snapshot_id": "snapshot-1"})

        with patch("backend.app.spotify_queue_playlist._spotify_get", spotify_get), patch(
            "backend.app.spotify_queue_playlist._spotify_post",
            spotify_post,
        ), patch("backend.app.spotify_queue_playlist._spotify_put", spotify_put):
            result = asyncio.run(
                sync_queue_playlist(
                    access_token="token",
                    spotify_user_id="user-1",
                    uris=["spotify:track:track-1", "spotify:track:track-2"],
                )
            )

        spotify_post.assert_not_called()
        spotify_put.assert_awaited_once_with(
            "token",
            "https://api.spotify.com/v1/playlists/playlist-1/tracks",
            {"uris": ["spotify:track:track-1", "spotify:track:track-2"]},
        )
        self.assertEqual("playlist-1", result["playlist_id"])
        self.assertEqual("spotify:playlist:playlist-1", result["playlist_uri"])
        self.assertEqual(2, result["item_count"])

    def test_sync_queue_playlist_creates_missing_playlist(self) -> None:
        created_playlist = {
            "id": "playlist-new",
            "uri": "spotify:playlist:playlist-new",
            "name": "ListenLab Queue",
            "public": False,
            "owner": {"id": "user-1"},
            "external_urls": {"spotify": "https://open.spotify.com/playlist/playlist-new"},
        }
        spotify_get = AsyncMock(return_value={"items": []})
        spotify_post = AsyncMock(return_value=created_playlist)
        spotify_put = AsyncMock(return_value={"snapshot_id": "snapshot-1"})

        with patch("backend.app.spotify_queue_playlist._spotify_get", spotify_get), patch(
            "backend.app.spotify_queue_playlist._spotify_post",
            spotify_post,
        ), patch("backend.app.spotify_queue_playlist._spotify_put", spotify_put):
            result = asyncio.run(
                sync_queue_playlist(
                    access_token="token",
                    spotify_user_id="user-1",
                    uris=["spotify:track:track-1"],
                )
            )

        spotify_post.assert_awaited_once_with(
            "token",
            "https://api.spotify.com/v1/users/user-1/playlists",
            {
                "name": "ListenLab Queue",
                "public": False,
                "description": "Private playback queue mirrored by ListenLab.",
            },
        )
        spotify_put.assert_awaited_once()
        self.assertEqual("playlist-new", result["playlist_id"])
        self.assertEqual(1, result["item_count"])


if __name__ == "__main__":
    unittest.main()
