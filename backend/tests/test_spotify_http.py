from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from backend.app.spotify_http import _fetch_spotify_profile, _spotify_get


class SpotifyHttpTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_converts_connect_timeout_to_service_unavailable(self) -> None:
        with patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=httpx.ConnectTimeout("timeout"))):
            with self.assertRaises(HTTPException) as raised:
                await _spotify_get("token", "https://api.spotify.com/v1/me/tracks/contains")

        self.assertEqual(503, raised.exception.status_code)
        self.assertEqual("Spotify data request timed out.", raised.exception.detail)

    async def test_profile_converts_connection_error_to_service_unavailable(self) -> None:
        request = httpx.Request("GET", "https://api.spotify.com/v1/me")
        error = httpx.ConnectError("offline", request=request)
        with patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=error)):
            with self.assertRaises(HTTPException) as raised:
                await _fetch_spotify_profile("token")

        self.assertEqual(503, raised.exception.status_code)
        self.assertEqual("Spotify profile request could not connect.", raised.exception.detail)


if __name__ == "__main__":
    unittest.main()
