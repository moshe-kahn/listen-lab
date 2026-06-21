from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException, status
from starlette.requests import Request

from backend.app.main import me_recent


class RecentRateLimitFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_token_rate_limit_uses_local_recent_payload(self) -> None:
        request = Request({"type": "http", "headers": []})
        request.scope["session"] = {"spotify_user": {"id": "test-user"}}
        local_payload = {"recent_range": "short_term", "recent_tracks": []}
        with (
            patch(
                "backend.app.main._require_token",
                side_effect=HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="cooldown"),
            ),
            patch("backend.app.main.me_local_recent", new=AsyncMock(return_value=local_payload)) as local_recent,
        ):
            payload = await me_recent(request, recent_range="short_term", limit=5)

        self.assertEqual(local_payload, payload)
        local_recent.assert_awaited_once_with(request, recent_range="short_term", limit=5)


if __name__ == "__main__":
    unittest.main()
