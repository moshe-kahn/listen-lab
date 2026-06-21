from __future__ import annotations

import unittest
from unittest.mock import patch

from starlette.requests import Request

from backend.app.routes.auth_routes import _offline_oauth_profile


def _request(session: dict[str, object]) -> Request:
    request = Request({"type": "http", "headers": []})
    request.scope["session"] = session
    return request


class AuthOfflineFallbackTests(unittest.TestCase):
    def test_uses_existing_session_identity_without_database_lookup(self) -> None:
        request = _request({"spotify_user": {"id": "known-user", "display_name": "Known"}})
        with patch("backend.app.routes.auth_routes.list_spotify_auth_users") as users:
            profile = _offline_oauth_profile(request)
        self.assertEqual({"id": "known-user", "display_name": "Known", "email": None}, profile)
        users.assert_not_called()

    def test_uses_only_local_identity_when_exactly_one_is_available(self) -> None:
        request = _request({})
        with patch(
            "backend.app.routes.auth_routes.list_spotify_auth_users",
            return_value=[{"user_id": "local-user", "spotify_user_id": "spotify-user"}],
        ):
            profile = _offline_oauth_profile(request)
        self.assertEqual({"id": "spotify-user", "display_name": None, "email": None}, profile)

    def test_refuses_ambiguous_local_identity(self) -> None:
        request = _request({})
        with patch(
            "backend.app.routes.auth_routes.list_spotify_auth_users",
            return_value=[{"user_id": "one"}, {"user_id": "two"}],
        ):
            profile = _offline_oauth_profile(request)
        self.assertIsNone(profile)


if __name__ == "__main__":
    unittest.main()
