from __future__ import annotations

import unittest

from backend.app.main import _normalize_recent_track_item_for_route


class RecentTracksRouteBoundaryTests(unittest.TestCase):
    def test_normalize_recent_track_item_for_route_preserves_legacy_shape(self) -> None:
        normalized = _normalize_recent_track_item_for_route(
            {
                "track_id": "track-1",
                "track_name": "Song 1",
                "artist_name": "Artist 1",
                "spotify_played_at": "2026-04-23T21:01:23.724000Z",
                "artists": [
                    {
                        "artist_id": "artist-1",
                        "name": "Artist 1",
                        "uri": "spotify:artist:artist-1",
                        "url": "https://open.spotify.com/artist/artist-1",
                    }
                ],
                "debug": {"source": "db"},
            }
        )

        self.assertEqual("2026-04-23T21:01:23.724Z", normalized["spotify_played_at"])
        self.assertEqual(
            [{"artist_id": "artist-1", "name": "Artist 1"}],
            normalized["artists"],
        )
        self.assertNotIn("debug", normalized)


if __name__ == "__main__":
    unittest.main()
