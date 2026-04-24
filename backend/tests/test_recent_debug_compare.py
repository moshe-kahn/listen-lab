from __future__ import annotations

import unittest

from backend.app.recent_debug_compare import build_recent_comparison_summary


class RecentDebugCompareTests(unittest.TestCase):
    def test_summary_flags_recent_track_order_duplicates_and_sparse_fields(self) -> None:
        legacy_payload = {
            "recent_tracks": [
                {
                    "track_id": "track-a",
                    "track_name": "Song A",
                    "artist_name": "Artist A",
                    "album_name": "Album A",
                    "album_id": "album-a",
                    "image_url": "https://img/a.jpg",
                    "spotify_played_at": "2026-04-20T10:00:00Z",
                    "spotify_played_at_unix_ms": 1000,
                    "artists": [{"artist_id": "artist-a", "name": "Artist A"}],
                },
                {
                    "track_id": "track-a",
                    "track_name": "Song A",
                    "artist_name": "Artist A",
                    "album_name": "Album A",
                    "album_id": "album-a",
                    "image_url": "https://img/a.jpg",
                    "spotify_played_at": "2026-04-20T09:00:00Z",
                    "spotify_played_at_unix_ms": 900,
                    "artists": [{"artist_id": "artist-a", "name": "Artist A"}],
                },
            ],
            "recent_top_tracks": [],
        }
        db_recent_tracks_payload = {
            "items": [
                {
                    "track_id": "track-a",
                    "track_name": "Song A",
                    "artist_name": "Artist A",
                    "album_name": "Album A",
                    "album_id": "album-a",
                    "image_url": None,
                    "spotify_played_at": "2026-04-20T10:00:00Z",
                    "spotify_played_at_unix_ms": 1000,
                    "artists": None,
                },
                {
                    "track_id": "track-a",
                    "track_name": "Song A",
                    "artist_name": "Artist A",
                    "album_name": "Album A",
                    "album_id": "album-a",
                    "image_url": None,
                    "spotify_played_at": "2026-04-20T09:00:00Z",
                    "spotify_played_at_unix_ms": 900,
                    "artists": None,
                },
            ]
        }
        db_recent_top_tracks_payload = {"items": []}
        db_recent_track_rows = [
            {"played_at": "2026-04-20T10:00:00Z", "raw_row_id": 22},
            {"played_at": "2026-04-20T10:00:00Z", "raw_row_id": 21},
        ]

        summary = build_recent_comparison_summary(
            legacy_payload=legacy_payload,
            db_recent_tracks_payload=db_recent_tracks_payload,
            db_recent_top_tracks_payload=db_recent_top_tracks_payload,
            db_recent_track_rows=db_recent_track_rows,
            inspect_limit=2,
        )

        self.assertTrue(summary["recent_tracks"]["legacy_order_check"]["descending_by_played_at_unix_ms"])
        self.assertTrue(summary["recent_tracks"]["db_order_check"]["ordered_by_played_at_desc_id_desc"])
        self.assertEqual(1, summary["recent_tracks"]["legacy_duplicates"]["duplicate_identity_count"])
        self.assertEqual(2, summary["recent_tracks"]["field_presence"]["legacy"]["image_url"])
        self.assertEqual(0, summary["recent_tracks"]["field_presence"]["db"]["image_url"])
        self.assertEqual(2, summary["recent_tracks"]["index_diffs"]["mismatch_counts"]["image_url"])

    def test_summary_highlights_recent_top_track_semantic_divergence(self) -> None:
        legacy_payload = {
            "recent_tracks": [],
            "recent_top_tracks": [
                {
                    "track_id": "track-a",
                    "track_name": "Song A",
                    "artist_name": "Artist A",
                    "album_name": "Album A",
                    "album_id": "album-a",
                    "image_url": "https://img/a.jpg",
                },
                {
                    "track_id": "track-b",
                    "track_name": "Song B",
                    "artist_name": "Artist B",
                    "album_name": "Album B",
                    "album_id": "album-b",
                    "image_url": "https://img/b.jpg",
                },
            ],
        }
        db_recent_tracks_payload = {"items": []}
        db_recent_top_tracks_payload = {
            "items": [
                {
                    "track_id": "track-b",
                    "track_name": "Song B",
                    "artist_name": "Artist B",
                    "album_name": "Album B",
                    "album_id": "album-b",
                    "image_url": None,
                    "play_count": 3,
                    "recent_play_count": 3,
                    "all_time_play_count": 9,
                    "last_played_at": "2026-04-20T10:00:00Z",
                },
                {
                    "track_id": "track-c",
                    "track_name": "Song C",
                    "artist_name": "Artist C",
                    "album_name": "Album C",
                    "album_id": "album-c",
                    "image_url": None,
                    "play_count": 2,
                    "recent_play_count": 2,
                    "all_time_play_count": 2,
                    "last_played_at": "2026-04-19T10:00:00Z",
                },
            ]
        }

        summary = build_recent_comparison_summary(
            legacy_payload=legacy_payload,
            db_recent_tracks_payload=db_recent_tracks_payload,
            db_recent_top_tracks_payload=db_recent_top_tracks_payload,
            db_recent_track_rows=[],
            inspect_limit=2,
        )

        self.assertEqual(1, summary["recent_top_tracks"]["overlap"]["shared_identity_count"])
        self.assertEqual(1, summary["recent_top_tracks"]["overlap"]["legacy_only_count"])
        self.assertEqual(1, summary["recent_top_tracks"]["overlap"]["db_only_count"])
        self.assertTrue(summary["recent_top_tracks"]["db_order_check"]["ordered_by_recent_play_count_then_recency"])
        self.assertEqual(2, summary["recent_top_tracks"]["index_diffs"]["mismatch_counts"]["track_id"])
        self.assertEqual(2, summary["recent_top_tracks"]["field_presence"]["legacy"]["image_url"])
        self.assertEqual(0, summary["recent_top_tracks"]["field_presence"]["db"]["image_url"])


if __name__ == "__main__":
    unittest.main()
