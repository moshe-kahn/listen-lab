from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.app.main import _load_local_history_insights_cache, _load_persistent_history_cache
from backend.app.cache.user_snapshot_cache import _load_user_recent_snapshot


class LocalCacheOfflineFallbackTests(unittest.TestCase):
    def test_persistent_sections_load_when_source_signature_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "history_sections.json"
            path.write_text(json.dumps({
                "cache_version": 1,
                "schema": "history_sections.v1",
                "history_signature": [["history.json", 1, 2]],
                "recent_window_days": 28,
                "sections": {"tracks_all_time": [{"track_name": "Cached Track"}]},
            }))
            with patch("backend.app.main._persistent_history_cache_path", return_value=path):
                sections = _load_persistent_history_cache(None, 28)
        self.assertEqual("Cached Track", sections["tracks_all_time"][0]["track_name"])

    def test_local_insights_load_when_source_signature_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "local_history_insights.json"
            path.write_text(json.dumps({
                "cache_version": 1,
                "schema": "local_history_insights.v1",
                "history_signature": [["history.json", 1, 2]],
                "entries": {"28": {"track_limit": 50, "insights": {"total_play_count": 42}}},
            }))
            with patch("backend.app.main._local_history_insights_cache_path", return_value=path):
                insights = _load_local_history_insights_cache(None, 28, 50)
        self.assertEqual(42, insights["total_play_count"])

    def test_saved_recent_activity_can_be_loaded_when_stale_in_local_mode(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "user_recent_sections.json"
            path.write_text(json.dumps({
                "cache_version": 1,
                "schema": "user_recent_sections.v1",
                "users": {
                    "listener": {
                        "stored_at": 1,
                        "recent_range": "short_term",
                        "snapshot": {"recent_tracks": [{"track_name": "Last Saved"}]},
                    },
                },
            }))
            with patch("backend.app.cache.user_snapshot_cache._user_recent_cache_path", return_value=path):
                snapshot = _load_user_recent_snapshot("listener", "short_term", allow_stale=True)
        self.assertEqual("Last Saved", snapshot["recent_tracks"][0]["track_name"])
        self.assertEqual(1, snapshot["_stored_at"])

    def test_single_saved_user_recent_activity_loads_without_restored_session(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "user_recent_sections.json"
            path.write_text(json.dumps({
                "cache_version": 1,
                "schema": "user_recent_sections.v1",
                "users": {
                    "listener": {
                        "stored_at": 1,
                        "recent_range": "short_term",
                        "snapshot": {"recent_tracks": [{"track_name": "Offline Activity"}]},
                    },
                },
            }))
            with patch("backend.app.cache.user_snapshot_cache._user_recent_cache_path", return_value=path):
                snapshot = _load_user_recent_snapshot(None, "short_term", allow_stale=True)
        self.assertEqual("Offline Activity", snapshot["recent_tracks"][0]["track_name"])

    def test_ambiguous_saved_users_do_not_cross_load_without_session(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "user_recent_sections.json"
            path.write_text(json.dumps({
                "cache_version": 1,
                "schema": "user_recent_sections.v1",
                "users": {
                    "listener-a": {"stored_at": 1, "recent_range": "short_term", "snapshot": {}},
                    "listener-b": {"stored_at": 1, "recent_range": "short_term", "snapshot": {}},
                },
            }))
            with patch("backend.app.cache.user_snapshot_cache._user_recent_cache_path", return_value=path):
                snapshot = _load_user_recent_snapshot(None, "short_term", allow_stale=True)
        self.assertIsNone(snapshot)


if __name__ == "__main__":
    unittest.main()
