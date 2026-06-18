from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from backend.app.db import apply_pending_migrations, ensure_sqlite_db
from backend.app.spotify_recent_sync import maybe_sync_spotify_recent


class SpotifyRecentSyncTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_spotify_recent_sync.sqlite3",
        )
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.environ["SQLITE_DB_PATH"] = self.db_path
        ensure_sqlite_db()
        apply_pending_migrations()

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    async def test_best_effort_recent_sync_returns_failed_summary_on_fetch_error(self) -> None:
        with patch(
            "backend.app.spotify_recent_sync.fetch_spotify_recent_play_page",
            side_effect=TimeoutError("spotify timed out"),
        ):
            summary = await maybe_sync_spotify_recent(
                "token",
                source_ref="test",
                force=True,
                raise_on_error=False,
            )

        self.assertEqual("failed", summary["status"])
        self.assertTrue(summary["skipped"])
        self.assertEqual("TimeoutError", summary["error_type"])
        self.assertEqual(0, summary["inserted_count"])
        self.assertEqual(0, summary["row_count"])

    async def test_recent_sync_raises_fetch_error_by_default(self) -> None:
        with patch(
            "backend.app.spotify_recent_sync.fetch_spotify_recent_play_page",
            side_effect=TimeoutError("spotify timed out"),
        ):
            with self.assertRaises(TimeoutError):
                await maybe_sync_spotify_recent("token", source_ref="test", force=True)
