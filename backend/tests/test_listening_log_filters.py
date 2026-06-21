from __future__ import annotations

import os
import sqlite3
import unittest
from contextlib import closing

from backend.app.db import apply_pending_migrations, ensure_sqlite_db
from backend.app.listening_log import query_listening_log


class ListeningLogFilterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(os.getcwd(), "backend", "tests", "_tmp_listening_log_filters.sqlite3")
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.environ["SQLITE_DB_PATH"] = self.db_path
        ensure_sqlite_db()
        apply_pending_migrations()

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_liked_filter_is_applied_before_limit(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.executemany(
                """
                INSERT INTO fact_play_event (
                  canonical_ended_at,
                  canonical_ms_played,
                  spotify_track_id,
                  track_name_canonical,
                  timing_source,
                  matched_state
                ) VALUES (?, 100000, ?, ?, 'history', 'history_only')
                """,
                (
                    ("2026-06-20T12:03:00Z", "new-unliked-2", "New Unliked 2"),
                    ("2026-06-20T12:02:00Z", "new-unliked-1", "New Unliked 1"),
                    ("2026-06-20T12:01:00Z", "older-liked", "Older Liked"),
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_liked_track_cache (
                  user_id,
                  spotify_track_id,
                  name,
                  liked_at,
                  is_liked,
                  first_seen_at,
                  last_seen_at
                ) VALUES (
                  'user-1',
                  'older-liked',
                  'Older Liked',
                  '2026-06-01T00:00:00Z',
                  1,
                  '2026-06-01T00:00:00Z',
                  '2026-06-20T00:00:00Z'
                )
                """
            )
            connection.commit()

        payload = query_listening_log(limit=1, user_id="user-1", liked_only=True)

        self.assertTrue(payload["liked_only"])
        self.assertEqual(["older-liked"], [item["track_id"] for item in payload["items"]])


if __name__ == "__main__":
    unittest.main()
