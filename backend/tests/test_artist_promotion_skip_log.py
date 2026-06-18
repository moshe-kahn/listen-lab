from __future__ import annotations

import os
import unittest

from backend.app.db import (
    apply_pending_migrations,
    ensure_sqlite_db,
    query_artist_promotion_skip_log,
    record_artist_promotion_skip,
    sqlite_connection,
)


class ArtistPromotionSkipLogTests(unittest.TestCase):
    def setUp(self) -> None:
        self._previous_sqlite_db_path = os.environ.get("SQLITE_DB_PATH")
        self.db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_artist_promotion_skip_log.sqlite3",
        )
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.environ["SQLITE_DB_PATH"] = self.db_path
        ensure_sqlite_db()
        apply_pending_migrations()

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        if self._previous_sqlite_db_path is None:
            os.environ.pop("SQLITE_DB_PATH", None)
        else:
            os.environ["SQLITE_DB_PATH"] = self._previous_sqlite_db_path

    def test_record_artist_promotion_skip_rolls_up_repeated_signature(self) -> None:
        with sqlite_connection(write=True) as connection:
            record_artist_promotion_skip(
                connection,
                reason="missing_album_track_evidence",
                normalized_name="radiohead",
                artist_id=12,
                release_album_id=5,
                release_track_id=8,
                text_only_artist_ids=[12],
            )
            record_artist_promotion_skip(
                connection,
                reason="missing_album_track_evidence",
                normalized_name="radiohead",
                artist_id=12,
                release_album_id=5,
                release_track_id=8,
                text_only_artist_ids=[12],
            )

        payload = query_artist_promotion_skip_log()

        self.assertEqual(payload["summary"]["total"], 2)
        self.assertEqual(payload["summary"]["reason_counts"]["missing_album_track_evidence"], 2)
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["occurrence_count"], 2)
        self.assertEqual(payload["items"][0]["text_only_artist_ids"], [12])


if __name__ == "__main__":
    unittest.main()
