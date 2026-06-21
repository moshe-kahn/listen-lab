from __future__ import annotations

import json
import os
import tempfile
import unittest

from backend.app.db import apply_pending_migrations, ensure_sqlite_db, sqlite_connection
from backend.app.spotify_recent_sync import _map_recent_item_with_context


class SpotifyRecentContextTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "recent-context.sqlite3")
        os.environ["SQLITE_DB_PATH"] = self.db_path
        ensure_sqlite_db()
        apply_pending_migrations()
        artist = {"id": "roosevelt", "name": "Roosevelt"}
        with sqlite_connection(write=True) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, album_type, release_date, total_tracks,
                  artists_json, images_json, fetched_at, last_status
                ) VALUES ('ep-album', 'Strangers', 'single', '2020-12-09', 4, ?, '[]', '2026-06-19T00:00:00Z', 'ok')
                """,
                (json.dumps([artist]),),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, album_id, artists_json,
                  raw_json, fetched_at, last_status
                ) VALUES ('ep-track', 'Strangers', 220726, 'ep-album', ?, '{}', '2026-06-19T00:00:00Z', 'ok')
                """,
                (json.dumps([artist]),),
            )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_album_context_resolves_equivalent_context_track(self) -> None:
        item = {
            "played_at": "2026-06-19T22:59:59.921Z",
            "context": {"type": "album", "uri": "spotify:album:ep-album"},
            "track": {
                "id": "full-album-track",
                "uri": "spotify:track:full-album-track",
                "name": "Strangers",
                "duration_ms": 220726,
                "artists": [{"id": "roosevelt", "name": "Roosevelt"}],
                "album": {"id": "full-album", "name": "Polydans"},
            },
        }

        mapped = _map_recent_item_with_context(item)

        self.assertEqual("ep-track", mapped["spotify_track_id"])
        self.assertEqual("ep-album", mapped["spotify_album_id"])
        self.assertEqual("Strangers", mapped["album_name_raw"])
        raw_payload = json.loads(mapped["raw_payload_json"])
        self.assertEqual("full-album-track", raw_payload["track"]["id"])
        self.assertEqual("full-album", raw_payload["track"]["album"]["id"])

    def test_non_album_context_keeps_returned_track(self) -> None:
        item = {
            "played_at": "2026-06-19T22:59:59.921Z",
            "context": {"type": "playlist", "uri": "spotify:playlist:test"},
            "track": {
                "id": "full-album-track",
                "uri": "spotify:track:full-album-track",
                "name": "Strangers",
                "duration_ms": 220726,
                "artists": [{"id": "roosevelt", "name": "Roosevelt"}],
                "album": {"id": "full-album", "name": "Polydans"},
            },
        }

        mapped = _map_recent_item_with_context(item)

        self.assertEqual("full-album-track", mapped["spotify_track_id"])
        self.assertEqual("full-album", mapped["spotify_album_id"])


if __name__ == "__main__":
    unittest.main()
