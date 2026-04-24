from __future__ import annotations

import json
import os
import time
import unittest

from backend.app.db import (
    apply_pending_migrations,
    ensure_sqlite_db,
    insert_ingest_run,
    insert_raw_spotify_history_observation,
)
from backend.app.play_event_projector import reconcile_fact_play_events_for_ingest_run
from backend.app.recent_top_tracks_db import build_recent_top_tracks_section_from_db


class RecentTopTracksDbTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_recent_top_tracks.sqlite3",
        )
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.environ["SQLITE_DB_PATH"] = self.db_path
        ensure_sqlite_db()
        apply_pending_migrations()

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            for _ in range(5):
                try:
                    os.remove(self.db_path)
                    break
                except PermissionError:
                    time.sleep(0.1)

    def test_build_recent_top_tracks_section_ranks_by_recent_window_then_recency(self) -> None:
        run_id = "history-run-1"
        insert_ingest_run(
            run_id=run_id,
            source_type="export",
            started_at="2026-04-20T12:00:00Z",
            source_ref="test",
        )

        self._insert_history_row(
            run_id=run_id,
            source_row_key="track-a-old",
            played_at="2026-01-01T10:00:00Z",
            spotify_track_id="track-a",
            spotify_track_uri="spotify:track:track-a",
            track_name="Song A",
            artist_name="Artist A",
            album_name="Album A",
        )
        self._insert_history_row(
            run_id=run_id,
            source_row_key="track-a-recent-1",
            played_at="2026-04-19T10:00:00Z",
            spotify_track_id="track-a",
            spotify_track_uri="spotify:track:track-a",
            track_name="Song A",
            artist_name="Artist A",
            album_name="Album A",
        )
        self._insert_history_row(
            run_id=run_id,
            source_row_key="track-a-recent-2",
            played_at="2026-04-18T10:00:00Z",
            spotify_track_id="track-a",
            spotify_track_uri="spotify:track:track-a",
            track_name="Song A",
            artist_name="Artist A",
            album_name="Album A",
        )
        self._insert_history_row(
            run_id=run_id,
            source_row_key="track-b-recent-1",
            played_at="2026-04-20T09:00:00Z",
            spotify_track_id="track-b",
            spotify_track_uri="spotify:track:track-b",
            track_name="Song B",
            artist_name="Artist B",
            album_name="Album B",
        )
        self._insert_history_row(
            run_id=run_id,
            source_row_key="track-b-recent-2",
            played_at="2026-04-17T09:00:00Z",
            spotify_track_id="track-b",
            spotify_track_uri="spotify:track:track-b",
            track_name="Song B",
            artist_name="Artist B",
            album_name="Album B",
        )
        self._insert_history_row(
            run_id=run_id,
            source_row_key="track-c-old",
            played_at="2025-01-01T09:00:00Z",
            spotify_track_id="track-c",
            spotify_track_uri="spotify:track:track-c",
            track_name="Song C",
            artist_name="Artist C",
            album_name="Album C",
        )

        reconcile_fact_play_events_for_ingest_run(source_type="export", run_id=run_id)

        payload = build_recent_top_tracks_section_from_db(
            limit=5,
            recent_range="short_term",
            recent_window_days=28,
            as_of_iso="2026-04-20T12:00:00Z",
        )

        self.assertTrue(payload["available"])
        self.assertEqual(["track-b", "track-a"], [item["track_id"] for item in payload["items"]])
        self.assertEqual(2, payload["items"][0]["recent_play_count"])
        self.assertEqual(2, payload["items"][1]["recent_play_count"])
        self.assertEqual(2, payload["items"][0]["all_time_play_count"])
        self.assertEqual(3, payload["items"][1]["all_time_play_count"])
        self.assertEqual(2, payload["items"][0]["play_count"])
        self.assertEqual("2026-04-20T09:00:00Z", payload["items"][0]["last_played_at"])
        self.assertEqual("2026-01-01T10:00:00Z", payload["items"][1]["first_played_at"])

    def test_build_recent_top_tracks_section_uses_synthetic_identity_when_spotify_ids_are_missing(self) -> None:
        run_id = "history-run-2"
        insert_ingest_run(
            run_id=run_id,
            source_type="export",
            started_at="2026-04-20T12:00:00Z",
            source_ref="test",
        )

        self._insert_history_row(
            run_id=run_id,
            source_row_key="local-1",
            played_at="2026-04-19T08:00:00Z",
            spotify_track_id=None,
            spotify_track_uri=None,
            track_name="Local Song",
            artist_name="Local Artist",
            album_name="Local Album",
        )
        self._insert_history_row(
            run_id=run_id,
            source_row_key="local-2",
            played_at="2026-04-18T08:00:00Z",
            spotify_track_id=None,
            spotify_track_uri=None,
            track_name="Local Song",
            artist_name="Local Artist",
            album_name="Local Album",
        )

        reconcile_fact_play_events_for_ingest_run(source_type="export", run_id=run_id)

        payload = build_recent_top_tracks_section_from_db(
            limit=5,
            recent_range="short_term",
            recent_window_days=28,
            as_of_iso="2026-04-20T12:00:00Z",
        )

        self.assertEqual(1, len(payload["items"]))
        self.assertEqual("__unknown__:local song:local artist", payload["items"][0]["track_id"])
        self.assertIsNone(payload["items"][0]["uri"])
        self.assertEqual(2, payload["items"][0]["recent_play_count"])

    def _insert_history_row(
        self,
        *,
        run_id: str,
        source_row_key: str,
        played_at: str,
        spotify_track_id: str | None,
        spotify_track_uri: str | None,
        track_name: str,
        artist_name: str,
        album_name: str,
    ) -> None:
        insert_raw_spotify_history_observation(
            ingest_run_id=run_id,
            source_row_key=source_row_key,
            played_at=played_at,
            ms_played=180000,
            spotify_track_uri=spotify_track_uri,
            spotify_track_id=spotify_track_id,
            track_name_raw=track_name,
            artist_name_raw=artist_name,
            album_name_raw=album_name,
            spotify_album_id=f"album-{source_row_key}",
            spotify_artist_ids_json=json.dumps([f"artist-{source_row_key}"]),
            reason_start="trackdone",
            reason_end="trackdone",
            skipped=0,
            shuffle=0,
            offline=0,
            platform="desktop",
            conn_country="US",
            private_session=0,
            raw_payload_json="{}",
        )


if __name__ == "__main__":
    unittest.main()
