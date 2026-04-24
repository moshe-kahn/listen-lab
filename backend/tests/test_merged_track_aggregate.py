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
    insert_raw_spotify_recent_observation,
)
from backend.app.merged_track_aggregate import get_merged_track_aggregate, list_merged_track_aggregate
from backend.app.play_event_projector import reconcile_fact_play_events_for_ingest_run


class MergedTrackAggregateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_merged_track_aggregate.sqlite3",
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

    def test_aggregate_reports_recent_history_and_both_provenance(self) -> None:
        recent_run = "recent-run-1"
        history_run = "history-run-1"
        insert_ingest_run(run_id=recent_run, source_type="spotify_recent", started_at="2026-04-20T12:00:00Z", source_ref="test")
        insert_ingest_run(run_id=history_run, source_type="export", started_at="2026-04-20T12:00:00Z", source_ref="test")

        insert_raw_spotify_recent_observation(
            ingest_run_id=recent_run,
            source_row_key="recent-only-1",
            played_at="2026-04-19T10:00:00Z",
            ms_played_estimate=180000,
            ms_played_method="api_chronology",
            ms_played_confidence="high",
            raw_payload_json="{}",
            spotify_track_id="track-recent",
            spotify_track_uri="spotify:track:track-recent",
            spotify_album_id="album-recent",
            spotify_artist_ids_json=json.dumps(["artist-recent"]),
            track_name_raw="Recent Song",
            artist_name_raw="Recent Artist",
            album_name_raw="Recent Album",
            track_duration_ms=180000,
        )
        insert_raw_spotify_history_observation(
            ingest_run_id=history_run,
            source_row_key="history-only-1",
            played_at="2026-04-10T10:00:00Z",
            ms_played=190000,
            raw_payload_json="{}",
            spotify_track_id="track-history",
            spotify_track_uri="spotify:track:track-history",
            spotify_album_id="album-history",
            spotify_artist_ids_json=json.dumps(["artist-history"]),
            track_name_raw="History Song",
            artist_name_raw="History Artist",
            album_name_raw="History Album",
        )
        insert_raw_spotify_recent_observation(
            ingest_run_id=recent_run,
            source_row_key="both-recent-1",
            played_at="2026-04-18T10:00:00Z",
            ms_played_estimate=200000,
            ms_played_method="api_chronology",
            ms_played_confidence="high",
            raw_payload_json="{}",
            spotify_track_id="track-both",
            spotify_track_uri="spotify:track:track-both",
            spotify_album_id="album-both",
            spotify_artist_ids_json=json.dumps(["artist-both"]),
            track_name_raw="Both Song",
            artist_name_raw="Both Artist",
            album_name_raw="Both Album",
            track_duration_ms=200000,
        )
        insert_raw_spotify_history_observation(
            ingest_run_id=history_run,
            source_row_key="both-history-1",
            played_at="2026-03-18T10:00:00Z",
            ms_played=200000,
            raw_payload_json="{}",
            spotify_track_id="track-both",
            spotify_track_uri="spotify:track:track-both",
            spotify_album_id="album-both",
            spotify_artist_ids_json=json.dumps(["artist-both"]),
            track_name_raw="Both Song",
            artist_name_raw="Both Artist",
            album_name_raw="Both Album",
        )

        reconcile_fact_play_events_for_ingest_run(source_type="spotify_recent", run_id=recent_run)
        reconcile_fact_play_events_for_ingest_run(source_type="export", run_id=history_run)

        items = list_merged_track_aggregate(limit=10, recent_window_days=28)
        by_id = {item["track_id"]: item for item in items}

        self.assertEqual("recent", by_id["track-recent"]["source_label"])
        self.assertTrue(by_id["track-recent"]["has_recent_source"])
        self.assertFalse(by_id["track-recent"]["has_history_source"])

        self.assertEqual("history", by_id["track-history"]["source_label"])
        self.assertTrue(by_id["track-history"]["has_history_source"])
        self.assertFalse(by_id["track-history"]["has_recent_source"])

        self.assertEqual("both", by_id["track-both"]["source_label"])
        self.assertTrue(by_id["track-both"]["has_recent_source"])
        self.assertTrue(by_id["track-both"]["has_history_source"])
        self.assertEqual(2, by_id["track-both"]["play_count"])
        self.assertEqual(1, by_id["track-both"]["recent_play_count"])

    def test_aggregate_source_filter_limits_results(self) -> None:
        recent_run = "recent-run-2"
        history_run = "history-run-2"
        insert_ingest_run(run_id=recent_run, source_type="spotify_recent", started_at="2026-04-20T12:00:00Z", source_ref="test")
        insert_ingest_run(run_id=history_run, source_type="export", started_at="2026-04-20T12:00:00Z", source_ref="test")

        insert_raw_spotify_recent_observation(
            ingest_run_id=recent_run,
            source_row_key="recent-filter-1",
            played_at="2026-04-19T10:00:00Z",
            ms_played_estimate=180000,
            ms_played_method="api_chronology",
            ms_played_confidence="high",
            raw_payload_json="{}",
            spotify_track_id="track-recent-only",
            spotify_track_uri="spotify:track:track-recent-only",
            track_name_raw="Recent Only",
            artist_name_raw="Recent Artist",
            album_name_raw="Recent Album",
            track_duration_ms=180000,
        )
        insert_raw_spotify_history_observation(
            ingest_run_id=history_run,
            source_row_key="history-filter-1",
            played_at="2026-04-10T10:00:00Z",
            ms_played=190000,
            raw_payload_json="{}",
            spotify_track_id="track-history-only",
            spotify_track_uri="spotify:track:track-history-only",
            track_name_raw="History Only",
            artist_name_raw="History Artist",
            album_name_raw="History Album",
        )

        reconcile_fact_play_events_for_ingest_run(source_type="spotify_recent", run_id=recent_run)
        reconcile_fact_play_events_for_ingest_run(source_type="export", run_id=history_run)

        recent_items = list_merged_track_aggregate(limit=10, source_filter="recent")
        history_items = list_merged_track_aggregate(limit=10, source_filter="history")

        self.assertEqual(["track-recent-only"], [item["track_id"] for item in recent_items])
        self.assertEqual(["track-history-only"], [item["track_id"] for item in history_items])

    def test_unknown_identity_rows_are_excluded_but_counted(self) -> None:
        history_run = "history-run-3"
        insert_ingest_run(run_id=history_run, source_type="export", started_at="2026-04-20T12:00:00Z", source_ref="test")

        insert_raw_spotify_history_observation(
            ingest_run_id=history_run,
            source_row_key="unknown-1",
            played_at="2026-04-10T10:00:00Z",
            ms_played=190000,
            raw_payload_json="{}",
            spotify_track_id=None,
            spotify_track_uri=None,
            track_name_raw=None,
            artist_name_raw=None,
            album_name_raw=None,
        )
        insert_raw_spotify_history_observation(
            ingest_run_id=history_run,
            source_row_key="known-1",
            played_at="2026-04-11T10:00:00Z",
            ms_played=190000,
            raw_payload_json="{}",
            spotify_track_id="known-track",
            spotify_track_uri="spotify:track:known-track",
            track_name_raw="Known Song",
            artist_name_raw="Known Artist",
            album_name_raw="Known Album",
        )

        reconcile_fact_play_events_for_ingest_run(source_type="export", run_id=history_run)

        result = get_merged_track_aggregate(limit=10)

        self.assertEqual(["known-track"], [item["track_id"] for item in result["items"]])
        self.assertEqual(1, result["excluded_unknown_identity_count"])


if __name__ == "__main__":
    unittest.main()
