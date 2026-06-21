from __future__ import annotations

import json
import os
import sqlite3
import time
import unittest
from contextlib import closing

from backend.app.db import (
    apply_pending_migrations,
    ensure_sqlite_db,
    insert_ingest_run,
    insert_listenlab_player_play,
    insert_raw_spotify_history_observation,
    insert_raw_spotify_recent_observation,
    refresh_source_track_play_count_cache,
    update_listenlab_player_play_progress,
)
from backend.app.play_event_projector import (
    audit_eligible_unlinked_history_rows,
    backfill_fact_play_event_release_track_identity,
    delete_projected_podcast_episode_facts,
    delete_projected_unidentifiable_history_facts,
    reconcile_fact_play_events_for_ingest_run,
)
from backend.scripts.ingest_history_with_checkpoints import commit_and_project_checkpoint
from backend.app.release_track_metadata import enrich_track_rows_with_release_metadata


class PlayEventProjectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_play_event_projection.sqlite3",
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

    def test_checkpoint_resume_globally_recovers_failed_run_orphans_idempotently(self) -> None:
        insert_ingest_run(
            run_id="crashed-checkpoint-run",
            source_type="export",
            started_at="2026-04-17T18:00:00Z",
            source_ref="test-crash",
            status="failed",
        )
        insert_ingest_run(
            run_id="resumed-checkpoint-run",
            source_type="export",
            started_at="2026-04-17T19:00:00Z",
            source_ref="test-resume",
        )
        for run_id, row_key, played_at, track_id in (
            ("crashed-checkpoint-run", "orphan-from-crash", "2026-04-17T18:10:00Z", "crash-track"),
            ("resumed-checkpoint-run", "row-from-resume", "2026-04-17T19:10:00Z", "resume-track"),
        ):
            insert_raw_spotify_history_observation(
                ingest_run_id=run_id,
                source_row_key=row_key,
                played_at=played_at,
                ms_played=240000,
                spotify_track_uri=f"spotify:track:{track_id}",
                spotify_track_id=track_id,
                track_name_raw=track_id,
                artist_name_raw="Checkpoint Artist",
                album_name_raw="Checkpoint Album",
                spotify_album_id="checkpoint-album",
                spotify_artist_ids_json=json.dumps(["checkpoint-artist"]),
                reason_start="trackdone",
                reason_end="trackdone",
                skipped=0,
                shuffle=0,
                offline=0,
                platform="test",
                conn_country="US",
                private_session=0,
                raw_payload_json="{}",
            )

        self.assertEqual(2, audit_eligible_unlinked_history_rows()["eligible_unlinked_history_count"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            first = commit_and_project_checkpoint(
                connection,
                run_id="resumed-checkpoint-run",
                row_count=1,
                inserted_count=1,
                duplicate_count=0,
                error_count=0,
            )
            second = commit_and_project_checkpoint(
                connection,
                run_id="resumed-checkpoint-run",
                row_count=1,
                inserted_count=1,
                duplicate_count=0,
                error_count=0,
            )
            run_row = connection.execute(
                """
                SELECT row_count, inserted_count, duplicate_count, error_count, last_heartbeat_at
                FROM ingest_run WHERE id = 'resumed-checkpoint-run'
                """
            ).fetchone()
            link_count = int(connection.execute("SELECT count(*) FROM fact_play_event_history_link").fetchone()[0])

        self.assertEqual(2, first["eligible_unlinked_history_before"])
        self.assertEqual(0, first["eligible_unlinked_history_after"])
        self.assertEqual(0, second["eligible_unlinked_history_before"])
        self.assertEqual(2, link_count)
        self.assertEqual((1, 1, 0, 0), run_row[:4])
        self.assertIsNotNone(run_row[4])
        self.assertEqual(0, audit_eligible_unlinked_history_rows()["eligible_unlinked_history_count"])

    def test_orphan_invariant_excludes_episode_and_unidentifiable_history(self) -> None:
        insert_ingest_run(
            run_id="non-music-history-run",
            source_type="export",
            started_at="2026-04-17T19:00:00Z",
            source_ref="test",
        )
        for row_key, payload in (
            ("episode", json.dumps({"spotify_episode_uri": "spotify:episode:episode-1"})),
            ("unidentifiable", "{}"),
        ):
            insert_raw_spotify_history_observation(
                ingest_run_id="non-music-history-run",
                source_row_key=row_key,
                played_at="2026-04-17T19:10:00Z",
                ms_played=240000,
                spotify_track_uri=None,
                spotify_track_id=None,
                track_name_raw="Podcast title" if row_key == "episode" else None,
                artist_name_raw=None,
                album_name_raw=None,
                spotify_album_id=None,
                spotify_artist_ids_json=None,
                reason_start=None,
                reason_end=None,
                skipped=0,
                shuffle=0,
                offline=0,
                platform="test",
                conn_country="US",
                private_session=0,
                raw_payload_json=payload,
            )

        self.assertEqual(0, audit_eligible_unlinked_history_rows()["eligible_unlinked_history_count"])
        summary = reconcile_fact_play_events_for_ingest_run(
            source_type="export", run_id="non-music-history-run"
        )
        self.assertEqual(0, summary["facts_touched_count"])

    def test_reconcile_creates_matched_fact_with_history_timing_precedence(self) -> None:
        run_recent = "run-recent-1"
        run_history = "run-history-1"
        insert_ingest_run(
            run_id=run_recent,
            source_type="spotify_recent",
            started_at="2026-04-17T19:00:00Z",
            source_ref="test",
        )
        insert_ingest_run(
            run_id=run_history,
            source_type="export",
            started_at="2026-04-17T19:00:00Z",
            source_ref="test",
        )

        insert_raw_spotify_recent_observation(
            ingest_run_id=run_recent,
            source_row_key="recent-row-1",
            source_event_id=None,
            played_at="2026-04-17T19:52:08.858000Z",
            ms_played_estimate=200000,
            ms_played_method="api_chronology",
            ms_played_confidence="high",
            ms_played_fallback_class=None,
            spotify_track_uri="spotify:track:track-1",
            spotify_track_id="track-1",
            track_name_raw="Song A",
            artist_name_raw="Artist A",
            album_name_raw="Album A",
            spotify_album_id="album-1",
            spotify_artist_ids_json=json.dumps(["artist-1"]),
            track_duration_ms=240000,
            context_type="playlist",
            context_uri="spotify:playlist:abc",
            raw_payload_json="{}",
        )
        insert_raw_spotify_history_observation(
            ingest_run_id=run_history,
            source_row_key="history-row-1",
            played_at="2026-04-17T19:52:01Z",
            ms_played=205600,
            spotify_track_uri="spotify:track:track-1",
            spotify_track_id="track-1",
            track_name_raw="Song A",
            artist_name_raw="Artist A",
            album_name_raw="Album A",
            spotify_album_id="album-1",
            spotify_artist_ids_json=json.dumps(["artist-1"]),
            reason_start="trackdone",
            reason_end="logout",
            skipped=0,
            shuffle=1,
            offline=0,
            platform="not_applicable",
            conn_country="US",
            private_session=0,
            raw_payload_json="{}",
        )

        summary_recent = reconcile_fact_play_events_for_ingest_run(
            source_type="spotify_recent",
            run_id=run_recent,
        )
        summary_history = reconcile_fact_play_events_for_ingest_run(
            source_type="export",
            run_id=run_history,
        )

        self.assertGreaterEqual(summary_recent["facts_touched_count"], 1)
        self.assertGreaterEqual(summary_recent["matched_pairs_count"], 1)
        self.assertGreaterEqual(summary_history["matched_pairs_count"], 0)

        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                """
                SELECT
                  f.timing_source,
                  f.matched_state,
                  f.canonical_ended_at,
                  f.canonical_ms_played,
                  f.canonical_reason_end,
                  f.canonical_context_type,
                  f.canonical_context_uri
                FROM fact_play_event f
                JOIN fact_play_event_recent_link rl
                  ON rl.fact_play_event_id = f.id
                JOIN fact_play_event_history_link hl
                  ON hl.fact_play_event_id = f.id
                LIMIT 1
                """
            ).fetchone()

        assert row is not None
        self.assertEqual("history", row[0])
        self.assertEqual("matched", row[1])
        self.assertEqual("2026-04-17T19:52:01Z", row[2])
        self.assertEqual(205600, row[3])
        self.assertEqual("logout", row[4])
        self.assertEqual("playlist", row[5])
        self.assertEqual("spotify:playlist:abc", row[6])

    def test_reconcile_creates_source_track_mapping_from_recent_spotify_id(self) -> None:
        run_recent = "run-recent-source-identity"
        insert_ingest_run(
            run_id=run_recent,
            source_type="spotify_recent",
            started_at="2026-04-17T19:00:00Z",
            source_ref="test",
        )
        insert_raw_spotify_recent_observation(
            ingest_run_id=run_recent,
            source_row_key="recent-source-identity-row",
            source_event_id=None,
            played_at="2026-04-17T19:52:08Z",
            ms_played_estimate=180000,
            ms_played_method="default_guess",
            ms_played_confidence="low",
            ms_played_fallback_class="fallback_likely_complete",
            spotify_track_uri="spotify:track:track-source-identity",
            spotify_track_id="track-source-identity",
            track_name_raw="Source Identity Song",
            artist_name_raw="Artist A",
            album_name_raw="Album A",
            spotify_album_id="album-1",
            spotify_artist_ids_json=json.dumps(["artist-1"]),
            track_duration_ms=240000,
            context_type=None,
            context_uri=None,
            raw_payload_json=json.dumps({"id": "track-source-identity"}),
        )

        summary_recent = reconcile_fact_play_events_for_ingest_run(
            source_type="spotify_recent",
            run_id=run_recent,
        )

        self.assertEqual(1, summary_recent["facts_touched_count"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                """
                SELECT
                  st.source_name,
                  st.external_id,
                  st.external_uri,
                  st.source_name_raw,
                  rt.primary_name,
                  rt.duration_ms,
                  stm.match_method,
                  stm.confidence,
                  stm.status
                FROM source_track st
                JOIN source_track_map stm
                  ON stm.source_track_id = st.id
                JOIN release_track rt
                  ON rt.id = stm.release_track_id
                WHERE st.source_name = 'spotify'
                  AND st.external_id = 'track-source-identity'
                LIMIT 1
                """
            ).fetchone()

        assert row is not None
        self.assertEqual("spotify", row[0])
        self.assertEqual("track-source-identity", row[1])
        self.assertEqual("spotify:track:track-source-identity", row[2])
        self.assertEqual("Source Identity Song", row[3])
        self.assertEqual("Source Identity Song", row[4])
        self.assertEqual(240000, row[5])
        self.assertEqual("spotify_provider_identity", row[6])
        self.assertEqual(1.0, row[7])
        self.assertEqual("accepted", row[8])

    def test_reconcile_creates_local_release_track_mapping_without_spotify_id(self) -> None:
        run_recent = "run-recent-local-identity"
        insert_ingest_run(
            run_id=run_recent,
            source_type="spotify_recent",
            started_at="2026-04-17T19:00:00Z",
            source_ref="test",
        )
        insert_raw_spotify_recent_observation(
            ingest_run_id=run_recent,
            source_row_key="recent-local-identity-row",
            source_event_id=None,
            played_at="2026-04-17T19:53:08Z",
            ms_played_estimate=180000,
            ms_played_method="default_guess",
            ms_played_confidence="low",
            ms_played_fallback_class="fallback_likely_complete",
            spotify_track_uri=None,
            spotify_track_id=None,
            track_name_raw="Local Identity Song",
            artist_name_raw="Artist B",
            album_name_raw="Album B",
            spotify_album_id=None,
            spotify_artist_ids_json=None,
            track_duration_ms=220000,
            context_type=None,
            context_uri=None,
            raw_payload_json=json.dumps({"name": "Local Identity Song"}),
        )

        summary_recent = reconcile_fact_play_events_for_ingest_run(
            source_type="spotify_recent",
            run_id=run_recent,
        )

        self.assertEqual(1, summary_recent["facts_touched_count"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                """
                SELECT
                  st.source_name,
                  st.source_name_raw,
                  rt.primary_name,
                  rt.duration_ms,
                  stm.match_method,
                  stm.confidence,
                  stm.status
                FROM source_track st
                JOIN source_track_map stm
                  ON stm.source_track_id = st.id
                JOIN release_track rt
                  ON rt.id = stm.release_track_id
                WHERE st.source_name = 'history_raw'
                LIMIT 1
                """
            ).fetchone()

        assert row is not None
        self.assertEqual("history_raw", row[0])
        self.assertEqual("Local Identity Song", row[1])
        self.assertEqual("Local Identity Song", row[2])
        self.assertEqual(220000, row[3])
        self.assertEqual("history_raw_text", row[4])
        self.assertEqual(0.75, row[5])
        self.assertEqual("accepted", row[6])

        enriched = enrich_track_rows_with_release_metadata([
            {
                "track_id": None,
                "track_name": "Local Identity Song",
                "artist_name": "Artist B",
                "album_name": "Album B",
            }
        ])
        self.assertIsInstance(enriched[0].get("release_track_id"), int)
        self.assertEqual("Local Identity Song", enriched[0].get("release_track_name"))

    def test_fact_identity_backfill_repairs_existing_fact_rows(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute(
                """
                INSERT INTO fact_play_event (
                  canonical_ended_at,
                  spotify_track_id,
                  spotify_track_uri,
                  track_name_canonical,
                  artist_name_canonical,
                  album_name_canonical,
                  timing_source,
                  matched_state
                )
                VALUES (?, ?, ?, ?, ?, ?, 'recent_fallback', 'recent_only')
                """,
                (
                    "2026-04-17T19:54:08Z",
                    "backfill-track-1",
                    "spotify:track:backfill-track-1",
                    "Backfill Song",
                    "Artist C",
                    "Album C",
                ),
            )
            connection.commit()

        summary = backfill_fact_play_event_release_track_identity()

        self.assertGreaterEqual(summary["rows_scanned"], 1)
        self.assertEqual(1, summary["rows_with_identity"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                """
                SELECT rt.primary_name, stm.status
                FROM source_track st
                JOIN source_track_map stm
                  ON stm.source_track_id = st.id
                JOIN release_track rt
                  ON rt.id = stm.release_track_id
                WHERE st.source_name = 'spotify'
                  AND st.external_id = 'backfill-track-1'
                LIMIT 1
                """
            ).fetchone()

        assert row is not None
        self.assertEqual("Backfill Song", row[0])
        self.assertEqual("accepted", row[1])

    def test_reconcile_skips_spotify_episode_history_rows(self) -> None:
        run_history = "run-history-episode"
        insert_ingest_run(
            run_id=run_history,
            source_type="export",
            started_at="2026-04-17T19:00:00Z",
            source_ref="test",
        )
        insert_raw_spotify_history_observation(
            ingest_run_id=run_history,
            source_row_key="history-episode-row",
            played_at="2026-04-17T19:58:01Z",
            ms_played=40149,
            spotify_track_uri=None,
            spotify_track_id=None,
            track_name_raw=None,
            artist_name_raw=None,
            album_name_raw=None,
            spotify_album_id=None,
            spotify_artist_ids_json=None,
            reason_start="clickrow",
            reason_end="endplay",
            skipped=1,
            shuffle=0,
            offline=0,
            platform="ios",
            conn_country="US",
            private_session=0,
            raw_payload_json=json.dumps(
                {
                    "spotify_episode_uri": "spotify:episode:episode-1",
                    "episode_name": "Episode A",
                    "episode_show_name": "Show A",
                    "spotify_track_uri": None,
                    "master_metadata_track_name": None,
                }
            ),
        )

        summary = reconcile_fact_play_events_for_ingest_run(
            source_type="export",
            run_id=run_history,
        )

        self.assertEqual(1, summary["skipped_history_episode_count"])
        self.assertEqual(0, summary["facts_touched_count"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            raw_count = connection.execute("SELECT count(*) FROM raw_spotify_history").fetchone()[0]
            link_count = connection.execute("SELECT count(*) FROM fact_play_event_history_link").fetchone()[0]
            fact_count = connection.execute("SELECT count(*) FROM fact_play_event").fetchone()[0]
        self.assertEqual(1, raw_count)
        self.assertEqual(0, link_count)
        self.assertEqual(0, fact_count)

    def test_reconcile_skips_unidentifiable_history_rows(self) -> None:
        run_history = "run-history-unidentifiable"
        insert_ingest_run(
            run_id=run_history,
            source_type="export",
            started_at="2026-04-17T19:00:00Z",
            source_ref="test",
        )
        insert_raw_spotify_history_observation(
            ingest_run_id=run_history,
            source_row_key="history-unidentifiable-row",
            played_at="2026-04-17T20:00:01Z",
            ms_played=17632,
            raw_payload_json=json.dumps(
                {
                    "spotify_episode_uri": None,
                    "spotify_track_uri": None,
                    "master_metadata_track_name": None,
                    "master_metadata_album_artist_name": None,
                }
            ),
        )

        summary = reconcile_fact_play_events_for_ingest_run(
            source_type="export",
            run_id=run_history,
        )

        self.assertEqual(1, summary["skipped_history_unidentifiable_count"])
        self.assertEqual(0, summary["facts_touched_count"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            raw_count = connection.execute("SELECT count(*) FROM raw_spotify_history").fetchone()[0]
            link_count = connection.execute("SELECT count(*) FROM fact_play_event_history_link").fetchone()[0]
            fact_count = connection.execute("SELECT count(*) FROM fact_play_event").fetchone()[0]
        self.assertEqual(1, raw_count)
        self.assertEqual(0, link_count)
        self.assertEqual(0, fact_count)

    def test_delete_projected_podcast_episode_facts_preserves_raw_history(self) -> None:
        run_history = "run-history-episode-cleanup"
        insert_ingest_run(
            run_id=run_history,
            source_type="export",
            started_at="2026-04-17T19:00:00Z",
            source_ref="test",
        )
        inserted = insert_raw_spotify_history_observation(
            ingest_run_id=run_history,
            source_row_key="history-episode-cleanup-row",
            played_at="2026-04-17T19:59:01Z",
            ms_played=5000,
            raw_payload_json=json.dumps(
                {
                    "spotify_episode_uri": "spotify:episode:episode-cleanup",
                    "episode_name": "Episode Cleanup",
                    "episode_show_name": "Show Cleanup",
                }
            ),
        )
        raw_history_id = int(inserted["row_id"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            cursor = connection.execute(
                """
                INSERT INTO fact_play_event (
                  canonical_ended_at,
                  canonical_ms_played,
                  timing_source,
                  matched_state
                )
                VALUES ('2026-04-17T19:59:01Z', 5000, 'history', 'history_only')
                """
            )
            fact_id = int(cursor.lastrowid)
            connection.execute(
                """
                INSERT INTO fact_play_event_history_link (
                  fact_play_event_id,
                  raw_spotify_history_id,
                  is_primary
                )
                VALUES (?, ?, 1)
                """,
                (fact_id, raw_history_id),
            )
            connection.commit()

        summary = delete_projected_podcast_episode_facts()

        self.assertEqual(1, summary["episode_fact_rows_found"])
        self.assertEqual(1, summary["history_links_deleted"])
        self.assertEqual(1, summary["fact_rows_deleted"])
        self.assertEqual(1, summary["raw_history_rows_preserved"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            raw_count = connection.execute("SELECT count(*) FROM raw_spotify_history").fetchone()[0]
            link_count = connection.execute("SELECT count(*) FROM fact_play_event_history_link").fetchone()[0]
            fact_count = connection.execute("SELECT count(*) FROM fact_play_event").fetchone()[0]
        self.assertEqual(1, raw_count)
        self.assertEqual(0, link_count)
        self.assertEqual(0, fact_count)

    def test_delete_projected_unidentifiable_history_facts_preserves_raw_history(self) -> None:
        run_history = "run-history-unidentifiable-cleanup"
        insert_ingest_run(
            run_id=run_history,
            source_type="export",
            started_at="2026-04-17T19:00:00Z",
            source_ref="test",
        )
        inserted = insert_raw_spotify_history_observation(
            ingest_run_id=run_history,
            source_row_key="history-unidentifiable-cleanup-row",
            played_at="2026-04-17T20:01:01Z",
            ms_played=5000,
            raw_payload_json=json.dumps({"spotify_track_uri": None, "spotify_episode_uri": None}),
        )
        raw_history_id = int(inserted["row_id"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            cursor = connection.execute(
                """
                INSERT INTO fact_play_event (
                  canonical_ended_at,
                  canonical_ms_played,
                  timing_source,
                  matched_state
                )
                VALUES ('2026-04-17T20:01:01Z', 5000, 'history', 'history_only')
                """
            )
            fact_id = int(cursor.lastrowid)
            connection.execute(
                """
                INSERT INTO fact_play_event_history_link (
                  fact_play_event_id,
                  raw_spotify_history_id,
                  is_primary
                )
                VALUES (?, ?, 1)
                """,
                (fact_id, raw_history_id),
            )
            connection.commit()

        summary = delete_projected_unidentifiable_history_facts()

        self.assertEqual(1, summary["unidentifiable_fact_rows_found"])
        self.assertEqual(1, summary["history_links_deleted"])
        self.assertEqual(1, summary["fact_rows_deleted"])
        self.assertEqual(1, summary["raw_history_rows_preserved"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            raw_count = connection.execute("SELECT count(*) FROM raw_spotify_history").fetchone()[0]
            link_count = connection.execute("SELECT count(*) FROM fact_play_event_history_link").fetchone()[0]
            fact_count = connection.execute("SELECT count(*) FROM fact_play_event").fetchone()[0]
        self.assertEqual(1, raw_count)
        self.assertEqual(0, link_count)
        self.assertEqual(0, fact_count)

    def test_reconcile_merges_recent_with_listenlab_player_fact(self) -> None:
        run_player = "run-player-1"
        run_recent = "run-recent-1"
        insert_ingest_run(
            run_id=run_player,
            source_type="listenlab_player",
            started_at="2026-04-17T19:00:00Z",
            source_ref="test",
        )
        insert_ingest_run(
            run_id=run_recent,
            source_type="spotify_recent",
            started_at="2026-04-17T19:00:00Z",
            source_ref="test",
        )

        insert_listenlab_player_play(
            ingest_run_id=run_player,
            source_row_key="player-row-1",
            source_event_id="player-event-1",
            user_id="user-1",
            played_at="2026-04-17T19:52:00Z",
            ms_played=62000,
            ms_played_confidence="paused",
            spotify_track_uri="spotify:track:track-1",
            spotify_track_id="track-1",
            track_name_raw="Song A",
            artist_name_raw="Artist A",
            album_name_raw="Album A",
            spotify_album_id="album-1",
            spotify_artist_ids_json=json.dumps(["artist-1"]),
            track_duration_ms=240000,
            raw_payload_json="{}",
        )
        summary_player = reconcile_fact_play_events_for_ingest_run(
            source_type="listenlab_player",
            run_id=run_player,
        )
        insert_raw_spotify_recent_observation(
            ingest_run_id=run_recent,
            source_row_key="recent-row-1",
            source_event_id=None,
            played_at="2026-04-17T19:52:08Z",
            ms_played_estimate=240000,
            ms_played_method="default_guess",
            ms_played_confidence="low",
            ms_played_fallback_class="fallback_likely_complete",
            spotify_track_uri="spotify:track:track-1",
            spotify_track_id="track-1",
            track_name_raw="Song A",
            artist_name_raw="Artist A",
            album_name_raw="Album A",
            spotify_album_id="album-1",
            spotify_artist_ids_json=json.dumps(["artist-1"]),
            track_duration_ms=240000,
            context_type=None,
            context_uri=None,
            raw_payload_json="{}",
        )
        summary_recent = reconcile_fact_play_events_for_ingest_run(
            source_type="spotify_recent",
            run_id=run_recent,
        )

        self.assertGreaterEqual(summary_player["facts_touched_count"], 1)
        self.assertGreaterEqual(summary_recent["matched_player_pairs_count"], 1)

        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                """
                SELECT
                  f.timing_source,
                  f.matched_state,
                  f.spotify_track_id,
                  f.canonical_ms_played
                FROM fact_play_event f
                JOIN fact_play_event_recent_link rl
                  ON rl.fact_play_event_id = f.id
                JOIN fact_play_event_player_link pl
                  ON pl.fact_play_event_id = f.id
                LIMIT 1
                """
            ).fetchone()

        assert row is not None
        self.assertEqual("recent_fallback", row[0])
        self.assertEqual("matched", row[1])
        self.assertEqual("track-1", row[2])
        self.assertEqual(240000, row[3])

    def test_player_listen_counts_only_after_crossing_sixty_five_percent(self) -> None:
        run_id = "run-player-threshold"
        insert_ingest_run(
            run_id=run_id,
            source_type="listenlab_player",
            started_at="2026-06-20T12:00:00Z",
            source_ref="test",
        )
        inserted = insert_listenlab_player_play(
            ingest_run_id=run_id,
            source_row_key="player-threshold-row",
            source_event_id="player-threshold-event",
            user_id="user-1",
            played_at="2026-06-20T12:00:00Z",
            ms_played=0,
            ms_played_confidence="in_progress",
            spotify_track_uri="spotify:track:threshold-track",
            spotify_track_id="threshold-track",
            track_name_raw="Threshold Song",
            artist_name_raw="Threshold Artist",
            album_name_raw="Threshold Album",
            spotify_album_id="threshold-album",
            spotify_artist_ids_json="[]",
            track_duration_ms=100_000,
            raw_payload_json="{}",
        )
        reconcile_fact_play_events_for_ingest_run(source_type="listenlab_player", run_id=run_id)

        with closing(sqlite3.connect(self.db_path)) as connection:
            before = connection.execute(
                "SELECT play_count FROM source_track_play_count_cache WHERE spotify_track_id = 'threshold-track'"
            ).fetchone()
        self.assertIsNone(before)

        below = update_listenlab_player_play_progress(
            row_id=int(inserted["row_id"]),
            user_id="user-1",
            ms_played=64_999,
            ms_played_confidence="in_progress",
        )
        self.assertFalse(below["crossed_listen_threshold"])

        qualified = update_listenlab_player_play_progress(
            row_id=int(inserted["row_id"]),
            user_id="user-1",
            ms_played=65_000,
            ms_played_confidence="listened",
        )
        self.assertTrue(qualified["crossed_listen_threshold"])
        refresh_source_track_play_count_cache()
        with closing(sqlite3.connect(self.db_path)) as connection:
            after = connection.execute(
                "SELECT play_count, last_played_at FROM source_track_play_count_cache WHERE spotify_track_id = 'threshold-track'"
            ).fetchone()
        self.assertEqual(1, after[0])
        self.assertEqual("2026-06-20T12:01:05Z", after[1])

    def test_play_count_combines_interrupted_same_track_fragments(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, fetched_at
                ) VALUES ('fragment-track', 100000, '2026-06-20T12:00:00Z')
                """
            )
            for index, (started_at, ended_at, played_ms) in enumerate(
                (
                    ("2026-06-20T12:00:00Z", "2026-06-20T12:00:30Z", 30000),
                    ("2026-06-20T12:05:00Z", "2026-06-20T12:05:30Z", 30000),
                    ("2026-06-20T12:10:00Z", "2026-06-20T12:10:30Z", 30000),
                )
            ):
                connection.execute(
                    """
                    INSERT INTO fact_play_event (
                      canonical_started_at,
                      canonical_ended_at,
                      canonical_ms_played,
                      ms_played_confidence,
                      spotify_track_id,
                      timing_source,
                      matched_state
                    ) VALUES (?, ?, ?, 'high', 'fragment-track', 'history', 'history_only')
                    """,
                    (started_at, ended_at, played_ms),
                )
            refresh_source_track_play_count_cache(connection)
            combined = connection.execute(
                """
                SELECT play_count, last_played_at
                FROM source_track_play_count_cache
                WHERE spotify_track_id = 'fragment-track'
                """
            ).fetchone()
            self.assertEqual((1, "2026-06-20T12:10:30Z"), combined)

            connection.execute(
                """
                INSERT INTO fact_play_event (
                  canonical_started_at,
                  canonical_ended_at,
                  canonical_ms_played,
                  ms_played_confidence,
                  spotify_track_id,
                  timing_source,
                  matched_state
                ) VALUES (
                  '2026-06-20T12:15:00Z',
                  '2026-06-20T12:16:20Z',
                  80000,
                  'high',
                  'fragment-track',
                  'history',
                  'history_only'
                )
                """
            )
            refresh_source_track_play_count_cache(connection)
            over_full_duration = connection.execute(
                """
                SELECT play_count
                FROM source_track_play_count_cache
                WHERE spotify_track_id = 'fragment-track'
                """
            ).fetchone()
            self.assertEqual((2,), over_full_duration)

    def test_play_count_does_not_combine_fragments_across_another_track(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.executemany(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, fetched_at
                ) VALUES (?, 100000, '2026-06-20T12:00:00Z')
                """,
                (("split-track",), ("intervening-track",)),
            )
            connection.executemany(
                """
                INSERT INTO fact_play_event (
                  canonical_started_at,
                  canonical_ended_at,
                  canonical_ms_played,
                  ms_played_confidence,
                  spotify_track_id,
                  timing_source,
                  matched_state
                ) VALUES (?, ?, ?, 'high', ?, 'history', 'history_only')
                """,
                (
                    ("2026-06-20T12:00:00Z", "2026-06-20T12:00:40Z", 40000, "split-track"),
                    ("2026-06-20T12:01:00Z", "2026-06-20T12:02:10Z", 70000, "intervening-track"),
                    ("2026-06-20T12:03:00Z", "2026-06-20T12:03:40Z", 40000, "split-track"),
                ),
            )
            refresh_source_track_play_count_cache(connection)
            rows = dict(
                connection.execute(
                    "SELECT spotify_track_id, play_count FROM source_track_play_count_cache"
                ).fetchall()
            )
            self.assertNotIn("split-track", rows)
            self.assertEqual(1, rows["intervening-track"])


if __name__ == "__main__":
    unittest.main()
