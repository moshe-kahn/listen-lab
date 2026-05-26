from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from typing import Any, Callable
from unittest.mock import patch

from backend.app.db import apply_pending_migrations, ensure_sqlite_db, insert_raw_play_event
from backend.app.main import app
from fastapi import HTTPException
from fastapi.testclient import TestClient
from backend.app.spotify_catalog_backfill import (
    append_resolution_evidence_candidate_tracklists_from_report,
    append_resolution_evidence_sibling_tracks_from_report,
    enqueue_spotify_catalog_backfill_items,
    dry_run_release_album_merge,
    get_identity_readiness_track_metadata_priority_comparison,
    get_spotify_track_metadata_priority_debug,
    inspect_spotify_album_metadata_display_gaps,
    inspect_spotify_catalog_queue_resolution_evidence,
    inspect_spotify_nested_metadata_integrity,
    inspect_source_release_album_display_gaps,
    list_spotify_catalog_backfill_queue,
    plan_source_release_album_display_enrichment,
    preview_release_album_merge,
    query_release_track_duration_conflicts,
    repair_release_track_durations_from_spotify_catalog,
    repair_incomplete_done_resolution_tracklist_queue_rows,
    repair_spotify_album_basic_metadata_from_track_payloads,
    repair_spotify_catalog_backfill_queue_statuses,
    run_spotify_resolution_evidence_album_tracklist_worker,
    run_spotify_resolution_evidence_track_metadata_worker,
    run_source_release_album_display_enrichment_worker,
    search_album_catalog_duplicate_by_name_identities,
    search_album_catalog_duplicate_spotify_identities,
    search_album_catalog_lookup,
    search_track_catalog_duplicate_spotify_identities,
    search_track_catalog_lookup,
    search_track_mapping_lineage,
    run_spotify_catalog_backfill,
    _upsert_track_catalog,
)
from backend.scripts.inspect_spotify_catalog_queue import (
    build_album_display_diagnostic_summary,
    build_queue_snapshot_export,
    build_summary_only_report,
    build_unknown_pending_queue_items_report,
    run_source_release_album_display_enrichment_loop,
    write_queue_snapshot_export,
)


def _track_payload(track_id: str, album_id: str) -> dict[str, Any]:
    return {
        "id": track_id,
        "name": f"Track {track_id}",
        "duration_ms": 123000,
        "explicit": False,
        "disc_number": 1,
        "track_number": 1,
        "album": {"id": album_id},
        "artists": [{"id": "artist-1", "name": "Artist 1"}],
        "external_ids": {"isrc": f"ISRC{track_id}"},
    }


def _album_payload(album_id: str) -> dict[str, Any]:
    return {
        "id": album_id,
        "name": f"Album {album_id}",
        "album_type": "album",
        "release_date": "2024-01-01",
        "release_date_precision": "day",
        "total_tracks": 2,
        "artists": [{"id": "artist-1", "name": "Artist 1"}],
        "images": [{"url": "https://image.test/1.jpg"}],
        "external_ids": {"upc": f"UPC{album_id}"},
    }


class SpotifyCatalogBackfillTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp_dir.name) / "spotify_catalog_backfill.sqlite3"
        os.environ["SQLITE_DB_PATH"] = str(self.db_path)
        ensure_sqlite_db()
        apply_pending_migrations()

    def tearDown(self) -> None:
        self._tmp_dir.cleanup()

    def _seed_source_tracks(self, track_ids: list[str]) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            for track_id in track_ids:
                connection.execute(
                    """
                    INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("spotify", track_id, f"spotify:track:{track_id}", f"Track {track_id}", "{}"),
                )
            connection.commit()

    def _seed_accepted_source_track(self, spotify_track_id: str, *, name: str | None = None) -> int:
        track_name = name or f"Track {spotify_track_id}"
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    (track_name, track_name.lower()),
                ).lastrowid
            )
            source_track_id = int(
                connection.execute(
                    """
                    INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("spotify", spotify_track_id, f"spotify:track:{spotify_track_id}", track_name, "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_id, release_track_id),
            )
            connection.commit()
        return release_track_id

    @staticmethod
    def _analysis_track_map_digest(connection: sqlite3.Connection) -> tuple[int, str]:
        rows = connection.execute(
            """
            SELECT
              release_track_id,
              analysis_track_id,
              match_method,
              confidence,
              status,
              is_user_confirmed,
              explanation
            FROM analysis_track_map
            ORDER BY release_track_id, analysis_track_id
            """
        ).fetchall()
        encoded = json.dumps([tuple(row) for row in rows], ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        return len(rows), hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _identity_album_digest(connection: sqlite3.Connection) -> str:
        payload = {}
        for table in ("release_album", "source_album_map", "album_artist", "album_track", "analysis_track_map"):
            rows = connection.execute(f"SELECT * FROM {table} ORDER BY id").fetchall()
            payload[table] = [tuple(row) for row in rows]
        encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def test_migration_creates_catalog_tables(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            rows = connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                AND name IN (
                  'spotify_track_catalog',
                  'spotify_album_catalog',
                  'spotify_album_track',
                  'spotify_catalog_backfill_run'
                )
                ORDER BY name
                """
            ).fetchall()
        self.assertEqual(
            [
                ("spotify_album_catalog",),
                ("spotify_album_track",),
                ("spotify_catalog_backfill_run",),
                ("spotify_track_catalog",),
            ],
            rows,
        )

    def test_track_album_album_tracks_upsert(self) -> None:
        self._seed_source_tracks(["t1", "t2"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            self.assertEqual("token", access_token)
            if url.endswith("/v1/tracks"):
                ids = str(params.get("ids") or "").split(",")
                self.assertLessEqual(len(ids), 50)
                self.assertEqual("US", params.get("market"))
                self.assertTrue(params.get("ids"))
                return 200, {}, {"tracks": [_track_payload(track_id, f"a{track_id}") for track_id in ids]}, None
            if url.endswith("/v1/albums"):
                ids = str(params.get("ids") or "").split(",")
                return 200, {}, {"albums": [_album_payload(album_id) for album_id in ids]}, None
            if "/v1/albums/" in url and url.endswith("/tracks"):
                album_id = url.split("/v1/albums/")[1].split("/tracks")[0]
                return 200, {}, {"items": [_track_payload(f"{album_id}-x", album_id), _track_payload(f"{album_id}-y", album_id)], "next": None}, None
            raise AssertionError(f"Unexpected URL {url}")

        result = run_spotify_catalog_backfill(
            access_token="token",
            limit=200,
            offset=0,
            market="US",
            include_albums=True,
            request_delay_seconds=0.20,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(2, result["tracks_upserted"])
        self.assertEqual(2, result["albums_fetched"])
        self.assertEqual(4, result["album_tracks_upserted"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            track_row = connection.execute(
                "SELECT duration_ms, album_id, market, last_status, last_error FROM spotify_track_catalog WHERE spotify_track_id = ?",
                ("t1",),
            ).fetchone()
            album_row = connection.execute(
                "SELECT name, album_type, market, last_status FROM spotify_album_catalog WHERE spotify_album_id = ?",
                ("at1",),
            ).fetchone()
            album_track_count = int(connection.execute("SELECT count(*) FROM spotify_album_track").fetchone()[0])
        self.assertEqual((123000, "at1", "US", "ok", None), track_row)
        self.assertEqual(("Album at1", "album", "US", "ok"), album_row)
        self.assertEqual(4, album_track_count)

    def test_upsert_track_catalog_populates_missing_release_track_duration(self) -> None:
        release_track_id = self._seed_accepted_source_track("track-duration-sync", name="Duration Sync")

        _upsert_track_catalog(
            track=_track_payload("track-duration-sync", "album-duration-sync"),
            market="US",
            fetched_at="2026-05-26T12:00:00Z",
            last_status="ok",
            last_error=None,
        )

        with closing(sqlite3.connect(self.db_path)) as connection:
            duration_ms, duration_source, duration_confidence = connection.execute(
                "SELECT duration_ms, duration_source, duration_confidence FROM release_track WHERE id = ?",
                (release_track_id,),
            ).fetchone()

        self.assertEqual(123000, duration_ms)
        self.assertEqual("spotify_track_catalog", duration_source)
        self.assertEqual("catalog_agrees", duration_confidence)

    def test_upsert_track_catalog_preserves_existing_release_track_duration(self) -> None:
        release_track_id = self._seed_accepted_source_track("track-duration-preserve", name="Duration Preserve")
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                "UPDATE release_track SET duration_ms = ? WHERE id = ?",
                (122000, release_track_id),
            )
            connection.commit()

        _upsert_track_catalog(
            track=_track_payload("track-duration-preserve", "album-duration-preserve"),
            market="US",
            fetched_at="2026-05-26T12:00:00Z",
            last_status="ok",
            last_error=None,
        )

        with closing(sqlite3.connect(self.db_path)) as connection:
            duration_ms = connection.execute(
                "SELECT duration_ms FROM release_track WHERE id = ?",
                (release_track_id,),
            ).fetchone()[0]

        self.assertEqual(122000, duration_ms)

    def test_repair_release_track_durations_from_spotify_catalog_updates_close_matches(self) -> None:
        first_release_track_id = self._seed_accepted_source_track("repair-duration-a", name="Repair Duration")
        second_release_track_id = self._seed_accepted_source_track("repair-duration-b", name="Repair Duration B")
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                "UPDATE source_track_map SET release_track_id = ? WHERE release_track_id = ?",
                (first_release_track_id, second_release_track_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (spotify_track_id, name, duration_ms, fetched_at, last_status)
                VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)
                """,
                (
                    "repair-duration-a",
                    "Repair Duration A",
                    180000,
                    "2026-05-26T12:00:00Z",
                    "ok",
                    "repair-duration-b",
                    "Repair Duration B",
                    181000,
                    "2026-05-26T12:00:00Z",
                    "ok",
                ),
            )
            connection.commit()

        dry_run = repair_release_track_durations_from_spotify_catalog(apply=False)
        self.assertEqual(1, dry_run["candidate_count"])
        self.assertEqual(0, dry_run["updated_count"])

        result = repair_release_track_durations_from_spotify_catalog(apply=True)

        with closing(sqlite3.connect(self.db_path)) as connection:
            duration_ms, duration_source, duration_confidence = connection.execute(
                "SELECT duration_ms, duration_source, duration_confidence FROM release_track WHERE id = ?",
                (first_release_track_id,),
            ).fetchone()
        self.assertEqual(1, result["updated_count"])
        self.assertEqual(180000, duration_ms)
        self.assertEqual("spotify_track_catalog", duration_source)
        self.assertEqual("catalog_agrees", duration_confidence)

    def test_repair_release_track_durations_from_spotify_catalog_uses_longest_accepted_conflict(self) -> None:
        first_release_track_id = self._seed_accepted_source_track("repair-conflict-a", name="Repair Conflict")
        second_release_track_id = self._seed_accepted_source_track("repair-conflict-b", name="Repair Conflict B")
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                "UPDATE source_track_map SET release_track_id = ? WHERE release_track_id = ?",
                (first_release_track_id, second_release_track_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (spotify_track_id, name, duration_ms, fetched_at, last_status)
                VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)
                """,
                (
                    "repair-conflict-a",
                    "Repair Conflict A",
                    180000,
                    "2026-05-26T12:00:00Z",
                    "ok",
                    "repair-conflict-b",
                    "Repair Conflict B",
                    185000,
                    "2026-05-26T12:00:00Z",
                    "ok",
                ),
            )
            connection.commit()

        result = repair_release_track_durations_from_spotify_catalog(apply=True)

        with closing(sqlite3.connect(self.db_path)) as connection:
            duration_ms, duration_source, duration_confidence, evidence_json = connection.execute(
                "SELECT duration_ms, duration_source, duration_confidence, duration_evidence_json FROM release_track WHERE id = ?",
                (first_release_track_id,),
            ).fetchone()
        self.assertEqual(1, result["updated_count"])
        self.assertEqual(1, result["annotated_uncertain_count"])
        self.assertEqual(0, result["conflict_count"])
        self.assertEqual(1, result["accepted_mapping_conflict_count"])
        self.assertEqual(185000, duration_ms)
        self.assertEqual("accepted_source_catalog_conflict", duration_source)
        self.assertEqual("uncertain_catalog_conflict", duration_confidence)
        self.assertIn("representative", evidence_json)

    def test_repair_release_track_durations_from_spotify_catalog_marks_existing_conflict_uncertain(self) -> None:
        first_release_track_id = self._seed_accepted_source_track("repair-existing-conflict-a", name="Repair Existing Conflict")
        second_release_track_id = self._seed_accepted_source_track("repair-existing-conflict-b", name="Repair Existing Conflict B")
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                "UPDATE source_track_map SET release_track_id = ? WHERE release_track_id = ?",
                (first_release_track_id, second_release_track_id),
            )
            connection.execute(
                "UPDATE release_track SET duration_ms = ? WHERE id = ?",
                (180000, first_release_track_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (spotify_track_id, name, duration_ms, fetched_at, last_status)
                VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)
                """,
                (
                    "repair-existing-conflict-a",
                    "Repair Existing Conflict A",
                    180000,
                    "2026-05-26T12:00:00Z",
                    "ok",
                    "repair-existing-conflict-b",
                    "Repair Existing Conflict B",
                    185000,
                    "2026-05-26T12:00:00Z",
                    "ok",
                ),
            )
            connection.commit()

        result = repair_release_track_durations_from_spotify_catalog(apply=True)

        with closing(sqlite3.connect(self.db_path)) as connection:
            duration_ms, duration_confidence = connection.execute(
                "SELECT duration_ms, duration_confidence FROM release_track WHERE id = ?",
                (first_release_track_id,),
            ).fetchone()
        self.assertEqual(0, result["updated_count"])
        self.assertEqual(1, result["annotated_uncertain_count"])
        self.assertEqual(180000, duration_ms)
        self.assertEqual("uncertain_catalog_conflict", duration_confidence)

    def test_query_release_track_duration_conflicts_returns_spotify_links(self) -> None:
        first_release_track_id = self._seed_accepted_source_track("duration-conflict-a", name="Duration Conflict")
        second_release_track_id = self._seed_accepted_source_track("duration-conflict-b", name="Duration Conflict B")
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                "UPDATE source_track_map SET release_track_id = ? WHERE release_track_id = ?",
                (first_release_track_id, second_release_track_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, explicit, album_id, raw_json, fetched_at, last_status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "duration-conflict-a",
                    "Duration Conflict A",
                    180000,
                    1,
                    "album-conflict-a",
                    json.dumps({"external_ids": {"isrc": "USCONFLICTA"}, "album": {"name": "Album A", "release_date": "2001"}}),
                    "2026-05-26T12:00:00Z",
                    "ok",
                    "duration-conflict-b",
                    "Duration Conflict B",
                    185000,
                    0,
                    "album-conflict-b",
                    json.dumps({"external_ids": {"isrc": "USCONFLICTB"}, "album": {"name": "Album B", "release_date": "2002"}}),
                    "2026-05-26T12:00:00Z",
                    "ok",
                ),
            )
            connection.commit()

        payload = query_release_track_duration_conflicts(limit=10, offset=0)

        self.assertEqual(1, payload["total"])
        item = payload["items"][0]
        self.assertEqual(first_release_track_id, item["release_track_id"])
        self.assertIsNone(item["release_track_duration_ms"])
        self.assertEqual("unknown", item["duration_confidence"])
        self.assertEqual(5000, item["duration_delta_ms"])
        self.assertEqual(2, len(item["source_tracks"]))
        self.assertEqual(
            "https://open.spotify.com/track/duration-conflict-a",
            item["source_tracks"][0]["spotify_url"],
        )
        self.assertFalse(payload["source"]["mutates_identity"])

    def test_omitted_request_delay_defaults_to_two_seconds(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual(2.0, result["initial_request_delay_seconds"])
        self.assertEqual(2.0, result["final_request_delay_seconds"])

    def test_explicit_request_delay_is_respected(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            request_delay_seconds=0.20,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual(0.20, result["initial_request_delay_seconds"])
        self.assertEqual(0.20, result["final_request_delay_seconds"])

    def test_album_tracklist_policy_none_fetches_no_album_tracks(self) -> None:
        self._seed_source_tracks(["t1"])
        album_tracks_calls = 0

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            nonlocal album_tracks_calls
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums"):
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if "/v1/albums/" in url and url.endswith("/tracks"):
                album_tracks_calls += 1
                return 200, {}, {"items": [_track_payload("a1-t1", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            album_tracklist_policy="none",
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, album_tracks_calls)
        self.assertEqual(0, result["album_tracks_upserted"])
        self.assertEqual(1, result["album_tracklists_seen"])
        self.assertEqual(1, result["album_tracklists_skipped_by_policy"])
        self.assertEqual(0, result["album_tracklists_fetched"])

    def test_album_tracklist_policy_priority_only_fetches_queued_albums(self) -> None:
        self._seed_source_tracks(["t1", "t2"])
        enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "album", "spotify_id": "a1", "reason": "test-priority", "priority": 80}]
        )
        fetched_album_track_ids: list[str] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                payloads = {
                    "t1": _track_payload("t1", "a1"),
                    "t2": _track_payload("t2", "a2"),
                }
                return 200, {}, {"tracks": [payloads[track_id] for track_id in ids]}, None
            if url.endswith("/v1/albums"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                return 200, {}, {"albums": [_album_payload(album_id) for album_id in ids]}, None
            if "/v1/albums/" in url and "/tracks" not in url:
                album_id = url.split("/v1/albums/")[1]
                return 200, {}, _album_payload(album_id), None
            if "/v1/albums/" in url and url.endswith("/tracks"):
                album_id = url.split("/v1/albums/")[1].split("/tracks")[0]
                fetched_album_track_ids.append(album_id)
                return 200, {}, {"items": [_track_payload(f"{album_id}-t1", album_id)], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            album_tracklist_policy="priority_only",
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(["a1"], fetched_album_track_ids)
        self.assertEqual(2, result["album_tracklists_seen"])
        self.assertEqual(1, result["album_tracklists_skipped_by_policy"])
        self.assertEqual(1, result["album_tracklists_fetched"])

    def test_album_tracklist_policy_relevant_albums_fetches_high_relevance_album(self) -> None:
        self._seed_source_tracks(["t1"])
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="rel-high-1",
            played_at="2026-04-28T12:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="play-track-1",
            spotify_album_id="a1",
        )
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="rel-high-2",
            played_at="2026-04-28T12:01:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="play-track-2",
            spotify_album_id="a1",
        )
        fetched_album_track_ids: list[str] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums"):
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if "/v1/albums/" in url and url.endswith("/tracks"):
                album_id = url.split("/v1/albums/")[1].split("/tracks")[0]
                fetched_album_track_ids.append(album_id)
                return 200, {}, {"items": [_track_payload("a1-t1", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            album_tracklist_policy="relevant_albums",
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(["a1"], fetched_album_track_ids)
        self.assertEqual(1, result["album_tracklists_fetched"])
        self.assertEqual(0, result["album_tracklists_skipped_by_policy"])

    def test_album_tracklist_policy_relevant_albums_skips_low_relevance_album(self) -> None:
        self._seed_source_tracks(["t1"])
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="rel-low-1",
            played_at="2026-04-28T12:02:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="play-track-1",
            spotify_album_id="a1",
        )
        album_tracks_calls = 0

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            nonlocal album_tracks_calls
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums"):
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if "/v1/albums/" in url and url.endswith("/tracks"):
                album_tracks_calls += 1
                return 200, {}, {"items": [_track_payload("a1-t1", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            album_tracklist_policy="relevant_albums",
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, album_tracks_calls)
        self.assertEqual(1, result["album_tracklists_seen"])
        self.assertEqual(1, result["album_tracklists_skipped_by_policy"])
        self.assertEqual(0, result["album_tracklists_fetched"])

    def test_album_tracklist_policy_all_preserves_tracklist_fetch_behavior(self) -> None:
        self._seed_source_tracks(["t1"])
        fetched_album_track_ids: list[str] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums"):
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if "/v1/albums/" in url and url.endswith("/tracks"):
                album_id = url.split("/v1/albums/")[1].split("/tracks")[0]
                fetched_album_track_ids.append(album_id)
                return 200, {}, {"items": [_track_payload("a1-t1", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            album_tracklist_policy="all",
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(["a1"], fetched_album_track_ids)
        self.assertEqual(1, result["album_tracklists_seen"])
        self.assertEqual(0, result["album_tracklists_skipped_by_policy"])
        self.assertEqual(1, result["album_tracklists_fetched"])

    def test_enqueue_skips_already_complete_catalog_rows(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, album_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("t-complete", 123000, "a1", "US", "2026-04-27T10:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a-complete", 1, "US", "2026-04-27T10:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a-complete", "t-a-complete", "US", "2026-04-27T10:00:00Z", "ok", None),
            )
            connection.commit()

        result = enqueue_spotify_catalog_backfill_items(
            items=[
                {"entity_type": "track", "spotify_id": "t-complete", "reason": "ui", "priority": 10},
                {"entity_type": "album", "spotify_id": "a-complete", "reason": "ui", "priority": 10},
            ]
        )
        self.assertTrue(result["ok"])
        self.assertEqual(2, result["received"])
        self.assertEqual(2, result["already_complete"])
        self.assertEqual(0, result["enqueued"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            queue_count = int(connection.execute("SELECT count(*) FROM spotify_catalog_backfill_queue").fetchone()[0])
        self.assertEqual(0, queue_count)

    def test_enqueue_dedupes_input(self) -> None:
        result = enqueue_spotify_catalog_backfill_items(
            items=[
                {"entity_type": "track", "spotify_id": "t1", "reason": "first", "priority": 10},
                {"entity_type": "track", "spotify_id": "t1", "reason": "second", "priority": 30},
            ]
        )
        self.assertTrue(result["ok"])
        self.assertEqual(2, result["received"])
        self.assertEqual(1, result["enqueued"])
        self.assertEqual(0, result["invalid"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT entity_type, spotify_id, priority, reason FROM spotify_catalog_backfill_queue"
            ).fetchone()
        self.assertEqual(("track", "t1", 30, "first | second"), row)

    def test_existing_queued_item_priority_increases(self) -> None:
        enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "track", "spotify_id": "t1", "reason": "seed", "priority": 20}]
        )
        result = enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "track", "spotify_id": "t1", "reason": "urgent", "priority": 80}]
        )
        self.assertTrue(result["ok"])
        self.assertEqual(1, result["updated"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT priority, reason, status FROM spotify_catalog_backfill_queue WHERE entity_type = 'track' AND spotify_id = 't1'"
            ).fetchone()
        self.assertEqual(80, int(row[0]))
        self.assertIn("seed", str(row[1]))
        self.assertIn("urgent", str(row[1]))
        self.assertEqual("pending", str(row[2]))

    def test_track_seed_prefers_most_listened_spotify_id_per_release_track(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Representative Track", "representative track"),
                ).lastrowid
            )
            source_track_low_id = int(
                connection.execute(
                    """
                    INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("spotify", "t-low", "spotify:track:t-low", "Track low", "{}"),
                ).lastrowid
            )
            source_track_high_id = int(
                connection.execute(
                    """
                    INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("spotify", "t-high", "spotify:track:t-high", "Track high", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_low_id, release_track_id),
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_high_id, release_track_id),
            )
            connection.commit()

        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="listen-low-1",
            played_at="2026-04-20T12:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="t-low",
            spotify_track_uri="spotify:track:t-low",
        )
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="listen-high-1",
            played_at="2026-04-20T12:01:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="t-high",
            spotify_track_uri="spotify:track:t-high",
        )
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="listen-high-2",
            played_at="2026-04-20T12:02:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="t-high",
            spotify_track_uri="spotify:track:t-high",
        )

        captured_track_ids: list[str] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                captured_track_ids.extend(ids)
                return 200, {}, {"tracks": [_track_payload(track_id, "album-x") for track_id in ids]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(["t-high"], captured_track_ids)
        self.assertEqual(1, result["tracks_seen"])

    def test_album_fetch_prefers_most_listened_spotify_id_per_release_album(self) -> None:
        self._seed_source_tracks(["t1", "t2"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Representative Album", "representative album"),
                ).lastrowid
            )
            source_album_low_id = int(
                connection.execute(
                    """
                    INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("spotify", "a-low", "spotify:album:a-low", "Album low", "{}"),
                ).lastrowid
            )
            source_album_high_id = int(
                connection.execute(
                    """
                    INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("spotify", "a-high", "spotify:album:a-high", "Album high", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_album_low_id, release_album_id),
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_album_high_id, release_album_id),
            )
            connection.commit()

        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="album-low-1",
            played_at="2026-04-20T13:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_album_id="a-low",
        )
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="album-high-1",
            played_at="2026-04-20T13:01:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_album_id="a-high",
        )
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="album-high-2",
            played_at="2026-04-20T13:02:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_album_id="a-high",
        )

        captured_album_batches: list[list[str]] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                payload_by_track = {
                    "t1": _track_payload("t1", "a-low"),
                    "t2": _track_payload("t2", "a-high"),
                }
                return 200, {}, {"tracks": [payload_by_track[track_id] for track_id in ids]}, None
            if url.endswith("/v1/albums"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                captured_album_batches.append(ids)
                return 200, {}, {"albums": [_album_payload(album_id) for album_id in ids]}, None
            if "/v1/albums/" in url and url.endswith("/tracks"):
                album_id = url.split("/v1/albums/")[1].split("/tracks")[0]
                return 200, {}, {"items": [_track_payload(f"{album_id}-x", album_id)], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["albums_seen"])
        self.assertEqual([["a-high"]], captured_album_batches)

    def test_idempotent_rerun_no_duplicates(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums"):
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": None}, None
            raise AssertionError(url)

        run_spotify_catalog_backfill(access_token="token", include_albums=True, sleeper=lambda _: None, fetcher=fetcher)
        run_spotify_catalog_backfill(access_token="token", include_albums=True, sleeper=lambda _: None, fetcher=fetcher)

        with closing(sqlite3.connect(self.db_path)) as connection:
            self.assertEqual(1, int(connection.execute("SELECT count(*) FROM spotify_track_catalog").fetchone()[0]))
            self.assertEqual(1, int(connection.execute("SELECT count(*) FROM spotify_album_catalog").fetchone()[0]))
            self.assertEqual(1, int(connection.execute("SELECT count(*) FROM spotify_album_track").fetchone()[0]))

    def test_existing_complete_track_row_is_skipped_no_track_request(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, album_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("t1", 123000, "a1", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            raise AssertionError(f"Unexpected Spotify request: {url}")

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, result["requests_total"])
        self.assertEqual(0, result["tracks_fetched"])
        self.assertEqual(1, result["skipped"])

    def test_existing_incomplete_track_row_is_fetched(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, album_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("t1", None, "a1", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["requests_total"])
        self.assertEqual(1, result["tracks_fetched"])
        self.assertEqual(0, result["skipped"])

    def test_existing_error_track_row_is_fetched(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, album_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("t1", 123000, "a1", "US", "2026-04-25T00:00:00Z", "error", "prior error"),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["requests_total"])
        self.assertEqual(1, result["tracks_fetched"])
        self.assertEqual(0, result["skipped"])

    def test_existing_complete_album_row_is_skipped(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 2, "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-y", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                raise AssertionError("Album batch should be skipped for complete catalog row")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["albums_seen"])
        self.assertEqual(0, result["albums_fetched"])
        self.assertEqual(1, result["skipped"])
        self.assertEqual(1, result["requests_total"])

    def test_existing_complete_album_tracklist_is_skipped(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-y", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                raise AssertionError("Album tracks should be skipped when complete tracklist exists")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["albums_fetched"])
        self.assertEqual(0, result["album_tracks_upserted"])
        self.assertEqual(1, result["skipped"])
        self.assertEqual(2, result["requests_total"])

    def test_complete_album_metadata_with_incomplete_tracklist_triggers_track_fetch(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 2, "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.commit()

        called_album_tracks = {"value": False}

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                raise AssertionError("Album metadata should be skipped when already complete")
            if url.endswith("/v1/albums/a1/tracks"):
                called_album_tracks["value"] = True
                return 200, {}, {"items": [_track_payload("a1-x", "a1"), _track_payload("a1-y", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertTrue(called_album_tracks["value"])
        self.assertEqual(2, result["album_tracks_upserted"])

    def test_complete_album_metadata_with_complete_tracklist_skips_track_fetch(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 2, "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-y", "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                raise AssertionError("Album tracks should be skipped for complete tracklist")
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                raise AssertionError("Album metadata should be skipped when already complete")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, result["album_tracks_upserted"])

    def test_album_track_error_row_triggers_tracklist_refetch(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 2, "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-27T00:00:00Z", "error", "prior"),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-y", "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.commit()

        called_album_tracks = {"value": False}

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                called_album_tracks["value"] = True
                return 200, {}, {"items": [_track_payload("a1-x", "a1"), _track_payload("a1-y", "a1")], "next": None}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                raise AssertionError("Album metadata should be skipped when already complete")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertTrue(called_album_tracks["value"])
        self.assertEqual(2, result["album_tracks_upserted"])

    def test_partial_prior_album_tracklist_resumes_without_force_refresh(self) -> None:
        self._seed_source_tracks(["t1"])

        def first_fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": "https://api.spotify.com/v1/albums/a1/tracks?offset=50"}, None
            raise AssertionError(url)

        first_result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            max_album_tracks_pages_per_album=1,
            sleeper=lambda _: None,
            fetcher=first_fetcher,
        )
        self.assertEqual("ok", first_result["status"])
        self.assertEqual(1, first_result["album_tracklists_capped"])

        album_batch_called = {"value": False}
        second_page_called = {"value": False}

        def second_fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                album_batch_called["value"] = True
                raise AssertionError("Second run should resume without album metadata fetch")
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": "https://api.spotify.com/v1/albums/a1/tracks?offset=50"}, None
            if "/v1/albums/a1/tracks?offset=50" in url:
                second_page_called["value"] = True
                return 200, {}, {"items": [_track_payload("a1-y", "a1")], "next": None}, None
            raise AssertionError(url)

        second_result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=second_fetcher,
        )
        self.assertEqual("ok", second_result["status"])
        self.assertFalse(album_batch_called["value"])
        self.assertTrue(second_page_called["value"])

    def test_album_track_resume_uses_existing_count_offset(self) -> None:
        self._seed_source_tracks(["t1"])

        first_page_requested = {"value": False}

        def first_fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                first_page_requested["value"] = True
                self.assertEqual(0, int(params.get("offset", 0)))
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": "https://api.spotify.com/v1/albums/a1/tracks?offset=50"}, None
            raise AssertionError(url)

        first_result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            max_album_tracks_pages_per_album=1,
            sleeper=lambda _: None,
            fetcher=first_fetcher,
        )
        self.assertEqual("ok", first_result["status"])
        self.assertTrue(first_page_requested["value"])

        second_run_first_offset = {"value": None}

        def second_fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                raise AssertionError("Album metadata should be skipped on resume")
            if url.endswith("/v1/albums/a1/tracks"):
                second_run_first_offset["value"] = int(params.get("offset", 0))
                self.assertEqual(1, second_run_first_offset["value"])
                return 200, {}, {"items": [_track_payload("a1-y", "a1")], "next": None}, None
            raise AssertionError(url)

        second_result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=second_fetcher,
        )
        self.assertEqual("ok", second_result["status"])
        self.assertEqual(1, second_run_first_offset["value"])

    def test_album_track_resume_force_refresh_starts_at_zero(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 2, "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.commit()

        requested_offset = {"value": None}

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                requested_offset["value"] = int(params.get("offset", 0))
                return 200, {}, {"items": [_track_payload("a1-x", "a1"), _track_payload("a1-y", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            force_refresh=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, requested_offset["value"])

    def test_album_track_resume_error_row_restarts_at_zero(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 2, "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-27T00:00:00Z", "error", "prior"),
            )
            connection.commit()

        requested_offset = {"value": None}

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                raise AssertionError("Album metadata should be skipped when already complete")
            if url.endswith("/v1/albums/a1/tracks"):
                requested_offset["value"] = int(params.get("offset", 0))
                return 200, {}, {"items": [_track_payload("a1-x", "a1"), _track_payload("a1-y", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, requested_offset["value"])

    def test_capped_album_eventually_completes_over_multiple_runs(self) -> None:
        self._seed_source_tracks(["t1"])

        def make_fetcher() -> Callable[[str, dict[str, Any], str], tuple[int, dict[str, str], dict[str, Any], str | None]]:
            def fetcher(
                url: str, params: dict[str, Any], access_token: str
            ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
                if url.endswith("/v1/tracks"):
                    return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
                if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                    payload = _album_payload("a1")
                    payload["total_tracks"] = 3
                    return 200, {}, {"albums": [payload]}, None
                if url.endswith("/v1/albums/a1/tracks"):
                    offset = int(params.get("offset", 0))
                    if offset == 0:
                        return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": "https://api.spotify.com/v1/albums/a1/tracks?offset=50"}, None
                    if offset == 1:
                        return 200, {}, {"items": [_track_payload("a1-y", "a1")], "next": "https://api.spotify.com/v1/albums/a1/tracks?offset=50"}, None
                    if offset == 2:
                        return 200, {}, {"items": [_track_payload("a1-z", "a1")], "next": None}, None
                    raise AssertionError(f"unexpected offset {offset}")
                raise AssertionError(url)
            return fetcher

        first = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            max_album_tracks_pages_per_album=1,
            sleeper=lambda _: None,
            fetcher=make_fetcher(),
        )
        second = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            max_album_tracks_pages_per_album=1,
            sleeper=lambda _: None,
            fetcher=make_fetcher(),
        )
        third = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            max_album_tracks_pages_per_album=1,
            sleeper=lambda _: None,
            fetcher=make_fetcher(),
        )
        self.assertEqual("ok", first["status"])
        self.assertEqual("ok", second["status"])
        self.assertEqual("ok", third["status"])
        self.assertEqual(1, first["album_tracklists_capped"])
        self.assertEqual(1, second["album_tracklists_capped"])
        self.assertEqual(0, third["album_tracklists_capped"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            final_count = int(connection.execute("SELECT count(*) FROM spotify_album_track WHERE spotify_album_id = ?", ("a1",)).fetchone()[0])
        self.assertEqual(3, final_count)

    def test_album_track_resume_does_not_mutate_analysis_track_map(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(connection.execute("INSERT INTO analysis_track (primary_name) VALUES (?)", ("Track A",)).lastrowid)
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id, analysis_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": "https://api.spotify.com/v1/albums/a1/tracks?offset=50"}, None
            raise AssertionError(url)

        _ = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            max_album_tracks_pages_per_album=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_force_refresh_still_fetches_album_tracklist(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 1, "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-27T00:00:00Z", "ok", None),
            )
            connection.commit()

        called_album_batch = {"value": False}
        called_album_tracks = {"value": False}

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                called_album_batch["value"] = True
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                called_album_tracks["value"] = True
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            force_refresh=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertTrue(called_album_batch["value"])
        self.assertTrue(called_album_tracks["value"])

    def test_force_refresh_fetches_despite_existing_rows(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, album_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("t1", 123000, "a1", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            force_refresh=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["requests_total"])
        self.assertEqual(1, result["tracks_fetched"])
        self.assertEqual(0, result["skipped"])

    def test_skip_existing_increments_skipped_counter(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, album_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("t1", 123000, "a1", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 2, "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-y", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            raise AssertionError(f"Unexpected Spotify request: {url}")

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, result["requests_total"])
        self.assertEqual(2, result["skipped"])

    def test_skip_existing_does_not_mutate_analysis_track_map(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, album_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("t1", 123000, "a1", "US", "2026-04-25T00:00:00Z", "ok", None),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=lambda *_: (_ for _ in ()).throw(AssertionError("No Spotify request expected")),
        )
        self.assertEqual("ok", result["status"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_bounded_run_returns_has_more(self) -> None:
        self._seed_source_tracks(["t1", "t2", "t3"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = str(params.get("ids") or "").split(",")
                return 200, {}, {"tracks": [_track_payload(track_id, f"a{track_id}") for track_id in ids]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            limit=2,
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertTrue(result["has_more"])
        self.assertEqual(2, result["tracks_seen"])

    def test_runner_processes_queued_item_before_bulk_backlog(self) -> None:
        self._seed_source_tracks(["t-bulk"])
        enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "track", "spotify_id": "t-queue", "reason": "visible", "priority": 80}]
        )
        call_urls: list[str] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            call_urls.append(url)
            if url.endswith("/v1/tracks/t-queue"):
                return 200, {}, _track_payload("t-queue", "a-q"), None
            if url.endswith("/v1/tracks"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                self.assertIn("t-bulk", ids)
                return 200, {}, {"tracks": [_track_payload(track_id, "a-b") for track_id in ids]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=2,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertGreaterEqual(len(call_urls), 2)
        self.assertTrue(call_urls[0].endswith("/v1/tracks/t-queue"))

    def test_queue_item_marked_done_on_successful_fetch(self) -> None:
        enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "track", "spotify_id": "t1", "reason": "visible", "priority": 80}]
        )

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks/t1"):
                return 200, {}, _track_payload("t1", "a1"), None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status, attempts, last_error FROM spotify_catalog_backfill_queue WHERE entity_type = 'track' AND spotify_id = 't1'"
            ).fetchone()
        self.assertEqual("done", str(row[0]))
        self.assertEqual(0, int(row[1] or 0))
        self.assertIsNone(row[2])

    def test_queue_orders_and_filters_by_reason_bucket(self) -> None:
        enqueue_spotify_catalog_backfill_items(
            items=[
                {"entity_type": "track", "spotify_id": "t-full", "reason": "full_backfill", "priority": 100},
                {"entity_type": "track", "spotify_id": "t-identity", "reason": "identity_metadata", "priority": 10},
                {"entity_type": "track", "spotify_id": "t-manual", "reason": "manual_priority", "priority": 80},
            ]
        )

        payload = list_spotify_catalog_backfill_queue(status_filter="pending", limit=10, offset=0)
        self.assertEqual(["t-identity", "t-manual", "t-full"], [item["spotify_id"] for item in payload["items"]])
        self.assertEqual(1, payload["reason_counts"]["identity_metadata"])
        self.assertEqual(1, payload["reason_counts"]["manual_priority"])
        self.assertEqual(1, payload["reason_counts"]["full_backfill"])

        filtered = list_spotify_catalog_backfill_queue(
            status_filter="pending",
            reason_filter="identity_metadata",
            limit=10,
            offset=0,
        )
        self.assertEqual(["t-identity"], [item["spotify_id"] for item in filtered["items"]])

    def test_metadata_only_fetches_source_album_metadata_without_tracklists(self) -> None:
        self._seed_accepted_source_track("t1")
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Source Album", "source album"),
                ).lastrowid
            )
            source_album_id = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "a-source", "spotify:album:a-source", "Source Album", "{}"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')",
                (source_album_id, release_album_id),
            )
            connection.commit()

        called_urls: list[str] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            called_urls.append(url)
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a-track")]}, None
            if url.endswith("/v1/albums"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                return 200, {}, {"albums": [_album_payload(album_id) for album_id in ids]}, None
            if "/tracks" in url:
                raise AssertionError(f"metadata_only should not fetch album tracklists: {url}")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="metadata_only",
            include_albums=False,
            album_tracklist_policy="all",
            priority_scope="all",
            limit=5,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual("metadata_only", result["run_mode"])
        self.assertEqual("identity_metadata", result["run_reason"])
        self.assertEqual("none", result["album_tracklist_policy"])
        self.assertTrue(result["include_albums"])
        self.assertEqual(0, result["album_tracklists_fetched"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            album_ids = {
                str(row[0])
                for row in connection.execute("SELECT spotify_album_id FROM spotify_album_catalog").fetchall()
            }
        self.assertIn("a-track", album_ids)
        self.assertIn("a-source", album_ids)
        self.assertFalse(any(url.endswith("/tracks") and "/albums/" in url for url in called_urls))

    def test_metadata_only_target_tracks_does_not_fetch_albums_even_when_include_albums_true(self) -> None:
        self._seed_accepted_source_track("t1")
        called_urls: list[str] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            called_urls.append(url)
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if "/v1/albums" in url:
                raise AssertionError(f"target=tracks should not fetch albums: {url}")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="metadata_only",
            target="tracks",
            include_albums=True,
            priority_scope="all",
            limit=5,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual("tracks", result["target"])
        self.assertFalse(result["include_albums"])
        self.assertEqual(1, result["tracks_fetched"])
        self.assertEqual(0, result["albums_seen"])
        self.assertEqual(0, result["albums_fetched"])
        self.assertEqual(0, result["album_tracks_upserted"])
        self.assertFalse(any("/v1/albums" in url for url in called_urls))

    def test_metadata_only_target_all_preserves_current_mixed_behavior(self) -> None:
        self._seed_accepted_source_track("t1")

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                return 200, {}, {"albums": [_album_payload(album_id) for album_id in ids]}, None
            if "/v1/albums/" in url and url.endswith("/tracks"):
                raise AssertionError(f"metadata_only target=all should not fetch tracklists: {url}")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="metadata_only",
            target="all",
            include_albums=False,
            album_tracklist_policy="all",
            priority_scope="all",
            limit=5,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual("all", result["target"])
        self.assertTrue(result["include_albums"])
        self.assertEqual("none", result["album_tracklist_policy"])
        self.assertEqual(1, result["tracks_fetched"])
        self.assertEqual(1, result["albums_fetched"])
        self.assertEqual(0, result["album_tracks_upserted"])

    def test_metadata_only_target_albums_fetches_album_metadata_only(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Album Only", "album only"),
                ).lastrowid
            )
            source_album_id = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "a-source", "spotify:album:a-source", "Album Only", "{}"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')",
                (source_album_id, release_album_id),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if "/v1/tracks" in url:
                raise AssertionError(f"target=albums should not fetch tracks: {url}")
            if url.endswith("/v1/albums"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                return 200, {}, {"albums": [_album_payload(album_id) for album_id in ids]}, None
            if "/v1/albums/" in url and url.endswith("/tracks"):
                raise AssertionError(f"target=albums should not fetch tracklists: {url}")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="metadata_only",
            target="albums",
            limit=5,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual("albums", result["target"])
        self.assertEqual(0, result["tracks_seen"])
        self.assertEqual(0, result["tracks_fetched"])
        self.assertEqual(1, result["albums_seen"])
        self.assertEqual(1, result["albums_fetched"])
        self.assertEqual(0, result["album_tracks_upserted"])

    def test_metadata_only_target_albums_filters_missing_before_limit(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            for index in range(5):
                album_id = f"a-complete-{index}"
                connection.execute(
                    "INSERT INTO raw_play_event (source_type, source_row_key, played_at, ms_played, ms_played_method, spotify_album_id, album_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    ("history", f"row-complete-{index}", "2026-04-28T00:00:00Z", 1000, "history_source", album_id, album_id, "{}"),
                )
                connection.execute(
                    """
                    INSERT INTO spotify_album_catalog (
                      spotify_album_id, release_date, total_tracks, raw_json, market, fetched_at, last_status, last_error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (album_id, "2024-01-01", 2, json.dumps({"external_ids": {"upc": f"UPC{album_id}"}}), "US", "2026-04-28T00:00:00Z", "ok", None),
                )
            connection.execute(
                "INSERT INTO raw_play_event (source_type, source_row_key, played_at, ms_played, ms_played_method, spotify_album_id, album_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("history", "row-upc-only", "2026-04-28T00:00:00Z", 1000, "history_source", "b-upc-only", "UPC Only", "{}"),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, release_date, total_tracks, raw_json, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("b-upc-only", "2024-01-01", 2, json.dumps({"external_ids": {}}), "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                "INSERT INTO raw_play_event (source_type, source_row_key, played_at, ms_played, ms_played_method, spotify_album_id, album_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("history", "row-missing-late", "2026-04-28T00:00:00Z", 1000, "history_source", "z-missing-late", "Missing Late", "{}"),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/albums"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                self.assertEqual(["z-missing-late"], ids)
                return 200, {}, {"albums": [_album_payload("z-missing-late")]}, None
            if "/v1/tracks" in url or "/v1/albums/" in url:
                raise AssertionError(url)
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="metadata_only",
            target="albums",
            limit=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["albums_seen"])
        self.assertEqual(1, result["albums_fetched"])
        self.assertEqual(0, result["tracks_fetched"])
        self.assertEqual(0, result["album_tracks_upserted"])

    def test_metadata_only_target_albums_prioritizes_missing_release_before_external_ids(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            for album_id, release_date, raw_json in (
                ("a-upc-only", "2024-01-01", json.dumps({"external_ids": {}})),
                ("z-missing-release", None, json.dumps({"external_ids": {"upc": "UPCz"}})),
            ):
                connection.execute(
                    "INSERT INTO raw_play_event (source_type, source_row_key, played_at, ms_played, ms_played_method, spotify_album_id, album_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    ("history", f"row-{album_id}", "2026-04-28T00:00:00Z", 1000, "history_source", album_id, album_id, "{}"),
                )
                connection.execute(
                    """
                    INSERT INTO spotify_album_catalog (
                      spotify_album_id, release_date, total_tracks, raw_json, market, fetched_at, last_status, last_error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (album_id, release_date, 2, raw_json, "US", "2026-04-28T00:00:00Z", "ok", None),
                )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/albums"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                self.assertEqual(["z-missing-release"], ids)
                return 200, {}, {"albums": [_album_payload("z-missing-release")]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="metadata_only",
            target="albums",
            limit=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["albums_fetched"])

    def test_target_album_tracklists_fetches_only_tracklists_when_explicit_policy_allows(self) -> None:
        enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "album", "spotify_id": "a1", "reason": "tracklist_completion", "priority": 80}]
        )
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, release_date, total_tracks, raw_json, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("a1", "2024-01-01", 2, json.dumps({"external_ids": {"upc": "UPCa1"}}), "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.commit()

        called_urls: list[str] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            called_urls.append(url)
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1"), _track_payload("a1-y", "a1")], "next": None}, None
            if url.endswith("/v1/tracks") or url.endswith("/v1/albums") or url.endswith("/v1/albums/a1"):
                raise AssertionError(f"target=album_tracklists should only fetch tracklists: {url}")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="tracklists_relevant",
            target="album_tracklists",
            album_tracklist_policy="priority_only",
            limit=5,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual("album_tracklists", result["target"])
        self.assertEqual(0, result["tracks_fetched"])
        self.assertEqual(0, result["albums_fetched"])
        self.assertEqual(2, result["album_tracks_upserted"])
        self.assertEqual([True], [url.endswith("/v1/albums/a1/tracks") for url in called_urls])

    def test_metadata_only_target_album_tracklists_is_rejected_without_spotify_calls(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            raise AssertionError(f"Invalid target should not make Spotify requests: {url}")

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="metadata_only",
            target="album_tracklists",
            album_tracklist_policy="priority_only",
            limit=5,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("failed", result["status"])
        self.assertEqual("album_tracklists", result["target"])
        self.assertEqual(0, result["requests_total"])
        self.assertGreater(result["errors"], 0)
        self.assertIn("metadata_only target=album_tracklists is invalid", str(result["last_error"]))

    def test_metadata_only_ignores_non_identity_queue_rows(self) -> None:
        self._seed_accepted_source_track("t-missing")
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, release_date, total_tracks, raw_json, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "alb-tracklist",
                    "2024-01-01",
                    2,
                    json.dumps({"external_ids": {"upc": "UPCalb-tracklist"}}),
                    "US",
                    "2026-04-28T00:00:00Z",
                    "ok",
                    None,
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "alb-tracklist", "album_lookup_visible_incomplete", 80, "pending", "2026-04-28T00:00:00Z", 0),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("track", "t-manual", "manual_priority", 80, "pending", "2026-04-28T00:00:01Z", 0),
            )
            connection.commit()

        called_urls: list[str] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            called_urls.append(url)
            if url.endswith("/v1/tracks"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                self.assertEqual(["t-missing"], ids)
                return 200, {}, {"tracks": [_track_payload("t-missing", "a-missing")]}, None
            if url.endswith("/v1/albums"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                self.assertEqual(["a-missing"], ids)
                return 200, {}, {"albums": [_album_payload("a-missing")]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="metadata_only",
            priority_scope="all",
            limit=5,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["tracks_fetched"])
        self.assertEqual(0, result["album_tracks_upserted"])
        self.assertFalse(any(url.endswith("/v1/albums/alb-tracklist") for url in called_urls))
        self.assertFalse(any(url.endswith("/v1/tracks/t-manual") for url in called_urls))
        with closing(sqlite3.connect(self.db_path)) as connection:
            rows = connection.execute(
                "SELECT spotify_id, status FROM spotify_catalog_backfill_queue ORDER BY spotify_id"
            ).fetchall()
        self.assertEqual([("alb-tracklist", "pending"), ("t-manual", "pending")], rows)

    def test_metadata_only_does_not_refetch_metadata_complete_album_for_missing_tracklist(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Complete Metadata Album", "complete metadata album"),
                ).lastrowid
            )
            source_album_id = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "a-complete-metadata", "spotify:album:a-complete-metadata", "Complete Metadata Album", "{}"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')",
                (source_album_id, release_album_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, release_date, total_tracks, raw_json, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "a-complete-metadata",
                    "2024-01-01",
                    2,
                    json.dumps({"external_ids": {"upc": "UPCa-complete-metadata"}}),
                    "US",
                    "2026-04-28T00:00:00Z",
                    "ok",
                    None,
                ),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            raise AssertionError(f"metadata_only should not refetch metadata-complete album: {url}")

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="metadata_only",
            limit=5,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual(0, result["albums_fetched"])
        self.assertEqual(0, result["album_tracks_upserted"])

    def test_metadata_only_selects_missing_track_metadata_before_complete_lexicographic_tracks(self) -> None:
        self._seed_accepted_source_track("000-complete")
        self._seed_accepted_source_track("999-missing")
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, disc_number, track_number, album_id, raw_json,
                  market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "000-complete",
                    180000,
                    1,
                    1,
                    "a-complete",
                    json.dumps({"external_ids": {"isrc": "ISRC000COMPLETE"}}),
                    "US",
                    "2026-04-28T00:00:00Z",
                    "ok",
                    None,
                ),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                self.assertEqual(["999-missing"], ids)
                return 200, {}, {"tracks": [_track_payload("999-missing", "a-missing")]}, None
            if url.endswith("/v1/albums"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                return 200, {}, {"albums": [_album_payload(album_id) for album_id in ids]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="metadata_only",
            priority_scope="all",
            limit=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["tracks_seen"])
        self.assertEqual(1, result["tracks_fetched"])
        self.assertEqual(1, result["tracks_upserted"])

    def test_priority_metadata_selects_identity_relevant_missing_tracks(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            duplicate_source_id = int(
                connection.execute(
                    """
                    INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("spotify", "trk-identity", "spotify:track:trk-identity", "Identity Track", "{}"),
                ).lastrowid
            )
            for name in ("Identity One", "Identity Two"):
                release_track_id = int(
                    connection.execute(
                        "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                        (name, name.lower()),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                    """,
                    (duplicate_source_id, release_track_id),
                )

            mapped_source_id = int(
                connection.execute(
                    """
                    INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("spotify", "trk-mapped", "spotify:track:trk-mapped", "Mapped Track", "{}"),
                ).lastrowid
            )
            mapped_release_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Mapped Track", "mapped track"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (mapped_source_id, mapped_release_id),
            )
            connection.execute(
                """
                INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("spotify", "trk-high", "spotify:track:trk-high", "High Listen Track", "{}"),
            )
            connection.execute(
                """
                INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("spotify", "trk-artist", "spotify:track:trk-artist", "Artist Boost Track", "{}"),
            )
            connection.execute(
                """
                INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("spotify", "trk-raw-tail", "spotify:track:trk-raw-tail", "Raw Tail Track", "{}"),
            )
            connection.execute(
                """
                INSERT INTO raw_spotify_history (
                  source_row_key, played_at, spotify_track_id, spotify_track_uri, track_name_raw, artist_name_raw, album_name_raw,
                  ms_played, raw_payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "raw-tail",
                    "2026-04-27T00:00:00Z",
                    "trk-raw-tail",
                    "spotify:track:trk-raw-tail",
                    "Raw Tail Track",
                    "Raw Tail Artist",
                    "Raw Tail Album",
                    1000,
                    "{}",
                ),
            )
            for index in range(5):
                connection.execute(
                    """
                    INSERT INTO fact_play_event (
                      canonical_ended_at, spotify_track_id, track_name_canonical, artist_name_canonical, timing_source, matched_state
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"2026-04-27T00:0{index}:00Z",
                        "trk-high",
                        "High Listen Track",
                        "High Listen Artist",
                        "seed",
                        "matched",
                    ),
                )
            connection.execute(
                """
                INSERT INTO fact_play_event (
                  canonical_ended_at, spotify_track_id, track_name_canonical, artist_name_canonical, timing_source, matched_state
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("2026-04-27T01:00:00Z", "trk-artist", "Artist Boost Track", "Artist Boost", "seed", "matched"),
            )
            for index in range(9):
                connection.execute(
                    """
                    INSERT INTO fact_play_event (
                      canonical_ended_at, spotify_track_id, track_name_canonical, artist_name_canonical, timing_source, matched_state
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"2026-04-27T02:0{index}:00Z",
                        f"artist-other-{index}",
                        f"Artist Other {index}",
                        "Artist Boost",
                        "seed",
                        "matched",
                    ),
                )
            connection.commit()

        seen_batches: list[list[str]] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                seen_batches.append(ids)
                return 200, {}, {"tracks": [_track_payload(track_id, f"album-{track_id}") for track_id in ids]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="metadata_only",
            target="tracks",
            include_albums=False,
            limit=5,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["tracks_fetched"])
        self.assertEqual([["trk-identity"]], seen_batches)

    def test_priority_metadata_selects_top_listened_missing_tracks(self) -> None:
        self._seed_accepted_source_track("trk-top", name="Top Track")
        self._seed_accepted_source_track("trk-deferred", name="Deferred Track")
        with closing(sqlite3.connect(self.db_path)) as connection:
            for index in range(5):
                connection.execute(
                    """
                    INSERT INTO fact_play_event (
                      canonical_ended_at, spotify_track_id, spotify_album_id, track_name_canonical, artist_name_canonical,
                      timing_source, matched_state
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"2026-04-27T00:0{index}:00Z",
                        "trk-top",
                        "album-top",
                        "Top Track",
                        "Top Artist",
                        "seed",
                        "matched",
                    ),
                )
            connection.commit()

        seen_batches: list[list[str]] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                seen_batches.append(ids)
                return 200, {}, {"tracks": [_track_payload(track_id, f"album-{track_id}") for track_id in ids]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="metadata_only",
            target="tracks",
            include_albums=False,
            limit=5,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual([["trk-top"]], seen_batches)

    def test_priority_metadata_includes_nonrepresentative_readiness_blocking_split_track(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Split Track", "split track"),
                ).lastrowid
            )
            for spotify_track_id in ("trk-complete", "trk-readiness-missing"):
                source_track_id = int(
                    connection.execute(
                        """
                        INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            "spotify",
                            spotify_track_id,
                            f"spotify:track:{spotify_track_id}",
                            "Split Track",
                            "{}",
                        ),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_track_id, release_track_id),
                )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, explicit, disc_number, track_number,
                  album_id, artists_json, raw_json, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "trk-complete",
                    "Split Track",
                    181000,
                    0,
                    1,
                    1,
                    "album-complete",
                    json.dumps([{"name": "Artist A"}]),
                    json.dumps({"external_ids": {"isrc": "ISRC1"}}),
                    "2026-05-01T12:00:00Z",
                    "ok",
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, release_date, total_tracks, artists_json, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "album-complete",
                    "Album Complete",
                    "2024-01-01",
                    1,
                    json.dumps([{"name": "Artist A"}]),
                    "2026-05-01T12:00:00Z",
                    "ok",
                ),
            )
            connection.commit()

        comparison = get_identity_readiness_track_metadata_priority_comparison(sample_limit=10)
        self.assertEqual(1, comparison["blocked_groups"])
        self.assertEqual(1, comparison["distinct_spotify_source_tracks_missing_track_metadata"])
        self.assertEqual(1, comparison["included_by_priority_scope"])
        self.assertEqual(0, comparison["not_included_by_priority_scope"])

        seen_batches: list[list[str]] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                seen_batches.append(ids)
                return 200, {}, {"tracks": [_track_payload(track_id, f"album-{track_id}") for track_id in ids]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="metadata_only",
            target="tracks",
            include_albums=False,
            limit=5,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual([["trk-readiness-missing"]], seen_batches)

    def test_priority_metadata_defers_missing_tracks_with_no_priority_flags(self) -> None:
        self._seed_accepted_source_track("trk-deferred", name="Deferred Track")

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            raise AssertionError(f"priority metadata should not fetch deferred track: {url}")

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="metadata_only",
            target="tracks",
            include_albums=False,
            limit=5,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual(0, result["tracks_seen"])
        self.assertEqual(0, result["tracks_fetched"])

    def test_broad_metadata_scope_can_select_deferred_missing_tracks(self) -> None:
        self._seed_accepted_source_track("trk-deferred", name="Deferred Track")
        seen_batches: list[list[str]] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                seen_batches.append(ids)
                return 200, {}, {"tracks": [_track_payload(track_id, f"album-{track_id}") for track_id in ids]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="metadata_only",
            target="tracks",
            include_albums=False,
            priority_scope="all",
            limit=5,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual([["trk-deferred"]], seen_batches)

    def test_progress_callback_emits_start_and_ten_track_progress(self) -> None:
        track_ids = [f"t{i:02d}" for i in range(10)]
        for track_id in track_ids:
            self._seed_accepted_source_track(track_id)
        events: list[dict[str, Any]] = []

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [value for value in str(params.get("ids") or "").split(",") if value]
                return 200, {}, {"tracks": [_track_payload(track_id, f"album-{track_id}") for track_id in ids]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            run_mode="metadata_only",
            target="tracks",
            include_albums=False,
            priority_scope="all",
            limit=10,
            sleeper=lambda _: None,
            fetcher=fetcher,
            progress_callback=events.append,
        )

        self.assertEqual("ok", result["status"])
        self.assertGreaterEqual(len(events), 2)
        self.assertEqual("start", events[0]["event"])
        self.assertEqual(result["run_id"], events[0]["run_id"])
        self.assertEqual(10, events[0]["candidate_counts"]["tracks_seen"])
        self.assertEqual("tracks", events[0]["worker_config"]["target"])
        self.assertEqual(f"Run {result['run_id']} started: 10 track candidates, has_more=False", events[0]["message"])
        progress_events = [event for event in events if event["event"] == "progress"]
        self.assertEqual(1, len(progress_events))
        progress = progress_events[0]
        self.assertEqual(10, progress["tracks_fetched"])
        self.assertEqual(1, progress["requests_total"])
        self.assertEqual(0, progress["requests_429"])
        self.assertIn("elapsed_seconds", progress)
        self.assertIn("10/10 tracks fetched", progress["message"])
        self.assertNotIn("access_token", json.dumps(events, sort_keys=True))
        self.assertNotIn("https://api.spotify.com", json.dumps(events, sort_keys=True))

    def test_queue_item_marked_error_on_failure(self) -> None:
        enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "track", "spotify_id": "t1", "reason": "visible", "priority": 80}]
        )

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks/t1"):
                return 500, {}, {"error": {"status": 500, "message": "boom"}}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertIn(result["status"], {"ok", "partial"})
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status, attempts, last_error FROM spotify_catalog_backfill_queue WHERE entity_type = 'track' AND spotify_id = 't1'"
            ).fetchone()
        self.assertEqual("error", str(row[0]))
        self.assertEqual(1, int(row[1] or 0))
        self.assertIn("status 500", str(row[2]))

    def test_max_requests_stops_run_with_partial_status(self) -> None:
        self._seed_source_tracks([f"t{i}" for i in range(60)])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [item for item in str(params.get("ids") or "").split(",") if item]
                return 200, {}, {"tracks": [_track_payload(track_id, f"a{track_id}") for track_id in ids]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=60,
            max_requests=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("partial", result["status"])
        self.assertTrue(result["partial"])
        self.assertEqual("max_requests", result["stop_reason"])
        self.assertGreater(result["tracks_upserted"], 0)

    def test_max_errors_stops_run_with_partial_status(self) -> None:
        self._seed_source_tracks(["t1", "t2", "t3"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden batch"}}, None
            if "/v1/tracks/" in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden single"}}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            max_errors=2,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("partial", result["status"])
        self.assertTrue(result["partial"])
        self.assertEqual("max_errors", result["stop_reason"])
        self.assertGreaterEqual(result["errors"], 2)

    def test_album_track_page_cap_stops_with_partial_status(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": "https://api.spotify.com/v1/albums/a1/tracks?offset=50"}, None
            if "/v1/albums/a1/tracks?offset=50" in url:
                raise AssertionError("Album track second page should not be requested when page cap is reached")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            max_album_tracks_pages_per_album=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertFalse(result["partial"])
        self.assertIsNone(result["stop_reason"])
        self.assertEqual(1, result["albums_fetched"])
        self.assertEqual(1, result["album_tracks_upserted"])
        self.assertEqual(1, result["album_tracklists_capped"])
        self.assertIn("album track pagination capped for a1", result["warnings"])

    def test_album_track_page_cap_is_local_and_runner_continues_to_next_album(self) -> None:
        self._seed_source_tracks(["t1", "t2"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                ids = [item for item in str(params.get("ids") or "").split(",") if item]
                payloads = []
                for track_id in ids:
                    album_id = "a1" if track_id == "t1" else "a2"
                    payloads.append(_track_payload(track_id, album_id))
                return 200, {}, {"tracks": payloads}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                ids = [item for item in str(params.get("ids") or "").split(",") if item]
                return 200, {}, {"albums": [_album_payload(album_id) for album_id in ids]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": "https://api.spotify.com/v1/albums/a1/tracks?offset=50"}, None
            if "/v1/albums/a1/tracks?offset=50" in url:
                raise AssertionError("Capped album should not request second page")
            if url.endswith("/v1/albums/a2/tracks"):
                return 200, {}, {"items": [_track_payload("a2-x", "a2")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            max_album_tracks_pages_per_album=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertFalse(result["partial"])
        self.assertEqual(2, result["albums_fetched"])
        self.assertEqual(2, result["album_tracks_upserted"])
        self.assertEqual(1, result["album_tracklists_capped"])
        self.assertIn("album track pagination capped for a1", result["warnings"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            a1_track_rows = int(connection.execute("SELECT count(*) FROM spotify_album_track WHERE spotify_album_id = ?", ("a1",)).fetchone()[0])
            a2_track_rows = int(connection.execute("SELECT count(*) FROM spotify_album_track WHERE spotify_album_id = ?", ("a2",)).fetchone()[0])
            a1_total_tracks = int(connection.execute("SELECT total_tracks FROM spotify_album_catalog WHERE spotify_album_id = ?", ("a1",)).fetchone()[0])
        self.assertEqual(1, a1_track_rows)
        self.assertEqual(1, a2_track_rows)
        self.assertLess(a1_track_rows, a1_total_tracks)

    def test_partial_result_persists_run_telemetry(self) -> None:
        self._seed_source_tracks([f"t{i}" for i in range(60)])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [item for item in str(params.get("ids") or "").split(",") if item]
                return 200, {}, {"tracks": [_track_payload(track_id, f"a{track_id}") for track_id in ids]}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=60,
            max_requests=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("partial", result["status"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status, tracks_upserted, requests_total, last_error FROM spotify_catalog_backfill_run ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual("partial", str(row[0]))
        self.assertGreater(int(row[1] or 0), 0)
        self.assertEqual(1, int(row[2] or 0))
        self.assertIn("max_requests", str(row[3] or ""))

    def test_error_status_and_last_error_stored(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            return 500, {}, {"error": {"status": 500, "message": "bad request body"}}, None

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("failed", result["status"])
        self.assertGreater(result["errors"], 0)
        with closing(sqlite3.connect(self.db_path)) as connection:
            run_row = connection.execute(
                "SELECT status, last_error FROM spotify_catalog_backfill_run ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertEqual("failed", str(run_row[0]))
        self.assertIn("status 500", str(run_row[1]))
        self.assertIn("tracks_batch", str(run_row[1]))
        self.assertIn("bad request body", str(run_row[1]))
        self.assertNotIn("token", str(run_row[1]).lower())

    def test_first_429_stops_partial_with_fake_sleeper(self) -> None:
        self._seed_source_tracks(["t1"])
        state = {"calls": 0}
        sleep_calls: list[float] = []

        def sleeper(seconds: float) -> None:
            sleep_calls.append(seconds)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                state["calls"] += 1
                if state["calls"] == 1:
                    return 429, {"Retry-After": "1"}, {}, None
                raise AssertionError("No further requests should be attempted after the first 429")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            request_delay_seconds=0.20,
            sleeper=sleeper,
            fetcher=fetcher,
        )
        self.assertEqual("partial", result["status"])
        self.assertTrue(result["partial"])
        self.assertEqual("rate_limited", result["stop_reason"])
        self.assertEqual(1, result["requests_429"])
        self.assertEqual(1, state["calls"])
        self.assertEqual(0, result["tracks_upserted"])
        self.assertGreaterEqual(result["last_retry_after_seconds"], 1.0)
        self.assertGreaterEqual(result["max_retry_after_seconds"], 1.0)
        self.assertIn("Stopped after first Spotify 429; cooldown recommended", result["warnings"])
        self.assertTrue(any(call >= 1.25 for call in sleep_calls))

    def test_track_batch_429_stops_without_retry_or_single_fallback(self) -> None:
        self._seed_source_tracks(["t1", "t2"])
        state = {"track_batch_calls": 0, "single_calls": 0}

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                state["track_batch_calls"] += 1
                if state["track_batch_calls"] == 1:
                    return 429, {"Retry-After": "1"}, {}, None
                raise AssertionError("429 should stop without retrying the batch endpoint")
            if "/v1/tracks/" in url:
                state["single_calls"] += 1
                raise AssertionError("429 should stop without single-item fallback")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            request_delay_seconds=0.20,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("partial", result["status"])
        self.assertEqual("rate_limited", result["stop_reason"])
        self.assertEqual(1, result["requests_429"])
        self.assertEqual(1, state["track_batch_calls"])
        self.assertEqual(0, state["single_calls"])
        self.assertEqual(0, result["tracks_upserted"])
        self.assertNotIn("Spotify batch track endpoint unavailable/forbidden; using single-track fallback", result["warnings"])
        self.assertIn("Stopped after first Spotify 429; cooldown recommended", result["warnings"])

    def test_429_without_retry_after_uses_fallback_cooldown_warning(self) -> None:
        self._seed_source_tracks(["t1"])
        state = {"calls": 0}
        sleep_calls: list[float] = []

        def sleeper(seconds: float) -> None:
            sleep_calls.append(seconds)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                state["calls"] += 1
                if state["calls"] == 1:
                    return 429, {}, {}, None
                raise AssertionError("No further requests should be attempted after the first 429")
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            request_delay_seconds=0.20,
            sleeper=sleeper,
            fetcher=fetcher,
        )
        self.assertEqual("partial", result["status"])
        self.assertEqual("rate_limited", result["stop_reason"])
        self.assertEqual(1, result["requests_429"])
        self.assertEqual(1, state["calls"])
        self.assertEqual(0.0, result["last_retry_after_seconds"])
        self.assertIn("429 without valid Retry-After; used fallback cooldown", result["warnings"])
        self.assertIn("Stopped after first Spotify 429; cooldown recommended", result["warnings"])
        self.assertTrue(any(call >= 5.0 for call in sleep_calls))

    def test_caller_max_429_above_one_still_stops_after_first_429(self) -> None:
        self._seed_source_tracks(["t1"])
        state = {"calls": 0}
        sleep_calls: list[float] = []

        def sleeper(seconds: float) -> None:
            sleep_calls.append(seconds)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                state["calls"] += 1
                return 429, {"Retry-After": "2"}, {}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            max_429=2,
            request_delay_seconds=0.20,
            sleeper=sleeper,
            fetcher=fetcher,
        )
        self.assertEqual("partial", result["status"])
        self.assertTrue(result["partial"])
        self.assertEqual("rate_limited", result["stop_reason"])
        self.assertTrue(result["has_more"])
        self.assertEqual("Stopped early due to rate_limited", result["last_error"])
        self.assertEqual(1, result["requests_429"])
        self.assertEqual(1, state["calls"])
        self.assertEqual(1, result["max_429"])
        self.assertGreaterEqual(result["max_retry_after_seconds"], 2.0)
        self.assertIn("Stopped after first Spotify 429; cooldown recommended", result["warnings"])
        self.assertTrue(any(call >= 2.25 for call in sleep_calls))

    def test_target_all_429_stops_before_album_and_tracklist_phases(self) -> None:
        self._seed_source_tracks(["t1"])
        calls: list[str] = []
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Album a1", "album a1"),
                ).lastrowid
            )
            source_album_id = int(
                connection.execute(
                    """
                    INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("spotify", "a1", "spotify:album:a1", "Album a1", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id,
                  release_album_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (source_album_id, release_album_id, "seed", 1.0, "accepted", 1, "seed"),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            calls.append(url)
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 429, {"Retry-After": "1"}, {}, None
            raise AssertionError(f"No later target phases should run after a 429: {url}")

        result = run_spotify_catalog_backfill(
            access_token="token",
            target="all",
            include_albums=True,
            album_tracklist_policy="all",
            request_delay_seconds=0.20,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("partial", result["status"])
        self.assertEqual("rate_limited", result["stop_reason"])
        self.assertEqual(1, result["requests_429"])
        self.assertEqual(0, result["albums_fetched"])
        self.assertEqual(0, result["album_tracklists_fetched"])
        self.assertEqual(0, result["album_tracks_upserted"])
        self.assertEqual(["https://api.spotify.com/v1/tracks"], calls)

    def test_analysis_track_map_unchanged(self) -> None:
        self._seed_source_tracks(["t1"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            raise AssertionError(url)

        run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_partial_stop_does_not_mutate_analysis_track_map(self) -> None:
        self._seed_source_tracks([f"t{i}" for i in range(60)])
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                ids = [item for item in str(params.get("ids") or "").split(",") if item]
                return 200, {}, {"tracks": [_track_payload(track_id, f"a{track_id}") for track_id in ids]}, None
            raise AssertionError(url)

        run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=60,
            max_requests=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_batch_403_triggers_single_track_fallback(self) -> None:
        self._seed_source_tracks(["t1", "t2"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden batch"}}, None
            if url.endswith("/v1/tracks/t1"):
                return 200, {}, _track_payload("t1", "a1"), None
            if url.endswith("/v1/tracks/t2"):
                return 200, {}, _track_payload("t2", "a2"), None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            max_429=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertFalse(result["partial"])
        self.assertIsNone(result["stop_reason"])
        self.assertEqual(0, result["requests_429"])
        self.assertEqual(2, result["tracks_upserted"])
        self.assertEqual(3, result["requests_total"])
        self.assertIn("Spotify batch track endpoint unavailable/forbidden; using single-track fallback", result["warnings"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            row_t1 = connection.execute(
                "SELECT duration_ms, album_id, last_status FROM spotify_track_catalog WHERE spotify_track_id = ?",
                ("t1",),
            ).fetchone()
            run_row = connection.execute(
                "SELECT last_error, warnings_json FROM spotify_catalog_backfill_run ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertEqual((123000, "a1", "ok"), row_t1)
        self.assertIsNone(run_row[0])
        self.assertIn(
            "Spotify batch track endpoint unavailable/forbidden; using single-track fallback",
            json.loads(str(run_row[1] or "[]")),
        )

    def test_track_batch_403_disables_track_batch_for_rest_of_run(self) -> None:
        track_ids = [f"t{i:02d}" for i in range(55)]
        self._seed_source_tracks(track_ids)
        calls = {"track_batch": 0, "track_single": 0}

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                calls["track_batch"] += 1
                return 403, {}, {"error": {"status": 403, "message": "forbidden batch"}}, None
            if "/v1/tracks/" in url:
                calls["track_single"] += 1
                track_id = url.rsplit("/", 1)[1]
                return 200, {}, _track_payload(track_id, f"a{track_id}"), None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=55,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        warning = "Spotify batch track endpoint unavailable/forbidden; using single-track fallback"
        self.assertEqual("ok", result["status"])
        self.assertEqual(55, result["tracks_upserted"])
        self.assertEqual(1, calls["track_batch"])
        self.assertEqual(55, calls["track_single"])
        self.assertEqual(56, result["requests_total"])
        self.assertEqual(1, result["warnings"].count(warning))

    def test_batch_403_fallback_single_failures_store_per_item_error(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden batch"}}, None
            if url.endswith("/v1/tracks/t1"):
                return 403, {}, {"error": {"status": 403, "message": "forbidden single"}}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, result["tracks_fetched"])
        self.assertEqual(1, result["errors"])
        self.assertEqual(2, result["requests_total"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT last_status, last_error FROM spotify_track_catalog WHERE spotify_track_id = ?",
                ("t1",),
            ).fetchone()
        self.assertEqual("error", str(row[0]))
        self.assertIn("tracks_single_fallback", str(row[1]))

    def test_batch_403_fallback_does_not_mutate_analysis_track_map(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden batch"}}, None
            if url.endswith("/v1/tracks/t1"):
                return 200, {}, _track_payload("t1", "a1"), None
            raise AssertionError(url)

        run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_album_batch_403_triggers_single_album_fallback(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden album batch"}}, None
            if url.endswith("/v1/albums/a1"):
                return 200, {}, _album_payload("a1"), None
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1"), _track_payload("a1-y", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["albums_fetched"])
        self.assertEqual(2, result["album_tracks_upserted"])
        self.assertIn("Spotify batch album endpoint unavailable/forbidden; using single-album fallback", result["warnings"])
        self.assertEqual(4, result["requests_total"])  # tracks batch + album batch + album single + album tracks

        with closing(sqlite3.connect(self.db_path)) as connection:
            album_row = connection.execute(
                "SELECT name, album_type, last_status FROM spotify_album_catalog WHERE spotify_album_id = ?",
                ("a1",),
            ).fetchone()
            run_row = connection.execute(
                "SELECT last_error, warnings_json FROM spotify_catalog_backfill_run ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertEqual(("Album a1", "album", "ok"), album_row)
        self.assertIsNone(run_row[0])
        self.assertIn(
            "Spotify batch album endpoint unavailable/forbidden; using single-album fallback",
            json.loads(str(run_row[1] or "[]")),
        )

    def test_album_batch_403_disables_album_batch_for_rest_of_run(self) -> None:
        track_ids = [f"t{i:02d}" for i in range(25)]
        self._seed_source_tracks(track_ids)
        calls = {"track_batch": 0, "album_batch": 0, "album_single": 0}

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                calls["track_batch"] += 1
                ids = [item for item in str(params.get("ids") or "").split(",") if item]
                return 200, {}, {"tracks": [_track_payload(track_id, f"a{track_id}") for track_id in ids]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                calls["album_batch"] += 1
                return 403, {}, {"error": {"status": 403, "message": "forbidden album batch"}}, None
            if "/v1/albums/" in url:
                calls["album_single"] += 1
                album_id = url.rsplit("/", 1)[1]
                return 200, {}, _album_payload(album_id), None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            album_tracklist_policy="none",
            limit=25,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        warning = "Spotify batch album endpoint unavailable/forbidden; using single-album fallback"
        self.assertEqual("ok", result["status"])
        self.assertEqual(25, result["albums_fetched"])
        self.assertEqual(0, result["album_tracks_upserted"])
        self.assertEqual(1, calls["track_batch"])
        self.assertEqual(1, calls["album_batch"])
        self.assertEqual(25, calls["album_single"])
        self.assertEqual(27, result["requests_total"])
        self.assertEqual(1, result["warnings"].count(warning))

    def test_album_single_fallback_upserts_payloads_before_max_request_stop(self) -> None:
        track_ids = [f"t{i:02d}" for i in range(25)]
        self._seed_source_tracks(track_ids)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                ids = [item for item in str(params.get("ids") or "").split(",") if item]
                return 200, {}, {"tracks": [_track_payload(track_id, f"a{track_id}") for track_id in ids]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden album batch"}}, None
            if "/v1/albums/" in url:
                album_id = url.rsplit("/", 1)[1]
                return 200, {}, _album_payload(album_id), None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            album_tracklist_policy="none",
            limit=25,
            max_requests=25,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        self.assertEqual("partial", result["status"])
        self.assertEqual("max_requests", result["stop_reason"])
        self.assertEqual(25, result["requests_total"])
        self.assertEqual(23, result["albums_fetched"])
        self.assertEqual(0, result["album_tracks_upserted"])

    def test_album_batch_403_single_album_failure_stores_per_album_error(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden album batch"}}, None
            if url.endswith("/v1/albums/a1"):
                return 403, {}, {"error": {"status": 403, "message": "forbidden album single"}}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(0, result["albums_fetched"])
        self.assertEqual(1, result["errors"])
        self.assertEqual(3, result["requests_total"])  # tracks batch + album batch + album single

        with closing(sqlite3.connect(self.db_path)) as connection:
            album_row = connection.execute(
                "SELECT last_status, last_error FROM spotify_album_catalog WHERE spotify_album_id = ?",
                ("a1",),
            ).fetchone()
        self.assertEqual("error", str(album_row[0]))
        self.assertIn("album_single_fallback", str(album_row[1]))

    def test_album_batch_403_fallback_does_not_mutate_analysis_track_map(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden album batch"}}, None
            if url.endswith("/v1/albums/a1"):
                return 200, {}, _album_payload("a1"), None
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-x", "a1")], "next": None}, None
            raise AssertionError(url)

        run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_catalog_backfill_unauthenticated_returns_401_shape(self) -> None:
        with patch(
            "backend.app.main._require_local_data_session",
            side_effect=HTTPException(status_code=401, detail="Not authenticated with Spotify."),
        ), patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.post("/debug/spotify/catalog-backfill", json={"limit": 25, "market": "US", "include_albums": True})
        self.assertEqual(401, response.status_code)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertEqual("unauthenticated", body["status"])
        self.assertEqual("spotify_not_authenticated", body["error"]["code"])
        self.assertEqual("Not authenticated with Spotify.", body["error"]["message"])
        run_mock.assert_not_called()

    def test_catalog_backfill_unauthenticated_no_run_row_inserted(self) -> None:
        with patch(
            "backend.app.main._require_local_data_session",
            side_effect=HTTPException(status_code=401, detail="Not authenticated with Spotify."),
        ):
            client = TestClient(app)
            response = client.post("/debug/spotify/catalog-backfill", json={"limit": 25, "market": "US", "include_albums": True})
        self.assertEqual(401, response.status_code)
        with closing(sqlite3.connect(self.db_path)) as connection:
            run_count = int(connection.execute("SELECT count(*) FROM spotify_catalog_backfill_run").fetchone()[0])
        self.assertEqual(0, run_count)

    def test_catalog_backfill_unauthenticated_no_spotify_request_made(self) -> None:
        with patch(
            "backend.app.main._require_local_data_session",
            side_effect=HTTPException(status_code=401, detail="Not authenticated with Spotify."),
        ), patch("backend.app.main.refresh_access_token_if_needed") as refresh_mock, patch(
            "backend.app.main.run_spotify_catalog_backfill"
        ) as run_mock:
            client = TestClient(app)
            response = client.post("/debug/spotify/catalog-backfill", json={"limit": 25, "market": "US", "include_albums": True})
        self.assertEqual(401, response.status_code)
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_catalog_backfill_endpoint_passes_target_parameter(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed",
            return_value={"access_token": "token"},
        ), patch(
            "backend.app.main.run_spotify_catalog_backfill",
            return_value={"status": "ok", "target": "tracks"},
        ) as run_mock:
            client = TestClient(app)
            response = client.post(
                "/debug/spotify/catalog-backfill",
                json={"run_mode": "metadata_only", "target": "tracks", "include_albums": True},
            )
        self.assertEqual(200, response.status_code)
        self.assertTrue(response.json()["ok"])
        run_mock.assert_called_once()
        self.assertEqual("tracks", run_mock.call_args.kwargs["target"])
        self.assertEqual(2.0, run_mock.call_args.kwargs["request_delay_seconds"])

    def test_catalog_backfill_endpoint_respects_explicit_request_delay(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed",
            return_value={"access_token": "token"},
        ), patch(
            "backend.app.main.run_spotify_catalog_backfill",
            return_value={"status": "ok"},
        ) as run_mock:
            client = TestClient(app)
            response = client.post(
                "/debug/spotify/catalog-backfill",
                json={"request_delay_seconds": 0.75},
            )
        self.assertEqual(200, response.status_code)
        self.assertTrue(response.json()["ok"])
        run_mock.assert_called_once()
        self.assertEqual(0.75, run_mock.call_args.kwargs["request_delay_seconds"])

    def test_catalog_backfill_runs_endpoint_empty(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.get("/debug/spotify/catalog-backfill/runs")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual([], body["items"])
        self.assertEqual(0, body["total"])

    def test_catalog_backfill_queue_endpoint_empty(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.get("/debug/spotify/catalog-backfill/queue")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual([], body["items"])
        self.assertEqual(0, body["total"])
        self.assertEqual({"pending": 0, "done": 0, "error": 0}, body["counts"])

    def test_catalog_backfill_queue_list_filters_by_status(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("track", "t-p", "pending", 10, "pending", "2026-04-27T12:00:00Z", 0),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "a-d", "done", 5, "done", "2026-04-27T12:01:00Z", 1),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("track", "t-e", "error", 8, "error", "2026-04-27T12:02:00Z", 2, "boom"),
            )
            connection.commit()

        pending_payload = list_spotify_catalog_backfill_queue(status_filter="pending", limit=50, offset=0)
        self.assertTrue(pending_payload["ok"])
        self.assertEqual(1, pending_payload["total"])
        self.assertEqual(1, len(pending_payload["items"]))
        self.assertEqual("pending", pending_payload["items"][0]["status"])

        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.get("/debug/spotify/catalog-backfill/queue?status=error&limit=50&offset=0")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(1, body["total"])
        self.assertEqual(1, len(body["items"]))
        self.assertEqual("error", body["items"][0]["status"])

    def test_catalog_backfill_queue_counts_by_status(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("track", "t1", 1, "pending", "2026-04-27T11:00:00Z", 0),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("track", "t2", 1, "pending", "2026-04-27T11:01:00Z", 0),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("album", "a1", 1, "done", "2026-04-27T11:02:00Z", 1),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts, last_error) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("album", "a2", 1, "error", "2026-04-27T11:03:00Z", 2, "err"),
            )
            connection.commit()

        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.get("/debug/spotify/catalog-backfill/queue")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual({"pending": 2, "done": 1, "error": 1}, body["counts"])

    def test_catalog_backfill_queue_endpoint_does_not_call_spotify(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.get("/debug/spotify/catalog-backfill/queue")
        self.assertEqual(200, response.status_code)
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_queue_resolution_evidence_report_classifies_existing_queue(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Shared Track", "shared track"),
                ).lastrowid
            )
            for spotify_track_id in ("ambig-1", "ambig-2"):
                source_track_id = int(
                    connection.execute(
                        """
                        INSERT INTO source_track (
                          source_name, external_id, external_uri, source_name_raw, raw_payload_json
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        ("spotify", spotify_track_id, f"spotify:track:{spotify_track_id}", "Shared Track", "{}"),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'seed', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_track_id, release_track_id),
                )
                connection.execute(
                    """
                    INSERT INTO spotify_track_catalog (
                      spotify_track_id, name, duration_ms, explicit, disc_number, track_number,
                      album_id, artists_json, raw_json, market, fetched_at, last_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        spotify_track_id,
                        "Shared Track",
                        123000,
                        0,
                        1,
                        1,
                        "album-1",
                        '[{"name":"Artist 1"}]',
                        json.dumps({"external_ids": {"isrc": "US123"}}),
                        "US",
                        "2026-04-27T12:00:00Z",
                        "ok",
                    ),
                )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, album_type, release_date, release_date_precision,
                  total_tracks, artists_json, images_json, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "album-1",
                    "Album 1",
                    "album",
                    "2026-01-01",
                    "day",
                    3,
                    '[{"name":"Artist 1"}]',
                    '[{"url":"https://image.test/1.jpg"}]',
                    json.dumps({"copyrights": [{"text": "C 2026 Label"}], "label": "Label"}),
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, album_type, release_date, release_date_precision,
                  total_tracks, artists_json, images_json, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "album-2",
                    "Album 2",
                    "album",
                    "2026-01-01",
                    "day",
                    1,
                    '[{"name":"Artist 1"}]',
                    '[{"url":"https://image.test/2.jpg"}]',
                    json.dumps({"copyrights": [{"text": "C 2026 Label"}], "label": "Label"}),
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, disc_number, track_number, name,
                  duration_ms, artists_json, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "album-1",
                    "sibling-1",
                    1,
                    2,
                    "Sibling Track",
                    120000,
                    '[{"name":"Artist 1"}]',
                    "{}",
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, disc_number, track_number, name,
                  duration_ms, artists_json, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "album-1",
                    "sibling-missing-metadata",
                    1,
                    3,
                    "Sibling Missing Metadata",
                    121000,
                    '[{"name":"Artist 1"}]',
                    "{}",
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, explicit, disc_number, track_number,
                  album_id, artists_json, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "sibling-1",
                    "Sibling Track",
                    120000,
                    0,
                    1,
                    2,
                    "album-1",
                    '[{"name":"Artist 1"}]',
                    json.dumps({"external_ids": {"isrc": "US456"}}),
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            queue_rows = [
                ("track", "ambig-1", "identity_metadata", 90, "pending", "2026-04-27T12:00:00Z", 0, None),
                ("album", "album-1", "tracklist_completion", 70, "pending", "2026-04-27T12:01:00Z", 0, None),
                ("track", "sibling-1", "manual_priority", 50, "pending", "2026-04-27T12:02:00Z", 0, None),
                ("album", "generic-album", "full_backfill", 10, "pending", "2026-04-27T12:03:00Z", 0, None),
                ("track", "broken-track", "manual_priority", 20, "error", "2026-04-27T12:04:00Z", 2, "boom"),
                ("album", "legacy-visible", "visible_incomplete", 5, "pending", "2026-04-27T12:05:00Z", 0, None),
            ]
            connection.executemany(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                queue_rows,
            )
            connection.commit()

        report = inspect_spotify_catalog_queue_resolution_evidence()

        self.assertTrue(report["ok"])
        self.assertEqual("read_only", report["mode"])
        self.assertEqual(6, report["queue_snapshot"]["total_queued_items"])
        self.assertEqual({"pending": 5, "error": 1}, report["queue_snapshot"]["counts_by_status"])
        self.assertEqual(
            {
                "directly_relevant": 2,
                "possibly_relevant": 1,
                "generic_catalog_backfill": 1,
                "stale_or_blocked": 1,
                "unknown": 1,
            },
            report["resolution_relevance"]["bucket_counts"],
        )
        self.assertEqual(2, report["resolution_relevance"]["bucket_counts_by_status"]["directly_relevant_pending"])
        self.assertEqual(1, report["resolution_relevance"]["bucket_counts_by_status"]["unknown_pending"])
        self.assertEqual(
            {"legacy_album_lookup_visible_incomplete": 1},
            report["resolution_relevance"]["unknown_reason_counts"],
        )
        self.assertEqual(1, report["evidence_coverage_hints"]["queued_candidate_albums"])
        self.assertEqual(1, report["evidence_coverage_hints"]["queued_sibling_tracks"])
        self.assertEqual(1, report["evidence_coverage_hints"]["queued_ambiguous_source_tracks"])
        self.assertEqual(1, report["evidence_coverage_hints"]["album_tracklist_gaps"])
        delta_counts = report["resolution_evidence_delta"]["counts"]
        self.assertEqual(1, delta_counts["ambiguous_source_tracks_missing_from_queue"])
        self.assertEqual(1, delta_counts["sibling_tracks_missing_from_queue"])
        self.assertEqual(0, delta_counts["sibling_tracks_already_present_locally_but_not_queued"])
        self.assertEqual(1, delta_counts["sibling_tracks_requiring_metadata"])
        self.assertEqual(1, delta_counts["candidate_albums_queued_but_missing_tracklists"])
        self.assertEqual(1, delta_counts["tracklists_needed_before_sibling_tracks_can_be_enumerated"])
        self.assertEqual("preserve_current_queue", report["safety_recommendation"]["action"])
        self.assertIn(
            "complete_candidate_album_tracklists_before_sibling_track_collection",
            report["safety_recommendation"]["recommended_steps"],
        )
        self.assertIn(
            "let_directly_relevant_pending_candidate_albums_or_tracklists_complete_first",
            report["safety_recommendation"]["recommended_steps"],
        )
        self.assertEqual(6, len(report["queue_items"]))
        plan = report["dry_run_resolution_evidence_plan"]
        self.assertEqual("dry_run", plan["mode"])
        self.assertEqual("none", plan["performed_action"])
        self.assertEqual("resolution_evidence", plan["suggested_reason"])
        self.assertEqual(1, plan["counts_by_plan_status"]["already_queued_pending"])
        self.assertEqual(2, plan["counts_by_plan_status"]["should_append_later"])
        self.assertEqual(1, plan["counts_by_plan_status"]["tracklist_pending"])
        self.assertEqual(
            {
                "ambiguity_group_count": 1,
                "candidate_album_count": 1,
                "candidate_album_tracklist_missing_count": 1,
                "broad_incomplete_album_tracklist_count": 2,
                "actual_sibling_track_count": 2,
            },
            plan["source_set_counts"],
        )
        self.assertEqual("album_tracklist", plan["items"][0]["planned_target"])
        self.assertEqual("already_queued_pending", plan["items"][0]["plan_status"])
        self.assertEqual("album-1", plan["items"][0]["spotify_id"])
        self.assertEqual(1, len(plan["candidate_album_tracklist_items"]))
        self.assertEqual(1, len(plan["actual_sibling_track_items"]))
        self.assertEqual(1, len(plan["blocked_sibling_collection_prerequisites"]))
        self.assertLess(
            [item["planned_target"] for item in plan["items"]].index("album_tracklist"),
            [item["planned_target"] for item in plan["items"]].index("track_metadata"),
        )
        self.assertEqual(
            1,
            sum(
                1
                for item in plan["items"]
                if item["planned_target"] == "album_tracklist"
                and item["spotify_id"] == "album-1"
                and item["plan_status"] == "already_queued_pending"
            ),
        )
        self.assertIn(
            {
                "planned_target": "album_tracklist",
                "entity_type": "album",
                "spotify_id": "album-1",
                "parent_album_id": None,
                "plan_status": "tracklist_pending",
                "suggested_reason": "resolution_evidence",
                "rationale": "candidate album tracklist must be fetched before sibling tracks can be enumerated",
            },
            plan["blocked_sibling_collection_prerequisites"],
        )
        for item in plan["blocked_sibling_collection_prerequisites"]:
            self.assertNotEqual("track", item["entity_type"])
        for item in plan["actual_sibling_track_items"]:
            self.assertEqual("track", item["entity_type"])
            self.assertNotEqual(item["spotify_id"], item["parent_album_id"])
        self.assertNotIn("album-2", {item["spotify_id"] for item in plan["items"]})
        self.assertEqual(2, len(report["samples"]["directly_relevant_pending_items"]))
        self.assertIn(
            "legacy_album_lookup_visible_incomplete",
            report["samples"]["unknown_pending_items_by_unknown_reason"],
        )
        self.assertEqual("none", report["safety_recommendation"]["performed_action"])

    def test_queue_resolution_evidence_report_does_not_mutate_queue_or_call_spotify(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("track", "t1", "manual_priority", 10, "pending", "2026-04-27T12:00:00Z", 0),
            )
            before = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status, requested_at, attempts, last_error
                FROM spotify_catalog_backfill_queue
                ORDER BY id
                """
            ).fetchall()
            connection.commit()

        with patch("backend.app.spotify_catalog_backfill.httpx.Client") as client_mock:
            _ = inspect_spotify_catalog_queue_resolution_evidence()
        client_mock.assert_not_called()

        with closing(sqlite3.connect(self.db_path)) as connection:
            after = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status, requested_at, attempts, last_error
                FROM spotify_catalog_backfill_queue
                ORDER BY id
                """
            ).fetchall()
        self.assertEqual(before, after)

    def test_queue_snapshot_export_writes_classified_rows_without_mutating_queue(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "legacy-visible", "visible_incomplete", 10, "pending", "2026-04-27T12:00:00Z", 0),
            )
            before = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status, requested_at, attempts, last_error
                FROM spotify_catalog_backfill_queue
                ORDER BY id
                """
            ).fetchall()
            connection.commit()

        with patch("backend.app.spotify_catalog_backfill.httpx.Client") as client_mock:
            report = inspect_spotify_catalog_queue_resolution_evidence()
            snapshot = build_queue_snapshot_export(report, timestamp="2026-05-08T12:00:00Z")
            output_path = Path(self._tmp_dir.name) / "queue-snapshot.json"
            written_path = write_queue_snapshot_export(report, output_path)
        client_mock.assert_not_called()

        self.assertEqual(output_path, written_path)
        self.assertEqual("2026-05-08T12:00:00Z", snapshot["timestamp"])
        self.assertEqual(1, snapshot["total_queued_items"])
        exported = json.loads(output_path.read_text(encoding="utf-8"))
        self.assertEqual(1, exported["total_queued_items"])
        self.assertEqual("legacy-visible", exported["queue_rows"][0]["spotify_id"])
        self.assertEqual("unknown", exported["queue_rows"][0]["relevance_bucket"])
        self.assertEqual("legacy_album_lookup_visible_incomplete", exported["queue_rows"][0]["unknown_reason"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            after = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status, requested_at, attempts, last_error
                FROM spotify_catalog_backfill_queue
                ORDER BY id
                """
            ).fetchall()
        self.assertEqual(before, after)

    def test_album_lookup_visible_incomplete_is_generic_not_unknown(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "legacy-album", "album_lookup_visible_incomplete", 80, "pending", "2026-04-27T12:00:00Z", 0),
            )
            before = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status, requested_at, attempts, last_error
                FROM spotify_catalog_backfill_queue
                ORDER BY id
                """
            ).fetchall()
            connection.commit()

        with patch("backend.app.spotify_catalog_backfill.httpx.Client") as client_mock:
            report = inspect_spotify_catalog_queue_resolution_evidence()
            unknown_report = build_unknown_pending_queue_items_report(report, summary_only=True)
        client_mock.assert_not_called()

        self.assertEqual(
            {
                "directly_relevant": 0,
                "possibly_relevant": 0,
                "generic_catalog_backfill": 1,
                "stale_or_blocked": 0,
                "unknown": 0,
            },
            report["resolution_relevance"]["bucket_counts"],
        )
        self.assertEqual(1, report["resolution_relevance"]["bucket_counts_by_status"]["generic_catalog_backfill_pending"])
        self.assertEqual(0, report["resolution_relevance"]["bucket_counts_by_status"]["unknown_pending"])
        self.assertEqual({}, report["resolution_relevance"]["unknown_reason_counts"])
        self.assertEqual("clear_and_replace_later", report["safety_recommendation"]["action"])
        self.assertEqual(0, unknown_report["unknown_pending_queue_item_count"])
        self.assertEqual([], unknown_report["sample_items"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            after = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status, requested_at, attempts, last_error
                FROM spotify_catalog_backfill_queue
                ORDER BY id
                """
            ).fetchall()
        self.assertEqual(before, after)

    def test_unknown_pending_queue_items_report_filters_unknown_pending_rows(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            rows = [
                ("album", "legacy-visible", "visible_incomplete", 10, "pending", "2026-04-27T12:00:00Z", 0, None),
                ("track", "weird-track", "weird_reason", 20, "pending", "2026-04-27T12:01:00Z", 1, "later"),
                ("album", "done-visible", "visible_incomplete", 10, "done", "2026-04-27T12:02:00Z", 0, None),
            ]
            connection.executemany(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            before = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status, requested_at, attempts, last_error
                FROM spotify_catalog_backfill_queue
                ORDER BY id
                """
            ).fetchall()
            connection.commit()

        with patch("backend.app.spotify_catalog_backfill.httpx.Client") as client_mock:
            report = inspect_spotify_catalog_queue_resolution_evidence()
            unknown_report = build_unknown_pending_queue_items_report(report)
            summary = build_unknown_pending_queue_items_report(report, summary_only=True, sample_limit=1)
        client_mock.assert_not_called()

        self.assertEqual("read_only", unknown_report["mode"])
        self.assertEqual("none", unknown_report["performed_action"])
        self.assertEqual(2, unknown_report["unknown_pending_queue_item_count"])
        self.assertEqual({"visible_incomplete": 1, "weird_reason": 1}, unknown_report["counts_by_reason"])
        self.assertEqual({"album": 1, "track": 1}, unknown_report["counts_by_entity_type"])
        self.assertEqual({"pending": 2}, unknown_report["counts_by_status"])
        self.assertEqual(
            {"legacy_album_lookup_visible_incomplete": 1, "not_ambiguous_source_track": 1},
            unknown_report["counts_by_unknown_reason"],
        )
        self.assertEqual(["legacy-visible", "weird-track"], [item["spotify_id"] for item in unknown_report["queue_items"]])
        self.assertEqual("later", unknown_report["queue_items"][1]["last_error"])
        self.assertEqual(1, summary["sample_limit"])
        self.assertEqual(1, len(summary["sample_items"]))
        self.assertNotIn("queue_items", summary)

        with closing(sqlite3.connect(self.db_path)) as connection:
            after = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status, requested_at, attempts, last_error
                FROM spotify_catalog_backfill_queue
                ORDER BY id
                """
            ).fetchall()
        self.assertEqual(before, after)

    def test_resolution_evidence_summary_only_omits_verbose_items(self) -> None:
        report = {
            "ok": True,
            "resolution_relevance": {
                "bucket_counts_by_status": {
                    "unknown_pending": 2,
                },
            },
            "resolution_evidence_delta": {
                "counts": {
                    "candidate_albums_queued_but_missing_tracklists": 3,
                    "sibling_tracks_missing_from_queue": 4,
                    "sibling_tracks_requiring_metadata": 5,
                    "tracklists_needed_before_sibling_tracks_can_be_enumerated": 6,
                },
                "samples": {
                    "sibling_tracks_requiring_metadata": [{"spotify_id": "sample"}],
                },
            },
            "dry_run_resolution_evidence_plan": {
                "mode": "dry_run",
                "performed_action": "none",
                "source_set_counts": {
                    "candidate_album_tracklist_missing_count": 7,
                },
                "counts_by_plan_status": {
                    "should_append_later": 8,
                },
                "items": [{"spotify_id": "verbose"}],
            },
            "safety_recommendation": {
                "action": "needs_manual_review",
                "rationale": "review",
                "counts": {
                    "unknown_pending": 2,
                },
            },
            "queue_items": [{"spotify_id": "verbose"}],
            "samples": {
                "unknown_pending_items_by_unknown_reason": {
                    "pending_but_not_resolution_related": [{"spotify_id": "verbose"}],
                },
            },
        }

        summary = build_summary_only_report(report)

        self.assertEqual(True, summary["ok"])
        self.assertEqual("none", summary["performed_action"])
        self.assertEqual("needs_manual_review", summary["safety_recommendation"]["action"])
        plan_summary = summary["dry_run_resolution_evidence_plan"]
        self.assertEqual(7, plan_summary["missing_candidate_album_tracklists_count"])
        self.assertEqual(5, plan_summary["missing_sibling_track_evidence_count"])
        self.assertNotIn("queue_items", summary)
        self.assertNotIn("samples", summary)
        self.assertNotIn("items", plan_summary)

    def test_album_display_diagnostic_summary_only_omits_verbose_samples(self) -> None:
        report = {
            "ok": True,
            "mode": "read_only",
            "counts": {
                "total_rows": 3597,
                "rows_with_source_album_display_info": 1571,
                "rows_with_source_album_display_after_embedded_fallback": 1571,
                "rows_with_no_spotify_album_evidence": 2026,
                "rows_with_album_spotify_id_but_no_local_album_name": 0,
                "rows_with_no_album_spotify_id": 2026,
                "rows_with_release_album_display_info": 3597,
            },
            "samples": {
                "no_spotify_album_evidence": [{"spotify_track_id": "verbose"}],
            },
            "notes": ["verbose"],
        }

        summary = build_album_display_diagnostic_summary(report)

        self.assertEqual(
            {
                "ok",
                "total_rows",
                "rows_with_source_album_display_info",
                "rows_with_source_album_display_after_embedded_fallback",
                "rows_with_no_spotify_album_evidence",
                "rows_with_album_spotify_id_but_no_local_album_name",
                "rows_with_no_album_spotify_id",
            },
            set(summary.keys()),
        )
        self.assertEqual(3597, summary["total_rows"])
        self.assertEqual(2026, summary["rows_with_no_spotify_album_evidence"])
        self.assertNotIn("samples", summary)
        self.assertNotIn("notes", summary)

    def test_album_display_diagnostic_summary_only_cli_output_path(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        env = {**os.environ, "SQLITE_DB_PATH": str(self.db_path)}

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "backend.scripts.inspect_spotify_catalog_queue",
                "--source-release-album-display-diagnostic",
                "--album-display-diagnostic-summary-only",
            ],
            cwd=repo_root,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )

        payload = json.loads(result.stdout)
        self.assertEqual(True, payload["ok"])
        self.assertEqual(0, payload["total_rows"])
        self.assertEqual(0, payload["rows_with_source_album_display_info"])
        self.assertEqual(0, payload["rows_with_source_album_display_after_embedded_fallback"])
        self.assertEqual(0, payload["rows_with_no_spotify_album_evidence"])
        self.assertEqual(0, payload["rows_with_album_spotify_id_but_no_local_album_name"])
        self.assertEqual(0, payload["rows_with_no_album_spotify_id"])
        self.assertNotIn("samples", payload)

        error_result = subprocess.run(
            [
                sys.executable,
                "-m",
                "backend.scripts.inspect_spotify_catalog_queue",
                "--album-display-diagnostic-summary-only",
            ],
            cwd=repo_root,
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(0, error_result.returncode)
        self.assertIn(
            "--album-display-diagnostic-summary-only requires --source-release-album-display-diagnostic",
            error_result.stderr,
        )

    def test_append_resolution_evidence_sibling_cli_dry_run_is_accepted_without_apply(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        env = {**os.environ, "SQLITE_DB_PATH": str(self.db_path)}

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "backend.scripts.inspect_spotify_catalog_queue",
                "--append-resolution-evidence-sibling-tracks",
            ],
            cwd=repo_root,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )

        payload = json.loads(result.stdout)
        append_result = payload["append_resolution_evidence_sibling_tracks"]
        self.assertEqual("dry_run", append_result["mode"])
        self.assertEqual("none", append_result["performed_action"])

    def test_append_resolution_evidence_candidate_tracklists_cli_dry_run_is_accepted_without_apply(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        env = {**os.environ, "SQLITE_DB_PATH": str(self.db_path)}

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "backend.scripts.inspect_spotify_catalog_queue",
                "--append-resolution-evidence-candidate-tracklists",
            ],
            cwd=repo_root,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )

        payload = json.loads(result.stdout)
        append_result = payload["append_resolution_evidence_candidate_tracklists"]
        self.assertEqual("dry_run", append_result["mode"])
        self.assertEqual("none", append_result["performed_action"])

    def test_append_resolution_evidence_sibling_cli_summary_only_reports_dry_run(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        env = {**os.environ, "SQLITE_DB_PATH": str(self.db_path)}

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "backend.scripts.inspect_spotify_catalog_queue",
                "--append-resolution-evidence-sibling-tracks",
                "--summary-only",
            ],
            cwd=repo_root,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )

        payload = json.loads(result.stdout)
        self.assertEqual("none", payload["performed_action"])
        append_summary = payload["append_resolution_evidence_sibling_tracks"]
        self.assertEqual("dry_run", append_summary["mode"])
        self.assertEqual("none", append_summary["performed_action"])
        self.assertNotIn("selected_items", append_summary)
        self.assertIn("appendability_diagnostic", append_summary)

    def test_append_resolution_evidence_cli_dry_run_does_not_mutate_queue(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("track", "existing-track", "manual_priority", 50, "pending", "2026-04-27T12:00:00Z", 0),
            )
            before = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status, requested_at, attempts, last_error
                FROM spotify_catalog_backfill_queue
                ORDER BY id
                """
            ).fetchall()
            connection.commit()

        repo_root = Path(__file__).resolve().parents[2]
        env = {**os.environ, "SQLITE_DB_PATH": str(self.db_path)}
        subprocess.run(
            [
                sys.executable,
                "-m",
                "backend.scripts.inspect_spotify_catalog_queue",
                "--append-resolution-evidence-sibling-tracks",
                "--summary-only",
            ],
            cwd=repo_root,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )

        with closing(sqlite3.connect(self.db_path)) as connection:
            after = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status, requested_at, attempts, last_error
                FROM spotify_catalog_backfill_queue
                ORDER BY id
                """
            ).fetchall()
        self.assertEqual(before, after)

    def test_append_resolution_evidence_candidate_tracklists_is_dry_run_then_idempotent_apply(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "already-pending", "tracklist_completion", 70, "pending", "2026-04-27T12:00:00Z", 0),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "already-done", "tracklist_completion", 70, "done", "2026-04-27T12:01:00Z", 0),
            )
            before = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status, attempts
                FROM spotify_catalog_backfill_queue
                ORDER BY id
                """
            ).fetchall()
            connection.commit()

        report = {
            "dry_run_resolution_evidence_plan": {
                "candidate_album_tracklist_items": [
                    {
                        "planned_target": "album_tracklist",
                        "entity_type": "album",
                        "spotify_id": "already-pending",
                        "plan_status": "already_queued_pending",
                        "suggested_reason": "resolution_evidence",
                    },
                    {
                        "planned_target": "album_tracklist",
                        "entity_type": "album",
                        "spotify_id": "already-done",
                        "plan_status": "already_queued_done",
                        "suggested_reason": "resolution_evidence",
                    },
                    {
                        "planned_target": "album_tracklist",
                        "entity_type": "album",
                        "spotify_id": "append-album",
                        "plan_status": "should_append_later",
                        "suggested_reason": "resolution_evidence",
                        "rationale": "candidate album tracklist is needed",
                    },
                ],
                "actual_sibling_track_items": [
                    {
                        "planned_target": "track_metadata",
                        "entity_type": "track",
                        "spotify_id": "sibling-track",
                        "parent_album_id": "append-album",
                        "plan_status": "should_append_later",
                        "suggested_reason": "resolution_evidence",
                    }
                ],
                "blocked_sibling_collection_prerequisites": [
                    {
                        "planned_target": "album_tracklist",
                        "entity_type": "album",
                        "spotify_id": "blocked-album",
                        "plan_status": "blocked_until_tracklist_exists",
                        "suggested_reason": "resolution_evidence",
                    }
                ],
            }
        }

        with patch("backend.app.spotify_catalog_backfill.httpx.Client") as client_mock:
            dry_run = append_resolution_evidence_candidate_tracklists_from_report(report=report, apply=False)
        client_mock.assert_not_called()
        self.assertEqual("dry_run", dry_run["mode"])
        self.assertEqual("none", dry_run["performed_action"])
        self.assertEqual(1, dry_run["selected_count"])
        self.assertEqual("append-album", dry_run["selected_items"][0]["spotify_id"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_dry_run = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status, attempts
                FROM spotify_catalog_backfill_queue
                ORDER BY id
                """
            ).fetchall()
        self.assertEqual(before, after_dry_run)

        with patch("backend.app.spotify_catalog_backfill.httpx.Client") as client_mock:
            applied = append_resolution_evidence_candidate_tracklists_from_report(report=report, apply=True)
        client_mock.assert_not_called()
        self.assertEqual("apply", applied["mode"])
        self.assertEqual("inserted_queue_rows", applied["performed_action"])
        self.assertEqual(1, applied["inserted"])
        self.assertEqual(0, applied["already_existing"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            rows = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status, attempts
                FROM spotify_catalog_backfill_queue
                ORDER BY spotify_id
                """
            ).fetchall()
        self.assertIn(("album", "append-album", "resolution_evidence", 80, "pending", 0), rows)
        self.assertNotIn(("track", "sibling-track", "resolution_evidence", 80, "pending", 0), rows)
        self.assertNotIn(("album", "blocked-album", "resolution_evidence", 80, "pending", 0), rows)
        self.assertIn(("album", "already-done", "tracklist_completion", 70, "done", 0), rows)
        self.assertIn(("album", "already-pending", "tracklist_completion", 70, "pending", 0), rows)

        second_apply = append_resolution_evidence_candidate_tracklists_from_report(report=report, apply=True)
        self.assertEqual(0, second_apply["inserted"])
        self.assertEqual(1, second_apply["already_existing"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            append_count = int(
                connection.execute(
                    """
                    SELECT count(*)
                    FROM spotify_catalog_backfill_queue
                    WHERE entity_type = 'album'
                      AND spotify_id = 'append-album'
                      AND reason = 'resolution_evidence'
                    """
                ).fetchone()[0]
            )
        self.assertEqual(1, append_count)

    def test_append_resolution_evidence_sibling_tracks_is_dry_run_then_idempotent_apply(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("track", "already-existing-track", "identity_metadata", 70, "done", "2026-04-27T12:00:00Z", 0),
            )
            before = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status, attempts
                FROM spotify_catalog_backfill_queue
                ORDER BY id
                """
            ).fetchall()
            connection.commit()

        report = {
            "dry_run_resolution_evidence_plan": {
                "candidate_album_tracklist_items": [
                    {
                        "planned_target": "album_tracklist",
                        "entity_type": "album",
                        "spotify_id": "album-should-not-insert",
                        "plan_status": "should_append_later",
                        "suggested_reason": "resolution_evidence",
                    }
                ],
                "actual_sibling_track_items": [
                    {
                        "planned_target": "track_metadata",
                        "entity_type": "track",
                        "spotify_id": "sibling-new",
                        "parent_album_id": "candidate-album",
                        "plan_status": "should_append_later",
                        "suggested_reason": "resolution_evidence",
                        "rationale": "sibling track from candidate album needs metadata",
                    },
                    {
                        "planned_target": "track_metadata",
                        "entity_type": "track",
                        "spotify_id": "already-existing-track",
                        "parent_album_id": "candidate-album",
                        "plan_status": "should_append_later",
                        "suggested_reason": "resolution_evidence",
                    },
                    {
                        "planned_target": "track_metadata",
                        "entity_type": "track",
                        "spotify_id": "already-queued-pending-track",
                        "parent_album_id": "candidate-album",
                        "plan_status": "already_queued_pending",
                        "suggested_reason": "resolution_evidence",
                    },
                    {
                        "planned_target": "album_tracklist",
                        "entity_type": "album",
                        "spotify_id": "wrong-section",
                        "plan_status": "should_append_later",
                        "suggested_reason": "resolution_evidence",
                    },
                    {
                        "planned_target": "track_metadata",
                        "entity_type": "track",
                        "spotify_id": "wrong-reason",
                        "parent_album_id": "candidate-album",
                        "plan_status": "should_append_later",
                        "suggested_reason": "generic_backfill",
                    },
                ],
                "blocked_sibling_collection_prerequisites": [
                    {
                        "planned_target": "album_tracklist",
                        "entity_type": "album",
                        "spotify_id": "blocked-album",
                        "plan_status": "tracklist_missing",
                        "suggested_reason": "resolution_evidence",
                    }
                ],
            }
        }

        with patch("backend.app.spotify_catalog_backfill.httpx.Client") as client_mock:
            dry_run = append_resolution_evidence_sibling_tracks_from_report(report=report, apply=False)
        client_mock.assert_not_called()
        self.assertEqual("dry_run", dry_run["mode"])
        self.assertEqual("none", dry_run["performed_action"])
        self.assertEqual(2, dry_run["selected_count"])
        self.assertEqual(["sibling-new", "already-existing-track"], [item["spotify_id"] for item in dry_run["selected_items"]])
        diagnostic = dry_run["appendability_diagnostic"]
        self.assertEqual(2, diagnostic["source_counts"]["append_selected_count"])
        self.assertEqual(5, diagnostic["source_counts"]["actual_sibling_track_items_count"])
        self.assertEqual(2, diagnostic["append_exclusion_counts"]["appendable"])
        self.assertEqual(1, diagnostic["append_exclusion_counts"]["already_queued_pending"])
        self.assertEqual(1, diagnostic["append_exclusion_counts"]["planned_target_filter_mismatch"])
        self.assertEqual(1, diagnostic["append_exclusion_counts"]["suggested_reason_filter_mismatch"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_dry_run = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status, attempts
                FROM spotify_catalog_backfill_queue
                ORDER BY id
                """
            ).fetchall()
        self.assertEqual(before, after_dry_run)

        with patch("backend.app.spotify_catalog_backfill.httpx.Client") as client_mock:
            applied = append_resolution_evidence_sibling_tracks_from_report(report=report, apply=True)
        client_mock.assert_not_called()
        self.assertEqual("apply", applied["mode"])
        self.assertEqual("inserted_queue_rows", applied["performed_action"])
        self.assertEqual(1, applied["inserted"])
        self.assertEqual(1, applied["already_existing"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            rows = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status, attempts
                FROM spotify_catalog_backfill_queue
                ORDER BY spotify_id
                """
            ).fetchall()
        self.assertIn(("track", "sibling-new", "resolution_evidence", 80, "pending", 0), rows)
        self.assertIn(("track", "already-existing-track", "identity_metadata", 70, "done", 0), rows)
        self.assertNotIn(("album", "album-should-not-insert", "resolution_evidence", 80, "pending", 0), rows)
        self.assertNotIn(("album", "blocked-album", "resolution_evidence", 80, "pending", 0), rows)
        self.assertNotIn(("track", "wrong-reason", "resolution_evidence", 80, "pending", 0), rows)

        second_apply = append_resolution_evidence_sibling_tracks_from_report(report=report, apply=True)
        self.assertEqual(0, second_apply["inserted"])
        self.assertEqual(2, second_apply["already_existing"])

    def test_append_resolution_evidence_sibling_tracks_explains_zero_candidates_from_broad_delta(self) -> None:
        report = {
            "resolution_evidence_delta": {
                "counts": {
                    "sibling_tracks_missing_from_queue": 62,
                    "sibling_tracks_already_present_locally_but_not_queued": 52,
                    "sibling_tracks_requiring_metadata": 10,
                    "tracklists_needed_before_sibling_tracks_can_be_enumerated": 985,
                },
                "samples": {
                    "sibling_tracks_requiring_metadata": [
                        {"spotify_id": "needs-metadata", "parent_album_id": "album-needs-tracklist"}
                    ],
                    "sibling_tracks_missing_from_queue": [
                        {"spotify_id": "missing-queue", "parent_album_id": "album-needs-tracklist"}
                    ],
                },
            },
            "dry_run_resolution_evidence_plan": {
                "actual_sibling_track_items": [],
            },
        }

        result = append_resolution_evidence_sibling_tracks_from_report(report=report, apply=False)

        self.assertEqual(0, result["selected_count"])
        diagnostic = result["appendability_diagnostic"]
        self.assertEqual(10, diagnostic["source_counts"]["sibling_tracks_requiring_metadata_count"])
        self.assertEqual(62, diagnostic["source_counts"]["sibling_tracks_missing_from_queue_count"])
        self.assertEqual(0, diagnostic["source_counts"]["actual_sibling_track_items_count"])
        self.assertEqual(10, diagnostic["broad_delta_not_in_focused_append_plan_count"])
        self.assertEqual(
            "not_in_focused_append_plan",
            diagnostic["samples"]["sibling_tracks_requiring_metadata"][0]["appendability_reason"],
        )
        self.assertIn("focused append plan", diagnostic["selection_note"])

    def _seed_resolution_tracklist_worker_case(self, *, extra_candidate_album: bool = False) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Resolution Track", "resolution track"),
                ).lastrowid
            )
            source_defs = [("worker-track-1", "worker-album")]
            if extra_candidate_album:
                source_defs.append(("worker-track-2", "worker-album-2"))
            else:
                source_defs.append(("worker-track-2", "worker-album"))
            for spotify_track_id, album_id in source_defs:
                source_track_id = int(
                    connection.execute(
                        """
                        INSERT INTO source_track (
                          source_name, external_id, external_uri, source_name_raw, raw_payload_json
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        ("spotify", spotify_track_id, f"spotify:track:{spotify_track_id}", "Resolution Track", "{}"),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'seed', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_track_id, release_track_id),
                )
                connection.execute(
                    """
                    INSERT INTO spotify_track_catalog (
                      spotify_track_id, name, duration_ms, explicit, disc_number, track_number,
                      album_id, artists_json, raw_json, market, fetched_at, last_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        spotify_track_id,
                        "Resolution Track",
                        100000,
                        0,
                        1,
                        1,
                        album_id,
                        '[{"name":"Artist 1"}]',
                        json.dumps({"external_ids": {"isrc": f"ISRC{spotify_track_id}"}}),
                        "US",
                        "2026-04-27T12:00:00Z",
                        "ok",
                    ),
                )
            for album_id in {"worker-album", *(["worker-album-2"] if extra_candidate_album else [])}:
                connection.execute(
                    """
                    INSERT INTO spotify_album_catalog (
                      spotify_album_id, name, album_type, release_date, release_date_precision,
                      total_tracks, artists_json, images_json, raw_json, market, fetched_at, last_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        album_id,
                        f"Album {album_id}",
                        "album",
                        "2026-01-01",
                        "day",
                        2,
                        '[{"name":"Artist 1"}]',
                        '[{"url":"https://image.test/1.jpg"}]',
                        json.dumps({"copyrights": [{"text": "C 2026 Label"}], "label": "Label"}),
                        "US",
                        "2026-04-27T12:00:00Z",
                        "ok",
                    ),
                )
            queue_rows = [
                ("album", "worker-album", "resolution_evidence", 80, "pending", "2026-04-27T12:00:00Z", 0, None),
                ("album", "legacy-album", "visible_incomplete", 80, "pending", "2026-04-27T12:01:00Z", 0, None),
                ("album", "generic-album", "full_backfill", 80, "pending", "2026-04-27T12:02:00Z", 0, None),
                ("track", "worker-track-1", "resolution_evidence", 80, "pending", "2026-04-27T12:03:00Z", 0, None),
            ]
            if extra_candidate_album:
                queue_rows.append(
                    (
                        "album",
                        "worker-album-2",
                        "album_lookup_visible_incomplete",
                        80,
                        "pending",
                        "2026-04-27T12:04:00Z",
                        0,
                        None,
                    )
                )
            connection.executemany(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                queue_rows,
            )
            connection.commit()

    def _seed_resolution_track_metadata_worker_case(self) -> None:
        self._seed_resolution_tracklist_worker_case()
        with closing(sqlite3.connect(self.db_path)) as connection:
            for track_id, track_number in (("sibling-track-1", 1), ("sibling-track-2", 2)):
                connection.execute(
                    """
                    INSERT INTO spotify_album_track (
                      spotify_album_id, spotify_track_id, disc_number, track_number, name,
                      duration_ms, artists_json, raw_json, market, fetched_at, last_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "worker-album",
                        track_id,
                        1,
                        track_number,
                        f"Sibling {track_number}",
                        100000 + track_number,
                        '[{"name":"Artist 1"}]',
                        "{}",
                        "US",
                        "2026-04-27T12:00:00Z",
                        "ok",
                    ),
                )
            connection.executemany(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("track", "sibling-track-1", "resolution_evidence", 80, "pending", "2026-04-27T12:04:00Z", 0, None),
                    ("track", "sibling-track-2", "resolution_evidence", 80, "pending", "2026-04-27T12:05:00Z", 0, None),
                    ("track", "identity-track", "identity_metadata", 90, "pending", "2026-04-27T12:06:00Z", 0, None),
                    ("track", "resolution-not-sibling", "resolution_evidence", 80, "pending", "2026-04-27T12:07:00Z", 0, None),
                ],
            )
            connection.commit()

    def test_resolution_album_tracklist_worker_dry_run_selects_only_focused_rows_without_mutation(self) -> None:
        self._seed_resolution_tracklist_worker_case()
        with closing(sqlite3.connect(self.db_path)) as connection:
            before = connection.execute(
                "SELECT entity_type, spotify_id, reason, status, attempts FROM spotify_catalog_backfill_queue ORDER BY id"
            ).fetchall()

        fetch_calls: list[str] = []

        def fetcher(url: str, params: dict[str, Any], token: str) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            fetch_calls.append(url)
            return 200, {}, {}, None

        result = run_spotify_resolution_evidence_album_tracklist_worker(
            access_token="token",
            limit=5,
            dry_run=True,
            fetcher=fetcher,
            sleeper=lambda _: None,
        )
        self.assertEqual("dry_run", result["status"])
        self.assertEqual("none", result["performed_action"])
        self.assertEqual(
            [
                {
                    "queue_id": 1,
                    "spotify_album_id": "worker-album",
                    "stored_reason": "resolution_evidence",
                    "planner_status": "already_queued_pending",
                }
            ],
            result["selected_items"],
        )
        self.assertEqual({"resolution_evidence": 1}, result["selected_count_by_stored_reason"])
        self.assertEqual([], fetch_calls)
        with closing(sqlite3.connect(self.db_path)) as connection:
            after = connection.execute(
                "SELECT entity_type, spotify_id, reason, status, attempts FROM spotify_catalog_backfill_queue ORDER BY id"
            ).fetchall()
        self.assertEqual(before, after)

    def test_resolution_album_tracklist_worker_processes_limit_and_marks_done(self) -> None:
        self._seed_resolution_tracklist_worker_case(extra_candidate_album=True)
        calls: list[str] = []

        def fetcher(url: str, params: dict[str, Any], token: str) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            calls.append(url)
            self.assertIn("/v1/albums/", url)
            self.assertIn("/tracks", url)
            self.assertNotIn("/v1/albums?ids=", url)
            return (
                200,
                {},
                {
                    "items": [
                        {
                            "id": "album-track-1",
                            "name": "Album Track 1",
                            "duration_ms": 100000,
                            "disc_number": 1,
                            "track_number": 1,
                            "artists": [{"name": "Artist 1"}],
                        },
                        {
                            "id": "album-track-2",
                            "name": "Album Track 2",
                            "duration_ms": 101000,
                            "disc_number": 1,
                            "track_number": 2,
                            "artists": [{"name": "Artist 1"}],
                        },
                    ],
                    "next": None,
                },
                None,
            )

        result = run_spotify_resolution_evidence_album_tracklist_worker(
            access_token="token",
            limit=1,
            max_requests=5,
            dry_run=False,
            fetcher=fetcher,
            sleeper=lambda _: None,
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["processed_count"])
        self.assertEqual(1, result["done_count"])
        self.assertEqual(1, result["requests_total"])
        self.assertEqual([1], result["queue_ids_processed"])
        self.assertEqual(["worker-album"], result["album_spotify_ids_processed"])
        self.assertEqual(
            {"resolution_evidence": 1},
            result["selected_count_by_stored_reason"],
        )
        dry_run_all = run_spotify_resolution_evidence_album_tracklist_worker(
            access_token="token",
            limit=10,
            dry_run=True,
            fetcher=fetcher,
            sleeper=lambda _: None,
        )
        self.assertEqual(
            {"album_lookup_visible_incomplete": 1},
            dry_run_all["selected_count_by_stored_reason"],
        )
        self.assertEqual(
            ["worker-album-2"],
            [item["spotify_album_id"] for item in dry_run_all["selected_items"]],
        )

        with closing(sqlite3.connect(self.db_path)) as connection:
            statuses = dict(
                connection.execute(
                    "SELECT spotify_id, status FROM spotify_catalog_backfill_queue WHERE entity_type = 'album'"
                ).fetchall()
            )
            track_count = int(
                connection.execute(
                    "SELECT count(*) FROM spotify_album_track WHERE spotify_album_id = 'worker-album'"
                ).fetchone()[0]
            )
            sibling_queue_count = int(
                connection.execute(
                    "SELECT count(*) FROM spotify_catalog_backfill_queue WHERE entity_type = 'track' AND spotify_id LIKE 'album-track-%'"
                ).fetchone()[0]
            )
        self.assertEqual("done", statuses["worker-album"])
        self.assertEqual("pending", statuses["worker-album-2"])
        self.assertEqual("pending", statuses["legacy-album"])
        self.assertEqual("pending", statuses["generic-album"])
        self.assertEqual(2, track_count)
        self.assertEqual(0, sibling_queue_count)

    def test_resolution_album_tracklist_worker_does_not_mark_done_when_no_tracks_stored(self) -> None:
        self._seed_resolution_tracklist_worker_case()

        def fetcher(url: str, params: dict[str, Any], token: str) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            return 200, {}, {"items": [], "next": None}, None

        result = run_spotify_resolution_evidence_album_tracklist_worker(
            access_token="token",
            limit=1,
            max_requests=5,
            dry_run=False,
            fetcher=fetcher,
            sleeper=lambda _: None,
        )
        self.assertEqual("partial", result["status"])
        self.assertEqual(1, result["processed_count"])
        self.assertEqual(0, result["done_count"])
        self.assertEqual(1, result["outcome_counts"]["fetched_but_not_complete"])
        self.assertEqual(0, result["outcomes"][0]["album_track_count"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                """
                SELECT status, attempts
                FROM spotify_catalog_backfill_queue
                WHERE spotify_id = 'worker-album'
                """
            ).fetchone()
            track_count = int(
                connection.execute(
                    "SELECT count(*) FROM spotify_album_track WHERE spotify_album_id = 'worker-album'"
                ).fetchone()[0]
            )
        self.assertEqual(("pending", 1), row)
        self.assertEqual(0, track_count)

    def test_resolution_album_tracklist_worker_fetches_missing_album_metadata_before_tracklist(self) -> None:
        self._seed_resolution_tracklist_worker_case()
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                UPDATE spotify_album_catalog
                SET release_date = NULL,
                    total_tracks = NULL,
                    images_json = NULL,
                    raw_json = '{}'
                WHERE spotify_album_id = 'worker-album'
                """
            )
            connection.commit()

        calls: list[str] = []

        def fetcher(url: str, params: dict[str, Any], token: str) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            calls.append(url)
            self.assertNotIn("/v1/albums?ids=", url)
            if url.endswith("/v1/albums/worker-album"):
                return 200, {}, _album_payload("worker-album"), None
            if url.endswith("/v1/albums/worker-album/tracks"):
                return (
                    200,
                    {},
                    {
                        "items": [
                            {
                                "id": "album-track-1",
                                "name": "Album Track 1",
                                "duration_ms": 100000,
                                "disc_number": 1,
                                "track_number": 1,
                                "artists": [{"name": "Artist 1"}],
                            },
                            {
                                "id": "album-track-2",
                                "name": "Album Track 2",
                                "duration_ms": 101000,
                                "disc_number": 1,
                                "track_number": 2,
                                "artists": [{"name": "Artist 1"}],
                            },
                        ],
                        "next": None,
                    },
                    None,
                )
            raise AssertionError(f"Unexpected URL: {url}")

        result = run_spotify_resolution_evidence_album_tracklist_worker(
            access_token="token",
            limit=1,
            max_requests=5,
            dry_run=False,
            fetcher=fetcher,
            sleeper=lambda _: None,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual(2, result["requests_total"])
        self.assertEqual(1, result["outcome_counts"]["fetched_album_metadata"])
        self.assertEqual(1, result["outcome_counts"]["fetched_tracklist"])
        self.assertEqual(1, result["outcome_counts"]["fetched_and_marked_done"])
        self.assertEqual(
            [
                "https://api.spotify.com/v1/albums/worker-album",
                "https://api.spotify.com/v1/albums/worker-album/tracks",
            ],
            calls,
        )
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                """
                SELECT q.status, sac.total_tracks, count(sat.spotify_track_id)
                FROM spotify_catalog_backfill_queue q
                JOIN spotify_album_catalog sac ON sac.spotify_album_id = q.spotify_id
                LEFT JOIN spotify_album_track sat ON sat.spotify_album_id = q.spotify_id
                WHERE q.spotify_id = 'worker-album'
                GROUP BY q.status, sac.total_tracks
                """
            ).fetchone()
        self.assertEqual(("done", 2, 2), row)

    def test_resolution_track_metadata_worker_dry_run_selects_only_focused_sibling_rows(self) -> None:
        self._seed_resolution_track_metadata_worker_case()
        with closing(sqlite3.connect(self.db_path)) as connection:
            before = connection.execute(
                "SELECT entity_type, spotify_id, reason, status, attempts FROM spotify_catalog_backfill_queue ORDER BY id"
            ).fetchall()

        fetch_calls: list[str] = []

        def fetcher(url: str, params: dict[str, Any], token: str) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            fetch_calls.append(url)
            return 200, {}, {}, None

        result = run_spotify_resolution_evidence_track_metadata_worker(
            access_token="token",
            limit=10,
            dry_run=True,
            fetcher=fetcher,
            sleeper=lambda _: None,
        )

        self.assertEqual("dry_run", result["status"])
        self.assertEqual("none", result["performed_action"])
        self.assertEqual(2, result["selected_count"])
        self.assertEqual({"resolution_evidence": 2}, result["selected_count_by_stored_reason"])
        self.assertEqual(
            ["sibling-track-1", "sibling-track-2"],
            [item["spotify_track_id"] for item in result["selected_items"]],
        )
        self.assertEqual([], fetch_calls)
        with closing(sqlite3.connect(self.db_path)) as connection:
            after = connection.execute(
                "SELECT entity_type, spotify_id, reason, status, attempts FROM spotify_catalog_backfill_queue ORDER BY id"
            ).fetchall()
        self.assertEqual(before, after)

    def test_resolution_track_metadata_worker_processes_only_focused_tracks_and_marks_done(self) -> None:
        self._seed_resolution_track_metadata_worker_case()
        calls: list[str] = []

        def fetcher(url: str, params: dict[str, Any], token: str) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            calls.append(url)
            self.assertIn("/v1/tracks/", url)
            self.assertNotIn("/v1/albums/", url)
            return 200, {}, _track_payload("sibling-track-1", "worker-album"), None

        result = run_spotify_resolution_evidence_track_metadata_worker(
            access_token="token",
            limit=1,
            max_requests=5,
            dry_run=False,
            fetcher=fetcher,
            sleeper=lambda _: None,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual(1, result["processed_count"])
        self.assertEqual(1, result["done_count"])
        self.assertEqual(1, result["requests_total"])
        self.assertEqual(["sibling-track-1"], result["track_spotify_ids_processed"])
        self.assertEqual(1, result["outcome_counts"]["fetched_track_metadata"])
        self.assertEqual(1, result["outcome_counts"]["fetched_and_marked_done"])
        self.assertEqual(["https://api.spotify.com/v1/tracks/sibling-track-1"], calls)
        with closing(sqlite3.connect(self.db_path)) as connection:
            statuses = dict(
                connection.execute(
                    """
                    SELECT spotify_id, status
                    FROM spotify_catalog_backfill_queue
                    WHERE spotify_id IN ('sibling-track-1', 'sibling-track-2', 'identity-track', 'resolution-not-sibling')
                    """
                ).fetchall()
            )
            track_count = int(
                connection.execute(
                    "SELECT count(*) FROM spotify_track_catalog WHERE spotify_track_id = 'sibling-track-1'"
                ).fetchone()[0]
            )
        self.assertEqual("done", statuses["sibling-track-1"])
        self.assertEqual("pending", statuses["sibling-track-2"])
        self.assertEqual("pending", statuses["identity-track"])
        self.assertEqual("pending", statuses["resolution-not-sibling"])
        self.assertEqual(1, track_count)

    def test_resolution_track_metadata_worker_warns_if_local_album_display_gaps_increase(self) -> None:
        self._seed_resolution_track_metadata_worker_case()

        def fetcher(url: str, params: dict[str, Any], token: str) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            return 200, {}, _track_payload("sibling-track-1", "worker-album"), None

        with patch(
            "backend.app.spotify_catalog_backfill.inspect_spotify_nested_metadata_integrity",
            side_effect=[
                {
                    "ok": True,
                    "mode": "read_only",
                    "counts": {
                        "tracks_with_album_spotify_id_missing_local_album_name": 0,
                        "tracks_with_artist_ids_missing_artist_names": 0,
                        "albums_with_artist_ids_missing_artist_names": 0,
                        "queue_rows_done_but_local_metadata_incomplete": 0,
                    },
                    "samples": {},
                },
                {
                    "ok": True,
                    "mode": "read_only",
                    "counts": {
                        "tracks_with_album_spotify_id_missing_local_album_name": 1,
                        "tracks_with_artist_ids_missing_artist_names": 0,
                        "albums_with_artist_ids_missing_artist_names": 0,
                        "queue_rows_done_but_local_metadata_incomplete": 0,
                    },
                    "samples": {},
                },
            ],
        ):
            result = run_spotify_resolution_evidence_track_metadata_worker(
                access_token="token",
                limit=1,
                max_requests=5,
                dry_run=False,
                fetcher=fetcher,
                sleeper=lambda _: None,
            )

        self.assertEqual("ok", result["status"])
        self.assertIn(
            "Local metadata integrity warning: track processing increased tracks_with_album_spotify_id_missing_local_album_name.",
            result["warnings"],
        )

    def test_resolution_track_metadata_worker_does_not_mark_done_when_metadata_incomplete(self) -> None:
        self._seed_resolution_track_metadata_worker_case()

        def fetcher(url: str, params: dict[str, Any], token: str) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            payload = _track_payload("sibling-track-1", "worker-album")
            payload["external_ids"] = {}
            return 200, {}, payload, None

        result = run_spotify_resolution_evidence_track_metadata_worker(
            access_token="token",
            limit=1,
            max_requests=5,
            dry_run=False,
            fetcher=fetcher,
            sleeper=lambda _: None,
        )

        self.assertEqual("partial", result["status"])
        self.assertEqual(1, result["processed_count"])
        self.assertEqual(0, result["done_count"])
        self.assertEqual(1, result["outcome_counts"]["fetched_but_not_complete"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                """
                SELECT status, attempts
                FROM spotify_catalog_backfill_queue
                WHERE spotify_id = 'sibling-track-1'
                """
            ).fetchone()
        self.assertEqual(("pending", 1), row)

    def test_track_metadata_upsert_persists_embedded_album_basic_fields_without_full_metadata(self) -> None:
        payload = _track_payload("track-with-album", "embedded-album")
        payload["album"] = {
            "id": "embedded-album",
            "name": "Embedded Album",
            "album_type": "album",
            "release_date": "2024-02-03",
            "release_date_precision": "day",
            "total_tracks": 9,
            "artists": [{"id": "artist-1", "name": "Artist 1"}],
            "images": [{"url": "https://image.test/embedded.jpg"}],
        }
        with patch("backend.app.spotify_catalog_backfill.httpx.Client") as client_mock:
            _upsert_track_catalog(
                track=payload,
                market="US",
                fetched_at="2026-04-27T12:00:00Z",
                last_status="ok",
                last_error=None,
            )
        client_mock.assert_not_called()
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                """
                SELECT name, album_type, release_date, release_date_precision, total_tracks, images_json, raw_json
                FROM spotify_album_catalog
                WHERE spotify_album_id = 'embedded-album'
                """
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual("Embedded Album", row[0])
        self.assertEqual("album", row[1])
        self.assertEqual("2024-02-03", row[2])
        self.assertEqual("day", row[3])
        self.assertEqual(9, row[4])
        self.assertIn("embedded.jpg", row[5])
        album_raw = json.loads(row[6])
        self.assertNotIn("label", album_raw)
        self.assertNotIn("external_ids", album_raw)
        integrity = inspect_spotify_nested_metadata_integrity()
        self.assertEqual(0, integrity["counts"]["tracks_with_album_spotify_id_missing_local_album_name"])
        self.assertEqual(0, integrity["counts"]["tracks_with_artist_ids_missing_artist_names"])
        self.assertEqual(0, integrity["counts"]["albums_with_artist_ids_missing_artist_names"])

    def test_track_metadata_upsert_does_not_overwrite_full_album_metadata_with_simplified_album(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, album_type, release_date, release_date_precision,
                  total_tracks, artists_json, images_json, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "existing-full-album",
                    "Full Album",
                    "album",
                    "2020-01-01",
                    "day",
                    12,
                    '[{"name":"Full Artist"}]',
                    '[{"url":"https://image.test/full.jpg"}]',
                    json.dumps(
                        {
                            "id": "existing-full-album",
                            "name": "Full Album",
                            "label": "Full Label",
                            "copyrights": [{"text": "C Full"}],
                            "external_ids": {"upc": "UPC123"},
                        }
                    ),
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            connection.commit()
        payload = _track_payload("track-existing-album", "existing-full-album")
        payload["album"] = {
            "id": "existing-full-album",
            "name": "Simplified Album",
            "album_type": "album",
            "release_date": "2024",
            "release_date_precision": "year",
            "total_tracks": 5,
            "artists": [{"name": "Simplified Artist"}],
            "images": [{"url": "https://image.test/simple.jpg"}],
        }
        _upsert_track_catalog(
            track=payload,
            market="US",
            fetched_at="2026-04-27T13:00:00Z",
            last_status="ok",
            last_error=None,
        )
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                """
                SELECT name, release_date, total_tracks, images_json, raw_json
                FROM spotify_album_catalog
                WHERE spotify_album_id = 'existing-full-album'
                """
            ).fetchone()
        self.assertEqual("Full Album", row[0])
        self.assertEqual("2020-01-01", row[1])
        self.assertEqual(12, row[2])
        self.assertIn("full.jpg", row[3])
        album_raw = json.loads(row[4])
        self.assertEqual("Full Label", album_raw["label"])
        self.assertEqual("UPC123", album_raw["external_ids"]["upc"])

    def test_album_metadata_display_gap_diagnostic_is_read_only(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, explicit, disc_number, track_number,
                  album_id, artists_json, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "diagnostic-track",
                    "Diagnostic Track",
                    123000,
                    0,
                    1,
                    1,
                    "diagnostic-album",
                    '[{"name":"Artist 1"}]',
                    json.dumps({"album": {"id": "diagnostic-album", "name": "Diagnostic Album", "total_tracks": 3}}),
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            before = connection.execute(
                "SELECT spotify_track_id, album_id, raw_json FROM spotify_track_catalog ORDER BY spotify_track_id"
            ).fetchall()
            connection.commit()
        with patch("backend.app.spotify_catalog_backfill.httpx.Client") as client_mock:
            report = inspect_spotify_album_metadata_display_gaps()
        client_mock.assert_not_called()
        self.assertTrue(report["ok"])
        self.assertEqual("read_only", report["mode"])
        self.assertEqual(1, report["counts"]["tracks_with_album_spotify_id_missing_local_album_name"])
        self.assertEqual("diagnostic-track", report["samples"][0]["spotify_track_id"])
        self.assertTrue(report["samples"][0]["can_populate_basic_album_display_from_track_payload"])
        self.assertTrue(report["samples"][0]["requires_full_album_fetch_for_label_copyright_external_ids"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            after = connection.execute(
                "SELECT spotify_track_id, album_id, raw_json FROM spotify_track_catalog ORDER BY spotify_track_id"
            ).fetchall()
        self.assertEqual(before, after)

    def test_track_mapping_lineage_uses_embedded_album_display_fallback(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Shared Track", "shared track"),
                ).lastrowid
            )
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Release Album", "release album"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (release_album_id, release_track_id),
            )
            source_ids: list[int] = []
            for spotify_track_id in ("local-track", "embedded-track"):
                source_ids.append(
                    int(
                        connection.execute(
                            """
                            INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            (
                                "spotify",
                                spotify_track_id,
                                f"spotify:track:{spotify_track_id}",
                                f"Track {spotify_track_id}",
                                "{}",
                            ),
                        ).lastrowid
                    )
                )
            for source_id in source_ids:
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_id, release_track_id),
                )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, release_date, total_tracks, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("local-album", "Local Album", "2020-01-01", 10, "{}", "US", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, explicit, disc_number, track_number,
                  album_id, artists_json, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "local-track",
                    "Local Track",
                    123000,
                    0,
                    1,
                    1,
                    "local-album",
                    "[]",
                    "{}",
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, explicit, disc_number, track_number,
                  album_id, artists_json, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "embedded-track",
                    "Embedded Track",
                    124000,
                    0,
                    1,
                    2,
                    "embedded-album",
                    "[]",
                    json.dumps(
                        {
                            "album": {
                                "id": "embedded-album",
                                "name": "Embedded Album",
                                "release_date": "2021-02-03",
                                "total_tracks": 12,
                            }
                        }
                    ),
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            before = connection.execute(
                "SELECT count(*) FROM spotify_album_catalog WHERE spotify_album_id = 'embedded-album'"
            ).fetchone()[0]
            connection.commit()

        with patch("backend.app.spotify_catalog_backfill.httpx.Client") as client_mock:
            lineage = search_track_mapping_lineage(limit=10)
            diagnostic = inspect_source_release_album_display_gaps(sample_limit=5)
        client_mock.assert_not_called()

        sources = lineage["source_release"]["groups"][0]["sources"]
        by_id = {item["external_id"]: item for item in sources}
        self.assertEqual("Local Album", by_id["local-track"]["album_name_display"])
        self.assertEqual("spotify_album_catalog", by_id["local-track"]["album_name_display_source"])
        self.assertEqual("Embedded Album", by_id["embedded-track"]["album_name_display"])
        self.assertEqual("embedded_track_album", by_id["embedded-track"]["album_name_display_source"])
        self.assertEqual("2021-02-03", by_id["embedded-track"]["album_release_date"])
        self.assertEqual(12, by_id["embedded-track"]["album_total_tracks"])
        self.assertIsNone(by_id["embedded-track"]["album_name"])

        self.assertEqual("read_only", diagnostic["mode"])
        self.assertEqual(2, diagnostic["counts"]["total_rows"])
        self.assertEqual(2, diagnostic["counts"]["rows_with_release_album_display_info"])
        self.assertEqual(1, diagnostic["counts"]["rows_with_source_album_display_info"])
        self.assertEqual(1, diagnostic["counts"]["rows_with_embedded_album_info"])
        self.assertEqual(1, diagnostic["counts"]["rows_with_album_spotify_id_but_no_local_album_name"])
        self.assertEqual(0, diagnostic["counts"]["rows_with_no_album_spotify_id"])
        self.assertEqual(2, diagnostic["counts"]["rows_with_source_album_display_after_embedded_fallback"])
        self.assertEqual("embedded-track", diagnostic["samples"]["embedded_album_available_but_not_local_album_name"][0]["spotify_track_id"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            after = connection.execute(
                "SELECT count(*) FROM spotify_album_catalog WHERE spotify_album_id = 'embedded-album'"
            ).fetchone()[0]
        self.assertEqual(before, after)

    def test_track_mapping_lineage_reports_sibling_metadata_completeness(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Sibling Metadata Track", "sibling metadata track"),
                ).lastrowid
            )
            for spotify_track_id in ("complete-sibling", "incomplete-sibling"):
                source_id = int(
                    connection.execute(
                        """
                        INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                        VALUES ('spotify', ?, ?, ?, '{}')
                        """,
                        (spotify_track_id, f"spotify:track:{spotify_track_id}", spotify_track_id),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'seed', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_id, release_track_id),
                )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, explicit, disc_number, track_number,
                  album_id, artists_json, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "incomplete-sibling",
                    "Incomplete Sibling",
                    None,
                    0,
                    None,
                    None,
                    None,
                    "[]",
                    "{}",
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            complete_release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("All Complete Sibling Track", "all complete sibling track"),
                ).lastrowid
            )
            for spotify_track_id in ("complete-sibling-a", "complete-sibling-b"):
                source_id = int(
                    connection.execute(
                        """
                        INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                        VALUES ('spotify', ?, ?, ?, '{}')
                        """,
                        (spotify_track_id, f"spotify:track:{spotify_track_id}", spotify_track_id),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'seed', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_id, complete_release_track_id),
                )
            safe_release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Safe Confirm Sibling Track", "safe confirm sibling track"),
                ).lastrowid
            )
            for spotify_track_id in ("safe-sibling-a", "safe-sibling-b"):
                source_id = int(
                    connection.execute(
                        """
                        INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                        VALUES ('spotify', ?, ?, ?, '{}')
                        """,
                        (spotify_track_id, f"spotify:track:{spotify_track_id}", "Safe Confirm Sibling Track"),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'seed', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_id, safe_release_track_id),
                )
            missing_album_name_release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Missing Album Name Sibling Track", "missing album name sibling track"),
                ).lastrowid
            )
            for spotify_track_id in ("missing-album-name-a", "missing-album-name-b"):
                source_id = int(
                    connection.execute(
                        """
                        INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                        VALUES ('spotify', ?, ?, ?, '{}')
                        """,
                        (spotify_track_id, f"spotify:track:{spotify_track_id}", spotify_track_id),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'seed', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_id, missing_album_name_release_track_id),
                )
                connection.execute(
                    """
                    INSERT INTO spotify_track_catalog (
                      spotify_track_id, name, duration_ms, explicit, disc_number, track_number,
                      album_id, artists_json, raw_json, market, fetched_at, last_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        spotify_track_id,
                        f"Missing Album Name {spotify_track_id}",
                        123000,
                        0,
                        1,
                        1,
                        f"{spotify_track_id}-album",
                        json.dumps([{"id": "artist-1", "name": "Artist 1"}]),
                        json.dumps({"external_ids": {"isrc": f"ISRC{spotify_track_id}"}}),
                        "US",
                        "2026-04-27T12:00:00Z",
                        "ok",
                    ),
                )
            connection.commit()

        for track_id, album_id, track_number in (
            ("complete-sibling", "complete-album", 2),
            ("complete-sibling-a", "complete-album-a", 1),
            ("complete-sibling-b", "complete-album-b", 2),
            ("safe-sibling-a", "safe-album-a", 7),
            ("safe-sibling-b", "safe-album-b", 7),
        ):
            _upsert_track_catalog(
                track={
                    "id": track_id,
                    "name": "Safe Confirm Sibling Track" if track_id.startswith("safe-") else f"Complete {track_id}",
                    "duration_ms": 123000,
                    "explicit": False,
                    "disc_number": 1,
                    "track_number": track_number,
                    "album": {
                        "id": album_id,
                        "name": "Shared Safe Album" if track_id.startswith("safe-") else f"Album {album_id}",
                        "album_type": "album",
                        "release_date": "2020-01-01",
                        "release_date_precision": "day",
                        "total_tracks": 10,
                        "artists": [{"id": "artist-1", "name": "Artist 1"}],
                        "images": [],
                    },
                    "artists": [{"id": "artist-1", "name": "Artist 1"}],
                    "external_ids": {"isrc": "ISRCSAFE" if track_id.startswith("safe-") else f"ISRC{track_id}"},
                },
                market="US",
                fetched_at="2026-04-27T12:00:00Z",
                last_status="ok",
                last_error=None,
            )

        lineage = search_track_mapping_lineage(limit=10)

        self.assertEqual(4, lineage["source_release"]["total"])
        group = next(
            item for item in lineage["source_release"]["groups"] if item["release_track_id"] == release_track_id
        )
        self.assertFalse(group["all_source_metadata_complete"])
        self.assertEqual(1, group["source_metadata_complete_count"])
        self.assertEqual(1, group["source_metadata_incomplete_count"])
        by_id = {item["external_id"]: item for item in group["sources"]}
        self.assertTrue(by_id["complete-sibling"]["metadata_complete"])
        self.assertEqual([], by_id["complete-sibling"]["metadata_gaps"])
        self.assertFalse(by_id["incomplete-sibling"]["metadata_complete"])
        self.assertIn("duration_ms", by_id["incomplete-sibling"]["metadata_gaps"])
        self.assertIn("album_id", by_id["incomplete-sibling"]["metadata_gaps"])

        missing_album_name_group = next(
            item for item in lineage["source_release"]["groups"] if item["release_track_id"] == missing_album_name_release_track_id
        )
        self.assertFalse(missing_album_name_group["all_source_metadata_complete"])
        self.assertEqual(0, missing_album_name_group["source_metadata_complete_count"])
        self.assertEqual(2, missing_album_name_group["source_metadata_incomplete_count"])
        for source in missing_album_name_group["sources"]:
            self.assertFalse(source["metadata_complete"])
            self.assertTrue(source["album_id"])
            self.assertIn("album_display_name", source["metadata_gaps"])

        complete_lineage = search_track_mapping_lineage(limit=10, source_metadata="complete")
        self.assertEqual(2, complete_lineage["source_release"]["total"])
        complete_by_id = {
            group["release_track_id"]: group
            for group in complete_lineage["source_release"]["groups"]
        }
        self.assertTrue(complete_by_id[complete_release_track_id]["all_source_metadata_complete"])
        self.assertEqual("unsafe", complete_by_id[complete_release_track_id]["confirmation_preview"]["readiness"])
        self.assertIn(
            "Source rows do not all share the same normalized album name.",
            complete_by_id[complete_release_track_id]["confirmation_preview"]["reasons"],
        )
        self.assertTrue(complete_by_id[safe_release_track_id]["all_source_metadata_complete"])
        self.assertEqual("safe_candidate", complete_by_id[safe_release_track_id]["confirmation_preview"]["readiness"])
        self.assertEqual(["shared safe album"], complete_by_id[safe_release_track_id]["confirmation_preview"]["evidence"]["normalized_album_names"])
        self.assertEqual(["1.7"], complete_by_id[safe_release_track_id]["confirmation_preview"]["evidence"]["positions"])

        certain_lineage = search_track_mapping_lineage(
            limit=10,
            source_metadata="complete",
            confirmation_certainty="certain",
        )
        self.assertEqual(1, certain_lineage["source_release"]["total"])
        self.assertEqual(safe_release_track_id, certain_lineage["source_release"]["groups"][0]["release_track_id"])
        self.assertEqual("certain", certain_lineage["source_release"]["confirmation_certainty_filter"])

        uncertain_lineage = search_track_mapping_lineage(
            limit=10,
            source_metadata="complete",
            confirmation_certainty="uncertain",
        )
        self.assertEqual(1, uncertain_lineage["source_release"]["total"])
        self.assertEqual(complete_release_track_id, uncertain_lineage["source_release"]["groups"][0]["release_track_id"])
        self.assertEqual("uncertain", uncertain_lineage["source_release"]["confirmation_certainty_filter"])

        incomplete_lineage = search_track_mapping_lineage(limit=10, source_metadata="incomplete")
        self.assertEqual(2, incomplete_lineage["source_release"]["total"])
        incomplete_release_track_ids = {
            group["release_track_id"] for group in incomplete_lineage["source_release"]["groups"]
        }
        self.assertEqual({release_track_id, missing_album_name_release_track_id}, incomplete_release_track_ids)
        self.assertTrue(
            all(not group["all_source_metadata_complete"] for group in incomplete_lineage["source_release"]["groups"])
        )

    def test_source_release_album_display_enrichment_plan_selects_only_missing_album_evidence(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Shared Plan Track", "shared plan track"),
                ).lastrowid
            )
            for source_name, spotify_track_id in (
                ("spotify", "needs-metadata"),
                ("spotify", "embedded-covered"),
                ("spotify", "catalog-covered"),
                ("spotify", "source-album-covered"),
                ("local", "local-invalid"),
            ):
                source_id = int(
                    connection.execute(
                        """
                        INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (source_name, spotify_track_id, f"{source_name}:track:{spotify_track_id}", spotify_track_id, "{}"),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'seed', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_id, release_track_id),
                )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, album_id, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("needs-metadata", "Needs Metadata", 123000, None, "{}", "US", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, album_id, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "embedded-covered",
                    "Embedded Covered",
                    123000,
                    None,
                    json.dumps({"album": {"id": "embedded-album", "name": "Embedded Album"}}),
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("catalog-album", "Catalog Album", "{}", "US", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, album_id, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("catalog-covered", "Catalog Covered", 123000, "catalog-album", "{}", "US", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("spotify", "source-only-album", "spotify:album:source-only-album", "Source Album", "{}"),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, album_id, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("source-album-covered", "Source Album Covered", 123000, "source-only-album", "{}", "US", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.commit()

        report = plan_source_release_album_display_enrichment(sample_limit=10)

        self.assertEqual("read_only", report["mode"])
        self.assertEqual(5, report["total_source_release_rows"])
        self.assertEqual(2, report["rows_with_no_spotify_album_evidence"])
        self.assertEqual(1, report["distinct_track_spotify_ids_needing_metadata"])
        self.assertEqual(1, report["eligible_to_fetch"])
        self.assertEqual(1, report["blocked_or_invalid"])
        self.assertEqual(["needs-metadata"], report["sample_track_spotify_ids"])

    def test_source_release_album_display_enrichment_worker_uses_bounded_limits(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Shared Worker Track", "shared worker track"),
                ).lastrowid
            )
            for spotify_track_id in ("worker-gap-1", "worker-gap-2", "worker-gap-3"):
                source_id = int(
                    connection.execute(
                        """
                        INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        ("spotify", spotify_track_id, f"spotify:track:{spotify_track_id}", spotify_track_id, "{}"),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'seed', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_id, release_track_id),
                )
                connection.execute(
                    """
                    INSERT INTO spotify_track_catalog (
                      spotify_track_id, name, duration_ms, raw_json, market, fetched_at, last_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (spotify_track_id, spotify_track_id, 123000, "{}", "US", "2026-04-27T12:00:00Z", "ok"),
                )
            connection.commit()

        calls: list[str] = []

        def fetcher(url: str, params: dict[str, Any], token: str) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            track_id = url.rsplit("/", 1)[-1]
            calls.append(track_id)
            payload = _track_payload(track_id, f"album-{track_id}")
            payload["album"]["name"] = f"Album {track_id}"
            return 200, {}, payload, None

        result = run_source_release_album_display_enrichment_worker(
            access_token="token",
            limit=2,
            max_requests=1,
            request_delay_seconds=0,
            fetcher=fetcher,
            sleeper=lambda _: None,
        )

        self.assertEqual("partial", result["status"])
        self.assertEqual(2, result["selected_count"])
        self.assertEqual(1, result["processed_count"])
        self.assertEqual(1, result["fetched_track_metadata"])
        self.assertEqual(1, result["fetched_and_album_evidence_added"])
        self.assertEqual(0, result["fetched_but_still_missing_album_evidence"])
        self.assertEqual(1, result["requests_total"])
        self.assertEqual(0, result["requests_429"])
        self.assertEqual(["worker-gap-1"], calls)
        after_plan = plan_source_release_album_display_enrichment(sample_limit=10)
        self.assertEqual(2, after_plan["distinct_track_spotify_ids_needing_metadata"])
        self.assertNotIn("worker-gap-1", after_plan["sample_track_spotify_ids"])

    def test_source_release_album_display_enrichment_loop_summarizes_batches_and_writes_jsonl(self) -> None:
        calls: list[int] = []
        clock = {"value": 0.0}
        jsonl_path = Path(self._tmp_dir.name) / "source-release-loop.jsonl"

        def runner(**_: Any) -> dict[str, Any]:
            calls.append(len(calls) + 1)
            if len(calls) == 1:
                return {
                    "ok": True,
                    "status": "ok",
                    "selected_count": 2,
                    "processed_count": 2,
                    "fetched_track_metadata": 2,
                    "fetched_and_album_evidence_added": 1,
                    "requests_total": 2,
                    "requests_429": 0,
                    "error_count": 0,
                    "selected_track_spotify_ids": ["verbose-1", "verbose-2"],
                    "cooldown_until": None,
                }
            return {
                "ok": True,
                "status": "ok",
                "selected_count": 0,
                "processed_count": 0,
                "fetched_track_metadata": 0,
                "fetched_and_album_evidence_added": 0,
                "requests_total": 0,
                "requests_429": 0,
                "error_count": 0,
                "selected_track_spotify_ids": [],
                "cooldown_until": None,
            }

        def sleeper(seconds: float) -> None:
            clock["value"] += seconds

        result = run_source_release_album_display_enrichment_loop(
            access_token="token",
            limit=5,
            max_requests=5,
            request_delay_seconds=0,
            market="US",
            max_runtime_minutes=1,
            between_runs_seconds=5,
            jsonl_output=str(jsonl_path),
            summary_only=True,
            runner=runner,
            sleeper=sleeper,
            monotonic=lambda: clock["value"],
        )

        self.assertEqual("loop", result["mode"])
        self.assertEqual(2, result["total_batches"])
        self.assertEqual(2, result["total_processed_count"])
        self.assertEqual(2, result["total_fetched_track_metadata"])
        self.assertEqual(1, result["total_fetched_and_album_evidence_added"])
        self.assertEqual(2, result["total_requests"])
        self.assertEqual(0, result["total_429"])
        self.assertEqual(0, result["total_errors"])
        self.assertEqual("no_selected_candidates", result["stop_reason"])
        self.assertNotIn("batches", result)
        self.assertEqual(str(jsonl_path), result["jsonl_output"])
        rows = [json.loads(line) for line in jsonl_path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(2, len(rows))
        self.assertEqual("batch_result", rows[0]["event"])
        self.assertEqual(["verbose-1", "verbose-2"], rows[0]["selected_track_spotify_ids"])

    def test_source_release_album_display_enrichment_loop_stops_on_429(self) -> None:
        result = run_source_release_album_display_enrichment_loop(
            access_token="token",
            limit=5,
            max_requests=5,
            request_delay_seconds=0,
            market="US",
            max_runtime_minutes=1,
            between_runs_seconds=5,
            summary_only=True,
            runner=lambda **_: {
                "ok": True,
                "status": "partial",
                "selected_count": 1,
                "processed_count": 1,
                "fetched_track_metadata": 0,
                "fetched_and_album_evidence_added": 0,
                "requests_total": 1,
                "requests_429": 1,
                "error_count": 0,
                "cooldown_until": "2026-05-09T12:00:00Z",
            },
            sleeper=lambda _: None,
            monotonic=lambda: 0.0,
        )

        self.assertEqual(1, result["total_batches"])
        self.assertEqual(1, result["total_429"])
        self.assertEqual("cooldown", result["stop_reason"])

    def test_resolution_track_metadata_worker_ignores_source_release_album_display_gaps(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Shared Gap Track", "shared gap track"),
                ).lastrowid
            )
            for spotify_track_id in ("gap-only-1", "gap-only-2"):
                source_id = int(
                    connection.execute(
                        """
                        INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        ("spotify", spotify_track_id, f"spotify:track:{spotify_track_id}", spotify_track_id, "{}"),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'seed', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_id, release_track_id),
                )
            connection.commit()

        plan = plan_source_release_album_display_enrichment(sample_limit=10)
        result = run_spotify_resolution_evidence_track_metadata_worker(access_token="", dry_run=True)

        self.assertEqual(2, plan["distinct_track_spotify_ids_needing_metadata"])
        self.assertEqual(0, result["selected_count"])
        self.assertEqual([], result["selected_items"])

    def test_nested_metadata_integrity_reports_incomplete_done_queue_rows(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, explicit, disc_number, track_number,
                  album_id, artists_json, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "done-incomplete-track",
                    "Done Incomplete",
                    123000,
                    0,
                    1,
                    1,
                    "done-incomplete-album",
                    '[{"id":"artist-1","name":"Artist 1"}]',
                    json.dumps({"external_ids": {"isrc": "ISRC1"}, "album": {"id": "done-incomplete-album", "name": "Missing Album"}}),
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("track", "done-incomplete-track", "resolution_evidence", 80, "done", "2026-04-27T12:01:00Z", 0),
            )
            connection.commit()

        report = inspect_spotify_nested_metadata_integrity()
        self.assertEqual(1, report["counts"]["tracks_with_album_spotify_id_missing_local_album_name"])
        self.assertEqual(1, report["counts"]["queue_rows_done_but_local_metadata_incomplete"])
        self.assertEqual(
            ["album_display_name"],
            report["samples"]["queue_rows_done_but_local_metadata_incomplete"][0]["gaps"],
        )

    def test_repair_album_metadata_display_gaps_populates_from_stored_track_payloads(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, explicit, disc_number, track_number,
                  album_id, artists_json, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "repair-track",
                    "Repair Track",
                    123000,
                    0,
                    1,
                    1,
                    "repair-album",
                    '[{"name":"Artist 1"}]',
                    json.dumps(
                        {
                            "album": {
                                "id": "repair-album",
                                "name": "Repair Album",
                                "album_type": "album",
                                "release_date": "2022-02-02",
                                "release_date_precision": "day",
                                "total_tracks": 8,
                                "artists": [{"name": "Artist 1"}],
                                "images": [{"url": "https://image.test/repair.jpg"}],
                            }
                        }
                    ),
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, album_type, release_date, release_date_precision,
                  total_tracks, artists_json, images_json, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "full-album",
                    "Full Album",
                    "album",
                    "2020-01-01",
                    "day",
                    12,
                    '[{"name":"Full Artist"}]',
                    '[{"url":"https://image.test/full.jpg"}]',
                    json.dumps(
                        {
                            "id": "full-album",
                            "name": "Full Album",
                            "label": "Full Label",
                            "copyrights": [{"text": "C Full"}],
                            "external_ids": {"upc": "UPC123"},
                        }
                    ),
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, explicit, disc_number, track_number,
                  album_id, artists_json, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "full-track",
                    "Full Track",
                    124000,
                    0,
                    1,
                    1,
                    "full-album",
                    '[{"name":"Artist 1"}]',
                    json.dumps(
                        {
                            "album": {
                                "id": "full-album",
                                "name": "Simplified Full Album",
                                "release_date": "2024",
                                "total_tracks": 3,
                                "images": [{"url": "https://image.test/simple.jpg"}],
                            }
                        }
                    ),
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            before = connection.execute(
                "SELECT spotify_album_id, name, raw_json FROM spotify_album_catalog ORDER BY spotify_album_id"
            ).fetchall()
            connection.commit()

        before_diagnostic = inspect_spotify_album_metadata_display_gaps()
        self.assertEqual(1, before_diagnostic["counts"]["tracks_with_album_spotify_id_missing_local_album_name"])

        with patch("backend.app.spotify_catalog_backfill.httpx.Client") as client_mock:
            dry_run = repair_spotify_album_basic_metadata_from_track_payloads(apply=False)
        client_mock.assert_not_called()
        self.assertEqual("dry_run", dry_run["mode"])
        self.assertEqual("none", dry_run["performed_action"])
        self.assertEqual(1, dry_run["candidate_count"])
        self.assertEqual(1, dry_run["would_update_count"])
        self.assertEqual(0, dry_run["updated_count"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            after_dry_run = connection.execute(
                "SELECT spotify_album_id, name, raw_json FROM spotify_album_catalog ORDER BY spotify_album_id"
            ).fetchall()
        self.assertEqual(before, after_dry_run)

        with patch("backend.app.spotify_catalog_backfill.httpx.Client") as client_mock:
            applied = repair_spotify_album_basic_metadata_from_track_payloads(apply=True)
        client_mock.assert_not_called()
        self.assertEqual("apply", applied["mode"])
        self.assertEqual("populated_basic_album_metadata", applied["performed_action"])
        self.assertEqual(1, applied["updated_count"])
        after_diagnostic = inspect_spotify_album_metadata_display_gaps()
        self.assertEqual(0, after_diagnostic["counts"]["tracks_with_album_spotify_id_missing_local_album_name"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            repaired = connection.execute(
                """
                SELECT name, release_date, total_tracks, images_json
                FROM spotify_album_catalog
                WHERE spotify_album_id = 'repair-album'
                """
            ).fetchone()
            full = connection.execute(
                "SELECT name, release_date, total_tracks, images_json, raw_json FROM spotify_album_catalog WHERE spotify_album_id = 'full-album'"
            ).fetchone()
        self.assertEqual(("Repair Album", "2022-02-02", 8), repaired[:3])
        self.assertIn("repair.jpg", repaired[3])
        self.assertEqual("Full Album", full[0])
        self.assertEqual("2020-01-01", full[1])
        self.assertEqual(12, full[2])
        self.assertIn("full.jpg", full[3])
        full_raw = json.loads(full[4])
        self.assertEqual("Full Label", full_raw["label"])
        self.assertEqual("UPC123", full_raw["external_ids"]["upc"])

    def test_repair_incomplete_done_resolution_tracklists_is_narrow_and_apply_gated(self) -> None:
        self._seed_resolution_tracklist_worker_case(extra_candidate_album=True)
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                "UPDATE spotify_catalog_backfill_queue SET status = 'done', reason = 'identity_metadata' WHERE spotify_id = 'worker-album'"
            )
            connection.execute(
                "UPDATE spotify_catalog_backfill_queue SET status = 'done' WHERE spotify_id = 'worker-album-2'"
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "nonfocused-identity", "identity_metadata", 80, "done", "2026-04-27T12:05:00Z", 0),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, disc_number, track_number, name,
                  duration_ms, artists_json, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "worker-album-2",
                    "complete-track-1",
                    1,
                    1,
                    "Complete Track 1",
                    100000,
                    '[{"name":"Artist 1"}]',
                    "{}",
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, disc_number, track_number, name,
                  duration_ms, artists_json, raw_json, market, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "worker-album-2",
                    "complete-track-2",
                    1,
                    2,
                    "Complete Track 2",
                    101000,
                    '[{"name":"Artist 1"}]',
                    "{}",
                    "US",
                    "2026-04-27T12:00:00Z",
                    "ok",
                ),
            )
            before = connection.execute(
                "SELECT spotify_id, status FROM spotify_catalog_backfill_queue ORDER BY spotify_id"
            ).fetchall()
            connection.commit()

        report = inspect_spotify_catalog_queue_resolution_evidence()
        plan = report["dry_run_resolution_evidence_plan"]
        worker_album_plan_items = [
            item for item in plan["candidate_album_tracklist_items"]
            if item["spotify_id"] == "worker-album"
        ]
        self.assertEqual(1, len(worker_album_plan_items))
        self.assertEqual("done_but_tracklist_incomplete", worker_album_plan_items[0]["plan_status"])
        self.assertEqual(0, plan["counts_by_plan_status"]["already_queued_done"])
        self.assertEqual(2, plan["counts_by_plan_status"]["done_but_tracklist_incomplete"])
        self.assertIn(
            {
                "planned_target": "album_tracklist",
                "entity_type": "album",
                "spotify_id": "worker-album",
                "parent_album_id": None,
                "plan_status": "done_but_tracklist_incomplete",
                "suggested_reason": "resolution_evidence",
                "rationale": "candidate album tracklist must be fetched before sibling tracks can be enumerated",
            },
            plan["blocked_sibling_collection_prerequisites"],
        )

        dry_run = repair_incomplete_done_resolution_tracklist_queue_rows(apply=False)
        self.assertEqual("dry_run", dry_run["mode"])
        self.assertEqual("none", dry_run["performed_action"])
        self.assertEqual(1, dry_run["selected_count"])
        self.assertEqual(1, dry_run["would_reset_count"])
        self.assertEqual("worker-album", dry_run["selected_items"][0]["spotify_album_id"])
        self.assertEqual("identity_metadata", dry_run["selected_items"][0]["stored_reason"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            after_dry_run = connection.execute(
                "SELECT spotify_id, status FROM spotify_catalog_backfill_queue ORDER BY spotify_id"
            ).fetchall()
        self.assertEqual(before, after_dry_run)

        applied = repair_incomplete_done_resolution_tracklist_queue_rows(apply=True)
        self.assertEqual("apply", applied["mode"])
        self.assertEqual(1, applied["reset_count"])
        self.assertEqual(1, applied["would_reset_count"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            statuses = dict(
                connection.execute(
                    """
                    SELECT spotify_id, status
                    FROM spotify_catalog_backfill_queue
                    WHERE spotify_id IN ('worker-album', 'worker-album-2', 'nonfocused-identity')
                    """
                ).fetchall()
            )
        self.assertEqual("pending", statuses["worker-album"])
        self.assertEqual("done", statuses["worker-album-2"])
        self.assertEqual("done", statuses["nonfocused-identity"])

    def test_resolution_album_tracklist_worker_stops_on_429_and_leaves_pending(self) -> None:
        self._seed_resolution_tracklist_worker_case(extra_candidate_album=True)
        calls: list[str] = []

        def fetcher(url: str, params: dict[str, Any], token: str) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            calls.append(url)
            return 429, {"Retry-After": "2"}, {"error": {"status": 429}}, None

        result = run_spotify_resolution_evidence_album_tracklist_worker(
            access_token="token",
            limit=2,
            max_requests=5,
            dry_run=False,
            fetcher=fetcher,
            sleeper=lambda _: None,
        )
        self.assertEqual("partial", result["status"])
        self.assertEqual(1, result["processed_count"])
        self.assertEqual(
            {"album_lookup_visible_incomplete": 1, "resolution_evidence": 1},
            result["selected_count_by_stored_reason"],
        )
        self.assertEqual(1, result["rate_limited_count"])
        self.assertEqual(1, result["requests_total"])
        self.assertIsNotNone(result["cooldown_until"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            rows = connection.execute(
                """
                SELECT spotify_id, reason, status, attempts
                FROM spotify_catalog_backfill_queue
                WHERE entity_type = 'album' AND spotify_id LIKE 'worker-album%'
                ORDER BY spotify_id
                """
            ).fetchall()
        self.assertEqual(
            [
                ("worker-album", "resolution_evidence", "pending", 1),
                ("worker-album-2", "album_lookup_visible_incomplete", "pending", 0),
            ],
            rows,
        )
        self.assertEqual(1, len(calls))

    def test_resolution_track_metadata_worker_stops_on_429_and_leaves_pending(self) -> None:
        self._seed_resolution_track_metadata_worker_case()
        calls: list[str] = []

        def fetcher(url: str, params: dict[str, Any], token: str) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            calls.append(url)
            return 429, {"Retry-After": "2"}, {"error": {"status": 429}}, None

        result = run_spotify_resolution_evidence_track_metadata_worker(
            access_token="token",
            limit=2,
            max_requests=5,
            dry_run=False,
            fetcher=fetcher,
            sleeper=lambda _: None,
        )

        self.assertEqual("partial", result["status"])
        self.assertEqual(1, result["processed_count"])
        self.assertEqual({"resolution_evidence": 2}, result["selected_count_by_stored_reason"])
        self.assertEqual(1, result["rate_limited_count"])
        self.assertEqual(1, result["requests_total"])
        self.assertIsNotNone(result["cooldown_until"])
        self.assertEqual(["sibling-track-1"], result["track_spotify_ids_processed"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            rows = connection.execute(
                """
                SELECT spotify_id, reason, status, attempts
                FROM spotify_catalog_backfill_queue
                WHERE spotify_id IN ('sibling-track-1', 'sibling-track-2', 'identity-track')
                ORDER BY spotify_id
                """
            ).fetchall()
        self.assertEqual(
            [
                ("identity-track", "identity_metadata", "pending", 0),
                ("sibling-track-1", "resolution_evidence", "pending", 1),
                ("sibling-track-2", "resolution_evidence", "pending", 0),
            ],
            rows,
        )
        self.assertEqual(1, len(calls))

    def test_queue_list_does_not_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("track", "t1", 1, "pending", "2026-04-27T11:00:00Z", 0),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        _ = list_spotify_catalog_backfill_queue(status_filter=None, limit=50, offset=0)

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_queue_album_metadata_complete_but_tracklist_incomplete_stays_pending(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-q1", 2, "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-q1", "t1", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "alb-q1", "seed", 80, "pending", "2026-04-28T00:00:00Z", 0),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": []}, None
            if url.endswith("/v1/albums/alb-q1"):
                return 200, {}, _album_payload("alb-q1"), None
            if url.endswith("/v1/albums/alb-q1/tracks"):
                return 200, {}, {"items": [], "next": None}, None
            raise AssertionError(url)

        _ = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            limit=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status FROM spotify_catalog_backfill_queue WHERE entity_type = 'album' AND spotify_id = ?",
                ("alb-q1",),
            ).fetchone()
        self.assertEqual("pending", str(row[0]))

    def test_queue_album_marked_done_after_tracklist_complete(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-q2", 2, "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-q2", "t1", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "alb-q2", "seed", 80, "pending", "2026-04-28T00:00:00Z", 0),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": []}, None
            if url.endswith("/v1/albums/alb-q2"):
                return 200, {}, _album_payload("alb-q2"), None
            if url.endswith("/v1/albums/alb-q2/tracks"):
                return 200, {}, {"items": [_track_payload("t2", "alb-q2")], "next": None}, None
            raise AssertionError(url)

        _ = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            limit=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status FROM spotify_catalog_backfill_queue WHERE entity_type = 'album' AND spotify_id = ?",
                ("alb-q2",),
            ).fetchone()
        self.assertEqual("done", str(row[0]))

    def test_queue_track_not_done_until_duration_and_album_present(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("track", "trk-q1", "seed", 80, "pending", "2026-04-28T00:00:00Z", 0),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks/trk-q1"):
                payload = _track_payload("trk-q1", "")
                payload["duration_ms"] = None
                payload["album"] = {}
                return 200, {}, payload, None
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": []}, None
            raise AssertionError(url)

        _ = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status FROM spotify_catalog_backfill_queue WHERE entity_type = 'track' AND spotify_id = ?",
                ("trk-q1",),
            ).fetchone()
        self.assertEqual("pending", str(row[0]))

    def test_pending_already_complete_queue_item_marked_done_without_spotify_call(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, album_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("trk-complete", 180000, "alb-complete", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("track", "trk-complete", "seed", 80, "pending", "2026-04-28T00:00:00Z", 0),
            )
            connection.commit()

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            raise AssertionError("No Spotify calls should be made for already-complete pending queue items")

        _ = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            limit=1,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status FROM spotify_catalog_backfill_queue WHERE entity_type = 'track' AND spotify_id = ?",
                ("trk-complete",),
            ).fetchone()
        self.assertEqual("done", str(row[0]))

    def test_done_but_incomplete_item_reopens_on_enqueue(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "alb-reopen", "seed", 10, "done", "2026-04-28T00:00:00Z", 0),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-reopen", 2, "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-reopen", "t1", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.commit()

        result = enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "album", "spotify_id": "alb-reopen", "reason": "again", "priority": 80}]
        )
        self.assertTrue(result["ok"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status, priority FROM spotify_catalog_backfill_queue WHERE entity_type = 'album' AND spotify_id = ?",
                ("alb-reopen",),
            ).fetchone()
        self.assertEqual("pending", str(row[0]))
        self.assertEqual(80, int(row[1]))

    def test_repair_queue_done_incomplete_album_becomes_pending(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-pending", 2, "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-pending", "track-1", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "alb-repair-pending", "seed", 50, "done", "2026-04-28T00:00:00Z", 0),
            )
            connection.commit()

        result = repair_spotify_catalog_backfill_queue_statuses()
        self.assertTrue(result["ok"])
        self.assertEqual(1, result["repaired_to_pending"])
        self.assertEqual(0, result["repaired_to_done"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status FROM spotify_catalog_backfill_queue WHERE entity_type = 'album' AND spotify_id = ?",
                ("alb-repair-pending",),
            ).fetchone()
        self.assertEqual("pending", str(row[0]))

    def test_repair_queue_pending_complete_album_becomes_done(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-done", 2, "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-done", "track-1", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-done", "track-2", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "alb-repair-done", "seed", 50, "pending", "2026-04-28T00:00:00Z", 0),
            )
            connection.commit()

        result = repair_spotify_catalog_backfill_queue_statuses()
        self.assertTrue(result["ok"])
        self.assertEqual(0, result["repaired_to_pending"])
        self.assertEqual(1, result["repaired_to_done"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status FROM spotify_catalog_backfill_queue WHERE entity_type = 'album' AND spotify_id = ?",
                ("alb-repair-done",),
            ).fetchone()
        self.assertEqual("done", str(row[0]))

    def test_skip_existing_uses_album_completeness_helper(self) -> None:
        self._seed_source_tracks(["t1"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", 2, "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("a1", "a1-x", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.commit()

        album_batch_called = {"value": False}

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks"):
                return 200, {}, {"tracks": [_track_payload("t1", "a1")]}, None
            if url.endswith("/v1/albums") and "/v1/albums/" not in url:
                album_batch_called["value"] = True
                return 200, {}, {"albums": [_album_payload("a1")]}, None
            if url.endswith("/v1/albums/a1/tracks"):
                return 200, {}, {"items": [_track_payload("a1-y", "a1")], "next": None}, None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=True,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertFalse(album_batch_called["value"])

    def test_search_albums_not_backfilled_album(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Unmapped Album", "unmapped album"),
                ).lastrowid
            )
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist U",)).lastrowid)
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (release_album_id, artist_id),
            )
            connection.commit()

        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.get("/debug/search/albums?catalog_status=not_backfilled&limit=50&offset=0")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(1, body["total"])
        self.assertEqual("Unmapped Album", body["items"][0]["release_album_name"])
        self.assertIsNone(body["items"][0]["spotify_album_id"])

    def test_search_albums_backfilled_tracklist_complete_and_incomplete(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            ra_complete = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Complete Album", "complete album")).lastrowid)
            ra_incomplete = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Incomplete Album", "incomplete album")).lastrowid)
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist A",)).lastrowid)
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)", (ra_complete, artist_id))
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)", (ra_incomplete, artist_id))

            sa_complete = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-complete", "spotify:album:alb-complete", "Complete Album", "{}"),
                ).lastrowid
            )
            sa_incomplete = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-incomplete", "spotify:album:alb-incomplete", "Incomplete Album", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (sa_complete, ra_complete),
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (sa_incomplete, ra_incomplete),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, total_tracks, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                ("alb-complete", "Spotify Complete Album", 2, "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, total_tracks, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                ("alb-incomplete", "Spotify Incomplete Album", 3, "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                "INSERT INTO spotify_album_track (spotify_album_id, spotify_track_id, fetched_at, last_status) VALUES (?, ?, ?, ?)",
                ("alb-complete", "c1", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                "INSERT INTO spotify_album_track (spotify_album_id, spotify_track_id, fetched_at, last_status) VALUES (?, ?, ?, ?)",
                ("alb-complete", "c2", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                "INSERT INTO spotify_album_track (spotify_album_id, spotify_track_id, fetched_at, last_status) VALUES (?, ?, ?, ?)",
                ("alb-incomplete", "i1", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.commit()

        complete_payload = search_album_catalog_lookup(catalog_status="tracklist_complete", limit=50, offset=0)
        self.assertTrue(complete_payload["ok"])
        self.assertEqual(1, complete_payload["total"])
        self.assertEqual("Complete Album", complete_payload["items"][0]["release_album_name"])
        self.assertTrue(complete_payload["items"][0]["tracklist_complete"])

        incomplete_payload = search_album_catalog_lookup(catalog_status="tracklist_incomplete", limit=50, offset=0)
        self.assertTrue(incomplete_payload["ok"])
        self.assertEqual(1, incomplete_payload["total"])
        self.assertEqual("Incomplete Album", incomplete_payload["items"][0]["release_album_name"])
        self.assertFalse(incomplete_payload["items"][0]["tracklist_complete"])

    def test_search_albums_status_filters_and_q_filter(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            ra_error = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Error Album", "error album")).lastrowid)
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist E",)).lastrowid)
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)", (ra_error, artist_id))
            sa_error = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-error", "spotify:album:alb-error", "Error Album", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (sa_error, ra_error),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, total_tracks, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-error", "Spotify Error Album", 2, "2026-04-27T12:00:00Z", "error", "failed fetch"),
            )
            connection.commit()

        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            error_response = client.get("/debug/search/albums?catalog_status=error")
            q_response = client.get("/debug/search/albums?q=alb-error")
        self.assertEqual(200, error_response.status_code)
        error_body = error_response.json()
        self.assertEqual(1, error_body["total"])
        self.assertEqual("Error Album", error_body["items"][0]["release_album_name"])
        self.assertEqual(200, q_response.status_code)
        q_body = q_response.json()
        self.assertEqual(1, q_body["total"])
        self.assertEqual("alb-error", q_body["items"][0]["spotify_album_id"])

    def test_search_albums_resolves_spotify_id_from_raw_play_event_track_path(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Witch: We Intend to Cause Havoc!", "witch: we intend to cause havoc!"),
                ).lastrowid
            )
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("WITCH",)).lastrowid)
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (release_album_id, artist_id),
            )
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Lazy Bones!!", "lazy bones!!"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (release_album_id, release_track_id),
            )
            source_track_id = int(
                connection.execute(
                    """
                    INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("spotify", "trk-witch-1", "spotify:track:trk-witch-1", "Lazy Bones!!", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_id, release_track_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, total_tracks, artists_json, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "28FR52kMwgdiIINFuzYP1q",
                    "Witch: We Intend to Cause Havoc!",
                    54,
                    json.dumps([{"name": "WITCH"}]),
                    "2026-04-28T00:00:00Z",
                    "ok",
                ),
            )
            connection.execute(
                "INSERT INTO spotify_album_track (spotify_album_id, spotify_track_id, fetched_at, last_status) VALUES (?, ?, ?, ?)",
                ("28FR52kMwgdiIINFuzYP1q", "w1", "2026-04-28T00:00:00Z", "ok"),
            )
            connection.execute(
                "INSERT INTO spotify_album_track (spotify_album_id, spotify_track_id, fetched_at, last_status) VALUES (?, ?, ?, ?)",
                ("28FR52kMwgdiIINFuzYP1q", "w2", "2026-04-28T00:00:00Z", "ok"),
            )
            connection.commit()

        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="witch-listen-1",
            played_at="2026-04-28T01:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="trk-witch-1",
            spotify_album_id="28FR52kMwgdiIINFuzYP1q",
        )
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="witch-listen-2",
            played_at="2026-04-28T01:01:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="trk-witch-1",
            spotify_album_id="28FR52kMwgdiIINFuzYP1q",
        )

        payload = search_album_catalog_lookup(q="Witch: We Intend to Cause Havoc!", catalog_status="all", limit=50, offset=0)
        self.assertTrue(payload["ok"])
        self.assertEqual(1, payload["total"])
        item = payload["items"][0]
        self.assertEqual("28FR52kMwgdiIINFuzYP1q", item["spotify_album_id"])
        self.assertEqual("Witch: We Intend to Cause Havoc!", item["spotify_album_name"])
        self.assertEqual(54, item["total_tracks"])
        self.assertEqual(2, item["album_track_rows"])
        self.assertFalse(item["tracklist_complete"])

    def test_search_albums_does_not_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        _ = search_album_catalog_lookup(catalog_status="all", limit=50, offset=0)

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_search_album_duplicates_groups_release_albums_by_resolved_spotify_album_id(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Dup Artist",)).lastrowid)
            duplicate_release_ids: list[int] = []
            for name in ("Dup Album One", "Dup Album Two"):
                release_album_id = int(
                    connection.execute(
                        "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                        (name, name.lower()),
                    ).lastrowid
                )
                duplicate_release_ids.append(release_album_id)
                connection.execute(
                    "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                    (release_album_id, artist_id),
                )
            source_album_id = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-dup-1", "spotify:album:alb-dup-1", "Dup Album One", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_album_id, duplicate_release_ids[0]),
            )
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Dup Track Two", "dup track two"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (duplicate_release_ids[1], release_track_id),
            )
            source_track_id = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "dup-track-2", "spotify:track:dup-track-2", "Dup Track Two", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_id, release_track_id),
            )
            single_release_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Single Album", "single album"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (single_release_id, artist_id),
            )
            single_source_album_id = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-single-1", "spotify:album:alb-single-1", "Single Album", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (single_source_album_id, single_release_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, total_tracks, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                ("alb-dup-1", "Spotify Duplicate Album", 2, "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, total_tracks, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                ("alb-single-1", "Spotify Single Album", 10, "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                "INSERT INTO spotify_album_track (spotify_album_id, spotify_track_id, fetched_at, last_status) VALUES (?, ?, ?, ?)",
                ("alb-dup-1", "d1", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                "INSERT INTO spotify_album_track (spotify_album_id, spotify_track_id, fetched_at, last_status) VALUES (?, ?, ?, ?)",
                ("alb-dup-1", "d2", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("album", "alb-dup-1", 80, "pending", "2026-04-27T11:00:00Z", 1),
            )
            connection.commit()

        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="dup-listen-2",
            played_at="2026-04-28T01:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json="{}",
            spotify_track_id="dup-track-2",
            spotify_album_id="alb-dup-1",
        )

        payload = search_album_catalog_duplicate_spotify_identities(limit=200, offset=0)
        self.assertTrue(payload["ok"])
        self.assertEqual(1, payload["total"])
        self.assertEqual(1, len(payload["items"]))
        group = payload["items"][0]
        self.assertEqual("alb-dup-1", group["spotify_album_id"])
        self.assertEqual("Spotify Duplicate Album", group["spotify_album_name"])
        self.assertEqual(2, group["duplicate_count"])
        self.assertEqual(2, len(group["release_albums"]))
        grouped_release_ids = {item["release_album_id"] for item in group["release_albums"]}
        self.assertEqual(set(duplicate_release_ids), grouped_release_ids)
        self.assertNotIn(single_release_id, grouped_release_ids)
        self.assertEqual({"pending"}, {item["queue_status"] for item in group["release_albums"]})

    def test_search_album_duplicates_does_not_call_spotify(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.get("/debug/search/albums/duplicates?limit=200&offset=0")
        self.assertEqual(200, response.status_code)
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_search_album_duplicates_does_not_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        _ = search_album_catalog_duplicate_spotify_identities(limit=200, offset=0)

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_search_album_duplicates_by_name_groups_expected_rows(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_tele_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Telekinesis",)).lastrowid)
            artist_tele_variant_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Telekinesis, Telekinesis",)).lastrowid)
            artist_other_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Other Artist",)).lastrowid)

            tele_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Telekinesis", "telekinesis")).lastrowid)
            tele_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Telekinesis", "telekinesis")).lastrowid)
            tele_other_artist_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Telekinesis", "telekinesis")).lastrowid)
            different_name_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Different Album", "different album")).lastrowid)

            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)", (tele_one_id, artist_tele_id))
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (tele_two_id, artist_tele_id),
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 2)",
                (tele_two_id, artist_tele_variant_id),
            )
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)", (tele_other_artist_id, artist_other_id))
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)", (different_name_id, artist_tele_id))

            source_one = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-tel-1", "spotify:album:alb-tel-1", "Telekinesis", "{}"),
                ).lastrowid
            )
            source_other_artist = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-other-artist", "spotify:album:alb-other-artist", "Telekinesis", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_one, tele_one_id),
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_other_artist, tele_other_artist_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, total_tracks, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                ("alb-tel-1", "Telekinesis", 11, "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, total_tracks, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                ("alb-other-artist", "Telekinesis", 11, "2026-04-27T12:00:00Z", "ok"),
            )
            connection.commit()

        payload = search_album_catalog_duplicate_by_name_identities(limit=200, offset=0)
        self.assertTrue(payload["ok"])
        self.assertEqual(1, payload["total"])
        self.assertEqual(1, len(payload["items"]))
        group = payload["items"][0]
        self.assertEqual("telekinesis", group["normalized_album_name"])
        self.assertEqual("telekinesis", group["normalized_primary_artist"])
        self.assertEqual(2, group["duplicate_count"])
        grouped_release_ids = {item["release_album_id"] for item in group["release_albums"]}
        self.assertEqual({tele_one_id, tele_two_id}, grouped_release_ids)
        self.assertNotIn(tele_other_artist_id, grouped_release_ids)
        self.assertNotIn(different_name_id, grouped_release_ids)
        self.assertIn("alb-tel-1", group["spotify_album_ids"])

    def test_search_album_duplicates_by_name_does_not_call_spotify(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.get("/debug/search/albums/duplicates-by-name?limit=200&offset=0")
        self.assertEqual(200, response.status_code)
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_search_album_duplicates_by_name_does_not_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        _ = search_album_catalog_duplicate_by_name_identities(limit=200, offset=0)

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_release_album_merge_preview_chooses_deterministic_survivor_and_lists_affected_rows(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist A",)).lastrowid)
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_three_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            track_one_id = int(connection.execute("INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)", ("Track 1", "track 1")).lastrowid)
            track_two_id = int(connection.execute("INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)", ("Track 2", "track 2")).lastrowid)
            for album_id in (album_one_id, album_two_id, album_three_id):
                connection.execute(
                    "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                    (album_id, artist_id),
                )
            connection.execute("INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)", (album_one_id, track_one_id))
            connection.execute("INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)", (album_two_id, track_one_id))
            connection.execute("INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)", (album_three_id, track_two_id))
            source_two_id = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-survivor", "spotify:album:alb-survivor", "Album A", "{}"),
                ).lastrowid
            )
            source_three_id = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "alb-duplicate", "spotify:album:alb-duplicate", "Album A", "{}"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')",
                (source_two_id, album_two_id),
            )
            connection.execute(
                "INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'suggested', 0, 'seed')",
                (source_three_id, album_three_id),
            )
            connection.execute(
                "INSERT INTO spotify_album_catalog (spotify_album_id, name, fetched_at, last_status) VALUES (?, ?, ?, ?)",
                ("alb-survivor", "Album A", "2026-04-29T12:00:00Z", "ok"),
            )
            connection.commit()
            insert_raw_play_event(
                source_type="spotify_recent",
                source_row_key="merge-preview-row",
                played_at="2026-04-29T12:00:00Z",
                ms_played=1000,
                ms_played_method="history_source",
                raw_payload_json="{}",
                track_name_raw="Track 1",
                artist_name_raw="Artist A",
                album_name_raw="Album A",
                spotify_album_id="alb-survivor",
            )

        payload = preview_release_album_merge([album_one_id, album_two_id, album_three_id])

        self.assertTrue(payload["ok"])
        self.assertEqual(album_two_id, payload["survivor_release_album_id"])
        self.assertEqual([album_one_id, album_three_id], payload["merge_release_album_ids"])
        self.assertEqual(1, payload["affected"]["source_album_map_rows"])
        self.assertEqual(2, payload["affected"]["album_artist_rows"])
        self.assertEqual(2, payload["affected"]["album_track_rows"])
        self.assertEqual(1, payload["affected"]["album_track_conflicts"])
        self.assertEqual(2, payload["affected"]["release_track_rows"])
        self.assertEqual(1, payload["affected"]["raw_play_event_rows"])
        self.assertEqual("needs_review", payload["merge_readiness"])
        self.assertTrue(any("album_track.release_album_id" in operation for operation in payload["proposed_operations"]))
        self.assertTrue(any("release_track rows directly" in operation for operation in payload["proposed_operations"]))

    def test_release_album_merge_preview_safe_candidate(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist A",)).lastrowid)
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            source_one_id = int(connection.execute("INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)", ("spotify", "alb-safe", "spotify:album:alb-safe", "Album A", "{}")).lastrowid)
            for album_id in (album_one_id, album_two_id):
                connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)", (album_id, artist_id))
            connection.execute("INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')", (source_one_id, album_one_id))
            connection.execute("INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')", (source_one_id, album_two_id))
            connection.execute("INSERT INTO spotify_album_catalog (spotify_album_id, name, fetched_at, last_status) VALUES (?, ?, ?, ?)", ("alb-safe", "Album A", "2026-04-29T12:00:00Z", "ok"))
            connection.commit()

        payload = preview_release_album_merge([album_one_id, album_two_id])

        self.assertEqual("safe_candidate", payload["merge_readiness"])
        self.assertEqual(0, payload["affected"]["album_track_conflicts"])

    def test_release_album_merge_preview_multiple_spotify_ids_needs_review(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist A",)).lastrowid)
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            source_one_id = int(connection.execute("INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)", ("spotify", "alb-one", "spotify:album:alb-one", "Album A", "{}")).lastrowid)
            source_two_id = int(connection.execute("INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)", ("spotify", "alb-two", "spotify:album:alb-two", "Album A", "{}")).lastrowid)
            for album_id in (album_one_id, album_two_id):
                connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)", (album_id, artist_id))
            connection.execute("INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')", (source_one_id, album_one_id))
            connection.execute("INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')", (source_two_id, album_two_id))
            connection.commit()

        payload = preview_release_album_merge([album_one_id, album_two_id])

        self.assertEqual("needs_review", payload["merge_readiness"])
        self.assertTrue(any("Multiple distinct Spotify album IDs" in reason for reason in payload["readiness_reasons"]))

    def test_release_album_merge_preview_different_name_or_artist_unsafe(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_one_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist A",)).lastrowid)
            artist_two_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist B",)).lastrowid)
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album B", "album b")).lastrowid)
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)", (album_one_id, artist_one_id))
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)", (album_two_id, artist_two_id))
            connection.commit()

        payload = preview_release_album_merge([album_one_id, album_two_id])

        self.assertEqual("unsafe", payload["merge_readiness"])
        self.assertTrue(any("different normalized album names" in reason for reason in payload["readiness_reasons"]))
        self.assertTrue(any("different normalized primary artists" in reason for reason in payload["readiness_reasons"]))

    def test_release_album_merge_preview_missing_ids_unsafe(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            connection.commit()

        payload = preview_release_album_merge([album_one_id, album_two_id, 999999])

        self.assertEqual("unsafe", payload["merge_readiness"])
        self.assertTrue(any("not found" in reason for reason in payload["readiness_reasons"]))

    def test_release_album_merge_preview_invalid_single_id_returns_warning(self) -> None:
        payload = preview_release_album_merge([1])

        self.assertFalse(payload["ok"])
        self.assertIsNone(payload["survivor_release_album_id"])
        self.assertEqual("unsafe", payload["merge_readiness"])
        self.assertTrue(payload["warnings"])

    def test_release_album_merge_preview_endpoint_does_not_call_spotify(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.post("/debug/identity/release-albums/merge-preview", json={"release_album_ids": [1]})
        self.assertEqual(200, response.status_code)
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_release_album_merge_preview_does_not_write_or_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            release_track_id = int(connection.execute("INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)", ("Track A", "track a")).lastrowid)
            analysis_track_id = int(connection.execute("INSERT INTO analysis_track (primary_name) VALUES (?)", ("Track A",)).lastrowid)
            connection.execute(
                "INSERT INTO analysis_track_map (release_track_id, analysis_track_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'seed', 0.5, 'suggested', 0, 'seed')",
                (release_track_id, analysis_track_id),
            )
            connection.commit()
            before_digest = self._identity_album_digest(connection)
            before_analysis_count, before_analysis_digest = self._analysis_track_map_digest(connection)

        _ = preview_release_album_merge([album_one_id, album_two_id])

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_digest = self._identity_album_digest(connection)
            after_analysis_count, after_analysis_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_digest, after_digest)
        self.assertEqual(before_analysis_count, after_analysis_count)
        self.assertEqual(before_analysis_digest, after_analysis_digest)

    def test_release_album_merge_dry_run_blocked_for_unsafe(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_one_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist A",)).lastrowid)
            artist_two_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist B",)).lastrowid)
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album B", "album b")).lastrowid)
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)", (album_one_id, artist_one_id))
            connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)", (album_two_id, artist_two_id))
            connection.commit()

        payload = dry_run_release_album_merge([album_one_id, album_two_id], survivor_release_album_id=album_one_id)

        self.assertFalse(payload["ok"])
        self.assertTrue(payload["blocked"])
        self.assertEqual("unsafe", payload["merge_readiness"])
        self.assertTrue(payload["blocked_reasons"])

    def test_release_album_merge_dry_run_allowed_for_safe_candidate(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist A",)).lastrowid)
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            source_id = int(connection.execute("INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)", ("spotify", "alb-safe-dry", "spotify:album:alb-safe-dry", "Album A", "{}")).lastrowid)
            for album_id in (album_one_id, album_two_id):
                connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)", (album_id, artist_id))
                connection.execute("INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')", (source_id, album_id))
            connection.execute("INSERT INTO spotify_album_catalog (spotify_album_id, name, fetched_at, last_status) VALUES (?, ?, ?, ?)", ("alb-safe-dry", "Album A", "2026-04-29T12:00:00Z", "ok"))
            connection.commit()

        preview = preview_release_album_merge([album_one_id, album_two_id])
        payload = dry_run_release_album_merge(
            [album_one_id, album_two_id],
            survivor_release_album_id=preview["survivor_release_album_id"],
        )

        self.assertTrue(payload["ok"])
        self.assertFalse(payload["blocked"])
        self.assertEqual("safe_candidate", payload["merge_readiness"])
        self.assertEqual(1, payload["rows_affected"]["source_album_map"])
        self.assertEqual(1, payload["rows_affected"]["release_album_retire"])

    def test_release_album_merge_dry_run_includes_album_track_repoints_and_conflicts(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Artist A",)).lastrowid)
            survivor_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            duplicate_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            source_id = int(connection.execute("INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)", ("spotify", "alb-conflict-dry", "spotify:album:alb-conflict-dry", "Album A", "{}")).lastrowid)
            source_duplicate_id = int(connection.execute("INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)", ("spotify", "alb-conflict-dry-other", "spotify:album:alb-conflict-dry-other", "Album A", "{}")).lastrowid)
            conflict_track_id = int(connection.execute("INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)", ("Track 1", "track 1")).lastrowid)
            repoint_track_id = int(connection.execute("INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)", ("Track 2", "track 2")).lastrowid)
            for album_id in (survivor_id, duplicate_id):
                connection.execute("INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)", (album_id, artist_id))
            connection.execute("INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')", (source_id, survivor_id))
            connection.execute("INSERT INTO source_album_map (source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'provider_identity', 1.0, 'suggested', 0, 'seed')", (source_duplicate_id, duplicate_id))
            connection.execute("INSERT INTO spotify_album_catalog (spotify_album_id, name, fetched_at, last_status) VALUES (?, ?, ?, ?)", ("alb-conflict-dry", "Album A", "2026-04-29T12:00:00Z", "ok"))
            connection.execute("INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)", (survivor_id, conflict_track_id))
            connection.execute("INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)", (duplicate_id, conflict_track_id))
            connection.execute("INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)", (duplicate_id, repoint_track_id))
            connection.commit()

        preview = preview_release_album_merge([survivor_id, duplicate_id])
        payload = dry_run_release_album_merge(
            [survivor_id, duplicate_id],
            survivor_release_album_id=preview["survivor_release_album_id"],
        )

        self.assertTrue(payload["ok"])
        self.assertEqual("needs_review", payload["merge_readiness"])
        self.assertEqual(1, payload["rows_affected"]["album_track_repoint"])
        self.assertEqual(1, payload["rows_affected"]["album_track_conflict_delete"])
        self.assertEqual(1, len(payload["plan"]["album_track_repoints"]))
        self.assertEqual(1, len(payload["plan"]["album_track_conflicts"]))

    def test_release_album_merge_dry_run_does_not_write_or_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            album_one_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            album_two_id = int(connection.execute("INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)", ("Album A", "album a")).lastrowid)
            release_track_id = int(connection.execute("INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)", ("Track A", "track a")).lastrowid)
            analysis_track_id = int(connection.execute("INSERT INTO analysis_track (primary_name) VALUES (?)", ("Track A",)).lastrowid)
            connection.execute(
                "INSERT INTO analysis_track_map (release_track_id, analysis_track_id, match_method, confidence, status, is_user_confirmed, explanation) VALUES (?, ?, 'seed', 0.5, 'suggested', 0, 'seed')",
                (release_track_id, analysis_track_id),
            )
            connection.commit()
            before_digest = self._identity_album_digest(connection)
            before_analysis_count, before_analysis_digest = self._analysis_track_map_digest(connection)

        _ = dry_run_release_album_merge([album_one_id, album_two_id], survivor_release_album_id=album_one_id)

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_digest = self._identity_album_digest(connection)
            after_analysis_count, after_analysis_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_digest, after_digest)
        self.assertEqual(before_analysis_count, after_analysis_count)
        self.assertEqual(before_analysis_digest, after_analysis_digest)

    def test_release_album_merge_dry_run_endpoint_does_not_call_spotify(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.post(
                "/debug/identity/release-albums/merge-dry-run",
                json={"release_album_ids": [1], "survivor_release_album_id": 1},
            )
        self.assertEqual(200, response.status_code)
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_search_tracks_not_backfilled_and_backfilled(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Track Artist",)).lastrowid)
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track Album", "track album"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (release_album_id, artist_id),
            )

            not_backfilled_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track Missing", "track missing"),
                ).lastrowid
            )
            backfilled_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track Backfilled", "track backfilled"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (not_backfilled_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (backfilled_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (release_album_id, not_backfilled_track_id),
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (release_album_id, backfilled_track_id),
            )

            source_track_id = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "trk-backfilled", "spotify:track:trk-backfilled", "Track Backfilled", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_id, backfilled_track_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, album_id, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("trk-backfilled", "Spotify Backfilled", 181000, "alb-1", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.commit()

        not_backfilled_payload = search_track_catalog_lookup(catalog_status="not_backfilled", limit=50, offset=0)
        self.assertTrue(not_backfilled_payload["ok"])
        self.assertEqual(1, not_backfilled_payload["total"])
        self.assertEqual("Track Missing", not_backfilled_payload["items"][0]["release_track_name"])
        self.assertIsNone(not_backfilled_payload["items"][0]["spotify_track_id"])

        backfilled_payload = search_track_catalog_lookup(catalog_status="backfilled", limit=50, offset=0)
        self.assertTrue(backfilled_payload["ok"])
        self.assertEqual(1, backfilled_payload["total"])
        self.assertEqual("Track Backfilled", backfilled_payload["items"][0]["release_track_name"])
        self.assertEqual("trk-backfilled", backfilled_payload["items"][0]["spotify_track_id"])
        self.assertEqual("3:01", backfilled_payload["items"][0]["duration_display"])

    def test_search_tracks_duration_missing_error_and_q_filter(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Track Artist B",)).lastrowid)
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Lookup Album", "lookup album"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (release_album_id, artist_id),
            )
            duration_missing_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track No Duration", "track no duration"),
                ).lastrowid
            )
            error_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track Error", "track error"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (duration_missing_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (error_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (release_album_id, duration_missing_track_id),
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (release_album_id, error_track_id),
            )

            source_track_duration_missing = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "trk-no-duration", "spotify:track:trk-no-duration", "Track No Duration", "{}"),
                ).lastrowid
            )
            source_track_error = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "trk-error", "spotify:track:trk-error", "Track Error", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_duration_missing, duration_missing_track_id),
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_error, error_track_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, album_id, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("trk-no-duration", "Spotify No Duration", None, "alb-2", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, album_id, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("trk-error", "Spotify Error", None, None, "2026-04-27T12:00:00Z", "error", "failed fetch"),
            )
            connection.commit()

        duration_missing_payload = search_track_catalog_lookup(catalog_status="duration_missing", limit=50, offset=0)
        self.assertTrue(duration_missing_payload["ok"])
        self.assertEqual(2, duration_missing_payload["total"])
        self.assertEqual(
            {"Track Error", "Track No Duration"},
            {item["release_track_name"] for item in duration_missing_payload["items"]},
        )

        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            error_response = client.get("/debug/search/tracks?catalog_status=error")
            q_response = client.get("/debug/search/tracks?q=trk-error")
        self.assertEqual(200, error_response.status_code)
        error_body = error_response.json()
        self.assertEqual(1, error_body["total"])
        self.assertEqual("Track Error", error_body["items"][0]["release_track_name"])
        self.assertEqual(200, q_response.status_code)
        q_body = q_response.json()
        self.assertEqual(1, q_body["total"])
        self.assertEqual("trk-error", q_body["items"][0]["spotify_track_id"])

    def test_search_tracks_does_not_call_spotify(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.get("/debug/search/tracks?catalog_status=all&queue_status=all")
        self.assertEqual(200, response.status_code)
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_search_tracks_does_not_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        _ = search_track_catalog_lookup(catalog_status="all", limit=50, offset=0)

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_search_track_duplicates_groups_release_tracks_by_resolved_spotify_track_id(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Dup Track Artist",)).lastrowid)
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Dup Track Album", "dup track album"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (release_album_id, artist_id),
            )

            duplicate_release_track_ids: list[int] = []
            for name in ("Dup Track One", "Dup Track Two"):
                release_track_id = int(
                    connection.execute(
                        "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                        (name, name.lower()),
                    ).lastrowid
                )
                duplicate_release_track_ids.append(release_track_id)
                connection.execute(
                    "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                    (release_track_id, artist_id),
                )
                connection.execute(
                    "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                    (release_album_id, release_track_id),
                )

            singleton_release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Singleton Track", "singleton track"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (singleton_release_track_id, artist_id),
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (release_album_id, singleton_release_track_id),
            )

            duplicate_source_track_id = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "trk-dup-1", "spotify:track:trk-dup-1", "Dup Track One", "{}"),
                ).lastrowid
            )
            for release_track_id in duplicate_release_track_ids:
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                    """,
                    (duplicate_source_track_id, release_track_id),
                )

            singleton_source_track_id = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "trk-single-1", "spotify:track:trk-single-1", "Singleton Track", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (singleton_source_track_id, singleton_release_track_id),
            )

            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, album_id, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("trk-dup-1", "Spotify Duplicate Track", 181000, "alb-dup-1", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, album_id, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("trk-single-1", "Spotify Singleton Track", 200000, "alb-single-1", "2026-04-27T12:00:00Z", "ok"),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("track", "trk-dup-1", 80, "pending", "2026-04-27T11:00:00Z", 1),
            )
            connection.commit()

        payload = search_track_catalog_duplicate_spotify_identities(limit=200, offset=0)
        self.assertTrue(payload["ok"])
        self.assertEqual(1, payload["total"])
        self.assertEqual(1, len(payload["items"]))
        group = payload["items"][0]
        self.assertEqual("trk-dup-1", group["spotify_track_id"])
        self.assertEqual("Spotify Duplicate Track", group["spotify_track_name"])
        self.assertEqual(181000, group["duration_ms"])
        self.assertEqual("3:01", group["duration_display"])
        self.assertEqual(2, group["duplicate_count"])
        self.assertEqual(2, len(group["release_tracks"]))
        grouped_release_track_ids = {item["release_track_id"] for item in group["release_tracks"]}
        self.assertEqual(set(duplicate_release_track_ids), grouped_release_track_ids)
        self.assertNotIn(singleton_release_track_id, grouped_release_track_ids)
        self.assertEqual({"pending"}, {item["queue_status"] for item in group["release_tracks"]})

    def test_search_track_duplicates_does_not_call_spotify(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.get("/debug/search/tracks/duplicates?limit=200&offset=0")
        self.assertEqual(200, response.status_code)
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_search_track_duplicates_does_not_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        _ = search_track_catalog_duplicate_spotify_identities(limit=200, offset=0)

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_search_albums_queue_status_filters_and_combined_filters(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Queue Artist Album",)).lastrowid)

            def _seed_album(name: str, spotify_album_id: str, *, catalog_status: str = "ok", catalog_error: str | None = None) -> None:
                release_album_id = int(
                    connection.execute(
                        "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                        (name, name.lower()),
                    ).lastrowid
                )
                connection.execute(
                    "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                    (release_album_id, artist_id),
                )
                source_album_id = int(
                    connection.execute(
                        "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                        ("spotify", spotify_album_id, f"spotify:album:{spotify_album_id}", name, "{}"),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_album_map (
                      source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_album_id, release_album_id),
                )
                connection.execute(
                    """
                    INSERT INTO spotify_album_catalog (
                      spotify_album_id, name, total_tracks, fetched_at, last_status, last_error
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (spotify_album_id, f"Spotify {name}", 2, "2026-04-27T12:00:00Z", catalog_status, catalog_error),
                )

            _seed_album("Album Pending", "alb-pending", catalog_status="error", catalog_error="catalog failed")
            _seed_album("Album Done", "alb-done")
            _seed_album("Album Queue Error", "alb-queue-error")
            _seed_album("Album Not Queued", "alb-not-queued")
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("album", "alb-pending", 80, "pending", "2026-04-27T11:00:00Z", 1),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("album", "alb-done", 40, "done", "2026-04-27T11:00:00Z", 2),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts, last_error) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("album", "alb-queue-error", 30, "error", "2026-04-27T11:00:00Z", 3, "queue failure"),
            )
            connection.commit()

        pending_payload = search_album_catalog_lookup(catalog_status="all", queue_status="pending", limit=50, offset=0)
        done_payload = search_album_catalog_lookup(catalog_status="all", queue_status="done", limit=50, offset=0)
        error_payload = search_album_catalog_lookup(catalog_status="all", queue_status="error", limit=50, offset=0)
        not_queued_payload = search_album_catalog_lookup(catalog_status="all", queue_status="not_queued", limit=50, offset=0)
        combined_payload = search_album_catalog_lookup(catalog_status="error", queue_status="pending", limit=50, offset=0)

        self.assertEqual({"alb-pending"}, {item["spotify_album_id"] for item in pending_payload["items"]})
        self.assertEqual({"alb-done"}, {item["spotify_album_id"] for item in done_payload["items"]})
        self.assertEqual({"alb-queue-error"}, {item["spotify_album_id"] for item in error_payload["items"]})
        self.assertEqual({"alb-not-queued"}, {item["spotify_album_id"] for item in not_queued_payload["items"]})
        self.assertEqual(1, combined_payload["total"])
        self.assertEqual("alb-pending", combined_payload["items"][0]["spotify_album_id"])
        self.assertEqual("pending", combined_payload["items"][0]["queue_status"])
        self.assertIn("queue_priority", combined_payload["items"][0])
        self.assertIn("queue_requested_at", combined_payload["items"][0])
        self.assertIn("queue_attempts", combined_payload["items"][0])
        self.assertIn("queue_last_error", combined_payload["items"][0])

        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            endpoint_response = client.get("/debug/search/albums?catalog_status=all&queue_status=pending")
        self.assertEqual(200, endpoint_response.status_code)
        self.assertEqual(1, endpoint_response.json().get("total"))

    def test_search_albums_sort_recently_backfilled_and_name(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Album Sort Artist",)).lastrowid)
            seeded = [
                ("Beta Album", "alb-sort-beta", "2026-04-27T12:00:01Z"),
                ("Alpha Album", "alb-sort-alpha", "2026-04-27T12:00:03Z"),
                ("Gamma Album", "alb-sort-gamma", "2026-04-27T12:00:02Z"),
            ]
            for release_name, spotify_album_id, fetched_at in seeded:
                release_album_id = int(
                    connection.execute(
                        "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                        (release_name, release_name.lower()),
                    ).lastrowid
                )
                connection.execute(
                    "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                    (release_album_id, artist_id),
                )
                source_album_id = int(
                    connection.execute(
                        "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                        ("spotify", spotify_album_id, f"spotify:album:{spotify_album_id}", release_name, "{}"),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_album_map (
                      source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_album_id, release_album_id),
                )
                connection.execute(
                    """
                    INSERT INTO spotify_album_catalog (
                      spotify_album_id, name, total_tracks, fetched_at, last_status
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (spotify_album_id, f"Spotify {release_name}", 2, fetched_at, "ok"),
                )
            connection.commit()

        recent_payload = search_album_catalog_lookup(catalog_status="backfilled", queue_status="all", sort="recently_backfilled", limit=50, offset=0)
        self.assertEqual(
            ["Alpha Album", "Gamma Album", "Beta Album"],
            [item["release_album_name"] for item in recent_payload["items"]],
        )
        name_payload = search_album_catalog_lookup(catalog_status="backfilled", queue_status="all", sort="name", limit=50, offset=0)
        self.assertEqual(
            ["Alpha Album", "Beta Album", "Gamma Album"],
            [item["release_album_name"] for item in name_payload["items"]],
        )
        combined_payload = search_album_catalog_lookup(catalog_status="backfilled", queue_status="not_queued", sort="name", limit=50, offset=0)
        self.assertEqual(3, combined_payload["total"])

    def test_search_tracks_queue_status_filters_and_combined_filters(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Queue Artist Track",)).lastrowid)
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Queue Track Album", "queue track album"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (release_album_id, artist_id),
            )

            def _seed_track(name: str, spotify_track_id: str, *, duration_ms: int | None = 180000, last_status: str = "ok", last_error: str | None = None) -> None:
                release_track_id = int(
                    connection.execute(
                        "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                        (name, name.lower()),
                    ).lastrowid
                )
                source_track_id = int(
                    connection.execute(
                        "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                        ("spotify", spotify_track_id, f"spotify:track:{spotify_track_id}", name, "{}"),
                    ).lastrowid
                )
                connection.execute(
                    "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                    (release_track_id, artist_id),
                )
                connection.execute(
                    "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                    (release_album_id, release_track_id),
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_track_id, release_track_id),
                )
                connection.execute(
                    """
                    INSERT INTO spotify_track_catalog (
                      spotify_track_id, name, duration_ms, album_id, fetched_at, last_status, last_error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (spotify_track_id, f"Spotify {name}", duration_ms, "alb-seeded", "2026-04-27T12:00:00Z", last_status, last_error),
                )

            _seed_track("Track Pending", "trk-pending", duration_ms=None, last_status="error", last_error="catalog fail")
            _seed_track("Track Done", "trk-done")
            _seed_track("Track Queue Error", "trk-queue-error")
            _seed_track("Track Not Queued", "trk-not-queued")
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("track", "trk-pending", 80, "pending", "2026-04-27T11:00:00Z", 1),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts) VALUES (?, ?, ?, ?, ?, ?)",
                ("track", "trk-done", 40, "done", "2026-04-27T11:00:00Z", 2),
            )
            connection.execute(
                "INSERT INTO spotify_catalog_backfill_queue (entity_type, spotify_id, priority, status, requested_at, attempts, last_error) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("track", "trk-queue-error", 30, "error", "2026-04-27T11:00:00Z", 3, "queue failure"),
            )
            connection.commit()

        pending_payload = search_track_catalog_lookup(catalog_status="all", queue_status="pending", limit=50, offset=0)
        done_payload = search_track_catalog_lookup(catalog_status="all", queue_status="done", limit=50, offset=0)
        error_payload = search_track_catalog_lookup(catalog_status="all", queue_status="error", limit=50, offset=0)
        not_queued_payload = search_track_catalog_lookup(catalog_status="all", queue_status="not_queued", limit=50, offset=0)
        combined_payload = search_track_catalog_lookup(catalog_status="error", queue_status="pending", limit=50, offset=0)

        self.assertEqual({"trk-pending"}, {item["spotify_track_id"] for item in pending_payload["items"]})
        self.assertEqual({"trk-done"}, {item["spotify_track_id"] for item in done_payload["items"]})
        self.assertEqual({"trk-queue-error"}, {item["spotify_track_id"] for item in error_payload["items"]})
        self.assertEqual({"trk-not-queued"}, {item["spotify_track_id"] for item in not_queued_payload["items"]})
        self.assertEqual(1, combined_payload["total"])
        self.assertEqual("trk-pending", combined_payload["items"][0]["spotify_track_id"])
        self.assertEqual("pending", combined_payload["items"][0]["queue_status"])
        self.assertIn("queue_priority", combined_payload["items"][0])
        self.assertIn("queue_requested_at", combined_payload["items"][0])
        self.assertIn("queue_attempts", combined_payload["items"][0])
        self.assertIn("queue_last_error", combined_payload["items"][0])

        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            endpoint_response = client.get("/debug/search/tracks?catalog_status=all&queue_status=pending")
        self.assertEqual(200, endpoint_response.status_code)
        self.assertEqual(1, endpoint_response.json().get("total"))

    def test_search_tracks_sort_recently_backfilled_and_name(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = int(connection.execute("INSERT INTO artist (canonical_name) VALUES (?)", ("Track Sort Artist",)).lastrowid)
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track Sort Album", "track sort album"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (release_album_id, artist_id),
            )

            seeded = [
                ("Beta Track", "trk-sort-beta", "2026-04-27T12:00:01Z"),
                ("Alpha Track", "trk-sort-alpha", "2026-04-27T12:00:03Z"),
                ("Gamma Track", "trk-sort-gamma", "2026-04-27T12:00:02Z"),
            ]
            for release_name, spotify_track_id, fetched_at in seeded:
                release_track_id = int(
                    connection.execute(
                        "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                        (release_name, release_name.lower()),
                    ).lastrowid
                )
                connection.execute(
                    "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                    (release_track_id, artist_id),
                )
                connection.execute(
                    "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                    (release_album_id, release_track_id),
                )
                source_track_id = int(
                    connection.execute(
                        "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                        ("spotify", spotify_track_id, f"spotify:track:{spotify_track_id}", release_name, "{}"),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                    ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                    """,
                    (source_track_id, release_track_id),
                )
                connection.execute(
                    """
                    INSERT INTO spotify_track_catalog (
                      spotify_track_id, name, duration_ms, album_id, fetched_at, last_status
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (spotify_track_id, f"Spotify {release_name}", 181000, "alb-sort", fetched_at, "ok"),
                )
            connection.commit()

        recent_payload = search_track_catalog_lookup(catalog_status="backfilled", queue_status="all", sort="recently_backfilled", limit=50, offset=0)
        self.assertEqual(
            ["Alpha Track", "Gamma Track", "Beta Track"],
            [item["release_track_name"] for item in recent_payload["items"]],
        )
        name_payload = search_track_catalog_lookup(catalog_status="backfilled", queue_status="all", sort="name", limit=50, offset=0)
        self.assertEqual(
            ["Alpha Track", "Beta Track", "Gamma Track"],
            [item["release_track_name"] for item in name_payload["items"]],
        )
        combined_payload = search_track_catalog_lookup(catalog_status="backfilled", queue_status="not_queued", sort="name", limit=50, offset=0)
        self.assertEqual(3, combined_payload["total"])

    def test_catalog_backfill_enqueue_does_not_call_spotify(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.post(
                "/debug/spotify/catalog-backfill/enqueue",
                json={"items": [{"entity_type": "track", "spotify_id": "t1", "reason": "visible", "priority": 80}]},
            )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(1, body["enqueued"])
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_catalog_backfill_queue_repair_endpoint_does_not_call_spotify(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-endpoint", 2, "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-endpoint", "track-1", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "alb-repair-endpoint", "seed", 50, "done", "2026-04-28T00:00:00Z", 0),
            )
            connection.commit()

        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock, patch("backend.app.main.run_spotify_catalog_backfill") as run_mock:
            client = TestClient(app)
            response = client.post("/debug/spotify/catalog-backfill/queue/repair")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(1, body["repaired_to_pending"])
        self.assertEqual(0, body["repaired_to_done"])
        refresh_mock.assert_not_called()
        run_mock.assert_not_called()

    def test_enqueue_does_not_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track A", "track a"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track A",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        enqueue_spotify_catalog_backfill_items(
            items=[{"entity_type": "track", "spotify_id": "t-enqueue", "reason": "visible", "priority": 80}]
        )

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_queue_repair_does_not_mutate_analysis_track_map(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track B", "track b"),
                ).lastrowid
            )
            analysis_track_id = int(
                connection.execute(
                    "INSERT INTO analysis_track (primary_name) VALUES (?)",
                    ("Track B",),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO analysis_track_map (
                  release_track_id,
                  analysis_track_id,
                  match_method,
                  confidence,
                  status,
                  is_user_confirmed,
                  explanation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_track_id, analysis_track_id, "seed", 0.5, "suggested", 0, "seed"),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, total_tracks, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-map", 2, "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, market, fetched_at, last_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alb-repair-map", "track-1", "US", "2026-04-28T00:00:00Z", "ok", None),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_queue (
                  entity_type, spotify_id, reason, priority, status, requested_at, attempts
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("album", "alb-repair-map", "seed", 50, "done", "2026-04-28T00:00:00Z", 0),
            )
            connection.commit()
            before_count, before_digest = self._analysis_track_map_digest(connection)

        _ = repair_spotify_catalog_backfill_queue_statuses()

        with closing(sqlite3.connect(self.db_path)) as connection:
            after_count, after_digest = self._analysis_track_map_digest(connection)
        self.assertEqual(before_count, after_count)
        self.assertEqual(before_digest, after_digest)

    def test_catalog_backfill_runs_endpoint_lists_recent_first(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_run (
                  started_at, completed_at, market, status, tracks_seen, tracks_fetched, tracks_upserted,
                  albums_seen, albums_fetched, album_tracks_upserted, skipped, errors, requests_total,
                  requests_success, requests_429, requests_failed, initial_request_delay_seconds,
                  final_request_delay_seconds, effective_requests_per_minute, peak_requests_last_30_seconds,
                  max_retry_after_seconds, has_more, last_error, warnings_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-04-21T12:00:00Z",
                    "2026-04-21T12:01:00Z",
                    "US",
                    "ok",
                    10,
                    10,
                    10,
                    4,
                    4,
                    8,
                    0,
                    0,
                    20,
                    20,
                    0,
                    0,
                    0.5,
                    0.5,
                    120.0,
                    5,
                    0.0,
                    0,
                    None,
                    json.dumps(["Spotify batch track endpoint unavailable/forbidden; using single-track fallback"]),
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_run (
                  started_at, completed_at, market, status, tracks_seen, tracks_fetched, tracks_upserted,
                  albums_seen, albums_fetched, album_tracks_upserted, skipped, errors, requests_total,
                  requests_success, requests_429, requests_failed, initial_request_delay_seconds,
                  final_request_delay_seconds, effective_requests_per_minute, peak_requests_last_30_seconds,
                  max_retry_after_seconds, has_more, last_error, warnings_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-04-21T13:00:00Z",
                    "2026-04-21T13:02:00Z",
                    "US",
                    "failed",
                    15,
                    12,
                    12,
                    6,
                    5,
                    10,
                    2,
                    1,
                    30,
                    25,
                    2,
                    3,
                    0.5,
                    0.9,
                    80.0,
                    8,
                    1.0,
                    1,
                    "failed run",
                    json.dumps([]),
                ),
            )
            connection.commit()

        with patch("backend.app.main._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.get("/debug/spotify/catalog-backfill/runs?limit=20&offset=0")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(2, body["total"])
        self.assertEqual(2, len(body["items"]))
        self.assertEqual("2026-04-21T13:00:00Z", body["items"][0]["started_at"])
        self.assertEqual("failed", body["items"][0]["status"])
        self.assertEqual(0, body["items"][0]["warnings_count"])
        self.assertEqual("2026-04-21T12:00:00Z", body["items"][1]["started_at"])
        self.assertEqual("ok", body["items"][1]["status"])
        self.assertEqual(1, body["items"][1]["warnings_count"])
        self.assertIn("Spotify batch track endpoint unavailable/forbidden; using single-track fallback", body["items"][1]["warnings"])

    def test_catalog_backfill_coverage_counts(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            release_track_1 = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track One", "track one"),
                ).lastrowid
            )
            release_track_2 = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                    ("Track Two", "track two"),
                ).lastrowid
            )
            source_track_1 = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "track-1", "spotify:track:track-1", "Track One", "{}"),
                ).lastrowid
            )
            source_track_2 = int(
                connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "track-2", "spotify:track:track-2", "Track Two", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_1, release_track_1),
            )
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_track_2, release_track_2),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, fetched_at, last_status
                ) VALUES (?, ?, ?, ?)
                """,
                ("track-1", 123000, "2026-04-22T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, duration_ms, fetched_at, last_status
                ) VALUES (?, ?, ?, ?)
                """,
                ("track-2", None, "2026-04-22T12:00:00Z", "ok"),
            )

            release_album_1 = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Album One", "album one"),
                ).lastrowid
            )
            source_album_1 = int(
                connection.execute(
                    "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw, raw_payload_json) VALUES (?, ?, ?, ?, ?)",
                    ("spotify", "album-1", "spotify:album:album-1", "Album One", "{}"),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, is_user_confirmed, explanation
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted', 0, 'seed')
                """,
                (source_album_1, release_album_1),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, fetched_at, last_status
                ) VALUES (?, ?, ?)
                """,
                ("album-1", "2026-04-22T12:00:00Z", "ok"),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, fetched_at, last_status
                ) VALUES (?, ?, ?, ?)
                """,
                ("album-1", "track-1", "2026-04-22T12:00:00Z", "ok"),
            )

            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_run (
                  started_at, completed_at, market, status, tracks_seen, tracks_fetched, tracks_upserted,
                  albums_seen, albums_fetched, album_tracks_upserted, skipped, errors, requests_total,
                  requests_success, requests_429, requests_failed, initial_request_delay_seconds,
                  final_request_delay_seconds, effective_requests_per_minute, peak_requests_last_30_seconds,
                  max_retry_after_seconds, has_more, last_error, warnings_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-04-22T12:00:00Z",
                    "2026-04-22T12:02:00Z",
                    "US",
                    "failed",
                    20,
                    18,
                    18,
                    10,
                    8,
                    30,
                    2,
                    1,
                    40,
                    35,
                    2,
                    3,
                    0.5,
                    1.1,
                    70.0,
                    9,
                    2.0,
                    1,
                    "rate limit",
                    json.dumps([]),
                ),
            )
            connection.commit()

        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock:
            client = TestClient(app)
            response = client.get("/debug/spotify/catalog-backfill/coverage")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(2, body["known_release_tracks"])
        self.assertEqual(2, body["track_catalog_rows"])
        self.assertEqual(1, body["track_duration_coverage_count"])
        self.assertEqual(50.0, body["track_duration_coverage_percent"])
        self.assertEqual(1, body["known_release_albums"])
        self.assertEqual(1, body["album_catalog_rows"])
        self.assertEqual(1, body["album_track_rows"])
        self.assertEqual(0, body["identity_critical"]["missing_priority_track_metadata"])
        self.assertEqual(0, body["identity_critical"]["missing_identity_ambiguous_track_metadata"])
        self.assertEqual(0, body["identity_critical"]["missing_top_track_metadata"])
        self.assertEqual(2, body["catalog_expansion"]["missing_deferred_track_metadata"])
        self.assertEqual(2, body["track_metadata_priority"]["counts"]["total_missing_accepted_source_track_metadata"])
        self.assertEqual(2, body["track_metadata_priority"]["counts"]["missing_deferred_track_metadata"])
        self.assertIn("deferred", body["track_metadata_priority"]["samples"])
        self.assertIsInstance(body["latest_run"], dict)
        self.assertEqual("failed", body["latest_run"]["status"])
        self.assertEqual(1, body["recent_errors_count"])
        refresh_mock.assert_not_called()

    def test_ok_run_with_fallback_warnings_persists_warnings_but_no_last_error(self) -> None:
        self._seed_source_tracks(["t1"])

        def fetcher(
            url: str, params: dict[str, Any], access_token: str
        ) -> tuple[int, dict[str, str], dict[str, Any], str | None]:
            if url.endswith("/v1/tracks") and "/v1/tracks/" not in url:
                return 403, {}, {"error": {"status": 403, "message": "forbidden batch"}}, None
            if url.endswith("/v1/tracks/t1"):
                return 200, {}, _track_payload("t1", "a1"), None
            raise AssertionError(url)

        result = run_spotify_catalog_backfill(
            access_token="token",
            include_albums=False,
            sleeper=lambda _: None,
            fetcher=fetcher,
        )
        self.assertEqual("ok", result["status"])
        self.assertIn("Spotify batch track endpoint unavailable/forbidden; using single-track fallback", result["warnings"])
        self.assertIsNone(result["last_error"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute(
                "SELECT status, last_error, warnings_json FROM spotify_catalog_backfill_run ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertEqual("ok", str(row[0]))
        self.assertIsNone(row[1])
        self.assertIn("Spotify batch track endpoint unavailable/forbidden; using single-track fallback", json.loads(str(row[2] or "[]")))

    def test_recent_errors_count_excludes_ok_warning_only_run(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_run (
                  started_at, completed_at, market, status, tracks_seen, tracks_fetched, tracks_upserted,
                  albums_seen, albums_fetched, album_tracks_upserted, skipped, errors, requests_total,
                  requests_success, requests_429, requests_failed, initial_request_delay_seconds,
                  final_request_delay_seconds, effective_requests_per_minute, peak_requests_last_30_seconds,
                  max_retry_after_seconds, has_more, last_error, warnings_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-04-22T10:00:00Z",
                    "2026-04-22T10:01:00Z",
                    "US",
                    "ok",
                    2,
                    2,
                    2,
                    1,
                    1,
                    2,
                    0,
                    0,
                    5,
                    5,
                    0,
                    0,
                    0.5,
                    0.5,
                    120.0,
                    3,
                    0.0,
                    0,
                    None,
                    json.dumps(["Spotify batch album endpoint unavailable/forbidden; using single-album fallback"]),
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_catalog_backfill_run (
                  started_at, completed_at, market, status, tracks_seen, tracks_fetched, tracks_upserted,
                  albums_seen, albums_fetched, album_tracks_upserted, skipped, errors, requests_total,
                  requests_success, requests_429, requests_failed, initial_request_delay_seconds,
                  final_request_delay_seconds, effective_requests_per_minute, peak_requests_last_30_seconds,
                  max_retry_after_seconds, has_more, last_error, warnings_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-04-22T11:00:00Z",
                    "2026-04-22T11:01:00Z",
                    "US",
                    "failed",
                    2,
                    1,
                    1,
                    1,
                    0,
                    0,
                    0,
                    1,
                    4,
                    2,
                    0,
                    2,
                    0.5,
                    1.0,
                    60.0,
                    2,
                    0.0,
                    1,
                    "fatal",
                    json.dumps([]),
                ),
            )
            connection.commit()

        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed"
        ) as refresh_mock:
            client = TestClient(app)
            response = client.get("/debug/spotify/catalog-backfill/coverage")
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual(1, body["recent_errors_count"])
        refresh_mock.assert_not_called()

    def test_catalog_access_probe_success_shape(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed",
            return_value={"access_token": "token"},
        ), patch(
            "backend.app.main._spotify_catalog_probe_track_request",
            return_value=(True, 200, "Catalog access succeeded.", {"id": "track-1", "name": "Track 1"}),
        ) as probe_mock:
            client = TestClient(app)
            response = client.post(
                "/debug/spotify/catalog-access-probe",
                json={"spotify_track_id": "track-1", "market": "US"},
            )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual("track-1", body["spotify_track_id"])
        self.assertEqual("US", body["market"])
        self.assertEqual(200, body["status"])
        self.assertIsInstance(body["body"], dict)
        probe_mock.assert_called_once()
        with closing(sqlite3.connect(self.db_path)) as connection:
            run_count = int(connection.execute("SELECT count(*) FROM spotify_catalog_backfill_run").fetchone()[0])
            track_catalog_count = int(connection.execute("SELECT count(*) FROM spotify_track_catalog").fetchone()[0])
            album_catalog_count = int(connection.execute("SELECT count(*) FROM spotify_album_catalog").fetchone()[0])
            album_track_count = int(connection.execute("SELECT count(*) FROM spotify_album_track").fetchone()[0])
        self.assertEqual(0, run_count)
        self.assertEqual(0, track_catalog_count)
        self.assertEqual(0, album_catalog_count)
        self.assertEqual(0, album_track_count)

    def test_catalog_access_probe_403_shape_includes_body_message(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed",
            return_value={"access_token": "token"},
        ), patch(
            "backend.app.main._spotify_catalog_probe_track_request",
            return_value=(False, 403, "Forbidden by Spotify policy.", {"error": {"status": 403, "message": "Forbidden"}}),
        ):
            client = TestClient(app)
            response = client.post(
                "/debug/spotify/catalog-access-probe",
                json={"spotify_track_id": "track-1", "market": "US"},
            )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertEqual(403, body["status"])
        self.assertIn("Forbidden", body["message"])
        self.assertIsInstance(body["body"], dict)
        self.assertIn("error", body["body"])

    def test_catalog_access_probe_missing_auth_returns_stable_401(self) -> None:
        with patch(
            "backend.app.main._require_local_data_session",
            side_effect=HTTPException(status_code=401, detail="Not authenticated with Spotify."),
        ), patch("backend.app.main._spotify_catalog_probe_track_request") as probe_mock:
            client = TestClient(app)
            response = client.post("/debug/spotify/catalog-access-probe", json={"spotify_track_id": "track-1", "market": "US"})
        self.assertEqual(401, response.status_code)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertEqual("unauthenticated", body["status"])
        self.assertEqual("spotify_not_authenticated", body["error"]["code"])
        self.assertEqual("Not authenticated with Spotify.", body["error"]["message"])
        probe_mock.assert_not_called()

    def test_catalog_access_probe_no_token_leakage(self) -> None:
        secret_token = "super-secret-token-should-not-leak"
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed",
            return_value={"access_token": secret_token},
        ), patch(
            "backend.app.main._spotify_catalog_probe_track_request",
            return_value=(False, 403, "Forbidden by Spotify policy.", {"error": {"status": 403, "message": "Forbidden"}}),
        ):
            client = TestClient(app)
            response = client.post(
                "/debug/spotify/catalog-access-probe",
                json={"spotify_track_id": "track-1", "market": "US"},
            )
        self.assertEqual(200, response.status_code)
        serialized = json.dumps(response.json(), ensure_ascii=True)
        self.assertNotIn(secret_token, serialized)

    def test_catalog_access_probe_batch_constructs_ids_query_param(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed",
            return_value={"access_token": "token"},
        ), patch(
            "backend.app.main.discover_known_spotify_track_ids",
            return_value=["track-1", "track-2", "track-3"],
        ) as discover_mock, patch(
            "backend.app.main._spotify_catalog_probe_tracks_batch_request",
            return_value=(True, 200, "Catalog batch access succeeded.", {"tracks": []}),
        ) as batch_probe_mock:
            client = TestClient(app)
            response = client.post(
                "/debug/spotify/catalog-access-probe",
                json={"mode": "batch", "limit": 3, "market": "US"},
            )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(3, body["ids_count"])
        self.assertEqual(["track-1", "track-2", "track-3"], body["ids_sample"])
        discover_mock.assert_called_once()
        batch_probe_mock.assert_called_once()
        called_kwargs = batch_probe_mock.call_args.kwargs
        self.assertEqual(["track-1", "track-2", "track-3"], called_kwargs["spotify_track_ids"])
        self.assertEqual("US", called_kwargs["market"])

    def test_catalog_access_probe_batch_403_returns_body_message(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed",
            return_value={"access_token": "token"},
        ), patch(
            "backend.app.main.discover_known_spotify_track_ids",
            return_value=["track-1", "track-2"],
        ), patch(
            "backend.app.main._spotify_catalog_probe_tracks_batch_request",
            return_value=(False, 403, "Forbidden for batch endpoint.", {"error": {"status": 403, "message": "Forbidden"}}),
        ):
            client = TestClient(app)
            response = client.post(
                "/debug/spotify/catalog-access-probe",
                json={"mode": "batch", "limit": 2, "market": "US"},
            )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertEqual(403, body["status"])
        self.assertIn("Forbidden", body["message"])
        self.assertIsInstance(body["body"], dict)
        self.assertEqual(2, body["ids_count"])

    def test_catalog_access_probe_batch_no_catalog_or_run_writes(self) -> None:
        with patch("backend.app.main._require_local_data_session", return_value="user-1"), patch(
            "backend.app.main.refresh_access_token_if_needed",
            return_value={"access_token": "token"},
        ), patch(
            "backend.app.main.discover_known_spotify_track_ids",
            return_value=["track-1"],
        ), patch(
            "backend.app.main._spotify_catalog_probe_tracks_batch_request",
            return_value=(True, 200, "Catalog batch access succeeded.", {"tracks": []}),
        ):
            client = TestClient(app)
            response = client.post(
                "/debug/spotify/catalog-access-probe",
                json={"mode": "batch", "limit": 1, "market": "US"},
            )
        self.assertEqual(200, response.status_code)
        with closing(sqlite3.connect(self.db_path)) as connection:
            run_count = int(connection.execute("SELECT count(*) FROM spotify_catalog_backfill_run").fetchone()[0])
            track_catalog_count = int(connection.execute("SELECT count(*) FROM spotify_track_catalog").fetchone()[0])
            album_catalog_count = int(connection.execute("SELECT count(*) FROM spotify_album_catalog").fetchone()[0])
            album_track_count = int(connection.execute("SELECT count(*) FROM spotify_album_track").fetchone()[0])
        self.assertEqual(0, run_count)
        self.assertEqual(0, track_catalog_count)
        self.assertEqual(0, album_catalog_count)
        self.assertEqual(0, album_track_count)


if __name__ == "__main__":
    unittest.main()
