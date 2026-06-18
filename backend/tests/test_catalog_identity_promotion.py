from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest

from backend.app.catalog_identity_promotion import promote_catalog_album_tracks_to_identity
from backend.app.db import (
    _ensure_source_artist_mapping_with_connection,
    _ensure_source_track_mapping_with_connection,
    apply_pending_migrations,
    ensure_sqlite_db,
    sqlite_connection,
)
from backend.app.recording_track_candidates import get_recording_track_candidates_for_release_track
from backend.app.release_track_metadata import enrich_album_track_rows_with_release_metadata
from backend.app.spotify_catalog_backfill import _upsert_album_track


class CatalogIdentityPromotionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "catalog-identity.sqlite3")
        os.environ["SQLITE_DB_PATH"] = self.db_path
        ensure_sqlite_db()
        apply_pending_migrations()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_promoted_album_appearance_joins_existing_recording_candidate(self) -> None:
        artist_payload = {
            "id": "artist-telenova",
            "name": "Telenova",
            "uri": "spotify:artist:artist-telenova",
        }
        with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
            artist_id = _ensure_source_artist_mapping_with_connection(
                connection,
                external_id="artist-telenova",
                external_uri="spotify:artist:artist-telenova",
                artist_name="Telenova",
                raw_payload_json=json.dumps(artist_payload),
            )
            original_release_track_id = _ensure_source_track_mapping_with_connection(
                connection,
                source_name="spotify",
                external_id="original-paralysis",
                external_uri="spotify:track:original-paralysis",
                track_name="PARALYSIS GHOSTS",
                track_duration_ms=300_000,
                raw_payload_json=json.dumps({"id": "original-paralysis", "name": "PARALYSIS GHOSTS"}),
                create_match_method="test",
                create_confidence=1.0,
                create_explanation="test seed",
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (original_release_track_id, artist_id),
            )
            warning_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES ('THE WARNING', 'the warning')"
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (warning_album_id, original_release_track_id),
            )
            connection.execute(
                """
                INSERT INTO source_track_play_count_cache (
                  spotify_track_id, play_count, first_played_at, last_played_at
                ) VALUES ('original-paralysis', 1, '2026-02-27T19:01:18Z', '2026-02-27T19:01:18Z')
                """
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, album_type, release_date, total_tracks,
                  artists_json, raw_json, fetched_at, last_status
                ) VALUES (?, 'BITCRUSH', 'single', '2025-08-20', 1, ?, ?, '2026-06-18T00:00:00Z', 'ok')
                """,
                (
                    "bitcrush-album",
                    json.dumps([artist_payload]),
                    json.dumps({"id": "bitcrush-album", "name": "BITCRUSH", "artists": [artist_payload]}),
                ),
            )

        _upsert_album_track(
            album_id="bitcrush-album",
            track={
                "id": "bitcrush-paralysis",
                "name": "PARALYSIS GHOSTS",
                "uri": "spotify:track:bitcrush-paralysis",
                "duration_ms": 300_000,
                "disc_number": 1,
                "track_number": 1,
                "artists": [artist_payload],
            },
            market="US",
            fetched_at="2026-06-18T00:00:00Z",
            last_status="ok",
            last_error=None,
        )

        result = promote_catalog_album_tracks_to_identity(album_ids=["bitcrush-album"], apply=True)

        promoted = next(item for item in result["promoted_tracks"] if item.get("spotify_track_id") == "bitcrush-paralysis")
        promoted_release_track_id = int(promoted["release_track_id"])
        self.assertNotEqual(original_release_track_id, promoted_release_track_id)
        candidates = get_recording_track_candidates_for_release_track(promoted_release_track_id)
        recording = next(item for item in candidates if item["candidate_type"] == "recording_track_candidate")
        self.assertEqual(
            {original_release_track_id, promoted_release_track_id},
            {member["release_track_id"] for member in recording["members"]},
        )
        enriched = enrich_album_track_rows_with_release_metadata([
            {"id": "bitcrush-paralysis", "name": "PARALYSIS GHOSTS"},
        ])[0]
        self.assertEqual(0, enriched["source_play_count"])
        self.assertEqual(0, enriched["play_count"])
        self.assertEqual(1, enriched["recording_play_count"])
        self.assertEqual("2026-02-27T19:01:18Z", enriched["recording_last_played_at"])

        repeated = promote_catalog_album_tracks_to_identity(album_ids=["bitcrush-album"], apply=True)
        self.assertEqual([], repeated["touched_release_track_ids"])
        self.assertIsNone(repeated["cluster_refresh"])
