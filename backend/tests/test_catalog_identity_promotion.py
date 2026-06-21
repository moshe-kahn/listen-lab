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

    def test_track_catalog_promotion_adds_missing_collaborator_credit(self) -> None:
        black_pumas = {"id": "black-pumas", "name": "Black Pumas", "uri": "spotify:artist:black-pumas"}
        lucius = {"id": "lucius", "name": "Lucius", "uri": "spotify:artist:lucius"}
        with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
            black_pumas_id = _ensure_source_artist_mapping_with_connection(
                connection,
                external_id="black-pumas",
                external_uri="spotify:artist:black-pumas",
                artist_name="Black Pumas",
                raw_payload_json=json.dumps(black_pumas),
            )
            release_track_id = _ensure_source_track_mapping_with_connection(
                connection,
                source_name="spotify",
                external_id="strangers-track",
                external_uri="spotify:track:strangers-track",
                track_name="Strangers",
                track_duration_ms=192_963,
                raw_payload_json=json.dumps({"id": "strangers-track", "name": "Strangers"}),
                create_match_method="test",
                create_confidence=1.0,
                create_explanation="test seed",
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (release_track_id, black_pumas_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, album_type, release_date, total_tracks,
                  artists_json, raw_json, fetched_at, last_status
                ) VALUES ('strangers-single', 'Strangers (From "Life In A Day")', 'single', '2021-02-04', 1,
                  ?, ?, '2026-06-19T00:00:00Z', 'ok')
                """,
                (json.dumps([black_pumas, lucius]), json.dumps({"artists": [black_pumas, lucius]})),
            )
            connection.execute(
                """
                INSERT INTO spotify_track_catalog (
                  spotify_track_id, name, duration_ms, disc_number, track_number, album_id,
                  artists_json, raw_json, fetched_at, last_status
                ) VALUES ('strangers-track', 'Strangers', 192963, 1, 1, 'strangers-single',
                  ?, ?, '2026-06-19T00:00:00Z', 'ok')
                """,
                (json.dumps([black_pumas, lucius]), json.dumps({"artists": [black_pumas, lucius]})),
            )

        promote_catalog_album_tracks_to_identity(album_ids=["strangers-single"], apply=True)

        with sqlite_connection(row_factory=sqlite3.Row) as connection:
            artists = connection.execute(
                """
                SELECT a.canonical_name
                FROM track_artist ta
                JOIN artist a ON a.id = ta.artist_id
                WHERE ta.release_track_id = ? AND ta.role = 'primary'
                ORDER BY ta.billing_index, a.canonical_name
                """,
                (release_track_id,),
            ).fetchall()
        self.assertEqual(["Black Pumas", "Lucius"], [row["canonical_name"] for row in artists])

    def test_existing_source_album_mapping_allows_tracklist_promotion_without_album_catalog(self) -> None:
        radiohead = {"id": "radiohead", "name": "Radiohead", "uri": "spotify:artist:radiohead"}
        remixer = {"id": "remixer", "name": "Remixer", "uri": "spotify:artist:remixer"}
        with sqlite_connection(write=True) as connection:
            release_album_id = int(connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Remix Album', 'remix album')"
            ).lastrowid)
            source_album_id = int(connection.execute(
                "INSERT INTO source_album (source_name, external_id, source_name_raw) VALUES ('spotify', 'remix-album', 'Remix Album')"
            ).lastrowid)
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status
                ) VALUES (?, ?, 'seed', 1.0, 'accepted')
                """,
                (source_album_id, release_album_id),
            )

        _upsert_album_track(
            album_id="remix-album",
            track={
                "id": "remix-track",
                "name": "Song - Remixer Rmx",
                "uri": "spotify:track:remix-track",
                "duration_ms": 180_000,
                "disc_number": 1,
                "track_number": 1,
                "artists": [radiohead, remixer],
            },
            market="US",
            fetched_at="2026-06-21T00:00:00Z",
            last_status="ok",
            last_error=None,
        )

        result = promote_catalog_album_tracks_to_identity(album_ids=["remix-album"], apply=True)

        promoted = next(item for item in result["promoted_tracks"] if item.get("spotify_track_id") == "remix-track")
        self.assertEqual(release_album_id, promoted["release_album_id"])
        self.assertIsInstance(promoted["release_track_id"], int)
