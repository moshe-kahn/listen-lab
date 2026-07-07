from __future__ import annotations

import json
import os
import sqlite3
import unittest
from contextlib import closing

from backend.app.db import apply_pending_migrations, ensure_sqlite_db, refresh_source_track_play_count_cache
from backend.app.library import list_personal_library_items, list_personal_library_tracks, personal_library_status, rebuild_personal_library
from backend.app.spotify_catalog_backfill import _upsert_track_catalog


class PersonalLibraryTests(unittest.TestCase):
    def setUp(self) -> None:
        self._previous_sqlite_db_path = os.environ.get("SQLITE_DB_PATH")
        self.db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_personal_library.sqlite3",
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

    def _seed_catalog_album(self, connection: sqlite3.Connection, album_id: str, name: str) -> None:
        connection.execute(
            """
            INSERT INTO spotify_album_catalog (
              spotify_album_id, name, album_type, release_date, total_tracks,
              artists_json, images_json, raw_json, market, fetched_at
            )
            VALUES (?, ?, 'album', '2026-01-01', 2, ?, ?, '{}', 'US', '2026-07-04T00:00:00Z')
            """,
            (
                album_id,
                name,
                json.dumps([{"id": "artist-1", "name": "Artist"}]),
                json.dumps([{"url": f"https://img.example/{album_id}.jpg"}]),
            ),
        )

    def _seed_album_track(self, connection: sqlite3.Connection, album_id: str, track_id: str, name: str) -> None:
        connection.execute(
            """
            INSERT INTO spotify_album_track (
              spotify_album_id, spotify_track_id, disc_number, track_number,
              name, duration_ms, artists_json, raw_json, market, fetched_at
            )
            VALUES (?, ?, 1, 1, ?, 180000, ?, '{}', 'US', '2026-07-04T00:00:00Z')
            """,
            (album_id, track_id, name, json.dumps([{"id": "artist-1", "name": "Artist"}])),
        )

    def _seed_playlist(self, connection: sqlite3.Connection, playlist_id: str, name: str, *, is_owned: bool) -> None:
        connection.execute(
            """
            INSERT INTO spotify_playlist_cache (
              user_id, playlist_id, name, owner_id, owner_name, is_public,
              is_collaborative, is_owned, owner_followed_by_you, playlist_category,
              snapshot_id, track_count, url, image_url, metadata_cached_at, raw_json
            )
            VALUES ('user-1', ?, ?, 'owner-1', 'Owner', 1, 0, ?, 0, 'added',
                    'snapshot', 1, NULL, NULL, '2026-07-04T00:00:00Z', '{}')
            """,
            (playlist_id, name, 1 if is_owned else 0),
        )

    def _seed_playlist_track(
        self,
        connection: sqlite3.Connection,
        playlist_id: str,
        track_id: str,
        name: str,
        *,
        position: int = 0,
    ) -> None:
        connection.execute(
            """
            INSERT INTO spotify_playlist_track_cache (
              user_id, playlist_id, position, spotify_track_id, uri, track_name,
              artist_name, album_name, album_id, duration_ms, image_url, url,
              artists_json, added_at, cached_at, raw_json
            )
            VALUES ('user-1', ?, ?, ?, ?, ?, 'Artist', 'Playlist Album',
                    'playlist-album', 180000, NULL, ?, ?, '2026-07-04T00:00:00Z',
                    '2026-07-04T00:00:00Z', '{}')
            """,
            (
                playlist_id,
                position,
                track_id,
                f"spotify:track:{track_id}",
                name,
                f"https://open.spotify.com/track/{track_id}",
                json.dumps([{"id": "artist-1", "name": "Artist"}]),
            ),
        )

    def _seed_unavailable_playlist_track(
        self,
        connection: sqlite3.Connection,
        playlist_id: str,
        track_id: str,
        *,
        position: int = 0,
    ) -> None:
        connection.execute(
            """
            INSERT INTO spotify_playlist_track_cache (
              user_id, playlist_id, position, spotify_track_id, uri, track_name,
              artist_name, album_name, album_id, duration_ms, image_url, url,
              artists_json, added_at, cached_at, raw_json
            )
            VALUES ('user-1', ?, ?, ?, ?, '', '', '', 'unavailable-album',
                    NULL, NULL, ?, '[]', '2026-07-04T00:00:00Z',
                    '2026-07-04T00:00:00Z', '{}')
            """,
            (
                playlist_id,
                position,
                track_id,
                f"spotify:track:{track_id}",
                f"https://open.spotify.com/track/{track_id}",
            ),
        )

    def _seed_release_mapping(
        self,
        connection: sqlite3.Connection,
        *,
        spotify_track_id: str,
        release_track_id: int,
        release_track_name: str,
    ) -> None:
        connection.execute(
            "INSERT OR IGNORE INTO release_track (id, primary_name, normalized_name) VALUES (?, ?, ?)",
            (release_track_id, release_track_name, release_track_name.lower()),
        )
        cursor = connection.execute(
            """
            INSERT INTO source_track (
              source_name, external_id, external_uri, source_name_raw
            )
            VALUES ('spotify', ?, ?, ?)
            """,
            (spotify_track_id, f"spotify:track:{spotify_track_id}", release_track_name),
        )
        source_track_id = int(cursor.lastrowid)
        connection.execute(
            """
            INSERT INTO source_track_map (
              source_track_id, release_track_id, match_method, confidence, status, explanation
            )
            VALUES (?, ?, 'test', 1.0, 'accepted', 'test')
            """,
            (source_track_id, release_track_id),
        )

    def _seed_recording_cluster(self, connection: sqlite3.Connection, release_track_ids: list[int], representative_id: int) -> None:
        cursor = connection.execute(
            """
            INSERT INTO generated_recording_track_cluster (
              candidate_key, candidate_type, safety_status, relationship_kind,
              relationship_strength, confidence, representative_release_track_id,
              representative_reason, member_count, candidate_snapshot_json, generated_at
            )
            VALUES ('test-recording', 'recording_track_candidate', 'safe', 'recording',
                    'strong', 1.0, ?, 'test', ?, '{}', '2026-07-04T00:00:00Z')
            """,
            (representative_id, len(release_track_ids)),
        )
        cluster_id = int(cursor.lastrowid)
        for index, release_track_id in enumerate(release_track_ids):
            connection.execute(
                """
                INSERT INTO generated_recording_track_cluster_member (
                  cluster_id, release_track_id, member_index, is_representative
                )
                VALUES (?, ?, ?, ?)
                """,
                (cluster_id, release_track_id, index, 1 if release_track_id == representative_id else 0),
            )

    def test_rebuild_maps_strengths_and_reasons(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            self._seed_catalog_album(connection, "album-1", "Context Album")
            self._seed_album_track(connection, "album-1", "one-listen", "One Listen")
            self._seed_album_track(connection, "album-1", "album-neighbor", "Album Neighbor")
            connection.execute(
                """
                INSERT INTO spotify_liked_track_cache (
                  user_id, spotify_track_id, uri, name, artist_names, album_name,
                  album_spotify_id, duration_ms, liked_at, first_seen_at, last_seen_at
                )
                VALUES ('user-1', 'liked-track', 'spotify:track:liked-track', 'Liked Track',
                        'Artist', 'Liked Album', 'liked-album', 180000,
                        '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')
                """
            )
            connection.executemany(
                """
                INSERT INTO source_track_play_count_cache (
                  spotify_track_id, play_count, first_played_at, last_played_at, updated_at
                )
                VALUES (?, ?, '2026-07-02T00:00:00Z', '2026-07-02T00:00:00Z', '2026-07-04T00:00:00Z')
                """,
                [
                    ("one-listen", 1),
                    ("three-listens", 3),
                    ("observed-only", 0),
                ],
            )
            self._seed_playlist(connection, "own-playlist", "Own Playlist", is_owned=True)
            self._seed_playlist(connection, "followed-playlist", "Followed Playlist", is_owned=False)
            self._seed_playlist(connection, "favorite-playlist", "Favorite Playlist", is_owned=False)
            connection.execute(
                "INSERT INTO playlist_category (user_id, name) VALUES ('user-1', 'Favorites')"
            )
            category_id = int(connection.execute("SELECT id FROM playlist_category WHERE name = 'Favorites'").fetchone()[0])
            connection.execute(
                "INSERT INTO playlist_category_member (user_id, category_id, playlist_id) VALUES ('user-1', ?, 'favorite-playlist')",
                (category_id,),
            )
            self._seed_playlist_track(connection, "own-playlist", "own-track", "Own Track")
            self._seed_playlist_track(connection, "followed-playlist", "followed-track", "Followed Track")
            self._seed_playlist_track(connection, "favorite-playlist", "favorite-track", "Favorite Track")
            connection.commit()

        summary = rebuild_personal_library("user-1")
        self.assertEqual("complete", summary["status"])
        all_tracks = list_personal_library_tracks("user-1", limit=50)["items"]
        strengths = {item["spotify_track_id"]: item["strength"] for item in all_tracks}

        self.assertEqual("primary", strengths["liked-track"])
        self.assertEqual("contextual", strengths["one-listen"])
        self.assertEqual("primary", strengths["three-listens"])
        self.assertEqual("ephemeral", strengths["observed-only"])
        self.assertEqual("primary", strengths["own-track"])
        self.assertEqual("potential", strengths["followed-track"])
        self.assertEqual("contextual", strengths["favorite-track"])
        self.assertEqual("contextual", strengths["album-neighbor"])

        status = personal_library_status("user-1")
        self.assertFalse(status["stale"])
        self.assertEqual(3, status["counts"]["primary"])
        self.assertGreaterEqual(status["counts"]["contextual"], 3)

    def test_search_and_strength_filter_use_cache(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            self._seed_playlist(connection, "followed-playlist", "Followed Playlist", is_owned=False)
            self._seed_playlist_track(connection, "followed-playlist", "target-track", "Needle Song", position=0)
            self._seed_playlist_track(connection, "followed-playlist", "other-track", "Other Song", position=1)
            connection.commit()

        rebuild_personal_library("user-1")
        response = list_personal_library_tracks(
            "user-1",
            strength="potential",
            q="needle",
            limit=10,
        )
        self.assertEqual(1, response["total"])
        self.assertEqual("target-track", response["items"][0]["spotify_track_id"])

    def test_identifier_only_rows_do_not_display_or_search_as_titles(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO source_track_play_count_cache (
                  spotify_track_id, play_count, first_played_at, last_played_at, updated_at
                )
                VALUES ('4GdKi4F976XGtr8hedHEro', 1, '2026-07-02T00:00:00Z',
                        '2026-07-02T00:00:00Z', '2026-07-04T00:00:00Z')
                """
            )
            connection.commit()

        rebuild_personal_library("user-1")

        all_tracks = list_personal_library_tracks("user-1", limit=10)
        self.assertEqual(1, all_tracks["total"])
        self.assertEqual("Unknown track", all_tracks["items"][0]["track_name"])

        search_response = list_personal_library_tracks("user-1", q="hero", limit=10)
        self.assertEqual(0, search_response["total"])

    def test_playlist_only_unavailable_rows_are_hidden(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            self._seed_playlist(connection, "own-playlist", "Own Playlist", is_owned=True)
            self._seed_unavailable_playlist_track(connection, "own-playlist", "blank-track")
            connection.commit()

        summary = rebuild_personal_library("user-1")
        self.assertEqual("complete", summary["status"])
        self.assertEqual(0, summary["row_count"])

        response = list_personal_library_tracks("user-1", limit=10)
        self.assertEqual(0, response["total"])
        self.assertEqual(0, personal_library_status("user-1")["row_count"])

    def test_history_metadata_fills_play_count_only_library_rows(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO fact_play_event (
                  canonical_ended_at, canonical_ms_played, spotify_track_id,
                  track_name_canonical, artist_name_canonical, album_name_canonical,
                  spotify_album_id, timing_source, matched_state
                )
                VALUES ('2026-07-02T00:00:00Z', NULL, 'history-track',
                        'History Title', 'History Artist', 'History Album',
                        'history-album', 'history', 'accepted')
                """
            )
            connection.commit()

        refresh_source_track_play_count_cache()
        rebuild_personal_library("user-1")

        response = list_personal_library_tracks("user-1", q="history", limit=10)
        self.assertEqual(1, response["total"])
        self.assertEqual("History Title", response["items"][0]["track_name"])
        self.assertEqual("History Artist", response["items"][0]["artist_name"])
        self.assertEqual("History Album", response["items"][0]["album_name"])

    def test_catalog_upsert_marks_completed_library_stale(self) -> None:
        rebuild_personal_library("user-1")
        self.assertFalse(personal_library_status("user-1")["stale"])

        _upsert_track_catalog(
            track={
                "id": "catalog-track",
                "name": "Catalog Track",
                "duration_ms": 180000,
                "explicit": False,
                "disc_number": 1,
                "track_number": 1,
                "artists": [{"id": "artist-1", "name": "Artist"}],
                "album": {
                    "id": "catalog-album",
                    "name": "Catalog Album",
                    "album_type": "album",
                    "release_date": "2026-01-01",
                    "release_date_precision": "day",
                    "total_tracks": 1,
                    "artists": [{"id": "artist-1", "name": "Artist"}],
                    "images": [],
                },
            },
            market="US",
            fetched_at="2026-07-05T00:00:00Z",
            last_status="ok",
            last_error=None,
        )

        self.assertTrue(personal_library_status("user-1")["stale"])

    def test_entity_result_modes_are_derived_from_cache(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            self._seed_playlist(connection, "followed-playlist", "Followed Playlist", is_owned=False)
            self._seed_playlist_track(connection, "followed-playlist", "target-track", "Needle Song", position=0)
            connection.commit()

        rebuild_personal_library("user-1")

        artists = list_personal_library_items("user-1", kind="artist", limit=10)
        albums = list_personal_library_items("user-1", kind="album", limit=10)
        playlists = list_personal_library_items("user-1", kind="playlist", limit=10)
        mixed = list_personal_library_items("user-1", kind="all", limit=10)

        self.assertEqual(1, artists["total"])
        self.assertEqual("Artist", artists["items"][0]["name"])
        self.assertEqual(1, albums["total"])
        self.assertEqual("Playlist Album", albums["items"][0]["name"])
        self.assertEqual(1, playlists["total"])
        self.assertEqual("Followed Playlist", playlists["items"][0]["name"])
        self.assertEqual(4, mixed["total"])
        self.assertEqual({"track", "artist", "album", "playlist"}, {item["kind"] for item in mixed["items"]})

    def test_simple_entity_search_matches_entity_title_not_related_tracks(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            self._seed_playlist(connection, "regular-playlist", "Regular Mix", is_owned=False)
            self._seed_playlist_track(connection, "regular-playlist", "hero-track", "Hero Song", position=0)
            connection.commit()

        rebuild_personal_library("user-1")

        simple_playlists = list_personal_library_items("user-1", kind="playlist", q="hero", limit=10)
        deep_playlists = list_personal_library_items("user-1", kind="playlist", q="hero", limit=10, deep=True)
        simple_mixed = list_personal_library_items("user-1", kind="all", q="hero", limit=10)

        self.assertEqual(0, simple_playlists["total"])
        self.assertEqual(1, deep_playlists["total"])
        self.assertEqual("Regular Mix", deep_playlists["items"][0]["name"])
        self.assertEqual({"track"}, {item["kind"] for item in simple_mixed["items"]})

    def test_track_results_group_release_duplicates_without_versions(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            self._seed_release_mapping(
                connection,
                spotify_track_id="release-source-a",
                release_track_id=101,
                release_track_name="Same Release Song",
            )
            self._seed_release_mapping(
                connection,
                spotify_track_id="release-source-b",
                release_track_id=101,
                release_track_name="Same Release Song",
            )
            connection.executemany(
                """
                INSERT INTO source_track_play_count_cache (
                  spotify_track_id, play_count, first_played_at, last_played_at, updated_at
                )
                VALUES (?, 1, '2026-07-02T00:00:00Z', '2026-07-02T00:00:00Z', '2026-07-04T00:00:00Z')
                """,
                [("release-source-a",), ("release-source-b",)],
            )
            connection.commit()

        rebuild_personal_library("user-1")
        response = list_personal_library_tracks("user-1", limit=10)

        self.assertEqual(1, response["total"])
        self.assertEqual(0, response["items"][0]["version_count"])
        self.assertEqual(2, response["items"][0]["source_version_count"])

    def test_track_results_group_recording_versions_with_popup_payload(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            self._seed_release_mapping(
                connection,
                spotify_track_id="recording-source-a",
                release_track_id=201,
                release_track_name="Recording Song",
            )
            self._seed_release_mapping(
                connection,
                spotify_track_id="recording-source-b",
                release_track_id=202,
                release_track_name="Recording Song - Version",
            )
            self._seed_recording_cluster(connection, [201, 202], representative_id=201)
            connection.executemany(
                """
                INSERT INTO source_track_play_count_cache (
                  spotify_track_id, play_count, first_played_at, last_played_at, updated_at
                )
                VALUES (?, 1, '2026-07-02T00:00:00Z', '2026-07-02T00:00:00Z', '2026-07-04T00:00:00Z')
                """,
                [("recording-source-a",), ("recording-source-b",)],
            )
            connection.commit()

        rebuild_personal_library("user-1")
        response = list_personal_library_tracks("user-1", limit=10)

        self.assertEqual(1, response["total"])
        self.assertEqual(2, response["items"][0]["version_count"])
        self.assertEqual(2, len(response["items"][0]["versions"]))

    def test_track_results_group_feature_parenthetical_title_variants(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.executemany(
                """
                INSERT INTO personal_library_track_cache (
                  user_id, spotify_track_id, track_name, artist_name, album_name,
                  album_id, image_url, uri, url, duration_ms, artists_json,
                  strength, reasons_json, play_count, first_played_at,
                  last_played_at, playlist_count, liked_at, is_liked,
                  source_playlist_id, source_playlist_name, source_album_id,
                  source_album_name, evidence_first_seen_at, evidence_last_seen_at,
                  release_track_id, recording_representative_release_track_id,
                  rule_version, rebuilt_at
                )
                VALUES (
                  'user-1', ?, ?, 'College, Electric Youth', 'A Real Hero',
                  'album-hero', NULL, ?, ?, 267000, ?,
                  'primary', '[{"reason":"liked","label":"Liked"}]', 1,
                  '2026-07-02T00:00:00Z', '2026-07-02T00:00:00Z',
                  0, NULL, 1, NULL, NULL, 'album-hero', 'A Real Hero',
                  '2026-07-02T00:00:00Z', '2026-07-02T00:00:00Z',
                  ?, NULL, 1, '2026-07-04T00:00:00Z'
                )
                """,
                [
                    (
                        "hero-feat",
                        "A Real Hero (feat. Electric Youth)",
                        "spotify:track:hero-feat",
                        "https://open.spotify.com/track/hero-feat",
                        json.dumps([{"id": "college", "name": "College"}, {"id": "electric-youth", "name": "Electric Youth"}]),
                        301,
                    ),
                    (
                        "hero-base",
                        "A Real Hero",
                        "spotify:track:hero-base",
                        "https://open.spotify.com/track/hero-base",
                        json.dumps([{"id": "college", "name": "College"}, {"id": "electric-youth", "name": "Electric Youth"}]),
                        302,
                    ),
                ],
            )
            connection.commit()

        response = list_personal_library_tracks("user-1", q="hero", limit=10)

        self.assertEqual(1, response["total"])
        self.assertEqual(2, response["items"][0]["version_count"])
        self.assertEqual(2, len(response["items"][0]["versions"]))


if __name__ == "__main__":
    unittest.main()
