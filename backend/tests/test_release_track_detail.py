from __future__ import annotations

import json
import os
import time
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.app.db import apply_pending_migrations, ensure_sqlite_db, sqlite_connection
from backend.app.main import app
from backend.app.release_track_detail import get_release_track_detail


class ReleaseTrackDetailTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_release_track_detail.sqlite3",
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

    def _seed_release_track(self, *, include_unusable: bool = False) -> int:
        with sqlite_connection(write=True) as connection:
            artist_id = int(
                connection.execute(
                    "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                    ("Canonical Artist", "canonical artist"),
                ).lastrowid
            )
            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name, duration_ms) VALUES (?, ?, ?)",
                    ("Canonical Song", "canonical song", 201000),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, ?, ?)",
                (release_track_id, artist_id, "primary", 0),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, images_json, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "album-a",
                    "Album A",
                    json.dumps([{"url": "https://images.example/album-a.jpg"}]),
                    "2026-05-24T12:00:00Z",
                    "ok",
                ),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, images_json, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "album-b",
                    "Album B",
                    json.dumps([{"url": "https://images.example/album-b.jpg"}]),
                    "2026-05-24T12:00:00Z",
                    "ok",
                ),
            )
            self._insert_source_version(
                connection,
                release_track_id=release_track_id,
                spotify_track_id="track-a",
                name="Source Song A",
                album_id="album-a",
                artists=[{"name": "Source Artist A"}],
                duration_ms=200000,
                explicit=0,
            )
            self._insert_source_version(
                connection,
                release_track_id=release_track_id,
                spotify_track_id="track-b",
                name="Source Song B",
                album_id="album-b",
                artists=[{"name": "Source Artist B"}],
                duration_ms=202000,
                explicit=1,
            )
            if include_unusable:
                source_track_id = int(
                    connection.execute(
                        """
                        INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw)
                        VALUES (?, ?, ?, ?)
                        """,
                        ("spotify", "", None, "Broken source"),
                    ).lastrowid
                )
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (source_track_id, release_track_id, "test", 1.0, "accepted"),
                )
        return release_track_id

    def _insert_source_version(
        self,
        connection,
        *,
        release_track_id: int,
        spotify_track_id: str,
        name: str,
        album_id: str,
        artists: list[dict[str, str]],
        duration_ms: int,
        explicit: int,
    ) -> None:
        source_track_id = int(
            connection.execute(
                """
                INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw)
                VALUES (?, ?, ?, ?)
                """,
                ("spotify", spotify_track_id, f"spotify:track:{spotify_track_id}", name),
            ).lastrowid
        )
        connection.execute(
            """
            INSERT INTO source_track_map (
              source_track_id, release_track_id, match_method, confidence, status
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (source_track_id, release_track_id, "test", 1.0, "accepted"),
        )
        connection.execute(
            """
            INSERT INTO spotify_track_catalog (
              spotify_track_id, name, duration_ms, explicit, album_id, artists_json, fetched_at, last_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                spotify_track_id,
                name,
                duration_ms,
                explicit,
                album_id,
                json.dumps(artists),
                "2026-05-24T12:00:00Z",
                "ok",
            ),
        )

    def test_valid_release_track_route_returns_stable_shape(self) -> None:
        release_track_id = self._seed_release_track()
        with patch("backend.app.routes.playback_routes._require_local_data_session", return_value="user-1"):
            response = TestClient(app).get(f"/tracks/release-track/{release_track_id}")

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual("Canonical Song", body["release_track"]["name"])
        self.assertEqual(2, body["release_track"]["source_count"])
        self.assertEqual("preferred_playable_source", body["playback"]["reason"])
        self.assertEqual("track-a", body["playback"]["spotify_track_id"])
        self.assertEqual("Source Song A", body["display"]["title"])
        self.assertEqual("track-a", body["display"]["source_spotify_track_id"])
        self.assertEqual(2, len(body["source_versions"]))

    def test_context_spotify_track_id_is_selected_when_it_belongs(self) -> None:
        release_track_id = self._seed_release_track()
        payload = get_release_track_detail(release_track_id, context_spotify_track_id="track-b")

        self.assertEqual("context_source", payload["playback"]["reason"])
        self.assertEqual("track-b", payload["playback"]["spotify_track_id"])
        flags = {
            version["spotify_track_id"]: (version["is_context"], version["is_playback_choice"])
            for version in payload["source_versions"]
        }
        self.assertEqual((False, False), flags["track-a"])
        self.assertEqual((True, True), flags["track-b"])

    def test_context_spotify_track_id_is_ignored_when_it_does_not_belong(self) -> None:
        release_track_id = self._seed_release_track()
        payload = get_release_track_detail(release_track_id, context_spotify_track_id="other-track")

        self.assertEqual("preferred_playable_source", payload["playback"]["reason"])
        self.assertEqual("track-a", payload["playback"]["spotify_track_id"])
        self.assertFalse(any(version["is_context"] for version in payload["source_versions"]))

    def test_preferred_playable_source_fallback_is_deterministic(self) -> None:
        release_track_id = self._seed_release_track()
        first = get_release_track_detail(release_track_id, context_spotify_track_id="missing")
        second = get_release_track_detail(release_track_id, context_spotify_track_id="missing")

        self.assertEqual("track-a", first["playback"]["spotify_track_id"])
        self.assertEqual(first["playback"], second["playback"])
        playback_choices = [version for version in first["source_versions"] if version["is_playback_choice"]]
        self.assertEqual(["track-a"], [version["spotify_track_id"] for version in playback_choices])

    def test_unavailable_playback_when_no_usable_source_exists(self) -> None:
        release_track_id = self._seed_release_track(include_unusable=True)
        with sqlite_connection(write=True) as connection:
            connection.execute("DELETE FROM source_track_map WHERE source_track_id IN (SELECT id FROM source_track WHERE external_id IN (?, ?))", ("track-a", "track-b"))
            connection.execute("DELETE FROM source_track WHERE external_id IN (?, ?)", ("track-a", "track-b"))

        payload = get_release_track_detail(release_track_id, context_spotify_track_id="track-a")

        self.assertEqual("unavailable", payload["playback"]["reason"])
        self.assertIsNone(payload["playback"]["spotify_track_id"])
        self.assertIsNone(payload["playback"]["uri"])
        self.assertFalse(any(version["is_playback_choice"] for version in payload["source_versions"]))
