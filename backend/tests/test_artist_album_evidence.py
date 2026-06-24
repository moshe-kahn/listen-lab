from __future__ import annotations

import json
import os
import time
import unittest

from backend.app.artist_album_evidence import list_artist_album_evidence
from backend.app.db import apply_pending_migrations, ensure_sqlite_db, sqlite_connection
from backend.app.routes.playback_routes import _enqueue_incomplete_artist_album_tracklists


def _artists(*names: str) -> str:
    return json.dumps([{"name": name} for name in names])


def _images(url: str) -> str:
    return json.dumps([{"url": url}])


class ArtistAlbumEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(os.getcwd(), "backend", "tests", "_tmp_artist_album_evidence.sqlite3")
        for suffix in ("", "-wal", "-shm"):
            path = f"{self.db_path}{suffix}"
            if os.path.exists(path):
                os.remove(path)
        os.environ["SQLITE_DB_PATH"] = self.db_path
        ensure_sqlite_db()
        apply_pending_migrations()
        self._seed_catalog()

    def tearDown(self) -> None:
        for suffix in ("", "-wal", "-shm"):
            path = f"{self.db_path}{suffix}"
            if os.path.exists(path):
                for _ in range(5):
                    try:
                        os.remove(path)
                        break
                    except PermissionError:
                        time.sleep(0.1)

    def _insert_album(
        self,
        album_id: str,
        name: str,
        album_artists: tuple[str, ...],
        total_tracks: int,
    ) -> None:
        with sqlite_connection(write=True) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, release_date, total_tracks, artists_json, images_json, raw_json, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    album_id,
                    name,
                    "2026-01-01",
                    total_tracks,
                    _artists(*album_artists),
                    _images(f"https://images.example/{album_id}.jpg"),
                    "{}",
                    "2026-05-01T00:00:00Z",
                    "ok",
                ),
            )

    def _insert_track(self, album_id: str, track_id: str, track_number: int, track_artists: tuple[str, ...]) -> None:
        with sqlite_connection(write=True) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_track (
                  spotify_album_id, spotify_track_id, disc_number, track_number, name, duration_ms, artists_json, raw_json, fetched_at, last_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    album_id,
                    track_id,
                    1,
                    track_number,
                    f"Track {track_number}",
                    180000,
                    _artists(*track_artists),
                    "{}",
                    "2026-05-01T00:00:00Z",
                    "ok",
                ),
            )

    def _insert_entity_album(
        self,
        album_name: str,
        artist_names: tuple[str, ...],
        spotify_album_id: str | None = None,
    ) -> int:
        with sqlite_connection(write=True) as connection:
            album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                (album_name, album_name.strip().lower()),
            ).lastrowid
            if spotify_album_id:
                source_album_id = connection.execute(
                    """
                    INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw)
                    VALUES ('spotify', ?, ?, ?)
                    """,
                    (spotify_album_id, f"spotify:album:{spotify_album_id}", album_name),
                ).lastrowid
                connection.execute(
                    """
                    INSERT INTO source_album_map (
                      source_album_id, release_album_id, match_method, confidence, status
                    ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted')
                    """,
                    (source_album_id, album_id),
                )
            for index, artist_name in enumerate(artist_names):
                artist_id = connection.execute(
                    "INSERT INTO artist (canonical_name) VALUES (?)",
                    (artist_name,),
                ).lastrowid
                connection.execute(
                    """
                    INSERT INTO album_artist (
                      release_album_id, artist_id, role, billing_index, credited_as, match_method, confidence, source_basis
                    ) VALUES (?, ?, 'primary', ?, ?, 'provider_identity', 1.0, 'spotify_structured_artist_ids')
                    """,
                    (album_id, artist_id, index, artist_name),
                )
            return int(album_id)

    def _insert_entity_album_source_track(
        self,
        release_album_id: int,
        *,
        release_track_name: str,
        spotify_track_id: str,
        spotify_album_id: str,
        spotify_album_name: str,
        image_url: str,
        release_date: str,
    ) -> None:
        with sqlite_connection(write=True) as connection:
            release_track_id = connection.execute(
                "INSERT INTO release_track (primary_name, normalized_name) VALUES (?, ?)",
                (release_track_name, release_track_name.strip().lower()),
            ).lastrowid
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (release_album_id, release_track_id),
            )
            source_track_id = connection.execute(
                """
                INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw, raw_payload_json)
                VALUES ('spotify', ?, ?, ?, ?)
                """,
                (
                    spotify_track_id,
                    f"spotify:track:{spotify_track_id}",
                    release_track_name,
                    json.dumps(
                        {
                            "track": {
                                "name": release_track_name,
                                "album": {
                                    "id": spotify_album_id,
                                    "name": spotify_album_name,
                                    "images": [{"url": image_url}],
                                    "release_date": release_date,
                                },
                            }
                        }
                    ),
                ),
            ).lastrowid
            connection.execute(
                """
                INSERT INTO source_track_map (
                  source_track_id, release_track_id, match_method, confidence, status
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted')
                """,
                (source_track_id, release_track_id),
            )

    def _seed_catalog(self) -> None:
        self._insert_album("album-primary", "Primary Album", ("Primary Artist",), 3)
        for index in range(1, 4):
            self._insert_track("album-primary", f"primary-{index}", index, ("Primary Artist",))

        self._insert_album("album-guest", "Guest Album", ("Main Artist",), 4)
        self._insert_track("album-guest", "guest-1", 1, ("Main Artist", "Guest Artist"))
        for index in range(2, 5):
            self._insert_track("album-guest", f"guest-{index}", index, ("Main Artist",))

        self._insert_album("album-incomplete", "Incomplete Album", ("Another Artist",), 4)
        self._insert_track("album-incomplete", "incomplete-1", 1, ("Another Artist", "Unknown Guest"))

        self._insert_album("album-shared", "Shared Album", ("Shared Main",), 3)
        self._insert_track("album-shared", "shared-1", 1, ("Shared Main", "Alpha"))
        self._insert_track("album-shared", "shared-2", 2, ("Shared Main", "Beta"))
        self._insert_track("album-shared", "shared-3", 3, ("Shared Main",))

    def test_single_album_artist_match(self) -> None:
        items = list_artist_album_evidence(["Primary Artist"])
        self.assertEqual("album-primary", items[0]["album_id"])
        self.assertEqual("album", items[0]["relationship"])
        self.assertEqual("Album artist match", items[0]["evidence"])

    def test_single_appears_on_match(self) -> None:
        items = list_artist_album_evidence(["Guest Artist"])
        self.assertEqual(1, len(items))
        self.assertEqual("album-guest", items[0]["album_id"])
        self.assertEqual("appears_on", items[0]["relationship"])
        self.assertEqual({"Guest Artist": 1}, items[0]["matching_track_count_by_artist"])

    def test_incomplete_tracklist_unknown(self) -> None:
        items = list_artist_album_evidence(["Unknown Guest"])
        self.assertEqual(1, len(items))
        self.assertEqual("album-incomplete", items[0]["album_id"])
        self.assertEqual("unknown", items[0]["relationship"])
        self.assertFalse(items[0]["tracklist_complete"])

    def test_incomplete_artist_album_tracklists_enqueue_background_backfill(self) -> None:
        items = [
            *list_artist_album_evidence(["Unknown Guest"]),
            *list_artist_album_evidence(["Primary Artist"]),
        ]

        payload = _enqueue_incomplete_artist_album_tracklists(items)

        self.assertEqual(1, payload["enqueued"])
        with sqlite_connection(row_factory=None) as connection:
            rows = connection.execute(
                """
                SELECT entity_type, spotify_id, reason, priority, status
                FROM spotify_catalog_backfill_queue
                ORDER BY spotify_id
                """
            ).fetchall()
        self.assertEqual(
            [("album", "album-incomplete", "tracklist_completion", 70, "pending")],
            rows,
        )

    def test_shared_artist_filters_to_all_targets_present(self) -> None:
        items = list_artist_album_evidence(["Alpha", "Beta"])
        self.assertEqual(["album-shared"], [item["album_id"] for item in items])
        self.assertTrue(items[0]["all_targets_present"])

    def test_shared_artist_relationship_is_album_or_unknown_only(self) -> None:
        relationships = {item["relationship"] for item in list_artist_album_evidence(["Alpha", "Beta"])}
        self.assertEqual({"unknown"}, relationships)

    def test_source_album_sorting_by_id_and_name(self) -> None:
        items_by_id = list_artist_album_evidence(["Main Artist"], source_album_id="album-guest")
        self.assertEqual("album-guest", items_by_id[0]["album_id"])
        items_by_name = list_artist_album_evidence(["Shared Main"], source_album_name="Shared Album")
        self.assertEqual("album-shared", items_by_name[0]["album_id"])

    def test_entity_album_link_fills_catalog_gap(self) -> None:
        self._insert_entity_album("Entity Album", ("Entity Artist",), spotify_album_id="entity-album")
        items = list_artist_album_evidence(["Entity Artist"])
        self.assertEqual(["entity-album"], [item["album_id"] for item in items])
        self.assertEqual("album", items[0]["relationship"])
        self.assertEqual("Internal album artist link", items[0]["evidence"])

    def test_entity_album_link_uses_linked_source_track_album_metadata(self) -> None:
        release_album_id = self._insert_entity_album("Entity Album", ("Entity Artist",), spotify_album_id="entity-album")
        self._insert_entity_album_source_track(
            release_album_id,
            release_track_name="Entity Track",
            spotify_track_id="entity-track",
            spotify_album_id="entity-album",
            spotify_album_name="Entity Album",
            image_url="https://images.example/entity-album.jpg",
            release_date="2026-05-01",
        )
        items = list_artist_album_evidence(["Entity Artist"])
        self.assertEqual(["entity-album"], [item["album_id"] for item in items])
        self.assertEqual("https://images.example/entity-album.jpg", items[0]["image_url"])
        self.assertEqual("2026", items[0]["release_year"])

    def test_entity_album_link_dedupes_catalog_match(self) -> None:
        self._insert_entity_album("Primary Album", ("Primary Artist",), spotify_album_id="album-primary")
        items = list_artist_album_evidence(["Primary Artist"])
        matching = [item for item in items if item["album_id"] == "album-primary"]
        self.assertEqual(1, len(matching))
        self.assertEqual("Album artist match", matching[0]["evidence"])

    def test_catalog_album_dedupes_different_spotify_ids_for_same_release(self) -> None:
        self._insert_album("duplicate-primary", "Primary Album", ("Primary Artist",), 3)
        for index in range(1, 4):
            self._insert_track("duplicate-primary", f"duplicate-primary-{index}", index, ("Primary Artist",))

        items = list_artist_album_evidence(["Primary Artist"])
        matching = [item for item in items if item["album_name"] == "Primary Album"]
        self.assertEqual(1, len(matching))

    def test_catalog_album_dedupes_spotify_ids_mapped_to_same_release(self) -> None:
        release_album_id = self._insert_entity_album("Mapped Album", ("Mapped Artist",), spotify_album_id="mapped-original")
        self._insert_album("mapped-original", "Mapped Album", ("Mapped Artist",), 2)
        self._insert_album("mapped-duplicate", "Mapped Album", ("Mapped Artist",), 2)
        for index in range(1, 3):
            self._insert_track("mapped-original", f"mapped-original-{index}", index, ("Mapped Artist",))
            self._insert_track("mapped-duplicate", f"mapped-duplicate-{index}", index, ("Mapped Artist",))
        with sqlite_connection(write=True) as connection:
            source_album_id = connection.execute(
                """
                INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw)
                VALUES ('spotify', ?, ?, ?)
                """,
                ("mapped-duplicate", "spotify:album:mapped-duplicate", "Mapped Album"),
            ).lastrowid
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status
                ) VALUES (?, ?, 'provider_identity', 1.0, 'accepted')
                """,
                (source_album_id, release_album_id),
            )

        items = list_artist_album_evidence(["Mapped Artist"])
        matching = [item for item in items if item["album_name"] == "Mapped Album"]
        self.assertEqual(1, len(matching))

    def test_entity_album_editions_keep_distinct_release_identity(self) -> None:
        self._insert_entity_album("Innerworld", ("Electric Youth",), spotify_album_id="innerworld-base")
        self._insert_entity_album("Innerworld (Deluxe Edition)", ("Electric Youth", "College"), spotify_album_id="innerworld-deluxe")
        self._insert_entity_album(
            "Innerworld (10th Anniversary Edition)",
            ("Electric Youth", "College"),
            spotify_album_id="innerworld-anniversary",
        )

        items = list_artist_album_evidence(["Electric Youth"])
        matching = [item for item in items if item["album_name"].startswith("Innerworld")]
        self.assertEqual(
            [
                "Innerworld",
                "Innerworld (10th Anniversary Edition)",
                "Innerworld (Deluxe Edition)",
            ],
            sorted(item["album_name"] for item in matching),
        )
        self.assertEqual({"album"}, {item["relationship"] for item in matching})

    def test_stable_response_shape(self) -> None:
        item = list_artist_album_evidence(["Guest Artist"])[0]
        self.assertEqual(
            {
                "album_id",
                "album_name",
                "album_artist_names",
                "image_url",
                "url",
                "release_year",
                "total_tracks",
                "album_type",
                "cached_track_count",
                "matching_artist_names",
                "matching_track_count_by_artist",
                "all_targets_present",
                "tracklist_complete",
                "relationship",
                "evidence",
            },
            set(item.keys()),
        )


if __name__ == "__main__":
    unittest.main()
