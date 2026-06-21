from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest

from backend.app.album_family import (
    _core_album_name,
    _edition_label,
    apply_reviewed_album_family_grouping,
    build_album_family_context,
)
from backend.app.db import (
    apply_pending_migrations,
    ensure_sqlite_db,
    mark_generated_recording_track_clusters_dirty_with_connection,
    sqlite_connection,
)


class AlbumFamilyReviewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "album-family.sqlite3")
        os.environ["SQLITE_DB_PATH"] = self.db_path
        ensure_sqlite_db()
        apply_pending_migrations()
        with sqlite_connection(write=True) as connection:
            self.artist_id = int(connection.execute(
                "INSERT INTO artist (canonical_name, sort_name) VALUES ('Family Artist', 'family artist')"
            ).lastrowid)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _seed_album(self, name: str, spotify_album_id: str, year: int, tracks: list[str]) -> int:
        artists = [{"id": "family-artist", "name": "Family Artist", "uri": "spotify:artist:family-artist"}]
        with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
            release_track_ids: list[int] = []
            release_album_id = int(connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name, release_year) VALUES (?, lower(?), ?)",
                (name, name, year),
            ).lastrowid)
            family_id = int(connection.execute(
                "INSERT INTO album_family (primary_name, normalized_name, release_year, canonical_release_album_id) VALUES (?, lower(?), ?, ?)",
                (name, name, year, release_album_id),
            ).lastrowid)
            connection.execute(
                """
                INSERT INTO album_family_map (
                  release_album_id, album_family_id, match_method, confidence, status, explanation
                ) VALUES (?, ?, 'seed', 1.0, 'accepted', 'seed')
                """,
                (release_album_id, family_id),
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (release_album_id, self.artist_id),
            )
            source_album_id = int(connection.execute(
                "INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw) VALUES ('spotify', ?, ?, ?)",
                (spotify_album_id, f"spotify:album:{spotify_album_id}", name),
            ).lastrowid)
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, explanation
                ) VALUES (?, ?, 'seed', 1.0, 'accepted', 'seed')
                """,
                (source_album_id, release_album_id),
            )
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, album_type, release_date, total_tracks,
                  artists_json, images_json, fetched_at, last_status
                ) VALUES (?, ?, 'album', ?, ?, ?, ?, '2026-06-18T00:00:00Z', 'ok')
                """,
                (
                    spotify_album_id,
                    name,
                    f"{year}-01-01",
                    len(tracks),
                    json.dumps(artists),
                    json.dumps([{"url": f"https://images.example/{spotify_album_id}.jpg"}]),
                ),
            )
            for index, track_name in enumerate(tracks, start=1):
                spotify_track_id = f"{spotify_album_id}-track-{index}"
                release_track_id = int(connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name, duration_ms) VALUES (?, lower(?), 180000)",
                    (track_name, track_name),
                ).lastrowid)
                release_track_ids.append(release_track_id)
                connection.execute(
                    "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                    (release_track_id, self.artist_id),
                )
                connection.execute(
                    "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                    (release_album_id, release_track_id),
                )
                source_track_id = int(connection.execute(
                    "INSERT INTO source_track (source_name, external_id, external_uri, source_name_raw) VALUES ('spotify', ?, ?, ?)",
                    (spotify_track_id, f"spotify:track:{spotify_track_id}", track_name),
                ).lastrowid)
                connection.execute(
                    """
                    INSERT INTO source_track_map (
                      source_track_id, release_track_id, match_method, confidence, status, explanation
                    ) VALUES (?, ?, 'seed', 1.0, 'accepted', 'seed')
                    """,
                    (source_track_id, release_track_id),
                )
                connection.execute(
                    """
                    INSERT INTO spotify_album_track (
                      spotify_album_id, spotify_track_id, disc_number, track_number, name,
                      duration_ms, artists_json, raw_json, fetched_at, last_status
                    ) VALUES (?, ?, 1, ?, ?, 180000, ?, '{}', '2026-06-18T00:00:00Z', 'ok')
                    """,
                    (spotify_album_id, spotify_track_id, index, track_name, json.dumps(artists)),
                )
            mark_generated_recording_track_clusters_dirty_with_connection(
                connection,
                release_track_ids,
                reason="album_family_test_seed",
            )
        return release_album_id

    def test_reviewed_family_exposes_versions_and_ghost_tracks(self) -> None:
        original_id = self._seed_album("Family Album", "album-original", 2020, ["Core One", "Core Two"])
        deluxe_id = self._seed_album("Family Album (Deluxe Edition)", "album-deluxe", 2021, ["Core One", "Core Two", "Bonus"])
        expanded_id = self._seed_album(
            "Family Album (Expanded Deluxe Edition)",
            "album-expanded",
            2022,
            ["Core One", "Core Two", "Bonus", "Expanded Only"],
        )
        # Albums ingested after the original one-to-one bootstrap may not have a family yet.
        with sqlite_connection(write=True) as connection:
            family_id = int(connection.execute(
                "SELECT album_family_id FROM album_family_map WHERE release_album_id = ?",
                (original_id,),
            ).fetchone()[0])
            connection.execute("DELETE FROM album_family_map WHERE release_album_id = ?", (original_id,))
            connection.execute("DELETE FROM album_family WHERE id = ?", (family_id,))

        preview = apply_reviewed_album_family_grouping(
            release_album_ids=[original_id, deluxe_id, expanded_id],
            canonical_release_album_id=original_id,
            rationale="Reviewed test family",
            apply=False,
        )
        self.assertTrue(preview["safe"])

        applied = apply_reviewed_album_family_grouping(
            release_album_ids=[original_id, deluxe_id, expanded_id],
            canonical_release_album_id=original_id,
            rationale="Reviewed test family",
            apply=True,
        )
        self.assertTrue(applied["applied"])

        selected_items = [
            {"id": "album-original-track-1", "name": "Core One"},
            {"id": "album-original-track-2", "name": "Core Two"},
        ]
        context = build_album_family_context(
            selected_spotify_album_id="album-original",
            selected_items=selected_items,
        )

        self.assertIsNotNone(context)
        self.assertEqual("Family Album", context["core_name"])
        self.assertEqual(
            ["Original", "Deluxe Edition", "Expanded Deluxe Edition"],
            [item["label"] for item in context["versions"]],
        )
        self.assertEqual([360000, 540000, 720000], [item["total_duration_ms"] for item in context["versions"]])
        ghost_names = [item["name"] for item in context["items"] if item["family_exclusive"]]
        self.assertEqual(["Bonus", "Expanded Only"], ghost_names)
        bonus = next(item for item in context["items"] if item["name"] == "Bonus")
        self.assertEqual("album-deluxe", bonus["family_switch_album_id"])
        with sqlite_connection(row_factory=sqlite3.Row) as connection:
            review_count = int(connection.execute("SELECT count(*) FROM album_family_review").fetchone()[0])
        self.assertEqual(1, review_count)

    def test_anniversary_remaster_uses_core_album_name_and_remaster_label(self) -> None:
        name = "Chutes Too Narrow (20th Anniversary Remaster)"
        self.assertEqual("Chutes Too Narrow", _core_album_name(name))
        self.assertEqual("Remaster", _edition_label(name, "Chutes Too Narrow"))

    def test_prefix_named_expansion_requires_complete_original_track_coverage(self) -> None:
        original_id = self._seed_album("OK Computer", "ok-original", 1997, ["Airbag", "Paranoid Android"])
        expanded_id = self._seed_album(
            "OK Computer OKNOTOK 1997 2017",
            "ok-expanded",
            2017,
            ["Airbag", "Paranoid Android", "I Promise"],
        )

        preview = apply_reviewed_album_family_grouping(
            release_album_ids=[original_id, expanded_id],
            canonical_release_album_id=original_id,
            rationale="Prefix expansion test",
            apply=False,
        )

        self.assertTrue(preview["safe"])
        self.assertEqual("core_title_prefix_extension", preview["title_relations"][expanded_id])
        self.assertEqual("Expanded Edition", _edition_label("OK Computer OKNOTOK 1997 2017", "OK Computer"))

    def test_prefix_named_expansion_rejects_missing_original_tracks(self) -> None:
        original_id = self._seed_album("Core Album", "core-original", 2020, ["One", "Two"])
        incomplete_id = self._seed_album("Core Album Revisited", "core-incomplete", 2022, ["One", "Bonus"])

        preview = apply_reviewed_album_family_grouping(
            release_album_ids=[original_id, incomplete_id],
            canonical_release_album_id=original_id,
            rationale="Incomplete prefix expansion test",
            apply=False,
        )

        self.assertFalse(preview["safe"])
        self.assertIn(f"incomplete_core_track_coverage:{incomplete_id}", preview["blockers"])

    def test_explicit_disk_two_is_allowed_as_companion_without_core_tracks(self) -> None:
        original_id = self._seed_album("In Rainbows", "rainbows-original", 2007, ["15 Step", "Bodysnatchers"])
        disk_two_id = self._seed_album("In Rainbows (Disk 2)", "rainbows-disk-two", 2007, ["MK 1", "Down Is the New Up"])

        preview = apply_reviewed_album_family_grouping(
            release_album_ids=[original_id, disk_two_id],
            canonical_release_album_id=original_id,
            rationale="Companion disc test",
            apply=False,
        )

        self.assertTrue(preview["safe"])
        self.assertIn(f"explicit_companion_disc:{disk_two_id}", preview["reasons"])
        self.assertEqual("In Rainbows", _core_album_name("In Rainbows (Disk 2)"))
        self.assertEqual("Disk 2", _edition_label("In Rainbows (Disk 2)", "In Rainbows"))

        applied = apply_reviewed_album_family_grouping(
            release_album_ids=[original_id, disk_two_id],
            canonical_release_album_id=original_id,
            rationale="Companion disc test",
            apply=True,
        )
        self.assertTrue(applied["applied"])
        context = build_album_family_context(
            selected_spotify_album_id="rainbows-original",
            selected_items=[
                {"id": "rainbows-original-track-1", "name": "15 Step", "disc_number": 1, "track_number": 1},
                {"id": "rainbows-original-track-2", "name": "Bodysnatchers", "disc_number": 1, "track_number": 2},
            ],
        )
        self.assertIsNotNone(context)
        assert context is not None
        self.assertEqual(
            {1, 2},
            {int(item["disc_number"]) for item in context["items"] if item.get("disc_number") is not None},
        )

        disk_two_context = build_album_family_context(
            selected_spotify_album_id="rainbows-disk-two",
            selected_items=[
                {"id": "rainbows-disk-two-track-1", "name": "MK 1", "disc_number": 1, "track_number": 1},
                {"id": "rainbows-disk-two-track-2", "name": "Down Is the New Up", "disc_number": 1, "track_number": 2},
            ],
        )
        self.assertIsNotNone(disk_two_context)
        assert disk_two_context is not None
        displayed_discs = [
            int(item["disc_number"])
            for item in disk_two_context["items"]
            if item.get("disc_number") is not None
        ]
        self.assertEqual(sorted(displayed_discs), displayed_discs)
        self.assertEqual(1, displayed_discs[0])
        self.assertEqual(2, displayed_discs[-1])

    def test_named_edition_without_reviewed_family_still_exposes_selector_context(self) -> None:
        album_id = self._seed_album(
            "Chutes Too Narrow (20th Anniversary Remaster)",
            "album-remaster",
            2023,
            ["Kissing the Lipless - 2023 Remaster"],
        )
        with sqlite_connection(write=True) as connection:
            connection.execute("DELETE FROM album_family_map WHERE release_album_id = ?", (album_id,))

        context = build_album_family_context(
            selected_spotify_album_id="album-remaster",
            selected_items=[{"id": "album-remaster-track-1", "name": "Kissing the Lipless - 2023 Remaster"}],
        )

        self.assertIsNotNone(context)
        self.assertEqual("Chutes Too Narrow", context["core_name"])
        self.assertEqual("Remaster", context["versions"][0]["label"])
        self.assertEqual("20th Anniversary Remaster", context["versions"][0]["menu_label"])
        self.assertEqual(2023, context["versions"][0]["release_year"])

    def test_named_edition_with_one_to_one_family_still_exposes_selector_context(self) -> None:
        self._seed_album(
            "Chutes Too Narrow (20th Anniversary Remaster)",
            "album-remaster",
            2023,
            ["Kissing the Lipless - 2023 Remaster"],
        )

        context = build_album_family_context(
            selected_spotify_album_id="album-remaster",
            selected_items=[{"id": "album-remaster-track-1", "name": "Kissing the Lipless - 2023 Remaster"}],
        )

        self.assertIsNotNone(context)
        self.assertEqual("Chutes Too Narrow", context["core_name"])
        self.assertEqual(["Remaster"], [version["label"] for version in context["versions"]])
