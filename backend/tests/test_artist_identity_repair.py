from __future__ import annotations

import json
import os
import sqlite3
import unittest
from contextlib import closing

from backend.app.artist_identity_repair import (
    build_duplicate_artist_audit,
    repair_composite_artist_credits,
    repair_duplicate_artists,
)
from backend.app.db import (
    _normalize_name,
    apply_pending_migrations,
    backfill_spotify_source_entities,
    backfill_local_text_entities,
    ensure_sqlite_db,
    insert_raw_play_event,
)


class ArtistIdentityRepairTests(unittest.TestCase):
    def setUp(self) -> None:
        self._previous_sqlite_db_path = os.environ.get("SQLITE_DB_PATH")
        self.db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_artist_identity_repair.sqlite3",
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

    def _insert_artist(self, connection: sqlite3.Connection, name: str) -> int:
        cursor = connection.execute(
            "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
            (name, _normalize_name(name)),
        )
        return int(cursor.lastrowid)

    def _insert_source_artist_map(
        self,
        connection: sqlite3.Connection,
        *,
        artist_id: int,
        source_name: str,
        external_id: str,
        match_method: str,
    ) -> int:
        cursor = connection.execute(
            """
            INSERT INTO source_artist (source_name, external_id, external_uri, source_name_raw)
            VALUES (?, ?, ?, ?)
            """,
            (source_name, external_id, f"{source_name}:artist:{external_id}", "Radiohead"),
        )
        source_artist_id = int(cursor.lastrowid)
        cursor = connection.execute(
            """
            INSERT INTO source_artist_map (
              source_artist_id,
              artist_id,
              match_method,
              confidence,
              status,
              explanation
            )
            VALUES (?, ?, ?, 1.0, 'accepted', 'test')
            """,
            (source_artist_id, artist_id, match_method),
        )
        return int(cursor.lastrowid)

    def _insert_source_album_map(
        self,
        connection: sqlite3.Connection,
        *,
        release_album_id: int,
        source_name: str,
        external_id: str,
    ) -> int:
        cursor = connection.execute(
            """
            INSERT INTO source_album (source_name, external_id, external_uri, source_name_raw)
            VALUES (?, ?, ?, ?)
            """,
            (source_name, external_id, f"{source_name}:album:{external_id}", "Album"),
        )
        source_album_id = int(cursor.lastrowid)
        cursor = connection.execute(
            """
            INSERT INTO source_album_map (
              source_album_id,
              release_album_id,
              match_method,
              confidence,
              status,
              explanation
            )
            VALUES (?, ?, 'test', 1.0, 'accepted', 'test')
            """,
            (source_album_id, release_album_id),
        )
        return int(cursor.lastrowid)

    def _seed_safe_duplicate_group(self) -> tuple[int, int]:
        with closing(sqlite3.connect(self.db_path)) as connection:
            spotify_artist_id = self._insert_artist(connection, "Radiohead")
            text_artist_id = self._insert_artist(connection, "Radiohead")
            self._insert_source_artist_map(
                connection,
                artist_id=spotify_artist_id,
                source_name="spotify",
                external_id="spotify-radiohead",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=text_artist_id,
                source_name="history_raw",
                external_id="history-radiohead",
                match_method="history_raw_text",
            )
            album_1 = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Kid A', 'kid a')"
            ).lastrowid
            album_2 = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Amnesiac', 'amnesiac')"
            ).lastrowid
            track_1 = connection.execute(
                "INSERT INTO release_track (primary_name, normalized_name) VALUES ('Idioteque', 'idioteque')"
            ).lastrowid
            track_2 = connection.execute(
                "INSERT INTO release_track (primary_name, normalized_name) VALUES ('Pyramid Song', 'pyramid song')"
            ).lastrowid
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (album_1, spotify_artist_id),
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (album_1, text_artist_id),
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (album_2, text_artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (track_1, spotify_artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (track_1, text_artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (track_2, text_artist_id),
            )
            connection.commit()
        return spotify_artist_id, text_artist_id

    def test_duplicate_artist_audit_reports_text_and_spotify_rows(self) -> None:
        spotify_artist_id, text_artist_id = self._seed_safe_duplicate_group()

        audit = build_duplicate_artist_audit()

        self.assertEqual(1, audit["groups_found"])
        group = audit["groups"][0]
        self.assertEqual("radiohead", group["normalized_name"])
        self.assertEqual(spotify_artist_id, group["recommended_canonical_artist_id"])
        self.assertEqual([text_artist_id], group["duplicate_candidate_artist_ids"])
        by_id = {artist["artist_id"]: artist for artist in group["artists"]}
        self.assertTrue(by_id[spotify_artist_id]["provider_backed"])
        self.assertFalse(by_id[text_artist_id]["provider_backed"])
        self.assertTrue(by_id[text_artist_id]["text_only"])
        self.assertEqual(1, by_id[spotify_artist_id]["album_artist_link_count"])
        self.assertEqual(2, by_id[text_artist_id]["track_artist_link_count"])
        self.assertEqual("spotify", by_id[spotify_artist_id]["source_artist_maps"][0]["source_name"])

    def test_dry_run_repair_does_not_mutate_database(self) -> None:
        self._seed_safe_duplicate_group()
        before = self._counts()

        result = repair_duplicate_artists()
        after = self._counts()

        self.assertTrue(result["dry_run"])
        self.assertEqual(1, len(result["safe_groups"]))
        self.assertEqual(before, after)

    def test_same_normalized_name_without_identity_evidence_does_not_repair(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            spotify_artist_id = self._insert_artist(connection, "John Williams")
            text_artist_id = self._insert_artist(connection, "John Williams")
            self._insert_source_artist_map(
                connection,
                artist_id=spotify_artist_id,
                source_name="spotify",
                external_id="spotify-john-williams",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=text_artist_id,
                source_name="history_raw",
                external_id="history-john-williams",
                match_method="history_raw_text",
            )
            provider_album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Star Wars', 'star wars')"
            ).lastrowid
            text_album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Jaws', 'jaws')"
            ).lastrowid
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (provider_album_id, spotify_artist_id),
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (text_album_id, text_artist_id),
            )
            connection.commit()

        result = repair_duplicate_artists(dry_run=False)

        self.assertEqual([], result["safe_groups"])
        self.assertEqual([], result["artist_rows_deleted"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            self.assertEqual(2, int(connection.execute("SELECT count(*) FROM artist").fetchone()[0]))

    def test_text_only_same_name_group_is_no_provider_review(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_1 = self._insert_artist(connection, "Michelle")
            artist_2 = self._insert_artist(connection, "MICHELLE")
            self._insert_source_artist_map(
                connection,
                artist_id=artist_1,
                source_name="history_raw",
                external_id="history-michelle-1",
                match_method="history_raw_text",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=artist_2,
                source_name="history_raw",
                external_id="history-michelle-2",
                match_method="history_raw_text",
            )
            connection.commit()

        audit = build_duplicate_artist_audit()

        self.assertEqual(1, audit["summary"]["exact_name_no_provider_review_only_groups"])
        group = audit["candidate_categories"]["exact_name_no_provider_review_only"]["groups"][0]
        self.assertEqual("michelle", group["normalized_name"])
        self.assertEqual("exact_name_no_provider_review_only", group["category"])
        self.assertFalse(group["repairable"])

    def test_orphan_placeholder_does_not_repair(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            spotify_artist_id = self._insert_artist(connection, "Phoenix")
            text_artist_id = self._insert_artist(connection, "Phoenix")
            self._insert_source_artist_map(
                connection,
                artist_id=spotify_artist_id,
                source_name="spotify",
                external_id="spotify-phoenix",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=text_artist_id,
                source_name="history_raw",
                external_id="history-phoenix",
                match_method="history_raw_text",
            )
            connection.commit()

        result = repair_duplicate_artists(dry_run=False)

        self.assertEqual([], result["safe_groups"])
        self.assertEqual("orphan_placeholder_without_identity_evidence", result["skipped_groups"][0]["reason"])
        self.assertEqual([], result["artist_rows_deleted"])

    def test_write_repair_repoints_links_deletes_duplicates_and_orphan_artist(self) -> None:
        spotify_artist_id, text_artist_id = self._seed_safe_duplicate_group()

        result = repair_duplicate_artists(dry_run=False)

        self.assertFalse(result["dry_run"])
        self.assertEqual([text_artist_id], result["artist_rows_deleted"])
        self.assertEqual(1, len(result["source_mappings_to_move"]))
        self.assertEqual(1, len(result["album_links_to_move"]))
        self.assertEqual(1, len(result["album_links_to_delete"]))
        self.assertEqual(1, len(result["track_links_to_move"]))
        self.assertEqual(1, len(result["track_links_to_delete"]))

        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_ids = [row[0] for row in connection.execute("SELECT id FROM artist ORDER BY id ASC").fetchall()]
            source_map_artist_ids = [
                row[0] for row in connection.execute("SELECT artist_id FROM source_artist_map ORDER BY id ASC").fetchall()
            ]
            album_artist_ids = [
                row[0] for row in connection.execute("SELECT artist_id FROM album_artist ORDER BY id ASC").fetchall()
            ]
            track_artist_ids = [
                row[0] for row in connection.execute("SELECT artist_id FROM track_artist ORDER BY id ASC").fetchall()
            ]
        self.assertEqual([spotify_artist_id], artist_ids)
        self.assertEqual([spotify_artist_id, spotify_artist_id], source_map_artist_ids)
        self.assertEqual([spotify_artist_id, spotify_artist_id], album_artist_ids)
        self.assertEqual([spotify_artist_id, spotify_artist_id], track_artist_ids)
        self.assertIn("shared_release_album_id", result["evidence_type_counts"])
        self.assertIn("shared_release_track_id", result["evidence_type_counts"])

    def test_shared_normalized_album_title_with_provider_context_repairs(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            spotify_artist_id = self._insert_artist(connection, "Radiohead")
            text_artist_id = self._insert_artist(connection, "Radiohead")
            self._insert_source_artist_map(
                connection,
                artist_id=spotify_artist_id,
                source_name="spotify",
                external_id="spotify-radiohead",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=text_artist_id,
                source_name="history_raw",
                external_id="history-radiohead",
                match_method="history_raw_text",
            )
            provider_album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Kid A', 'kid a')"
            ).lastrowid
            text_album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Kid A', 'kid a')"
            ).lastrowid
            self._insert_source_album_map(
                connection,
                release_album_id=provider_album_id,
                source_name="spotify",
                external_id="spotify-kid-a",
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (provider_album_id, spotify_artist_id),
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (text_album_id, text_artist_id),
            )
            connection.commit()

        result = repair_duplicate_artists(dry_run=False)

        self.assertEqual(1, len(result["safe_groups"]))
        self.assertEqual("exact_name_album_title_provider_context_safe_repair", result["safe_groups"][0]["category"])
        self.assertEqual({"shared_normalized_album_title_with_provider_context": 1}, result["evidence_type_counts"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_ids = [row[0] for row in connection.execute("SELECT id FROM artist ORDER BY id ASC").fetchall()]
            album_artist_ids = [
                row[0] for row in connection.execute("SELECT artist_id FROM album_artist ORDER BY release_album_id ASC").fetchall()
            ]
        self.assertEqual([spotify_artist_id], artist_ids)
        self.assertEqual([spotify_artist_id, spotify_artist_id], album_artist_ids)

    def test_same_album_title_without_provider_context_does_not_repair(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            spotify_artist_id = self._insert_artist(connection, "Radiohead")
            text_artist_id = self._insert_artist(connection, "Radiohead")
            self._insert_source_artist_map(
                connection,
                artist_id=spotify_artist_id,
                source_name="spotify",
                external_id="spotify-radiohead",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=text_artist_id,
                source_name="history_raw",
                external_id="history-radiohead",
                match_method="history_raw_text",
            )
            provider_album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Kid A', 'kid a')"
            ).lastrowid
            text_album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Kid A', 'kid a')"
            ).lastrowid
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (provider_album_id, spotify_artist_id),
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (text_album_id, text_artist_id),
            )
            connection.commit()

        result = repair_duplicate_artists(dry_run=False)

        self.assertEqual([], result["safe_groups"])
        self.assertEqual([], result["artist_rows_deleted"])

    def test_shared_release_track_repairs(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            spotify_artist_id = self._insert_artist(connection, "Alex G")
            text_artist_id = self._insert_artist(connection, "Alex G")
            self._insert_source_artist_map(
                connection,
                artist_id=spotify_artist_id,
                source_name="spotify",
                external_id="spotify-alex-g",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=text_artist_id,
                source_name="history_raw",
                external_id="history-alex-g",
                match_method="history_raw_text",
            )
            track_id = connection.execute(
                "INSERT INTO release_track (primary_name, normalized_name) VALUES ('Runner', 'runner')"
            ).lastrowid
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (track_id, spotify_artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (track_id, text_artist_id),
            )
            connection.commit()

        result = repair_duplicate_artists(dry_run=False)

        self.assertEqual(1, len(result["safe_groups"]))
        self.assertIn("shared_release_track_id", result["evidence_type_counts"])

    def test_two_provider_backed_artists_with_same_name_are_skipped(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_1 = self._insert_artist(connection, "Radiohead")
            artist_2 = self._insert_artist(connection, "Radiohead")
            self._insert_source_artist_map(
                connection,
                artist_id=artist_1,
                source_name="spotify",
                external_id="spotify-1",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=artist_2,
                source_name="spotify",
                external_id="spotify-2",
                match_method="provider_identity",
            )
            connection.commit()

        result = repair_duplicate_artists()

        self.assertEqual([], result["safe_groups"])
        self.assertEqual("multiple_provider_backed_artists", result["skipped_groups"][0]["reason"])
        self.assertEqual([], result["artist_rows_to_delete"])

    def test_audit_reports_stylization_candidates_separately(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_1 = self._insert_artist(connection, "Beyoncé")
            artist_2 = self._insert_artist(connection, "Beyonce")
            self._insert_source_artist_map(
                connection,
                artist_id=artist_1,
                source_name="spotify",
                external_id="spotify-beyonce",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=artist_2,
                source_name="history_raw",
                external_id="history-beyonce",
                match_method="history_raw_text",
            )
            connection.commit()

        audit = build_duplicate_artist_audit()

        self.assertEqual(0, audit["summary"]["exact_name_groups"])
        self.assertEqual(1, audit["summary"]["stylization_groups"])
        group = audit["candidate_categories"]["stylization"]["groups"][0]
        self.assertFalse(group["repairable"])
        self.assertEqual("beyonce", group["matching_key"])
        self.assertEqual("beyonce", group["uniform_matching_text"])
        self.assertEqual(["beyonce", "beyoncé"], group["normalized_names"])
        self.assertEqual({artist_1, artist_2}, {artist["artist_id"] for artist in group["artists"]})

    def test_stylization_candidates_are_never_repaired(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_1 = self._insert_artist(connection, "Beyoncé")
            artist_2 = self._insert_artist(connection, "Beyonce")
            self._insert_source_artist_map(
                connection,
                artist_id=artist_1,
                source_name="spotify",
                external_id="spotify-beyonce",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=artist_2,
                source_name="history_raw",
                external_id="history-beyonce",
                match_method="history_raw_text",
            )
            provider_album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Lemonade', 'lemonade')"
            ).lastrowid
            text_album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Lemonade', 'lemonade')"
            ).lastrowid
            self._insert_source_album_map(
                connection,
                release_album_id=provider_album_id,
                source_name="spotify",
                external_id="spotify-lemonade",
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (provider_album_id, artist_1),
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (text_album_id, artist_2),
            )
            connection.commit()

        result = repair_duplicate_artists(dry_run=False)

        self.assertEqual([], result["safe_groups"])
        self.assertEqual([], result["artist_rows_deleted"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            self.assertEqual(2, int(connection.execute("SELECT count(*) FROM artist").fetchone()[0]))

    def test_audit_reports_non_latin_same_album_candidates(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_1 = self._insert_artist(connection, "Ofra Haza")
            artist_2 = self._insert_artist(connection, "עפרה חזה")
            self._insert_source_artist_map(
                connection,
                artist_id=artist_1,
                source_name="spotify",
                external_id="spotify-ofra",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=artist_2,
                source_name="history_raw",
                external_id="history-ofra-hebrew",
                match_method="history_raw_text",
            )
            album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Shared Album', 'shared album')"
            ).lastrowid
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (album_id, artist_1),
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (album_id, artist_2),
            )
            connection.commit()

        audit = build_duplicate_artist_audit()

        self.assertEqual(0, audit["summary"]["exact_name_groups"])
        self.assertEqual(1, audit["summary"]["similar_same_album_groups"])
        group = audit["candidate_categories"]["similar_same_album"]["groups"][0]
        self.assertFalse(group["repairable"])
        self.assertEqual("shared_album_with_non_latin_name", group["reason"])
        self.assertEqual(1, group["shared_album_count"])
        self.assertEqual({artist_1, artist_2}, {artist["artist_id"] for artist in group["artists"]})

    def test_comma_separated_text_credit_is_separate_from_same_album_candidates(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            spotify_artist_id = self._insert_artist(connection, "JJ Whitefield")
            second_spotify_artist_id = self._insert_artist(connection, "Myríad")
            composite_artist_id = self._insert_artist(connection, "Myríad, JJ Whitefield")
            text_artist_id = self._insert_artist(connection, "JJ Whitefield")
            self._insert_source_artist_map(
                connection,
                artist_id=spotify_artist_id,
                source_name="spotify",
                external_id="spotify-jj-whitefield",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=second_spotify_artist_id,
                source_name="spotify",
                external_id="spotify-myriad",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=composite_artist_id,
                source_name="history_raw",
                external_id="history-composite-credit",
                match_method="history_raw_text",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=text_artist_id,
                source_name="history_raw",
                external_id="history-jj-whitefield",
                match_method="history_raw_text",
            )
            album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Shared Album', 'shared album')"
            ).lastrowid
            track_id = connection.execute(
                "INSERT INTO release_track (primary_name, normalized_name) VALUES ('Shared Track', 'shared track')"
            ).lastrowid
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (album_id, track_id),
            )
            text_album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Unrelated Album', 'unrelated album')"
            ).lastrowid
            spotify_album_link_id = connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (album_id, spotify_artist_id),
            ).lastrowid
            second_album_link_id = connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 2)",
                (album_id, second_spotify_artist_id),
            ).lastrowid
            composite_album_link_id = connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (album_id, composite_artist_id),
            ).lastrowid
            spotify_track_link_id = connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (track_id, spotify_artist_id),
            ).lastrowid
            second_track_link_id = connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 2)",
                (track_id, second_spotify_artist_id),
            ).lastrowid
            composite_track_link_id = connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (track_id, composite_artist_id),
            ).lastrowid
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (text_album_id, text_artist_id),
            )
            connection.commit()

        audit = build_duplicate_artist_audit()

        exact_group = audit["candidate_categories"]["exact_name"]["groups"][0]
        self.assertEqual("jj whitefield", exact_group["normalized_name"])
        self.assertEqual("exact_name_only_review", exact_group["category"])
        self.assertFalse(exact_group["repairable"])
        self.assertEqual([text_artist_id], exact_group["duplicate_candidate_artist_ids"])
        self.assertEqual(0, audit["summary"]["similar_same_album_groups"])
        self.assertEqual(1, audit["summary"]["composite_credit_groups"])
        group = audit["candidate_categories"]["composite_credit"]["groups"][0]
        self.assertEqual("composite_credit", group["category"])
        self.assertFalse(group["repairable"])
        self.assertEqual("comma_separated_history_credit_on_same_album", group["reason"])
        self.assertEqual(
            {spotify_artist_id, second_spotify_artist_id, composite_artist_id},
            {artist["artist_id"] for artist in group["artists"]},
        )
        cleanup_plan = group["cleanup_plan"]
        self.assertTrue(cleanup_plan["ready_for_cleanup"])
        self.assertEqual(composite_artist_id, cleanup_plan["composite_artist_id"])
        self.assertEqual(
            ["myríad", "jj whitefield"],
            [part["normalized_name"] for part in cleanup_plan["credit_parts"]],
        )
        self.assertEqual(
            {spotify_artist_id, second_spotify_artist_id},
            set(cleanup_plan["matched_artist_ids"]),
        )
        self.assertEqual([composite_album_link_id], [item["link_id"] for item in cleanup_plan["album_links_to_delete"]])
        self.assertEqual([], cleanup_plan["album_links_to_insert"])
        self.assertEqual([composite_track_link_id], [item["link_id"] for item in cleanup_plan["track_links_to_delete"]])
        self.assertEqual([], cleanup_plan["track_links_review_only"])
        self.assertNotIn(spotify_album_link_id, [item["link_id"] for item in cleanup_plan["album_links_to_delete"]])
        self.assertNotIn(second_album_link_id, [item["link_id"] for item in cleanup_plan["album_links_to_delete"]])
        self.assertNotIn(spotify_track_link_id, [item["link_id"] for item in cleanup_plan["track_links_to_delete"]])
        self.assertNotIn(second_track_link_id, [item["link_id"] for item in cleanup_plan["track_links_to_delete"]])

        dry_run = repair_duplicate_artists(dry_run=True)
        write_result = repair_duplicate_artists(dry_run=False)

        self.assertEqual([], dry_run["safe_groups"])
        self.assertEqual([], dry_run["artist_rows_to_delete"])
        self.assertEqual([], write_result["safe_groups"])
        self.assertEqual([], write_result["artist_rows_deleted"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_ids = [row[0] for row in connection.execute("SELECT id FROM artist ORDER BY id ASC").fetchall()]
        self.assertEqual([spotify_artist_id, second_spotify_artist_id, composite_artist_id, text_artist_id], artist_ids)

    def test_comma_credit_with_friends_suffix_matches_existing_artists_for_review(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            perfume_artist_id = self._insert_artist(connection, "Perfume Genius")
            sharon_artist_id = self._insert_artist(connection, "Sharon Van Etten")
            composite_artist_id = self._insert_artist(connection, "Perfume Genius, Sharon Van Etten & Friends")
            self._insert_source_artist_map(
                connection,
                artist_id=perfume_artist_id,
                source_name="history_raw",
                external_id="history-perfume",
                match_method="history_raw_text",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=sharon_artist_id,
                source_name="spotify",
                external_id="spotify-sharon",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=composite_artist_id,
                source_name="history_raw",
                external_id="history-composite-friends",
                match_method="history_raw_text",
            )
            album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Day of the Dead', 'day of the dead')"
            ).lastrowid
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (album_id, composite_artist_id),
            )
            connection.commit()

        audit = build_duplicate_artist_audit()

        groups = audit["candidate_categories"]["composite_credit"]["groups"]
        group = next(group for group in groups if group["composite_artist_id"] == composite_artist_id)
        self.assertEqual("comma_credit_parts_match_existing_artists_review_only", group["reason"])
        self.assertEqual(
            {perfume_artist_id, sharon_artist_id, composite_artist_id},
            {artist["artist_id"] for artist in group["artists"]},
        )
        cleanup_plan = group["cleanup_plan"]
        self.assertFalse(cleanup_plan["ready_for_cleanup"])
        self.assertTrue(cleanup_plan["all_parts_matched"])
        self.assertEqual(
            ["perfume genius", "sharon van etten"],
            [part["normalized_name"] for part in cleanup_plan["credit_parts"]],
        )

    def test_composite_credit_cleanup_dry_run_does_not_mutate(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            spotify_artist_id = self._insert_artist(connection, "JJ Whitefield")
            second_spotify_artist_id = self._insert_artist(connection, "Myríad")
            composite_artist_id = self._insert_artist(connection, "Myríad, JJ Whitefield")
            self._insert_source_artist_map(
                connection,
                artist_id=spotify_artist_id,
                source_name="spotify",
                external_id="spotify-jj-whitefield",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=second_spotify_artist_id,
                source_name="spotify",
                external_id="spotify-myriad",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=composite_artist_id,
                source_name="history_raw",
                external_id="history-composite-credit",
                match_method="history_raw_text",
            )
            album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Shared Album', 'shared album')"
            ).lastrowid
            track_id = connection.execute(
                "INSERT INTO release_track (primary_name, normalized_name) VALUES ('Shared Track', 'shared track')"
            ).lastrowid
            connection.execute("INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)", (album_id, track_id))
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (album_id, spotify_artist_id),
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 2)",
                (album_id, second_spotify_artist_id),
            )
            composite_album_link_id = connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (album_id, composite_artist_id),
            ).lastrowid
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (track_id, spotify_artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 2)",
                (track_id, second_spotify_artist_id),
            )
            composite_track_link_id = connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (track_id, composite_artist_id),
            ).lastrowid
            connection.commit()

        dry_run = repair_composite_artist_credits(dry_run=True)

        self.assertTrue(dry_run["dry_run"])
        self.assertEqual(1, len(dry_run["safe_groups"]))
        self.assertEqual([composite_album_link_id], [item["link_id"] for item in dry_run["album_links_to_delete"]])
        self.assertEqual([composite_track_link_id], [item["link_id"] for item in dry_run["track_links_to_delete"]])
        with closing(sqlite3.connect(self.db_path)) as connection:
            self.assertEqual(3, int(connection.execute("SELECT count(*) FROM album_artist").fetchone()[0]))
            self.assertEqual(3, int(connection.execute("SELECT count(*) FROM track_artist").fetchone()[0]))
            self.assertEqual(3, int(connection.execute("SELECT count(*) FROM artist").fetchone()[0]))

    def test_composite_credit_cleanup_deletes_ready_links_but_keeps_source_mapped_artist(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            spotify_artist_id = self._insert_artist(connection, "JJ Whitefield")
            second_spotify_artist_id = self._insert_artist(connection, "Myríad")
            composite_artist_id = self._insert_artist(connection, "Myríad, JJ Whitefield")
            self._insert_source_artist_map(
                connection,
                artist_id=spotify_artist_id,
                source_name="spotify",
                external_id="spotify-jj-whitefield",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=second_spotify_artist_id,
                source_name="spotify",
                external_id="spotify-myriad",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=composite_artist_id,
                source_name="history_raw",
                external_id="history-composite-credit",
                match_method="history_raw_text",
            )
            album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Shared Album', 'shared album')"
            ).lastrowid
            track_id = connection.execute(
                "INSERT INTO release_track (primary_name, normalized_name) VALUES ('Shared Track', 'shared track')"
            ).lastrowid
            connection.execute("INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)", (album_id, track_id))
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (album_id, spotify_artist_id),
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 2)",
                (album_id, second_spotify_artist_id),
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (album_id, composite_artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (track_id, spotify_artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 2)",
                (track_id, second_spotify_artist_id),
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (track_id, composite_artist_id),
            )
            connection.commit()

        result = repair_composite_artist_credits(dry_run=False)

        self.assertFalse(result["dry_run"])
        self.assertEqual(1, len(result["safe_groups"]))
        self.assertEqual([], result["artist_rows_deleted"])
        self.assertEqual(composite_artist_id, result["artist_row_deletes_skipped"][0]["artist_id"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            self.assertEqual(2, int(connection.execute("SELECT count(*) FROM album_artist").fetchone()[0]))
            self.assertEqual(2, int(connection.execute("SELECT count(*) FROM track_artist").fetchone()[0]))
            self.assertEqual(3, int(connection.execute("SELECT count(*) FROM artist").fetchone()[0]))
            composite_album_links = int(
                connection.execute("SELECT count(*) FROM album_artist WHERE artist_id = ?", (composite_artist_id,)).fetchone()[0]
            )
            composite_track_links = int(
                connection.execute("SELECT count(*) FROM track_artist WHERE artist_id = ?", (composite_artist_id,)).fetchone()[0]
            )
        self.assertEqual(0, composite_album_links)
        self.assertEqual(0, composite_track_links)

    def test_composite_credit_cleanup_skips_review_only_groups(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            perfume_artist_id = self._insert_artist(connection, "Perfume Genius")
            sharon_artist_id = self._insert_artist(connection, "Sharon Van Etten")
            composite_artist_id = self._insert_artist(connection, "Perfume Genius, Sharon Van Etten & Friends")
            self._insert_source_artist_map(
                connection,
                artist_id=perfume_artist_id,
                source_name="history_raw",
                external_id="history-perfume",
                match_method="history_raw_text",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=sharon_artist_id,
                source_name="spotify",
                external_id="spotify-sharon",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=composite_artist_id,
                source_name="history_raw",
                external_id="history-composite-friends",
                match_method="history_raw_text",
            )
            album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Day of the Dead', 'day of the dead')"
            ).lastrowid
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (album_id, composite_artist_id),
            )
            connection.commit()

        result = repair_composite_artist_credits(dry_run=False)

        self.assertEqual([], result["safe_groups"])
        self.assertEqual(1, len(result["skipped_groups"]))
        with closing(sqlite3.connect(self.db_path)) as connection:
            self.assertEqual(1, int(connection.execute("SELECT count(*) FROM album_artist").fetchone()[0]))
            self.assertEqual(3, int(connection.execute("SELECT count(*) FROM artist").fetchone()[0]))

    def test_composite_credit_cleanup_skips_provider_backed_full_comma_name(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_id = self._insert_artist(connection, "Bela Fleck, Zakir Hussein & Edgar Meyer")
            self._insert_source_artist_map(
                connection,
                artist_id=artist_id,
                source_name="spotify",
                external_id="spotify-trio",
                match_method="provider_identity",
            )
            album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('As We Speak', 'as we speak')"
            ).lastrowid
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (album_id, artist_id),
            )
            connection.commit()

        result = repair_composite_artist_credits(dry_run=False)

        self.assertEqual(0, result["groups_found"])
        self.assertEqual([], result["safe_groups"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            self.assertEqual(1, int(connection.execute("SELECT count(*) FROM album_artist").fetchone()[0]))
            self.assertEqual(1, int(connection.execute("SELECT count(*) FROM artist").fetchone()[0]))

    def test_similar_same_album_candidates_are_never_repaired(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_1 = self._insert_artist(connection, "Ofra Haza")
            artist_2 = self._insert_artist(connection, "עפרה חזה")
            self._insert_source_artist_map(
                connection,
                artist_id=artist_1,
                source_name="spotify",
                external_id="spotify-ofra",
                match_method="provider_identity",
            )
            self._insert_source_artist_map(
                connection,
                artist_id=artist_2,
                source_name="history_raw",
                external_id="history-ofra-hebrew",
                match_method="history_raw_text",
            )
            album_id = connection.execute(
                "INSERT INTO release_album (primary_name, normalized_name) VALUES ('Shared Album', 'shared album')"
            ).lastrowid
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (album_id, artist_1),
            )
            connection.execute(
                "INSERT INTO album_artist (release_album_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
                (album_id, artist_2),
            )
            connection.commit()

        result = repair_duplicate_artists(dry_run=False)

        self.assertEqual([], result["safe_groups"])
        self.assertEqual([], result["artist_rows_deleted"])

    def test_spotify_ingest_does_not_promote_existing_text_only_artist_without_shared_evidence(self) -> None:
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="history-row",
            played_at="2026-04-18T10:00:00Z",
            ms_played=90000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps({"master_metadata_album_artist_name": "Radiohead"}, sort_keys=True),
            spotify_track_uri=None,
            spotify_track_id=None,
            track_name_raw="History Track",
            artist_name_raw="Radiohead",
            album_name_raw="History Album",
            spotify_album_id=None,
            spotify_artist_ids_json=None,
        )
        local = backfill_local_text_entities()
        self.assertEqual(1, local["artists_created"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            text_artist_id = int(connection.execute("SELECT id FROM artist").fetchone()[0])

        insert_raw_play_event(
            source_type="spotify_recent",
            source_row_key="recent-row",
            played_at="2026-04-18T11:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps(
                {"item": {"artists": [{"id": "spotify-radiohead", "name": "Radiohead"}]}},
                sort_keys=True,
            ),
            spotify_track_uri="spotify:track:spotify-track",
            spotify_track_id="spotify-track",
            track_name_raw="Spotify Track",
            artist_name_raw="Radiohead",
            album_name_raw="Spotify Album",
            spotify_album_id="spotify-album",
            spotify_artist_ids_json=json.dumps(["spotify-radiohead"]),
        )
        exact = backfill_spotify_source_entities()

        self.assertEqual(1, exact["artists_created"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_count = int(connection.execute("SELECT count(*) FROM artist").fetchone()[0])
            spotify_map_artist_id = int(
                connection.execute(
                    """
                    SELECT sam.artist_id
                    FROM source_artist_map sam
                    JOIN source_artist sa
                      ON sa.id = sam.source_artist_id
                    WHERE sa.source_name = 'spotify'
                      AND sa.external_id = 'spotify-radiohead'
                    """
                ).fetchone()[0]
            )
        self.assertEqual(2, artist_count)
        self.assertNotEqual(text_artist_id, spotify_map_artist_id)

    def test_spotify_ingest_promotes_existing_text_only_artist_with_shared_track_evidence(self) -> None:
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="history-row",
            played_at="2026-04-18T10:00:00Z",
            ms_played=90000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps({"master_metadata_album_artist_name": "Radiohead"}, sort_keys=True),
            spotify_track_uri="spotify:track:spotify-track",
            spotify_track_id="spotify-track",
            track_name_raw="Shared Track",
            artist_name_raw="Radiohead",
            album_name_raw="Shared Album",
            spotify_album_id="spotify-album",
            spotify_artist_ids_json=None,
        )
        local = backfill_local_text_entities()
        self.assertEqual(1, local["artists_created"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            text_artist_id = int(connection.execute("SELECT id FROM artist").fetchone()[0])

        insert_raw_play_event(
            source_type="spotify_recent",
            source_row_key="recent-row",
            played_at="2026-04-18T11:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps(
                {"item": {"artists": [{"id": "spotify-radiohead", "name": "Radiohead"}]}},
                sort_keys=True,
            ),
            spotify_track_uri="spotify:track:spotify-track",
            spotify_track_id="spotify-track",
            track_name_raw="Shared Track",
            artist_name_raw="Radiohead",
            album_name_raw="Shared Album",
            spotify_album_id="spotify-album",
            spotify_artist_ids_json=json.dumps(["spotify-radiohead"]),
        )
        exact = backfill_spotify_source_entities()

        self.assertEqual(0, exact["artists_created"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_count = int(connection.execute("SELECT count(*) FROM artist").fetchone()[0])
            spotify_map_artist_id = int(
                connection.execute(
                    """
                    SELECT sam.artist_id
                    FROM source_artist_map sam
                    JOIN source_artist sa
                      ON sa.id = sam.source_artist_id
                    WHERE sa.source_name = 'spotify'
                      AND sa.external_id = 'spotify-radiohead'
                    """
                ).fetchone()[0]
            )
        self.assertEqual(1, artist_count)
        self.assertEqual(text_artist_id, spotify_map_artist_id)

    def test_spotify_ingest_promotes_existing_text_only_artist_with_album_title_provider_context(self) -> None:
        insert_raw_play_event(
            source_type="spotify_history",
            source_row_key="history-row",
            played_at="2026-04-18T10:00:00Z",
            ms_played=90000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps({"master_metadata_album_artist_name": "Radiohead"}, sort_keys=True),
            spotify_track_uri=None,
            spotify_track_id=None,
            track_name_raw="History Track",
            artist_name_raw="Radiohead",
            album_name_raw="Shared Album",
            spotify_album_id=None,
            spotify_artist_ids_json=None,
        )
        local = backfill_local_text_entities()
        self.assertEqual(1, local["artists_created"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            text_artist_id = int(connection.execute("SELECT id FROM artist").fetchone()[0])

        insert_raw_play_event(
            source_type="spotify_recent",
            source_row_key="recent-row",
            played_at="2026-04-18T11:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps(
                {"item": {"artists": [{"id": "spotify-radiohead", "name": "Radiohead"}]}},
                sort_keys=True,
            ),
            spotify_track_uri="spotify:track:spotify-track",
            spotify_track_id="spotify-track",
            track_name_raw="Spotify Track",
            artist_name_raw="Radiohead",
            album_name_raw="Shared Album",
            spotify_album_id="spotify-album",
            spotify_artist_ids_json=json.dumps(["spotify-radiohead"]),
        )
        exact = backfill_spotify_source_entities()

        self.assertEqual(0, exact["artists_created"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_count = int(connection.execute("SELECT count(*) FROM artist").fetchone()[0])
            spotify_map_artist_id = int(
                connection.execute(
                    """
                    SELECT sam.artist_id
                    FROM source_artist_map sam
                    JOIN source_artist sa
                      ON sa.id = sam.source_artist_id
                    WHERE sa.source_name = 'spotify'
                      AND sa.external_id = 'spotify-radiohead'
                    """
                ).fetchone()[0]
            )
        self.assertEqual(1, artist_count)
        self.assertEqual(text_artist_id, spotify_map_artist_id)

    def test_spotify_ingest_uses_existing_source_artist_map(self) -> None:
        payload = {"item": {"artists": [{"id": "artist-known", "name": "Known Artist"}]}}
        insert_raw_play_event(
            source_type="spotify_recent",
            source_row_key="recent-row-1",
            played_at="2026-04-18T11:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps(payload, sort_keys=True),
            spotify_track_uri="spotify:track:track-1",
            spotify_track_id="track-1",
            track_name_raw="Track 1",
            artist_name_raw="Known Artist",
            album_name_raw="Album 1",
            spotify_album_id="album-1",
            spotify_artist_ids_json=json.dumps(["artist-known"]),
        )
        insert_raw_play_event(
            source_type="spotify_recent",
            source_row_key="recent-row-2",
            played_at="2026-04-18T12:00:00Z",
            ms_played=100000,
            ms_played_method="history_source",
            raw_payload_json=json.dumps(payload, sort_keys=True),
            spotify_track_uri="spotify:track:track-2",
            spotify_track_id="track-2",
            track_name_raw="Track 2",
            artist_name_raw="Known Artist",
            album_name_raw="Album 2",
            spotify_album_id="album-2",
            spotify_artist_ids_json=json.dumps(["artist-known"]),
        )

        first = backfill_spotify_source_entities()
        second = backfill_spotify_source_entities()

        self.assertEqual(1, first["artists_created"])
        self.assertEqual(0, second["artists_created"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            artist_count = int(connection.execute("SELECT count(*) FROM artist").fetchone()[0])
            spotify_source_count = int(
                connection.execute(
                    "SELECT count(*) FROM source_artist WHERE source_name = 'spotify' AND external_id = 'artist-known'"
                ).fetchone()[0]
            )
        self.assertEqual(1, artist_count)
        self.assertEqual(1, spotify_source_count)

    def _counts(self) -> dict[str, int]:
        with closing(sqlite3.connect(self.db_path)) as connection:
            return {
                "artist": int(connection.execute("SELECT count(*) FROM artist").fetchone()[0]),
                "source_artist_map": int(connection.execute("SELECT count(*) FROM source_artist_map").fetchone()[0]),
                "album_artist": int(connection.execute("SELECT count(*) FROM album_artist").fetchone()[0]),
                "track_artist": int(connection.execute("SELECT count(*) FROM track_artist").fetchone()[0]),
            }
