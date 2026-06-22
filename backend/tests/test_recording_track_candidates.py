from __future__ import annotations

import json
import os
import sqlite3
import time
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.app.db import (
    apply_pending_migrations,
    ensure_sqlite_db,
    mark_generated_recording_track_clusters_dirty_with_connection,
    refresh_source_track_play_count_cache,
    sqlite_connection,
)
from backend.app.main import app
from backend.app.recording_track_candidates import (
    RecordingTrackCandidateMember,
    classify_recording_track_candidate_group,
    get_recording_track_candidate_for_release_track,
    get_recording_track_candidates_for_release_track,
    query_recording_track_candidates,
    rebuild_generated_recording_track_clusters,
    refresh_generated_recording_track_clusters_for_release_tracks,
    summarize_recording_track_candidates,
)
from backend.app.release_track_metadata import release_track_metadata_for_spotify_ids


def _member(
    release_track_id: int,
    title: str,
    *,
    artist: str = "Artist A",
    album: str = "Album A",
    album_release_dates: list[str] | None = None,
    source_track_ids: list[str] | None = None,
    source_track_db_ids: list[int] | None = None,
    source_track_uris: list[str] | None = None,
    isrc: str | None = None,
    duration_ms: int | None = 180_000,
) -> RecordingTrackCandidateMember:
    return {
        "release_track_id": release_track_id,
        "title": title,
        "artist": artist,
        "artists": [{"name": name.strip()} for name in artist.split("|") if name.strip()],
        "album": album,
        "release_album_ids": [release_track_id],
        "spotify_album_ids": [],
        "album_image_urls": [],
        "album_release_dates": album_release_dates or [],
        "album_types": [],
        "source_track_ids": source_track_ids or [f"spotify-{release_track_id}"],
        "source_track_db_ids": source_track_db_ids or [release_track_id],
        "source_track_uris": [f"spotify:track:spotify-{release_track_id}"] if source_track_uris is None else source_track_uris,
        "play_count": 0,
        "isrc": isrc,
        "isrc_values": [isrc] if isrc else [],
        "duration_ms": duration_ms,
        "duration_values_ms": [duration_ms] if duration_ms is not None else [],
        "evidence": {
            "normalized_title": title.lower(),
            "version_tokens": [],
            "album_context": "album",
            "duration_delta_ms": 0,
        },
    }


class RecordingTrackClassifierTests(unittest.TestCase):
    def test_remix_album_context_links_artist_parenthetical_to_original(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, "Codex", artist="Radiohead", album="The King Of Limbs"),
                _member(2, "Codex (Illum Sphere)", artist="Radiohead", album="TKOL RMX 1234567"),
            ]
        )

        self.assertEqual("track_family_candidate", item["candidate_type"])
        self.assertEqual("remix", item["relationship_kind"])

    def test_same_isrc_album_single_is_recording_safe_candidate(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, "Song A", album="Album A", isrc="USAAA1", duration_ms=181_000),
                _member(2, "Song A", album="Song A - Single", isrc="USAAA1", duration_ms=181_500),
            ]
        )

        self.assertEqual("recording_track_candidate", item["candidate_type"])
        self.assertEqual("safe_candidate", item["safety_status"])
        self.assertEqual("single_release", item["relationship_kind"])
        self.assertIn("same ISRC", item["why_grouped"])

    def test_same_isrc_compilation_and_soundtrack_are_recording_safe_candidates(self) -> None:
        compilation = classify_recording_track_candidate_group(
            [
                _member(1, "Song B", album="Album B", isrc="USBBB1"),
                _member(2, "Song B", album="Greatest Hits Compilation", isrc="USBBB1"),
            ]
        )
        soundtrack = classify_recording_track_candidate_group(
            [
                _member(3, "Song C", album="Album C", isrc="USCCC1"),
                _member(4, "Song C", album="Original Motion Picture Soundtrack", isrc="USCCC1"),
            ]
        )

        self.assertEqual("safe_candidate", compilation["safety_status"])
        self.assertEqual("compilation_appearance", compilation["relationship_kind"])
        self.assertEqual("safe_candidate", soundtrack["safety_status"])
        self.assertEqual("soundtrack_appearance", soundtrack["relationship_kind"])

    def test_remaster_with_compatible_duration_is_recording_candidate(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, "Song D", duration_ms=200_000),
                _member(2, "Song D - 2015 Remaster", duration_ms=201_000),
            ]
        )

        self.assertEqual("recording_track_candidate", item["candidate_type"])
        self.assertEqual("safe_candidate", item["safety_status"])
        self.assertEqual("remaster", item["relationship_kind"])
        self.assertEqual("missing_isrc_but_compatible_metadata", item["evidence_bucket"])

    def test_same_normalized_title_duration_mismatch_stays_reviewable_recording_candidate(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, "Song Drift", album="Song Drift", duration_ms=180_000),
                _member(2, "Song Drift", album="Song Drift - Single", duration_ms=205_000),
            ]
        )

        self.assertEqual("recording_track_candidate", item["candidate_type"])
        self.assertEqual("needs_review", item["safety_status"])
        self.assertEqual("single_release", item["relationship_kind"])
        self.assertEqual("missing_isrc_but_compatible_metadata", item["evidence_bucket"])
        self.assertIn("compatible title and artist metadata", item["why_grouped"])
        self.assertIn("duration delta is 25000ms", item["why_review"])

    def test_expanded_album_context_is_recording_level_rerelease_candidate(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, "Song Expanded", album="Original Album", duration_ms=180_000),
                _member(2, "Song Expanded", album="Original Album (Expanded Edition)", duration_ms=182_500),
            ]
        )

        self.assertEqual("recording_track_candidate", item["candidate_type"])
        self.assertEqual("needs_review", item["safety_status"])
        self.assertEqual("rerelease", item["relationship_kind"])
        self.assertEqual("missing_isrc_but_compatible_metadata", item["evidence_bucket"])

    def test_conflicting_isrc_remaster_with_compatible_metadata_needs_review_not_weak(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, "Song D", isrc="USDIF1", duration_ms=200_000),
                _member(2, "Song D - 2015 Remaster", isrc="USDIF2", duration_ms=201_000),
            ]
        )

        self.assertEqual("recording_track_candidate", item["candidate_type"])
        self.assertEqual("needs_review", item["safety_status"])
        self.assertEqual("remaster", item["relationship_kind"])
        self.assertEqual("conflicting_isrc_but_compatible_metadata", item["evidence_bucket"])
        self.assertGreaterEqual(item["confidence"], 0.8)
        self.assertIn("compatible title, artist, and duration metadata", item["why_grouped"])

    def test_partial_isrc_single_with_compatible_metadata_is_reviewable_candidate(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, "Song D", isrc="USPARTIAL1", duration_ms=200_000),
                _member(2, "Song D", album="Song D - Single", isrc=None, duration_ms=200_500),
            ]
        )

        self.assertEqual("recording_track_candidate", item["candidate_type"])
        self.assertEqual("needs_review", item["safety_status"])
        self.assertEqual("single_release", item["relationship_kind"])
        self.assertEqual("partial_isrc_match", item["evidence_bucket"])
        self.assertGreaterEqual(item["confidence"], 0.78)

    def test_live_demo_acoustic_remix_and_rerecording_are_family_candidates(self) -> None:
        cases = [
            ("Song E - Live", "live"),
            ("Song E - Demo", "demo"),
            ("Song E - Acoustic", "acoustic"),
            ("Song E - Instrumental", "instrumental"),
            ("Song E - Remix", "remix"),
            ("Song E - Rerecorded 2014 Version", "rerecording"),
        ]
        for title, relationship_kind in cases:
            with self.subTest(title=title):
                item = classify_recording_track_candidate_group(
                    [
                        _member(1, "Song E", isrc="USEEE1"),
                        _member(2, title, isrc="USEEE1"),
                    ]
                )

                self.assertEqual("track_family_candidate", item["candidate_type"])
                self.assertNotEqual("safe_candidate", item["safety_status"])
                self.assertEqual(relationship_kind, item["relationship_kind"])

    def test_same_live_recording_across_live_releases_can_remain_recording_candidate(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, "Song E - Live in Oxford", album="Live Album", isrc="USLIVE1", duration_ms=254_000),
                _member(2, "Song E - Live in Oxford", album="Live Album Deluxe", isrc="USLIVE1", duration_ms=254_000),
            ]
        )

        self.assertEqual("recording_track_candidate", item["candidate_type"])
        self.assertEqual("same_isrc", item["evidence_bucket"])

    def test_mixed_live_and_studio_versions_are_family_even_with_rerelease_context(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, "Song E", album="Original Album", isrc="USMIXEDLIVE1", duration_ms=254_000),
                _member(2, "Song E - Live in Oxford", album="Original Album Deluxe", isrc="USMIXEDLIVE1", duration_ms=254_000),
            ]
        )

        self.assertEqual("track_family_candidate", item["candidate_type"])
        self.assertEqual("live", item["relationship_kind"])
        self.assertIn("live/studio variants belong at Track Family layer", item["why_review"])

    def test_same_variant_label_with_same_isrc_stays_recording_candidate(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(
                    1,
                    "Deadcrush (feat. Danny Brown) - Alchemist x Trooko Version",
                    album="Deadcrush (feat. Danny Brown)",
                    isrc="GB5KW1801528",
                    duration_ms=243_573,
                ),
                _member(
                    2,
                    "Deadcrush (feat. Danny Brown) - Alchemist x Trooko Version",
                    album="Reduxer",
                    isrc="GB5KW1801528",
                    duration_ms=243_573,
                ),
            ]
        )

        self.assertEqual("recording_track_candidate", item["candidate_type"])
        self.assertEqual("safe_candidate", item["safety_status"])
        self.assertEqual("same_isrc", item["evidence_bucket"])

    def test_same_instrumental_variant_with_same_isrc_can_form_recording_candidate(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, "Speak Publicly - Instrumental", album="Single", isrc="GBKPL2033436"),
                _member(2, "Speak Publicly - Instrumental", album="Album", isrc="GBKPL2033436"),
            ]
        )

        self.assertEqual("recording_track_candidate", item["candidate_type"])
        self.assertEqual("same_isrc", item["evidence_bucket"])

    def test_mixed_named_versions_are_track_family_not_recording_candidate(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, "Deadcrush - Spike Stent Mix", album="Deadcrush", duration_ms=None),
                _member(
                    2,
                    "Deadcrush (feat. Danny Brown) - Alchemist x Trooko Version",
                    album="Deadcrush (feat. Danny Brown)",
                    isrc="GB5KW1801528",
                    duration_ms=243_573,
                ),
                _member(
                    3,
                    "Deadcrush (feat. Danny Brown) - Alchemist x Trooko Version",
                    album="Reduxer",
                    isrc="GB5KW1801528",
                    duration_ms=243_573,
                ),
            ]
        )

        self.assertEqual("track_family_candidate", item["candidate_type"])
        self.assertEqual("variant_flag_excluded", item["evidence_bucket"])
        self.assertIn("recording-distinct variant labels belong at Track Family layer", item["why_review"])

    def test_structural_parts_are_track_family_segments_not_recording_candidate(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, '"Instrumentals" Volume 1 - Part 1', duration_ms=None),
                _member(2, '"Instrumentals" Volume 1 - Part 2', duration_ms=None),
                _member(3, '"Instrumentals" Volume 1 - Part 3', duration_ms=None),
            ]
        )

        self.assertEqual("track_family_candidate", item["candidate_type"])
        self.assertEqual("structural_segment", item["relationship_kind"])
        self.assertEqual("variant_flag_excluded", item["evidence_bucket"])

    def test_content_rating_and_format_variants_can_remain_recording_candidates(self) -> None:
        explicit = classify_recording_track_candidate_group(
            [
                _member(1, "Changes - Explicit", duration_ms=268_960),
                _member(2, "Changes - Clean", duration_ms=268_960),
            ]
        )
        mono = classify_recording_track_candidate_group(
            [
                _member(3, "If You Could See Me Now", duration_ms=318_466),
                _member(4, "If You Could See Me Now - Mono", duration_ms=319_092),
            ]
        )

        self.assertEqual("recording_track_candidate", explicit["candidate_type"])
        self.assertEqual("recording_track_candidate", mono["candidate_type"])
        self.assertNotEqual("variant_flag_excluded", explicit["evidence_bucket"])
        self.assertNotEqual("variant_flag_excluded", mono["evidence_bucket"])

    def test_representative_prefers_original_album_over_compilation(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(
                    1,
                    "Lovely Day",
                    album="The Essential Artist A",
                    album_release_dates=["2013-01-01"],
                    isrc="USAAA2",
                ),
                _member(
                    2,
                    "Lovely Day",
                    album="Original Album",
                    album_release_dates=["1977-01-01"],
                    isrc="USAAA1",
                ),
                _member(
                    3,
                    "Lovely Day",
                    album="Best Of Artist A",
                    album_release_dates=["2004-01-01"],
                    isrc="USAAA3",
                ),
            ]
        )

        self.assertEqual(2, item["representative"]["release_track_id"])
        self.assertIn("preferred original album context", item["representative"]["reason"])
        self.assertIn("earliest release year 1977", item["representative"]["reason"])

    def test_representative_treats_title_album_as_single_context(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, "Hunter", album="Hunter", isrc=None, duration_ms=None),
                _member(2, "Hunter", album="The Greatest Part", isrc=None, duration_ms=None),
            ]
        )

        self.assertEqual(2, item["representative"]["release_track_id"])
        self.assertIn("preferred original album context", item["representative"]["reason"])

    def test_representative_prefers_clean_title_over_format_variant(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, "If You Could See Me Now - Mono", album="Chet", album_release_dates=["1959"], duration_ms=319_092),
                _member(2, "If You Could See Me Now", album="Chet", album_release_dates=["2007-01-01"], duration_ms=318_466),
            ]
        )

        self.assertEqual(2, item["representative"]["release_track_id"])
        self.assertIn("clean base title preferred for display", item["representative"]["reason"])

    def test_representative_prefers_clean_original_single_over_later_derived_album_version(self) -> None:
        original = _member(
            1,
            "Fog",
            album="Knives Out",
            album_release_dates=["2001-08-06"],
            isrc="GBORIGINAL1",
            duration_ms=244_133,
        )
        original["album_types"] = ["single"]
        later_version = _member(
            2,
            "Fog - Again Again Version",
            album="KID A MNESIA",
            album_release_dates=["2021-11-05"],
            isrc=None,
            duration_ms=145_443,
        )
        later_version["album_types"] = ["album"]

        item = classify_recording_track_candidate_group([original, later_version])

        self.assertEqual("track_family_candidate", item["candidate_type"])
        self.assertEqual(1, item["representative"]["release_track_id"])
        self.assertIn("clean base title preferred for display", item["representative"]["reason"])
        self.assertIn("earliest release year 2001", item["representative"]["reason"])

    def test_representative_uses_source_backed_rerelease_before_compilation(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(
                    1,
                    "Song R - Remastered 2009",
                    album="Song R 2009 Remaster",
                    album_release_dates=["2009-01-01"],
                    isrc="USRERELEASE1",
                ),
                _member(
                    2,
                    "Song R",
                    album="Best Of Artist A",
                    album_release_dates=["2019-01-01"],
                    isrc="USRERELEASE2",
                ),
            ]
        )

        self.assertEqual(1, item["representative"]["release_track_id"])
        self.assertIn("rerelease/remaster preferred over compilation fallback", item["representative"]["reason"])

    def test_radio_edit_is_review_required_not_recording_safe(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, "Song F", duration_ms=240_000),
                _member(2, "Song F - Radio Edit", duration_ms=190_000),
            ]
        )

        self.assertEqual("track_family_candidate", item["candidate_type"])
        self.assertEqual("needs_review", item["safety_status"])
        self.assertEqual("radio_edit", item["relationship_kind"])
        self.assertEqual("variant_flag_excluded", item["evidence_bucket"])
        self.assertIn("radio/edit variant should not silently collapse into recording_track", item["why_review"])

    def test_title_artist_only_missing_isrc_and_duration_needs_review(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, "Song G", isrc=None, duration_ms=None),
                _member(2, "Song G", isrc=None, duration_ms=None),
            ]
        )

        self.assertEqual("recording_track_candidate", item["candidate_type"])
        self.assertEqual("needs_review", item["safety_status"])
        self.assertIn("missing ISRC support", item["why_review"])
        self.assertIn("missing duration comparison", item["why_review"])

    def test_same_title_across_many_albums_without_strong_evidence_needs_review(self) -> None:
        item = classify_recording_track_candidate_group(
            [
                _member(1, "Song H", album="Album One", isrc=None, duration_ms=None),
                _member(2, "Song H", album="Album Two", isrc=None, duration_ms=None),
                _member(3, "Song H", album="Album Three", isrc=None, duration_ms=None),
            ]
        )

        self.assertEqual("needs_review", item["safety_status"])
        self.assertIn("same title appears across many albums without strong ISRC or duration support", item["why_review"])


class RecordingTrackCandidateEndpointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_recording_track_candidates.sqlite3",
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

    def _seed_release_track(
        self,
        *,
        title: str,
        artist: str,
        album: str,
        spotify_id: str,
        isrc: str | None,
        duration_ms: int | None,
        catalog_isrc: str | None = None,
        catalog_duration_ms: int | None = None,
        catalog_album_id: str | None = None,
    ) -> int:
        with sqlite_connection(write=True) as connection:
            artist_row = connection.execute(
                "SELECT id FROM artist WHERE canonical_name = ?",
                (artist,),
            ).fetchone()
            if artist_row is None:
                artist_id = int(
                    connection.execute(
                        "INSERT INTO artist (canonical_name, sort_name) VALUES (?, ?)",
                        (artist, artist.lower()),
                    ).lastrowid
                )
            else:
                artist_id = int(artist_row[0])

            release_track_id = int(
                connection.execute(
                    "INSERT INTO release_track (primary_name, normalized_name, duration_ms) VALUES (?, ?, ?)",
                    (title, title.lower(), duration_ms),
                ).lastrowid
            )
            release_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    (album, album.lower()),
                ).lastrowid
            )
            source_track_id = int(
                connection.execute(
                    """
                    INSERT INTO source_track (
                      source_name, external_id, external_uri, isrc, source_name_raw
                    ) VALUES ('spotify', ?, ?, ?, ?)
                    """,
                    (spotify_id, f"spotify:track:{spotify_id}", isrc, title),
                ).lastrowid
            )
            if catalog_isrc is not None or catalog_duration_ms is not None or catalog_album_id is not None:
                connection.execute(
                    """
                    INSERT INTO spotify_track_catalog (
                      spotify_track_id, name, duration_ms, album_id, raw_json, fetched_at, last_status
                    ) VALUES (?, ?, ?, ?, ?, '2026-05-25T00:00:00Z', 'ok')
                    """,
                    (
                        spotify_id,
                        title,
                        catalog_duration_ms,
                        catalog_album_id,
                        json.dumps({"external_ids": {"isrc": catalog_isrc}} if catalog_isrc else {}),
                    ),
                )
                if catalog_album_id is not None:
                    connection.execute(
                        """
                        INSERT OR REPLACE INTO spotify_album_catalog (
                          spotify_album_id, name, album_type, release_date, fetched_at, last_status
                        ) VALUES (?, ?, 'album', '2020-01-01', '2026-05-25T00:00:00Z', 'ok')
                        """,
                        (catalog_album_id, album),
                    )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 0)",
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
                ) VALUES (?, ?, 'seed', 1.0, 'accepted', 1, 'test')
                """,
                (source_track_id, release_track_id),
            )
            mark_generated_recording_track_clusters_dirty_with_connection(
                connection,
                [release_track_id],
                reason="test_seed",
            )
            return release_track_id

    def test_query_returns_evidence_and_representative_without_mutating_identity(self) -> None:
        album_release_track_id = self._seed_release_track(
            title="Endpoint Song",
            artist="Endpoint Artist",
            album="Endpoint Album",
            spotify_id="endpoint-album",
            isrc="USENDPOINT1",
            duration_ms=210_000,
        )
        self._seed_release_track(
            title="Endpoint Song",
            artist="Endpoint Artist",
            album="Endpoint Song - Single",
            spotify_id="endpoint-single",
            isrc="USENDPOINT1",
            duration_ms=210_500,
        )

        payload = query_recording_track_candidates(limit=10, offset=0)

        self.assertEqual(1, payload["total"])
        item = payload["items"][0]
        self.assertEqual("recording_track_candidate", item["candidate_type"])
        self.assertEqual("safe_candidate", item["safety_status"])
        self.assertEqual("single_release", item["relationship_kind"])
        self.assertEqual(2, len(item["members"]))
        self.assertIn("representative", item)
        self.assertIn("reason", item["representative"])
        self.assertIn("why_grouped", item)
        self.assertIn("why_review", item)
        self.assertEqual("Endpoint Artist", item["members"][0]["artists"][0]["name"])
        self.assertEqual("primary", item["members"][0]["artists"][0]["role"])
        self.assertEqual(0, item["members"][0]["artists"][0]["billing_index"])
        self.assertEqual("USENDPOINT1", item["members"][0]["isrc"])
        self.assertIn("duration_delta_ms", item["members"][0]["evidence"])

        with sqlite_connection() as connection:
            analysis_track_count = int(connection.execute("SELECT count(*) FROM analysis_track").fetchone()[0])
            analysis_track_map_count = int(connection.execute("SELECT count(*) FROM analysis_track_map").fetchone()[0])
        self.assertEqual(0, analysis_track_count)
        self.assertEqual(0, analysis_track_map_count)

        lookup_item = get_recording_track_candidate_for_release_track(album_release_track_id)
        self.assertIsNotNone(lookup_item)
        self.assertEqual(item["candidate_key"], lookup_item["candidate_key"])

    def test_shared_primary_artist_connects_attributed_version_to_original_family(self) -> None:
        original_id = self._seed_release_track(
            title="Connected Song",
            artist="Shared Artist",
            album="Original Album",
            spotify_id="connected-original",
            isrc=None,
            duration_ms=290_000,
        )
        version_id = self._seed_release_track(
            title="Connected Song - Guest Version",
            artist="Shared Artist",
            album="Guest Version Single",
            spotify_id="connected-version",
            isrc=None,
            duration_ms=230_000,
        )
        with sqlite_connection(write=True) as connection:
            guest_artist_id = int(
                connection.execute(
                    "INSERT INTO artist (canonical_name, sort_name) VALUES ('Guest Artist', 'guest artist')"
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO track_artist (release_track_id, artist_id, role, billing_index) VALUES (?, ?, 'primary', 1)",
                (version_id, guest_artist_id),
            )

        rebuild_generated_recording_track_clusters()
        candidates = get_recording_track_candidates_for_release_track(original_id)
        family = next(item for item in candidates if item["candidate_type"] == "track_family_candidate")

        self.assertEqual("derived_version", family["relationship_kind"])
        self.assertEqual({original_id, version_id}, {member["release_track_id"] for member in family["members"]})

    def test_scoped_refresh_spans_duplicate_exact_name_artist_identities(self) -> None:
        first_original_id = self._seed_release_track(
            title="Scoped Identity Song",
            artist="Scoped Identity Artist",
            album="Scoped Identity Album",
            spotify_id="scoped-identity-original-one",
            isrc=None,
            duration_ms=240_000,
        )
        second_original_id = self._seed_release_track(
            title="Scoped Identity Song",
            artist="Scoped Identity Artist",
            album="Scoped Identity Album",
            spotify_id="scoped-identity-original-two",
            isrc=None,
            duration_ms=240_000,
        )
        remaster_id = self._seed_release_track(
            title="Scoped Identity Song - Remastered",
            artist="Scoped Identity Artist",
            album="Scoped Identity Album Expanded",
            spotify_id="scoped-identity-remaster",
            isrc=None,
            duration_ms=243_000,
        )
        with sqlite_connection(write=True) as connection:
            duplicate_artist_id = int(connection.execute(
                "INSERT INTO artist (canonical_name, sort_name) VALUES ('Scoped Identity Artist', 'scoped identity artist')"
            ).lastrowid)
            connection.execute(
                "UPDATE track_artist SET artist_id = ? WHERE release_track_id = ? AND role = 'primary'",
                (duplicate_artist_id, first_original_id),
            )

        result = refresh_generated_recording_track_clusters_for_release_tracks([remaster_id])
        candidates = get_recording_track_candidates_for_release_track(remaster_id)
        recording = next(item for item in candidates if item["candidate_type"] == "recording_track_candidate")

        self.assertTrue(result["refreshed"])
        self.assertEqual(
            {first_original_id, second_original_id, remaster_id},
            {member["release_track_id"] for member in recording["members"]},
        )

    def test_album_editions_keep_core_and_live_recordings_separate_beneath_family(self) -> None:
        studio_ids = {
            self._seed_release_track(
                title="Edition Split Song",
                artist="Edition Artist",
                album="Edition Album",
                spotify_id="edition-studio-original",
                isrc=None,
                duration_ms=240_000,
            ),
            self._seed_release_track(
                title="Edition Split Song",
                artist="Edition Artist",
                album="Edition Album (Deluxe Edition)",
                spotify_id="edition-studio-deluxe",
                isrc=None,
                duration_ms=240_000,
            ),
            self._seed_release_track(
                title="Edition Split Song",
                artist="Edition Artist",
                album="Edition Album (Expanded Deluxe Edition)",
                spotify_id="edition-studio-expanded",
                isrc=None,
                duration_ms=240_000,
            ),
        }
        live_ids = {
            self._seed_release_track(
                title="Edition Split Song - Live In Studio",
                artist="Edition Artist",
                album="Edition Album (Deluxe Edition)",
                spotify_id="edition-live-deluxe",
                isrc=None,
                duration_ms=260_000,
            ),
            self._seed_release_track(
                title="Edition Split Song - Live In Studio",
                artist="Edition Artist",
                album="Edition Album (Expanded Deluxe Edition)",
                spotify_id="edition-live-expanded",
                isrc=None,
                duration_ms=260_000,
            ),
        }

        payload = query_recording_track_candidates(q="Edition Split Song", limit=20)
        recording_member_sets = {
            frozenset(member["release_track_id"] for member in item["members"])
            for item in payload["items"]
            if item["candidate_type"] == "recording_track_candidate"
        }
        family = next(item for item in payload["items"] if item["candidate_type"] == "track_family_candidate")

        self.assertIn(frozenset(studio_ids), recording_member_sets)
        self.assertIn(frozenset(live_ids), recording_member_sets)
        self.assertEqual(studio_ids | live_ids, {member["release_track_id"] for member in family["members"]})

    def test_unique_cached_album_name_hydrates_missing_release_album_art(self) -> None:
        first_id = self._seed_release_track(
            title="Artwork Song",
            artist="Artwork Artist",
            album="Artwork Album",
            spotify_id="artwork-one",
            isrc=None,
            duration_ms=210_000,
        )
        self._seed_release_track(
            title="Artwork Song",
            artist="Artwork Artist",
            album="Artwork Single",
            spotify_id="artwork-two",
            isrc=None,
            duration_ms=210_000,
        )
        with sqlite_connection(write=True) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, album_type, release_date, artists_json, images_json, fetched_at, last_status
                ) VALUES (?, ?, 'album', '2024-01-02', ?, ?, '2026-06-17T00:00:00Z', 'ok')
                """,
                (
                    "artwork-album-id",
                    "Artwork Album",
                    json.dumps([{"name": "Artwork Artist"}]),
                    json.dumps([{"url": "https://images.example/artwork.jpg"}]),
                ),
            )

        rebuild_generated_recording_track_clusters()
        candidate = get_recording_track_candidate_for_release_track(first_id)
        first_member = next(member for member in candidate["members"] if member["release_track_id"] == first_id)

        self.assertEqual(["artwork-album-id"], first_member["spotify_album_ids"])
        self.assertEqual(["https://images.example/artwork.jpg"], first_member["album_image_urls"])

    def test_shared_release_album_payload_hydrates_art_for_every_family_member(self) -> None:
        first_id = self._seed_release_track(
            title="Shared Artwork Song - Remix One",
            artist="Shared Artwork Artist",
            album="Shared Artwork Remix Album",
            spotify_id="shared-artwork-one",
            isrc=None,
            duration_ms=210_000,
        )
        second_id = self._seed_release_track(
            title="Shared Artwork Song (Remix Two)",
            artist="Shared Artwork Artist",
            album="Temporary Duplicate Album",
            spotify_id="shared-artwork-two",
            isrc=None,
            duration_ms=215_000,
        )
        album_payload = {
            "track": {
                "album": {
                    "id": "shared-artwork-album-id",
                    "album_type": "album",
                    "release_date": "2024-02-03",
                    "images": [{"url": "https://images.example/shared-artwork.jpg"}],
                }
            }
        }
        with sqlite_connection(write=True) as connection:
            shared_album_id = int(connection.execute(
                "SELECT release_album_id FROM album_track WHERE release_track_id = ?",
                (first_id,),
            ).fetchone()[0])
            duplicate_album_id = int(connection.execute(
                "SELECT release_album_id FROM album_track WHERE release_track_id = ?",
                (second_id,),
            ).fetchone()[0])
            connection.execute("DELETE FROM album_track WHERE release_track_id = ?", (second_id,))
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (shared_album_id, second_id),
            )
            connection.execute("DELETE FROM release_album WHERE id = ?", (duplicate_album_id,))
            source_album_id = int(connection.execute(
                """
                INSERT INTO source_album (
                  source_name, external_id, external_uri, source_name_raw, raw_payload_json
                ) VALUES ('spotify', ?, ?, ?, ?)
                """,
                (
                    "shared-artwork-album-id",
                    "spotify:album:shared-artwork-album-id",
                    "Shared Artwork Remix Album",
                    json.dumps(album_payload),
                ),
            ).lastrowid)
            connection.execute(
                """
                INSERT INTO source_album_map (
                  source_album_id, release_album_id, match_method, confidence, status, explanation
                ) VALUES (?, ?, 'seed', 1.0, 'accepted', 'test')
                """,
                (source_album_id, shared_album_id),
            )

        payload = query_recording_track_candidates(q="Shared Artwork Song", limit=10)
        family = next(item for item in payload["items"] if item["candidate_type"] == "track_family_candidate")

        self.assertEqual({first_id, second_id}, {member["release_track_id"] for member in family["members"]})
        for member in family["members"]:
            self.assertEqual(["https://images.example/shared-artwork.jpg"], member["album_image_urls"])
            self.assertEqual(["2024-02-03"], member["album_release_dates"])
            self.assertEqual(["album"], member["album_types"])
            self.assertEqual("https://images.example/shared-artwork.jpg", member["album_versions"][0]["image_url"])

    def test_unique_cached_album_name_does_not_cross_primary_artists(self) -> None:
        first_id = self._seed_release_track(
            title="Strangers",
            artist="Black Pumas",
            album="Strangers",
            spotify_id="black-pumas-strangers",
            isrc="USBLACKPUMAS1",
            duration_ms=192_963,
        )
        self._seed_release_track(
            title='Strangers - From "Life In A Day"',
            artist="Black Pumas",
            album="Black Pumas (Expanded Deluxe Edition)",
            spotify_id="black-pumas-strangers-expanded",
            isrc="USBLACKPUMAS1",
            duration_ms=192_963,
        )
        with sqlite_connection(write=True) as connection:
            connection.execute(
                """
                INSERT INTO spotify_album_catalog (
                  spotify_album_id, name, album_type, release_date, artists_json,
                  images_json, fetched_at, last_status
                ) VALUES (?, 'Strangers', 'single', '2020-12-09', ?, ?, '2026-06-19T00:00:00Z', 'ok')
                """,
                (
                    "roosevelt-strangers-album",
                    json.dumps([{"name": "Roosevelt"}]),
                    json.dumps([{"url": "https://images.example/roosevelt.jpg"}]),
                ),
            )

        rebuild_generated_recording_track_clusters()
        candidate = get_recording_track_candidate_for_release_track(first_id)
        first_member = next(member for member in candidate["members"] if member["release_track_id"] == first_id)

        self.assertNotIn("roosevelt-strangers-album", first_member["spotify_album_ids"])
        self.assertNotIn("https://images.example/roosevelt.jpg", first_member["album_image_urls"])

    def test_query_sums_member_play_counts_from_source_tracks(self) -> None:
        first_release_track_id = self._seed_release_track(
            title="Counted Song",
            artist="Counted Artist",
            album="Counted Album",
            spotify_id="counted-album",
            isrc="USCOUNTED1",
            duration_ms=180_000,
        )
        second_release_track_id = self._seed_release_track(
            title="Counted Song",
            artist="Counted Artist",
            album="Counted Song - Single",
            spotify_id="counted-single",
            isrc="USCOUNTED1",
            duration_ms=180_500,
        )
        with sqlite_connection(write=True) as connection:
            for index in range(20):
                connection.execute(
                    """
                    INSERT INTO fact_play_event (
                      canonical_ended_at, spotify_track_id, timing_source, matched_state
                    ) VALUES (?, 'counted-album', 'test', 'matched')
                    """,
                    (f"2026-01-01T00:{index:02d}:00Z",),
                )
            for index in range(9):
                connection.execute(
                    """
                    INSERT INTO fact_play_event (
                      canonical_ended_at, spotify_track_id, timing_source, matched_state
                    ) VALUES (?, 'counted-single', 'test', 'matched')
                    """,
                    (f"2026-01-02T00:{index:02d}:00Z",),
                )
        refresh_source_track_play_count_cache()

        item = get_recording_track_candidate_for_release_track(first_release_track_id)

        self.assertIsNotNone(item)
        counts_by_release_track_id = {
            member["release_track_id"]: member["play_count"]
            for member in item["members"]
        }
        self.assertEqual(20, counts_by_release_track_id[first_release_track_id])
        self.assertEqual(9, counts_by_release_track_id[second_release_track_id])
        self.assertEqual(29, sum(counts_by_release_track_id.values()))
        dates_by_release_track_id = {
            member["release_track_id"]: (member["first_played_at"], member["last_played_at"])
            for member in item["members"]
        }
        self.assertEqual(("2026-01-01T00:00:00Z", "2026-01-01T00:19:00Z"), dates_by_release_track_id[first_release_track_id])
        self.assertEqual(("2026-01-02T00:00:00Z", "2026-01-02T00:08:00Z"), dates_by_release_track_id[second_release_track_id])
        self.assertEqual(
            {
                "recording_total_play_count": 29,
                "recording_member_count": 2,
            },
            item["listen_counts"],
        )

    def test_release_metadata_marks_recording_candidate_members_with_rt_badge_flag(self) -> None:
        self._seed_release_track(
            title="Metadata Cluster Song",
            artist="Metadata Artist",
            album="Metadata Album",
            spotify_id="metadata-cluster-album",
            isrc="USMETA1",
            duration_ms=210_000,
        )
        self._seed_release_track(
            title="Metadata Cluster Song",
            artist="Metadata Artist",
            album="Metadata Cluster Song - Single",
            spotify_id="metadata-cluster-single",
            isrc="USMETA1",
            duration_ms=210_500,
        )

        metadata = release_track_metadata_for_spotify_ids(["metadata-cluster-album"])

        self.assertTrue(metadata["metadata-cluster-album"]["has_release_track_siblings"])
        self.assertEqual(2, metadata["metadata-cluster-album"]["release_track_source_count"])

    def test_catalog_isrc_and_duration_are_surfaced_when_source_track_isrc_is_missing(self) -> None:
        self._seed_release_track(
            title="Catalog Song",
            artist="Catalog Artist",
            album="Catalog Album",
            spotify_id="catalog-album",
            isrc=None,
            duration_ms=None,
            catalog_isrc="USCATALOG1",
            catalog_duration_ms=199_000,
            catalog_album_id="catalog-album-id",
        )
        self._seed_release_track(
            title="Catalog Song",
            artist="Catalog Artist",
            album="Catalog Song - Single",
            spotify_id="catalog-single",
            isrc=None,
            duration_ms=None,
            catalog_isrc="USCATALOG1",
            catalog_duration_ms=199_500,
            catalog_album_id="catalog-single-id",
        )

        payload = query_recording_track_candidates(same_isrc_only=True, limit=10)

        self.assertEqual(1, payload["total"])
        item = payload["items"][0]
        self.assertEqual("safe_candidate", item["safety_status"])
        self.assertEqual("USCATALOG1", item["members"][0]["isrc"])
        self.assertEqual(["USCATALOG1"], item["members"][0]["isrc_values"])
        self.assertIn(199_000, item["members"][0]["duration_values_ms"])
        self.assertIn("catalog-album-id", item["members"][0]["spotify_album_ids"])

    def test_raw_payload_album_fields_are_surfaced_and_album_names_are_deduped(self) -> None:
        first_release_track_id = self._seed_release_track(
            title="Raw Candidate Song",
            artist="Raw Candidate Artist",
            album="Raw Candidate Song",
            spotify_id="raw-candidate-single",
            isrc=None,
            duration_ms=170_107,
        )
        self._seed_release_track(
            title="Raw Candidate Song",
            artist="Raw Candidate Artist",
            album="Raw Candidate Album",
            spotify_id="raw-candidate-album",
            isrc=None,
            duration_ms=170_107,
        )
        raw_payload = {
            "track": {
                "duration_ms": 170107,
                "album": {
                    "id": "raw-candidate-album-id",
                    "album_type": "single",
                    "release_date": "2026-05-01",
                    "images": [{"url": "https://images.example/raw-candidate.jpg"}],
                },
            }
        }
        with sqlite_connection(write=True) as connection:
            duplicate_album_id = int(
                connection.execute(
                    "INSERT INTO release_album (primary_name, normalized_name) VALUES (?, ?)",
                    ("Raw Candidate Song", "raw candidate song"),
                ).lastrowid
            )
            connection.execute(
                "INSERT INTO album_track (release_album_id, release_track_id) VALUES (?, ?)",
                (duplicate_album_id, first_release_track_id),
            )
            connection.execute(
                "UPDATE source_track SET raw_payload_json = ? WHERE external_id = ?",
                (json.dumps(raw_payload), "raw-candidate-single"),
            )

        payload = query_recording_track_candidates(limit=10, offset=0)

        self.assertEqual(1, payload["total"])
        first_member = next(
            member for member in payload["items"][0]["members"]
            if member["release_track_id"] == first_release_track_id
        )
        self.assertEqual("Raw Candidate Song", first_member["album"])
        self.assertEqual(["raw-candidate-album-id"], first_member["spotify_album_ids"])
        self.assertEqual(["https://images.example/raw-candidate.jpg"], first_member["album_image_urls"])
        self.assertEqual(["2026-05-01"], first_member["album_release_dates"])
        self.assertEqual(["single"], first_member["album_types"])
        self.assertIn(170107, first_member["duration_values_ms"])

    def test_spotify_uri_source_rows_use_catalog_album_metadata(self) -> None:
        first_release_track_id = self._seed_release_track(
            title="URI Candidate Song",
            artist="URI Candidate Artist",
            album="URI Candidate Album",
            spotify_id="uri-candidate-album-track",
            isrc="USURICANDIDATE1",
            duration_ms=None,
            catalog_duration_ms=180_000,
            catalog_album_id="uri-candidate-album-id",
        )
        self._seed_release_track(
            title="URI Candidate Song",
            artist="URI Candidate Artist",
            album="URI Candidate Single",
            spotify_id="uri-candidate-single-track",
            isrc="USURICANDIDATE1",
            duration_ms=None,
            catalog_duration_ms=180_000,
            catalog_album_id="uri-candidate-single-id",
        )
        with sqlite_connection(write=True) as connection:
            connection.execute(
                """
                UPDATE source_track
                SET source_name = 'spotify_uri',
                    external_id = ?,
                    external_uri = NULL
                WHERE external_id = ?
                """,
                ("spotify:track:uri-candidate-album-track", "uri-candidate-album-track"),
            )
            connection.execute(
                """
                UPDATE spotify_album_catalog
                SET images_json = ?, release_date = ?
                WHERE spotify_album_id = ?
                """,
                (
                    json.dumps([{"url": "https://images.example/uri-candidate-album.jpg"}]),
                    "2026-06-01",
                    "uri-candidate-album-id",
                ),
            )

        payload = query_recording_track_candidates(same_isrc_only=True, limit=10)

        self.assertEqual(1, payload["total"])
        first_member = next(
            member for member in payload["items"][0]["members"]
            if member["release_track_id"] == first_release_track_id
        )
        self.assertIn("uri-candidate-album-track", first_member["source_track_ids"])
        self.assertIn("spotify:track:uri-candidate-album-track", first_member["source_track_uris"])
        self.assertEqual(["https://images.example/uri-candidate-album.jpg"], first_member["album_image_urls"])
        self.assertEqual(["2026-06-01"], first_member["album_release_dates"])

    def test_generated_candidate_lookup_hydrates_stale_member_album_metadata(self) -> None:
        first_release_track_id = self._seed_release_track(
            title="Hydrated Candidate Song",
            artist="Hydrated Candidate Artist",
            album="Hydrated Candidate Album",
            spotify_id="hydrated-candidate-album-track",
            isrc="USHYDRATED1",
            duration_ms=None,
            catalog_duration_ms=180_000,
            catalog_album_id="hydrated-candidate-album-id",
        )
        self._seed_release_track(
            title="Hydrated Candidate Song",
            artist="Hydrated Candidate Artist",
            album="Hydrated Candidate Single",
            spotify_id="hydrated-candidate-single-track",
            isrc="USHYDRATED1",
            duration_ms=None,
            catalog_duration_ms=180_000,
            catalog_album_id="hydrated-candidate-single-id",
        )
        with sqlite_connection(write=True) as connection:
            connection.execute(
                """
                UPDATE spotify_album_catalog
                SET images_json = ?, release_date = ?
                WHERE spotify_album_id = ?
                """,
                (
                    json.dumps([{"url": "https://images.example/hydrated-candidate-album.jpg"}]),
                    "2026-06-02",
                    "hydrated-candidate-album-id",
                ),
            )

        rebuild_generated_recording_track_clusters()
        with sqlite_connection(write=True, row_factory=sqlite3.Row) as connection:
            rows = connection.execute("SELECT id, candidate_snapshot_json FROM generated_recording_track_cluster").fetchall()
            for row in rows:
                snapshot = json.loads(str(row["candidate_snapshot_json"] or "{}"))
                for member in snapshot.get("members", []):
                    if isinstance(member, dict):
                        member["album_image_urls"] = []
                        member["album_release_dates"] = []
                connection.execute(
                    "UPDATE generated_recording_track_cluster SET candidate_snapshot_json = ? WHERE id = ?",
                    (json.dumps(snapshot), int(row["id"])),
                )

        item = get_recording_track_candidate_for_release_track(first_release_track_id)

        self.assertIsNotNone(item)
        assert item is not None
        first_member = next(
            member for member in item["members"]
            if member["release_track_id"] == first_release_track_id
        )
        self.assertEqual(["https://images.example/hydrated-candidate-album.jpg"], first_member["album_image_urls"])
        self.assertEqual(["2026-06-02"], first_member["album_release_dates"])

    def test_clean_track_without_generated_candidate_does_not_scan_all_tracks(self) -> None:
        release_track_id = self._seed_release_track(
            title="Standalone Song",
            artist="Standalone Artist",
            album="Standalone Album",
            spotify_id="standalone-track",
            isrc=None,
            duration_ms=180_000,
        )
        with sqlite_connection(write=True) as connection:
            connection.execute(
                "DELETE FROM generated_recording_track_cluster_dirty WHERE release_track_id = ?",
                (release_track_id,),
            )

        with patch("backend.app.recording_track_candidates._candidate_items") as candidate_items:
            candidates = get_recording_track_candidates_for_release_track(release_track_id)

        self.assertEqual([], candidates)
        candidate_items.assert_not_called()

    def test_same_isrc_with_conflicting_title_downgrades_to_needs_review(self) -> None:
        self._seed_release_track(
            title="First Title",
            artist="Conflict Artist",
            album="Conflict Album",
            spotify_id="conflict-one",
            isrc=None,
            duration_ms=180_000,
            catalog_isrc="USCONFLICT1",
            catalog_duration_ms=180_000,
        )
        self._seed_release_track(
            title="Different Title",
            artist="Conflict Artist",
            album="Conflict Single",
            spotify_id="conflict-two",
            isrc=None,
            duration_ms=180_000,
            catalog_isrc="USCONFLICT1",
            catalog_duration_ms=180_000,
        )

        payload = query_recording_track_candidates(same_isrc_only=True, limit=10)

        self.assertEqual(1, payload["total"])
        item = payload["items"][0]
        self.assertEqual("recording_track_candidate", item["candidate_type"])
        self.assertEqual("needs_review", item["safety_status"])
        self.assertIn("normalized base titles differ", item["why_review"])

    def test_endpoint_route_returns_read_only_payload(self) -> None:
        self._seed_release_track(
            title="Route Song",
            artist="Route Artist",
            album="Route Album",
            spotify_id="route-album",
            isrc="USROUTE1",
            duration_ms=190_000,
        )
        self._seed_release_track(
            title="Route Song",
            artist="Route Artist",
            album="Route Soundtrack",
            spotify_id="route-soundtrack",
            isrc="USROUTE1",
            duration_ms=190_000,
        )

        with patch("backend.app.routes.audit_routes._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.get("/debug/tracks/recording-track-candidates?limit=50&offset=0")

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertFalse(body["source"]["uses_spotify_api"])
        self.assertFalse(body["source"]["mutates_identity"])
        self.assertEqual(1, body["returned"])
        self.assertEqual("recording_track_candidate", body["items"][0]["candidate_type"])

    def test_endpoint_route_applies_filters(self) -> None:
        self._seed_release_track(
            title="Route Filter",
            artist="Route Filter Artist",
            album="Route Filter Album",
            spotify_id="route-filter-album",
            isrc="USROUTEFILTER1",
            duration_ms=200_000,
        )
        self._seed_release_track(
            title="Route Filter",
            artist="Route Filter Artist",
            album="Route Filter Single",
            spotify_id="route-filter-single",
            isrc="USROUTEFILTER1",
            duration_ms=200_000,
        )
        self._seed_release_track(
            title="Route Variant",
            artist="Route Filter Artist",
            album="Route Variant Album",
            spotify_id="route-variant-album",
            isrc="USROUTEVARIANT1",
            duration_ms=200_000,
        )
        self._seed_release_track(
            title="Route Variant - Live",
            artist="Route Filter Artist",
            album="Route Variant Live",
            spotify_id="route-variant-live",
            isrc="USROUTEVARIANT1",
            duration_ms=200_000,
        )

        with patch("backend.app.routes.audit_routes._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.get(
                "/debug/tracks/recording-track-candidates"
                "?candidate_type=recording_track_candidate"
                "&safety_status=safe_candidate"
                "&include_track_family_candidates=false"
                "&q=Route%20Filter"
            )

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual(1, body["total"])
        self.assertEqual("Route Filter", body["items"][0]["display_name"])
        self.assertEqual("recording_track_candidate", body["items"][0]["candidate_type"])
        self.assertEqual("safe_candidate", body["items"][0]["safety_status"])
        self.assertFalse(body["filters"]["include_track_family_candidates"])

    def test_query_filters_candidate_items(self) -> None:
        self._seed_release_track(
            title="Filter Song",
            artist="Filter Artist",
            album="Filter Album",
            spotify_id="filter-album",
            isrc="USFILTER1",
            duration_ms=200_000,
        )
        self._seed_release_track(
            title="Filter Song",
            artist="Filter Artist",
            album="Filter Song - Single",
            spotify_id="filter-single",
            isrc="USFILTER1",
            duration_ms=200_500,
        )
        self._seed_release_track(
            title="Variant Song",
            artist="Filter Artist",
            album="Variant Album",
            spotify_id="variant-album",
            isrc="USVARIANT1",
            duration_ms=200_000,
        )
        self._seed_release_track(
            title="Variant Song - Live",
            artist="Filter Artist",
            album="Variant Live",
            spotify_id="variant-live",
            isrc="USVARIANT1",
            duration_ms=202_000,
        )

        safe_payload = query_recording_track_candidates(safety_status="safe_candidate", q="Filter", limit=10)
        no_family_payload = query_recording_track_candidates(include_track_family_candidates=False, limit=10)
        family_payload = query_recording_track_candidates(candidate_type="track_family_candidate", limit=10)

        self.assertEqual(1, safe_payload["total"])
        self.assertEqual("Filter Song", safe_payload["items"][0]["display_name"])
        self.assertEqual(1, no_family_payload["total"])
        self.assertEqual("recording_track_candidate", no_family_payload["items"][0]["candidate_type"])
        self.assertEqual(1, family_payload["total"])
        self.assertEqual("track_family_candidate", family_payload["items"][0]["candidate_type"])
        self.assertEqual("live", family_payload["items"][0]["relationship_kind"])

    def test_summary_returns_counts_and_samples(self) -> None:
        self._seed_release_track(
            title="Summary Song",
            artist="Summary Artist",
            album="Summary Album",
            spotify_id="summary-album",
            isrc="USSUMMARY1",
            duration_ms=210_000,
        )
        self._seed_release_track(
            title="Summary Song",
            artist="Summary Artist",
            album="Summary Compilation",
            spotify_id="summary-compilation",
            isrc="USSUMMARY1",
            duration_ms=210_500,
        )
        self._seed_release_track(
            title="Summary Variant",
            artist="Summary Artist",
            album="Summary Album",
            spotify_id="summary-variant-album",
            isrc="USSUMMARY2",
            duration_ms=210_000,
        )
        self._seed_release_track(
            title="Summary Variant - Remix",
            artist="Summary Artist",
            album="Summary Remix",
            spotify_id="summary-variant-remix",
            isrc="USSUMMARY2",
            duration_ms=220_000,
        )

        summary = summarize_recording_track_candidates(sample_limit=2)

        self.assertEqual(2, summary["total_candidate_groups"])
        self.assertEqual(1, summary["count_by_candidate_type"]["recording_track_candidate"])
        self.assertEqual(1, summary["count_by_candidate_type"]["track_family_candidate"])
        self.assertEqual(1, summary["count_by_safety_status"]["safe_candidate"])
        self.assertEqual(1, summary["count_by_evidence_bucket"]["same_isrc"])
        self.assertEqual(1, summary["count_by_evidence_bucket"]["variant_flag_excluded"])
        self.assertGreaterEqual(len(summary["sample_safe_candidate_groups"]), 1)
        self.assertGreaterEqual(len(summary["sample_track_family_candidate_groups"]), 1)
        self.assertFalse(summary["source"]["mutates_identity"])

    def test_summary_buckets_separate_isrc_and_metadata_paths(self) -> None:
        self._seed_release_track(
            title="Bucket Same",
            artist="Bucket Artist",
            album="Bucket Album",
            spotify_id="bucket-same-album",
            isrc="USBUCKET1",
            duration_ms=180_000,
        )
        self._seed_release_track(
            title="Bucket Same",
            artist="Bucket Artist",
            album="Bucket Same - Single",
            spotify_id="bucket-same-single",
            isrc="USBUCKET1",
            duration_ms=180_000,
        )
        self._seed_release_track(
            title="Bucket Conflict",
            artist="Bucket Artist",
            album="Bucket Album",
            spotify_id="bucket-conflict-album",
            isrc="USBUCKET2",
            duration_ms=181_000,
        )
        self._seed_release_track(
            title="Bucket Conflict - 2020 Remaster",
            artist="Bucket Artist",
            album="Bucket Reissue",
            spotify_id="bucket-conflict-remaster",
            isrc="USBUCKET3",
            duration_ms=181_500,
        )
        self._seed_release_track(
            title="Bucket Missing",
            artist="Bucket Artist",
            album="Bucket Album",
            spotify_id="bucket-missing-album",
            isrc=None,
            duration_ms=182_000,
        )
        self._seed_release_track(
            title="Bucket Missing",
            artist="Bucket Artist",
            album="Bucket Missing - Single",
            spotify_id="bucket-missing-single",
            isrc=None,
            duration_ms=182_500,
        )
        self._seed_release_track(
            title="Bucket Partial",
            artist="Bucket Artist",
            album="Bucket Album",
            spotify_id="bucket-partial-album",
            isrc="USBUCKET4",
            duration_ms=183_000,
        )
        self._seed_release_track(
            title="Bucket Partial",
            artist="Bucket Artist",
            album="Bucket Partial - Single",
            spotify_id="bucket-partial-single",
            isrc=None,
            duration_ms=183_500,
        )
        self._seed_release_track(
            title="Bucket Variant",
            artist="Bucket Artist",
            album="Bucket Album",
            spotify_id="bucket-variant-album",
            isrc="USBUCKET5",
            duration_ms=184_000,
        )
        self._seed_release_track(
            title="Bucket Variant - Remix",
            artist="Bucket Artist",
            album="Bucket Remix",
            spotify_id="bucket-variant-remix",
            isrc="USBUCKET5",
            duration_ms=184_000,
        )

        summary = summarize_recording_track_candidates(sample_limit=2)

        self.assertEqual(1, summary["count_by_evidence_bucket"]["same_isrc"])
        self.assertEqual(1, summary["count_by_evidence_bucket"]["conflicting_isrc_but_compatible_metadata"])
        self.assertEqual(1, summary["count_by_evidence_bucket"]["missing_isrc_but_compatible_metadata"])
        self.assertEqual(1, summary["count_by_evidence_bucket"]["partial_isrc_match"])
        self.assertEqual(1, summary["count_by_evidence_bucket"]["variant_flag_excluded"])
        self.assertGreaterEqual(len(summary["sample_conflicting_isrc_compatible_metadata_groups"]), 1)

    def test_summary_route_returns_counts(self) -> None:
        self._seed_release_track(
            title="Route Summary",
            artist="Route Summary Artist",
            album="Route Summary Album",
            spotify_id="route-summary-album",
            isrc="USROUTESUMMARY1",
            duration_ms=180_000,
        )
        self._seed_release_track(
            title="Route Summary",
            artist="Route Summary Artist",
            album="Route Summary Soundtrack",
            spotify_id="route-summary-soundtrack",
            isrc="USROUTESUMMARY1",
            duration_ms=180_000,
        )

        with patch("backend.app.routes.audit_routes._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            response = client.get("/debug/tracks/recording-track-candidates/summary?sample_limit=3")

        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertEqual(1, body["total_candidate_groups"])
        self.assertEqual(1, body["count_with_same_isrc_evidence"])
        self.assertIn("sample_safe_candidate_groups", body)


if __name__ == "__main__":
    unittest.main()
