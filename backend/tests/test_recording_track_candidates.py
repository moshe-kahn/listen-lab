from __future__ import annotations

import json
import os
import time
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.app.db import apply_pending_migrations, ensure_sqlite_db, sqlite_connection
from backend.app.main import app
from backend.app.recording_track_candidates import (
    RecordingTrackCandidateMember,
    classify_recording_track_candidate_group,
    query_recording_track_candidates,
    summarize_recording_track_candidates,
)


def _member(
    release_track_id: int,
    title: str,
    *,
    artist: str = "Artist A",
    album: str = "Album A",
    source_track_ids: list[str] | None = None,
    source_track_db_ids: list[int] | None = None,
    isrc: str | None = None,
    duration_ms: int | None = 180_000,
) -> RecordingTrackCandidateMember:
    return {
        "release_track_id": release_track_id,
        "title": title,
        "artist": artist,
        "album": album,
        "release_album_ids": [release_track_id],
        "spotify_album_ids": [],
        "album_release_dates": [],
        "album_types": [],
        "source_track_ids": source_track_ids or [f"spotify-{release_track_id}"],
        "source_track_db_ids": source_track_db_ids or [release_track_id],
        "source_track_uris": [f"spotify:track:spotify-{release_track_id}"],
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
            return release_track_id

    def test_query_returns_evidence_and_representative_without_mutating_identity(self) -> None:
        self._seed_release_track(
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
        self.assertEqual("USENDPOINT1", item["members"][0]["isrc"])
        self.assertIn("duration_delta_ms", item["members"][0]["evidence"])

        with sqlite_connection() as connection:
            analysis_track_count = int(connection.execute("SELECT count(*) FROM analysis_track").fetchone()[0])
            analysis_track_map_count = int(connection.execute("SELECT count(*) FROM analysis_track_map").fetchone()[0])
        self.assertEqual(0, analysis_track_count)
        self.assertEqual(0, analysis_track_map_count)

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
