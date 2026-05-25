from __future__ import annotations

import os
import time
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.app.db import apply_pending_migrations, ensure_sqlite_db, sqlite_connection
from backend.app.main import app
from backend.app.recording_track_candidate_reviews import (
    get_recording_track_candidate_review,
    list_recording_track_candidate_reviews,
    save_recording_track_candidate_review,
)


def _review_payload(candidate_key: str = "artist|song") -> dict:
    return {
        "candidate_key": candidate_key,
        "decision": "accepted",
        "reviewer_note": "manual review note",
        "preferred_representative_release_track_id": 10,
        "preferred_playback_source_track_id": 20,
        "candidate_snapshot": {
            "candidate_key": candidate_key,
            "display_name": "Song",
            "evidence_bucket": "same_isrc",
            "members": [
                {
                    "release_track_id": 10,
                    "title": "Song",
                    "isrc_values": ["USREVIEW1"],
                }
            ],
            "why_grouped": ["same ISRC"],
            "why_review": [],
        },
    }


class RecordingTrackCandidateReviewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(
            os.getcwd(),
            "backend",
            "tests",
            "_tmp_recording_track_candidate_reviews.sqlite3",
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

    def test_save_review_decision_preserves_snapshot(self) -> None:
        result = save_recording_track_candidate_review(_review_payload())

        self.assertTrue(result["ok"])
        item = result["item"]
        self.assertEqual("artist|song", item["candidate_key"])
        self.assertEqual("accepted", item["decision"])
        self.assertEqual("manual review note", item["reviewer_note"])
        self.assertEqual(10, item["preferred_representative_release_track_id"])
        self.assertEqual("same_isrc", item["candidate_snapshot"]["evidence_bucket"])
        self.assertEqual(["same ISRC"], item["candidate_snapshot"]["why_grouped"])
        self.assertFalse(result["source"]["mutates_identity"])

    def test_read_saved_reviews(self) -> None:
        saved = save_recording_track_candidate_review(_review_payload("artist|read"))

        listed = list_recording_track_candidate_reviews(limit=20, offset=0)
        readback = get_recording_track_candidate_review(saved["item"]["id"])

        self.assertEqual(1, listed["total"])
        self.assertEqual("artist|read", listed["items"][0]["candidate_key"])
        self.assertEqual(saved["item"]["id"], readback["item"]["id"])
        self.assertEqual("accepted", readback["item"]["decision"])

    def test_update_existing_review_for_same_candidate_key(self) -> None:
        first = save_recording_track_candidate_review(_review_payload("artist|update"))
        payload = _review_payload("artist|update")
        payload["decision"] = "wrong_representative"
        payload["reviewer_note"] = "prefer the single"
        payload["preferred_representative_release_track_id"] = 11
        second = save_recording_track_candidate_review(payload)

        listed = list_recording_track_candidate_reviews(limit=20, offset=0)

        self.assertEqual(first["item"]["id"], second["item"]["id"])
        self.assertEqual(1, listed["total"])
        self.assertEqual("wrong_representative", second["item"]["decision"])
        self.assertEqual("prefer the single", second["item"]["reviewer_note"])
        self.assertEqual(11, second["item"]["preferred_representative_release_track_id"])
        self.assertEqual(first["item"]["created_at"], second["item"]["created_at"])

    def test_saving_review_does_not_create_identity_mappings(self) -> None:
        with sqlite_connection() as connection:
            before_analysis_track = int(connection.execute("SELECT count(*) FROM analysis_track").fetchone()[0])
            before_analysis_map = int(connection.execute("SELECT count(*) FROM analysis_track_map").fetchone()[0])
            before_release_track = int(connection.execute("SELECT count(*) FROM release_track").fetchone()[0])

        save_recording_track_candidate_review(_review_payload("artist|no-mutate"))

        with sqlite_connection() as connection:
            after_analysis_track = int(connection.execute("SELECT count(*) FROM analysis_track").fetchone()[0])
            after_analysis_map = int(connection.execute("SELECT count(*) FROM analysis_track_map").fetchone()[0])
            after_release_track = int(connection.execute("SELECT count(*) FROM release_track").fetchone()[0])

        self.assertEqual(before_analysis_track, after_analysis_track)
        self.assertEqual(before_analysis_map, after_analysis_map)
        self.assertEqual(before_release_track, after_release_track)

    def test_review_routes_save_list_and_read(self) -> None:
        with patch("backend.app.routes.audit_routes._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            save_response = client.post("/debug/tracks/recording-track-candidate-reviews", json=_review_payload("artist|route"))
            list_response = client.get("/debug/tracks/recording-track-candidate-reviews?limit=20&offset=0")

        self.assertEqual(200, save_response.status_code)
        saved = save_response.json()
        self.assertEqual("accepted", saved["item"]["decision"])
        self.assertFalse(saved["source"]["mutates_identity"])
        self.assertEqual(200, list_response.status_code)
        listed = list_response.json()
        self.assertEqual(1, listed["total"])

        with patch("backend.app.routes.audit_routes._require_local_data_session", return_value="user-1"):
            client = TestClient(app)
            read_response = client.get(f"/debug/tracks/recording-track-candidate-reviews/{saved['item']['id']}")

        self.assertEqual(200, read_response.status_code)
        self.assertEqual("artist|route", read_response.json()["item"]["candidate_key"])


if __name__ == "__main__":
    unittest.main()
