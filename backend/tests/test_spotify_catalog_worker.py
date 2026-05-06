from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

from backend.app.db import apply_pending_migrations, ensure_sqlite_db
from backend.app.spotify_catalog_worker import (
    FALLBACK_RATE_LIMIT_COOLDOWN_SECONDS,
    SPOTIFY_TRACK_METADATA_WORKER,
    TRACK_METADATA_WORKER_CONFIG,
    _iso_utc,
    _parse_iso_utc,
    run_spotify_track_metadata_worker,
)
from backend.scripts.run_spotify_track_metadata_worker import main as run_worker_script_main


class SpotifyCatalogWorkerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp_dir.name) / "spotify_catalog_worker.sqlite3"
        os.environ["SQLITE_DB_PATH"] = str(self.db_path)
        ensure_sqlite_db()
        apply_pending_migrations()
        self.now = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)

    def tearDown(self) -> None:
        self._tmp_dir.cleanup()

    def _token_lister(self, **_: Any) -> list[dict[str, Any]]:
        return [{"user_id": "user-1"}]

    def _token_refresher(self, user_id: str) -> dict[str, Any]:
        self.assertEqual("user-1", user_id)
        return {"access_token": "access-token"}

    def _ok_backfill(self, **kwargs: Any) -> dict[str, Any]:
        return {
            "run_id": 101,
            "status": "ok",
            "requests_total": 10,
            "requests_429": 0,
            "tracks_fetched": 8,
            "tracks_upserted": 8,
            "stop_reason": None,
            "last_error": None,
        }

    def _row(self, table: str) -> sqlite3.Row | None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.row_factory = sqlite3.Row
            return connection.execute(f"SELECT * FROM {table} ORDER BY rowid DESC LIMIT 1").fetchone()

    def test_exits_when_cooldown_is_active(self) -> None:
        cooldown_until = _iso_utc(self.now + timedelta(minutes=10))
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_worker_state (worker_name, cooldown_until, updated_at)
                VALUES (?, ?, ?)
                """,
                (SPOTIFY_TRACK_METADATA_WORKER, cooldown_until, _iso_utc(self.now)),
            )
            connection.commit()

        def backfill_runner(**_: Any) -> dict[str, Any]:
            raise AssertionError("backfill should not run during cooldown")

        result = run_spotify_track_metadata_worker(
            now=self.now,
            backfill_runner=backfill_runner,
            user_lister=self._token_lister,
            token_refresher=self._token_refresher,
        )

        self.assertEqual("skipped_cooldown", result["status"])
        self.assertEqual(cooldown_until, result["cooldown_until"])
        invocation = self._row("spotify_catalog_worker_invocation")
        self.assertIsNotNone(invocation)
        self.assertEqual("skipped_cooldown", invocation["status"])
        self.assertEqual("cooldown_active", invocation["skip_reason"])

    def test_starts_when_cooldown_expired(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_worker_state (worker_name, cooldown_until, updated_at)
                VALUES (?, ?, ?)
                """,
                (
                    SPOTIFY_TRACK_METADATA_WORKER,
                    _iso_utc(self.now - timedelta(minutes=1)),
                    _iso_utc(self.now - timedelta(minutes=2)),
                ),
            )
            connection.commit()

        result = run_spotify_track_metadata_worker(
            now=self.now,
            backfill_runner=self._ok_backfill,
            user_lister=self._token_lister,
            token_refresher=self._token_refresher,
        )

        self.assertEqual("ok", result["status"])
        state = self._row("spotify_catalog_worker_state")
        self.assertIsNotNone(state)
        self.assertIsNone(state["cooldown_until"])

    def test_rate_limited_result_stores_cooldown_from_retry_after(self) -> None:
        completed = self.now + timedelta(seconds=5)

        def backfill_runner(**_: Any) -> dict[str, Any]:
            return {
                "run_id": 102,
                "status": "partial",
                "stop_reason": "rate_limited",
                "max_retry_after_seconds": 120.0,
                "requests_total": 1,
                "requests_429": 1,
                "tracks_fetched": 0,
                "tracks_upserted": 0,
                "last_error": "Stopped early due to rate_limited",
            }

        with patch("backend.app.spotify_catalog_worker._utc_now", return_value=completed):
            result = run_spotify_track_metadata_worker(
                now=self.now,
                backfill_runner=backfill_runner,
                user_lister=self._token_lister,
                token_refresher=self._token_refresher,
            )

        expected = _iso_utc(completed + timedelta(seconds=120))
        self.assertEqual("partial", result["status"])
        self.assertEqual(expected, result["cooldown_until"])
        state = self._row("spotify_catalog_worker_state")
        invocation = self._row("spotify_catalog_worker_invocation")
        self.assertEqual(expected, state["cooldown_until"])
        self.assertEqual(expected, invocation["cooldown_until"])

    def test_rate_limited_result_without_retry_after_stores_fallback_cooldown(self) -> None:
        completed = self.now + timedelta(seconds=5)

        def backfill_runner(**_: Any) -> dict[str, Any]:
            return {
                "run_id": 103,
                "status": "partial",
                "stop_reason": "rate_limited",
                "max_retry_after_seconds": 0.0,
                "requests_total": 1,
                "requests_429": 1,
                "tracks_fetched": 0,
                "tracks_upserted": 0,
                "last_error": "Stopped early due to rate_limited",
            }

        with patch("backend.app.spotify_catalog_worker._utc_now", return_value=completed):
            result = run_spotify_track_metadata_worker(
                now=self.now,
                backfill_runner=backfill_runner,
                user_lister=self._token_lister,
                token_refresher=self._token_refresher,
            )

        cooldown = _parse_iso_utc(str(result["cooldown_until"]))
        self.assertIsNotNone(cooldown)
        self.assertEqual(completed + timedelta(seconds=FALLBACK_RATE_LIMIT_COOLDOWN_SECONDS), cooldown)

    def test_overlapping_lock_prevents_second_run(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_worker_lock (worker_name, locked_at, owner)
                VALUES (?, ?, ?)
                """,
                (SPOTIFY_TRACK_METADATA_WORKER, _iso_utc(self.now), "other"),
            )
            connection.commit()

        def backfill_runner(**_: Any) -> dict[str, Any]:
            raise AssertionError("backfill should not run when lock is active")

        result = run_spotify_track_metadata_worker(
            now=self.now,
            backfill_runner=backfill_runner,
            user_lister=self._token_lister,
            token_refresher=self._token_refresher,
        )

        self.assertEqual("skipped_overlap", result["status"])
        invocation = self._row("spotify_catalog_worker_invocation")
        self.assertEqual("skipped_overlap", invocation["status"])
        self.assertEqual("worker_lock_active", invocation["skip_reason"])

    def test_recent_request_budget_skips_without_calling_backfill(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            for run_id in range(6):
                connection.execute(
                    """
                    INSERT INTO spotify_catalog_backfill_run (
                      id, started_at, completed_at, status, requests_total, requests_429, tracks_fetched
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        1000 + run_id,
                        _iso_utc(self.now - timedelta(minutes=run_id * 5)),
                        _iso_utc(self.now - timedelta(minutes=run_id * 5) + timedelta(seconds=10)),
                        "ok",
                        101,
                        0,
                        100,
                    ),
                )
            connection.commit()

        def backfill_runner(**_: Any) -> dict[str, Any]:
            raise AssertionError("backfill should not run when recent request budget is exhausted")

        result = run_spotify_track_metadata_worker(
            now=self.now,
            backfill_runner=backfill_runner,
            user_lister=self._token_lister,
            token_refresher=self._token_refresher,
        )

        self.assertEqual("skipped_request_budget", result["status"])
        self.assertEqual("request_budget_cooldown", result["skip_reason"])
        self.assertEqual(606, result["recent_requests_60m"])
        self.assertEqual(_iso_utc(self.now + timedelta(minutes=15)), result["cooldown_until"])
        invocation = self._row("spotify_catalog_worker_invocation")
        state = self._row("spotify_catalog_worker_state")
        self.assertEqual("skipped_request_budget", invocation["status"])
        self.assertEqual("request_budget_cooldown", invocation["skip_reason"])
        self.assertEqual("skipped_request_budget", state["last_status"])
        self.assertEqual(result["cooldown_until"], state["cooldown_until"])

    def test_recent_request_budget_hard_limit_uses_longer_cooldown(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            for run_id in range(7):
                connection.execute(
                    """
                    INSERT INTO spotify_catalog_backfill_run (
                      id, started_at, completed_at, status, requests_total, requests_429, tracks_fetched
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        2000 + run_id,
                        _iso_utc(self.now - timedelta(minutes=run_id * 5)),
                        _iso_utc(self.now - timedelta(minutes=run_id * 5) + timedelta(seconds=10)),
                        "ok",
                        101,
                        0,
                        100,
                    ),
                )
            connection.commit()

        result = run_spotify_track_metadata_worker(
            now=self.now,
            backfill_runner=lambda **_: self._ok_backfill(),
            user_lister=self._token_lister,
            token_refresher=self._token_refresher,
        )

        self.assertEqual("skipped_request_budget", result["status"])
        self.assertEqual(707, result["recent_requests_60m"])
        self.assertEqual(_iso_utc(self.now + timedelta(minutes=30)), result["cooldown_until"])

    def test_stale_lock_can_be_replaced(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                INSERT INTO spotify_catalog_worker_lock (worker_name, locked_at, owner)
                VALUES (?, ?, ?)
                """,
                (SPOTIFY_TRACK_METADATA_WORKER, _iso_utc(self.now - timedelta(hours=3)), "stale"),
            )
            connection.commit()

        result = run_spotify_track_metadata_worker(
            now=self.now,
            owner="new-owner",
            backfill_runner=self._ok_backfill,
            user_lister=self._token_lister,
            token_refresher=self._token_refresher,
        )

        self.assertEqual("ok", result["status"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            lock_count = int(connection.execute("SELECT count(*) FROM spotify_catalog_worker_lock").fetchone()[0])
        self.assertEqual(0, lock_count)

    def test_lock_releases_on_success(self) -> None:
        result = run_spotify_track_metadata_worker(
            now=self.now,
            owner="owner",
            backfill_runner=self._ok_backfill,
            user_lister=self._token_lister,
            token_refresher=self._token_refresher,
        )

        self.assertEqual("ok", result["status"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            lock_count = int(connection.execute("SELECT count(*) FROM spotify_catalog_worker_lock").fetchone()[0])
        self.assertEqual(0, lock_count)

    def test_lock_releases_on_exception(self) -> None:
        def backfill_runner(**_: Any) -> dict[str, Any]:
            raise RuntimeError("boom")

        result = run_spotify_track_metadata_worker(
            now=self.now,
            owner="owner",
            backfill_runner=backfill_runner,
            user_lister=self._token_lister,
            token_refresher=self._token_refresher,
        )

        self.assertEqual("failed", result["status"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            lock_count = int(connection.execute("SELECT count(*) FROM spotify_catalog_worker_lock").fetchone()[0])
        self.assertEqual(0, lock_count)

    def test_invokes_backfill_with_exact_track_only_config(self) -> None:
        captured: dict[str, Any] = {}

        def backfill_runner(**kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return self._ok_backfill(**kwargs)

        result = run_spotify_track_metadata_worker(
            now=self.now,
            backfill_runner=backfill_runner,
            user_lister=self._token_lister,
            token_refresher=self._token_refresher,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual("access-token", captured.pop("access_token"))
        self.assertEqual(TRACK_METADATA_WORKER_CONFIG, captured)

    def test_passes_progress_callback_to_backfill_when_supplied(self) -> None:
        captured: dict[str, Any] = {}
        events: list[dict[str, Any]] = []

        def backfill_runner(**kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            callback = kwargs.get("progress_callback")
            self.assertTrue(callable(callback))
            callback({"event": "progress", "tracks_fetched": 10})
            return self._ok_backfill(**kwargs)

        result = run_spotify_track_metadata_worker(
            now=self.now,
            backfill_runner=backfill_runner,
            user_lister=self._token_lister,
            token_refresher=self._token_refresher,
            progress_callback=events.append,
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual([{"event": "progress", "tracks_fetched": 10}], events)
        self.assertEqual("access-token", captured.pop("access_token"))
        captured.pop("progress_callback")
        self.assertEqual(TRACK_METADATA_WORKER_CONFIG, captured)

    def test_script_main_emits_json_line_payloads_from_progress_and_final(self) -> None:
        emitted: list[dict[str, object]] = []

        def worker_runner(**kwargs: Any) -> dict[str, object]:
            callback = kwargs["progress_callback"]
            callback({"event": "start", "run_id": 123, "tracks_fetched": 0})
            callback({"event": "progress", "run_id": 123, "tracks_fetched": 10})
            return {
                "worker_name": "spotify_track_metadata",
                "status": "ok",
                "backfill_run_id": 123,
                "requests_total": 11,
                "requests_429": 0,
                "tracks_fetched": 10,
                "tracks_upserted": 10,
                "stop_reason": None,
                "cooldown_until": None,
                "invocation_id": 99,
            }

        run_worker_script_main(progress_writer=emitted.append, worker_runner=worker_runner)

        self.assertEqual("start", emitted[0]["event"])
        self.assertEqual("progress", emitted[1]["event"])
        self.assertEqual("ok", emitted[2]["status"])
        self.assertEqual("Run 123 finished ok: 10 tracks fetched, 11 requests, 0 rate limits", emitted[2]["message"])
        self.assertNotIn("access_token", json.dumps(emitted, sort_keys=True))

    def test_script_loop_repeats_after_ok_and_prints_condensed_lines(self) -> None:
        terminal: list[str] = []
        sleeps: list[float] = []
        state = {"calls": 0, "monotonic": 0.0}

        def worker_runner(**kwargs: Any) -> dict[str, object]:
            state["calls"] += 1
            if state["calls"] == 1:
                kwargs["progress_callback"](
                    {
                        "event": "start",
                        "run_id": 123,
                        "tracks_fetched": 0,
                        "requests_total": 0,
                        "requests_429": 0,
                        "worker_config": {"limit": 250},
                    }
                )
                return {
                    "worker_name": "spotify_track_metadata",
                    "status": "ok",
                    "backfill_run_id": 123,
                    "requests_total": 251,
                    "requests_429": 0,
                    "tracks_fetched": 250,
                    "tracks_upserted": 250,
                    "stop_reason": None,
                    "cooldown_until": None,
                    "invocation_id": 1,
                }
            return {
                "worker_name": "spotify_track_metadata",
                "status": "failed",
                "backfill_run_id": None,
                "requests_total": 0,
                "requests_429": 0,
                "tracks_fetched": 0,
                "tracks_upserted": 0,
                "stop_reason": None,
                "cooldown_until": None,
                "invocation_id": 2,
            }

        def sleeper(seconds: float) -> None:
            sleeps.append(seconds)
            state["monotonic"] += seconds

        run_worker_script_main(
            args=["--loop", "--between-runs-seconds", "300", "--max-runtime-minutes", "20"],
            terminal_writer=terminal.append,
            worker_runner=worker_runner,
            sleeper=sleeper,
            monotonic=lambda: state["monotonic"],
        )

        self.assertEqual(2, state["calls"])
        self.assertEqual([300.0], sleeps)
        self.assertIn("run 123 start limit=250", terminal)
        self.assertIn("run 123 ok fetched=250 req=251 429=0", terminal)
        self.assertIn("sleep 300s", terminal)

    def test_script_loop_sleeps_until_cooldown_status(self) -> None:
        terminal: list[str] = []
        sleeps: list[float] = []
        now_value = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
        state = {"calls": 0, "monotonic": 0.0}

        def worker_runner(**_: Any) -> dict[str, object]:
            state["calls"] += 1
            if state["calls"] == 1:
                return {
                    "worker_name": "spotify_track_metadata",
                    "status": "skipped_request_budget",
                    "skip_reason": "request_budget_cooldown",
                    "cooldown_until": _iso_utc(now_value + timedelta(minutes=15)),
                    "recent_requests_60m": 606,
                    "invocation_id": 1,
                }
            return {"worker_name": "spotify_track_metadata", "status": "failed", "invocation_id": 2}

        def sleeper(seconds: float) -> None:
            sleeps.append(seconds)
            state["monotonic"] += seconds

        run_worker_script_main(
            args=["--loop", "--max-runtime-minutes", "20"],
            terminal_writer=terminal.append,
            worker_runner=worker_runner,
            sleeper=sleeper,
            monotonic=lambda: state["monotonic"],
            now=lambda: now_value,
        )

        self.assertEqual([900.0], sleeps)
        self.assertIn("cooldown 15m", terminal)
        self.assertEqual(2, state["calls"])

    def test_script_loop_sleeps_until_rate_limit_cooldown(self) -> None:
        terminal: list[str] = []
        sleeps: list[float] = []
        now_value = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
        state = {"calls": 0, "monotonic": 0.0}

        def worker_runner(**_: Any) -> dict[str, object]:
            state["calls"] += 1
            if state["calls"] == 1:
                return {
                    "worker_name": "spotify_track_metadata",
                    "status": "partial",
                    "stop_reason": "rate_limited",
                    "cooldown_until": _iso_utc(now_value + timedelta(minutes=30)),
                    "requests_total": 3,
                    "requests_429": 1,
                    "tracks_fetched": 1,
                    "invocation_id": 1,
                }
            return {"worker_name": "spotify_track_metadata", "status": "failed", "invocation_id": 2}

        def sleeper(seconds: float) -> None:
            sleeps.append(seconds)
            state["monotonic"] += seconds

        run_worker_script_main(
            args=["--loop", "--max-runtime-minutes", "40"],
            terminal_writer=terminal.append,
            worker_runner=worker_runner,
            sleeper=sleeper,
            monotonic=lambda: state["monotonic"],
            now=lambda: now_value,
        )

        self.assertEqual([1800.0], sleeps)
        self.assertIn("cooldown 30m", terminal)
        self.assertEqual(2, state["calls"])

    def test_script_loop_keyboard_interrupt_exits_cleanly(self) -> None:
        terminal: list[str] = []

        def worker_runner(**_: Any) -> dict[str, object]:
            raise KeyboardInterrupt

        run_worker_script_main(
            args=["--loop"],
            terminal_writer=terminal.append,
            worker_runner=worker_runner,
            sleeper=lambda _: None,
        )

        self.assertEqual(["stopped ctrl_c"], terminal)

    def test_script_jsonl_output_appends_full_event_stream_and_condenses_terminal(self) -> None:
        terminal: list[str] = []
        output_path = Path(self._tmp_dir.name) / "worker.jsonl"

        def worker_runner(**kwargs: Any) -> dict[str, object]:
            kwargs["progress_callback"](
                {
                    "event": "start",
                    "run_id": 123,
                    "tracks_fetched": 0,
                    "requests_total": 0,
                    "requests_429": 0,
                    "worker_config": {"limit": 250},
                }
            )
            return {
                "worker_name": "spotify_track_metadata",
                "status": "ok",
                "backfill_run_id": 123,
                "requests_total": 251,
                "requests_429": 0,
                "tracks_fetched": 250,
                "tracks_upserted": 250,
                "stop_reason": None,
                "cooldown_until": None,
                "invocation_id": 1,
            }

        run_worker_script_main(
            args=["--jsonl-output", str(output_path)],
            terminal_writer=terminal.append,
            worker_runner=worker_runner,
        )

        self.assertEqual(["run 123 start limit=250", "run 123 ok fetched=250 req=251 429=0"], terminal)
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual("start", rows[0]["event"])
        self.assertEqual({"limit": 250}, rows[0]["worker_config"])
        self.assertEqual("ok", rows[1]["status"])
        self.assertEqual(250, rows[1]["tracks_fetched"])
        self.assertNotIn("access_token", json.dumps(rows, sort_keys=True))

    def test_never_passes_album_tracklist_or_full_backfill_config(self) -> None:
        captured: dict[str, Any] = {}

        def backfill_runner(**kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return self._ok_backfill(**kwargs)

        run_spotify_track_metadata_worker(
            now=self.now,
            backfill_runner=backfill_runner,
            user_lister=self._token_lister,
            token_refresher=self._token_refresher,
        )

        self.assertEqual("tracks", captured["target"])
        self.assertEqual("metadata_only", captured["run_mode"])
        self.assertEqual("identity_metadata", captured["reason"])
        self.assertEqual("none", captured["album_tracklist_policy"])
        self.assertFalse(captured["include_albums"])
        self.assertNotEqual("full_catalog", captured["run_mode"])

    def test_persisted_invocation_row_records_result_fields(self) -> None:
        result = run_spotify_track_metadata_worker(
            now=self.now,
            backfill_runner=self._ok_backfill,
            user_lister=self._token_lister,
            token_refresher=self._token_refresher,
        )

        self.assertEqual("ok", result["status"])
        invocation = self._row("spotify_catalog_worker_invocation")
        self.assertEqual(101, invocation["backfill_run_id"])
        self.assertEqual(10, invocation["requests_total"])
        self.assertEqual(0, invocation["requests_429"])
        self.assertEqual(8, invocation["tracks_fetched"])
        self.assertEqual(8, invocation["tracks_upserted"])
        stored_result = json.loads(str(invocation["result_json"]))
        self.assertEqual(101, stored_result["run_id"])

    def test_skipped_no_token_is_persisted_and_does_not_call_backfill(self) -> None:
        def backfill_runner(**_: Any) -> dict[str, Any]:
            raise AssertionError("backfill should not run without a token")

        result = run_spotify_track_metadata_worker(
            now=self.now,
            backfill_runner=backfill_runner,
            user_lister=lambda **_: [],
            token_refresher=self._token_refresher,
        )

        self.assertEqual("skipped_no_token", result["status"])
        invocation = self._row("spotify_catalog_worker_invocation")
        state = self._row("spotify_catalog_worker_state")
        self.assertEqual("skipped_no_token", invocation["status"])
        self.assertEqual("no_token", invocation["skip_reason"])
        self.assertEqual("skipped_no_token", state["last_status"])
        with closing(sqlite3.connect(self.db_path)) as connection:
            lock_count = int(connection.execute("SELECT count(*) FROM spotify_catalog_worker_lock").fetchone()[0])
        self.assertEqual(0, lock_count)


if __name__ == "__main__":
    unittest.main()
