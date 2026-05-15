from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.app.db import ensure_sqlite_db, list_spotify_auth_users
from backend.app.spotify_catalog_backfill import run_spotify_resolution_evidence_track_metadata_worker
from backend.app.spotify_catalog_worker import reset_spotify_track_metadata_worker_cooldown
from backend.app.spotify_token_store import refresh_access_token_if_needed


def _access_token() -> tuple[str | None, str | None]:
    users = list_spotify_auth_users(active_only=True, limit=1)
    if not users:
        return None, "No active Spotify auth user found."
    user_id = str(users[0].get("user_id") or "").strip()
    if not user_id:
        return None, "Active Spotify auth user has no user_id."
    token_row = refresh_access_token_if_needed(user_id)
    token = str(token_row.get("access_token") or "").strip()
    if not token:
        return None, "Spotify access token is unavailable."
    return token, None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process only focused pending Spotify sibling track metadata queue rows from resolution evidence."
    )
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--max-requests", type=int, default=40)
    parser.add_argument("--request-delay-seconds", type=float, default=1.5)
    parser.add_argument("--market", type=str, default="US")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--reset-cooldown",
        action="store_true",
        help="Clear any active Spotify track metadata worker cooldown before running. In dry-run, only report what would reset.",
    )
    parser.add_argument("--json-output", action="store_true", help="Print JSON summary to stdout.")
    args = parser.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    ensure_sqlite_db()
    cooldown_reset = None
    if args.reset_cooldown:
        cooldown_reset = reset_spotify_track_metadata_worker_cooldown(apply=not bool(args.dry_run))
    if args.dry_run:
        result = run_spotify_resolution_evidence_track_metadata_worker(
            access_token="",
            limit=args.limit,
            max_requests=args.max_requests,
            request_delay_seconds=args.request_delay_seconds,
            market=args.market,
            dry_run=True,
        )
    else:
        token, error = _access_token()
        if error or not token:
            result = {
                "ok": False,
                "status": "skipped_auth",
                "performed_action": "none",
                "error": error or "Spotify access token is unavailable.",
            }
        else:
            result = run_spotify_resolution_evidence_track_metadata_worker(
                access_token=token,
                limit=args.limit,
                max_requests=args.max_requests,
                request_delay_seconds=args.request_delay_seconds,
                market=args.market,
                dry_run=False,
            )
    if cooldown_reset is not None:
        result["cooldown_reset"] = cooldown_reset
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
