from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.app.catalog_identity_promotion import promote_catalog_album_tracks_to_identity
from backend.app.db import ensure_sqlite_db


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Promote selected cached Spotify album-track rows into ListenLab identity rows."
    )
    parser.add_argument(
        "--album-id",
        action="append",
        default=[],
        help="Spotify album ID to promote. Repeat for multiple albums.",
    )
    parser.add_argument("--apply", action="store_true", help="Mutate identity rows. Default is dry-run.")
    parser.add_argument(
        "--no-refresh-clusters",
        action="store_true",
        help="Skip generated recording/family cluster refresh after applying.",
    )
    args = parser.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    ensure_sqlite_db()
    result = promote_catalog_album_tracks_to_identity(
        album_ids=args.album_id,
        apply=bool(args.apply),
        refresh_clusters=not bool(args.no_refresh_clusters),
    )
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
