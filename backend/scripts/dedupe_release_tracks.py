from __future__ import annotations

import json

from backend.app.db import (
    apply_pending_migrations,
    ensure_sqlite_db,
    merge_conservative_same_album_release_track_duplicates,
)


def main() -> None:
    ensure_sqlite_db()
    apply_pending_migrations()
    result = merge_conservative_same_album_release_track_duplicates()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
