from __future__ import annotations

import json

from backend.app.db import (
    apply_pending_migrations,
    backfill_local_text_entities,
    backfill_spotify_source_entities,
    ensure_sqlite_db,
)


def main() -> None:
    ensure_sqlite_db()
    apply_pending_migrations()
    result = {
        "spotify_exact": backfill_spotify_source_entities(),
        "local_text": backfill_local_text_entities(),
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
