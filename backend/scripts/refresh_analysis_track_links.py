from __future__ import annotations

import json

from backend.app.db import (
    apply_pending_migrations,
    ensure_sqlite_db,
    refresh_conservative_analysis_track_links,
)


def main() -> None:
    ensure_sqlite_db()
    apply_pending_migrations()
    result = refresh_conservative_analysis_track_links()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
