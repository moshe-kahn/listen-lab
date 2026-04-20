from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from backend.app.config import BACKEND_DIR, get_settings
from backend.app.db import ensure_sqlite_db, sqlite_connection


def _rows() -> list[tuple[object, ...]]:
    with sqlite_connection() as connection:
        return connection.execute(
            """
            SELECT
              id,
              obsolete_release_track_id,
              canonical_release_track_id,
              release_album_id,
              obsolete_primary_name,
              canonical_primary_name,
              match_method,
              confidence,
              status,
              explanation,
              created_at
            FROM release_track_merge_log
            ORDER BY confidence DESC, created_at DESC, id DESC
            """
        ).fetchall()


def _build_output(rows: list[tuple[object, ...]]) -> str:
    settings = get_settings()
    lines = [
        "Release Track Merge Log",
        "=======================",
        f"DB path: {settings.sqlite_db_path}",
        f"Generated at: {datetime.now(UTC).isoformat().replace('+00:00', 'Z')}",
        f"Rows: {len(rows)}",
        "",
    ]

    if not rows:
        lines.append("No merge-log rows found.")
        return "\n".join(lines) + "\n"

    for (
        log_id,
        obsolete_release_track_id,
        canonical_release_track_id,
        release_album_id,
        obsolete_primary_name,
        canonical_primary_name,
        match_method,
        confidence,
        status,
        explanation,
        created_at,
    ) in rows:
        lines.append(
            f"[log {log_id}] confidence={float(confidence):.2f} status={status} "
            f"method={match_method} created_at={created_at}"
        )
        lines.append(
            f"  obsolete_release_track_id={obsolete_release_track_id} "
            f"({obsolete_primary_name})"
        )
        lines.append(
            f"  canonical_release_track_id={canonical_release_track_id} "
            f"({canonical_primary_name})"
        )
        lines.append(f"  release_album_id={release_album_id}")
        lines.append(f"  explanation={explanation}")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    ensure_sqlite_db()
    rows = _rows()

    logs_dir = BACKEND_DIR / "data" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_path = logs_dir / f"release_track_merge_log_{timestamp}.txt"
    output_path.write_text(_build_output(rows), encoding="utf-8")
    print(output_path)


if __name__ == "__main__":
    main()
