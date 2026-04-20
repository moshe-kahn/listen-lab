from __future__ import annotations

import re
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime

from backend.app.config import BACKEND_DIR, get_settings
from backend.app.db import ensure_sqlite_db, sqlite_connection


TRAILING_BRACKET_BLOCK_PATTERN = re.compile(r"\s*[\(\[]([^\)\]]+)[\)\]]\s*$")
TRAILING_DASH_BLOCK_PATTERN = re.compile(r"\s*[-–—:]\s*([^–—:\(\)\[\]]+)\s*$")

GENERIC_VERSION_LABELS = {
    "single version",
    "album version",
    "extended version",
    "original version",
    "full version",
    "short version",
    "clean version",
    "explicit version",
    "radio version",
    "instrumental version",
    "mono version",
    "stereo version",
    "version",
}


def _normalize(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _extract_suffix(title: str | None) -> str | None:
    if title is None:
        return None
    working = str(title).strip()
    if not working:
        return None
    bracket_match = TRAILING_BRACKET_BLOCK_PATTERN.search(working)
    if bracket_match:
        return bracket_match.group(1).strip()
    dash_match = TRAILING_DASH_BLOCK_PATTERN.search(working)
    if dash_match:
        return dash_match.group(1).strip()
    return None


def _classify_version_label(label: str) -> str:
    normalized = _normalize(label)
    if normalized in GENERIC_VERSION_LABELS:
        return "generic_version"
    if "remaster" in normalized or "remastered" in normalized:
        return "remaster_version"
    if "re-record" in normalized or "rerecord" in normalized:
        return "rerecorded_version"
    if re.fullmatch(r"\d{4} version", normalized):
        return "year_version"
    if "alternate version" == normalized:
        return "alternate_version"
    if "karaoke version" == normalized:
        return "karaoke_version"
    if "solo version" in normalized or "duet version" in normalized or "orchestral version" in normalized:
        return "performance_style_version"
    if re.match(r"^[a-z0-9&.' -]+ version$", normalized):
        return "attributed_or_named_version"
    if "version" in normalized:
        return "other_version"
    return "not_version"


def _rows() -> list[tuple[object, ...]]:
    with sqlite_connection() as connection:
        return connection.execute(
            """
            WITH primary_artists AS (
              SELECT ordered.release_track_id, group_concat(ordered.artist_name, ' | ') AS artist_signature
              FROM (
                SELECT ta.release_track_id, a.canonical_name AS artist_name
                FROM track_artist ta
                JOIN artist a ON a.id = ta.artist_id
                WHERE ta.role = 'primary'
                ORDER BY ta.release_track_id, COALESCE(ta.billing_index, 999999), ta.id, a.canonical_name
              ) ordered
              GROUP BY ordered.release_track_id
            ),
            release_albums AS (
              SELECT at.release_track_id, group_concat(ra.primary_name, ' | ') AS album_names
              FROM album_track at
              JOIN release_album ra ON ra.id = at.release_album_id
              GROUP BY at.release_track_id
            )
            SELECT
              rt.id,
              rt.primary_name,
              COALESCE(pa.artist_signature, '') AS artist_signature,
              COALESCE(ral.album_names, '') AS album_names
            FROM release_track rt
            LEFT JOIN primary_artists pa ON pa.release_track_id = rt.id
            LEFT JOIN release_albums ral ON ral.release_track_id = rt.id
            WHERE rt.primary_name IS NOT NULL
            ORDER BY rt.id ASC
            """
        ).fetchall()


def _build_output() -> str:
    rows = _rows()
    class_counter: Counter[str] = Counter()
    examples: dict[str, list[str]] = defaultdict(list)
    label_counter: dict[str, Counter[str]] = defaultdict(Counter)

    for row in rows:
        suffix = _extract_suffix(str(row[1]))
        if suffix is None:
            continue
        normalized_suffix = _normalize(suffix)
        if "version" not in normalized_suffix:
            continue

        bucket = _classify_version_label(normalized_suffix)
        class_counter[bucket] += 1
        label_counter[bucket][normalized_suffix] += 1
        example_line = f"{row[2]} | {row[1]} [release_track {row[0]}] | albums: {row[3]}"
        if example_line not in examples[bucket] and len(examples[bucket]) < 12:
            examples[bucket].append(example_line)

    settings = get_settings()
    lines = [
        "Version Suffix Audit",
        "====================",
        f"DB path: {settings.sqlite_db_path}",
        f"Generated at: {datetime.now(UTC).isoformat().replace('+00:00', 'Z')}",
        "",
        f"version-labeled release_track rows: {sum(class_counter.values())}",
        "",
        "Buckets",
        "-------",
    ]

    for bucket, count in class_counter.most_common():
        lines.append(f"{count:>5}  {bucket}")
        top_labels = label_counter[bucket].most_common(10)
        if top_labels:
            lines.append("       top labels:")
            for label, label_count in top_labels:
                lines.append(f"         {label_count:>4}  {label}")
        if examples[bucket]:
            lines.append("       examples:")
            for example in examples[bucket]:
                lines.append(f"         {example}")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ensure_sqlite_db()
    logs_dir = BACKEND_DIR / "data" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_path = logs_dir / f"version_suffix_audit_{timestamp}.txt"
    output_path.write_text(_build_output(), encoding="utf-8")
    print(output_path)


if __name__ == "__main__":
    main()
