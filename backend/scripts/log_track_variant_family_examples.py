from __future__ import annotations

import sys
from collections import defaultdict
from datetime import UTC, datetime

from backend.app.config import BACKEND_DIR, get_settings
from backend.app.db import ensure_sqlite_db, sqlite_connection
from backend.app.track_variant_policy import interpret_track_variant_title, load_track_variant_policy


def _rows() -> list[tuple[int, str]]:
    with sqlite_connection() as connection:
        return [
            (int(row[0]), str(row[1]))
            for row in connection.execute(
                """
                SELECT id, primary_name
                FROM release_track
                WHERE primary_name IS NOT NULL
                ORDER BY id ASC
                """
            ).fetchall()
        ]


def _build_output() -> str:
    policy = load_track_variant_policy()
    family_examples: dict[str, list[str]] = defaultdict(list)
    rows = _rows()

    for release_track_id, title in rows:
        interpretation = interpret_track_variant_title(title)
        components = interpretation.components
        if not components:
            continue
        for component in components:
            if len(family_examples[component.family]) >= 5:
                continue
            rendered = (
                f"{title} [release_track {release_track_id}] | "
                f"base={interpretation.base_title_anchor or '(none)'} | "
                f"dominant={interpretation.dominant_family or '(none)'} | "
                f"component={component.normalized_label} | "
                f"semantic={component.semantic_category} | "
                f"groupable={component.groupable_by_default}"
            )
            if rendered not in family_examples[component.family]:
                family_examples[component.family].append(rendered)

    settings = get_settings()
    lines = [
        "Track Variant Family Examples",
        "=============================",
        f"DB path: {settings.sqlite_db_path}",
        f"Generated at: {datetime.now(UTC).isoformat().replace('+00:00', 'Z')}",
        "",
    ]

    for family in policy.families:
        lines.append(f"[{family.family}] semantic={family.semantic_category} review={family.needs_review}")
        examples = family_examples.get(family.family, [])
        if not examples:
            lines.append("  (no examples found in current DB)")
        else:
            for example in examples:
                lines.append(f"  - {example}")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ensure_sqlite_db()
    logs_dir = BACKEND_DIR / "data" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_path = logs_dir / f"track_variant_family_examples_{timestamp}.txt"
    output_path.write_text(_build_output(), encoding="utf-8")
    print(output_path)


if __name__ == "__main__":
    main()
