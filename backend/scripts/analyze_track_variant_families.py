from __future__ import annotations

import re
import sys
from collections import Counter
from datetime import UTC, datetime

from backend.app.config import BACKEND_DIR, get_settings
from backend.app.db import ensure_sqlite_db, sqlite_connection


TRAILING_BRACKET_BLOCK_PATTERN = re.compile(r"\s*[\(\[]([^\)\]]+)[\)\]]\s*$")
TRAILING_DASH_BLOCK_PATTERN = re.compile(r"\s*[-–—:]\s*([^–—:\(\)\[\]]+)\s*$")


def _normalize_label(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _extract_suffix(title: str | None) -> tuple[str, str] | None:
    if title is None:
        return None
    working = str(title).strip()
    if not working:
        return None

    bracket_match = TRAILING_BRACKET_BLOCK_PATTERN.search(working)
    if bracket_match:
        return ("bracket", bracket_match.group(1).strip())

    dash_match = TRAILING_DASH_BLOCK_PATTERN.search(working)
    if dash_match:
        return ("dash", dash_match.group(1).strip())

    return None


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


def _family_for_label(label: str) -> str:
    normalized = _normalize_label(label)

    if "remix" in normalized:
        return "remix"
    if "rework" in normalized:
        return "rework"
    if "cover" in normalized:
        return "cover"
    if "live" in normalized:
        return "live"
    if "acoustic" in normalized:
        return "acoustic"
    if "instrumental" in normalized:
        return "instrumental"
    if "demo" in normalized:
        return "demo"
    if "remaster" in normalized or "remastered" in normalized:
        return "remaster"
    if "master" in normalized:
        return "master"
    if "explicit" in normalized or "clean" in normalized:
        return "content_rating"
    if "edit" in normalized:
        return "edit"
    if "version" in normalized:
        return "version"
    if normalized == "original":
        return "version"
    if "mix" in normalized:
        return "mix"
    if "session" in normalized or "sessions" in normalized:
        return "session"
    if "commentary" in normalized:
        return "commentary"
    if "bonus" in normalized:
        return "packaging"
    if "deluxe" in normalized or "expanded" in normalized:
        return "packaging"
    if "mono" in normalized or "stereo" in normalized:
        return "format"
    if "score" in normalized or "soundtrack" in normalized or "from " in normalized:
        return "score_soundtrack"
    if normalized.startswith("feat.") or normalized.startswith("featuring ") or " feat. " in normalized:
        return "featured_credit"
    if re.fullmatch(r"\d{4}", normalized):
        return "year_tag"
    if (
        "solo" in normalized
        or "duet" in normalized
        or "trio" in normalized
        or "quartet" in normalized
        or "orchestral" in normalized
    ):
        return "performance_style"
    if (
        "hz" in normalized
        or "alpha waves" in normalized
        or "delta waves" in normalized
        or "theta waves" in normalized
        or "sleep" in normalized
        or "meditation" in normalized
    ):
        return "wellness_frequency"
    if (
        "interlude" in normalized
        or normalized.startswith("pt. ")
        or normalized.startswith("part ")
        or normalized == "intro"
        or normalized == "outro"
        or "skit" in normalized
        or "reprise" in normalized
    ):
        return "structural"
    if (
        "recorded at " in normalized
        or "recorded live at " in normalized
        or normalized.startswith("live from ")
        or "studio" in normalized
    ):
        return "recording_context"
    if "stripped" in normalized:
        return "acoustic"
    return "other"


def _build_output() -> str:
    rows = _rows()
    family_counter: Counter[str] = Counter()
    family_label_counter: dict[str, Counter[str]] = {}
    family_examples: dict[str, list[str]] = {}
    exact_label_counter: Counter[str] = Counter()
    total_with_suffix = 0

    for _, title in rows:
        suffix = _extract_suffix(title)
        if suffix is None:
            continue

        _, label = suffix
        normalized_label = _normalize_label(label)
        if not normalized_label:
            continue

        total_with_suffix += 1
        exact_label_counter[normalized_label] += 1

        family = _family_for_label(normalized_label)
        family_counter[family] += 1
        family_label_counter.setdefault(family, Counter())
        family_label_counter[family][normalized_label] += 1
        family_examples.setdefault(family, [])
        if len(family_examples[family]) < 5 and title not in family_examples[family]:
            family_examples[family].append(title)

    settings = get_settings()
    lines = [
        "Track Title Variant Family Inventory",
        "====================================",
        f"DB path: {settings.sqlite_db_path}",
        f"Generated at: {datetime.now(UTC).isoformat().replace('+00:00', 'Z')}",
        "",
        f"release_track rows scanned: {len(rows)}",
        f"titles with trailing suffix block: {total_with_suffix}",
        "",
        "Family Counts",
        "-------------",
    ]

    for family, count in family_counter.most_common():
        lines.append(f"{count:>5}  {family}")
        examples = "; ".join(family_examples.get(family, []))
        if examples:
            lines.append(f"       examples: {examples}")
        top_labels = family_label_counter[family].most_common(10)
        if top_labels:
            lines.append("       top labels:")
            for label, label_count in top_labels:
                lines.append(f"         {label_count:>4}  {label}")

    lines.extend(
        [
            "",
            "Top Exact Labels Across All Families",
            "------------------------------------",
        ]
    )
    for label, count in exact_label_counter.most_common(100):
        lines.append(f"{count:>5}  {label}")

    return "\n".join(lines) + "\n"


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    ensure_sqlite_db()
    logs_dir = BACKEND_DIR / "data" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_path = logs_dir / f"track_variant_family_inventory_{timestamp}.txt"
    output_path.write_text(_build_output(), encoding="utf-8")
    print(output_path)


if __name__ == "__main__":
    main()
