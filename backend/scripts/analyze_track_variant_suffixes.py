from __future__ import annotations

import re
import sys
from collections import Counter
from datetime import UTC, datetime

from backend.app.config import BACKEND_DIR, get_settings
from backend.app.db import ensure_sqlite_db, sqlite_connection


TRAILING_BRACKET_BLOCK_PATTERN = re.compile(r"\s*[\(\[]([^\)\]]+)[\)\]]\s*$")
TRAILING_DASH_BLOCK_PATTERN = re.compile(r"\s*[-–—:]\s*([^–—:\(\)\[\]]+)\s*$")
WORD_PATTERN = re.compile(r"[a-z0-9']+")
STOP_WORDS = {
    "a",
    "an",
    "and",
    "at",
    "by",
    "for",
    "from",
    "in",
    "of",
    "on",
    "the",
    "to",
    "with",
}


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


def _build_output() -> str:
    rows = _rows()
    label_counter: Counter[str] = Counter()
    label_examples: dict[str, list[str]] = {}
    word_counter: Counter[str] = Counter()
    source_counter: Counter[str] = Counter()

    total_with_suffix = 0

    for _, title in rows:
        suffix = _extract_suffix(title)
        if suffix is None:
            continue

        source_kind, label = suffix
        normalized_label = _normalize_label(label)
        if not normalized_label:
            continue

        total_with_suffix += 1
        source_counter[source_kind] += 1
        label_counter[normalized_label] += 1
        label_examples.setdefault(normalized_label, [])
        if len(label_examples[normalized_label]) < 3 and title not in label_examples[normalized_label]:
            label_examples[normalized_label].append(title)

        for word in WORD_PATTERN.findall(normalized_label):
            if word in STOP_WORDS:
                continue
            word_counter[word] += 1

    settings = get_settings()
    lines = [
        "Track Title Variant Suffix Inventory",
        "====================================",
        f"DB path: {settings.sqlite_db_path}",
        f"Generated at: {datetime.now(UTC).isoformat().replace('+00:00', 'Z')}",
        "",
        f"release_track rows scanned: {len(rows)}",
        f"titles with trailing suffix block: {total_with_suffix}",
        f"dash-style suffixes: {source_counter.get('dash', 0)}",
        f"bracket-style suffixes: {source_counter.get('bracket', 0)}",
        "",
        "Most Common Exact Suffix Labels",
        "-------------------------------",
    ]

    for label, count in label_counter.most_common(100):
        examples = "; ".join(label_examples.get(label, []))
        lines.append(f"{count:>5}  {label}")
        if examples:
            lines.append(f"       examples: {examples}")

    lines.extend(
        [
            "",
            "Most Common Words Inside Suffix Labels",
            "--------------------------------------",
        ]
    )
    for word, count in word_counter.most_common(100):
        lines.append(f"{count:>5}  {word}")

    return "\n".join(lines) + "\n"


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    ensure_sqlite_db()
    logs_dir = BACKEND_DIR / "data" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_path = logs_dir / f"track_variant_suffix_inventory_{timestamp}.txt"
    output_path.write_text(_build_output(), encoding="utf-8")
    print(output_path)


if __name__ == "__main__":
    main()
