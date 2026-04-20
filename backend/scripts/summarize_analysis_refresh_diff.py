from __future__ import annotations

import re
import sys
from datetime import UTC, datetime

from backend.app.config import BACKEND_DIR


ENTRY_PATTERN = re.compile(r"^\[release_track (\d+)\] (.+)$")
INDENT_PATTERN = re.compile(r"^\s{2}(.+)$")


def _latest_diff_path():
    logs_dir = BACKEND_DIR / "data" / "logs"
    candidates = sorted(logs_dir.glob("analysis_track_refresh_diff_*.txt"))
    if not candidates:
        raise FileNotFoundError("No analysis_track_refresh_diff log files found.")
    return candidates[-1]


def _parse_sections(lines: list[str]) -> dict[str, list[dict[str, object]]]:
    sections: dict[str, list[dict[str, object]]] = {"Added": [], "Regrouped": []}
    current_section: str | None = None
    current_entry: dict[str, object] | None = None

    for line in lines:
        stripped = line.strip()
        if stripped in sections:
            current_section = stripped
            current_entry = None
            continue
        if current_section is None:
            continue
        if not stripped or stripped.startswith("-") or stripped == "(none)":
            continue
        entry_match = ENTRY_PATTERN.match(line)
        if entry_match:
            current_entry = {
                "release_track_id": int(entry_match.group(1)),
                "headline": entry_match.group(2),
                "details": [],
            }
            sections[current_section].append(current_entry)
            continue
        detail_match = INDENT_PATTERN.match(line)
        if detail_match and current_entry is not None:
            current_entry["details"].append(detail_match.group(1))

    return sections


def _build_summary(source_path: str, sections: dict[str, list[dict[str, object]]]) -> str:
    lines = [
        "Analysis Refresh Review",
        "=======================",
        f"Source diff log: {source_path}",
        f"Generated at: {datetime.now(UTC).isoformat().replace('+00:00', 'Z')}",
        "",
        f"Added entries: {len(sections['Added'])}",
        f"Regrouped entries: {len(sections['Regrouped'])}",
        "",
        "Added",
        "-----",
    ]

    if not sections["Added"]:
        lines.append("(none)")
    for entry in sections["Added"]:
        lines.append(f"[release_track {entry['release_track_id']}] {entry['headline']}")
        for detail in entry["details"]:
            lines.append(f"  {detail}")
        lines.append("")

    lines.extend(["Regrouped", "---------"])
    if not sections["Regrouped"]:
        lines.append("(none)")
    for entry in sections["Regrouped"]:
        lines.append(f"[release_track {entry['release_track_id']}] {entry['headline']}")
        for detail in entry["details"]:
            lines.append(f"  {detail}")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    source_path = _latest_diff_path()
    source_lines = source_path.read_text(encoding="utf-8").splitlines()
    sections = _parse_sections(source_lines)

    logs_dir = BACKEND_DIR / "data" / "logs"
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_path = logs_dir / f"analysis_refresh_review_{timestamp}.txt"
    output_path.write_text(_build_summary(str(source_path), sections), encoding="utf-8")
    print(output_path)


if __name__ == "__main__":
    main()
