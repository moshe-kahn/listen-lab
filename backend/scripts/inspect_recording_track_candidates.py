from __future__ import annotations

import argparse
from typing import Any

from backend.app.recording_track_candidates import query_recording_track_candidates


def _join_limited(values: list[str], *, limit: int = 3) -> str:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        clean = str(value or "").strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        deduped.append(clean)
    if len(deduped) > limit:
        return ", ".join(deduped[:limit]) + f" (+{len(deduped) - limit})"
    return ", ".join(deduped) if deduped else "-"


def _duration_delta(item: dict[str, Any]) -> str:
    deltas = [
        member.get("evidence", {}).get("duration_delta_ms")
        for member in item.get("members", [])
        if member.get("evidence", {}).get("duration_delta_ms") is not None
    ]
    return f"{int(deltas[0])}ms" if deltas else "-"


def _print_item(item: dict[str, Any]) -> None:
    members = item.get("members", [])
    albums = _join_limited([str(member.get("album") or "") for member in members])
    isrcs = _join_limited([str(member.get("isrc") or "") for member in members])
    why_grouped = _join_limited([str(reason) for reason in item.get("why_grouped", [])], limit=2)
    why_review = _join_limited([str(reason) for reason in item.get("why_review", [])], limit=2)
    print(
        "\t".join(
            [
                str(item.get("safety_status") or "-"),
                str(item.get("relationship_kind") or "-"),
                str(item.get("confidence") or "-"),
                str(item.get("display_name") or "-"),
                str(len(members)),
                albums,
                isrcs,
                _duration_delta(item),
                why_grouped,
                why_review,
            ]
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect read-only recording-track candidate groups.")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--safety-status")
    parser.add_argument("--candidate-type")
    parser.add_argument("--relationship-kind")
    parser.add_argument("--min-confidence", type=float)
    parser.add_argument("--exclude-track-family-candidates", action="store_true")
    parser.add_argument("--same-isrc-only", action="store_true")
    parser.add_argument("--show-isrc", action="store_true")
    parser.add_argument("--show-release-context", action="store_true")
    parser.add_argument("--q")
    parser.add_argument("--artist")
    args = parser.parse_args()

    payload = query_recording_track_candidates(
        limit=args.limit,
        offset=args.offset,
        safety_status=args.safety_status,
        candidate_type=args.candidate_type,
        relationship_kind=args.relationship_kind,
        min_confidence=args.min_confidence,
        include_track_family_candidates=not args.exclude_track_family_candidates,
        same_isrc_only=args.same_isrc_only,
        q=args.q,
        artist=args.artist,
    )

    print(
        f"recording_track_candidates total={payload['total']} returned={payload['returned']} "
        f"limit={payload['limit']} offset={payload['offset']} has_more={payload['has_more']}"
    )
    print(
        "\t".join(
            [
                "safety",
                "relationship",
                "confidence",
                "display_name",
                "members",
                "albums",
                "isrcs",
                "duration_delta",
                "why_grouped",
                "why_review",
            ]
        )
    )
    for item in payload["items"]:
        _print_item(item)
        if args.show_isrc:
            for member in item.get("members", []):
                print(f"  isrc release_track={member.get('release_track_id')}: {member.get('isrc_values') or []}")
        if args.show_release_context:
            for member in item.get("members", []):
                print(
                    "  release "
                    f"release_track={member.get('release_track_id')} "
                    f"release_albums={member.get('release_album_ids') or []} "
                    f"spotify_albums={member.get('spotify_album_ids') or []} "
                    f"release_dates={member.get('album_release_dates') or []} "
                    f"album_types={member.get('album_types') or []}"
                )


if __name__ == "__main__":
    main()
