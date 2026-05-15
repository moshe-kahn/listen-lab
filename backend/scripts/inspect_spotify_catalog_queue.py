from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

# Allow direct execution via:
#   ./.venv/bin/python backend/scripts/inspect_spotify_catalog_queue.py
# from repository root, where `backend` is not always importable by default.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.app.db import ensure_sqlite_db, list_spotify_auth_users
from backend.app.spotify_catalog_backfill import (
    append_resolution_evidence_candidate_tracklists_from_report,
    append_resolution_evidence_sibling_tracks_from_report,
    inspect_source_release_album_display_gaps,
    inspect_spotify_album_metadata_display_gaps,
    inspect_spotify_catalog_queue_resolution_evidence,
    inspect_spotify_nested_metadata_integrity,
    plan_source_release_album_display_enrichment,
    repair_spotify_album_basic_metadata_from_track_payloads,
    run_source_release_album_display_enrichment_worker,
)
from backend.app.spotify_token_store import refresh_access_token_if_needed


def _utc_timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def build_queue_snapshot_export(report: dict[str, object], *, timestamp: str | None = None) -> dict[str, object]:
    return {
        "timestamp": timestamp or _utc_timestamp(),
        "total_queued_items": report.get("queue_snapshot", {}).get("total_queued_items", 0)
        if isinstance(report.get("queue_snapshot"), dict)
        else 0,
        "queue_snapshot": report.get("queue_snapshot", {}),
        "queue_rows": report.get("queue_items", []),
    }


def write_queue_snapshot_export(report: dict[str, object], output_path: str | Path) -> Path:
    path = Path(output_path)
    if not path.is_absolute():
        path = _REPO_ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    snapshot = build_queue_snapshot_export(report)
    path.write_text(json.dumps(snapshot, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _jsonl_writer(path: str | None) -> Callable[[dict[str, object]], None] | None:
    if not path:
        return None
    output_path = Path(path).expanduser()
    if not output_path.is_absolute():
        output_path = _REPO_ROOT / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    def _write(payload: dict[str, object]) -> None:
        with output_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True))
            handle.write("\n")

    return _write


def _access_token() -> tuple[str | None, str | None]:
    users = list_spotify_auth_users(active_only=True, limit=1)
    if not users:
        return None, "No active Spotify auth user found."
    user_id = str(users[0].get("user_id") or "").strip()
    if not user_id:
        return None, "Active Spotify auth user has no user_id."
    token_row = refresh_access_token_if_needed(user_id)
    token = str(token_row.get("access_token") or "").strip()
    if not token:
        return None, "Spotify access token is unavailable."
    return token, None


def _int_value(payload: dict[str, Any], key: str) -> int:
    try:
        return int(payload.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def run_source_release_album_display_enrichment_loop(
    *,
    access_token: str,
    limit: int,
    max_requests: int,
    request_delay_seconds: float,
    market: str,
    max_runtime_minutes: float | None,
    between_runs_seconds: float,
    jsonl_output: str | None = None,
    summary_only: bool = False,
    runner: Callable[..., dict[str, Any]] = run_source_release_album_display_enrichment_worker,
    sleeper: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    started = monotonic()
    max_runtime_seconds = None if max_runtime_minutes is None else max(0.0, float(max_runtime_minutes) * 60.0)
    write_jsonl = _jsonl_writer(jsonl_output)
    output_path = None
    if jsonl_output:
        path = Path(jsonl_output).expanduser()
        output_path = str(path if path.is_absolute() else _REPO_ROOT / path)

    totals = {
        "total_batches": 0,
        "total_processed_count": 0,
        "total_fetched_track_metadata": 0,
        "total_fetched_and_album_evidence_added": 0,
        "total_requests": 0,
        "total_429": 0,
        "total_errors": 0,
    }
    batches: list[dict[str, Any]] = []
    stop_reason = "max_runtime"

    while True:
        elapsed = monotonic() - started
        if max_runtime_seconds is not None and elapsed >= max_runtime_seconds:
            stop_reason = "max_runtime"
            break

        result = runner(
            access_token=access_token,
            limit=limit,
            max_requests=max_requests,
            request_delay_seconds=request_delay_seconds,
            market=market,
        )
        totals["total_batches"] += 1
        totals["total_processed_count"] += _int_value(result, "processed_count")
        totals["total_fetched_track_metadata"] += _int_value(result, "fetched_track_metadata")
        totals["total_fetched_and_album_evidence_added"] += _int_value(result, "fetched_and_album_evidence_added")
        totals["total_requests"] += _int_value(result, "requests_total")
        totals["total_429"] += _int_value(result, "requests_429")
        totals["total_errors"] += _int_value(result, "error_count")

        batch_payload = {"event": "batch_result", "batch_index": totals["total_batches"], **result}
        if write_jsonl:
            write_jsonl(batch_payload)
        if not summary_only:
            batches.append(batch_payload)

        if result.get("cooldown_until"):
            stop_reason = "cooldown"
            break
        if _int_value(result, "requests_429") > 0:
            stop_reason = "rate_limited"
            break
        if _int_value(result, "error_count") > 0:
            stop_reason = "errors"
            break
        if _int_value(result, "selected_count") == 0:
            stop_reason = "no_selected_candidates"
            break
        if _int_value(result, "processed_count") == 0:
            stop_reason = "no_processed_candidates"
            break

        elapsed = monotonic() - started
        if max_runtime_seconds is not None and elapsed >= max_runtime_seconds:
            stop_reason = "max_runtime"
            break
        sleep_seconds = max(0.0, float(between_runs_seconds))
        if max_runtime_seconds is not None and elapsed + sleep_seconds > max_runtime_seconds:
            stop_reason = "max_runtime"
            break
        if sleep_seconds > 0:
            sleeper(sleep_seconds)

    summary: dict[str, Any] = {
        "ok": True,
        "mode": "loop",
        "worker_name": "source_release_album_display_enrichment",
        **totals,
        "stop_reason": stop_reason,
        "elapsed_seconds": round(monotonic() - started, 3),
    }
    if output_path:
        summary["jsonl_output"] = output_path
    if not summary_only:
        summary["batches"] = batches
    return summary


def _compact_append_result(result: object) -> dict[str, object]:
    if not isinstance(result, dict):
        return {}
    compact: dict[str, object] = {}
    for source_key, summary_key in (
        ("ok", "ok"),
        ("mode", "mode"),
        ("performed_action", "performed_action"),
        ("selected_count", "candidate_count"),
        ("selected_count", "planned_count"),
        ("inserted", "appended_count"),
        ("already_existing", "skipped_existing_count"),
        ("skipped", "skipped_count"),
    ):
        if source_key in result:
            compact[summary_key] = result[source_key]
    if "appendability_diagnostic" in result:
        compact["appendability_diagnostic"] = result["appendability_diagnostic"]
    return compact


def build_summary_only_report(report: dict[str, object]) -> dict[str, object]:
    safety_recommendation = report.get("safety_recommendation")
    resolution_relevance = report.get("resolution_relevance")
    plan = report.get("dry_run_resolution_evidence_plan")
    delta = report.get("resolution_evidence_delta")
    delta_counts = delta.get("counts", {}) if isinstance(delta, dict) else {}
    plan_source_counts = plan.get("source_set_counts", {}) if isinstance(plan, dict) else {}

    summary: dict[str, object] = {
        "ok": report.get("ok"),
    }
    if "status" in report:
        summary["status"] = report.get("status")
    if isinstance(plan, dict):
        summary["performed_action"] = plan.get("performed_action")

    if isinstance(safety_recommendation, dict):
        summary["safety_recommendation"] = {
            "action": safety_recommendation.get("action"),
            "rationale": safety_recommendation.get("rationale"),
            "counts": safety_recommendation.get("counts", {}),
        }
    if isinstance(resolution_relevance, dict):
        summary["resolution_relevance"] = {
            "bucket_counts_by_status": resolution_relevance.get("bucket_counts_by_status", {}),
        }
    if isinstance(plan, dict):
        summary["dry_run_resolution_evidence_plan"] = {
            "mode": plan.get("mode"),
            "performed_action": plan.get("performed_action"),
            "counts_by_plan_status": plan.get("counts_by_plan_status", {}),
            "missing_candidate_album_tracklists_count": plan_source_counts.get(
                "candidate_album_tracklist_missing_count",
                delta_counts.get("missing_candidate_album_tracklists", 0),
            ),
            "candidate_albums_queued_but_missing_tracklists_count": delta_counts.get(
                "candidate_albums_queued_but_missing_tracklists", 0
            ),
            "sibling_tracks_missing_from_queue_count": delta_counts.get("sibling_tracks_missing_from_queue", 0),
            "sibling_tracks_requiring_metadata_count": delta_counts.get("sibling_tracks_requiring_metadata", 0),
            "missing_sibling_track_evidence_count": delta_counts.get("sibling_tracks_requiring_metadata", 0),
            "tracklists_needed_before_sibling_tracks_can_be_enumerated_count": delta_counts.get(
                "tracklists_needed_before_sibling_tracks_can_be_enumerated", 0
            ),
        }

    for key in (
        "append_resolution_evidence_candidate_tracklists",
        "append_resolution_evidence_sibling_tracks",
    ):
        if key in report:
            compact_result = _compact_append_result(report.get(key))
            summary[key] = compact_result
            if compact_result.get("performed_action") is not None:
                summary["performed_action"] = compact_result["performed_action"]

    return summary


def build_album_display_diagnostic_summary(report: dict[str, object]) -> dict[str, object]:
    counts = report.get("counts", {})
    if not isinstance(counts, dict):
        counts = {}
    summary: dict[str, object] = {
        "ok": report.get("ok"),
        "total_rows": counts.get("total_rows", 0),
        "rows_with_source_album_display_info": counts.get("rows_with_source_album_display_info", 0),
        "rows_with_source_album_display_after_embedded_fallback": counts.get(
            "rows_with_source_album_display_after_embedded_fallback", 0
        ),
        "rows_with_no_spotify_album_evidence": counts.get("rows_with_no_spotify_album_evidence", 0),
        "rows_with_album_spotify_id_but_no_local_album_name": counts.get(
            "rows_with_album_spotify_id_but_no_local_album_name", 0
        ),
        "rows_with_no_album_spotify_id": counts.get("rows_with_no_album_spotify_id", 0),
    }
    if "status" in report:
        summary["status"] = report.get("status")
    return summary


def _increment_count(counts: dict[str, int], value: object) -> None:
    key = str(value or "none")
    counts[key] = counts.get(key, 0) + 1


def build_unknown_pending_queue_items_report(
    report: dict[str, object],
    *,
    summary_only: bool = False,
    sample_limit: int = 5,
) -> dict[str, object]:
    queue_items = report.get("queue_items", [])
    unknown_items = [
        item
        for item in queue_items
        if isinstance(item, dict)
        and item.get("relevance_bucket") == "unknown"
        and item.get("status") == "pending"
    ]
    counts_by_reason: dict[str, int] = {}
    counts_by_entity_type: dict[str, int] = {}
    counts_by_status: dict[str, int] = {}
    counts_by_unknown_reason: dict[str, int] = {}
    for item in unknown_items:
        _increment_count(counts_by_reason, item.get("reason"))
        _increment_count(counts_by_entity_type, item.get("entity_type"))
        _increment_count(counts_by_status, item.get("status"))
        _increment_count(counts_by_unknown_reason, item.get("unknown_reason"))

    payload: dict[str, object] = {
        "ok": report.get("ok"),
        "mode": "read_only",
        "performed_action": "none",
        "unknown_pending_queue_item_count": len(unknown_items),
        "counts_by_reason": counts_by_reason,
        "counts_by_entity_type": counts_by_entity_type,
        "counts_by_status": counts_by_status,
        "counts_by_unknown_reason": counts_by_unknown_reason,
    }
    if summary_only:
        payload["sample_limit"] = int(sample_limit)
        payload["sample_items"] = unknown_items[:sample_limit]
    else:
        payload["queue_items"] = unknown_items
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect the Spotify catalog queue without mutating it.")
    parser.add_argument(
        "--resolution-evidence-report",
        action="store_true",
        help="Classify queued work against source-track resolution evidence needs.",
    )
    parser.add_argument(
        "--export-json",
        help="Write the current classified queue snapshot to this JSON path.",
    )
    parser.add_argument(
        "--dry-run-resolution-evidence-plan",
        action="store_true",
        help="Include the dry-run focused resolution-evidence append plan. No queue rows are changed.",
    )
    parser.add_argument(
        "--append-resolution-evidence-candidate-tracklists",
        action="store_true",
        help="Plan or apply append-only candidate album tracklist queue rows from the focused resolution evidence plan.",
    )
    parser.add_argument(
        "--append-resolution-evidence-sibling-tracks",
        action="store_true",
        help="Plan or apply append-only sibling track metadata queue rows from the focused resolution evidence plan.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the append-only queue insert. Without this flag, append modes are dry-run only.",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Print compact JSON with high-signal counts only. Default output is unchanged.",
    )
    parser.add_argument(
        "--unknown-pending-queue-items",
        action="store_true",
        help="Read-only list of pending queue rows classified as unknown by the resolution-evidence report.",
    )
    parser.add_argument(
        "--album-metadata-display-gaps",
        action="store_true",
        help="Report tracks with Spotify album IDs whose local album display metadata is missing. Read-only.",
    )
    parser.add_argument(
        "--source-release-album-display-diagnostic",
        action="store_true",
        help="Report Source -> Release album display gap counts and samples. Read-only.",
    )
    parser.add_argument(
        "--album-display-diagnostic-summary-only",
        action="store_true",
        help="With --source-release-album-display-diagnostic, print compact Source -> Release album display counts only.",
    )
    parser.add_argument(
        "--source-release-album-display-enrichment-plan",
        action="store_true",
        help="Plan single-track metadata fetches for Source -> Release rows with no Spotify album evidence. Read-only.",
    )
    parser.add_argument(
        "--run-source-release-album-display-enrichment",
        action="store_true",
        help="Fetch single-track metadata for the Source -> Release album-display enrichment plan.",
    )
    parser.add_argument("--limit", type=int, default=25, help="Worker selection limit for enrichment runs.")
    parser.add_argument("--max-requests", type=int, default=40, help="Worker request cap for enrichment runs.")
    parser.add_argument(
        "--request-delay-seconds",
        type=float,
        default=1.5,
        help="Initial delay between Spotify single-track requests for enrichment runs.",
    )
    parser.add_argument("--market", type=str, default="US", help="Spotify market for enrichment single-track requests.")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="With --run-source-release-album-display-enrichment, run bounded repeated enrichment batches.",
    )
    parser.add_argument(
        "--max-runtime-minutes",
        type=float,
        default=None,
        help="Maximum wall-clock runtime for --loop.",
    )
    parser.add_argument(
        "--between-runs-seconds",
        type=float,
        default=300.0,
        help="Sleep between clean loop batches.",
    )
    parser.add_argument(
        "--jsonl-output",
        type=str,
        default=None,
        help="Append one JSON object per enrichment loop batch to this path.",
    )
    parser.add_argument(
        "--repair-album-metadata-display-gaps",
        action="store_true",
        help="Populate missing basic spotify_album_catalog fields from stored track payload album objects. Dry-run unless --apply is set.",
    )
    parser.add_argument(
        "--nested-metadata-integrity",
        action="store_true",
        help="Report local-only nested Spotify metadata integrity guardrail counts. Read-only.",
    )
    args = parser.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if args.album_display_diagnostic_summary_only and not args.source_release_album_display_diagnostic:
        parser.error("--album-display-diagnostic-summary-only requires --source-release-album-display-diagnostic.")
    if (
        args.loop
        or args.max_runtime_minutes is not None
        or args.jsonl_output
        or args.between_runs_seconds != 300.0
    ) and not args.run_source_release_album_display_enrichment:
        parser.error(
            "--loop, --max-runtime-minutes, --between-runs-seconds, and --jsonl-output require "
            "--run-source-release-album-display-enrichment."
        )
    if (
        not args.resolution_evidence_report
        and not args.unknown_pending_queue_items
        and not args.append_resolution_evidence_candidate_tracklists
        and not args.append_resolution_evidence_sibling_tracks
        and not args.album_metadata_display_gaps
        and not args.source_release_album_display_diagnostic
        and not args.source_release_album_display_enrichment_plan
        and not args.run_source_release_album_display_enrichment
        and not args.repair_album_metadata_display_gaps
        and not args.nested_metadata_integrity
    ):
        parser.error(
            "Pass --resolution-evidence-report, --unknown-pending-queue-items, "
            "--append-resolution-evidence-candidate-tracklists, --append-resolution-evidence-sibling-tracks, "
            "--album-metadata-display-gaps, --source-release-album-display-diagnostic, "
            "--source-release-album-display-enrichment-plan, --run-source-release-album-display-enrichment, "
            "--repair-album-metadata-display-gaps, or --nested-metadata-integrity."
        )

    ensure_sqlite_db()
    needs_resolution_report = bool(
        args.resolution_evidence_report
        or args.unknown_pending_queue_items
        or args.append_resolution_evidence_candidate_tracklists
        or args.append_resolution_evidence_sibling_tracks
    )
    report = inspect_spotify_catalog_queue_resolution_evidence() if needs_resolution_report else {"ok": True}
    source_release_album_display_diagnostic: dict[str, object] | None = None
    if args.source_release_album_display_diagnostic:
        source_release_album_display_diagnostic = inspect_source_release_album_display_gaps()
    source_release_album_display_enrichment_plan: dict[str, object] | None = None
    if args.source_release_album_display_enrichment_plan:
        source_release_album_display_enrichment_plan = plan_source_release_album_display_enrichment()
    source_release_album_display_enrichment_run: dict[str, object] | None = None
    if args.run_source_release_album_display_enrichment:
        token, error = _access_token()
        if error or not token:
            source_release_album_display_enrichment_run = {
                "ok": False,
                "status": "skipped_auth",
                "performed_action": "none",
                "error": error or "Spotify access token is unavailable.",
            }
        elif args.loop:
            source_release_album_display_enrichment_run = run_source_release_album_display_enrichment_loop(
                access_token=token,
                limit=args.limit,
                max_requests=args.max_requests,
                request_delay_seconds=args.request_delay_seconds,
                market=args.market,
                max_runtime_minutes=args.max_runtime_minutes,
                between_runs_seconds=args.between_runs_seconds,
                jsonl_output=args.jsonl_output,
                summary_only=bool(args.summary_only),
            )
        else:
            source_release_album_display_enrichment_run = run_source_release_album_display_enrichment_worker(
                access_token=token,
                limit=args.limit,
                max_requests=args.max_requests,
                request_delay_seconds=args.request_delay_seconds,
                market=args.market,
            )
    if args.album_metadata_display_gaps:
        report["album_metadata_display_gaps"] = inspect_spotify_album_metadata_display_gaps()
    if args.repair_album_metadata_display_gaps:
        report["repair_album_metadata_display_gaps"] = repair_spotify_album_basic_metadata_from_track_payloads(
            apply=bool(args.apply)
        )
    if args.nested_metadata_integrity:
        report["nested_metadata_integrity"] = inspect_spotify_nested_metadata_integrity()
    if args.export_json:
        written_path = write_queue_snapshot_export(report, args.export_json)
        report["snapshot_export"] = {
            "path": str(written_path),
            "performed_action": "wrote_snapshot_file",
        }
    if args.dry_run_resolution_evidence_plan:
        report["dry_run_resolution_evidence_plan"]["requested"] = True
    if args.append_resolution_evidence_candidate_tracklists:
        report["append_resolution_evidence_candidate_tracklists"] = append_resolution_evidence_candidate_tracklists_from_report(
            report=report,
            apply=bool(args.apply),
        )
    if args.append_resolution_evidence_sibling_tracks:
        report["append_resolution_evidence_sibling_tracks"] = append_resolution_evidence_sibling_tracks_from_report(
            report=report,
            apply=bool(args.apply),
        )
    elif args.apply:
        if not args.append_resolution_evidence_candidate_tracklists and not args.repair_album_metadata_display_gaps:
            parser.error(
                "--apply requires --append-resolution-evidence-candidate-tracklists "
                "or --append-resolution-evidence-sibling-tracks "
                "or --repair-album-metadata-display-gaps."
            )
    if args.unknown_pending_queue_items:
        output = build_unknown_pending_queue_items_report(report, summary_only=bool(args.summary_only))
    elif source_release_album_display_enrichment_run is not None:
        output = source_release_album_display_enrichment_run
    elif source_release_album_display_enrichment_plan is not None:
        output = source_release_album_display_enrichment_plan
    elif source_release_album_display_diagnostic is not None:
        output = (
            build_album_display_diagnostic_summary(source_release_album_display_diagnostic)
            if args.album_display_diagnostic_summary_only
            else source_release_album_display_diagnostic
        )
    else:
        output = build_summary_only_report(report) if args.summary_only else report
    print(json.dumps(output, ensure_ascii=True, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
