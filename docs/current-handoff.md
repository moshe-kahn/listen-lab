# Current Handoff

## Read First
For a new chat, start here, then read only the topic docs needed for the task.

Recommended docs:
- `docs/reference/drafts/entity-model-draft.md` for release/source/analysis identity, duplicate diagnostics, and merge preview/dry-run.
- `docs/reference/spotify-catalog-backfill.md` for catalog enrichment, queue behavior, lookup, and backfill invariants.
- `docs/reference/raw-ingest.md` for raw play events, recent/history ingest, and fallback history text.
- `docs/reference/drafts/identity-audit-submission-contract.md` only when working on saved track-audit submissions.
- `docs/reference/album-family-review-policy.md` only when working on album-family candidate review.

Avoid asking future agents to read every doc by default. The repo docs include historical and product planning material that is not always relevant.

## Current Active Area
Active branch: `ui-identity-audit-work`.

Current work is playback overlay / listen-event capture, plus existing source-track / album identity resolution evidence for Track Mapping and Spotify catalog metadata quality.

Session handoff update, 2026-05-14:
- User asked `end session`; this handoff was updated before closing.
- Branch remains `ui-identity-audit-work`.
- Working tree is dirty and broad. Do not assume all uncommitted changes belong together.
- No backend, Vite frontend, or catalog worker process was left running at handoff time.
- Keep identity resolution read-only unless the user explicitly asks for an apply/confirm/promote path.

Latest Source -> Release album-display enrichment state:
- User ran repeated live Source -> Release album-display enrichment batches with:
  `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --run-source-release-album-display-enrichment --loop --max-runtime-minutes 60 --between-runs-seconds 200 --limit 50 --max-requests 50 --request-delay-seconds 4 --summary-only --jsonl-output backend/data/logs/source-release-album-display-enrichment-loop.jsonl`
- Final worker output stopped with `stop_reason="no_selected_candidates"`, `total_requests=0`, `total_429=0`, `total_errors=0`.
- Read-only planner now reports:
  - `total_source_release_rows`: `3597`
  - `rows_with_no_spotify_album_evidence`: `0`
  - `distinct_track_spotify_ids_needing_metadata`: `0`
  - `eligible_to_fetch`: `0`
  - `blocked_or_invalid`: `0`
- Compact diagnostic now reports:
  - `total_rows`: `3597`
  - `rows_with_source_album_display_info`: `3597`
  - `rows_with_source_album_display_after_embedded_fallback`: `3597`
  - `rows_with_no_spotify_album_evidence`: `0`
  - `rows_with_album_spotify_id_but_no_local_album_name`: `0`
  - `rows_with_no_album_spotify_id`: `0`
- Broader Source -> Release metadata check via `search_track_mapping_lineage(mapping_kind='source_release', source_metadata='incomplete', limit=10)` returned `0` groups.
- Interpretation: the Source -> Release album-display metadata/evidence gap is complete. This does not mean source/release identity mappings have been confirmed or applied; it only means the evidence needed for review is now present.

Latest Track Mapping speed / loading work:
- Track Mapping query slowdown was from backend query shape, not the browser.
- Added indexes in `backend/app/db.py` for Track Mapping access patterns:
  - `idx_source_track_map_status_release_source`
  - `idx_source_track_map_status_source_release`
  - `idx_raw_play_event_spotify_track_id`
- Reworked `search_track_mapping_lineage(...)` in `backend/app/spotify_catalog_backfill.py`:
  - added `mapping_kind`, defaulting to `source_release`
  - avoids exact full counts for Track Mapping list views
  - uses bounded page-plus-one totals with `total_is_exact=false`
  - skips release-family loading unless requested
  - skips source-release loading when `mapping_kind='release_family'`
- Frontend Track Mapping now defaults to Source to release, requests 10 groups initially, and has a 20s fetch timeout so loading cannot spin forever silently.
- Clean backend/proxy verification after clearing DB lock:
  - `http://127.0.0.1:8000/debug/search/tracks/lineage?...mapping_kind=source_release...` returned HTTP 200 in about `1.1s`
  - `http://127.0.0.1:5173/api/debug/search/tracks/lineage?...mapping_kind=source_release...` returned HTTP 200 in about `1.0s`

Latest DB-lock diagnosis and safeguard:
- Track Mapping appeared to load forever because a stale Uvicorn process (`PID 34603`) held `backend/data/listenlabs.sqlite3` open.
- New backend startup hit `sqlite3.OperationalError: database is locked` in `ensure_sqlite_db()` and API requests hung/failed behind the frontend.
- Stale process was force-killed; `lsof backend/data/listenlabs.sqlite3` then showed no DB holder.
- Added safer startup behavior:
  - `ensure_sqlite_db()` now checks existing initialized DBs read-only and skips the schema initialization write path when `schema_version` already exists.
  - startup catches `sqlite3.OperationalError` containing `database is locked`, logs `event=sqlite_startup_locked`, and raises a clearer RuntimeError with the DB path and stale-process hint.
- Recommended future diagnosis:
  - `lsof backend/data/listenlabs.sqlite3`
  - `ps -axo pid,ppid,stat,etime,command | rg 'uvicorn|inspect_spotify_catalog_queue|python'`
- Prefer running Uvicorn without `--reload` while doing DB-heavy worker runs, because reload creates parent/child processes and made cleanup messier.

Latest verification:
- `./.venv/bin/python -m py_compile backend/app/db.py backend/app/main.py` passed.
- `./.venv/bin/python -m py_compile backend/app/spotify_catalog_backfill.py backend/app/main.py` passed.
- `cd frontend && npm run build` passed after Track Mapping timeout/page-size changes.
- `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --source-release-album-display-enrichment-plan` passed and reported zero candidates.
- `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --source-release-album-display-diagnostic --album-display-diagnostic-summary-only` passed and reported all `3597` rows have source album display info.

Recommended next task:
- Review and group the broad dirty diff before committing. Likely split at least:
  1. Track Mapping/catalog evidence/query performance/DB-lock safeguards.
  2. Playback overlay / ListenLab player listen-event capture work.
  3. Docs/data/log artifacts as appropriate.
- If continuing identity work, next logical step is a read-only review/confirmation design for Source -> Release mappings now that album evidence is complete. Do not apply identity mappings without a new explicit request.

Recommended resume prompt:
Continue in `/Users/kahntra/Documents/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. The Source -> Release album-display enrichment queue is complete (`eligible_to_fetch=0`, all `3597` rows have source album display info). Track Mapping loading was fixed by query/index changes plus a frontend timeout, and a stale Uvicorn DB lock was diagnosed and guarded. Review the broad dirty diff and split commit scopes intentionally; keep identity resolution read-only unless explicitly asked to apply/confirm mappings.

Session handoff update, 2026-05-14:
- User asked `end session`; Codex initially missed this handoff update because `AGENTS.md` had not been read. This entry repairs that missed step.
- Branch remains `ui-identity-audit-work`.
- Working tree is dirty and broad. Do not assume all uncommitted changes belong to the playback work.
- Pre-existing dirty work remains in catalog/identity files, docs, scripts, and data. Review before committing.

Latest playback overlay / listen-event work:
- Added ListenLab player playback as a third raw/canonical play source:
  - new raw source/table: `raw_listenlab_player_play`
  - new fact link table: `fact_play_event_player_link`
  - canonical view `v_fact_play_event_with_sources` now includes player source IDs and match metadata
- Added backend endpoint:
  - `POST /auth/player-listen-event`
  - creates a `listenlab_player` ingest run/event when the in-app player starts a song
  - updates the same event with progress while playback continues or pauses
- Updated canonical projector:
  - `listenlab_player` events get fact rows immediately
  - later Spotify recently-played rows merge with matching player events for the same track within the matching window
  - Spotify recent/history timing still takes precedence when available
- Updated playback overlay:
  - left rail shows recently played songs
  - repeated songs are deduped for display, keeping the most recent occurrence
  - rail backfills from the listening log to maintain up to 50 unique display candidates when duplicates collapse
  - only about 4 rows are visible at once; the rail scrolls
  - header no longer shows a numeric count
  - footer button text is `complete listen log` and opens the Listen Log page
  - rows show a completion/progress bar
  - right rail shows Spotify queue from `/v1/me/player/queue`
  - center remains current playback controls
- Frontend queue/recent UI is a presentation layer; real authenticated Spotify state was not visually verified because the local browser session was not authenticated.

Latest verification:
- `cd frontend && npm run build` passed after playback overlay/recent/queue work.
- `./.venv/bin/python -m unittest backend.tests.test_play_event_projection backend.tests.test_recent_tracks_route_boundary` passed.
- `pytest` is not installed in the local venv; use unittest unless dependencies are installed.

Recommended resume prompt:
Continue in `/Users/kahntra/Documents/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Review the dirty diff before editing. Current playback work added ListenLab player listen events, merging with Spotify recent, recent-rail dedupe/backfill, progress bars, a `complete listen log` button, and a Spotify queue rail. Verify with authenticated Spotify if possible, then decide whether to refine UI sizing or commit the playback scope separately from pre-existing catalog/identity changes.

Session handoff update, 2026-05-09:
- User asked `end session`; this handoff was updated before closing.
- Branch remains `ui-identity-audit-work`.
- Working tree is still dirty and broad. Do not assume all uncommitted changes are one commit.
- Latest work focused on Source -> Release album display gaps and resolution-evidence queue inspection output/diagnostics.
- No merge/apply/promote/identity mutation behavior was added.
- Live Spotify calls were run only for the explicitly requested Source -> Release album-display enrichment verification/canary:
  - one-shot: `limit=1`, `max_requests=1`, fetched `1`, album evidence added `1`, `requests_429=0`, `error_count=0`
  - loop canary: `limit=5`, `max_requests=5`, `request_delay_seconds=3`, `max_runtime_minutes=1`, fetched `20`, album evidence added `20`, `requests_429=0`, `error_count=0`, stopped `max_runtime`
  - JSONL written to `backend/data/logs/source-release-album-display-enrichment-loop-test.jsonl` with `4` batch rows
- Sandbox blocked token refresh before network approval; the same command succeeded after escalation.

Latest inspect script / resolution-evidence queue output work:
- Added `--summary-only` compact JSON for `backend/scripts/inspect_spotify_catalog_queue.py` while preserving default verbose output.
- Added `--unknown-pending-queue-items` read-only inspect mode for rows classified as unknown/pending by the resolution-evidence classifier.
- Reclassified exact legacy rows `entity_type='album'` + `reason='album_lookup_visible_incomplete'` as `generic_catalog_backfill`, not `unknown`.
  - Before: `unknown_pending=32`, safety action `needs_manual_review`.
  - After: `unknown_pending=0`, `generic_catalog_backfill_pending=32`, safety action `preserve_current_queue`.
- Fixed CLI validation so append modes are valid without also passing `--resolution-evidence-report`:
  - `--append-resolution-evidence-candidate-tracklists`
  - `--append-resolution-evidence-sibling-tracks`
- Append modes remain dry-run unless `--apply` is passed.
- Added sibling-append appendability diagnostics in summary output:
  - `appendability_diagnostic.source_counts`
  - `actual_sibling_track_items_by_plan_status`
  - `append_exclusion_counts`
  - `broad_delta_not_in_focused_append_plan_count`
  - annotated samples for `sibling_tracks_requiring_metadata` and `sibling_tracks_missing_from_queue`
- Root cause for sibling append showing zero candidates:
  - broad delta reported `sibling_tracks_requiring_metadata_count=10` and `sibling_tracks_missing_from_queue_count=62`
  - append selection reads only `dry_run_resolution_evidence_plan.actual_sibling_track_items` with `plan_status='should_append_later'`
  - current local `actual_sibling_track_items_count=0`, so `candidate_count=0` is expected
  - the 10 missing-metadata samples are outside the focused append plan; many missing-queue samples already have complete metadata

Latest Source -> Release album-display enrichment loop work:
- Added loop mode to `backend/scripts/inspect_spotify_catalog_queue.py` for only `--run-source-release-album-display-enrichment`.
- New flags:
  - `--loop`
  - `--max-runtime-minutes`
  - `--between-runs-seconds`
  - `--jsonl-output`
- Loop uses the existing `run_source_release_album_display_enrichment_worker(...)` path directly.
- It does not use the generic track metadata worker path.
- Loop stop conditions:
  - max runtime reached
  - selected count is `0`
  - processed count is `0`
  - `cooldown_until` is non-null
  - `requests_429 > 0`
  - `error_count > 0`
- Loop summary includes totals:
  - `total_batches`
  - `total_processed_count`
  - `total_fetched_track_metadata`
  - `total_fetched_and_album_evidence_added`
  - `total_requests`
  - `total_429`
  - `total_errors`
  - `stop_reason`
  - `elapsed_seconds`
  - `jsonl_output`
- In loop `--summary-only`, selected track ID lists are omitted from final stdout, but per-batch JSONL still includes full batch details.

Latest completed Source -> Release album-display work:
- Source -> Release endpoint remains `GET /debug/search/tracks/lineage`, implemented in `backend/app/spotify_catalog_backfill.py` via `search_track_mapping_lineage(...)`.
- Frontend rendering is in `frontend/src/App.tsx` under Track Mapping / Source tracks into one release track.
- Added display-only fallback fields to source rows:
  - `album_name_display`
  - `album_name_display_source`
  - `embedded_album_name`
  - `source_album_name`
- Fallback order for source-row album display:
  1. `spotify_album_catalog.name`
  2. embedded `spotify_track_catalog.raw_json.album.name`
  3. matching `source_album.source_name_raw`
  4. UI fallback `Unknown album`
- Album ID display also falls back from `spotify_track_catalog.album_id` to embedded `raw_json.album.id`.
- Release date and total tracks fall back from album catalog to embedded track album payload.
- Fallback is display-only. It does not mutate catalog rows or identity mappings.

Latest Source -> Release album-display diagnostics:
- Added read-only diagnostic endpoint:
  - `GET /debug/search/tracks/lineage/album-display-diagnostic`
- Added script flags in `backend/scripts/inspect_spotify_catalog_queue.py`:
  - verbose diagnostic: `--source-release-album-display-diagnostic`
  - compact summary: `--source-release-album-display-diagnostic --album-display-diagnostic-summary-only`
- Current compact command:
  `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --source-release-album-display-diagnostic --album-display-diagnostic-summary-only`
- Latest local compact output:
  - `total_rows`: `3597`
  - `rows_with_source_album_display_info`: `1571`
  - `rows_with_source_album_display_after_embedded_fallback`: `1571`
  - `rows_with_no_spotify_album_evidence`: `2026`
  - `rows_with_album_spotify_id_but_no_local_album_name`: `0`
  - `rows_with_no_album_spotify_id`: `2026`
- Interpretation: fallback is working. Remaining Source -> Release blanks are truly missing source-side Spotify track metadata, not a UI fallback or missing album join.

Latest Source -> Release album-display enrichment planner/worker:
- Added read-only planner:
  `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --source-release-album-display-enrichment-plan`
- Latest local planner output:
  - `total_source_release_rows`: `3597`
  - `rows_with_no_spotify_album_evidence`: `2026`
  - `distinct_track_spotify_ids_needing_metadata`: `2026`
  - `eligible_to_fetch`: `2026`
  - `blocked_or_invalid`: `0`
- Planner selects distinct Spotify source track IDs from Source -> Release rows where:
  - source album display is missing
  - `spotify_track_catalog.album_id` is null/blank
  - embedded `spotify_track_catalog.raw_json.album.id/name` is missing
  - source Spotify track ID exists
- Added bounded worker mode:
  `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --run-source-release-album-display-enrichment --limit 50 --max-requests 60 --request-delay-seconds 5.0`
- Worker uses existing single-track `GET /v1/tracks/{id}` fetch/upsert logic only.
- Worker does not use Spotify batch endpoints.
- Worker respects bounded `limit`, `max_requests`, request delay, active `spotify_track_metadata` cooldown, and first-429 stop behavior.
- Existing `run_spotify_resolution_track_metadata_worker` behavior remains unchanged; tests confirm it still ignores source-release-only album display gaps.
- Do not run the worker unless the user explicitly asks to fetch metadata. If any 429 appears, stop and respect cooldown.
- Added bounded loop mode for this same worker path:
  `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --run-source-release-album-display-enrichment --loop --max-runtime-minutes 1 --between-runs-seconds 5 --limit 5 --max-requests 5 --request-delay-seconds 3 --summary-only --jsonl-output backend/data/logs/source-release-album-display-enrichment-loop-test.jsonl`
- The short loop canary succeeded with `20` tracks fetched and album evidence added, `0` 429s, `0` errors.

Recommended next task:
- Decide whether to continue Source -> Release album-display enrichment with the new loop mode, or pause live fetching and review the remaining gaps first.
- If continuing live fetch, use conservative settings and stop on any 429:
  `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --run-source-release-album-display-enrichment --loop --max-runtime-minutes 30 --between-runs-seconds 30 --limit 25 --max-requests 25 --request-delay-seconds 3 --summary-only --jsonl-output backend/data/logs/source-release-album-display-enrichment-loop.jsonl`
- Keep identity resolution read-only; do not apply merge/promote/confirm mappings without a new explicit request.

Session handoff update, 2026-05-08:
- User asked `end session`; update this handoff before closing.
- Branch remains `ui-identity-audit-work`.
- There is no separate local feature branch to join with; local branches visible were `main` and `ui-identity-audit-work`, remote branch visible was `origin/main`.
- Working tree is dirty and broad. Do not assume all uncommitted changes belong to one commit without reviewing.
- Current uncommitted scope includes source-track resolution policy/report/planner/worker work, nested Spotify metadata persistence guardrails, related tests/docs, and unrelated-looking frontend/UI edits.
- Before committing, inspect and group changes intentionally. Likely split backend resolution-evidence/catalog guardrails from UI work if practical.

Immediate next task:
- Review the uncommitted diff and decide commit grouping.
- If committing, first produce a staged-ready summary and proposed commit message; do not commit unless user explicitly confirms.
- Keep identity resolution behavior safe: no merge/apply/promote/confirm identity mappings unless explicitly requested and reviewed.

Current intent:
- Finish resolving multiple `source_track` Spotify IDs into one `release_track`.
- Do not collapse source rows; different Spotify IDs should remain separate `source_track` rows.
- Resolution likely means confirming or correcting `source_track_map` relationships, not deleting source identity.
- No apply/merge/promote endpoint exists for this UI work yet.

Latest completed source-track resolution evidence work:
- Created read-only source-track resolution policy/checklist:
  - `docs/reference/source-track-resolution-policy.md`
- Added read-only Spotify catalog queue inspection and focused resolution-evidence planner:
  - `backend/scripts/inspect_spotify_catalog_queue.py`
  - command: `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --resolution-evidence-report --dry-run-resolution-evidence-plan`
- Added append-only apply paths, dry-run by default and `--apply` required:
  - candidate album tracklists: `--append-resolution-evidence-candidate-tracklists`
  - sibling track metadata: `--append-resolution-evidence-sibling-tracks`
- Added focused album-tracklist worker:
  - `backend/scripts/run_spotify_album_tracklist_resolution_worker.py`
  - selects focused planner `candidate_album_tracklist_items`, not generic album backlog
  - fetches selected parent album metadata when needed, then selected album tracklists
  - marks done only after local tracklist verification
- Added focused resolution track metadata worker:
  - `backend/scripts/run_spotify_resolution_track_metadata_worker.py`
  - selects only pending `resolution_evidence` track metadata queue rows
  - no album fetches, no album tracklists, no sibling enqueueing
- Added reset-cooldown support to the existing generic worker for explicit real runs:
  - correct flag spelling is `--reset-cooldown`; do not use a Unicode em dash.
  - reset cooldown should not be needed for dry-run.

Latest Spotify nested metadata guardrail work:
- Root cause found: existing track metadata payloads had embedded `track.album` data, but basic album display fields had not been populated for many already-stored rows.
- Local repair reduced `tracks_with_album_spotify_id_missing_local_album_name` from `3136` to `2`; remaining `2` have empty embedded album names and need full album fetch later.
- Track metadata upsert now persists basic embedded `track.album` display fields for future fetches:
  - album Spotify ID, name, album type, release date, release date precision, total tracks, images, album artists when supported by existing schema.
- Existing fuller album fields must not be overwritten by simplified track payloads:
  - label, copyrights, external IDs / UPC / EAN, stronger non-null album metadata.
- Added local-only diagnostics/repair:
  - `--album-metadata-display-gaps`
  - `--repair-album-metadata-display-gaps`, dry-run by default and `--apply` required.
- Added nested metadata integrity diagnostic:
  - `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --nested-metadata-integrity`
  - reports album display gaps, artist JSON ID/name gaps, and done queue rows with incomplete local metadata.
- Centralized completion verification now prevents track metadata rows from being treated as complete when embedded album names exist but local album display names were not persisted.
- Worker summaries include local integrity output and warn if a track metadata run increases local album-display gaps after marking rows done.
- No dedicated Spotify artist catalog/link table was found in this source-catalog layer, so artist guardrails currently use existing JSON evidence (`artists_json`) rather than schema changes.

Latest targeted verification:
- `./.venv/bin/python -m unittest backend.tests.test_spotify_catalog_backfill.SpotifyCatalogBackfillTests.test_track_metadata_upsert_persists_embedded_album_basic_fields_without_full_metadata backend.tests.test_spotify_catalog_backfill.SpotifyCatalogBackfillTests.test_track_metadata_upsert_does_not_overwrite_full_album_metadata_with_simplified_album backend.tests.test_spotify_catalog_backfill.SpotifyCatalogBackfillTests.test_album_metadata_display_gap_diagnostic_is_read_only backend.tests.test_spotify_catalog_backfill.SpotifyCatalogBackfillTests.test_repair_album_metadata_display_gaps_populates_from_stored_track_payloads backend.tests.test_spotify_catalog_backfill.SpotifyCatalogBackfillTests.test_nested_metadata_integrity_reports_incomplete_done_queue_rows backend.tests.test_spotify_catalog_backfill.SpotifyCatalogBackfillTests.test_resolution_track_metadata_worker_processes_only_focused_tracks_and_marks_done backend.tests.test_spotify_catalog_backfill.SpotifyCatalogBackfillTests.test_resolution_track_metadata_worker_warns_if_local_album_display_gaps_increase`
- `./.venv/bin/python -m unittest backend.tests.test_spotify_catalog_backfill.SpotifyCatalogBackfillTests.test_resolution_track_metadata_worker_dry_run_selects_only_focused_sibling_rows backend.tests.test_spotify_catalog_backfill.SpotifyCatalogBackfillTests.test_resolution_track_metadata_worker_does_not_mark_done_when_metadata_incomplete backend.tests.test_spotify_catalog_backfill.SpotifyCatalogBackfillTests.test_resolution_track_metadata_worker_stops_on_429_and_leaves_pending`
- `./.venv/bin/python -m py_compile backend/app/spotify_catalog_backfill.py backend/scripts/inspect_spotify_catalog_queue.py backend/scripts/run_spotify_resolution_track_metadata_worker.py backend/tests/test_spotify_catalog_backfill.py`
- All passed.

Latest explicit safety confirmations:
- No live Spotify calls during tests/guardrail work.
- No queue clearing or replacement.
- No generic backfill run.
- No schema migration.
- No sibling-track enqueueing during workers unless using the explicit append-only planner apply path for sibling track metadata.
- No identity merge/apply/promote behavior.

Latest completed work in this branch:
- Committed previous backend/readiness/backfill work on `main`:
  - `5053bea Add identity audit readiness and priority metadata controls`
- Created branch `ui-identity-audit-work`.
- Added read-only Search / Lookup tab: `Track Mapping`.
- Added backend endpoint:
  - `GET /debug/search/tracks/lineage`
  - function: `search_track_mapping_lineage(...)`
- Track Mapping now shows:
  - source tracks -> one release track
  - release tracks -> one track family (`analysis_track`)
  - filters for mapping type and confirmation state
  - source maps included: `accepted`
  - family maps included: `accepted`, `suggested`
  - current DB has `0` user-confirmed rows in both `source_track_map` and `analysis_track_map`
- Source->release UI was redesigned after it looked strange:
  - album sections
  - compact track groups
  - compact source rows
  - evidence columns for source, album, stats, and mapping state
  - no nested tables/cards in the source->release view
- Source rows now show:
  - clickable Spotify track ID
  - clickable Spotify album ID when available
  - catalog track name
  - album name, release date, album ID, copyright
  - disc/track position as `disc.track / total_tracks` (example: `1.3 / 11`)
  - duration, play count, mapping method, confidence, confirmed/unconfirmed

Current source->release metadata state:
- There are `1706` release-track groups with multiple accepted Spotify source tracks.
- Those groups contain `3597` distinct Spotify source-track IDs.
- After recent metadata fetches, latest checked counts:
  - `has_metadata`: `1502`
  - `missing_or_error_metadata`: `2095`
- The user ran:
  - `caffeinate -imsu -- ./.venv/bin/python -m backend.scripts.run_spotify_track_metadata_worker --loop --max-runtime-minutes 120 --between-runs-seconds 300 --jsonl-output backend/data/logs/track-metadata-worker-loop.jsonl`
  - observed runs `67` through `72` complete cleanly, and run `73` fetched 12 before Ctrl-C
  - all displayed runs had `0` 429s
  - this reduced source->release missing metadata from `2312` to `2095`
- Earlier in-chat manual runs:
  - Run `64`: tracks fetched `50`, requests `51`, 429 `0`
  - Run `65`: tracks fetched `50`, requests `51`, 429 `0`
  - Run `66`: album metadata fetched `9`, requests `10`, 429 `0`, partial only because `max_requests`

Bookshelf / jizue finding:
- User focused on `Bookshelf` by `jizue`.
- Local evidence now confirms the fetched `Bookshelf` album IDs:
  - `2jzlE2zx3ucZJb2NPkmSRV`
  - `3vWw8xYPwpYzlvK253yLdQ`
  - `70v7OcDOYpKVOVxSYXQXjm`
- All three resolve to album name `Bookshelf`, release date `2016-05-25`, `11` tracks, bud music copyright.
- For `Intro`, `Island`, `Sister`, `Tower`, missing alternate Spotify track metadata was fetched.
- Evidence for those alternates now shows same duration and same disc/track position across album versions.
- User noted patterns:
  - same label/length
  - capitalization differences (`Intro` vs `intro`, `Sister` vs `sister`)
  - one song appears sometimes in Japanese and sometimes English transliteration/title form
- Next session should capture all manual review rules/observations from the user before implementing anything.

Important invariants:
- Identity Audit / Track Mapping remains read-only.
- Catalog backfill is enrichment-only.
- Do not apply, merge, promote, delete, or mutate identity mappings without an explicit new request and reviewed safety plan.
- Duplicate diagnostics must not call Spotify.
- Merge preview/dry-run must not write.
- `analysis_track_map` must not mutate in release-album preview/dry-run paths.

Latest completed work:
- Added read-only track Identity Audit readiness report:
  - `GET /debug/tracks/identity-audit/readiness`
  - builder: `build_track_identity_readiness_report(...)`
  - shared CTE helper: `track_identity_readiness_source_ctes()`
- Report buckets duplicate/split track cases into:
  - `metadata-complete`
  - `evidence-agrees`
  - `blocked-by-missing-metadata`
  - `safe-candidate`
  - `needs-review`
  - `unsafe`
- Current local DB readiness numbers from latest run:
  - blocked duplicate/split groups: `1706`
  - distinct Spotify source tracks in blocked groups: `3597`
  - tracks missing track metadata: `2463`
  - those included by default `identity_and_top_listened` priority scope: `2463`
  - not included by priority scope: `0`
  - album-metadata-only blockers: `1045`
- Added read-only priority validation debug under catalog coverage:
  - `track_metadata_priority.identity_readiness_blockers`
  - helper: `get_identity_readiness_track_metadata_priority_comparison(...)`
- Priority selector now evaluates identity relevance across all accepted Spotify source tracks in duplicate/split cases, then dedupes by Spotify ID.
  - This fixed/guards the non-representative split case where one source track was complete and another same-release-track source was still missing metadata.
  - Deferred singleton backlog remains excluded from default `identity_metadata` selection.
- No Spotify calls, Full Backfill, merge/apply, or identity mapping mutations were added.

Clarified identity model:
- `source_track` remains source/provider identity: one Spotify ID per row.
- Different Spotify IDs that are likely the same real track should generally remain separate source rows and map to the same `release_track`, not collapse into one `source_track`.
- `analysis_track` is current schema name for the higher analytics/song-family-ish grouping layer; product wording may call this Track Family later.
- Current metadata evidence helps readiness/ranking only. There is no automatic merge/apply from readiness.
- Spotify `popularity` is not currently available in local fetched track payloads:
  - local `spotify_track_catalog` rows inspected: `2844`
  - rows with `raw_json.popularity`: `0`
  - sample raw keys: `album`, `artists`, `disc_number`, `duration_ms`, `explicit`, `external_ids`, `external_urls`, `href`, `id`, `is_local`, `is_playable`, `name`, `track_number`, `type`, `uri`

Current Catalog Backfill UI structure:
- `Overview`
- `Priority Metadata`
- `Full Backfill`
- `Queue`
- `Recent Runs`

`Priority Metadata` is the default active workflow.

Priority Metadata:
- purpose: fetch source-layer Spotify metadata needed for identity decisions
- default mixed run config:
  - `run_mode = "metadata_only"`
  - `include_albums = true`
  - `album_tracklist_policy = "none"`
  - `reason = "identity_metadata"`
  - `priority_scope = "identity_and_top_listened"`
- new explicit targets:
  - `target = "tracks"` for true track metadata only
  - `target = "albums"` for true album metadata only
  - `target = "album_tracklists"` for explicit tracklist-only expansion modes
  - `target = "all"` for current mixed behavior
- fetches priority source-mapped Spotify tracks only: identity-ambiguous candidates or tracks connected to top listened tracks/albums/artists/recent repeats
- leaves other accepted Spotify source tracks visible as deferred catalog backlog
- fetches Spotify albums referenced by those tracks and existing accepted source album/raw album IDs
- does not fetch full album tracklists
- does not expand unlistened album tracks

Full Backfill:
- purpose: slower catalog expansion / tracklist completion
- run modes:
  - `tracklists_relevant`
  - `full_catalog`
- tracklist policies:
  - `relevant_albums`
  - `priority_only`
  - `all`
- reasons:
  - `tracklist_completion`
  - `full_backfill`

Queue:
- still uses `entity_type = track | album`
- `reason` now distinguishes:
  - `identity_metadata`
  - `manual_priority`
  - `tracklist_completion`
  - `full_backfill`
- pending queue processing prioritizes `identity_metadata` first
- queue API supports reason filtering and returns reason counts

Coverage:
- coverage now separates `identity_critical` from `catalog_expansion`
- identity-critical examples:
  - missing source track metadata
  - missing source album metadata
  - missing track ISRC
  - missing track duration
  - missing album release date
  - missing album external IDs
- catalog-expansion examples:
  - missing album tracklists
  - relevant album tracklist backlog
  - unlistened tracklist rows

Lookup/Search:
- lookup remains useful for visibility and manual priority enqueue
- it is no longer the main operational controller; execution/status belongs on Catalog Backfill

Current Identity Audit UI structure:
- First-level selector: `Tracks`, `Albums`, `Artists`.
- `Tracks` keeps the existing workflow: overview, canonical splits, release track joins/splits, composition groups, family/ambiguous review.
- `Albums` owns release-album duplicate review.
- `Artists` is a placeholder only.

Current Albums diagnostics:
- Duplicate Albums by same resolved Spotify album ID.
- Duplicate Albums by normalized album name + normalized primary artist.

Current Tracks release diagnostics:
- Duplicate Tracks by same resolved Spotify track ID.
- Release Track Split Signals.

All duplicate diagnostics are read-only.

## Current Merge Tooling
Read-only release album merge tooling exists:
- `POST /debug/identity/release-albums/merge-preview`
- `POST /debug/identity/release-albums/merge-dry-run`

Preview:
- chooses a deterministic survivor
- returns warnings and affected counts
- classifies readiness as `safe_candidate`, `needs_review`, or `unsafe`

Dry run:
- reuses preview/readiness
- blocks `unsafe`
- blocks survivor mismatch
- allows `safe_candidate` and `needs_review`
- returns exact row-level plan sections

No apply/merge endpoint exists yet.

Important schema rule:
- `release_track` has no `release_album_id`
- album membership lives in `album_track`
- album merge would repoint `album_track.release_album_id`
- `release_track` rows are not changed directly

## Current Invariants
- Catalog backfill is enrichment-only.
- Catalog backfill must not mutate identity or analysis mapping tables.
- Duplicate diagnostics must not call Spotify.
- Merge preview and dry-run must not write.
- `analysis_track_map` must not mutate in release-album preview/dry-run paths.
- Spotify catalog tables are metadata evidence, not merge decisions by themselves.
- Album bulk preview in the frontend is session-only state and still local/read-only.

## Album Review UI Current State
The frontend now has a more usable album review flow under Identity Audit -> Albums.

Album tabs:
- `Overview`
- `Duplicate Spotify IDs`
- `Duplicate Name + Artist`
- `Merge Review`

Duplicate Spotify IDs:
- Shows album review cards instead of dense raw tables.
- Has preview-state filters: `All`, `Not previewed`, `Safe candidate`, `Needs review`, `Unsafe`.
- Has reason filters after preview, e.g. `Clean safety checks`, `Album-track conflicts`, `Name mismatch`.
- Has `Preview listed groups` for session-only bulk preview.
- Spotify album IDs/names link to Spotify when an ID exists.

Duplicate Name + Artist:
- Has subgroup filters: `All`, `Single Spotify ID`, `Multiple Spotify IDs`, `No Spotify ID`.
- Has the same preview-state and reason filters.
- Also supports `Preview listed groups`.

Cards show:
- album name
- primary artist
- Spotify album ID when relevant
- duplicate count
- `release_album_ids`
- readiness after preview
- plain-English reason for the readiness category
- warning count/summary
- raw preview/dry-run JSON behind details
- row-level dry-run sections including `album_track_conflicts`

Representative observed cases:
- `Boys by Girls` -> `safe_candidate`: names/artists align, strong single Spotify album evidence, no album-track conflicts.
- `Alma's Cove` -> `needs_review`: same Spotify album identity but needs manual review, likely due to row-level album-track conflict or structural issue.
- `Runaway` / `Runaway (Deluxe)` -> `unsafe`: normalized album names differ.

## Recent Prevention Fix
Fallback/history text entity keys now normalize artist text with `_normalize_fallback_artist_text(...)`.

This prevents new fallback splits like:
- `Telekinesis`
- `Telekinesis, Telekinesis`

Scope:
- applies only to fallback/history text keys
- preserves raw artist text for display/raw fields
- does not affect Spotify-ID identity paths
- does not repair existing duplicate rows

## Catalog Backfill Current State
Catalog backfill supports:
- track catalog enrichment
- album catalog enrichment
- album tracklist enrichment
- one-shot local/dev track metadata worker
- explicit target modes:
  - `tracks`
  - `albums`
  - `album_tracklists`
  - `all`
- queue-first processing
- explicit run modes:
  - `metadata_only`
  - `tracklists_relevant`
  - `full_catalog`
- queue repair
- Overview, Priority Metadata, Full Backfill, Queue, and Recent Runs tabs in the frontend
- album tracklist policies: `all`, `relevant_albums`, `priority_only`, `none`
- queue reason filtering and identity-metadata-first processing

Recent backend behavior fixes:
- `metadata_only` queue processing only accepts `identity_metadata` queue rows.
- `metadata_only` uses metadata completeness, not album tracklist completeness.
- source-track metadata selection filters missing/incomplete candidates before `limit`.
- source-track metadata selection now prioritizes identity-relevant candidates before broad backlog:
  - duplicate resolved Spotify track groups
  - release-track source splits
  - suggested analysis groups
  - then track listen count, artist listen count, accepted mapping, Spotify ID
- `target=tracks` is true tracks-only, even if `include_albums=true`.
- `target=albums` is true album metadata only.
- `target=album_tracklists` requires explicit tracklist mode/policy and is rejected with `metadata_only`.
- Spotify batch catalog endpoints are currently forbidden for this app/token path:
  - `GET /v1/tracks?ids=...` returns 403
  - `GET /v1/albums?ids=...` returns 403
  - single track/album endpoints work
- runner warns once per endpoint type and uses single-object fallback.
- first Spotify `429` stops the run as `partial` with `stop_reason=rate_limited`.
- omitted `request_delay_seconds` defaults to `2.0`; explicit caller values are still respected.
- `429` handling records valid `Retry-After`; Spotify has sometimes returned 429 without valid `Retry-After`.
- album metadata candidate selection now filters/prioritizes incomplete album metadata before applying `limit`.
- identity metadata track selection now defaults to `priority_scope=identity_and_top_listened`; broad accepted Spotify source-track backlog remains visible as deferred catalog backlog and is not selected by default.
- identity metadata track selection now covers all readiness-blocking duplicate/split source tracks, including non-representative accepted Spotify IDs on a split release track.
- coverage debug now reports readiness blocker priority comparison at `track_metadata_priority.identity_readiness_blockers`.

One-shot worker:
- module: `backend/app/spotify_catalog_worker.py`
- CLI: `python3 -m backend.scripts.run_spotify_track_metadata_worker`
- worker name: `spotify_track_metadata`
- current job config: `target=tracks`, `run_mode=metadata_only`, `reason=identity_metadata`, `priority_scope=identity_and_top_listened`, `album_tracklist_policy=none`, `include_albums=false`, `limit=50`, `max_requests=60`, `max_runtime_seconds=360`, `request_delay_seconds=5.0`, `market=US`
- persists `spotify_catalog_worker_state`, `spotify_catalog_worker_lock`, and `spotify_catalog_worker_invocation`
- skips on active cooldown, skips on non-stale overlap, replaces locks older than 2 hours, then exits after one bounded job
- rate-limit cooldown is valid `Retry-After` when present, otherwise 60 minutes
- has a local rolling request budget guard before Spotify calls:
  - counts `spotify_catalog_backfill_run.requests_total` for the last 60 minutes
  - skips with `skipped_request_budget` at `>=550` requests and sets 15 minute local cooldown
  - skips with 30 minute local cooldown at `>=650` requests
  - when below `550`, caps the next run's `max_requests` to the remaining soft budget and lowers `limit` accordingly
  - Spotify's published app-wide Web API rate limit is a rolling 30-second window with an app-specific quota; the local 60-minute budget is an extra conservative guard, not Spotify's documented quota window
  - real Spotify `429` cooldown remains separate and still uses valid `Retry-After`, otherwise 60 minutes
- has a post-cooldown canary gate:
  - runs only after an expired worker cooldown whose previous result stopped for `rate_limited`
  - selects one missing/incomplete priority source Spotify track via the existing priority missing-metadata selector
  - makes exactly one single-track `GET /v1/tracks/{id}` request with the same local token/client path
  - on success, upserts that track catalog row and continues the normal worker run
  - on 429, stops before normal backfill with `skipped_canary_rate_limited`, `stop_reason=post_cooldown_canary_429`
  - tracks `consecutive_post_cooldown_canary_429s` in `spotify_catalog_worker_state`
  - canary 429 fallback cooldown uses exponential backoff: 6 hours, then 12 hours, then capped at 24 hours

## Latest Verification
Commands run after readiness/priority validation work:
- `./.venv/bin/python -m unittest backend.tests.test_track_identity_audit_read_models backend.tests.test_track_identity_audit_routes`
- `./.venv/bin/python -m unittest backend.tests.test_spotify_catalog_worker`
- selector-focused `backend.tests.test_spotify_catalog_backfill` tests:
  - `test_priority_metadata_selects_identity_relevant_missing_tracks`
  - `test_priority_metadata_selects_top_listened_missing_tracks`
  - `test_priority_metadata_includes_nonrepresentative_readiness_blocking_split_track`
  - `test_priority_metadata_defers_missing_tracks_with_no_priority_flags`
  - `test_broad_metadata_scope_can_select_deferred_missing_tracks`
  - `test_catalog_backfill_coverage_counts`
- full catalog backfill module:
  - `./.venv/bin/python -m unittest backend.tests.test_spotify_catalog_backfill`
- py compile:
  - `./.venv/bin/python -m py_compile backend/app/track_identity_audit.py backend/app/spotify_catalog_backfill.py backend/app/main.py backend/app/spotify_catalog_worker.py backend/tests/test_track_identity_audit_read_models.py backend/tests/test_track_identity_audit_routes.py backend/tests/test_spotify_catalog_backfill.py backend/tests/test_spotify_catalog_worker.py`

All passed.

## Recommended Next Task
Stay read-only. Inspect representative-selection policy for duplicate/split Spotify track IDs:
- Current system does not choose representative Spotify ID from readiness metadata yet.
- Define a read-only preview/ranking policy before any mutation:
  - user listen count first
  - complete metadata
  - same ISRC/duration/name evidence
  - album type/release date rules
  - deterministic tie-break
- Do not use Spotify popularity unless future fetched payloads contain it.
  - Current local payloads do not.
  - if Spotify provides `Retry-After`, canary cooldown uses `max(retry_after_seconds, fallback_cooldown_seconds)`
  - canary 429 JSONL/result fields include `consecutive_post_cooldown_canary_429s`, `retry_after_seconds`, `fallback_cooldown_seconds`, `cooldown_until`, and `stop_reason=post_cooldown_canary_429`
  - resets `consecutive_post_cooldown_canary_429s` to `0` only after a successful canary followed by a normal successful/partial non-429 worker run
  - on non-429 API failure, stops before normal backfill with `skipped_canary_failed` and does not set metadata complete
  - if no candidate exists, emits `canary_skipped_no_candidate` and continues normally
  - JSONL/progress events: `canary_attempt`, `canary_success`, `canary_rate_limited`, `canary_failed_non_429`, `canary_skipped_no_candidate`
- CLI default remains one-shot and JSON-compatible.
- CLI loop mode exists:
  - `--loop`
  - `--max-runtime-minutes N`
  - `--between-runs-seconds N` default `300`
  - `--jsonl-output PATH`
  - loop mode terminal output is condensed human-readable lines
  - `--jsonl-output` appends full machine-readable JSONL event stream
  - loop respects worker cooldowns and `skipped_request_budget`; it does not bypass or recalculate them
- does not run Full Backfill, album metadata, album tracklists, Identity Audit, merge/apply, or any identity mutation

Current catalog coverage after Run 62:
- missing source track metadata: `27,506`
- measured missing accepted source track metadata: `25,617`
- priority missing source track metadata: `4,732`
- identity-ambiguous missing source track metadata: `1,815`
- top-listened missing source track metadata: `3,237`
- identity/top overlap: `320`
- deferred accepted source track metadata: `20,885`
- missing track ISRC: `27,508`
- missing track duration: `27,506`
- missing source album metadata: `0`
- missing album release date: `0`
- missing album UPC/EAN: `3`
- missing album tracklists: `228` separate catalog-expansion backlog
- relevant album tracklist backlog: `29`
- unlistened tracklist rows: `345`

Known remaining album UPC/EAN gaps are acceptable single/EP-like releases:
- `0cFaOb8C6eLR26DFx3vAVo` - `Yo-yo`, `album_type=single`, `total_tracks=1`
- `0iepUkNqUGDPxkKcC2Uwo8` - `Wax Poetry`, `album_type=single`, `total_tracks=3`
- `1wIX39AruT1zSwkSldKSQR` - `Chips & Dip`, `album_type=single`, `total_tracks=2`

Recent live runs:
- Run 35: `metadata_only`, `target=tracks`, `limit=150`, `request_delay_seconds=1.5`, status `ok`, tracks fetched/upserted `150/150`, albums `0/0`, requests `151`, 429 `0`.
- Run 36: `metadata_only`, `target=albums`, `limit=50`, status `ok`, albums fetched `26`, tracks `0`, tracklists `0`, 429 `0`.
- Run 37: `metadata_only`, `target=albums`, `limit=100`, status `ok`, albums fetched `41`, 429 `0`.
- Run 38: `metadata_only`, `target=albums`, `limit=150`, status `ok`, albums fetched `40`, 429 `0`.
- Run 39: `metadata_only`, `target=albums`, `limit=200`, status `ok`, albums fetched `39`, 429 `0`.
- Run 40: before album selector fix, `target=albums`, fetched 3 UPC/EAN-only albums and coverage did not improve.
- Run 41: after album selector fix, `metadata_only`, `target=albums`, `limit=50`, status `ok`, albums fetched `24`, source album metadata `21 -> 0`, release date `21 -> 0`, external IDs `24 -> 3`, has_more `false`, 429 `0`.
- Run 42: `metadata_only`, `target=tracks`, `limit=150`, `request_delay_seconds=1.5`, status `ok`, tracks fetched/upserted `150/150`, albums `0`, tracklists `0`, requests `151`, 429 `0`.
- Run 43: `metadata_only`, `target=tracks`, `limit=150`, `request_delay_seconds=1.5`, status `partial`, tracks fetched/upserted `8/8`, requests `12`, 429 `3`, stopped `rate_limited` before first-429 behavior was implemented.
- Run 44: one-shot worker canary, status `partial`, requests `2`, 429 `1`, fetched `0`; fallback cooldown because Spotify gave no valid `Retry-After`.
- Run 45: one-shot worker, `limit=100`, status `ok`, tracks fetched/upserted `100/100`, requests `101`, 429 `0`.
- Run 46: one-shot worker, `limit=100`, status `ok`, tracks fetched/upserted `99/99`, requests `101`, 429 `0`.
- Run 47: one-shot worker, `limit=100`, status `ok`, tracks fetched/upserted `100/100`, requests `101`, 429 `0`.
- Run 48: one-shot worker, `limit=100`, status `ok`, tracks fetched/upserted `100/100`, requests `101`, 429 `0`.
- Run 49: one-shot worker, `limit=100`, status `ok`, tracks fetched/upserted `100/100`, requests `101`, 429 `0`.
- Run 50: one-shot worker, `limit=100`, status `ok`, tracks fetched/upserted `100/100`, requests `101`, 429 `0`.
- Run 51: after changing worker to `limit=250`, status `partial`, fetched/upserted `1/1`, requests `3`, 429 `1`; Spotify provided no valid `Retry-After`, fallback cooldown used.
- Run 52: manual early cooldown test after shortening local DB cooldown to ~5 minutes, status `partial`, fetched/upserted `0/0`, requests `2`, 429 `1`; confirms 5 minute retry was too early. Current worker state cooldown until `2026-05-06T03:41:20.672449Z`.
- Run 53: old 250-limit config was still in effect, status `ok`, tracks fetched/upserted `250/250`, requests `251`, 429 `0`.
- Run 54: old 250-limit config was still in effect, status `ok`, tracks fetched/upserted `250/250`, requests `251`, 429 `0`.
- Run 55: old 250-limit config was still in effect, status `partial`, tracks fetched/upserted `100/100`, requests `102`, 429 `1`; Spotify provided no valid `Retry-After`, fallback cooldown used. Worker state cooldown until `2026-05-06T12:39:55.840067Z`.
- Run 56: corrected 100-limit config, status `partial`, tracks fetched/upserted `0/0`, requests `2`, 429 `1`; Spotify immediately rate-limited after cooldown. Worker state cooldown until `2026-05-06T21:29:40.234532Z`. Post-cooldown canary gate was added after this run to avoid full normal starts when Spotify is still throttling.
- Run 57: one-shot worker, corrected 100-limit config, status `ok`, tracks fetched/upserted `100/100`, requests `101`, 429 `0`.
- Run 58: one-shot worker, corrected 100-limit config, status `ok`, tracks fetched/upserted `100/100`, requests `101`, 429 `0`.
- Run 59: one-shot worker, corrected 100-limit config, status `ok`, tracks fetched/upserted `100/100`, requests `101`, 429 `0`.
- Run 60: one-shot worker, corrected 100-limit config, status `ok`, tracks fetched/upserted `100/100`, requests `101`, 429 `0`.
- Run 61: one-shot worker, corrected 100-limit config, status `ok`, tracks fetched/upserted `100/100`, requests `101`, 429 `0`.
- Run 62: one-shot worker, corrected 100-limit config, status `partial`, tracks fetched/upserted `99/99`, requests `101`, 429 `1`; Spotify provided no valid `Retry-After`, fallback cooldown used. Worker state cooldown was `2026-05-07T21:16:47.497747Z`.
- Post-Run 62 canary: after cooldown, canary emitted `canary_attempt` then `canary_rate_limited`, requests `1`, 429 `1`, source_track_id `24646`, Spotify track `2HNKqls4pZWD6sIzyHFqFt`, no valid `Retry-After`. Worker stopped before normal backfill with `skipped_canary_rate_limited`, `stop_reason=post_cooldown_canary_429`, `consecutive_post_cooldown_canary_429s=1`, fallback cooldown `21600` seconds, cooldown until `2026-05-08T03:16:48.708996Z`.

Operational guidance:
- For track metadata, use `target=tracks`, `run_mode=metadata_only`, `reason=identity_metadata`, `album_tracklist_policy=none`, `request_delay_seconds=5.0` for the current slow diagnostic profile.
- Prefer the worker CLI for local/dev background progress.
- Current loop command used:
  `python3 -m backend.scripts.run_spotify_track_metadata_worker --loop --max-runtime-minutes 90 --between-runs-seconds 300 --jsonl-output backend/data/logs/track-metadata-worker-loop.jsonl`
- Because the post-Run 62 canary hit 429, do not retry before current cooldown `2026-05-08T03:16:48.708996Z`.
- A thread heartbeat is scheduled for `2026-05-07 20:18 America/Los_Angeles` to resume after cooldown and run only one canary-gated worker attempt.
- Current code imports as slow diagnostic `limit=50`, `max_requests=60`, `max_runtime_seconds=360`; verify terminal says `[run N] start: limit=50 delay=5.0s` or a lower dynamically budget-capped limit before letting any future loop continue.
- After a rate-limit cooldown expires, expect canary JSONL/progress events before a normal `run N start...` line. If `canary_rate_limited` appears, the normal run was blocked and a new cooldown was set.
- Do not shorten the real 429 fallback cooldown. The 5 minute manual test was too aggressive.
- For album metadata, current source album/release-date gaps are cleared. Do not keep retrying the 3 UPC/EAN-only singles unless reporting is changed.
- Do not fetch album tracklists unless explicitly working catalog expansion.

Track variant runtime config moved to `docs/config/track-variant-policy.json`.
Run `python3 -m unittest backend.tests.test_track_variant_policy` after changes touching that config or its loader.

See `docs/reference/spotify-catalog-backfill.md` for catalog details.

## Known Naming Follow-Up
Domain language should now prefer `track_family` for the grouping layer above `release_track`.

Current implementation note:
- schema/code still use `analysis_track` and `analysis_track_map`
- this language mismatch is intentional for now during the phase-1 wording cleanup

Deferred follow-up phases:
- Phase 2: add API/UI compatibility aliases so `track_family` can become the external/default payload term
- Phase 3: rename internal code symbols, scripts, and counters away from `analysis_track`
- Phase 4: evaluate a real database migration from `analysis_track` / `analysis_track_map` to `track_family` / `track_family_map`

## Next Likely Task
Recommended next task:
- If user wants to reduce Track Mapping / Source -> Release blank album rows, first run read-only:
  `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --source-release-album-display-enrichment-plan`
- If user explicitly asks to fetch the missing metadata, run a bounded live worker, e.g.:
  `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --run-source-release-album-display-enrichment --limit 50 --max-requests 60 --request-delay-seconds 5.0`
- After any live worker run, rerun compact diagnostic:
  `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --source-release-album-display-diagnostic --album-display-diagnostic-summary-only`
- Expected success signal: `rows_with_no_spotify_album_evidence` decreases.
- Do not use Spotify batch endpoints; this worker uses single-track fetches only.
- If any 429 appears, stop and respect cooldown.

Alternative/manual review task:
- Ask the user to explain what they learned from manually reviewing the source->release groups.
- Convert that explanation into a concise read-only resolution policy/checklist before adding new behavior.
- Focus current discussion on source tracks resolving into one release track.
- Do not call Spotify unless the user explicitly asks to continue metadata fetching.
- Do not apply/merge/promote/confirm identity mappings yet.

Likely policy topics to capture from the user:
- How to treat same label/album/date/track position/duration with different Spotify IDs.
- How to treat capitalization-only title differences.
- How to treat localized/transliterated title differences such as Japanese vs English title text.
- Which mismatches should block auto-confirmation and require manual review.
- Whether current `provider_identity` and `same_album_exact_title_primary_artist` labels are clear enough in UI.

Deferred Spotify backfill:
- Recent track metadata worker loop ran cleanly with `0` displayed 429s, but still keep backfill secondary to the user explanation task.
- Current safe loop command if user asks to continue:
  `caffeinate -imsu -- ./.venv/bin/python -m backend.scripts.run_spotify_track_metadata_worker --loop --max-runtime-minutes 120 --between-runs-seconds 300 --jsonl-output backend/data/logs/track-metadata-worker-loop.jsonl`
- A faster but less conservative variant discussed:
  same command with `--between-runs-seconds 60`
- If any 429 appears, stop and respect worker cooldown/canary behavior.
- Album metadata gaps are currently cleared except the acceptable UPC/EAN-only singles; album backfill probably shares Spotify quota risk, so defer unless there is a specific high-value read-only need.
- Do not fetch album tracklists unless explicitly working catalog expansion.

Future options, not next unless explicitly requested:
- Evaluate alternate metadata sources for stubborn cases, but treat non-Spotify matching as lower confidence and read-only until there is a clear matching contract.
- Use already-fetched Spotify metadata to try deterministic name + artist + album + duration matching where Spotify IDs are absent.
- Revisit top-track/top-album/top-artist algorithms; this now matters because top-list relevance drives priority metadata eligibility.
- Brush up Catalog Backfill and Identity Audit UI once the read-only readiness workflow is clearer.
- Adjust coverage/reporting so album UPC/EAN gaps for `album_type=single` are separated/lower severity.
- Update frontend Catalog Backfill controls to expose explicit target modes if live backfill work resumes.

Manual verification checklist:
- Catalog Backfill page has tabs: Overview, Priority Metadata, Full Backfill, Queue, Recent Runs.
- Priority Metadata is the default active tab.
- Priority Metadata copy says it does not expand unlistened album tracklists.
- Priority Metadata action sends `metadata_only`, `include_albums=true`, `album_tracklist_policy=none`, `reason=identity_metadata`.
- New target-aware UI should send `target=tracks` or `target=albums` for isolated workflows.
- Full Backfill is visually secondary and tracklist-capable.
- Queue tab shows and filters reason buckets.
- Recent Runs shows run mode, reason, and album tracklist policy.
- Search/Lookup points users to Catalog Backfill for execution/status.
- Identity Audit has no apply/merge endpoint or behavior.

After catalog completeness improves, return to Albums duplicate review:
- treat `safe_candidate` as potentially apply-eligible only if preview is rerun in a future transaction
- keep remaster/deluxe/anniversary differences as separate `release_album` rows linked later by `album_family`
- keep `needs_review` conflict cases manual until tracklist/source metadata is better

## Verification Commands
Common checks for the current work area:

```bash
python3 -m unittest backend.tests.test_spotify_catalog_backfill
python3 -m unittest backend.tests.test_spotify_catalog_worker
python3 -m unittest backend.tests.test_entity_backfill
python3 -m py_compile backend/app/main.py backend/app/spotify_catalog_backfill.py backend/app/spotify_catalog_worker.py backend/scripts/run_spotify_track_metadata_worker.py backend/app/db.py
cd frontend && npm run build
```

Most recent verification:
- `./.venv/bin/python -m py_compile backend/scripts/inspect_spotify_catalog_queue.py backend/tests/test_spotify_catalog_backfill.py` passed after Source -> Release album-display enrichment loop mode.
- Targeted loop tests passed:
  - `test_source_release_album_display_enrichment_loop_summarizes_batches_and_writes_jsonl`
  - `test_source_release_album_display_enrichment_loop_stops_on_429`
  - `test_source_release_album_display_enrichment_worker_uses_bounded_limits`
- `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --help` passed and shows `--loop`, `--max-runtime-minutes`, `--between-runs-seconds`, and `--jsonl-output`.
- One-shot live verification passed after network escalation:
  `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --run-source-release-album-display-enrichment --limit 1 --max-requests 1 --request-delay-seconds 3 --summary-only`
  - fetched `1`, album evidence added `1`, `requests_429=0`, `error_count=0`
- Short live loop canary passed after network escalation:
  `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --run-source-release-album-display-enrichment --loop --max-runtime-minutes 1 --between-runs-seconds 5 --limit 5 --max-requests 5 --request-delay-seconds 3 --summary-only --jsonl-output backend/data/logs/source-release-album-display-enrichment-loop-test.jsonl`
  - `total_batches=4`
  - `total_processed_count=20`
  - `total_fetched_track_metadata=20`
  - `total_fetched_and_album_evidence_added=20`
  - `total_requests=20`
  - `total_429=0`
  - `total_errors=0`
  - `stop_reason=max_runtime`
- Resolution-evidence inspect/summary targeted checks from this session passed:
  - compact `--summary-only`
  - `--unknown-pending-queue-items`
  - append-mode CLI validation without `--resolution-evidence-report`
  - sibling appendability diagnostics explaining zero candidates
- `./.venv/bin/python -m unittest backend.tests.test_spotify_catalog_backfill` passed: 176 tests after Source -> Release album-display fallback, compact diagnostic, enrichment planner, and bounded worker.
- `./.venv/bin/python -m py_compile backend/app/spotify_catalog_backfill.py backend/app/main.py backend/scripts/inspect_spotify_catalog_queue.py backend/tests/test_spotify_catalog_backfill.py` passed after planner/worker changes.
- `cd frontend && npm run build` passed after Source -> Release album-display changes.
- `./.venv/bin/python -m backend.scripts.inspect_spotify_catalog_queue --source-release-album-display-enrichment-plan` ran read-only and reported `2026` eligible distinct track IDs needing metadata.
- `python3 -m unittest backend.tests.test_spotify_catalog_backfill` passed: 145 tests after priority-scope selector and coverage split.
- `python3 -m unittest backend.tests.test_spotify_catalog_worker` passed: 34 tests after priority canary/config and terminal output changes.
- `python3 -m py_compile backend/app/spotify_catalog_backfill.py backend/app/spotify_catalog_worker.py backend/app/main.py backend/scripts/run_spotify_track_metadata_worker.py` passed after priority-scope wiring.
- `cd frontend && npm run build` passed after Catalog Backfill coverage UI fields.
- `python3 -m unittest backend.tests.test_spotify_catalog_worker` passed: 33 tests after post-cooldown canary and canary-429 exponential backoff.
- `python3 -m unittest backend.tests.test_spotify_catalog_backfill` passed: 142 tests after adding single-track canary helper.
- `python3 -m py_compile backend/app/db.py backend/app/spotify_catalog_worker.py backend/app/spotify_catalog_backfill.py backend/scripts/run_spotify_track_metadata_worker.py` passed after canary-backoff migration/worker changes.
- `python3 -m py_compile backend/app/spotify_catalog_backfill.py backend/app/main.py backend/app/spotify_catalog_worker.py backend/scripts/run_spotify_track_metadata_worker.py` passed after worker implementation.
- `python3 -m unittest backend.tests.test_spotify_catalog_backfill` passed: 142 tests after priority/progress changes.
- `python3 -m unittest backend.tests.test_spotify_catalog_worker` passed: 21 tests after loop mode, JSONL output, rolling request budget, and current worker config changes.
- `python3 -m py_compile backend/scripts/run_spotify_track_metadata_worker.py backend/tests/test_spotify_catalog_worker.py` passed after loop mode.
- `python3 -m py_compile backend/app/spotify_catalog_worker.py backend/tests/test_spotify_catalog_worker.py` passed after rolling request budget.
- Earlier `cd frontend && npm run build` passed after Catalog Backfill tab/mode refactor; frontend was not rebuilt after backend target-mode changes.
