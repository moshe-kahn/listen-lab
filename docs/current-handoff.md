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
The active work has pivoted from release-album apply planning to Spotify catalog backfill/completeness.

Reason for pivot:
- Identity Audit duplicate review is under-informed until source-catalog evidence is more complete.
- Spotify metadata should be fetched at the source layer, not directly for release-layer rows.
- Identity Audit remains read-only and should consume catalog evidence later.

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
- new explicit targets:
  - `target = "tracks"` for true track metadata only
  - `target = "albums"` for true album metadata only
  - `target = "album_tracklists"` for explicit tracklist-only expansion modes
  - `target = "all"` for current mixed behavior
- fetches listened/source-mapped Spotify tracks
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

One-shot worker:
- module: `backend/app/spotify_catalog_worker.py`
- CLI: `python3 -m backend.scripts.run_spotify_track_metadata_worker`
- worker name: `spotify_track_metadata`
- current job config: `target=tracks`, `run_mode=metadata_only`, `reason=identity_metadata`, `album_tracklist_policy=none`, `include_albums=false`, `limit=250`, `max_requests=275`, `max_runtime_seconds=900`, `request_delay_seconds=2.0`, `market=US`
- persists `spotify_catalog_worker_state`, `spotify_catalog_worker_lock`, and `spotify_catalog_worker_invocation`
- skips on active cooldown, skips on non-stale overlap, replaces locks older than 2 hours, then exits after one bounded job
- rate-limit cooldown is valid `Retry-After` when present, otherwise 60 minutes
- has a local rolling request budget guard before Spotify calls:
  - counts `spotify_catalog_backfill_run.requests_total` for the last 60 minutes
  - skips with `skipped_request_budget` at `>=550` requests and sets 15 minute local cooldown
  - skips with 30 minute local cooldown at `>=650` requests
  - real Spotify `429` cooldown remains separate and still uses valid `Retry-After`, otherwise 60 minutes
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

Current catalog coverage after Run 52:
- missing source track metadata: `28,706`
- missing track ISRC: `28,708`
- missing track duration: `28,706`
- missing source album metadata: `0`
- missing album release date: `0`
- missing album UPC/EAN: `3`
- missing album tracklists: `228` separate catalog-expansion backlog

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

Operational guidance:
- For track metadata, use `target=tracks`, `run_mode=metadata_only`, `reason=identity_metadata`, `album_tracklist_policy=none`, `request_delay_seconds=2.0`.
- Prefer the worker CLI for local/dev background progress.
- Current loop command used:
  `python3 -m backend.scripts.run_spotify_track_metadata_worker --loop --max-runtime-minutes 90 --between-runs-seconds 300 --jsonl-output backend/data/logs/track-metadata-worker-loop.jsonl`
- Because runs 51 and 52 hit immediate 429, do not retry before current cooldown.
- Recommended next change before more live runs: revert worker config from `limit=250`, `max_requests=275`, `max_runtime_seconds=900` back to safer `limit=100`, `max_requests=110`, `max_runtime_seconds=300` while keeping loop mode and rolling request budget.
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
Pick one:
- after cooldown, first revert worker config to safer `limit=100`, `max_requests=110`, `max_runtime_seconds=300`; keep loop mode and rolling request budget
- then test/run worker loop with JSONL logging, e.g. `python3 -m backend.scripts.run_spotify_track_metadata_worker --loop --max-runtime-minutes 90 --between-runs-seconds 300 --jsonl-output backend/data/logs/track-metadata-worker-loop.jsonl`
- inspect worker rows after a run in `spotify_catalog_worker_invocation` and `spotify_catalog_worker_state`
- continue bounded `target=tracks` Priority Metadata passes to reduce the remaining track backlog, preferably through the one-shot worker
- adjust coverage/reporting so album UPC/EAN gaps for `album_type=single` are separated/lower severity
- update frontend Catalog Backfill controls to expose explicit target modes
- return to Identity Audit/Albums duplicate review using improved source album metadata

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
- `python3 -m py_compile backend/app/spotify_catalog_backfill.py backend/app/main.py backend/app/spotify_catalog_worker.py backend/scripts/run_spotify_track_metadata_worker.py` passed after worker implementation.
- `python3 -m unittest backend.tests.test_spotify_catalog_backfill` passed: 142 tests after priority/progress changes.
- `python3 -m unittest backend.tests.test_spotify_catalog_worker` passed: 21 tests after loop mode, JSONL output, rolling request budget, and current worker config changes.
- `python3 -m py_compile backend/scripts/run_spotify_track_metadata_worker.py backend/tests/test_spotify_catalog_worker.py` passed after loop mode.
- `python3 -m py_compile backend/app/spotify_catalog_worker.py backend/tests/test_spotify_catalog_worker.py` passed after rolling request budget.
- Earlier `cd frontend && npm run build` passed after Catalog Backfill tab/mode refactor; frontend was not rebuilt after backend target-mode changes.
