# Spotify Catalog Backfill

## Purpose
Spotify catalog backfill enriches existing local identities with Spotify catalog metadata.

It is enrichment-only. It must not create, merge, promote, or repair ListenLab identity rows.

## Tables
- `spotify_track_catalog`
- `spotify_album_catalog`
- `spotify_album_track`
- `spotify_catalog_backfill_run`
- `spotify_catalog_backfill_queue`
- `spotify_catalog_worker_state`
- `spotify_catalog_worker_lock`
- `spotify_catalog_worker_invocation`

## Behavior
- Discovers known Spotify track and album IDs from source mappings and raw play rows.
- Uses representative Spotify IDs for release-level lookup rows, preferring most-listened evidence where multiple candidates exist.
- Fetches track metadata, album metadata, and album tracklists.
- Skips complete non-error catalog rows unless `force_refresh=true`.
- Retries error rows on later runs.
- Resumes incomplete album tracklists by using the existing stored track count as the next offset.
- Applies album tracklist page caps per album, not as a global run stop.
- Processes queue-first work before broader discovered work.
- Supports explicit target modes:
  - `tracks`
  - `albums`
  - `album_tracklists`
  - `all`
- In `metadata_only`, `target=tracks` selects missing/incomplete track metadata only and does not fetch albums or tracklists.
- `priority_scope=identity_and_top_listened` is the default for `reason=identity_metadata`.
- Priority track metadata selection includes only:
  - duplicate resolved Spotify track groups
  - release-track source splits
  - suggested analysis groups
  - top listened tracks derived from play facts
  - tracks on top listened albums
  - tracks by top listened artists
  - recent repeat tracks
- `priority_scope=all` remains available for intentional broad/deferred metadata passes.
- Track metadata candidate ordering prioritizes:
  - identity relevance
  - top-listened relevance
  - track listen count
  - artist listen count
  - Spotify ID tie-break

## Request Controls
- `limit`
- `offset`
- `market`
- `include_albums`
- `force_refresh`
- `request_delay_seconds`
- `max_runtime_seconds`
- `max_requests`
- `max_errors`
- `max_429`
- `max_album_tracks_pages_per_album`
- `album_tracklist_policy`
- `run_mode`
- `priority_scope`
- `reason`
- `target`

Album tracklist policies:
- `all`
- `relevant_albums`
- `priority_only`
- `none`

## Reliability
- Handles Spotify 429 with `Retry-After` when available.
- Stops as partial with `stop_reason = "rate_limited"` after the first 429.
- Uses fallback 60 minute cooldown when Spotify returns 429 without a valid `Retry-After`.
- Falls back from forbidden batch track/album endpoints to single-item requests.
- Stores compact error diagnostics without token/header leakage.
- Keeps run telemetry for request counts, warning counts, skip counts, and retry-after timing.

## One-Shot Track Metadata Worker
`python3 -m backend.scripts.run_spotify_track_metadata_worker` runs one bounded local/dev enrichment job, then exits.

Worker defaults:
- `target=tracks`
- `run_mode=metadata_only`
- `reason=identity_metadata`
- `album_tracklist_policy=none`
- `include_albums=false`
- `limit=50`, `max_requests=60`, `max_runtime_seconds=360`
- `request_delay_seconds=5.0`
- `market=US`

The worker:
- skips when a stored cooldown is active
- skips before Spotify calls when the local rolling request budget is exhausted:
  - 60 minute window
  - 15 minute local cooldown at `>=550` requests
  - 30 minute local cooldown at `>=650` requests
  - when below `550`, caps the next run's `max_requests` to the remaining soft budget and lowers `limit` accordingly
- prevents overlapping worker runs with `spotify_catalog_worker_lock`
- replaces locks older than 2 hours
- stores each invocation in `spotify_catalog_worker_invocation`
- stores latest state/cooldown in `spotify_catalog_worker_state`
- sets rate-limit cooldown from valid `Retry-After`, otherwise 60 minutes
- after an expired rate-limit cooldown, runs a one-request single-track canary before normal backfill:
  - `canary_attempt`
  - `canary_success`
  - `canary_rate_limited`
  - `canary_failed_non_429`
  - `canary_skipped_no_candidate`
- consecutive `canary_rate_limited` results use exponential fallback cooldowns:
  - first canary 429: 6 hours
  - second canary 429: 12 hours
  - third and later canary 429: 24 hours
  - valid Spotify `Retry-After` can extend but not shorten that fallback
- never runs album metadata, album tracklists, Full Backfill, Identity Audit, or merge/apply behavior

Default CLI behavior remains one-shot and JSON-line compatible:

```bash
python3 -m backend.scripts.run_spotify_track_metadata_worker
```

Optional loop mode:

```bash
python3 -m backend.scripts.run_spotify_track_metadata_worker \
  --loop \
  --max-runtime-minutes 90 \
  --between-runs-seconds 300 \
  --jsonl-output backend/data/logs/track-metadata-worker-loop.jsonl
```

Loop mode:
- runs one worker iteration at a time
- sleeps between clean runs
- sleeps until worker-reported cooldown for `skipped_cooldown`, `skipped_request_budget`, `skipped_canary_rate_limited`, or `partial/rate_limited`
- exits cleanly on Ctrl-C
- prints condensed terminal status lines
- appends full JSONL events when `--jsonl-output` is provided

## Frontend
The Catalog Backfill page includes:
- Overview, Priority Metadata, Full Backfill, Queue, and Recent Runs tabs
- Priority Metadata as the default workflow for identity-critical source metadata
- Full Backfill as the secondary workflow for slower catalog expansion and tracklists
- queue status and reason filters
- queue repair button
- run mode, reason, target, and album tracklist policy visibility in recent runs

Search / Lookup includes:
- Album Catalog Lookup
- Track Catalog Lookup
- duplicate album diagnostics
- duplicate track diagnostics
- queue-aware statuses
- manual prioritize actions for visible incomplete albums/tracks

## Invariants
- No identity tables are mutated by catalog backfill.
- No `analysis_track_map` rows are mutated.
- No merge/apply behavior belongs in catalog backfill.
- Catalog rows are metadata evidence, not identity decisions by themselves.

## Verification
Common checks:
- `python3 -m unittest backend.tests.test_spotify_catalog_backfill`
- `python3 -m unittest backend.tests.test_spotify_catalog_worker`
- `python3 -m py_compile backend/app/main.py backend/app/spotify_catalog_backfill.py backend/app/spotify_catalog_worker.py backend/scripts/run_spotify_track_metadata_worker.py backend/app/db.py`
- `npm run build`
