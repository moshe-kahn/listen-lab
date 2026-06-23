# Spotify Catalog Backfill

## Purpose
Spotify catalog backfill enriches existing local identities with Spotify catalog metadata.

It is enrichment-only. It must not create, merge, promote, or repair ListenLab identity rows.

Artist identity promotion is separate from catalog backfill. When a Spotify artist ID arrives through ingest/catalog promotion code, it may attach the provider source map to one existing text-only artist only under exact-name plus album/track evidence gates. Catalog backfill itself remains evidence-only and must not perform duplicate repair.

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
- Persists basic nested metadata from Spotify track payloads:
  - `track.album` populates basic `spotify_album_catalog` display fields such as album name, type, release date, total tracks, images, and album artists.
  - `track.artists` and `track.album.artists` are retained as JSON evidence in the track and album catalog rows.
  - Full album fetch is still required for label, copyrights, external IDs, UPC/EAN, and complete album-level provenance.
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

## Playback Album-Track Fallback
The playback route `/auth/playback/album-tracks` can fetch a Spotify album tracklist when local catalog data is missing or incomplete.

Behavior:
- use complete `spotify_album_track` rows from the local ListenLab database when available
- otherwise use `spotify_track_catalog` rows for that album as a partial local fallback
- return partial local rows with `partial: true` when the caller has not requested a forced Spotify refresh
- when the frontend gets `partial: true` for a track overlay and Spotify is not cooling down, call the endpoint again with `force_spotify=true`
- `force_spotify=true` skips the early partial local return and fetches `/v1/albums/{album_id}/tracks` from Spotify, then caches those rows for later opens
- `local_only=true` prevents any Spotify request and returns only complete/partial local database evidence; the frontend uses this during Spotify cooldown
- use the same cached/local fallback path for the song overlay and homepage album expansion
- preserve album queue context when a track is started from an album row

The fallback remains catalog enrichment-only by default. A track-overlay request explicitly sends
`promote_identity=true` after the user opens an album context. Once the album tracklist is complete,
the route invokes the separate conservative catalog identity-promotion workflow. That workflow creates
missing source/release/album membership rows, marks affected generated recording clusters dirty, and
refreshes them. It does not merge or repair existing canonical identities. Homepage and generic catalog
requests do not enable this flag.

## History/Spotify Release-Album Repair
The debug repair path for history-only release albums and Spotify-backed release albums is separate from catalog backfill. It may merge release-album rows only through explicit preview/dry-run/apply flow.

Current safe-equivalence rules:
- require source-track evidence that the release track belongs on the target Spotify album, or on a different Spotify album catalog row that is equivalent to the target
- treat Spotify album catalog rows as equivalent when normalized album name matches, and release date plus total track count do not conflict when both sides have values
- intentionally ignore duration for album equivalence and same-album release-track duplicate repair, because Spotify duration metadata has known drift
- handle duplicate album-track collisions by retaining the survivor album-track row and deleting the conflicting retired row before deleting the retired release album
- repoint `album_family`, `album_family_map`, and `release_track_merge_log` references before deleting retired release albums
- mark affected generated recording clusters dirty after accepted release-album/release-track repoints

This path is still evidence-gated identity repair. Do not move it into automatic catalog backfill.

## Artist Album Evidence Fallback
The artist album evidence route `/auth/artist-albums` primarily uses Spotify catalog album and album-track metadata, but it also falls back to internal `album_artist` links when catalog album rows are missing or incomplete.

Behavior:
- return catalog-derived album/appears-on evidence when available
- append missing albums from internal `album_artist` links
- prefer a Spotify source album ID when an internal release album has one
- collapse duplicate same-title history/provider album rows for the same selected artist
- return cache-backed artist track rows from `spotify_album_track` as `tracks`
- sort artist track rows with dated album metadata first, then album/disc/track order
- include source-track play-count cache fields where available for artist track rows

This fallback reads identity links that already exist. It must not create catalog rows, repair albums, or merge identities.

Current limitation:
- The album evidence list can be broader than the artist `tracks` list.
- Albums may appear from Spotify album catalog rows or internal album-artist links even when their Spotify album tracklist is not cached.
- Those albums should be labeled or treated as metadata-only until a bounded background backfill fetches the missing tracklist.
- Artist preview must remain cache-only; it should not issue live Spotify loops to complete a discography.

Recommended gap-handling:
- detect visible artist albums that have Spotify album IDs but missing/incomplete `spotify_album_track` rows
- enqueue low-priority catalog backfill work for those album IDs
- deduplicate by Spotify album ID
- process with strict request limits, delay, and immediate stop on 429 / `Retry-After`
- do not block the artist overlay on this work
- keep future non-Spotify catalog sources such as MusicBrainz/Cover Art Archive as cached metadata inputs, not live artist-view calls

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
- Includes local nested-metadata integrity summaries in worker output.
- Emits a warning if a track metadata run increases local album-display gaps after marking rows done.

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
- Artist album evidence fallback can read `album_artist`, but it must remain read-only.

## Verification
Common checks:
- `python3 -m unittest backend.tests.test_spotify_catalog_backfill`
- `python3 -m unittest backend.tests.test_spotify_catalog_worker`
- `python3 -m py_compile backend/app/main.py backend/app/spotify_catalog_backfill.py backend/app/spotify_catalog_worker.py backend/scripts/run_spotify_track_metadata_worker.py backend/app/db.py`
- `npm run build`
