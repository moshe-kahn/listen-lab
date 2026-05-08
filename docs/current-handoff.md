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
The active work should now pivot back from Spotify catalog backfill to read-only Identity Audit readiness.

Reason for pivot:
- Spotify track metadata backfill is currently constrained by practical 429/cooldown behavior.
- The priority worker now limits identity metadata fetching to priority tracks instead of the full accepted Spotify source-track backlog.
- Enough additional Spotify source-track metadata has been fetched to inspect whether duplicate/split Identity Audit cases are now better informed.
- Identity Audit remains read-only and should consume catalog evidence; do not apply, merge, promote, or mutate identity mappings.

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
- Build or inspect a read-only Identity Audit readiness report using the source catalog metadata already fetched. Do not call Spotify. Do not apply/merge/promote.
- Start with track duplicate/split cases, then album duplicate cases if useful.
- Useful readiness buckets:
  - duplicate Spotify track ID groups now metadata-complete
  - release-track source splits where duration/ISRC/album evidence now agrees
  - suggested analysis/track-family groups that now have enough duration/ISRC/album evidence
  - groups still blocked by missing catalog metadata
  - safe candidates vs needs-review vs unsafe, read-only only

Deferred Spotify backfill:
- Do not keep pushing track backfill while Spotify 429 behavior is unclear.
- If resuming after cooldown (`2026-05-08T03:16:48.708996Z`), first inspect worker state and only then run one canary-gated worker attempt with JSONL logging if appropriate.
- If the next post-cooldown canary gets 429, confirm it returns `skipped_canary_rate_limited`, increments `consecutive_post_cooldown_canary_429s`, and sets at least a 12 hour cooldown before trying again.
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
