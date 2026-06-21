# Current Handoff

## Read First
Start in `/Users/kahntra/Programming/Personal Projects/ListenLab/listen-lab-main`, then read `AGENTS.md` and this file.

Recommended topic docs:
- `docs/reference/drafts/entity-model-draft.md` for current source/text identity reconciliation rules and long-term model notes.
- `docs/reference/raw-ingest.md` for recent/history ingest, canonical play-event projection, and entity backfill behavior.
- `docs/reference/spotify-catalog-backfill.md` for cached Spotify catalog and album tracklist behavior.
- `docs/reference/source-track-resolution-policy.md` for source/release/recording/track-family identity policy.
- `docs/reference/refactor-notes.md` for frontend/playback refactor and route extraction notes.

Avoid reading every doc by default.

## Current State
Active branch: `frontend-app-refactor`.

Latest feature commit:
- `Refine track detail source history`

Worktree at handoff:
- dirty and staged-ready after the combined Activity/recent-sync, artist-promotion audit, and track relationship/album promotion batch
- do not include `backend/tests/_tmp_entity_backfill.sqlite3-shm` or `backend/tests/_tmp_entity_backfill.sqlite3-wal` in the commit

Important local instruction:
- `AGENTS.md` says substantial frontend UI should not be added directly to `frontend/src/App.tsx`. This branch still has a large `App.tsx` integration surface from iterative UI work. Future UI work should extract focused components instead of growing `App.tsx` further.

## Immediate Next Task: Recover Orphan History Projection
The user discovered that `Radiohead - Paranoid Android` incorrectly had no qualified listen. A raw-title search found the missing complete play:
- raw history row `70164`
- `2025-10-08T23:04:12Z`
- Spotify track `6LgJvl0Xdtc73RJ1mmpotq`
- `387355` ms played
- `reason_end=trackdone`
- no `fact_play_event_history_link`, so it never reached canonical history or `source_track_play_count_cache`

Root cause:
- `backend/scripts/ingest_history_with_checkpoints.py` commits raw rows at checkpoints but projects facts only after the entire import completes.
- two early checkpoint runs stopped before projection:
  - `36c8c0f2-2555-44a4-ac5d-ca5f5d53fcd1`: 3,750 raw rows
  - `196a5f2c-d80f-49fb-a755-02416d97b097`: 6,000 raw rows, including Paranoid Android
- later retries saw those source rows as duplicates, but raw rows retained the failed run IDs. Projection is scoped by ingest run, so retries did not recover them.
- stale-run startup recovery only marked runs failed; it did not project their durable raw checkpoints.
- current DB audit: 9,980 unprojected raw history rows belonging to failed runs, plus 24 unprojected rows belonging to completed runs.

Implement next:
1. Add an idempotent orphan-history projection/backfill that selects eligible `raw_spotify_history` rows lacking `fact_play_event_history_link`, regardless of original run status/ID.
2. Run it against the full local DB, rebuild source-track play counts, and verify Paranoid Android gains the 2025-10-08 listen.
3. Harden checkpoint ingestion:
   - project each committed checkpoint or enqueue it durably;
   - update heartbeat and run counters each checkpoint;
   - make retry/finalization sweep global eligible orphans, not only rows owned by the current run;
   - make stale-run recovery enqueue orphan projection;
   - add a crash-after-checkpoint/resume regression test and an invariant audit for eligible unlinked rows.
4. Preserve raw rows and canonical idempotency; do not reimport or duplicate source observations.

Do not confuse this with the proposed listen-threshold change. The current cache uses 65%. The user has not yet confirmed changing it to 50%/four minutes. The newly found 387355 ms event qualifies even under 65%, so orphan recovery should fix this case without changing threshold policy.

## Latest UI/Identity Work This Session
- General album-family inference now supports complete title-prefix expansions and explicit `Disc N` / `Disk N` companion releases.
- Local mappings applied:
  - `In Rainbows` + `In Rainbows (Disk 2)`
  - `OK Computer` + `OK Computer OKNOTOK 1997 2017`
- Companion family tracklists project Disc 1 before Disc 2 even when Disc 2 is selected.
- Gray companion tracks now say `Switch to include these tracks`; clicking includes/ungrays them without changing selected album. Play All excludes them until included. Queue rows retain source album art/title when playback crosses releases.
- `Rmx` is recognized as remix. Full `TKOL RMX 1234567` identity was promoted; remix-album context supplies remix semantics where a safe single-song original cannot be linked.
- Song Family artist display deduplicates equal normalized artist names (`Radiohead, Radiohead` fix).
- Activity `Liked` now filters canonical history before the limit, returning the latest 50 matching listens rather than filtering the latest 50 total.
- Album-context Spotify queue now uses fixed album order from track 1 through the final track and does not display wrapped duplicate cycles.
- Play-count cache currently combines consecutive interrupted same-track fragments within four hours and counts full durations plus a >=65% remainder; exact-history changes were applied retroactively.

Latest checks passed during this session:
- `python -m unittest backend.tests.test_album_family_review` (7 tests)
- combined focused backend suites up to 64 tests passed during album/remix work
- frontend `npm run build` passed repeatedly; existing large-chunk warning remains
- `git diff --check` passed

## Current Uncommitted Batch
Activity and recent-sync changes:
- Activity completion filtering is now `Completed` / `All`, with independent `Liked` and `Tagged` toggles.
- Grouped Activity rows render completion markers on the progress bar; repeated identical completion points show a count.
- Listen Log forced refresh treats Spotify recent-sync failure as best effort and returns the failure summary while preserving local log results.

Artist identity observability:
- SQLite schema version `35` adds `artist_promotion_skip_log`.
- blocked text-to-provider artist promotions record stable reason/context signatures and roll up occurrence counts.
- Identity Audit `Artists` is split into `Promotion Skips` and `Duplicate Repair` focused components.

Track relationship and album behavior:
- same-title groups with overlapping primary artists can form Track Family candidates across credited-artist signature changes.
- unique cached Spotify album names hydrate missing recording-member artwork when no explicit source-album map exists.
- family rows collapse multiple release appearances to the representative recording.
- track modal uses `Song Family` with `Original`, `Cover`, `Remix`, `Version`, `Rework`, `Sibling Cover`, and `Sibling Remix` badges.
- `Also Appears On` is a collapsible bottom bar inside the album box.
- local stars remain visible across album track rows.
- a complete track-overlay album fetch explicitly requests conservative catalog identity promotion; newly promoted release appearances refresh generated recording clusters without canonical merges.
- album-track payloads now expose generated recording-group history separately from exact source/release history; recording view uses the aggregate values.
- stale frontend album-row caches without recording-history fields are invalidated.

Local BITCRUSH / PARALYSIS GHOSTS state:
- BITCRUSH Spotify album `6LU67HlNUaukF21Tr6ymuD` is cached with the correct three tracks: `BITCRUSH`, `MOUNTAIN LION // ADORE`, and `PARALYSIS GHOSTS`.
- promoted BITCRUSH `PARALYSIS GHOSTS` release track `30567` joins recording candidate `13220` with release tracks `28598` and `30525`.
- direct metadata enrichment returns recording history of 3 listens, latest `2026-06-17T15:35:32.011000Z`, while exact BITCRUSH-source history remains zero.
- unresolved user QA: after the latest restart, BITCRUSH reportedly displayed the wrong tracks even though `_cached_album_track_rows('6LU67HlNUaukF21Tr6ymuD')` returned the correct three rows. First next-session task is to capture the visible wrong rows and trace frontend selected album id/cache state.

## Latest Album/Recording Identity Work
This handoff follows a focused QA cycle around track, album, artist, and recording variation previews.

User-confirmed fixed flows:
- Kutiman & Dekel / `Everybody Needs To Be` track and album flows now preserve the correct album context.
- `Hope` album opened from artist view now has Spotify-backed metadata when source-track evidence proves the album.
- `Hope` album cover subtitle no longer renders as `2026 - 2026`.
- Recording variation navigation no longer drops album art/date after clicking between related release tracks.
- Missing recording variation dates no longer display as `Year unknown`; the year line is omitted.

Backend changes:
- `release_track_detail` now falls back to raw Spotify payload album fields when Spotify catalog rows are missing.
- release-track source versions now expose structured artist ids/URLs where available, not only artist names.
- recording candidate members now expose structured `artists: [...]` in addition to the display `artist` signature.
- recording candidate source rows normalize `spotify_uri` source rows before joining Spotify catalog data.
- recording candidate source rows use safe `json_valid` raw-payload fallback for ISRC, duration, Spotify album id, album art, release date, and album type.
- generated recording candidate snapshots are hydrated from current source/member metadata before returning from by-release-track lookup, so stale `candidate_snapshot_json` does not keep serving blank album art/date.
- history-only and Spotify-backed release album rows can be reconciled through a targeted safe repair at:
  - `POST /debug/identity/release-albums/history-spotify-repair?dry_run=true|false&limit=50`
- safe release-album repair merges only when Spotify/source-track evidence supports the move, handles duplicate same-track album-track collisions, and marks generated recording clusters dirty.
- artist album evidence enriches internal album-artist links from linked Spotify source-track payload/catalog data, including album id, image, release year, and total tracks when safe.
- `/auth/artist-albums` returns enriched requested artist metadata so artist previews can populate real artist images when cache/API evidence exists.
- release-album history/Spotify repair dry-run was run locally with `limit=50`:
  - `candidate_count=50`
  - `safe_candidate_count=28`
  - `applied_count=0`
  - 22 candidates blocked because one or more album-track repoints lacked Spotify album evidence

Frontend changes:
- recording variation cards and navigation use structured artist entries when available.
- album-track preview rows preserve track `artists` arrays and use structured artists before display-string fallback.
- artist preview no longer uses album art as an artist-photo fallback.
- album heading formatting now treats album previews correctly: album label/source album name is the album name, and `detail` is only a year when it looks like a year.
- missing recording variation years are omitted in dashboard modal and Identity Audit candidate rows.
- Identity Audit now has `Albums -> Repair` for the release-album history/Spotify repair endpoint:
  - limit input
  - dry-run button
  - apply-safe button gated behind dry-run confirmation
  - summary chips, all/safe/blocked filters, evidence/reason display, and moved-track details
- release/recording/family badges render concatenated `D/R/V/C` tags instead of showing a generic `R` for every related-track case.
- representative track pages show true Track Family rows for variations, covers, remixes, and related family members.
  - same-recording rows from different albums stay in the recording/release appearance area and are excluded from the family list.
  - family lookup now fetches sibling release-track candidate payloads from the current recording group, because broader family candidates may be attached to a sibling rather than the exact opened release track.

Important implementation note:
- Some backend fixes affect generated recording candidate lookup. If QA appears stale, restart the backend first, then hard refresh the frontend.

## Current Track Modal Album/Release QA Batch
This latest work is mostly frontend modal behavior plus a small playback album-track route control.

User-facing track modal changes:
- Track view label changed from `Recording variations` to `Also Appears On:`.
- Recording-variation rows are cleared on track changes instead of briefly showing the previous track's rows.
- `D/R/V/C` badges no longer show recording `R` just because duplicate-source evidence exists.
- Track and album artist headings render inline artist images when known.
- Last-listened and listen-count tags moved to the bottom behind a divider, with reserved space to avoid delayed layout jump.
- Track top controls now use a compact play/time `PlaybackActionMenu` at top left with star/bookmark beside it.
- The old inline `Play now`, `Play next`, and `Add to queue` buttons were removed.
- Gear menu keeps Spotify/settings actions; `View release track` is hidden when a track has no separate Spotify/source release versions.

Album behavior from track view:
- If the selected album is only partially known locally, the frontend first shows local database rows, then automatically requests Spotify completion when cooldown allows.
- During Spotify cooldown, the frontend uses `local_only=true` and shows `More tracks on Spotify` with an album link when local rows are incomplete.
- Loading copy distinguishes no rows yet (`Fetching Album...`) from partial rows being completed (`Loading Album...`).
- Backend `/auth/playback/album-tracks` now supports:
  - `force_spotify=true`: skip early partial DB return and fetch/cache the full Spotify album tracklist.
  - `local_only=true`: use only local DB evidence and do not call Spotify.
- Album main artists are derived from album-wide tracklist evidence when possible, so an album opened from a feature track does not keep feature artists as album artists after the full album is fetched.

Release/recording view behavior:
- Release album `Rep` badges now use stable representative-source metadata instead of following the highlighted/current source album.
- Release view bottom listened/date/listen-count tags use only the currently selected Spotify source version.
- Recording view bottom listened/date/listen-count tags combine source versions across the current generated recording group, and only show click breakdown popovers when more than one recording member contributes.
- Release view album-track stars are exact to that Spotify source track.
- Recording view album-track stars keep aggregate behavior across related source versions.
- The bottom tag order is now `Liked`, album-context tag such as `Single`/`Soundtrack`/`Compilation`, listened range, then listen count.
- Album tracklist `Last` uses week units for sub-year ages instead of month units.
- Playback home no longer shows a `Connecting to Spotify player...` status line; disabled/connecting controls expose that state through the hover tooltip.

Backend release detail changes:
- `ReleaseTrackDetailSourceVersion` now exposes `is_representative_choice`.
- Representative source selection stays stable while playback/highlight source can change with context.
- `ReleaseTrackDetailSourceVersion` also exposes album type/release date/total tracks plus first/last/listen-count source history from the source-track play-count cache.

## Current Source-History And In Rainbows Merge Batch
Backend changes in this batch:
- SQLite schema version `34` adds `source_track_play_count_cache` with `spotify_track_id`, `play_count`, `first_played_at`, and `last_played_at`.
- Recent/history projection refreshes that cache after touched facts are reloaded, so track overlays do not recompute source-version counts from the fact view on every album open.
- Release-track metadata enrichment reads source exact history from `source_track_play_count_cache`, then still provides aggregate release-track history for recording view.
- Generated recording candidate members now include `play_count`, `first_played_at`, and `last_played_at`, plus candidate-level listen-count totals for recording view.
- Same-album release-track duplicate merging no longer treats duration mismatch as a hard blocker, because Spotify duration data has known drift.
- Safe history/Spotify release-album repair accepts equivalent Spotify album IDs when normalized album name matches and release date/track count do not conflict; duration is intentionally ignored.
- `apply_release_album_merge` now repoints/deletes album-family and release-track merge-log references before deleting retired release albums.

Local DB repair already applied for `Radiohead - In Rainbows`:
- Merged duplicate release albums `5` and `2830` into survivor `5`.
- Ran conservative same-album release-track duplicate merge and regenerated dirty recording clusters.
- `15 Step` is now one release track on album `5` with two Spotify source versions:
  - `4oXg7xT4ksBxHTx8PcmSXw`: 26 listens, first `2019-01-01T01:56:07Z`, last `2026-06-14T01:59:51.376000Z`
  - `6dsq7Nt5mIFzvm5kIYNORy`: 5 listens, first `2020-11-16T21:49:40Z`, last `2020-12-05T01:23:18Z`
- Both Spotify source albums are catalog-equivalent `In Rainbows` rows with release date `2007-12-28` and `10` tracks.

## Current Startup-Load Work
Committed startup-load work changed initial dashboard loading behavior and bundle shape.

Backend:
- `GET /me` now accepts `mode=shell`
- shell mode returns a fast authenticated profile shell after fetching Spotify profile identity only
- shell payload preserves the existing `ProfileResponse` shape with empty section arrays/false availability flags
- normal quick `/me` remains available for all-time/top/profile sections after first viewport data is ready

Frontend:
- initial quick load calls `/me?mode=shell`
- route-level lazy chunks were added for Formula Lab, Recent Debug, Catalog Backfill, and Search Lookup
- `DetailPreviewModal` is lazy-loaded only after a preview item is selected
- startup loading now targets visible-screen readiness instead of earliest possible paint:
  - profile shell must exist
  - playback/current/queue/recent-player first attempts must complete
  - Activity/recent sections must load, or a real recent-load error must occur
- top/all-time sections load after visible startup data is ready
- once the dashboard is released for a Spotify user, the full-screen loading screen is latched off until the user/session changes
- loading copy no longer falls back to internal progress history such as `initial Loading your Spotify data (0.3s)`

User QA during this session:
- Earlier versions produced an empty page/layout jump and then a flash back/reload.
- User confirmed the committed startup-load behavior works.
- Full-screen loading no longer returns after dashboard release during authenticated QA.

## Recent Artist Identity Work
The latest committed work adds backend-focused artist duplicate audit/repair and ingest prevention for text-only history artists vs provider-backed Spotify artists.

Implemented backend behavior:
- read-only duplicate artist audit at `GET /debug/artists/duplicate-audit`
- dry-run/write repair at `POST /debug/artists/duplicate-repair?dry_run=true|false`
- repair writes run inside a transaction
- automatic repair only mutates exact-name groups with exactly one provider-backed artist and evidence-backed text-only duplicates
- repair supports identity evidence and strict shared-normalized-album-title evidence
- same-name-only groups, stylization variants, similar-name same-album groups, orphan placeholders, and ambiguous provider-backed groups remain review-only
- source maps, album artist links, and track artist links are repointed only for safe groups, with duplicate semantic links removed before orphan artist deletion
- source/text Spotify ingest promotion now first checks exact Spotify source maps, then promotes a safe text-only artist only with album/track or strict album-title evidence
- source-name-only promotion is blocked and logged

Composite credit handling:
- raw history composite artist values such as `Dave Harrington, Tim Mislock` are classified as `composite_credit_review_only`
- evidenced composite history credits are skipped during history text artist mapping instead of creating a fake single artist when structured/provider evidence proves the parts are separate credited artists
- repeated duplicate text such as `Telekinesis, Telekinesis` is normalized for fallback identity
- legitimate comma-bearing artist names such as `Peter, Paul & Mary`, `Earth, Wind & Fire`, or `Crosby, Stills, Nash & Young` are preserved as valid credited artist identities
- composite cleanup endpoint exists at `POST /debug/artists/composite-credit-cleanup?dry_run=true|false`; write cleanup deletes only ready composite album/track links and deletes a composite artist row only when references are gone

Frontend/debug UI:
- Identity Audit includes an artist duplicate audit tab with safe repair categories, review-only categories, evidence labels, row details, and composite cleanup controls
- album/track modal artist display now preserves comma-bearing display names when there is no stable structured artist identity, preventing UI-only fake splits such as `Crosby`, `Stills`, `Nash & Young`
- artist album pages opened from tracks include the source album as a highlighted fallback row when backend evidence is sparse

Artist album evidence:
- `/auth/artist-albums` now falls back to internal `album_artist` links when Spotify catalog album metadata is incomplete
- fallback prefers a Spotify source album ID when available and collapses duplicate same-title history/provider album rows for the same selected artist
- local CSNY check returned `Looking Forward` with Spotify album ID plus `Deja Vu` from internal links

## Recording And Track-Family Identity
Hierarchy direction remains:

`source_track -> release_track -> recording_track -> track_family`

No durable canonical `recording_track` table, promotion/apply endpoint, or default aggregation change exists yet. Generated recording/track-family cluster tables are SQLite evidence caches only.

Relevant current rules:
- generated candidate metadata exposes cluster type and relationship kind to frontend rows
- backend candidate lookup prioritizes recording-level candidates over broader family candidates when both exist
- candidate row tags use `D/R/V/C` order:
  - `D`: duplicate source-track grouping for same release track
  - `R`: recording group
  - `V`: variation/context/style family
  - `C`: cover/remix/rework family
- track relation rows in overlays are separated into `Recording variations`, `Variations`, and `Covers / remixes`
- representative track family lists are anchored to the full current recording group for lookup, but display excludes current/same-recording release appearances so only true alternate versions/covers/family rows appear there.

## Source/Text Identity Status
Artist reconciliation now has audit, safe repair, composite cleanup, and ingest prevention.

Remaining broader source/text identity work:
- album duplicate repair still needs stricter source/text reconciliation beyond existing release-album preview/dry-run tooling
- track duplicate repair still needs evidence rules by title + artist + album + duration/ISRC/context
- frontend de-duping remains a defensive display fallback only; backend identity remains the source of truth

Do not broaden automatic repair to similar names, stylization variants, or multiple provider-backed rows without an explicit review/apply design.

## Tests And Verification
Checks run for the current uncommitted batch:
- `python3 -m unittest backend.tests.test_artist_promotion_skip_log backend.tests.test_spotify_recent_sync backend.tests.test_catalog_identity_promotion backend.tests.test_recording_track_candidates backend.tests.test_artist_identity_repair`
  - `Ran 66 tests ... OK`
- `python3 -m py_compile backend/app/db.py backend/app/catalog_identity_promotion.py backend/app/recording_track_candidates.py backend/app/release_track_metadata.py backend/app/routes/admin_routes.py backend/app/routes/audit_routes.py backend/app/routes/playback_routes.py backend/app/spotify_recent_sync.py`
- `npm run build` from `frontend/`
- `git diff --check`

Checks run and passed before the latest commit:
- `python3 -m unittest backend.tests.test_artist_identity_repair backend.tests.test_entity_backfill backend.tests.test_artist_album_evidence`
  - `Ran 62 tests ... OK`
- `npm run build` from `frontend/`
- `git diff --check`

Vite still reports the existing large chunk warning after frontend builds.

Manual QA completed by user:
- `Crosby, Stills, Nash & Young` album/track modal display looked correct after the comma-display fix.

Checks run for committed startup-load work:
- `npm run build` from `frontend/`
- `python3 -m py_compile backend/app/main.py`
- `git diff --check`

Manual QA completed by user:
- authenticated startup loading works after the committed changes
- dashboard does not flash back to full-screen loading after release

Checks run for album/recording identity metadata work:
- `python3 -m unittest backend.tests.test_artist_album_evidence backend.tests.test_release_track_detail backend.tests.test_recording_track_candidates backend.tests.test_spotify_catalog_backfill.SpotifyCatalogBackfillTests.test_safe_history_spotify_album_repair_applies_duplicate_track_collision backend.tests.test_spotify_catalog_backfill.SpotifyCatalogBackfillTests.test_safe_history_spotify_album_repair_blocks_extra_track_without_album_evidence`
  - `Ran 53 tests ... OK`
- `python3 -m unittest backend.tests.test_recording_track_candidates`
  - `Ran 35 tests ... OK`
- `python3 -m py_compile backend/app/artist_album_evidence.py backend/app/release_track_detail.py backend/app/routes/playback_routes.py backend/app/recording_track_candidates.py backend/app/spotify_catalog_backfill.py backend/app/main.py`
- `npm run build` from `frontend/`
- `git diff --check`

Manual QA completed by user:
- Kutiman & Dekel album/artist/track metadata flow works.
- Telenova recording variation art/date navigation issue works after backend restart/reload.
- `Hope` album subtitle no longer shows `2026 - 2026`.

Checks run for the release-album repair UI / relation badge / track-family list work:
- `npm run build` from `frontend/`
- `git diff --check`

Manual/browser QA completed:
- Identity Audit `Albums -> Repair` dry-run rendered candidates and summary counts without console errors.

Checks run for track modal album/release-view QA batch:
- `python3 -m unittest backend.tests.test_release_track_detail`
- `python3 -m py_compile backend/app/routes/playback_routes.py`
- `npm run build` from `frontend/` after each frontend behavior batch
- `git diff --check`

Checks run for the current source-history / In Rainbows merge batch:
- `python3 -m unittest backend.tests.test_release_track_detail.ReleaseTrackDetailTests.test_valid_release_track_route_returns_stable_shape backend.tests.test_release_track_detail.ReleaseTrackDetailTests.test_equivalent_spotify_album_source_versions_remain_separate_release_sources`
- `python3 -m unittest backend.tests.test_spotify_catalog_backfill.SpotifyCatalogBackfillTests.test_safe_history_spotify_album_repair_applies_duplicate_track_collision backend.tests.test_spotify_catalog_backfill.SpotifyCatalogBackfillTests.test_safe_history_spotify_album_repair_allows_equivalent_spotify_album_id_without_duration`
- `python3 -m unittest backend.tests.test_entity_backfill.EntityBackfillTests.test_merge_conservative_same_album_release_track_duplicates_merges_safe_case backend.tests.test_entity_backfill.EntityBackfillTests.test_merge_conservative_same_album_release_track_duplicates_ignores_duration_mismatch backend.tests.test_entity_backfill.EntityBackfillTests.test_merge_conservative_same_album_release_track_duplicates_merges_same_album_variant_titles backend.tests.test_entity_backfill.EntityBackfillTests.test_merge_conservative_same_album_release_track_duplicates_skips_mono_stereo_split`
- `python3 -m unittest backend.tests.test_recording_track_candidates.RecordingTrackCandidateEndpointTests.test_query_sums_member_play_counts_from_source_tracks`
- `python3 -m py_compile backend/app/spotify_catalog_backfill.py backend/app/db.py backend/app/release_track_metadata.py backend/app/release_track_detail.py`
- `npm run build` from `frontend/`

Browser smoke checks completed:
- local Vite frontend loaded with no console errors after album artist/automatic album fetch changes.

Outstanding QA:
- Authenticated end-to-end browser QA was not rerun after the final source-history cache and release-view exact-history changes.
- Good follow-up manual checks:
  - track with partial local album automatically completes when Spotify is available
  - cooldown mode shows `More tracks on Spotify`
  - release view album-track stars are exact-source only
  - release view listened/listen-count tags are exact-source only
  - recording view stars/listened/listen counts remain aggregate
  - `Radiohead - 15 Step` on `In Rainbows` no longer shows the same album as an extra recording appearance

## Known Limitations
- Frontend bookmark/star behavior is local placeholder UI; bookmark persistence is not implemented.
- Source/text album and track reconciliation are not broadly implemented.
- Catalog backfill remains enrichment-only; complete album tracklists opened from a track overlay can now opt into the separate conservative catalog identity-promotion workflow.
- Generated recording/track-family clusters are caches only; no durable `recording_track` promotion exists.
- `source_track_play_count_cache` is a derived cache; if old local data looks stale, run a projection/refresh path before debugging frontend counts.
- Frontend still lacks focused tests for overlay/tracklist workflows.
- Artist group/member modeling remains intentionally out of scope. Future musician/member graph work is tracked as low-priority future work in `docs/overview/roadmap.md`.

## Recommended Next Task
Recommended project-level next steps:

1. Reproduce the reported BITCRUSH wrong-track display and capture the visible rows plus selected preview/source album ids; the backend cache currently returns the correct three tracks.
2. Re-run authenticated browser QA for exact release-source rows, aggregate recording rows, `Song Family`, and the collapsible `Also Appears On` album footer.
3. Push this branch after the commit if remote backup/review is desired.
4. If desired, run the release-album history/Spotify repair write path only after reviewing the 28 safe candidates from the dry-run.
5. Continue source/text identity reconciliation for albums and tracks:
   - album duplicate repair beyond the safe history/Spotify merge path
   - track duplicate repair using title + artist + album + duration/ISRC/context evidence
   - promote only through explicit dry-run/apply flows, not automatic broad mutation
6. Extract more detail modal/preview logic out of `frontend/src/App.tsx` before adding new UI surface.
7. Add focused frontend tests for modal navigation regressions:
   - track -> album -> artist -> album
   - recording variation A -> variation B -> A
   - album heading year/name formatting

## Guardrails
- Do not delete old branches or stashes unless explicitly requested.
- Do not merge to `main` unless explicitly requested.
- Keep `frontend-app-refactor` as the integration baseline.
- Backend owns source/release/recording/family identity evidence and future promotion semantics.
- Keep Spotify track id / URI as concrete playback identity.
- Do not make `recording_track` the default aggregation layer without explicit scope.
- Keep catalog backfill enrichment-only; identity mutation belongs in explicit dry-run/apply repair or promotion flows.

## Resume Prompt
Continue in `/Users/kahntra/Programming/Personal Projects/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch is `frontend-app-refactor`. The current batch adds Activity completion markers and independent filters, best-effort Listen Log recent sync, artist-promotion skip telemetry/UI, cross-credit Track Family candidates, `Song Family` relationship badges, a collapsible album-box `Also Appears On` footer, explicit track-overlay catalog identity promotion, and separate recording-group album-row history. Focused backend tests (66), `py_compile`, frontend build, and `git diff --check` passed. The unresolved first task is the user report that BITCRUSH shows the wrong tracks: direct backend inspection of Spotify album `6LU67HlNUaukF21Tr6ymuD` still returns `BITCRUSH`, `MOUNTAIN LION // ADORE`, and `PARALYSIS GHOSTS`, so capture the visible frontend rows and selected preview/source album ids before changing identity data.
