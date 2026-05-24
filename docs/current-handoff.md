# Current Handoff

## Read First
Start here in a new chat, then open only the docs relevant to the requested task.

Recommended topic docs:
- `docs/reference/refactor-notes.md` for frontend/playback refactor and route extraction notes.
- `docs/reference/source-track-resolution-policy.md` for source-track/release-track identity.
- `docs/reference/spotify-catalog-backfill.md` for cached Spotify catalog and album tracklist behavior.
- `docs/reference/raw-ingest.md` for raw recent/history ingest and canonical play-event projection.
- `docs/reference/album-family-review-policy.md` only when working on album-family candidate review.

Avoid reading every doc by default.

## Current State
Active branch: `frontend-app-refactor`.

Latest committed baseline before this handoff update:
- `ed2514c` `Add artist album evidence overlays`

Current uncommitted scope is ready to commit:
- Phase 3 Activity/Listened display grouping now prefers internal `release_track_id`.
- Spotify `track_id` / `uri` remain the playback identity.
- Fallback behavior remains Spotify track id first, then normalized text identity when no release-track id exists.
- No backend identity, merge, promotion, or write behavior was changed.

Known local processes:
- Codex started frontend Vite on `127.0.0.1:5174` for QA and stopped it.
- Backend was already running on `127.0.0.1:8000`; Codex did not start or stop that process.

## Work Completed
Phase 2 checkpoint:
- Audited release-track-aware liked logic before Phase 3.
- Dashboard cards, Activity liked filter, album rows, track detail overlay, current player, queue views, and recent/player list rows already use release-track-aware liked helpers.
- No liked-state gaps were found.

Phase 3 Activity grouping:
- Added `activityRecentTrackKey` in `frontend/src/utils/playbackUtils.ts`.
- `filterAndDedupeRecentTracksForActivity` now groups Activity rows with this order:
  1. `release_track:<release_track_id>` when the id is a positive finite number.
  2. `spotify_track:<track_id>` when Spotify/source track id is present.
  3. normalized `track_name` / `artist_name` text key.
- The existing player recent dedupe helper still uses the old Spotify-first key so player queue/recent behavior is not unintentionally widened.
- Grouped Activity rows still use an actual `RecentTrack` representative row, preserving Spotify `track_id` / `uri` for playback actions.
- Representative selection behavior is unchanged: best completion ratio wins, with existing order as tie fallback.

Manual browser QA:
- Activity/Listened rendered normally with no obvious blank or duplicate rows in the visible set.
- Liked stars and RT badges rendered.
- Opened Activity row `Dreaming`; detail overlay rendered with Spotify track URL `https://open.spotify.com/track/3gLacTBcajZXTxzBWfDRTK`.
- Clicked `Play in ListenLab`; no browser console errors appeared.
- Album/track overlay still showed liked state, RT badge, album rows, preview/play controls, and played date.

## Files Changed
Frontend:
- `frontend/src/utils/playbackUtils.ts`

Docs:
- `docs/current-handoff.md`
- `docs/reference/refactor-notes.md`
- `docs/reference/source-track-resolution-policy.md`

## Verification
Passed during this scope:
- `npm run build --prefix frontend`
- `git diff --check`

Data/audit checks:
- Existing read-only audit `build_activity_release_track_coverage_audit(activity_limit=200, backing_limit=5000, sample_limit=3)`:
  - visible Activity rows: `200`
  - visible release-track coverage: `100%`
  - visible sibling groups that would collapse: `0`
  - visible missing release-track ids: `0`
  - backing play-event rows: `5000`
  - backing sibling groups: `4`
  - backing missing release-track ids: `0`

Manual QA notes:
- Browser console error/warn log was empty except normal Vite/React development messages.
- Browser performance API resource inspection was not available through the browser runtime, so network failures were checked by visible UI state, console logs, and backend session/API availability.

Not fully verified:
- Actual Spotify playback could not be proven because the app showed `Connecting to Spotify player...`; the click path did not send `release_track_id` visibly and no frontend error appeared.
- No frontend unit tests were added because this repo has no frontend test harness or existing `*.test.ts(x)` pattern.

## Recommended Next Task
Commit this Phase 3 Activity grouping change after user confirmation.

Suggested follow-up after commit:
1. Continue Phase 4 planning for Activity -> Liked grouping by release track.
2. Keep liked-count wording explicit because Spotify liked-song count and grouped release-track count can differ.
3. Consider adding a frontend test harness before the next grouping/refactor step if more UI identity behavior changes are planned.

## Guardrails
- Do not delete old branches or stashes unless explicitly requested.
- Do not merge to `main` unless explicitly requested.
- Keep `frontend-app-refactor` as the integration baseline.
- Backend owns source-track to release-track identity mapping.
- Do not make frontend-only identity guesses when backend catalog or identity evidence is available.
- Keep Spotify track id / uri as the playback identity even when display grouping uses `release_track_id`.
- Do not add merge, promotion, or write behavior for release-track identity without explicit scope.

## Resume Prompt
Continue in `/Users/kahntra/Programming/Personal Projects/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch is `frontend-app-refactor`. Current uncommitted scope changes Activity/Listened display grouping to prefer `release_track_id` via `activityRecentTrackKey` in `frontend/src/utils/playbackUtils.ts`, with Spotify track id and normalized text fallbacks. Playback identity is preserved from the representative `RecentTrack` row. Verification passed with `npm run build --prefix frontend` and `git diff --check`; browser QA passed for Activity row `Dreaming` and its album/track overlay. Next action: commit after explicit user confirmation.
