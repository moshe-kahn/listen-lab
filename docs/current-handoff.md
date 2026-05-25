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

Worktree status at handoff update start:
- Clean before editing this handoff.

Latest commits:
- `072e863` `Add release-track detail endpoint`
- `2d2a8b5` `Show release-track detail in track overlays`

Release-track overlay work is committed end to end:
- Backend exposes a read-only canonical release-track detail endpoint.
- Frontend track overlays fetch that endpoint when a selected preview has `releaseTrackId`.
- Activity/Listened grouping by `release_track_id` remains unchanged.
- Spotify source track id / URI remain the playback identity.
- No write, merge, promotion, raw event mutation, liked sync change, or identity semantics change exists in this scope.

Known local processes:
- Backend and frontend dev servers started for normal-proxy QA were stopped before commit.
- No dev server is intentionally left running.

## Endpoint
Backend endpoint:
- `GET /tracks/release-track/{release_track_id}?context_spotify_track_id=...`

Behavior:
- Returns canonical ListenLab release-track data under `release_track`.
- Returns source-backed display fields under `display`.
- Returns explicit playback choice metadata under `playback`.
- Returns mapped Spotify source versions under `source_versions`.
- Chooses playback from `context_spotify_track_id` when it belongs to the release track and has usable Spotify identity, with `playback.reason = "context_source"`.
- Otherwise chooses a deterministic preferred playable source, with `playback.reason = "preferred_playable_source"`.
- Returns `playback.reason = "unavailable"` with null Spotify identity when no usable source exists.
- Marks source-version rows with `is_context` and `is_playback_choice`.
- Does not call Spotify APIs.
- Does not mutate data.

Backend files from `072e863`:
- `backend/app/release_track_detail.py`
- `backend/app/routes/playback_routes.py`
- `backend/tests/test_release_track_detail.py`

## Frontend Overlay
Frontend behavior from `2d2a8b5`:
- Adds API/types for release-track detail:
  - `fetchReleaseTrackDetail(...)`
  - `ReleaseTrackDetailResponse`
  - source-version and artist detail types.
- When selected track preview has `releaseTrackId`, the overlay fetches release-track detail without blocking the overlay from opening.
- Sends selected preview Spotify track id as `context_spotify_track_id` when available.
- Uses canonical release-track name in the overlay once detail loads.
- Keeps representative/source title, artist, album, image, and Spotify URL as loading/error/no-release fallback.
- Shows `Playing source version: ...` so the playback source remains explicit.
- Uses `releaseTrackDetail.playback.uri` / `spotify_track_id` for `Play in ListenLab` only when detail loads and playback is not unavailable.
- Falls back to the selected preview Spotify playback URI/id when detail is missing, failed, or unavailable.
- Shows a compact source versions section when multiple source versions are present.
- Shows `Selected` for the context source and `Playback` for the chosen playback source.
- Suppresses the older `Grouped with N source versions` note when the full source versions section is loaded.
- Keeps the old note only as fallback before detail is loaded or when detail fails.
- No release-track id is used as a playback identity.

Frontend files from `2d2a8b5`:
- `frontend/src/api/appApi.ts`
- `frontend/src/types/appTypes.ts`
- `frontend/src/App.tsx`
- `frontend/src/styles.css`
- `frontend/src/utils/dashboardUtils.ts`

## Verification
Backend endpoint verification:
- `.venv` unittest passed for `backend.tests.test_release_track_detail`.
- `py_compile` passed for changed backend files.
- `git diff --check` passed.
- Real-data smoke passed on `127.0.0.1:8765` before backend commit:
  - `release_track_id=252`
  - `Innocent Love`
  - context Spotify track `6BE1ayeoIJQHPPqJN79AvU`
  - context source selected with `playback.reason = "context_source"`
  - no-context call returned `preferred_playable_source`
  - invalid id returned `404`

Frontend verification:
- `npm run build --prefix frontend` passed.
- `git diff --check` passed.
- Normal-proxy QA passed with frontend on `127.0.0.1:5174` and backend on `127.0.0.1:8000`.
- Direct backend `8000` checks passed:
  - `/auth/session` returned `200`.
  - `/tracks/release-track/252?context_spotify_track_id=6BE1ayeoIJQHPPqJN79AvU` returned `200`, `playback.reason = "context_source"`, one context flag, and one playback-choice flag.
- Browser QA passed with `Dreaming`:
  - release-track detail loaded through the normal frontend/proxy path.
  - source versions section appeared.
  - exactly one `Selected` badge in the source versions list.
  - exactly one source-version `Playback` badge in the source versions list.
  - old grouped note was not duplicated when full detail loaded.
  - `Play in ListenLab` stayed on Spotify source playback identity, not `releaseTrackId`.
  - browser console warn/error logs were empty.
- No-release fallback QA passed with `Useless Information`:
  - overlay opened without a source versions section.

## Known Limitations
- `playable` is currently null from cached data.
- Display art and Spotify URL are source-version-backed, not canonical release-track fields.
- No write, merge, promotion, raw event mutation, or identity-edit behavior exists.
- Frontend has no dedicated test harness yet, so overlay verification is browser/build based.

## Recommended Next Task
Start a new chat for the next task to reduce stale context.

Recommended next task:
- Add focused frontend tests or a lightweight component/integration test harness for track overlay identity behavior, especially release-track detail loading, playback fallback, and source-version badge rendering.

Other possible follow-ups:
- Decide whether Activity `Liked` should group by `release_track_id`; be explicit because Spotify liked-track count and internal liked-release-track count can differ.
- Improve cached source-version `playable` coverage if reliable local data is available.

## Guardrails
- Do not delete old branches or stashes unless explicitly requested.
- Do not merge to `main` unless explicitly requested.
- Keep `frontend-app-refactor` as the integration baseline.
- Backend owns source-track to release-track identity mapping.
- Do not make frontend-only identity guesses when backend catalog or identity evidence is available.
- Keep Spotify track id / uri as the playback identity even when display grouping uses `release_track_id`.
- Do not add merge, promotion, or write behavior for release-track identity without explicit scope.

## Resume Prompt
Continue in `/Users/kahntra/Programming/Personal Projects/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch is `frontend-app-refactor`; worktree was clean before this handoff update. Latest commits are `072e863 Add release-track detail endpoint` and `2d2a8b5 Show release-track detail in track overlays`. Backend now has read-only `GET /tracks/release-track/{release_track_id}?context_spotify_track_id=...`, returning canonical release-track data, source-backed display fields, explicit playback choice metadata, and mapped source versions with `is_context` / `is_playback_choice` flags. Frontend track overlays fetch this detail when `releaseTrackId` exists, show canonical release-track identity plus compact source versions, keep source-backed art/Spotify URL explicit, and preserve Spotify URI/id playback identity. Verification passed with backend unit/compile checks, real-data smoke for release track `252`, frontend build, `git diff --check`, and normal-proxy browser QA on `5174 -> 8000`. Known limitations: `playable` is currently null from cached data, display art/Spotify URL are source-version-backed, and no write/merge/promotion behavior exists. Recommended next task: add focused frontend tests or a lightweight overlay test harness for release-track detail loading, playback fallback, and source-version badge rendering.
