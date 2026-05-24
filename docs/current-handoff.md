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
- `8668edf` `Group Activity listens by release track`

Scope in latest commit:
- Track preview payloads now expose release-track metadata at top level on `PreviewItem`.
- Track detail overlays use those explicit preview fields for RT badge/source-count reads before falling back to the existing Spotify track-id metadata lookup.
- Track overlays show a small read-only note, `Grouped with N source versions`, when the selected preview has release-track siblings.
- Spotify `track_id` / `uri` remain the playback identity.
- Overlay title, artist, album, image, and Spotify URL behavior remain source/representative-row based.
- No backend endpoint, identity write, merge, promotion, or Activity grouping behavior was changed in this scope.

Known local processes:
- Codex started frontend Vite on `127.0.0.1:5174` for QA and stopped it.
- Backend was already running on `127.0.0.1:8000` during checkpoint QA and returned HTTP 200 for `/auth/session`; Codex did not start or stop that process. It was no longer listening during the final pre-commit recheck.

## Work Completed
Committed before this handoff:
- Phase 3 Activity/Listened display grouping now prefers internal `release_track_id`.
- `activityRecentTrackKey` in `frontend/src/utils/playbackUtils.ts` groups Activity rows by:
  1. `release_track:<release_track_id>` when the id is a positive finite number.
  2. `spotify_track:<track_id>` when Spotify/source track id is present.
  3. normalized `track_name` / `artist_name` text key.
- Grouped Activity rows still use an actual `RecentTrack` representative row, preserving Spotify `track_id` / `uri` for playback actions.
- Player recent dedupe remains Spotify-first.

Current overlay polish:
- Added top-level optional `PreviewItem` fields:
  - `releaseTrackId`
  - `releaseTrackName`
  - `releaseTrackSourceCount`
  - `hasReleaseTrackSiblings`
- `DashboardListCard` copies those fields from `previewTrack` when building a track preview.
- `selectedPreviewReleaseSiblingSourceCount` and `selectedPreviewHasReleaseSibling` prefer the explicit preview fields.
- The track overlay displays `Grouped with N source versions` only for track previews with sibling source versions.
- `sourceTrack`, `trackId`, `url`, `trackUri`, and playback handling are preserved.

## Files Changed
Frontend:
- `frontend/src/App.tsx`
- `frontend/src/components/dashboard/DashboardListCard.tsx`
- `frontend/src/styles.css`
- `frontend/src/types/appTypes.ts`

Docs:
- `docs/current-handoff.md`
- `docs/overview/architecture.md`
- `docs/overview/context.md`
- `docs/reference/drafts/entity-model-draft.md`
- `docs/reference/refactor-notes.md`
- `docs/reference/source-track-resolution-policy.md`

## Verification
Passed during this scope:
- `npm run build --prefix frontend`
- `git diff --check`
- Backend smoke during checkpoint: `GET /auth/session` returned HTTP 200 with authenticated session.

Manual browser QA:
- Activity/Listened rendered.
- Activity row `Lory` opened the track overlay.
- Activity sibling row `Dreaming` opened the track overlay with Spotify track URL `https://open.spotify.com/track/3gLacTBcajZXTxzBWfDRTK`.
- `Dreaming` overlay showed RT badge and `Grouped with 4 source versions`.
- `Play in ListenLab` used the Spotify track URI/ID path and updated player state during QA.
- Liked stars rendered in overlay, album rows, player, and queue.
- Artist overlay from a track artist opened and showed album entries.
- Browser console warn/error logs were empty.

Known limitation:
- Final pre-commit backend recheck could not connect because no process was listening on `127.0.0.1:8000`; earlier checkpoint smoke had passed while the existing backend was running.
- A fresh Activity list DOM snapshot did not expose a liked star on the visible row checked during QA, while the same track showed liked state correctly in overlay/album/player surfaces. Treat this as a residual visual/DOM verification gap, not a confirmed regression.
- No frontend unit tests were added because this repo has no frontend test harness or existing `*.test.ts(x)` pattern.

## Recommended Next Task
After commit, start a new chat for the next task to reduce stale context.

Suggested follow-ups:
1. Decide whether to group Activity `Liked` by `release_track_id`, with explicit wording because Spotify liked-track count and internal liked-release-track count can differ.
2. If overlay identity work continues, design a read-only release-track detail endpoint separately from playback behavior.
3. Consider adding a frontend test harness before more UI identity behavior changes.

## Guardrails
- Do not delete old branches or stashes unless explicitly requested.
- Do not merge to `main` unless explicitly requested.
- Keep `frontend-app-refactor` as the integration baseline.
- Backend owns source-track to release-track identity mapping.
- Do not make frontend-only identity guesses when backend catalog or identity evidence is available.
- Keep Spotify track id / uri as the playback identity even when display grouping uses `release_track_id`.
- Do not add merge, promotion, or write behavior for release-track identity without explicit scope.

## Resume Prompt
Continue in `/Users/kahntra/Programming/Personal Projects/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch is `frontend-app-refactor`. Activity/Listened display grouping by `release_track_id` is committed in `8668edf`; overlay source-version polish is the latest commit, `Show release-track source versions in overlay`. The latest scope adds top-level release-track metadata to `PreviewItem`, populates it from representative `RecentTrack` rows, and shows a read-only track overlay note like `Grouped with 4 source versions` when release-track siblings exist. Playback still uses Spotify track id/URI. Verification passed with `npm run build --prefix frontend`, `git diff --check`, backend `/auth/session` smoke during checkpoint, and browser QA for Activity overlays, the source-version note, liked badges, player/queue, and artist album overlay.
