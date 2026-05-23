# Current Handoff

## Read First
Start here in a new chat, then open only the docs relevant to the requested task.

Recommended topic docs:
- `docs/reference/raw-ingest.md` for raw recent/history ingest, canonical play-event projection, and the music-only fact boundary.
- `docs/reference/drafts/entity-model-draft.md` for source/release/analysis track identity.
- `docs/reference/source-track-resolution-policy.md` for source-track/release-track/Track Family review policy.
- `docs/reference/spotify-catalog-backfill.md` for catalog enrichment and backfill invariants.
- `docs/reference/refactor-notes.md` for frontend/playback refactor and route extraction notes.
- `docs/reference/drafts/identity-audit-submission-contract.md` only when working on saved track-audit submissions.
- `docs/reference/album-family-review-policy.md` only when working on album-family candidate review.

Avoid reading every doc by default.

## Current State
Active branch: `frontend-app-refactor`.

Latest committed baseline before this handoff update:
- `af2ebf8` `Add liked-track cache sync and refine playback queue UI`

Current uncommitted scope is ready to commit:
- release-track identity enrichment across backend payloads
- release-track-aware liked-star checks in frontend
- Activity/Listened release-track coverage audit endpoint
- automatic release-track identity creation during play-event projection
- one-time local cleanup/backfill of missing release-track identities
- music-only fact boundary for history projection, preserving raw podcast/unidentifiable history rows but excluding them from `fact_play_event`

Known local processes:
- No new long-running dev server was started by Codex for this scope.

## Work Completed
Release-track payload identity:
- Added `release_track_id`, `release_track_name`, `release_track_source_count`, and `has_release_track_siblings` to track-like payloads where mapping is available.
- Enrichment is shared through `backend/app/release_track_metadata.py`.
- Payloads touched include recent/listened rows, liked-cache rows, top/all-time rows, album track rows, player/current playback rows, and local `/me` payload sections.
- Existing Spotify track IDs and playback URIs remain in payloads for playback/version identity.

Release-track-aware liked state:
- Frontend liked checks now prefer `release_track_id` and fall back to Spotify track ID.
- If any accepted Spotify source under a release track is liked, sibling rows display as liked.
- No Activity grouping, liked-cache semantic grouping, play-count aggregation, or ranking formula changes were made.

Activity/Listened coverage audit:
- Added read-only endpoint:
  - `GET /debug/activity/release-track-coverage?activity_limit=50&backing_limit=1000&sample_limit=5`
- The audit reports visible Activity coverage and backing play-event coverage, sibling groups that would collapse, missing examples, and suspicious groups.
- After cleanup, local sample coverage is:
  - visible Activity sample: `50/50` release-track mapped
  - backing 1,000 music fact sample: `1000/1000` release-track mapped

Automatic identity creation:
- `backend/app/play_event_projector.py` now ensures release-track identity while projecting music facts.
- If `spotify_track_id` exists, projection creates/reuses exact Spotify provider identity:
  - `source_track.source_name = 'spotify'`
  - accepted `source_track_map`
  - linked `release_track`
- If no Spotify track ID exists but track text exists, projection uses the existing local `history_raw` text fallback path.
- Backend enrichment can now resolve no-Spotify-ID rows through the same `history_raw` key.

Local cleanup/backfill already run:
- `backfill_fact_play_event_release_track_identity()` scanned `76,113` projected play events.
- It created:
  - `96` release tracks
  - `96` source tracks
  - `96` track maps
- It found `0` scannable music facts without identity after the run.

Music-only fact boundary:
- Raw Spotify history remains source-faithful and preserves podcast/unidentifiable rows.
- `fact_play_event` is now treated as the music fact table.
- History projection skips rows when:
  - `spotify_episode_uri` is present, or
  - there is no Spotify track ID, no Spotify track URI, and no raw track name.
- Local cleanup already removed derived non-music facts while preserving raw rows:
  - raw podcast episode rows preserved: `390`
  - projected podcast facts now: `0`
  - projected unidentifiable history facts now: `0`
  - fact rows with no music identity now: `0`

## Files Changed
Backend:
- `backend/app/activity_release_track_audit.py`
- `backend/app/liked_tracks.py`
- `backend/app/main.py`
- `backend/app/merged_track_aggregate.py`
- `backend/app/play_event_projector.py`
- `backend/app/recent_top_tracks_db.py`
- `backend/app/recent_tracks_db.py`
- `backend/app/release_track_metadata.py`
- `backend/app/routes/admin_routes.py`
- `backend/app/spotify_current_playback.py`
- `backend/app/track_sections.py`

Frontend:
- `frontend/src/App.tsx`
- `frontend/src/components/dashboard/DashboardTrackColumn.tsx`
- `frontend/src/types/appTypes.ts`
- `frontend/src/utils/playbackUtils.ts`

Tests/docs:
- `backend/tests/test_merged_track_aggregate.py`
- `backend/tests/test_play_event_projection.py`
- `docs/current-handoff.md`
- relevant overview/reference docs updated for this scope

## Verification
Passed:
- `python3 -m py_compile backend/app/play_event_projector.py backend/app/activity_release_track_audit.py backend/tests/test_play_event_projection.py backend/tests/test_merged_track_aggregate.py`
- `python3 -m unittest backend.tests.test_liked_tracks backend.tests.test_recent_top_tracks_db backend.tests.test_merged_track_aggregate backend.tests.test_play_event_projection`
- `npm run build --prefix frontend`
- `git diff --check`

Manual/database checks:
- Release-track identity backfill created `96` missing mappings.
- Activity release-track audit now reports `100%` coverage for both visible Activity sample and backing music fact sample.
- Raw podcast rows remain in `raw_spotify_history`; no podcast rows remain projected into `fact_play_event`.

Not fully verified:
- Browser QA of release-track-aware liked stars after the latest backend cleanup.
- Browser QA of Activity layout after future grouping, because grouping has not been implemented yet.

## Recommended Next Task
Phase 3: group Activity `Listened` display rows by `release_track_id`.

Implementation plan:
1. Group only Activity/Listened display rows, not raw play-event history.
2. Use `release_track_id` as the preferred grouping key.
3. Fallback to Spotify track ID, then existing normalized title/artist behavior only when release identity is missing.
4. Preserve event/play counts by summing grouped rows.
5. Use latest play as the representative row for display and playback.
6. Keep playback on concrete Spotify track ID/URI.
7. Add focused tests for grouping, fallback, count preservation, and latest-play representative selection.

Do not bundle this with Activity `Liked` grouping. Keep `Liked` as Phase 4.

## Future Follow-Up After Current Project
Explore sibling/family album artwork near the home playback album-art expansion.

Do not implement this as a frontend-only guess. First add a read-only backend/debug helper that, given a Spotify track ID or `release_track_id`, returns related album appearances:
- exact release-track siblings first
- then Track Family / `analysis_track` related songs if available
- album art, album name, source track ID/URI, and relationship reason
- duplicate album-art suppression
- playable representative Spotify URI when available

After inspecting real payloads, design a small album-art strip beside the current playback album art. Clicking an item should have explicit behavior: inspect/switch expanded album, not silently mutate playback.

## Guardrails
- Do not delete old branches or stashes unless explicitly requested.
- Do not merge to `main` unless explicitly requested.
- Keep `frontend-app-refactor` as the integration baseline.
- Raw history is source-faithful. Do not delete podcast rows from `raw_spotify_history`; exclude them only from music facts/queries unless building podcast features.
- Do not implement release-track grouping as frontend title/artist guessing. Backend owns identity mapping.

## Resume Prompt
Continue in `/Users/kahntra/Documents/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch is `frontend-app-refactor`. Latest completed scope adds release-track identity payload enrichment, release-track-aware liked stars, Activity release-track coverage audit, automatic release-track identity creation during music fact projection, local identity backfill, and music-only fact cleanup that preserves raw podcast history but removes podcast/unidentifiable rows from `fact_play_event`. Verification passed with targeted backend tests, py_compile, frontend build, and `git diff --check`. Recommended next task: Phase 3, group Activity `Listened` display rows by `release_track_id`.
