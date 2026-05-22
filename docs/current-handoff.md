# Current Handoff

## Read First
Start here in a new chat, then open only the docs relevant to the requested task.

Recommended topic docs:
- `docs/reference/refactor-notes.md` for frontend/playback refactor and route extraction notes.
- `docs/reference/raw-ingest.md` for raw play events, recent/history ingest, recent-sync throttling, and fallback history text.
- `docs/reference/spotify-catalog-backfill.md` for catalog enrichment, album-track fallback/cache, lookup, and backfill invariants.
- `docs/reference/drafts/entity-model-draft.md` for release/source/analysis identity and duplicate diagnostics.
- `docs/reference/drafts/identity-audit-submission-contract.md` only when working on saved track-audit submissions.
- `docs/reference/album-family-review-policy.md` only when working on album-family candidate review.

Avoid reading every doc by default.

## Current State
Active branch: `frontend-app-refactor`.

Latest committed baseline before this handoff update:
- `af2ebf8` `Add liked-track cache sync and refine playback queue UI`

This session prepared a follow-up commit with liked-cache artwork, Activity liked/listened UI refinements, shared liked stars, and release-track sibling metadata badges.

Known local processes:
- Existing backend/frontend dev servers may be listening on `127.0.0.1:8000` and `127.0.0.1:5173` from outside this session.
- No new long-running servers were started by Codex for the final work in this session.

## Work Completed
Liked-track cache enrichment:
- `spotify_liked_track_cache` now stores `album_image_url` and `artist_ids`.
- Liked-track cache responses return album art and structured artist IDs/images when available.
- Added `backend/app/artwork.py` for cache-first track and artist artwork resolution.
- `GET /me/liked-tracks` resolves missing album/artist artwork from local cache/catalog first and only uses Spotify fetches when needed and auth is available.

Liked-track Activity UI:
- Frontend now loads all cached liked-track pages through `fetchAllLikedTracks`, using backend paging.
- Activity `Liked` is scrollable, not paginated.
- Activity liked controls support:
  - `100` vs total cached-like count
  - `Recent` vs `Older`
  - ordered vs shuffle icon toggle
  - repeated shuffle clicks reshuffle again
- `Refresh` runs a full liked-track sync, then reloads the local liked cache.
- Cache status copy shows cached count and whether full refresh completed or stopped partially.

Liked stars:
- Added shared `LikedBadge`.
- Liked stars show across dashboard track cards, Activity Listened/Liked, Tracks views, player titles, queue rows, recent dropdown rows, track detail overlays, and album tracklists.
- Liked state is still primarily Spotify-track-ID based, with fallback to liked-cache source rows. The next semantic improvement should move this to release-track-level identity.

Album and artist artwork:
- Album art now flows from liked cache rows and resolver fallback paths.
- Artist art resolver checks local/static cache first, then bounded Spotify artist fetches by artist ID.
- Album tracklists can benefit from cached metadata where available.

Release-track sibling badges:
- Added `backend/app/release_track_metadata.py` shared helper.
- Added `POST /tracks/release-track-metadata`.
- Album track endpoint enriches rows with release-track metadata.
- Frontend fetches release-track metadata in batches and remembers checked IDs, including non-matches, so later IDs are not starved behind early misses.
- Shared `ReleaseSiblingBadge` renders `RT` where a Spotify track maps to an accepted local `release_track` with multiple accepted source tracks.
- `RT` is currently source-map backed:
  - requires `source_track_map.status = 'accepted'`
  - does not require `is_user_confirmed = 1`
  - does not include suggested/rejected/unaccepted candidates

Important product direction identified:
- The UI should eventually use internal `release_track_id` as the primary song identity, not Spotify track ID.
- Desired behavior:
  - if any sibling version is liked, the release track is liked
  - listening to sibling Spotify IDs counts as listening to the same song
  - Activity Liked/Listened should group by `release_track_id`
  - Spotify IDs remain playback/version identifiers, not UI identity

## Verification
Passed during this session:
- `python3 -m unittest backend.tests.test_liked_tracks`
- `python3 -m py_compile backend/app/artwork.py backend/app/liked_tracks.py backend/app/main.py backend/app/db.py`
- `python3 -m py_compile backend/app/release_track_metadata.py backend/app/routes/playback_routes.py backend/app/main.py`
- `npm run build --prefix frontend`
- `git diff --check`

Manual/database checks:
- User reported full liked refresh completed with `3,129` cached liked songs.
- Local DB check found `1,706` accepted multi-source release tracks and `3,597` Spotify IDs in those groups.
- `Rain Dog` maps to release track `284` with four accepted Spotify source IDs.
- Backend metadata helper returned expected `has_release_track_siblings: true` for known `Rain Dog`/sibling IDs.

Not fully verified:
- Browser QA after the final release-track metadata batching fix.
- Browser layout QA for album tracklist liked column and `RT` badges.
- End-to-end release-track grouping semantics; current UI is not yet release-track-level for liked/listened aggregation.

## Recommended Next Task
Move liked/listened semantics from Spotify track ID to internal release-track identity.

Suggested phased approach:
1. Add `release_track_id`, `release_track_name`, source count, and release-track-level liked state to all track payloads where possible.
2. Derive liked state from release track: if any accepted source Spotify ID under a release track is liked, the release track is liked.
3. Group Activity `Listened` by `release_track_id`, falling back to Spotify track ID or normalized title/artist only when no release mapping exists.
4. Group Activity `Liked` by `release_track_id`; make labels clear because Spotify liked-song count and internal liked-release-track count can differ.
5. Update queue/player/detail edge cases to display release-track-level liked state while preserving Spotify URI for playback.

Do not implement this as frontend title/artist guessing. The backend owns the mapping and should return explicit release-track identity/state.

## Guardrails
- Do not delete old branches or stashes unless explicitly requested.
- Do not merge to `main` unless explicitly requested.
- Keep `frontend-app-refactor` as the integration baseline.
- Old topic branches should remain archive/source-only.
- If the user says `end session`, update this file and provide a short resume prompt.
- If the user says `end session and commit`, update this file and commit only after the user explicitly confirms. In this session, the user confirmed with `end session and commit`.

## Resume Prompt
Continue in `/Users/kahntra/Documents/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch is `frontend-app-refactor`. The latest work committed after `af2ebf8` adds liked-cache artwork enrichment, full cached-liked loading, Activity Liked/Listened UI refinements, shared liked stars, and release-track sibling `RT` metadata badges. Verification passed with targeted backend tests/py_compile, frontend build, and `git diff --check`. Browser QA after the final `RT` batching fix was not performed. Recommended next task: make frontend liked/listened semantics release-track-level instead of Spotify-track-ID-level.
