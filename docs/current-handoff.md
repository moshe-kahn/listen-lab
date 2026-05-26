# Current Handoff

## Read First
Start here in a new chat, then open only the docs relevant to the requested task.

Recommended topic docs:
- `docs/reference/source-track-resolution-policy.md` for source/release/recording/track-family identity policy.
- `docs/reference/drafts/entity-model-draft.md` for the proposed long-term identity model.
- `docs/reference/spotify-catalog-backfill.md` for cached Spotify catalog and album tracklist behavior.
- `docs/reference/refactor-notes.md` for frontend/playback refactor and route extraction notes.
- `docs/reference/raw-ingest.md` for raw recent/history ingest and canonical play-event projection.
- `docs/reference/album-family-review-policy.md` only when working on album-family candidate review.

Avoid reading every doc by default.

## Current State
Active branch: `frontend-app-refactor`.

This handoff covers uncommitted identity-audit work after manual review of 15 recording-track candidates, release-track duration repair, and a new duration-conflict review tab.

Latest committed baseline before this work:
- `c6676a8` `Simplify release-track overlay source display`
- `6510bb4` `Aggregate release-track listens in frontend views`
- `b7f620b` `Update release-track overlay handoff`
- `2d2a8b5` `Show release-track detail in track overlays`
- `072e863` `Add release-track detail endpoint`

Important local instruction:
- `AGENTS.md` says substantial frontend UI should not be added directly to `frontend/src/App.tsx`; keep `App.tsx` for routing/layout/wiring, feature UI under `frontend/src/components/...`, API calls in `frontend/src/api/appApi.ts`, and shared types in `frontend/src/types/appTypes.ts`.

## Recording-Track Candidate Rules
Hierarchy direction remains:

`source_track -> release_track -> recording_track -> track_family`

No durable `recording_track` table, promotion/apply endpoint, overlay integration, playback change, or default aggregation change exists yet. Candidate and review behavior is still read-only/debug-only.

Manual review outcomes were encoded into classifier policy:
- Same full variant label plus strong evidence can form a recording-track subgroup.
- Mixed recording-distinct variant labels become `track_family_candidate`, not one recording candidate.
- Named/attributed mixes or versions such as `Spike Stent Mix` vs `Alchemist x Trooko Version` split at recording level but can remain related at family level.
- Structural labels such as `Part 1`, `Part 2`, `pt. 1`, intro/interlude/skit/reprise are family/segment relations, not recording merges.
- Instrumental variants are separate recording/listening objects by default, usually same Track Family.
- Explicit/clean variants can still be same `recording_track`; preserve content-rating metadata for frontend filtering/playback preference.
- Mono/stereo or format variants can remain same `recording_track` when evidence agrees; prefer clean/base title for display.
- Rerelease/remaster belongs before single in representative fallback order.

Representative selection now prefers:
1. source-backed original album
2. rerelease/remaster
3. single
4. soundtrack
5. compilation

It also prefers clean/base titles over format/remaster suffixes when otherwise compatible.

Current real candidate counts after duration repair/rule changes:
- total candidate groups: `1240`
- recording-track candidates: `862`
- track-family candidates: `378`
- safe recording candidates: `302`
- needs-review recording candidates: `560`
- needs-review family candidates: `378`
- safe candidates represent `626` unique release tracks and `709` unique source tracks
- needs-review recording candidates represent `1050` unique release tracks and `1095` unique source tracks
- family candidates represent `881` unique release tracks and `964` unique source tracks

Backend endpoints:
- `GET /debug/tracks/recording-track-candidates`
- `GET /debug/tracks/recording-track-candidates/summary`
- `POST /debug/tracks/recording-track-candidate-reviews`
- `GET /debug/tracks/recording-track-candidate-reviews`
- `GET /debug/tracks/recording-track-candidate-reviews/{id}`

## Duration Repair And Conflicts
Found a propagation gap:
- many release tracks had accepted Spotify mappings and `spotify_track_catalog.duration_ms`, but `release_track.duration_ms` stayed null.
- `_upsert_track_catalog(...)` now fills missing `release_track.duration_ms` for accepted Spotify mappings.
- Existing non-null release durations are preserved.

One-time repair was applied to the local DB:
- `3833` `release_track.duration_ms` values filled.
- `49` release tracks skipped because accepted mapped Spotify catalog durations differ by more than `2000ms`.
- release tracks with duration went from `33` to `3866`.
- remaining close-match repair candidates: `0`.

New backend helper/endpoint:
- `repair_release_track_durations_from_spotify_catalog(...)`
- `query_release_track_duration_conflicts(...)`
- `GET /debug/tracks/release-track-duration-conflicts`

Duration-conflict endpoint is read-only:
- uses SQLite only
- does not call Spotify
- does not mutate identity
- returns each conflicting release track with accepted Spotify source tracks, durations, album context, ISRC, explicit flag, and Spotify URLs.

## Frontend Identity Audit
Existing tab:
- Tracks -> `Recording Tracks`
- Component: `frontend/src/components/identityAudit/RecordingTrackCandidatesTab.tsx`
- Saves review decisions only; does not apply identity changes.

New tab:
- Tracks -> `Duration Conflicts`
- Component: `frontend/src/components/identityAudit/ReleaseTrackDurationConflictsTab.tsx`
- Lists the 49 release-track duration conflicts skipped by repair.
- Each source track links directly to Spotify with `https://open.spotify.com/track/...`.
- Shows source duration, album, release date, album type, explicit flag, ISRC, and match method.

`App.tsx` only wires the tab into routing; feature UI is isolated in the component.

Frontend deliberately still does not include:
- accept/reject/apply identity mutation
- schema promotion
- overlay integration
- playback changes
- default aggregation changes

## Files Changed
Backend:
- `backend/app/recording_track_candidates.py`
- `backend/app/routes/audit_routes.py`
- `backend/app/spotify_catalog_backfill.py`
- `backend/tests/test_recording_track_candidates.py`
- `backend/tests/test_spotify_catalog_backfill.py`
- `backend/tests/test_track_identity_audit_routes.py`

Frontend:
- `frontend/src/components/identityAudit/ReleaseTrackDurationConflictsTab.tsx`
- `frontend/src/components/identityAudit/IssueFeed.tsx`
- `frontend/src/api/appApi.ts`
- `frontend/src/types/appTypes.ts`
- `frontend/src/App.tsx`
- `frontend/src/styles.css`

Docs:
- `docs/current-handoff.md`
- `docs/overview/context.md`
- `docs/reference/source-track-resolution-policy.md`

Note: older uncommitted recording-track review-persistence files from the previous handoff may still be part of the broader branch history/scope if not already committed.

## Verification
Backend tests/checks run and passed:
- `./.venv/bin/python -m unittest backend.tests.test_recording_track_candidates`
- `./.venv/bin/python -m unittest backend.tests.test_recording_track_candidate_reviews`
- `./.venv/bin/python -m unittest backend.tests.test_track_identity_audit_routes`
- `./.venv/bin/python -m unittest backend.tests.test_spotify_catalog_backfill.SpotifyCatalogBackfillTests.test_query_release_track_duration_conflicts_returns_spotify_links`
- focused duration repair tests:
  - `test_repair_release_track_durations_from_spotify_catalog_updates_close_matches`
  - `test_repair_release_track_durations_from_spotify_catalog_skips_large_conflicts`
  - `test_upsert_track_catalog_populates_missing_release_track_duration`
  - `test_upsert_track_catalog_preserves_existing_release_track_duration`
- `./.venv/bin/python -m py_compile backend/app/spotify_catalog_backfill.py backend/app/recording_track_candidates.py backend/app/routes/audit_routes.py backend/tests/test_spotify_catalog_backfill.py backend/tests/test_recording_track_candidates.py`

Frontend/checks run and passed:
- `npm run build --prefix frontend`
- `git diff --check`

Vite still reports the existing large chunk warning after frontend builds.

Browser note:
- Vite was started briefly for verification and then stopped.
- The app could not fully render Identity Audit in that browser session because backend/auth was not running there, but the endpoint and frontend build passed.
- User later confirmed the page worked once frontend/backend were up.

## Known Limitations
- Candidate discovery remains debug/read-only and intentionally conservative.
- Saved recording-track reviews are audit metadata only; they do not drive classifier tuning, identity promotion, or aggregation yet.
- Different-ISRC compatible remasters/reissues stay review-required.
- Frontend review filters for evidence bucket, ISRC state, and sorting apply to the currently loaded candidate page.
- Becca Mancari-style album/single representative choice can still be weak when album metadata lacks album type/release dates.
- The 49 duration conflicts are review-only; no duration was chosen for them.
- Frontend still lacks a dedicated test harness for Identity Audit components; verification is build/type based plus backend tests.

## Recommended Next Task
Commit this work after reviewing the staged-ready scope.

Then use Identity Audit -> Tracks -> Duration Conflicts to review the 49 duration conflicts and decide whether each is:
- same release track with metadata drift
- wrong release-track merge
- alternate recording/version that should split
- needs more Spotify/album evidence

Other possible follow-ups:
- Add focused frontend tests for `ReleaseTrackDurationConflictsTab` and `RecordingTrackCandidatesTab`.
- Add a backend export/report endpoint for duration conflicts and reviewed recording-track candidates if manual review volume grows.
- Add durable `recording_track` schema only after review findings justify promotion semantics.

## Guardrails
- Do not delete old branches or stashes unless explicitly requested.
- Do not merge to `main` unless explicitly requested.
- Keep `frontend-app-refactor` as the integration baseline.
- Backend owns source/release/recording/family identity evidence and future promotion semantics.
- Keep Spotify track id / uri as the playback identity.
- Saved recording-track reviews are not active identity.
- Do not make `recording_track` the default aggregation layer without explicit scope.
- Do not add apply, promotion, overlay, playback, or aggregation behavior based only on saved reviews.

## Resume Prompt
Continue in `/Users/kahntra/Programming/Personal Projects/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch is `frontend-app-refactor`. Recording-track candidate rules were tuned from 15 manual reviews: mixed recording-distinct variant labels now become Track Family candidates, structural parts are family/segment relations, explicit/clean can remain same recording with metadata preserved, mono/stereo can remain same recording, and representative selection prefers album -> rerelease/remaster -> single -> soundtrack -> compilation. A catalog propagation gap was fixed: future Spotify track catalog writes fill missing `release_track.duration_ms`, and a one-time repair filled 3833 existing conservative durations while leaving 49 conflicts. New endpoint `GET /debug/tracks/release-track-duration-conflicts` and frontend tab Identity Audit -> Tracks -> Duration Conflicts show those conflicts with clickable Spotify track links. Verification passed with backend candidate/review/route/catalog tests, py_compile, frontend build, and `git diff --check`. Next task: review the 49 duration conflicts or commit the current work.
