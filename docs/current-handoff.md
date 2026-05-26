# Current Handoff

## Read First
Start in `/Users/kahntra/Programming/Personal Projects/ListenLab/listen-lab-main`, then read `AGENTS.md` and this file.

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

This handoff covers committed identity-audit work after manual review of 15 recording-track candidates, release-track duration repair/provenance, priority catalog backfill queueing for identity candidates, and duration-conflict review.

Important local instruction:
- `AGENTS.md` says substantial frontend UI should not be added directly to `frontend/src/App.tsx`; keep `App.tsx` for routing/layout/wiring, feature UI under `frontend/src/components/...`, API calls in `frontend/src/api/appApi.ts`, and shared types in `frontend/src/types/appTypes.ts`.

## Recording-Track Candidate Rules
Hierarchy direction remains:

`source_track -> release_track -> recording_track -> track_family`

No durable `recording_track` table, promotion/apply endpoint, overlay integration, playback change, or default aggregation change exists yet. Candidate and review behavior is still read-only/debug-only.

Manual review outcomes encoded into classifier policy:
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

## Duration Repair, Provenance, And Conflicts
Found a propagation gap:
- many release tracks had accepted Spotify mappings and `spotify_track_catalog.duration_ms`, but `release_track.duration_ms` stayed null.
- `_upsert_track_catalog(...)` now fills missing `release_track.duration_ms` for accepted Spotify mappings and records duration provenance.
- Existing non-null release durations are preserved.

Schema/version note:
- local DB is now schema version `31`.
- `release_track` now has `duration_source`, `duration_confidence`, and `duration_evidence_json`.
- close/agreed catalog durations are marked `catalog_agrees`.
- accepted mappings with conflicting catalog durations are marked `uncertain_catalog_conflict`.

One-time repair/provenance update was applied to the local DB:
- `3833` `release_track.duration_ms` values filled.
- `49` release tracks have accepted mapped Spotify catalog durations differing by more than `2000ms`.
- those 49 retain a representative duration but are explicitly marked `uncertain_catalog_conflict`; the representative value is display/evidence only, not authoritative playback length.
- release tracks with duration went from `33` to `3866`.
- remaining close-match repair candidates: `0`.
- current duration-conflict audit total: `49`.

Backend helper/endpoint:
- `repair_release_track_durations_from_spotify_catalog(...)`
- `query_release_track_duration_conflicts(...)`
- `GET /debug/tracks/release-track-duration-conflicts`

Duration-conflict endpoint is read-only:
- uses SQLite only
- does not call Spotify
- does not mutate identity
- returns each conflicting release track with accepted Spotify source tracks, durations, album context, ISRC, explicit flag, Spotify URLs, `release_track_duration_ms`, `duration_source`, and `duration_confidence`.

Spotify playback-duration note:
- Spotify track catalog `duration_ms` can disagree with the duration shown/experienced during playback.
- A future `observed_playback` duration source can be added if a reliable non-playing or minimally invasive player observation flow is proven.
- Do not treat the longest or shortest catalog value as authoritative without stronger playback evidence.

Catalog queue note:
- `append_candidate_identity_metadata_queue(apply=True)` was applied locally.
- It inserted/upgraded `695` pending `identity_metadata` queue rows at priority `95` for missing source catalog metadata used by recording/family candidate review.
- Candidate-layer missing metadata was concentrated in missing Spotify track catalog rows, not incomplete existing catalog rows.

## Frontend Identity Audit
Existing tab:
- Tracks -> `Recording Tracks`
- Component: `frontend/src/components/identityAudit/RecordingTrackCandidatesTab.tsx`
- Saves review decisions only; does not apply identity changes.

Duration tab:
- Tracks -> `Duration Conflicts`
- Component: `frontend/src/components/identityAudit/ReleaseTrackDurationConflictsTab.tsx`
- Lists the 49 release-track duration conflicts, including rows with filled representative durations marked uncertain.
- Each source track links directly to Spotify with `https://open.spotify.com/track/...`.
- Shows representative duration, confidence, source duration, album, release date, album type, explicit flag, ISRC, and match method.

Frontend deliberately still does not include:
- accept/reject/apply identity mutation
- schema promotion
- overlay integration
- playback changes
- default aggregation changes

## Files Changed In Latest Commit
Backend:
- `backend/app/db.py`
- `backend/app/spotify_catalog_backfill.py`
- `backend/app/spotify_catalog_worker.py`
- `backend/tests/test_entity_backfill.py`
- `backend/tests/test_spotify_catalog_backfill.py`
- `backend/tests/test_spotify_catalog_worker.py`

Frontend:
- `frontend/src/components/identityAudit/ReleaseTrackDurationConflictsTab.tsx`
- `frontend/src/types/appTypes.ts`

Docs:
- `docs/current-handoff.md`
- `docs/overview/context.md`
- `docs/reference/source-track-resolution-policy.md`

## Verification
Backend tests/checks run and passed:
- focused duration/catalog/worker tests, including:
  - `test_upsert_track_catalog_populates_missing_release_track_duration`
  - `test_upsert_track_catalog_preserves_existing_release_track_duration`
  - `test_repair_release_track_durations_from_spotify_catalog_updates_close_matches`
  - `test_repair_release_track_durations_from_spotify_catalog_uses_longest_accepted_conflict`
  - `test_repair_release_track_durations_from_spotify_catalog_marks_existing_conflict_uncertain`
  - `test_query_release_track_duration_conflicts_returns_spotify_links`
  - `test_successful_backfill_runs_release_duration_repair`
- `./.venv/bin/python -m unittest backend.tests.test_spotify_catalog_backfill backend.tests.test_spotify_catalog_worker backend.tests.test_entity_backfill`
- `./.venv/bin/python -m py_compile backend/app/db.py backend/app/spotify_catalog_backfill.py backend/app/spotify_catalog_worker.py`

Frontend/checks run and passed:
- `npm run build` from `frontend/`
- `git diff --check`

Vite still reports the existing large chunk warning after frontend builds.

## Known Limitations
- Candidate discovery remains debug/read-only and intentionally conservative.
- Saved recording-track reviews are audit metadata only; they do not drive classifier tuning, identity promotion, or aggregation yet.
- Different-ISRC compatible remasters/reissues stay review-required.
- Frontend review filters for evidence bucket, ISRC state, and sorting apply to the currently loaded candidate page.
- Becca Mancari-style album/single representative choice can still be weak when album metadata lacks album type/release dates.
- The 49 duration conflicts have representative durations for display but remain uncertain; do not use those values as verified playback lengths.
- Frontend still lacks a dedicated test harness for Identity Audit components; verification is build/type based plus backend tests.

## Recommended Next Task
Use Identity Audit -> Tracks -> Duration Conflicts to continue review of the 49 duration conflicts and decide whether each is:
- same release track with metadata drift
- wrong release-track merge
- alternate recording/version that should split
- needs more Spotify/album evidence

Other possible follow-ups:
- Add focused frontend tests for `ReleaseTrackDurationConflictsTab` and `RecordingTrackCandidatesTab`.
- Add a backend export/report endpoint for duration conflicts and reviewed recording-track candidates if manual review volume grows.
- Add an observed-playback duration source only if Spotify/player behavior can be measured reliably without unwanted playback.
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
Continue in `/Users/kahntra/Programming/Personal Projects/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch is `frontend-app-refactor`. Recording-track candidate rules were tuned from 15 manual reviews: mixed recording-distinct variant labels now become Track Family candidates, structural parts are family/segment relations, explicit/clean can remain same recording with metadata preserved, mono/stereo can remain same recording, and representative selection prefers album -> rerelease/remaster -> single -> soundtrack -> compilation. A catalog propagation gap was fixed: future Spotify track catalog writes fill missing `release_track.duration_ms` with duration provenance, and local repair filled 3833 existing durations. The 49 accepted-mapping duration disagreements remain visible in `GET /debug/tracks/release-track-duration-conflicts` and Identity Audit -> Tracks -> Duration Conflicts; they now have representative durations marked `uncertain_catalog_conflict`. Identity candidate metadata backfill was prioritized with 695 pending `identity_metadata` queue rows at priority 95. Verification passed with affected backend catalog/worker/entity tests, py_compile, frontend build, and `git diff --check`. Next task: continue reviewing duration conflicts or decide whether/how to add a trustworthy observed-playback duration source.
