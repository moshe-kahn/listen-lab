# Current Handoff

## Read First
Start here in a new chat, then open only the docs relevant to the requested task.

Recommended topic docs:
- `docs/reference/source-track-resolution-policy.md` for source/release/recording/track-family identity policy.
- `docs/reference/drafts/entity-model-draft.md` for the proposed long-term identity model.
- `docs/reference/refactor-notes.md` for frontend/playback refactor and route extraction notes.
- `docs/reference/spotify-catalog-backfill.md` for cached Spotify catalog and album tracklist behavior.
- `docs/reference/raw-ingest.md` for raw recent/history ingest and canonical play-event projection.
- `docs/reference/album-family-review-policy.md` only when working on album-family candidate review.

Avoid reading every doc by default.

## Current State
Active branch: `frontend-app-refactor`.

This handoff covers the uncommitted recording-track candidate and review-persistence work that is now ready to commit.

Latest committed baseline before this work:
- `c6676a8` `Simplify release-track overlay source display`
- `6510bb4` `Aggregate release-track listens in frontend views`
- `b7f620b` `Update release-track overlay handoff`
- `2d2a8b5` `Show release-track detail in track overlays`
- `072e863` `Add release-track detail endpoint`

New project instruction:
- `AGENTS.md` now says substantial frontend UI should not be added directly to `frontend/src/App.tsx`; keep `App.tsx` for routing/layout/wiring, feature UI under `frontend/src/components/...`, API calls in `frontend/src/api/appApi.ts`, and shared types in `frontend/src/types/appTypes.ts`.

## Recording-Track Candidate Layer
The current hierarchy direction is:

`source_track -> release_track -> recording_track -> track_family`

Current implementation status:
- `release_track` behavior remains unchanged and conservative.
- No durable `recording_track` entity/table exists yet.
- No promotion/apply endpoint exists.
- No overlay/playback/default aggregation behavior uses `recording_track`.
- The new work is debug/read-only evidence plus saved review judgments only.

Backend candidate endpoint:
- `GET /debug/tracks/recording-track-candidates`

Supported filters:
- `limit`
- `offset`
- `safety_status`
- `candidate_type`
- `relationship_kind`
- `min_confidence`
- `include_track_family_candidates`
- `same_isrc_only`
- `q`
- `artist`

Backend summary endpoint:
- `GET /debug/tracks/recording-track-candidates/summary`

Summary includes:
- total candidate groups
- counts by candidate type, safety status, relationship kind, relationship strength, and evidence bucket
- source/release-track ISRC availability
- same/missing ISRC counts
- top review reasons
- samples for safe, needs-review, track-family, same-ISRC, metadata-missing, conflicting-ISRC-compatible, missing-ISRC-compatible, partial-ISRC, and variant-excluded groups

Classifier/evidence behavior:
- Same ISRC is a high-confidence path.
- Same ISRC is not the only strong path.
- Compatible metadata can also be strong evidence for remasters/reissues/singles/compilations when ISRC differs or is missing.
- Different ISRC remaster/reissue/single-style matches stay review-required unless future manual findings justify a stronger rule.
- Live/demo/acoustic/remix/rerecording/radio-edit style variants route to family-level/review-required classification rather than safe recording-track collapse.

Candidate `evidence_bucket` values:
- `same_isrc`
- `conflicting_isrc_but_compatible_metadata`
- `missing_isrc_but_compatible_metadata`
- `partial_isrc_match`
- `variant_flag_excluded`
- `metadata_review_required`

Real DB inspection before the review UI showed useful signal after catalog ISRC joins:
- total candidate groups: 1237
- recording-track candidates: 939
- track-family candidates: 298
- safe candidates: 255
- needs-review candidates: 982
- same-ISRC candidate groups: 271
- groups with any ISRC evidence: 841
- groups still missing ISRC evidence: 396
- `source_track.isrc` was mostly empty; ISRC evidence primarily comes from `spotify_track_catalog.raw_json.external_ids.isrc`

CLI inspector:
- `./.venv/bin/python -m backend.scripts.inspect_recording_track_candidates --limit 50`
- useful flags include `--same-isrc-only`, `--safety-status`, `--relationship-kind`, `--show-isrc`, and `--show-release-context`

## Review Persistence
Read-only review persistence was added so manual judgments can be saved during inspection.

New schema migration:
- migration `30`
- table `recording_track_candidate_review`

Saved fields:
- `id`
- `candidate_key`
- `decision`
- `reviewer_note`
- `preferred_representative_release_track_id`
- `preferred_playback_source_track_id`
- `candidate_snapshot_json`
- `created_at`
- `updated_at`

Decisions:
- `accepted`
- `rejected`
- `unsure`
- `needs_more_metadata`
- `wrong_representative`
- `maybe_split`
- `maybe_merge_more`

Backend review endpoints:
- `POST /debug/tracks/recording-track-candidate-reviews`
- `GET /debug/tracks/recording-track-candidate-reviews`
- `GET /debug/tracks/recording-track-candidate-reviews/{id}`

Review behavior:
- Saves or updates by `candidate_key`.
- Stores full candidate snapshot JSON so future classifier changes do not erase what was reviewed.
- Does not mutate `source_track`, `release_track`, `analysis_track`, playback, overlay, aggregation, or any future `recording_track` mapping.
- Saved review decisions are metadata only and are not active canonical identity.

## Frontend Identity Audit
New Identity Audit tab:
- Tracks -> `Recording Tracks`
- Component: `frontend/src/components/identityAudit/RecordingTrackCandidatesTab.tsx`

`App.tsx` only wires the tab into the existing high-level Identity Audit routing. The feature UI is isolated in the component.

Frontend behavior:
- Loads candidate summary.
- Loads candidate groups from the debug candidate endpoint.
- Loads saved candidate reviews.
- Shows summary metrics and top review reasons.
- Shows inspection filters for safety status, candidate type, relationship kind, evidence bucket, ISRC state, title query, artist query, review state, and page size.
- Supports local sorting by confidence and member count.
- Shows candidate cards with risk flags, representative diagnostics, why-grouped and why-review lists, and compact expanded release/source member evidence.
- Shows saved review state, note, timestamp, and preferred representative when available.
- Adds review controls:
  - Accept
  - Reject
  - Unsure
  - Needs metadata
  - Wrong representative
  - Maybe split
  - Maybe merge more
  - freeform note
  - optional preferred representative from candidate members
- Labels the controls: `Review decision only - does not apply identity changes.`

Frontend deliberately does not include:
- accept/reject/apply identity mutation
- schema promotion
- overlay integration
- playback changes
- default aggregation changes

## Files Changed
Backend:
- `backend/app/recording_track_candidates.py`
- `backend/app/recording_track_candidate_reviews.py`
- `backend/app/routes/audit_routes.py`
- `backend/app/db.py`
- `backend/scripts/inspect_recording_track_candidates.py`
- `backend/tests/test_recording_track_candidates.py`
- `backend/tests/test_recording_track_candidate_reviews.py`

Frontend:
- `frontend/src/components/identityAudit/RecordingTrackCandidatesTab.tsx`
- `frontend/src/components/identityAudit/IssueFeed.tsx`
- `frontend/src/api/appApi.ts`
- `frontend/src/types/appTypes.ts`
- `frontend/src/App.tsx`
- `frontend/src/styles.css`

Docs/instructions:
- `AGENTS.md`
- `docs/current-handoff.md`
- `docs/overview/context.md`
- `docs/reference/source-track-resolution-policy.md`
- `docs/reference/drafts/entity-model-draft.md`

## Verification
Backend tests/checks run and passed:
- `./.venv/bin/python -m unittest backend.tests.test_recording_track_candidates`
- `./.venv/bin/python -m unittest backend.tests.test_recording_track_candidate_reviews`
- `./.venv/bin/python -m unittest backend.tests.test_track_identity_audit_routes`
- `./.venv/bin/python -m unittest backend.tests.test_release_track_detail`
- `./.venv/bin/python -m py_compile backend/app/recording_track_candidates.py backend/app/recording_track_candidate_reviews.py backend/app/routes/audit_routes.py backend/app/db.py backend/scripts/inspect_recording_track_candidates.py`

Frontend/checks run and passed:
- `npm run build --prefix frontend`
- `git diff --check`

Vite still reports the existing large chunk warning after frontend builds.

## Known Limitations
- Candidate discovery remains debug/read-only and intentionally conservative.
- Different-ISRC compatible remasters/reissues are surfaced as review-required, not safe by default.
- Review persistence is an audit note layer only; it does not yet drive classifier tuning, identity promotion, or aggregation.
- Frontend review filters for evidence bucket, ISRC state, and sorting apply to the currently loaded candidate page.
- Frontend still lacks a dedicated test harness for Identity Audit components; verification is build/type based plus backend route/model tests.

## Recommended Next Task
Start a new chat for the next task to reduce stale context.

Recommended next task:
- Use the Recording Tracks tab to inspect saved reviews and collect manual findings by evidence bucket. After there are real accepted/rejected patterns, tune classifier thresholds/rules with tests.

Other possible follow-ups:
- Add focused frontend tests for `RecordingTrackCandidatesTab`.
- Add a backend export/report endpoint for reviewed recording-track candidates if manual review volume grows.
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
Continue in `/Users/kahntra/Programming/Personal Projects/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch is `frontend-app-refactor`. Recording-track candidate/debug work is now implemented: backend exposes `GET /debug/tracks/recording-track-candidates`, `GET /debug/tracks/recording-track-candidates/summary`, and review endpoints under `/debug/tracks/recording-track-candidate-reviews`. Candidate classification is read-only, keeps release-track behavior unchanged, and uses evidence buckets including `same_isrc`, `conflicting_isrc_but_compatible_metadata`, `missing_isrc_but_compatible_metadata`, `partial_isrc_match`, and `variant_flag_excluded`. A debug-only review table stores manual decisions and full candidate snapshots, but saved reviews do not apply identity changes. Frontend Identity Audit has a `Recording Tracks` tab implemented in `frontend/src/components/identityAudit/RecordingTrackCandidatesTab.tsx`; it supports filters, sorting, risk flags, representative diagnostics, compact member evidence, saved review display, review controls, notes, and preferred representative selection. Verification passed with backend recording-track candidate/review tests, track identity audit route tests, release-track detail tests, backend py_compile, frontend build, and `git diff --check`. Recommended next task: manually inspect real candidates and use saved review patterns to decide whether classifier thresholds/rules need tuning.
