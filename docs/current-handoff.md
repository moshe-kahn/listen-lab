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

This handoff covers the pending frontend playback-action menu work on top of the recording-view integration baseline: overlay and album play buttons now open a choice menu before playback or queue mutation.

Important local instruction:
- `AGENTS.md` says substantial frontend UI should not be added directly to `frontend/src/App.tsx`; keep `App.tsx` for routing/layout/wiring, feature UI under `frontend/src/components/...`, API calls in `frontend/src/api/appApi.ts`, and shared types in `frontend/src/types/appTypes.ts`.

## Recording-Track Candidate Rules
Hierarchy direction remains:

`source_track -> release_track -> recording_track -> track_family`

No durable canonical `recording_track` table, promotion/apply endpoint, or default aggregation change exists yet. Generated recording/track-family cluster tables now exist as SQLite evidence caches for fast lookup, but they are not identity promotion tables and do not apply saved review decisions.

Manual review outcomes encoded into classifier policy:
- Same full variant label plus strong evidence can form a recording-track subgroup.
- Mixed recording-distinct variant labels become `track_family_candidate`, not one recording candidate.
- Named/attributed mixes or versions such as `Spike Stent Mix` vs `Alchemist x Trooko Version` split at recording level but can remain related at family level.
- Structural labels such as `Part 1`, `Part 2`, `pt. 1`, intro/interlude/skit/reprise are family/segment relations, not recording merges.
- Instrumental variants are separate recording/listening objects by default, usually same Track Family.
- Explicit/clean variants can still be same `recording_track`; preserve content-rating metadata for frontend filtering/playback preference.
- Mono/stereo or format variants can remain same `recording_track` when evidence agrees; prefer clean/base title for display.
- Duration is supporting evidence only: matching durations are a good sign, but mismatched Spotify catalog durations cannot rule out the same release/recording because catalog metadata can differ from playback reality.
- Same normalized title plus compatible primary artist is the main recording-candidate gate; duration mismatches should push review, not automatic exclusion.
- Expanded, deluxe, and anniversary edition appearances belong at the recording/rerelease candidate layer rather than being merged into the same release track.
- Rerelease/remaster belongs before single in representative fallback order.

Representative selection now prefers:
1. source-backed original album
2. rerelease/remaster
3. single
4. soundtrack
5. compilation

It also prefers clean/base titles over format/remaster suffixes when otherwise compatible.

Current real candidate counts after duration/supporting-evidence rule changes:
- total candidate groups: `1258`
- recording-track candidates: `879`
- track-family candidates: `379`
- safe recording candidates: `665`
- needs-review recording candidates: `214`
- needs-review family candidates: `379`
- safe candidates represent `1362` unique release tracks and `1474` unique source tracks
- needs-review recording candidates represent `471` unique release tracks and `492` unique source tracks
- family candidates represent `881` unique release tracks and `964` unique source tracks

Backend endpoints:
- `GET /debug/tracks/recording-track-candidates`
- `GET /debug/tracks/recording-track-candidates/by-release/{release_track_id}`
- `GET /debug/tracks/recording-track-candidates/summary`
- `POST /debug/tracks/recording-track-candidate-reviews`
- `GET /debug/tracks/recording-track-candidate-reviews`
- `GET /debug/tracks/recording-track-candidate-reviews/{id}`

Generated candidate cache:
- local DB is now schema version `33`.
- `generated_recording_track_cluster` stores generated candidate snapshots.
- `generated_recording_track_cluster_member` maps generated clusters to release tracks.
- `generated_recording_track_cluster_dirty` tracks release IDs needing scoped refresh.
- Startup builds the generated cluster cache only if it is empty.
- Source-track map upserts, album-track inserts, and release-track merge repoints mark affected release tracks dirty.
- Recent-play sync drains a small dirty batch after inserted rows, so newly added tracks can join generated clusters without rebuilding the whole database.
- Track views use `by-release` lookup for fast "also appears on" / variation evidence and fall back to live candidate generation if no generated rows are present.

## Duration Repair, Provenance, And Conflicts
Found a propagation gap:
- many release tracks had accepted Spotify mappings and `spotify_track_catalog.duration_ms`, but `release_track.duration_ms` stayed null.
- `_upsert_track_catalog(...)` now fills missing `release_track.duration_ms` for accepted Spotify mappings and records duration provenance.
- Existing non-null release durations are preserved.

Schema/version note:
- duration provenance was introduced in schema version `31`; generated candidate caches were added in versions `32` and `33`.
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

## Frontend Track And Playback UI
Track views:
- User-facing track opens default to recording view; backend/debug surfaces can still open release view.
- Recording view shows the representative release track, same-recording album appearances under "Also appears on", and broader Track Family items under "Variations".
- Release view shows source-version albums instead of a raw source-version list, and the representative source has a visible badge.
- Gear menu actions replace the old bottom switch button: open in Spotify and switch between release/recording view.
- Top track header emphasizes artist and title, removes the top RT tag, and shows listen count when available.
- Variation cards show album art plus subtitle/version text where available, such as live-location suffixes.
- Clicking variation/family/release-source album cards switches the selected album and keeps the album tracklist open when it was already open.
- Album tracklists default closed in recording view and open by clicking album art.
- Open album tracklists scroll the highlighted/current track into view and show a small scrollbar marker only when the list can actually scroll.
- Album tracklist headers show track count/runtime, move liked badges beside the track name, and use a `Tags` column for release/recording tags.
- The `With` column is hidden for albums without guest artists.
- Tracklist loading keeps existing rows visible during same-view album switches.

Homepage playback:
- Homepage album art expands a compact album tracklist in the left player column; album title opens the full album overlay.
- The queue remains visible while the homepage album tracklist is open.
- The compact homepage list only shows play/title/played columns and no preview/tags columns.
- Repeated Spotify queue cycles are collapsed for display.
- The compact list no longer shows the scrollbar-position marker for short non-scrollable lists.
- Overlay track play buttons, album track row play buttons, homepage album row play buttons, and album `Play all` buttons now open a playback-action menu instead of immediately starting playback.
- Playback-action choices are `Play now`, `Play next`, and `Add to queue`.
- `Play now` preserves existing playback behavior and album queue seeding.
- `Play next` inserts the selected track or album block after the active queue item in the ListenLab queue.
- `Add to queue` appends the selected track or album block to the end of the ListenLab queue.
- The menu is rendered through a portal, anchored over/near the pressed play button, keeps the original play glyph as the trigger, and shows action icons for all choices.

Liked state:
- Targeted `GET /me/liked-tracks/contains` checks fill liked-star gaps for selected tracks and visible album-track rows without forcing a full liked-track cache load.

## Frontend Identity Audit
Existing tab:
- Tracks -> `Recording Tracks`
- Component: `frontend/src/components/identityAudit/RecordingTrackCandidatesTab.tsx`
- Saves review decisions only; does not apply identity changes.
- Candidate rows now expose generated cluster member counts and show RT for all tracks that are part of a generated recording or family cluster, not only release-candidate rows.

Duration tab:
- Tracks -> `Duration Conflicts`
- Component: `frontend/src/components/identityAudit/ReleaseTrackDurationConflictsTab.tsx`
- Lists the 49 release-track duration conflicts, including rows with filled representative durations marked uncertain.
- Each source track links directly to Spotify with `https://open.spotify.com/track/...`.
- Shows representative duration, confidence, source duration, album, release date, album type, explicit flag, ISRC, and match method.

Frontend deliberately still does not include:
- accept/reject/apply identity mutation
- schema promotion
- default aggregation changes

## Files Changed In Pending Commit
Frontend:
- `frontend/src/App.tsx`
- `frontend/src/components/playback/PlaybackActionMenu.tsx`
- `frontend/src/styles.css`

Docs:
- `docs/current-handoff.md`
- `docs/overview/architecture.md`
- `docs/overview/context.md`
- `docs/reference/refactor-notes.md`

## Verification
Frontend/checks run and passed:
- `npm run build` from `frontend/`
- `git diff --check`

Vite still reports the existing large chunk warning after frontend builds.

Manual/browser verification note:
- A Vite dev server was started and stopped.
- Browser smoke test reached the frontend, but full local-app QA was blocked because `./.venv/bin/uvicorn` points at an old missing Python interpreter path: `/Users/kahntra/Documents/ListenLab/listen-lab-main/.venv/bin/python3.13`.

## Known Limitations
- Candidate discovery remains debug/read-only and intentionally conservative.
- Generated recording/track-family cluster tables are caches only; they are not durable identity or promotion state.
- Saved recording-track reviews are audit metadata only; they do not drive classifier tuning, identity promotion, or aggregation yet.
- Different-ISRC compatible remasters/reissues stay review-required.
- Frontend review filters for evidence bucket, ISRC state, and sorting apply to the currently loaded candidate page.
- Playback-action menu queue mutations are frontend ListenLab-queue mutations; they do not call Spotify's native queue-add endpoint.
- Becca Mancari-style album/single representative choice can still be weak when album metadata lacks album type/release dates.
- The 49 duration conflicts have representative durations for display but remain uncertain; do not use those values as verified playback lengths.
- Frontend still lacks a dedicated test harness for Identity Audit components; verification is build/type based plus backend tests.
- Recording/release/variation UI still needs manual QA against an active Spotify session and real catalog examples.

## Recommended Next Task
Manual QA the playback-action menu and track view flow with known cases:
- overlay track play button: `Play now`, `Play next`, `Add to queue`
- album track row play button and album `Play all`: queue insertion as a track or album block
- homepage compact album tracklist row play button and `Play all`
- same recording across albums, such as original album vs deluxe/expanded edition
- broader live/demo/remix family variations
- release view source-version album cards
- homepage playback album expansion with the queue visible

Other possible follow-ups:
- Continue reviewing the 49 duration conflicts in Identity Audit -> Tracks -> Duration Conflicts.
- Add focused frontend tests for track overlays, homepage playback album expansion, `ReleaseTrackDurationConflictsTab`, and `RecordingTrackCandidatesTab`.
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
Continue in `/Users/kahntra/Programming/Personal Projects/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch is `frontend-app-refactor`. Pending work adds a frontend playback-action menu for overlay track play, album track rows, homepage album rows, and album `Play all`: `Play now` preserves playback and queue seeding, `Play next` inserts after the active ListenLab queue item, and `Add to queue` appends. The menu lives in `frontend/src/components/playback/PlaybackActionMenu.tsx`, renders through a portal anchored to the pressed button, keeps the original play glyph trigger, and shows icons for all actions. Verification passed with `npm run build` from `frontend/` and `git diff --check`; Vite still reports the existing large chunk warning. Full local browser QA was blocked by a stale `.venv` interpreter path for uvicorn. Next task: manual QA playback-action choices against active Spotify playback, then commit if satisfied.
