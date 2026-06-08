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
- `Preserve album metadata across identity previews`

Worktree at handoff:
- clean after committing album/recording identity metadata fixes

Important local instruction:
- `AGENTS.md` says substantial frontend UI should not be added directly to `frontend/src/App.tsx`. This branch still has a large `App.tsx` integration surface from iterative UI work. Future UI work should extract focused components instead of growing `App.tsx` further.

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

Frontend changes:
- recording variation cards and navigation use structured artist entries when available.
- album-track preview rows preserve track `artists` arrays and use structured artists before display-string fallback.
- artist preview no longer uses album art as an artist-photo fallback.
- album heading formatting now treats album previews correctly: album label/source album name is the album name, and `detail` is only a year when it looks like a year.
- missing recording variation years are omitted in dashboard modal and Identity Audit candidate rows.

Important implementation note:
- Some backend fixes affect generated recording candidate lookup. If QA appears stale, restart the backend first, then hard refresh the frontend.

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

## Source/Text Identity Status
Artist reconciliation now has audit, safe repair, composite cleanup, and ingest prevention.

Remaining broader source/text identity work:
- album duplicate repair still needs stricter source/text reconciliation beyond existing release-album preview/dry-run tooling
- track duplicate repair still needs evidence rules by title + artist + album + duration/ISRC/context
- frontend de-duping remains a defensive display fallback only; backend identity remains the source of truth

Do not broaden automatic repair to similar names, stylization variants, or multiple provider-backed rows without an explicit review/apply design.

## Tests And Verification
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

## Known Limitations
- Frontend bookmark/star behavior is local placeholder UI; bookmark persistence is not implemented.
- Source/text album and track reconciliation are not broadly implemented.
- Catalog album-track promotion exists as a targeted script, not as automatic catalog backfill behavior.
- Generated recording/track-family clusters are caches only; no durable `recording_track` promotion exists.
- Frontend still lacks focused tests for overlay/tracklist workflows.
- Artist group/member modeling remains intentionally out of scope. Future musician/member graph work is tracked as low-priority future work in `docs/overview/roadmap.md`.

## Recommended Next Task
Recommended project-level next steps:

1. Push this branch after the commit if remote backup/review is desired.
2. Fix the local `.venv` interpreter path or recreate the venv. The current repo venv points at an old `/Users/kahntra/Documents/ListenLab/...` path, which blocks the standard `./.venv/bin/uvicorn backend.app.main:app --host 127.0.0.1 --port 8765` startup command.
3. Run the safe release-album repair dry-run on the real local DB:
   - `POST /debug/identity/release-albums/history-spotify-repair?dry_run=true&limit=50`
   - inspect candidates before running write mode.
4. Continue source/text identity reconciliation for albums and tracks:
   - album duplicate repair beyond the safe history/Spotify merge path
   - track duplicate repair using title + artist + album + duration/ISRC/context evidence
   - promote only through explicit dry-run/apply flows, not automatic broad mutation
5. Extract more detail modal/preview logic out of `frontend/src/App.tsx` before adding new UI surface.
6. Add focused frontend tests for modal navigation regressions:
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
Continue in `/Users/kahntra/Programming/Personal Projects/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch is `frontend-app-refactor`. Latest committed work fixes album/recording identity metadata preservation across track, album, artist, and recording variation preview flows. User confirmed the previously reported Kutiman/Dekel, Telenova recording variation, and `Hope` `2026 - 2026` issues work after restart/reload. Verification passed with the focused backend unittest suite, frontend `npm run build`, Python compile checks, and `git diff --check`. Next: push if desired, fix/recreate the broken local venv path, run the release-album history/Spotify repair dry-run on the real DB, then continue album/track source-text identity reconciliation with explicit dry-run/apply repair flows.
