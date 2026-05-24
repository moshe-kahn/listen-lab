# Current Handoff

## Read First
Start here in a new chat, then open only the docs relevant to the requested task.

Recommended topic docs:
- `docs/reference/refactor-notes.md` for frontend/playback refactor and route extraction notes.
- `docs/reference/spotify-catalog-backfill.md` for cached Spotify catalog and album tracklist behavior.
- `docs/reference/raw-ingest.md` for raw recent/history ingest and canonical play-event projection.
- `docs/reference/source-track-resolution-policy.md` for source-track/release-track identity.
- `docs/reference/album-family-review-policy.md` only when working on album-family candidate review.

Avoid reading every doc by default.

## Current State
Active branch: `frontend-app-refactor`.

Latest committed baseline before this handoff update:
- `af2ebf8` `Add liked-track cache sync and refine playback queue UI`

Current uncommitted scope is ready to commit:
- artist album evidence backend endpoint
- artist overlay album/appears-on rendering from backend evidence
- album and track overlay artist splitting
- album tracklist `With` column and artist highlight behavior
- path updates in release-album validation docs

Known local processes:
- No long-running dev server was started by Codex for this scope.

## Work Completed
Backend artist album evidence:
- Added `backend/app/artist_album_evidence.py`.
- Added `GET /auth/artist-albums` in `backend/app/routes/playback_routes.py`.
- Endpoint accepts repeated `artist_names` query params plus optional `source_album_id` and `source_album_name`.
- Endpoint is read-only and uses cached `spotify_album_catalog` / `spotify_album_track` data.
- Single artist response classifies albums as `album`, `appears_on`, or `unknown`.
- Shared artist response includes only albums where all selected artists are supported by album or track evidence.
- Source album sorting is supported by album id or normalized album name.
- Raw SQL errors are hidden behind a generic route error.

Frontend artist overlay:
- Added typed frontend API wrapper `fetchArtistAlbumEvidence`.
- Artist overlay now calls `/auth/artist-albums` and falls back to the older local/profile-derived list if the request fails.
- Single artist pages split backend evidence into `Albums` and `Appears on`.
- Shared artist pages render one combined album list.
- Empty album sections do not render headings or empty text.
- Source album is highlighted when provided.

Album and track overlay artist behavior:
- Album overlay title shows year inline.
- Album overlay summary shows track count and album runtime once tracks are loaded.
- Album main artists are derived from loaded track evidence: artists on a majority of album tracks are treated as main; otherwise the album metadata artists are used as fallback.
- Guest/sub-artists are shown in a top `with ...` list and in each row's `With` column.
- Hover/focus on the top `with ...` list delays briefly, highlights matching track rows, and scrolls the first matching row into view when needed.
- Hovering the row-level `With` column does not highlight; those row artists remain clickable.
- Clicking a row-level `With` artist opens a shared artist page for the derived album main artist(s) plus that guest.
- Track overlay artist display now shows album-main artist(s) first and then `with ...` for other artists on that track.
- Track-view album headings include source album year when available.

Other frontend cleanup in this uncommitted scope:
- Removed representative artist/album song UI helpers/types from the frontend surface that no longer uses them.
- Homepage activity collapsed album art now suppresses duplicate album covers across the collapsed set.
- Album track rows reserve highlight space and share grid variables between header and rows to reduce table drift.

## Files Changed
Backend:
- `backend/app/artist_album_evidence.py`
- `backend/app/routes/playback_routes.py`

Frontend:
- `frontend/src/App.tsx`
- `frontend/src/api/appApi.ts`
- `frontend/src/styles.css`
- `frontend/src/types/appTypes.ts`
- `frontend/src/utils/dashboardUtils.ts`

Tests/docs:
- `backend/tests/test_artist_album_evidence.py`
- `docs/current-handoff.md`
- `docs/reference/release-album-merge-validation.md`
- `docs/reference/refactor-notes.md`

## Verification
Passed during this scope:
- `./.venv/bin/python -m unittest backend.tests.test_artist_album_evidence`
- `./.venv/bin/python -m py_compile backend/app/artist_album_evidence.py backend/app/routes/playback_routes.py`
- `npm run build --prefix frontend`
- `git diff --check`

Manual/data checks:
- Local cached catalog has examples for co-main artists plus guests:
  - `TajMo`: `Taj Mahal` and `Keb' Mo'` on `11/11`, guest `Lizz Wright` on `1/11`.
  - `Hymne au soleil`: `Laurent Bardainne` and `Tigre d'Eau Douce` on `11/11`, guests `Celia Wa` and `Bertrand Belin` on `1/11`.

Not fully verified:
- Browser visual QA for the album tracklist grid alignment after the latest CSS changes.
- Live OAuth/session request check for `/auth/artist-albums` in the browser network panel.

## Recommended Next Task
Start a fresh chat before the next feature task to reduce context noise.

First suggested follow-up:
1. Run the app.
2. Open `Next to Nothing Remixed`.
3. Verify the album header treats only majority-track artists as main.
4. Verify the row `With` clicks open shared artist pages.
5. Inspect the album tracklist grid in browser devtools if any column drift remains.

## Guardrails
- Do not delete old branches or stashes unless explicitly requested.
- Do not merge to `main` unless explicitly requested.
- Keep `frontend-app-refactor` as the integration baseline.
- Do not make frontend-only identity guesses when backend catalog or identity evidence is available.
- Keep `/auth/artist-albums` read-only; do not add merge/promotion/write behavior there.

## Resume Prompt
Continue in `/Users/kahntra/Programming/Personal Projects/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch is `frontend-app-refactor`. Latest completed scope adds read-only `/auth/artist-albums`, frontend artist overlay backend evidence, single/shared artist album rendering, album/track artist main-vs-with splitting, album row `With` behavior, delayed top guest hover highlighting, and docs updates. Verification passed with `backend.tests.test_artist_album_evidence`, `py_compile`, `npm run build --prefix frontend`, and `git diff --check`. Recommended next task: fresh browser QA of `Next to Nothing Remixed` album/track overlays and table alignment.
