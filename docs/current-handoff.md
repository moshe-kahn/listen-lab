# Current Handoff

## Read First
Start in `/Users/kahntra/Programming/Personal Projects/ListenLab/listen-lab-main`, then read `AGENTS.md` and this file.

Recommended topic docs:
- `docs/reference/source-track-resolution-policy.md` for source/release/recording/track-family identity policy.
- `docs/reference/drafts/entity-model-draft.md` for the proposed long-term identity model and source/text identity reconciliation notes.
- `docs/reference/spotify-catalog-backfill.md` for cached Spotify catalog and album tracklist behavior.
- `docs/reference/refactor-notes.md` for frontend/playback refactor and route extraction notes.
- `docs/reference/raw-ingest.md` for raw recent/history ingest and canonical play-event projection.

Avoid reading every doc by default.

## Current State
Active branch: `frontend-app-refactor`.

This handoff covers the pending track-view, album-tracklist, playback-action menu, and source/catalog identity promotion work. The worktree is ready to commit after final verification.

Important local instruction:
- `AGENTS.md` says substantial frontend UI should not be added directly to `frontend/src/App.tsx`; this branch still has a large `App.tsx` integration diff from iterative UI work. Future work should extract focused components instead of growing `App.tsx` further.

## Recording And Track-Family Identity
Hierarchy direction remains:

`source_track -> release_track -> recording_track -> track_family`

No durable canonical `recording_track` table, promotion/apply endpoint, or default aggregation change exists yet. Generated recording/track-family cluster tables are SQLite evidence caches only.

Relevant rules now encoded/used:
- Generated candidate metadata exposes cluster type and relationship kind to frontend rows.
- Backend candidate lookup prioritizes recording-level candidates over broader family candidates when both exist.
- Candidate row tags use `D/R/V/C` order:
  - `D`: duplicate source-track grouping for same release track
  - `R`: recording group
  - `V`: variation/context/style family
  - `C`: cover/remix/rework family
- Frontend album tracklist combines those letters into one compact badge, e.g. `D`, `DV`, `C`.
- Track relation rows in the overlay are separated into:
  - `Recording variations`
  - `Variations`
  - `Covers / remixes`

Local data note:
- `backend/scripts/promote_catalog_album_tracks.py` promotes selected cached `spotify_album_track` rows into source/release identity rows. It defaults to dry-run; use `--apply` only for targeted albums.
- Applied locally for Spotify album `1vdQ5t7iO2gC3OX7j2GFCt` (`I Might Be Wrong`): created `7` new `release_track`/`source_track` mappings, added `8` album-track links, refreshed generated clusters, and linked live versions as Track Family candidates against Kid A-era base tracks.
- Local SQLite DB changes are not part of git.

## Source/Text Identity Split
Found a broader source identity problem:
- History/raw import can create text-only identities from artist/album/track text.
- Spotify API/catalog paths later create provider-backed identities using Spotify IDs.
- Current ingest does not generally reconcile text-only rows to provider-backed rows.

Confirmed local example:
- `Radiohead` has two `artist` rows:
  - Spotify-backed row mapped to Spotify artist `4Z8W4fKeB5YxbusRsdQVPb`
  - history-text row mapped to `history_raw:a4a5c3...`
- No duplicate-name group with multiple distinct Spotify artist IDs was found in the quick local check. The issue is provider-backed vs text-only split, not conflicting Spotify artist IDs.

Same class applies to albums/tracks:
- `Amnesiac` and `Kid A` currently appear as `history_raw` release-album rows in local DB, which can explain missing year/artwork when UI opens the history-text album instead of a Spotify-backed source.
- Tracks have Spotify ID vs Spotify URI reconciliation, but no broad safe `history_raw_track` -> Spotify track merge by title/artist/album/duration/ISRC.

Recommended future fix:
- Add read-only audit for text/provider duplicate candidates across artist, album, and track.
- Add safe merge/repoint tools:
  - artist: prefer provider-backed artist, repoint `source_artist_map`, `album_artist`, `track_artist`, dedupe links
  - album: require normalized album + primary artist + year/catalog evidence, repoint `source_album_map`, `album_artist`, `album_track`
  - track: require title + artist + album + duration/ISRC/context evidence, repoint `source_track_map`, `track_artist`, `album_track`
- Update ingest/promotion paths to attach safe text-only sources to provider-backed identities when Spotify IDs arrive.

## Track View And Album Tracklist UI
Track view:
- Track opens show album artist below title and top action buttons above metadata.
- Play button opens a menu with `Play now`, `Play next`, `Add to queue`, and bookmark placeholder.
- Bookmark/star buttons are independent local UI placeholders; durable bookmark behavior is not implemented.
- Top metadata tags show duration, last listened, and listen count. Last-listened uses `M/D`, adding `/YY` only for previous years.
- Listen count was moved to the right side of the top metadata row to reduce visual jump.
- Overlay top is anchored so relation rows extend downward rather than recentering the modal.

Album tracklist in track view:
- Always visible beside album art on track view.
- Album art opens album view.
- Album tracklist hides the `With` column on track view and keeps preview buttons only on album view.
- Current row highlight extends full row width.
- Switching tracks in the same album keeps the current tracklist scroll position; it no longer recenters each row click.
- `Last` column shows compact relative time (`4h`, `1d`, `3w`, `4m`, `7y`) and is clickable:
  - click once: newest first with down arrow
  - click again: oldest/no-history first with up arrow
  - click third time: original album order
- `Last` sort state is independent between homepage album list and overlay/detail list.
- New/unlistened marker is a subdued monochrome `✦` in the `Last` column, not the Tags column.
- Star/liked state is release-track-aware and can inherit selected track detail source-version liked state when album row Spotify ID differs.

Backend album-track history:
- `/auth/playback/album-tracks` enrichment now adds `play_count` and `last_played_at` from persisted play facts.
- `release_track_detail.source_versions` now includes `last_played_at` as well as `play_count`.
- Frontend patches visible album rows from selected release detail so the tracklist agrees with top track-view listen counts.

## Playback Action Menu
- Overlay track play buttons, album track row play buttons, homepage album row play buttons, and album `Play all` buttons open a playback-action menu instead of immediately starting playback.
- `Play now` preserves existing playback and queue seeding.
- `Play next` inserts the selected track or album block after the active ListenLab queue item.
- `Add to queue` appends the selected track or album block.
- The menu is rendered through a portal and can overlay the pressed button so selecting `Play now` feels like a double-click.
- The menu has compact row heights to match tracklist rows.

## Files Changed In Pending Commit
Backend:
- `backend/app/catalog_identity_promotion.py`
- `backend/app/recording_track_candidates.py`
- `backend/app/release_track_detail.py`
- `backend/app/release_track_metadata.py`
- `backend/scripts/promote_catalog_album_tracks.py`

Frontend:
- `frontend/src/App.tsx`
- `frontend/src/components/common/CoverRemixBadge.tsx`
- `frontend/src/components/common/DuplicateBadge.tsx`
- `frontend/src/components/common/FamilyBadge.tsx`
- `frontend/src/components/common/NewTrackBadge.tsx`
- `frontend/src/components/common/ReleaseSiblingBadge.tsx`
- `frontend/src/components/playback/PlaybackActionMenu.tsx`
- `frontend/src/styles.css`
- `frontend/src/types/appTypes.ts`
- `frontend/src/utils/dashboardUtils.ts`

Docs:
- `docs/current-handoff.md`
- `docs/reference/drafts/entity-model-draft.md`

## Verification
Checks run and passed during the session:
- `python3 -m py_compile backend/app/recording_track_candidates.py backend/app/release_track_detail.py backend/app/release_track_metadata.py`
- `npm run build` from `frontend/`
- `git diff --check`

Vite still reports the existing large chunk warning after frontend builds.

Manual/browser QA is still limited. Backend server reload is required for new backend fields (`last_played_at`, album-track play history) to be visible in the app.

## Known Limitations
- Frontend bookmark/star behavior is local placeholder UI; bookmark persistence is not implemented.
- Source/text identity reconciliation is not implemented; only analysis and docs were added.
- Catalog album-track promotion exists as a targeted script, not as automatic catalog backfill behavior.
- Generated recording/track-family clusters are caches only; no durable `recording_track` promotion exists.
- Frontend still lacks focused tests for these overlay/tracklist workflows.
- Manual QA should verify real cases after backend restart.

## Recommended Next Task
After commit, next best task is source/text identity reconciliation:
1. Add read-only duplicate audit for artists, albums, and tracks.
2. Start with safe artist merge/repoint from text-only duplicates to provider-backed artists.
3. Then design album and track merge criteria separately; names alone are not enough.

Manual QA examples:
- Kid A / Amnesiac metadata and artist linkage.
- Idioteque liked/star and listened state across track view and album tracklist.
- I Might Be Wrong live tracks and Kid A family/variation rows.
- `Last` sort cycle in both homepage and detail album tracklists.
- Playback action menu for row play buttons and `Play all`.

## Guardrails
- Do not delete old branches or stashes unless explicitly requested.
- Do not merge to `main` unless explicitly requested.
- Keep `frontend-app-refactor` as the integration baseline.
- Backend owns source/release/recording/family identity evidence and future promotion semantics.
- Keep Spotify track id / URI as concrete playback identity.
- Do not make `recording_track` the default aggregation layer without explicit scope.
- Do not let catalog backfill mutate identity rows automatically; use targeted promotion/repair flows with dry-run/apply separation.

## Resume Prompt
Continue in `/Users/kahntra/Programming/Personal Projects/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch is `frontend-app-refactor`. Recent work added track-view album tracklist UI, combined relation tags (`D/R/V/C`), compact `Last` sorting, backend album-track play-history enrichment, release-detail `last_played_at`, playback-action menus, and targeted Spotify catalog album-track identity promotion. Verification passed with py_compile, `npm run build`, and `git diff --check`; Vite still reports the existing large chunk warning. Next task: build source/text identity reconciliation audit and safe merge/repoint flow, starting with artist duplicates such as text-only `Radiohead` vs Spotify-backed `Radiohead`.
