# Library / Personal Catalog Plan

## Summary

Build Library as a cached personal catalog, not a live computed view. Existing evidence feeds a rebuild job, and the UI reads fast paginated cache rows. Rule changes should trigger a background recompute instead of slowing every Library open.

Use these strength levels:

- `primary`: liked/saved, manual future add, own-playlist tracks, 3+ qualified listens
- `contextual`: 1-2 qualified listens, tracks from followed playlists in a category named `Favorite`, `Favorites`, `Liked`, or `Likes`, album-neighbor tracks
- `potential`: tracks from followed/added playlists not in a favorite-like category
- `ephemeral`: skip/preview/search/opened-only evidence

## Key Changes

- Add a backend Library cache table for track rows, with `user_id`, `spotify_track_id`, display metadata, `strength`, `reasons_json`, first/last evidence timestamps, listen count, playlist count, liked state, and source album/playlist hints.
- Add a small rebuild-state table with status, started/completed timestamps, rule version, row counts, and latest error.
- Add a rebuild service that derives rows from existing tables: liked-track cache, playlist track cache, playlist categories, source play-count cache, and Spotify album track cache.
- Use a rule version constant. When rules change, bump it and mark the cache stale. The user can keep using old cache rows while rebuild runs.

## API / UI

- Add `GET /me/library/tracks` with filters: `strength`, `q`, `limit`, `offset`, and sort options like `recent`, `name`, `listen_count`, `playlist_count`.
- Add `GET /me/library/status` so the UI can show stale/rebuilding state.
- Add `POST /me/library/rebuild` to enqueue or run a background rebuild for the current user.
- Add a `Library` dashboard surface as a focused component, not directly inside `App.tsx`, with strength tabs: `Primary`, `Contextual`, `Potential`, `Ephemeral`.
- Each row should show why it is there, such as `Liked`, `Own playlist`, `2 listens`, `Favorite playlist`, or `Album context`.

## Rebuild Rules

- One track can have many reasons; final strength is the strongest applicable rule after policy mapping.
- Favorite followed playlist tracks are `contextual`, not `primary`.
- Normal followed/added playlist tracks are `potential`.
- Own-playlist tracks are `primary` because the user intentionally placed them there.
- Metadata-empty unavailable tracks with no listening evidence are hidden from the default Library cache.
- Album-neighbor rows are included only when the album tracklist is already cached; v1 does not crawl Spotify to complete Library.
- Spotify-wide Catalog remains search/backfill only, not scrollable and not inserted into Library unless evidence makes it personal.

## Short-Term Todo

- Add an explicit show/hide control for unavailable tracks, including metadata-empty playlist items and tracks Spotify can identify but no longer play.

## Test Plan

- Backend unit tests for strength precedence and reason generation.
- Tests for favorite-category followed playlists becoming `contextual`.
- Tests for followed playlists outside favorite categories becoming `potential`.
- Tests for 1-2 listens as `contextual` and 3+ listens as `primary`.
- API tests for pagination, filtering, stale status, and rebuild status.
- Frontend build check plus manual QA on Library tabs, row reason labels, and stale/rebuilding banner.

## Assumptions

- V1 is track-first. Albums/artists can be facets or later tabs after track Library feels right.
- Manual add/remove overrides are not in v1.
- Cache can be stale during rebuild; the UI should say so without blocking browsing.
- No global Spotify catalog crawl. Search remains bounded and user-triggered.
