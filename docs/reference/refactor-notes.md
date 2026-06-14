# Refactor Notes

This document keeps durable refactor context out of `docs/current-handoff.md`.

## Backend `main.py` Extractions
Goal: reduce `backend/app/main.py` incrementally without changing route contracts or backend behavior.

Recent committed extraction sequence:
- `b3c4196` Extract dashboard progress tracker
- `9369329` Extract backend session and cache helpers
- `5237ad3` Extract backend time helpers
- `226ec14` Move session restore helper
- `9664bb1` Extract backend token helpers
- `021ab00` Extract OAuth helper functions
- `49957d9` Extract Spotify rate limit helpers
- `5265ec8` Extract Spotify HTTP helpers
- `b5ce3a3` Move Spotify profile fetch helper
- `a90fc21` Extract Spotify normalization helpers
- `bd2afca` Extract Spotify lookup helpers
- `cc6a224` Extract static metadata cache

Current extracted modules:
- `backend/app/progress_tracker.py`
  - dashboard load progress state, progress keying, progress log writing
- `backend/app/cache/file_cache.py`
  - generic cache directory and JSON file read/write helpers
- `backend/app/cache/static_metadata_cache.py`
  - shared static Spotify artist/album/track metadata cache state, hydration, trimming, persistence, and remember helpers
- `backend/app/auth/session.py`
  - session user lookup, auth requirement, token-store-backed session restoration
- `backend/app/auth/token.py`
  - token refresh requirement helpers used by authenticated Spotify calls
- `backend/app/auth/oauth_helpers.py`
  - OAuth configuration check, callback redirect URL construction, PKCE code challenge
- `backend/app/utils/time_helpers.py`
  - ISO UTC parsing, Spotify expiry formatting, recent-track timestamp formatting
- `backend/app/spotify_rate_limit.py`
  - in-process Spotify rate-limit cooldown state, `Retry-After` parsing, rate-limit detail strings
- `backend/app/spotify_http.py`
  - generic Spotify GET helpers and Spotify profile fetch helper
- `backend/app/spotify_normalization.py`
  - pure artist/track normalization and artist/album/track identity key helpers
- `backend/app/spotify_lookup_helpers.py`
  - pure enrichment lookup and history merge helpers
- `backend/app/cache/short_cache.py`
  - short-lived in-memory section cache state and helpers
- `backend/app/cache/history_cache.py`
  - history/local insight cache paths, cache rebuild support, and dashboard cache clearing
- `backend/app/cache/user_snapshot_cache.py`
  - per-user profile and recent-section snapshot cache helpers
- `backend/app/merged_track_aggregate.py`
  - merged track aggregate query and route payload helper
- `backend/app/spotify_preview.py`
  - representative artist/album track preview helpers
- `backend/app/liked_tracks.py`
  - user-scoped read-only liked-track cache listing, contains checks, Spotify saved-track quick/full sync, sync metadata, unlike marking after completed full sync, and guarded dev/test failure simulation response shape

Extracted route modules:
- `backend/app/routes/audit_routes.py`
- `backend/app/routes/admin_routes.py`
- `backend/app/routes/playback_routes.py`
- `backend/app/routes/auth_routes.py`

Current audit-route additions:
- `GET /debug/artists/duplicate-audit`
  - implemented through `backend/app/artist_identity_repair.py`
  - read-only duplicate/composite/stylization artist audit
- `POST /debug/artists/duplicate-repair`
  - dry-run by default; write repair is transaction-scoped and evidence-gated
- `POST /debug/artists/composite-credit-cleanup`
  - dry-run by default; write cleanup only removes ready composite links/rows

Current playback-route addition:
- `GET /auth/artist-albums`
  - implemented with `backend/app/artist_album_evidence.py`
  - read-only cache/internal-link query for artist overlay album evidence
  - accepts repeated `artist_names` plus optional `source_album_id` / `source_album_name`
  - returns album metadata, matching track counts, all-target presence, tracklist completeness, relationship, and evidence text

Remaining route groups:
- `dashboard_routes.py`
- `ingest_routes.py`
- `catalog_routes.py`

Dashboard extraction blockers:
- local/history payload dependency stack
- recent Spotify fetch helper stack
- live top album/artist normalization stack

Only `_normalize_recent_track_item_for_route` and `_normalize_recent_tracks_payload_for_route` currently appear safe to move independently from the dashboard helpers.

Current `backend/app/main.py` role:
- FastAPI app construction and middleware
- startup initialization
- route handlers
- dashboard assembly and domain-specific fetch/enrichment orchestration
- remaining dashboard, ingest, and catalog route handlers
- remaining dashboard orchestration helpers
- liked-track cache routes remain inline for now and should move with the future dashboard route extraction, not as a broad standalone refactor during unrelated work

Frontend `App.tsx` current branch scope:
- Recent Likes cache integration prefers `GET /me/liked-tracks`, keeps direct Spotify latest-likes rows as a labeled fallback, and exposes `Sync Likes` quick sync.
- Playback/queue work includes queue organizer controls, queue played-state markers, preview playback resume, album play-all, playback-action choice menus, and dropdown outside-click handling.
- Artist duplicate audit UI is extracted to `frontend/src/components/identityAudit/ArtistDuplicateAuditTab.tsx`, while modal artist-display wiring still lives in `App.tsx`.
- Further frontend extraction should preserve these behaviors and avoid mixing feature work with component moves.

Backend verification pattern:
- `./.venv/bin/python -m py_compile <changed modules>`
- import smoke for the new module plus `backend.app.main`
- `./.venv/bin/python -m unittest discover -s backend/tests`
- `git diff --check -- <changed files>`

## Inline route inventory
Historical inventory taken from `backend/app/main.py` route decorators before the latest route extractions. Extracted groups above are no longer inline. Line numbers are approximate and should be refreshed before using this as an extraction map.

Recommended next backend session:
1. Move `_normalize_recent_track_item_for_route` and `_normalize_recent_tracks_payload_for_route` only.
2. Re-scan dashboard routes.
3. Defer `/me` route extraction until its orchestration helpers are untangled.

### `auth_routes.py` candidates
| Method | Path | Handler | Line | Coupling notes |
| --- | --- | --- | ---: | --- |
| GET | `/auth/login` | `auth_login` | 2399 | Uses OAuth config helpers, PKCE, session state, and `settings` Spotify authorize URL/scope. |
| GET | `/auth/callback` | `auth_callback` | 2431 | Tightly coupled to OAuth token exchange, `httpx`, session mutation, token store writes, Spotify profile fetch, callback redirects, and optional recent ingest sync. |
| GET | `/auth/session` | `auth_session` | 2744 | Uses session restore helpers and token store state. |
| GET | `/auth/full-availability` | `auth_full_availability` | 2847 | Uses session/token store checks, Spotify cooldown helpers, profile fetch, and refresh-token fallback. |
| GET | `/auth/token` | `auth_token` | 2955 | Requires authenticated user, refreshes Spotify access token, and may clear session on unauthorized. |
| POST | `/auth/logout` | `auth_logout` | 3014 | Only clears Starlette session. |

### `dashboard_routes.py` candidates
| Method | Path | Handler | Line | Coupling notes |
| --- | --- | --- | ---: | --- |
| GET | `/me/progress` | `me_progress` | 3055 | Reads `LOAD_PROGRESS` via progress key helper and uses `time.perf_counter`. |
| GET | `/me/local/recent` | `me_local_recent` | 3073 | Uses local profile builder, progress tracker, local history/cache orchestration, and section limits. |
| GET | `/me/local` | `me_local` | 3109 | Uses local profile builder, progress tracker, session profile snapshot, and user profile cache. |
| GET | `/me/recent` | `me_recent` | 3142 | High coupling to Spotify token/profile refresh, recent DB sync, DB recent sections, liked tracks, top tracks/artists fetches, short cache, user recent/profile snapshots, and progress tracker. |
| GET | `/debug/me/recent/compare` | `debug_me_recent_compare` | 3343 | Debug comparison route coupled to legacy recent payload builder, DB recent sections, recent rows, and comparison summary builder. |
| GET | `/me/recent/archive` | `me_recent_archive` | 4064 | Uses token requirement, recent DB row query, row-to-canonical mapping, and route payload normalization. |
| GET | `/me` | `me` | 4091 | Largest dashboard route; couples Spotify profile/top data, playlists, history insights, persistent/user/short caches, static metadata hydration, image backfills, recent sync, local history, and progress tracking. |

### `ingest_routes.py` candidates
| Method | Path | Handler | Line | Coupling notes |
| --- | --- | --- | ---: | --- |
| GET | `/auth/recent-ingest/result` | `auth_recent_ingest_result` | 2565 | Reads and pops recent ingest result from session. |
| GET | `/auth/recent-ingest/probe-before` | `auth_recent_ingest_probe_before` | 2573 | Uses auth token, UTC date math, Spotify recent page fetch, cursor conversion, and HTTP error mapping. |
| GET | `/auth/recent-ingest/probe-backfill` | `auth_recent_ingest_probe_backfill` | 2617 | Uses auth token and paged Spotify recent-play fetch loop with before cursors. |
| GET | `/auth/recent-ingest/debug-items` | `auth_recent_ingest_debug_items` | 2682 | Uses auth token, Spotify recent page fetch, and inline item normalization for debugging. |
| POST | `/auth/recent-ingest/poll-now` | `auth_recent_ingest_poll_now` | 2841 | Requires user id and delegates to recent polling service. |

### `catalog_routes.py` candidates
| Method | Path | Handler | Line | Coupling notes |
| --- | --- | --- | ---: | --- |
| POST | `/debug/spotify/catalog-backfill` | `debug_spotify_catalog_backfill` | 3574 | High coupling to local-data auth, token refresh, request body parsing, catalog backfill runner, retry-on-401 behavior, and JSON unauthenticated response shape. |
| GET | `/debug/spotify/catalog-backfill/runs` | `debug_spotify_catalog_backfill_runs` | 3691 | Requires local-data session and delegates to catalog backfill run listing. |
| GET | `/debug/spotify/catalog-backfill/coverage` | `debug_spotify_catalog_backfill_coverage` | 3701 | Requires local-data session and delegates to catalog backfill coverage query. |
| POST | `/debug/spotify/catalog-backfill/enqueue` | `debug_spotify_catalog_backfill_enqueue` | 3709 | Requires local-data session, parses `items`, and delegates to queue enqueue helper. |
| GET | `/debug/spotify/catalog-backfill/queue` | `debug_spotify_catalog_backfill_queue` | 3720 | Requires local-data session and delegates to queue listing with filters. |
| POST | `/debug/spotify/catalog-backfill/queue/repair` | `debug_spotify_catalog_backfill_queue_repair` | 3732 | Requires local-data session and delegates to queue repair helper. |
| GET | `/debug/search/albums` | `debug_search_albums` | 3740 | Requires local-data session and delegates to album catalog lookup search. |
| GET | `/debug/search/albums/duplicates` | `debug_search_album_duplicates` | 3761 | Requires local-data session and delegates to duplicate album Spotify identity search. |
| GET | `/debug/search/albums/duplicates-by-name` | `debug_search_album_duplicates_by_name` | 3771 | Requires local-data session and delegates to duplicate album name identity search. |
| GET | `/debug/search/tracks` | `debug_search_tracks` | 3781 | Requires local-data session and delegates to track catalog lookup search. |
| GET | `/debug/search/tracks/duplicates` | `debug_search_track_duplicates` | 3802 | Requires local-data session and delegates to duplicate track Spotify identity search. |
| GET | `/debug/search/tracks/lineage` | `debug_search_track_lineage` | 3812 | Requires local-data session and delegates to track mapping lineage search. |
| GET | `/debug/search/tracks/lineage/album-display-diagnostic` | `debug_search_track_lineage_album_display_diagnostic` | 3833 | Requires local-data session and delegates to album display gap diagnostic. |
| POST | `/debug/identity/release-albums/merge-preview` | `debug_identity_release_album_merge_preview` | 3852 | Requires local-data session, validates release album id list, and delegates to merge preview. |
| POST | `/debug/identity/release-albums/merge-dry-run` | `debug_identity_release_album_merge_dry_run` | 3864 | Requires local-data session, validates merge body/survivor id, and delegates to dry-run merge. |
| POST | `/debug/spotify/catalog-access-probe` | `debug_spotify_catalog_access_probe` | 3955 | Coupled to local-data auth, token refresh, Spotify catalog probe helpers, known track id discovery, and custom unauthenticated/404 JSON responses. |

### `audit_routes.py` candidates
| Method | Path | Handler | Line | Coupling notes |
| --- | --- | --- | ---: | --- |
| GET | `/tracks/merged-aggregate` | `tracks_merged_aggregate` | 3421 | Requires local-data session and delegates through shared merged aggregate payload helper. |
| GET | `/debug/tracks/merged-aggregate` | `debug_tracks_merged_aggregate` | 3436 | Same merged aggregate helper and local-data session dependency as public aggregate route. |
| GET | `/debug/tracks/identity-audit` | `debug_tracks_identity_audit` | 3451 | Requires local-data session and delegates to track identity audit builder. |
| GET | `/debug/tracks/identity-audit/ambiguous-review` | `debug_tracks_identity_audit_ambiguous_review` | 3460 | Requires local-data session and delegates to ambiguous review queue query. |
| GET | `/debug/tracks/identity-audit/suggested-groups` | `debug_tracks_identity_audit_suggested_groups` | 3479 | Requires local-data session and delegates to suggested analysis group query. |
| POST | `/debug/tracks/identity-audit/submission-preview/validate` | `debug_tracks_identity_audit_submission_preview_validate` | 3494 | Requires local-data session, validates JSON object payload, and delegates to submission preview validator. |
| POST | `/debug/tracks/identity-audit/submissions` | `debug_tracks_identity_audit_submissions_create` | 3508 | Requires local-data session, validates JSON object payload, and delegates to submission save helper. |
| GET | `/debug/tracks/identity-audit/submissions` | `debug_tracks_identity_audit_submissions_list` | 3522 | Requires local-data session and delegates to submission listing. |
| GET | `/debug/tracks/identity-audit/submissions/{submission_id}` | `debug_tracks_identity_audit_submissions_read` | 3532 | Requires local-data session, delegates to submission lookup, and returns custom 404 JSON shape. |
| POST | `/debug/tracks/identity-audit/submissions/{submission_id}/dry-run` | `debug_tracks_identity_audit_submissions_dry_run` | 3553 | Requires local-data session, delegates to dry-run helper, and returns custom 404 JSON shape. |
| GET | `/debug/tracks/identity-audit/readiness` | `debug_track_identity_readiness` | 3842 | Requires local-data session and delegates to readiness report builder. |

### `admin_routes.py` candidates
| Method | Path | Handler | Line | Coupling notes |
| --- | --- | --- | ---: | --- |
| GET | `/health` | `health` | 2394 | No dependencies beyond FastAPI route registration. |
| POST | `/cache/rebuild` | `cache_rebuild` | 3020 | Coupled to dashboard cache clear, history signature/insights loading, persistent/local history cache writes, settings history dir, and section limits. |
| GET | `/debug/listening-log` | `debug_listening_log` | 4048 | Requires local-data session and delegates to listening log query with source filter normalization. |

### `playback_routes.py` candidates
| Method | Path | Handler | Line | Coupling notes |
| --- | --- | --- | ---: | --- |
| GET | `/auth/current-playback` | `auth_current_playback` | 2759 | Requires user id and delegates to current playback service. |
| POST | `/auth/player-listen-event` | `auth_player_listen_event` | 2765 | Coupled to user auth, event payload normalization, listenlab player ingest run writes, play progress updates, token store lookup, event projection, and UUID/time helpers. |
| GET | `/preview/representative` | `preview_representative` | 2975 | Uses auth token, profile market lookup, representative artist/album track fetch helpers, and Spotify error-to-reason mapping. |

### fallback/other
No inline `@app.get`, `@app.post`, `@app.delete`, `@app.put`, or `@app.patch` routes currently need this bucket.

## Frontend App Extraction
Goal: reduce `frontend/src/App.tsx` without changing Identity Audit UX or backend API contracts.

Current frontend extraction:
- `frontend/src/api/appApi.ts`
  - API fetch/post wrappers that were previously in `App.tsx`
- `frontend/src/components/identityAudit/IssueFeed.tsx`
  - Identity Audit issue feed component and related issue sort/review types
- `frontend/src/components/identityAudit/AlbumHistorySpotifyRepairTab.tsx`
  - Identity Audit Albums repair dry-run/apply UI for `POST /debug/identity/release-albums/history-spotify-repair`
- `frontend/src/components/identityAudit/IdentityAuditDiagnostics.tsx`
  - Identity Audit diagnostic rendering helpers
- `frontend/src/constants/appConstants.ts`
  - shared app constants, initial section state, Spotify logo URL, UI option arrays
- `frontend/src/types/appTypes.ts`
  - broad response/UI/player/profile/catalog/audit types
- `frontend/src/utils/identityAuditPrefs.ts`
  - Identity Audit localStorage preference load/save helpers and persisted preference types
- `frontend/src/utils/trackRelationTags.ts`
  - shared `D/R/V/C` relation-tag construction for duplicate source, recording, variation, and cover/remix/family badges
- route/page chunks:
  - Formula Lab, Recent Debug, Catalog Backfill, and Search Lookup are lazy-loaded from `DashboardSections`
  - `DetailPreviewModal` is lazy-loaded from `App.tsx` only after a preview item is selected

Current startup-load behavior:
- quick full-mode initial load requests `/me?mode=shell` so the backend returns profile identity and empty section placeholders quickly
- full-screen loading remains visible until the first visible dashboard area is ready:
  - profile shell exists
  - playback/current/queue/player-recent first attempts have completed
  - Activity/recent sections have loaded, or a real recent-load error occurred
- normal quick `/me` for top/all-time/profile sections runs after visible startup readiness
- full-screen loading is latched off after dashboard release for the current Spotify user/session to avoid flashes during later refreshes

Current relation UI behavior:
- relation badges concatenate `D/R/V/C` in that order instead of using one generic `R`
- representative track pages list Track Family rows under `Variations` and `Covers / remixes / family`
- same-recording release appearances, including the same track on different albums, stay out of the Track Family list
- family lookup for a representative track is anchored to all release-track members in the current recording group because family candidates can be attached to a sibling release track

Current frontend size:
- `frontend/src/App.tsx`: `11,223` lines / `508,612` bytes.
- Original pre-extraction size in this branch was `13,387` lines / `566,339` bytes.

Frontend behavior constraints:
- no Identity Audit redesign in this refactor
- no backend API contract changes
- preserve persisted reviewer state/localStorage semantics
- preserve issue detail lazy rendering

Frontend verification already passed earlier in this branch:
- `cd frontend && npm run build`
- `git diff --check`

Recommended next frontend step:
- Live authenticated QA for startup load order and loading-screen latch behavior.
- If extracting more after startup QA, take one large Identity Audit view at a time, starting with Track Mapping or Review Queue.

## Playback/Homepage UI State
Current playback UI behavior:
- Homepage playback is the full-control surface above Activity.
- The compact popup remains intentionally smaller and omits queue loop/settings.
- Homepage song title opens the track popup in recording view.
- Homepage no longer shows an `Up next` text line under the album area.
- Homepage album art lives below play controls, with album name below the art.
- Clicking homepage album art expands only the left player column and renders a compact album tracklist while keeping the queue visible; clicking album name opens the full album overlay.
- Homepage compact album tracklists use play/title/played columns only, hide preview/tags, stretch across the left column, and only show the scrollbar marker when the list can scroll.
- Album expansion must not stretch or hide the queue column.
- Queue item text opens the track overlay; queue item art starts playback at that queue item.
- Repeated Spotify queue cycles such as `A, B, A, B` are collapsed for display.
- Back/Forward and track-end auto-advance use the ListenLab queue cursor and explicitly start the target track.
- Delay menu behavior:
  - `After this song` advances to the next queued song at 0:00 and pauses it.
  - `15 minutes` is an exact sleep timer.
- Artist overlay behavior:
  - artist pages call `/auth/artist-albums` and use profile/local arrays only as request-failure fallback
  - single artists split backend evidence into `Albums` and `Appears on`
  - shared artist pages show one combined album list
  - empty album sections do not render headings
- Album/track overlay behavior:
  - album title shows year inline and album summary shows loaded track count/runtime
  - album main artists are derived from artists present across the loaded album tracklist when possible, falling back to majority/metadata evidence when album-wide evidence is unavailable
  - album artist names render inline artist images when available
  - album and track overlays split artists into main artist(s) plus `with ...` guests
  - album tracklist header shows track count/runtime instead of `Title`
  - album tracklist has optional `With`, `Tags`, `Preview`, and `Played` columns; the `With` column is removed for albums with no guest artists
  - liked badges render next to track names, while release/recording tags render in `Tags`
  - release view album-track rows show liked state only for the exact Spotify source track; recording view keeps the aggregate liked fallback across related source versions
  - tracklist can stay mounted while the selected album changes, so rows update instead of disappearing into a loading-only state
  - partial track-overlay album lists auto-complete from Spotify when cooldown allows; during cooldown they stay local-only and show a `More tracks on Spotify` album link
  - tracklist opens centered on the highlighted/current track when possible and renders a small scrollbar-position marker only when the list can scroll
  - top `with ...` list controls delayed row highlighting and scrolls the first matching row into view
  - row-level `With` artists are clickable but do not trigger hover highlighting
  - row-level `With` clicks open a shared artist page for derived album main artist(s) plus the clicked guest
  - overlay track play, album row play, homepage album row play, and album `Play all` use `PlaybackActionMenu` before taking action
  - playback action choices are `Play now`, `Play next`, and `Add to queue`
  - `Play now` keeps existing playback behavior and album queue seeding
  - `Play next` and `Add to queue` mutate the frontend ListenLab queue, preserving the active cursor when possible
  - the menu is portal-rendered so it anchors to the pressed button instead of being clipped or displaced by overlay scroll containers
  - track preview payloads expose top-level release-track metadata copied from the representative `RecentTrack`
  - track overlays default to recording view on user-facing pages, while Identity Audit/Search/Listen Log backend pages may request release view
  - recording view shows same-recording album appearances separately from broader `Variations`; clicking variation/family cards switches the selected album and representative release track
  - release view shows source-version albums and marks the representative source; selecting a source version keeps star, liked, listened range, listen count, and album-track `Last` exact to that Spotify source version
  - the bottom gear menu exposes Spotify and View Release/Recording Track actions; `View release track` is hidden when there are no separate source versions
  - track overlay playback uses a compact top-left play/time menu; star/bookmark actions sit beside it, and liked/context/listened/listen-count tags sit at the bottom behind a divider
  - recording view combines source-version listen counts and listened dates across generated recording members; release view uses the selected source version only
  - listened/listen-count breakdown popovers are only clickable in recording view when multiple recording members contribute, and opening one closes the other
  - album context tags such as `Single`, `Soundtrack`, and `Compilation` appear in the bottom tag row when source album metadata or album-title evidence supports them
  - track overlays keep Spotify source track id/URI as playback identity even when release/recording/family evidence is shown

Manual QA still required with an active Spotify device/Web Playback SDK session.

## Recording Candidate Cache State
Current backend behavior:
- SQLite schema versions `32` and `33` add generated recording/track-family cluster tables plus a dirty release-track table.
- SQLite schema version `34` adds `source_track_play_count_cache`, a derived per-Spotify-track cache of play count, first listened, and last listened.
- Startup creates the generated candidate cache if it is empty.
- Source-track map upserts, album-track inserts, and release-track merge repoints mark affected release tracks dirty.
- Recent-play sync drains a small dirty batch after inserted rows so newly added tracks can join existing generated clusters without a full rebuild.
- Recent/history projection refreshes `source_track_play_count_cache` after touched fact rows are reloaded.
- `GET /debug/tracks/recording-track-candidates/by-release/{release_track_id}` returns cached generated candidate items for track overlays and falls back to live candidate generation when needed.
- Generated recording candidate members include source-derived play counts and first/last listened dates so recording view can aggregate without scanning the fact view in the frontend.
- The generated tables remain evidence caches only; they do not promote, apply, or mutate canonical identity.

## Activity and Listen Log UI State
Current recent-listening behavior:
- Activity recent list scrolls instead of paginating.
- Activity shows progress bars for listened amount.
- Activity filter options are `Listened` and `All`; default is `Listened`.
- `Listened` means at least 65% of the track was played.
- Activity applies the filter before repeat/dedupe counting.
- Activity display grouping uses `release_track_id` first, then Spotify track id, then normalized text identity.
- Activity grouped rows keep a representative Spotify track id/uri from an actual `RecentTrack` row for playback.
- Player recent dedupe remains Spotify-first and should not be widened without a separate QA pass.
- Activity removed the visible `Recently played` label and the `Skipped` option.
- Activity places the `Listen Log` button at the far right of the header.

Listen Log behavior:
- Title is `Listen Log`.
- Rows include album art and listened progress.
- Play amount toggle options are `Listened`, `All`, and `Skipped`; default is `Listened`.
- `Reload` forces Spotify recent sync before reloading the log.
- Loading more rows does not force Spotify sync.
