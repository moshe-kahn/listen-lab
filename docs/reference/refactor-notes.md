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

Extracted route modules:
- `backend/app/routes/audit_routes.py`
- `backend/app/routes/admin_routes.py`
- `backend/app/routes/playback_routes.py`
- `backend/app/routes/auth_routes.py`

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

Current uncommitted frontend extraction:
- `frontend/src/api/appApi.ts`
  - API fetch/post wrappers that were previously in `App.tsx`
- `frontend/src/components/identityAudit/IssueFeed.tsx`
  - Identity Audit issue feed component and related issue sort/review types
- `frontend/src/components/identityAudit/IdentityAuditDiagnostics.tsx`
  - Identity Audit diagnostic rendering helpers
- `frontend/src/constants/appConstants.ts`
  - shared app constants, initial section state, Spotify logo URL, UI option arrays
- `frontend/src/types/appTypes.ts`
  - broad response/UI/player/profile/catalog/audit types
- `frontend/src/utils/identityAuditPrefs.ts`
  - Identity Audit localStorage preference load/save helpers and persisted preference types

Current frontend size:
- `frontend/src/App.tsx`: `11,367` lines / `498,613` bytes.
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
- Review and commit the existing extraction if satisfied.
- If extracting more first, take one large Identity Audit view at a time, starting with Track Mapping or Review Queue.
