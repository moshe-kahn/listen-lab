# Current Handoff

## Read First
Start here in a new chat, then open only the docs relevant to the requested task.

Recommended topic docs:
- `docs/reference/refactor-notes.md` for frontend/playback refactor and route extraction notes.
- `docs/reference/raw-ingest.md` for raw play events, recent/history ingest, recent-sync throttling, and fallback history text.
- `docs/reference/spotify-catalog-backfill.md` for catalog enrichment, album-track fallback/cache, lookup, and backfill invariants.
- `docs/reference/drafts/entity-model-draft.md` for release/source/analysis identity and duplicate diagnostics.
- `docs/reference/drafts/identity-audit-submission-contract.md` only when working on saved track-audit submissions.
- `docs/reference/album-family-review-policy.md` only when working on album-family candidate review.

Avoid reading every doc by default.

## Current State
Active branch: `frontend-app-refactor`.

Latest commit:
- `9dcb175` `Refine playback queue and recent listening UX`

Working tree is dirty with uncommitted changes:
- `backend/app/config.py`
- `backend/app/db.py`
- `backend/app/main.py`
- `backend/app/liked_tracks.py` (new)
- `backend/tests/test_liked_tracks.py` (new)
- `docs/current-handoff.md`
- `docs/overview/architecture.md`
- `docs/overview/context.md`
- `docs/overview/roadmap.md`
- `docs/reference/refactor-notes.md`
- `frontend/src/App.tsx`
- `frontend/src/api/appApi.ts`
- `frontend/src/styles.css`
- `frontend/src/types/appTypes.ts`

Known local processes:
- Existing `Python` process may be listening on `127.0.0.1:8000` from outside this session.
- Existing `node` process may be listening on `127.0.0.1:5173` from outside this session.
- Temporary QA servers started by Codex on `127.0.0.1:8765` and `127.0.0.1:5174` were stopped.

## Uncommitted Work Summary
Playback home and queue polish:
- Playback home title was enlarged and made to use available width.
- Fallback ListenLab queue label changed from `Recent likes` to `Recent Likes`.
- Queue timer and gear dropdowns now close when clicking outside.
- Opening timer closes gear; opening gear closes timer.
- Queue controls now support organize mode with remove, drag reorder, sort, and group controls.
- ListenLab queue tracks keep played/current/up-next state more visibly.
- Queue cursor scrolling now applies to both popup and home queue surfaces.
- Preview playback preserves base playback state and can resume after preview stop.
- Preview playback disables normal transport movement while active.
- Album track rows support preview/play flows and album `Play all` actions.

Liked-track cache and sync:
- Added user-scoped read-only liked-track cache tables via `backend/app/db.py` migration:
  - `spotify_liked_track_cache`
  - `spotify_liked_track_sync_state`
- Added `backend/app/liked_tracks.py` service/helper module.
- Sync uses existing Spotify `_spotify_get` path and `GET /me/tracks` with `limit=50` and offset pagination.
- Quick sync fetches bounded pages for freshness.
- Full sync pages until natural end unless stopped by cap, timeout, rate limit, auth/forbidden, network, parse, or malformed response.
- Unlike detection only runs after a full sync reaches Spotify natural end.
- Partial full syncs never mark cached rows unliked.
- Page-level Spotify response shape is validated before natural-end checks.
- Individual malformed saved-track items are skipped with warnings.
- Cache rows are scoped by `(user_id, spotify_track_id)` and use `is_liked=1` for active likes.
- Cache reads do not require a live Spotify profile call.
- Dev/test sync-failure simulation exists for local QA only:
  - frontend sends simulation body/header only in Vite dev
  - backend honors it only when `LISTENLAB_ENABLE_DEBUG_SYNC_FAILURE=1` and `X-ListenLab-Debug-Sync-Failure: 1` are both present
  - simulated failures do not mutate cache rows or sync metadata and do not call token/Spotify helpers

Liked-track endpoints:
- Added `GET /me/liked-tracks`.
- Added `POST /me/liked-tracks/sync`.
- Added `GET /me/liked-tracks/contains?spotify_track_id=...`.
- `contains` is cache-only, user-scoped, returns `{ spotify_track_id, is_liked }`, and does not call Spotify.

Frontend liked-track integration:
- Recent Likes now reads `GET /me/liked-tracks` on dashboard load.
- Cached liked tracks are preferred when available.
- If cache is empty but old direct Spotify latest-likes payload exists, UI labels it as latest/fallback and still shows `Sync Likes`.
- Added visible `Sync Likes` action that calls quick sync.
- Successful sync reloads `GET /me/liked-tracks`.
- Missing/failed sync keeps visible cached/fallback rows and shows error copy.
- No like/unlike write UI was added.
- Frontend does not use `/me/liked-tracks/contains` yet.

Docs updated for staged-ready scope:
- `docs/overview/architecture.md`
- `docs/overview/context.md`
- `docs/overview/roadmap.md`
- `docs/reference/refactor-notes.md`
- `docs/current-handoff.md`

Deferred:
- Like/unlike write actions.
- `user-library-modify` scope plumbing.
- Live Spotify contains helper.
- Full UI pagination for large liked-track cache.
- Browser smoke for every playback queue/preview edge case.

## Verification
Passed in this session:
- `python3 -m unittest backend.tests.test_liked_tracks`
- `python3 -m py_compile backend/app/liked_tracks.py backend/app/main.py`
- `python3 -m py_compile backend/app/liked_tracks.py backend/app/main.py backend/app/db.py`
- `npm run build --prefix frontend`
- `git diff --check`
- Earlier targeted `git diff --check` for liked-track and frontend files also passed.

Manual QA performed:
- Authenticated session `kahnman91` loaded dashboard successfully.
- Initial empty liked-track cache showed old `profile.recent_likes_tracks` rows under `Latest from Spotify. Sync Likes to populate the local cache.`
- Clicking `Sync Likes` populated cache and removed fallback label.
- Backend `/me/liked-tracks` returned `source_label: "liked_cache"` rows, `has_more: true`, `last_sync_mode: "quick"`, `last_stopped_reason: "cap_reached"`, `last_tracks_seen: 100`, `last_active_count: 100`.
- Simulated `missing_scope` sync failure using dev query/header path kept visible rows and showed library-access guidance.
- Browser console had no errors during liked-track success/failure QA.

Not fully verified:
- Full browser smoke for all playback queue organizer, preview resume, album queue, and cursor preservation behavior.
- Failure preservation against a real Spotify auth/scope failure; simulated failure was verified instead.
- Production deployment env was not exercised; production frontend build was checked to omit debug simulation strings.

## Next Task
Recommended next step:
- Review the full dirty diff once more, then confirm whether to commit.

Before commit, already passed:
- `python3 -m unittest backend.tests.test_liked_tracks`
- `python3 -m py_compile backend/app/liked_tracks.py backend/app/main.py`
- `npm run build --prefix frontend`
- `git diff --check`

Optional extra before commit:
- Run broader backend test discovery if you want more confidence across unrelated backend surfaces.
- Browser smoke the playback queue organizer and preview resume flows with an active Spotify device/Web Playback SDK session.

## Guardrails
- Do not delete old branches or stashes unless explicitly requested.
- Do not merge to `main` unless explicitly requested.
- Keep `frontend-app-refactor` as the integration baseline.
- Old topic branches should remain archive/source-only.
- If the user says `end session`, update this file and provide a short resume prompt.
- If the user says `end session and commit`, update this file and reference docs, summarize staged-ready changes, and propose a commit message. Do not commit unless the user explicitly confirms.

## Resume Prompt
Continue in `/Users/kahntra/Documents/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch is `frontend-app-refactor`; latest commit is `9dcb175`. Working tree has uncommitted liked-track cache/sync work, guarded dev/test sync-failure simulation, frontend Recent Likes cache UI, and playback/queue/preview polish. Dirty files include backend config/db/main, new `backend/app/liked_tracks.py`, new `backend/tests/test_liked_tracks.py`, overview/reference docs, and frontend `App.tsx`, API helpers, styles, and types. Targeted liked-track tests, backend py_compile, frontend build, and `git diff --check` passed. Manual QA verified liked-track fallback, successful sync, cached rows, and simulated failure preservation. Next step is review full diff and confirm commit; optional extra is broader backend tests or playback browser smoke.
