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
- `15d2368` `Merge branch 'playback-tweaks-v2' into frontend-app-refactor`

Working tree is dirty with uncommitted changes:
- `backend/app/listening_log.py`
- `backend/app/main.py`
- `backend/app/recent_tracks_db.py`
- `backend/app/routes/admin_routes.py`
- `backend/app/routes/auth_routes.py`
- `backend/app/routes/playback_routes.py`
- `backend/app/spotify_recent_polling.py`
- `backend/app/spotify_recent_sync.py`
- `docs/current-handoff.md`
- `frontend/src/App.tsx`
- `frontend/src/components/dashboard/DashboardListCard.tsx`
- `frontend/src/components/dashboard/DashboardTrackColumn.tsx`
- `frontend/src/components/recentDebug/RecentDebugPage.tsx`
- `frontend/src/constants/appConstants.ts`
- `frontend/src/styles.css`
- `frontend/src/types/appTypes.ts`
- `frontend/src/utils/playbackUtils.ts`

Diff size before this handoff/reference-doc refresh:
- 17 files changed, 2779 insertions, 372 deletions.

Known local processes:
- Existing `node` processes may be listening on `127.0.0.1:5173` from outside this session.
- No new dev server was intentionally left running by Codex.

## Uncommitted Work Summary
Playback overlay and queue:
- Replaced the overlay external-link arrow with a Spotify logo.
- Restored visible song title text in the playback overlay.
- Playback controls now render as a full homepage panel above Activity; the popup menu remains compact.
- Homepage player title opens the song popup.
- Removed the homepage `Up next` line under the album area.
- Homepage album art moved below play controls; album name moved below the art.
- Clicking homepage album art/name expands only the left player column and shows album tracks using the same row display/actions as the song overlay.
- Album expansion no longer stretches the queue column.
- Changed ListenLab queues from upcoming-only to full queue plus cursor.
- Back/Forward explicitly starts the target queued track.
- Track-end auto-advance starts the next ListenLab queue item.
- Queue display auto-scrolls so `Up next` is anchored at the top; current/prior tracks remain available above by scrolling.
- Queue item text opens the track overlay; queue item album art jumps playback to that queue item.
- Spotify queue loading prepends the current track when Spotify only returns upcoming items.
- Queue controls include queue loop, shuffle, stopwatch delay menu, and settings/clear queue.
- Current-song loop is separate from queue loop.
- Clear queue suppresses fallback queue refill.
- Queue title uses the queue context label and links when a Spotify URL is available.
- Compact popup has a separate `Up next` section below transport with clickable title/artist.
- Compact popup action buttons are current-song loop, delay, and shuffle; queue loop/settings are omitted.
- Delay menu:
  - `After this song` advances to the next queued song at 0:00 and pauses it.
  - `15 minutes` is an exact sleep timer that pauses playback when it expires.
  - Delay controls can take over read-only external Spotify playback.

Album tracks/fallback:
- Backend `/auth/playback/album-tracks` fetches Spotify album tracklists when local DB data is insufficient.
- Fetched album tracks/catalog rows are cached for later opens.
- Homepage album expansion uses the same endpoint.
- Overlay album queue and homepage album row play use album queue context.

Activity / Recently played:
- Activity recent list is scrollable instead of paginated.
- Activity progress bars show how much was listened.
- Activity filter options are `Listened` by default and `All`; `Skipped` was removed from Activity.
- `Listened` means listened ratio is at least 65%; `All` includes skips.
- Filter is applied before dedupe/repeat counting.
- Repeat counts use the filtered set: two 70% plays plus one 30% play shows `x2` for `Listened` and `x3` for `All`.
- Activity bars use the same completion helper as playback recent rows.
- Backend recent section now prefers unified Listen Log timing data when a recent row matches.
- Removed the visible `Recently played` header text.
- `Listen Log` button moved to the far right of the Activity header and opens the Listen Log page.

Listen Log:
- Page title is `Listen Log`.
- Rows show album art when available.
- Backend response joins recent/history/player/catalog sources for duration, art, URL, gap, and completion fields.
- Completion ratio can show full bars when duration/estimated-played data support it.
- Listen Log has play amount toggle: `Listened`, `All`, `Skipped`; default is `Listened`; threshold is 65%.
- `Reload log` was renamed to `Reload`.
- Opening Listen Log and clicking `Reload` force a Spotify recent sync first.
- Loading more Listen Log rows does not force a Spotify recent sync.

Spotify recent sync / polling:
- Frontend live playback poll interval is 30 minutes.
- Removed the immediate track-end `/auth/recent-ingest/poll-now` trigger.
- Backend added `maybe_sync_spotify_recent(access_token, source_ref, force=false, min_interval_seconds=30*60, limit=50)` with an async lock and last-completed throttle check.
- Scheduled/background polling uses a 30 minute minimum interval.
- App/profile/recent-section loads use a 10 minute minimum interval.
- Explicit manual paths force sync:
  - experimental overlay refresh icon
  - opening Listen Log
  - Listen Log `Reload`
  - `/auth/recent-ingest/poll-now`
  - OAuth recent ingest flow
- Ordinary dashboard reloads/range changes are not forced.
- Experimental overlay has a top-right refresh icon that forces Spotify data refresh unless Spotify cooldown/loading blocks it.
- `/me/recent` supports `force_recent_sync`.
- `spotify_recent_polling.poll_recent_for_user(user_id, force=False)` reports skipped/throttled status metadata.

## Verification
Passed after the uncommitted changes:
- `npm run build --prefix frontend`
- `git diff --check`
- `./.venv/bin/python -m py_compile backend/app/routes/playback_routes.py backend/app/listening_log.py backend/app/recent_tracks_db.py backend/app/spotify_recent_sync.py backend/app/spotify_recent_polling.py backend/app/main.py backend/app/routes/auth_routes.py backend/app/routes/admin_routes.py`
- `./.venv/bin/python -m unittest backend.tests.test_spotify_queue_playlist backend.tests.test_spotify_current_playback`

Browser smoke:
- Opened `http://127.0.0.1:5173` with the in-app browser.
- Inactive homepage player rendered.
- Earlier smoke verified the homepage player and compact popup shape, but active Spotify playback still needs a connected device/Web Playback SDK session.

## Next Task
Recommended next step:
- Manually QA playback and recent sync with a connected Spotify device:
  - queue Back/Forward starts the intended previous/next track
  - track-end auto-advance starts next queued item
  - queue scroll anchors `Up next`
  - queue text opens the track overlay
  - queue album art jumps playback
  - `After this song` queues next track paused at 0:00
  - `15 minutes` pauses exactly at timer expiry
  - compact popup title/artist/buttons work
  - homepage album expansion shows album rows without stretching queue
  - homepage album rows play/open/preview like overlay rows
  - Activity `Listened`/`All` filters, bars, and repeat counts match 65% rules
  - Listen Log open/reload force recent sync; load more does not
  - experimental overlay refresh forces sync and respects Spotify cooldown
  - ordinary dashboard reloads/range changes do not force sync

After manual QA:
- If clean, commit the uncommitted playback/listen-log/recent-sync changes together.

## Guardrails
- Do not delete old branches or stashes unless explicitly requested.
- Do not merge to `main` unless explicitly requested.
- Keep `frontend-app-refactor` as the integration baseline.
- Old topic branches should remain archive/source-only.
- If the user says `end session`, update this file and provide a short resume prompt.
- If the user says `end session and commit`, update this file and reference docs, summarize staged-ready changes, and propose a commit message. Do not commit unless the user explicitly confirms.

## Resume Prompt
Continue in `/Users/kahntra/Documents/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch is `frontend-app-refactor`; latest commit is `15d2368` (`Merge branch 'playback-tweaks-v2' into frontend-app-refactor`). Working tree has uncommitted playback/queue, homepage player album expansion, album-track fallback/cache, Activity recently-played filters/bars, Listen Log play amount filters/forced reload, and Spotify recent-sync throttling changes. Build, `git diff --check`, backend py_compile, and targeted backend unittest commands passed. Next step is manual QA with a connected Spotify device, especially queue navigation, auto-advance, delay timer, sleep timer, compact popup controls, homepage album expansion, Activity 65% listened filtering, Listen Log forced reload, and experimental forced Spotify refresh.
