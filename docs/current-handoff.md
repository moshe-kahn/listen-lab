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

Latest commit:
- `f4c256d Tighten artist identity audit and repair`

Worktree at handoff:
- docs were updated after the commit above; commit these doc changes separately if desired

Important local instruction:
- `AGENTS.md` says substantial frontend UI should not be added directly to `frontend/src/App.tsx`. This branch still has a large `App.tsx` integration surface from iterative UI work. Future UI work should extract focused components instead of growing `App.tsx` further.

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

## Known Limitations
- Frontend bookmark/star behavior is local placeholder UI; bookmark persistence is not implemented.
- Source/text album and track reconciliation are not broadly implemented.
- Catalog album-track promotion exists as a targeted script, not as automatic catalog backfill behavior.
- Generated recording/track-family clusters are caches only; no durable `recording_track` promotion exists.
- Frontend still lacks focused tests for overlay/tracklist workflows.
- Artist group/member modeling remains intentionally out of scope. Future musician/member graph work is tracked as low-priority future work in `docs/overview/roadmap.md`.

## Recommended Next Task
Best next task depends on the user's priority:

1. Continue source/text identity reconciliation for albums and tracks.
2. Extract the artist duplicate audit UI out of the growing frontend integration surface if more UI work is planned.
3. Add targeted frontend tests or browser smoke checks for the CSNY/comma-name modal behavior.

## Guardrails
- Do not delete old branches or stashes unless explicitly requested.
- Do not merge to `main` unless explicitly requested.
- Keep `frontend-app-refactor` as the integration baseline.
- Backend owns source/release/recording/family identity evidence and future promotion semantics.
- Keep Spotify track id / URI as concrete playback identity.
- Do not make `recording_track` the default aggregation layer without explicit scope.
- Keep catalog backfill enrichment-only; identity mutation belongs in explicit dry-run/apply repair or promotion flows.

## Resume Prompt
Continue in `/Users/kahntra/Programming/Personal Projects/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch is `frontend-app-refactor`, latest commit is `f4c256d Tighten artist identity audit and repair`, and the worktree was clean after commit. Recent work added artist duplicate audit/repair, evidence-gated Spotify/text artist promotion, composite credit classification/cleanup, artist album evidence fallback to internal `album_artist` links, and frontend comma-name display preservation for durable group names such as `Crosby, Stills, Nash & Young`. Verification passed with backend identity tests, `npm run build`, and `git diff --check`. Next likely work: source/text reconciliation for albums/tracks or frontend extraction/tests for the new artist audit UI.
