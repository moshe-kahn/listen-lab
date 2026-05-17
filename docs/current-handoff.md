# Current Handoff

## Read First
Start here in a new chat, then open only the docs relevant to the requested task.

Recommended topic docs:
- `docs/reference/refactor-notes.md` for prior extraction notes.
- `docs/reference/spotify-catalog-backfill.md` for catalog enrichment, lookup, and backfill invariants.
- `docs/reference/raw-ingest.md` for raw play events, recent/history ingest, and fallback history text.
- `docs/reference/drafts/entity-model-draft.md` for release/source/analysis identity and duplicate diagnostics.
- `docs/reference/drafts/identity-audit-submission-contract.md` only when working on saved track-audit submissions.
- `docs/reference/album-family-review-policy.md` only when working on album-family candidate review.

Avoid reading every doc by default.

## Current State
Active branch: `frontend-app-refactor`.

Branch status:
- Created from `playback-tweaks`.
- Working tree is clean before this handoff update.
- Latest commit before this branch: `5cd4c80` (`Fix startup player source priority`).
- No backend, Vite frontend, or catalog worker process is known to be running.

Playback state:
- Recent playback queue/source work is committed on the branch history:
  - `442a187` `Fix player queue fallback and duplicate live queue`
  - `7f16f35` `Add Spotify queue playlist mirror backend`
  - `497da99` `Sync ListenLab queue playlist before playback`
  - `0303c97` `Play ListenLab queue from playlist context`
  - `5cd4c80` `Fix startup player source priority`
- Do not change playback behavior, SDK lifecycle, queue source priority, or queue semantics during the first frontend refactor slice.

Frontend App size:
- `frontend/src/App.tsx`: `500,999` bytes / `11,402` lines.
- Babel warns because this file exceeds 500 KB.

## Refactor Plan
Goal: reduce `App.tsx` size and Codex context use with behavior-preserving slices.

Largest embedded clusters in `App.tsx`:
- App/global state and effects near the top of the component.
- Playback/player logic and player menu JSX. This area changed recently; avoid first.
- Navigation/auth/profile loading.
- Search/catalog lookup helpers and preview openers.
- Ranking/formula/dashboard pure helpers.
- Dashboard card render helpers.
- Identity Audit tabs and helpers. High payoff, but larger prop/state surface.
- Catalog Backfill page.
- Search / Lookup page.
- Recent Debug page.
- Data loaders/actions.
- Main return and detail modal JSX.

Recommended first slice:
- Extract pure, stateless dashboard/ranking/debug helpers into `frontend/src/utils/dashboardUtils.ts`.
- Move only helpers that do not close over React state or setters.
- Leave JSX render helpers, hooks, effects, playback functions, SDK lifecycle, and queue behavior in `App.tsx`.

Likely first files:
- `frontend/src/App.tsx`
- `frontend/src/utils/dashboardUtils.ts`

Later candidate files:
- `frontend/src/components/recentDebug/RecentDebugPage.tsx`
- `frontend/src/components/catalogBackfill/CatalogBackfillPage.tsx`
- `frontend/src/components/searchLookup/SearchLookupPage.tsx`
- `frontend/src/components/identityAudit/*Tab.tsx`

## Verification
Run after each refactor slice:
- `cd frontend && npm run build`
- `git diff --check`

For larger JSX/component slices, manually smoke check:
- Dashboard loads.
- Track/artist/album previews still open.
- Identity Audit tab still renders.
- Catalog/Search pages still load.
- Player menu still opens, without changing playback behavior.

## Guardrails
- Do not touch playback hooks, Spotify SDK lifecycle, queue source logic, queue playlist sync, or `playTrackUri` in the first refactor slice.
- Do not move stateful effects in the first slice.
- Avoid changing render order or callback behavior.
- Avoid splitting `types/appTypes.ts` in the same slice.
- Stage only the requested files if committing.

## Resume Prompt
Continue in `/Users/kahntra/Documents/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Current branch is `frontend-app-refactor`, created from `playback-tweaks`; latest commit is `5cd4c80` (`Fix startup player source priority`). `App.tsx` is `500,999` bytes / `11,402` lines, and the next intended task is a behavior-preserving frontend refactor. First recommended slice: extract pure stateless dashboard/ranking/debug helpers to `frontend/src/utils/dashboardUtils.ts`. Do not touch playback behavior, SDK lifecycle, queue source priority, queue playlist sync, or `playTrackUri` in the first slice.
