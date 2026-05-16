# Current Handoff

## Read First
Start here in a new chat, then open only the docs relevant to the requested task.

Recommended topic docs:
- `docs/reference/refactor-notes.md` for the current backend `main.py` extraction map and the uncommitted frontend App extraction.
- `docs/reference/drafts/entity-model-draft.md` for release/source/analysis identity, duplicate diagnostics, and merge preview/dry-run.
- `docs/reference/spotify-catalog-backfill.md` for catalog enrichment, queue behavior, lookup, and backfill invariants.
- `docs/reference/raw-ingest.md` for raw play events, recent/history ingest, and fallback history text.
- `docs/reference/drafts/identity-audit-submission-contract.md` only when working on saved track-audit submissions.
- `docs/reference/album-family-review-policy.md` only when working on album-family candidate review.

Avoid reading every doc by default. Historical handoff details were moved into reference docs where they are durable.

## Current State
Active branch: `ui-identity-audit-work`.

Branch status:
- Ahead of `origin/ui-identity-audit-work` by 12 commits.
- Latest commit: `cc6a224` (`Extract static metadata cache`).
- No backend, Vite frontend, or catalog worker process is known to be running.

Working tree:
- `docs/current-handoff.md` is being reworked in this doc cleanup.
- Frontend refactor remains intentionally uncommitted:
  - `frontend/src/App.tsx`
  - `frontend/src/api/appApi.ts`
  - `frontend/src/components/identityAudit/IdentityAuditDiagnostics.tsx`
  - `frontend/src/components/identityAudit/IssueFeed.tsx`
  - `frontend/src/constants/appConstants.ts`
  - `frontend/src/types/appTypes.ts`
  - `frontend/src/utils/identityAuditPrefs.ts`

Backend `main.py` extraction status:
- The recent backend helper extractions are committed.
- `backend/app/main.py` is currently `4,742` lines / `193,125` bytes.
- The extracted module map is documented in `docs/reference/refactor-notes.md`.

Frontend App extraction status:
- `frontend/src/App.tsx` is currently `11,367` lines / `498,613` bytes.
- The uncommitted extraction scope and constraints are documented in `docs/reference/refactor-notes.md`.

## Guardrails
- Do not redesign Identity Audit in the current frontend refactor.
- Do not change backend API contracts unless explicitly requested.
- Keep identity resolution read-only unless the user explicitly asks for an apply/confirm/promote path.
- If committing, stage only the requested scope. The frontend refactor is intentionally dirty until separately reviewed/committed.

## Last Verified
Backend static metadata extraction, before commit `cc6a224`:
- `./.venv/bin/python -m py_compile backend/app/main.py backend/app/cache/static_metadata_cache.py`
- `./.venv/bin/python -c "import backend.app.cache.static_metadata_cache; import backend.app.main; print('main import ok')"`
- `./.venv/bin/python -m unittest discover -s backend/tests` (`328 tests OK`)
- `git diff --check -- backend/app/main.py backend/app/cache/static_metadata_cache.py`

Frontend refactor, earlier in this branch:
- `cd frontend && npm run build`
- `git diff --check`

## Recommended Next Task
Review the remaining frontend refactor and commit it if satisfied. If continuing extraction instead, extract one large Identity Audit view component at a time, starting with Track Mapping or Review Queue.

## Resume Prompt
Continue in `/Users/kahntra/Documents/ListenLab/listen-lab-main`. Read `AGENTS.md` and `docs/current-handoff.md` first. Branch `ui-identity-audit-work` is ahead by 12 backend extraction commits through `cc6a224`. Backend helper extractions are committed and documented in `docs/reference/refactor-notes.md`. The remaining intentional dirty work is the frontend App refactor plus current doc edits: `App.tsx` was reduced to `11,367` lines / `498,613` bytes by extracting API wrappers, Identity Audit issue/diagnostic components, constants, types, and prefs helpers. Do not redesign Identity Audit or change backend APIs.
