# ListenLab Architecture

## Purpose
This document is the implementation-oriented technical source of truth for the ListenLab MVP. It is written for AI coding agents first and human collaborators second.

## Current State vs Target State
### Current state
- The repository includes a React dashboard and FastAPI backend running locally.
- Spotify OAuth login, callback handling, session persistence, and authenticated `GET /me` profile loading are implemented.
- The dashboard currently renders profile identity, playlists, recent listening, liked tracks, top tracks, top artists, and top albums.
- A local exported-history analyzer can calibrate artist and album rankings from Spotify extended streaming history when a history directory is configured.
- The core overlooked-artist analysis flow and playlist creation flow are still not implemented.

### Target MVP state
- A React single-page app handles login, analysis actions, result display, and playlist creation controls.
- A FastAPI backend handles Spotify OAuth, Spotify API orchestration, aggregation, scoring, explanation generation, and playlist creation.
- Spotify is the source of truth for user library and listening-related signals.
- Analysis is computed on demand for the active session.
- No persistent application database is used in MVP.
- Local development is the primary target, with a later path to simple cloud hosting for the frontend and a single backend service.

## Architectural Principles
- Use real listening evidence, not recommendation inference.
- Keep scoring modular and configurable.
- Separate Spotify data access from aggregation and scoring logic.
- Always return an explanation for each surfaced result.
- Prefer a small number of reliable system parts over early optimization.
- Design for degraded-but-useful results when Spotify signal availability is limited.

## MVP System Overview
### Runtime shape
- Browser client: React SPA
- API server: FastAPI application
- External dependency: Spotify Web API
- State model: session-based auth, on-demand computation, no persisted app data

### Implemented today
- frontend authenticated dashboard shell
- frontend callback handling
- backend OAuth endpoints
- backend token exchange and session storage
- authenticated `GET /me` snapshot endpoint
- best-effort live Spotify data fetches for profile, playlists, recent listening, liked tracks, top tracks, top artists, and top albums
- optional local history-based artist and album ranking calibration

### High-level flow
1. The user opens the React app and starts Spotify login.
2. The backend runs OAuth with Spotify and stores session state for the logged-in user.
3. The frontend calls `GET /auth/session` and `GET /me`.
4. The backend fetches Spotify profile and listening-related data, and optionally merges in rankings from a local Spotify extended streaming history export.
5. The frontend renders the dashboard snapshot.
6. A later milestone will add `POST /analysis` and `POST /playlist`.

## Recommended Project Layout
This layout should be used when scaffolding the repository:

```text
backend/
  app/
    api/
    clients/
    core/
    models/
    schemas/
    services/
    main.py
frontend/
  src/
    app/
    api/
    features/
      analysis/
      auth/
      playlist/
    types/
docs/
```

## Frontend Responsibilities
The React SPA is responsible for:
- showing authenticated vs unauthenticated states
- starting the Spotify login flow
- providing an "Analyze my listening" action
- rendering ranked overlooked artists
- rendering explanation text and signal breakdowns
- allowing artist selection for playlist generation
- handling loading, empty, and degraded-signal states cleanly

### Frontend constraints
- The frontend must not implement scoring logic.
- The frontend should treat backend responses as the source of ranking truth.
- MVP should use a simple route structure and minimal global state.
- Results should be easy to scan and explanation-first.

## Backend Responsibilities
The FastAPI backend is responsible for:
- Spotify OAuth start and callback handling
- secure session management for the active user
- fetching Spotify data from required and optional endpoints
- normalizing source data into artist-level aggregates
- computing engagement scores through a dedicated scoring service
- generating plain-language explanations
- creating playlists from selected artists

### Backend constraints
- Do not add a database in MVP.
- Keep services composable and testable in isolation.
- Keep Spotify API access behind client or adapter interfaces.
- Return stable response shapes even when some signals are unavailable.

## Session and Persistence Model
### MVP choice
- Use server-side in-memory session storage keyed by a signed session cookie.
- Store Spotify access token, refresh token, expiry metadata, and minimal user identity in the session.
- Assume a single backend instance for MVP and early cloud deployment.

### Implications
- Sessions may be lost on server restart in local development.
- Horizontal scaling is out of scope for MVP.
- Moving to Redis or a database-backed session store is a future enhancement, not an MVP requirement.

## Internal Service Boundaries
### Spotify client or adapters
- Responsible only for calling Spotify endpoints, handling pagination, refreshing tokens, and normalizing low-level API errors.
- Must not contain scoring or business rules.

### Aggregation pipeline
- Converts liked tracks, saved albums, followed artists, and listening proxies into artist-level records.
- Produces `ArtistProfile` objects with raw counts and derived signal fields.

### Scoring service
- Accepts normalized artist profiles and scoring configuration.
- Computes engagement scores independently of fetch logic.
- Supports rich-signal and fallback modes without changing the public result shape.

### Explanation builder
- Converts scoring inputs and filtered results into concise evidence-based text.
- Explains why an artist was surfaced using the strongest available signals.

### Playlist service
- Takes selected artist IDs, fetches top tracks, deduplicates tracks, and creates the Spotify playlist.

## API Surface
The MVP backend should expose the following endpoints.

### `GET /health`
Purpose:
- Simple local and deployment health check.

Response:
- `200 OK` with a minimal status payload.

### `GET /auth/login`
Purpose:
- Start Spotify OAuth.

Behavior:
- Generate OAuth state.
- Redirect the browser to Spotify authorization.

### `GET /auth/callback`
Purpose:
- Complete Spotify OAuth.

Behavior:
- Validate OAuth state.
- Exchange the authorization code for tokens.
- Create or update the server-side session.
- Redirect back to the frontend app.

### `GET /auth/session`
Purpose:
- Return current authentication state for the SPA.

Response shape:
- `authenticated: boolean`
- `display_name: string | null`
- `spotify_user_id: string | null`

### `POST /auth/logout`
Purpose:
- Clear the active session.

### `GET /me`
Purpose:
- Return the authenticated dashboard snapshot for the current user.

Implemented data today:
- profile identity and Spotify profile URL
- playlists owned by the user and marked public by Spotify
- recently played tracks
- recently liked tracks
- top tracks
- top artists
- top albums
- optional `history_insights_available` flag when local exported history is being used to rank artists and albums

### `POST /analysis`
Purpose:
- Run the full ListenLab analysis for the current user.

Status:
- Planned, not implemented yet.

Request body:
```json
{
  "limit": 25
}
```

Response shape:
```json
{
  "generated_at": "2026-04-08T00:00:00Z",
  "scoring_mode": "rich_signals",
  "results": []
}
```

Notes:
- `limit` is optional and should default to a sensible UI count such as 25.
- Scoring weights are configured server-side in MVP and are not a public API surface yet.

### `POST /playlist`
Purpose:
- Create a playlist from selected surfaced artists.

Status:
- Planned, not implemented yet.

Request body:
```json
{
  "name": "Overlooked Favorites",
  "artist_ids": ["artist_1", "artist_2"],
  "tracks_per_artist": 2
}
```

Response shape:
```json
{
  "playlist_id": "spotify_playlist_id",
  "playlist_name": "Overlooked Favorites",
  "playlist_url": "https://open.spotify.com/playlist/...",
  "added_track_count": 4,
  "skipped_artist_ids": []
}
```

## Core Data Contracts
### `ArtistProfile`
Internal artist-level aggregate used before and after scoring.

Fields:
- `artist_id: string`
- `artist_name: string`
- `is_followed: boolean`
- `liked_track_count: int`
- `saved_album_count: int`
- `play_count: int`
- `listening_minutes: float`
- `recent_play_count: int`
- `recent_listening_minutes: float`
- `engagement_score: float | null`

Rules:
- Numeric counts should default to `0`, not `null`.
- When a signal is unavailable, the profile still exists with zero-value fields; availability is explained through the breakdown contract.

### `EngagementSignalBreakdown`
Structured explanation of how the score was formed.

Fields:
- `formula_version: string`
- `listening_minutes: { raw: float, weight: float, weighted: float, status: string }`
- `play_count: { raw: int, weight: float, weighted: float, status: string }`
- `liked_track_count: { raw: int, weight: float, weighted: float, status: string }`
- `saved_album_count: { raw: int, weight: float, weighted: float, status: string }`
- `recency_bonus: { raw: float, weight: float, weighted: float, status: string }`
- `total_score: float`

Status values:
- `observed`
- `estimated`
- `derived`
- `unavailable`

### `OverlookedArtistResult`
Public analysis result returned to the frontend.

Fields:
- `artist_id: string`
- `artist_name: string`
- `engagement_score: float`
- `explanation: string`
- `breakdown: EngagementSignalBreakdown`

Rules:
- Only artists with `is_followed = false` are returned.
- Explanation text must cite the strongest available signals and remain evidence-based.

### `PlaylistCreationRequest`
Payload sent from the frontend to the backend.

Fields:
- `name: string`
- `artist_ids: string[]`
- `tracks_per_artist: int`

Rules:
- `artist_ids` should come from the current analysis result set.
- `tracks_per_artist` should default to `2` in the UI.

### `PlaylistCreationResult`
Result returned after successful playlist creation.

Fields:
- `playlist_id: string`
- `playlist_name: string`
- `playlist_url: string`
- `added_track_count: int`
- `skipped_artist_ids: string[]`

## Spotify Data Strategy
### Required Spotify data
- liked tracks from `/me/tracks`
- followed artists from `/me/following`
- recent listening from `/me/player/recently-played`
- top artists from `/me/top/artists`
- top tracks from `/me/top/tracks`
- playlists from `/me/playlists`
- top tracks for playlist creation from `/artists/{id}/top-tracks`

### Optional or best-effort Spotify data
- saved albums from `/me/albums` in later milestones
- local extended streaming history export for calibration and richer artist/album ranking
- best-effort album enrichment through lightweight Spotify album search when history-ranked albums need images and URLs

### Local history calibration path
- When `SPOTIFY_HISTORY_DIR` points to a valid Spotify extended streaming history export, the backend loads the local JSON files and derives artist and album rankings from them.
- This path is meant to calibrate formulas and support power-user local development.
- It must not become a hard dependency for the MVP, because most users will only provide live Spotify API access.

### Two scoring paths
#### Rich-signal path
- Use observed listening minutes, recent activity, and play-based signals when available.

#### Fallback path
- Use proxies such as play frequency indicators, liked tracks, saved albums, and recency heuristics.

Requirement:
- Both paths must produce the same public result schema.

## Filtering and Ranking Rules
- Aggregate all artist signals before filtering.
- Exclude followed artists from surfaced overlooked-artist results.
- Rank remaining artists by engagement score descending.
- Break score ties by stronger listening-minute signal, then stronger play-count signal, then artist name.

## Error Handling Expectations
- Unauthenticated requests to `POST /analysis` and `POST /playlist` should return `401`.
- Spotify API failures should be translated into clear backend errors without leaking raw provider payloads.
- Partial data availability should degrade results, not crash analysis.
- The frontend should receive stable response shapes even when zero results are returned.

## Deployment Model
### Local development
- Run the React app locally.
- Run the FastAPI server locally.
- Use Spotify developer app credentials via environment variables.

### Simple cloud target
- Host the React app as a static SPA.
- Host the FastAPI app as a single web service.
- Keep one backend instance for MVP so in-memory sessions remain valid.

## Non-Goals for MVP
- no recommendation engine based on similarity or genre
- no persistent analytics database
- no background jobs or scheduled sync
- no multi-user admin features
- no concert, album completion, or social features

## Implementation Defaults
- Frontend framework: React
- Backend framework: FastAPI
- Auth provider: Spotify OAuth
- Persistence: none beyond in-memory session storage
- Ranking logic owner: backend scoring service
- Explanation owner: backend explanation service
