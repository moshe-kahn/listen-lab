# ListenLab

ListenLab is a Spotify web app that analyzes real listening behavior to surface overlooked artists, music, and actionable insights.

The core idea is simple: use what you actually listen to, not generic recommendations, to help you rediscover what you already care about.

---

## MVP

The initial version focuses on one primary feature:
- surfacing artists you clearly engage with but have not followed

Planned MVP components:
- React frontend
- FastAPI backend
- Spotify OAuth
- engagement-based scoring
- explanation-first results
- optional playlist generation

---

## Status

This repository now includes:
- React dashboard for authenticated Spotify snapshots
- FastAPI backend with Spotify OAuth and session-based auth
- live profile, playlists, recent listening, liked tracks, top tracks, top artists, and top albums views
- playback controls and session-aware player UI
- restricted local mode, full mode, and a test path for probing Spotify availability
- local history-based artist and album ranking calibration using exported Spotify extended streaming history
- section-level caching for live data, history-derived favorites, history enrichment, and saved Spotify-only sections
- on-disk local analysis cache plus shared static metadata cache for artist and album artwork
- a post-login loading handoff plus sticky dashboard navigation, account/project popovers, and multiple dashboard UI polish passes

The core overlooked-artist analysis flow and playlist generation are still not implemented.

Known gaps still being worked:
- album ranking and album breadth counts still need correction in some cases
- some local-mode artist and album images still fail to persist or hydrate reliably
- recent album lists can still become too sparse for certain accounts and windows

---

## Project Direction

ListenLab is built around **"signal over suggestion"**:
- prioritize real listening behavior over inferred taste
- combine multiple engagement signals such as listening, likes, and saves
- explain why results are surfaced
- avoid black-box recommendations

---

## Docs

- [Architecture](docs/architecture.md)
- [Context](docs/context.md)
- [Roadmap](docs/roadmap.md)
- [Formula Calibration](docs/formula-calibration.md)
- [Auth Milestone Notes](docs/auth-milestone.md)

---

## Current Product Direction

Build toward a web app that:
- connects to a user's Spotify account
- builds reliable artist and album signals from live Spotify data
- calibrates scoring heuristics against exported listening history when available
- remains usable in a local cached mode when Spotify is unavailable or rate-limited
- eventually ranks overlooked artists by actual engagement
- explains why each result was surfaced
- optionally creates a playlist from those artists

---

## Implementation Defaults

- Spotify is the source of truth for live account data
- local exported history can be used for scoring calibration and local-mode fallback, not as a required product dependency
- recent sections should stay fresh while stable favorites and saved Spotify-only sections can come from cache
- analysis runs on demand
- no standalone application database in MVP beyond local cache files
- local development first, simple cloud deployment later
