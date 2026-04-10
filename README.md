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
- local history-based artist and album ranking calibration using exported Spotify extended streaming history

The core overlooked-artist analysis flow and playlist generation are still not implemented.

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
- [Auth Milestone Notes](docs/auth-milestone.md)

---

## Current Product Direction

Build toward a web app that:
- connects to a user's Spotify account
- builds reliable artist and album signals from live Spotify data
- calibrates scoring heuristics against exported listening history when available
- eventually ranks overlooked artists by actual engagement
- explains why each result was surfaced
- optionally creates a playlist from those artists

---

## Implementation Defaults

- Spotify is the source of truth for live account data
- local exported history can be used for scoring calibration, not as a required product dependency
- analysis runs on demand
- no database in MVP
- local development first, simple cloud deployment later
