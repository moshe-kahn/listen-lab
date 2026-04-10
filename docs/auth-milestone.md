# Authenticated Dashboard Run Notes

## What this milestone now includes
- React frontend dashboard with Spotify login
- frontend callback handling
- FastAPI backend with Spotify OAuth endpoints
- backend token exchange and session storage
- authenticated `GET /me` snapshot endpoint
- dashboard sections for playlists, recent activity, top tracks, top artists, and top albums
- optional local history calibration for artist and album rankings

## Status
- Implemented
- Manually verified locally against Spotify OAuth
- Uses `127.0.0.1` consistently for backend, frontend, and Spotify redirect configuration

## Backend setup
1. Copy `backend/.env.example` to `backend/.env`.
2. Fill in Spotify app credentials.
3. Install dependencies:

```bash
py -m pip install -r backend/requirements.txt
```

4. Run the API:

```bash
py -m uvicorn app.main:app --reload --app-dir backend
```

## Frontend setup
1. Copy `frontend/.env.example` to `frontend/.env`.
2. Install dependencies:

```bash
npm install --prefix frontend
```

3. Run the app:

```bash
npm run dev --prefix frontend
```

## Spotify app settings
Set the Spotify redirect URI to:

```text
http://127.0.0.1:8000/auth/callback
```

## Manual verification flow
1. Open the frontend at `http://127.0.0.1:5173`.
2. Click `Log in with Spotify`.
3. Complete Spotify authorization.
4. Confirm you return to the frontend and see an authenticated session.
5. Confirm the dashboard loads profile data from Spotify.
6. Confirm playlists, recent activity, and top sections appear without repeated auth errors.
7. If local history calibration is configured, confirm top artists and albums reflect exported listening history.

## Optional local history calibration
If you have a Spotify extended streaming history export locally, set:

```text
SPOTIFY_HISTORY_DIR=C:\path\to\Spotify Extended Streaming History
```

This is optional and intended for local calibration and richer artist/album ranking. The product should still work for users who only provide live Spotify API access.

## Known next step
- turn the current snapshot dashboard into the final overlooked-artist analysis experience with explanation-first ranking
