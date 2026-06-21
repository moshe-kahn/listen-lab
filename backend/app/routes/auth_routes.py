from __future__ import annotations

import logging
import secrets
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import RedirectResponse

from backend.app.auth.oauth_helpers import _callback_redirect_url, _is_configured, _pkce_code_challenge
from backend.app.auth.session import (
    _require_user_id,
    _restore_session_user_from_token_store,
    _session_user_id,
)
from backend.app.auth.token import _refresh_spotify_access_token, _require_token
from backend.app.config import get_settings
from backend.app.db import list_spotify_auth_users
from backend.app.spotify_http import _fetch_spotify_profile
from backend.app.spotify_rate_limit import (
    _spotify_cooldown_seconds_remaining,
    _spotify_rate_limit_detail,
)
from backend.app.spotify_recent_sync import maybe_sync_spotify_recent
from backend.app.spotify_token_store import get_spotify_tokens, upsert_spotify_tokens
from backend.app.utils.time_helpers import _expires_at_from_expires_in

settings = get_settings()
logger = logging.getLogger("listenlabs.auth")
router = APIRouter(tags=["auth"])


def _offline_oauth_profile(request: Request) -> dict[str, Any] | None:
    existing_user = request.session.get("spotify_user") or {}
    existing_id = str(existing_user.get("id") or request.session.get("user_id") or "").strip()
    if existing_id:
        return {
            "id": existing_id,
            "display_name": existing_user.get("display_name"),
            "email": existing_user.get("email"),
        }
    active_users = list_spotify_auth_users(active_only=True, limit=2)
    if len(active_users) != 1:
        return None
    spotify_user_id = str(active_users[0].get("spotify_user_id") or active_users[0].get("user_id") or "").strip()
    return {"id": spotify_user_id, "display_name": None, "email": None} if spotify_user_id else None


@router.get("/auth/login")
async def auth_login(
    request: Request,
    mode: str | None = None,
) -> RedirectResponse:
    if not _is_configured():
        raise HTTPException(status_code=500, detail="Spotify OAuth is not configured.")

    oauth_mode = "recent_ingest" if mode == "recent_ingest" else "default"
    oauth_scope = "user-read-recently-played" if oauth_mode == "recent_ingest" else settings.spotify_scope
    state = secrets.token_urlsafe(32)
    code_verifier = secrets.token_urlsafe(64)
    code_challenge = _pkce_code_challenge(code_verifier)
    request.session["oauth_state"] = state
    request.session["oauth_mode"] = oauth_mode
    request.session["oauth_code_verifier"] = code_verifier

    query = urlencode(
        {
            "client_id": settings.spotify_client_id,
            "response_type": "code",
            "redirect_uri": settings.spotify_redirect_uri,
            "scope": oauth_scope,
            "state": state,
            "code_challenge_method": "S256",
            "code_challenge": code_challenge,
        }
    )

    return RedirectResponse(url=f"{settings.spotify_authorize_url}?{query}", status_code=302)


@router.get("/auth/callback")
async def auth_callback(request: Request, code: str | None = None, state: str | None = None) -> RedirectResponse:
    expected_state = request.session.get("oauth_state")
    if not code or not state or state != expected_state:
        logger.warning("Spotify callback state validation failed.")
        return RedirectResponse(url=_callback_redirect_url("state_error"), status_code=302)

    oauth_mode = str(request.session.get("oauth_mode") or "default")
    code_verifier = request.session.get("oauth_code_verifier")

    token_request_data: dict[str, str] = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": settings.spotify_client_id,
        "redirect_uri": settings.spotify_redirect_uri,
    }
    if code_verifier:
        token_request_data["code_verifier"] = str(code_verifier)

    async with httpx.AsyncClient(timeout=15.0) as client:
        token_response = await client.post(
            settings.spotify_token_url,
            data=token_request_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    if token_response.status_code >= 400:
        detail = ""
        try:
            payload = token_response.json()
            detail = payload.get("error_description") or payload.get("error") or ""
        except ValueError:
            detail = token_response.text[:120]

        logger.warning(
            "Spotify token exchange failed with status %s: %s",
            token_response.status_code,
            detail or "<no detail>",
        )
        return RedirectResponse(
            url=_callback_redirect_url("token_error", detail or f"http_{token_response.status_code}"),
            status_code=302,
        )

    token_data = token_response.json()
    access_token = token_data.get("access_token")
    if not access_token:
        logger.warning("Spotify token exchange succeeded without an access token.")
        return RedirectResponse(url=_callback_redirect_url("token_missing"), status_code=302)

    refresh_token = str(token_data.get("refresh_token") or "").strip()
    if not refresh_token:
        logger.warning("Spotify token exchange succeeded without a refresh token.")
        return RedirectResponse(url=_callback_redirect_url("token_missing_refresh"), status_code=302)

    expires_at = _expires_at_from_expires_in(token_data.get("expires_in"))
    scopes = str(token_data.get("scope") or ("user-read-recently-played" if oauth_mode == "recent_ingest" else settings.spotify_scope))

    try:
        profile = await _fetch_spotify_profile(access_token)
    except HTTPException as exc:
        profile = _offline_oauth_profile(request) if exc.status_code == status.HTTP_429_TOO_MANY_REQUESTS else None
        if profile is None:
            logger.warning("Spotify profile fetch failed after token exchange: %s", exc.detail)
            return RedirectResponse(url=_callback_redirect_url("profile_error"), status_code=302)
        logger.warning(
            "Spotify profile fetch was rate-limited after token exchange; completing OAuth with existing local identity %s.",
            profile["id"],
        )

    spotify_user_id = str(profile.get("id") or "").strip()
    if not spotify_user_id:
        logger.warning("Spotify profile fetch after token exchange returned no user id.")
        return RedirectResponse(url=_callback_redirect_url("profile_missing_id"), status_code=302)

    try:
        upsert_spotify_tokens(
            user_id=spotify_user_id,
            spotify_user_id=spotify_user_id,
            access_token=str(access_token),
            refresh_token=refresh_token,
            expires_at=expires_at,
            scopes=scopes,
        )
    except RuntimeError as exc:
        logger.warning("Failed to persist Spotify tokens after OAuth callback: %s", exc)
        return RedirectResponse(url=_callback_redirect_url("token_store_error"), status_code=302)

    request.session.pop("oauth_state", None)
    request.session.pop("oauth_mode", None)
    request.session.pop("oauth_code_verifier", None)
    request.session["user_id"] = spotify_user_id
    request.session["token_type"] = token_data.get("token_type") or "Bearer"
    request.session["expires_in"] = int(token_data.get("expires_in") or 0)
    request.session["spotify_user"] = {
        "id": spotify_user_id,
        "display_name": profile.get("display_name"),
        "email": profile.get("email"),
    }

    if oauth_mode == "recent_ingest":
        ingest_result: dict[str, Any] = {
            "flow": "recent_ingest",
            "auth_succeeded": True,
            "ingest_succeeded": False,
            "error": None,
            "row_count": 0,
            "earliest_api_played_at": None,
            "latest_api_played_at": None,
        }
        try:
            summary = await maybe_sync_spotify_recent(
                access_token,
                source_ref="oauth_recent_ingest",
                force=True,
                limit=50,
            )
            ingest_result.update(
                {
                    "ingest_succeeded": True,
                    "row_count": int(summary.get("row_count") or 0),
                    "inserted_count": int(summary.get("inserted_count") or 0),
                    "duplicate_count": int(summary.get("duplicate_count") or 0),
                    "already_seen_source_row_count": int(summary.get("already_seen_source_row_count") or 0),
                    "merged_duplicate_row_count": int(summary.get("merged_duplicate_row_count") or 0),
                    "earliest_api_played_at": summary.get("earliest_played_at"),
                    "latest_api_played_at": summary.get("latest_played_at"),
                }
            )
        except Exception as exc:
            ingest_result["error"] = str(exc)

        request.session["recent_ingest_result"] = ingest_result
        return RedirectResponse(
            url=_callback_redirect_url("success", extra={"flow": "recent_ingest"}),
            status_code=302,
        )

    return RedirectResponse(url=_callback_redirect_url("success"), status_code=302)


@router.get("/auth/session")
async def auth_session(request: Request) -> dict[str, Any]:
    user_id = _session_user_id(request) or _restore_session_user_from_token_store(request)
    user = request.session.get("spotify_user") or {}
    token_state = get_spotify_tokens(user_id) if user_id else None
    authenticated = bool(token_state and not token_state.get("reauth_required"))

    return {
        "authenticated": authenticated,
        "display_name": user.get("display_name"),
        "spotify_user_id": user.get("id") or (str(token_state.get("spotify_user_id")) if token_state else None),
        "email": user.get("email"),
        "spotify_cooldown_seconds_remaining": _spotify_cooldown_seconds_remaining(),
    }


@router.get("/auth/full-availability")
async def auth_full_availability(request: Request) -> dict[str, Any]:
    user_id = _session_user_id(request)
    if not user_id:
        return {
            "available": False,
            "blocked": False,
            "reason": "not_authenticated",
            "detail": "Spotify is not connected for this session.",
            "retry_after_seconds": None,
        }

    token_state = get_spotify_tokens(user_id)
    if token_state is None:
        return {
            "available": False,
            "blocked": False,
            "reason": "not_authenticated",
            "detail": "Spotify is not connected for this session.",
            "retry_after_seconds": None,
        }
    if token_state.get("reauth_required"):
        return {
            "available": False,
            "blocked": False,
            "reason": "reauth_required",
            "detail": str(token_state.get("reauth_reason") or "Spotify reauthorization is required."),
            "retry_after_seconds": None,
        }

    remaining = _spotify_cooldown_seconds_remaining()
    if remaining > 0:
        return {
            "available": False,
            "blocked": True,
            "reason": "cooldown_active",
            "detail": _spotify_rate_limit_detail("Spotify is rate-limiting requests right now."),
            "retry_after_seconds": remaining,
        }

    try:
        token = _require_token(request)
        await _fetch_spotify_profile(token)
    except HTTPException as exc:
        if exc.status_code == status.HTTP_401_UNAUTHORIZED:
            try:
                refreshed = await _refresh_spotify_access_token(request)
                await _fetch_spotify_profile(refreshed)
            except HTTPException as retry_exc:
                if retry_exc.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
                    retry_after = _spotify_cooldown_seconds_remaining()
                    return {
                        "available": False,
                        "blocked": True,
                        "reason": "rate_limited",
                        "detail": retry_exc.detail,
                        "retry_after_seconds": retry_after,
                    }
                return {
                    "available": False,
                    "blocked": False,
                    "reason": "unauthorized",
                    "detail": retry_exc.detail,
                    "retry_after_seconds": None,
                }
            else:
                return {
                    "available": True,
                    "blocked": False,
                    "reason": "ok",
                    "detail": "Full Spotify experience is available.",
                    "retry_after_seconds": None,
                }

        if exc.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
            retry_after = _spotify_cooldown_seconds_remaining()
            return {
                "available": False,
                "blocked": True,
                "reason": "rate_limited",
                "detail": exc.detail,
                "retry_after_seconds": retry_after,
            }
        if exc.status_code == status.HTTP_403_FORBIDDEN:
            return {
                "available": False,
                "blocked": False,
                "reason": "missing_scope",
                "detail": exc.detail,
                "retry_after_seconds": None,
            }
        return {
            "available": False,
            "blocked": False,
            "reason": "spotify_unavailable",
            "detail": exc.detail,
            "retry_after_seconds": None,
        }

    return {
        "available": True,
        "blocked": False,
        "reason": "ok",
        "detail": "Full Spotify experience is available.",
        "retry_after_seconds": None,
    }


@router.get("/auth/token")
async def auth_token(request: Request) -> dict[str, Any]:
    _require_user_id(request)

    # Return a freshly refreshed token for playback/API clients so we don't hand
    # out an expired session token.
    try:
        token = await _refresh_spotify_access_token(request)
    except HTTPException as exc:
        if exc.status_code == status.HTTP_401_UNAUTHORIZED:
            request.session.clear()
        raise

    return {
        "access_token": token,
        "token_type": request.session.get("token_type") or "Bearer",
        "expires_in": request.session.get("expires_in"),
    }


@router.post("/auth/logout")
async def auth_logout(request: Request) -> dict[str, str]:
    request.session.clear()
    return {"status": "logged_out"}
