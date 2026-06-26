from __future__ import annotations

from typing import Any

import httpx
from fastapi import HTTPException, status

from backend.app.config import get_settings
from backend.app.spotify_rate_limit import (
    _enforce_spotify_cooldown,
    _note_spotify_rate_limit,
    _parse_retry_after_seconds,
    _spotify_rate_limit_detail,
)

settings = get_settings()


def _spotify_error_detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
        error_payload = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error_payload, dict):
            return str(error_payload.get("message") or error_payload.get("reason") or "").strip()
        return str(payload.get("error_description") or payload.get("error") or "").strip() if isinstance(payload, dict) else ""
    except ValueError:
        return response.text[:160].strip()


async def _fetch_spotify_profile(access_token: str) -> dict[str, Any]:
    _enforce_spotify_cooldown()
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(
                settings.spotify_me_url,
                headers={"Authorization": f"Bearer {access_token}"},
            )
    except httpx.TimeoutException as exc:
        raise HTTPException(status_code=503, detail="Spotify profile request timed out.") from exc
    except httpx.RequestError as exc:
        raise HTTPException(status_code=503, detail="Spotify profile request could not connect.") from exc

    if response.status_code == status.HTTP_401_UNAUTHORIZED:
        raise HTTPException(status_code=401, detail="Spotify access token is no longer valid.")
    if response.status_code == status.HTTP_403_FORBIDDEN:
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("error", {}).get("message") or payload.get("error_description") or ""
        except ValueError:
            detail = response.text[:160]
        raise HTTPException(
            status_code=403,
            detail=f"Spotify profile access was denied{f': {detail}' if detail else ''}.",
        )
    if response.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
        retry_after_header = response.headers.get("Retry-After")
        retry_after_seconds = _parse_retry_after_seconds(retry_after_header)
        _note_spotify_rate_limit(retry_after_seconds)
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("error", {}).get("message") or payload.get("error_description") or ""
        except ValueError:
            detail = response.text[:160]
        raise HTTPException(
            status_code=429,
            detail=_spotify_rate_limit_detail(
                f"Spotify rate limit reached while fetching your profile{f': {detail}' if detail else ''}.",
            ),
        )
    if response.status_code >= 400:
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("error", {}).get("message") or payload.get("error_description") or ""
        except ValueError:
            detail = response.text[:160]
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch Spotify profile (status {response.status_code}){f': {detail}' if detail else ''}.",
        )

    return response.json()


async def _spotify_get(access_token: str, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    _enforce_spotify_cooldown()
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {access_token}"},
            )
    except httpx.TimeoutException as exc:
        raise HTTPException(status_code=503, detail="Spotify data request timed out.") from exc
    except httpx.RequestError as exc:
        raise HTTPException(status_code=503, detail="Spotify data request could not connect.") from exc

    if response.status_code == status.HTTP_401_UNAUTHORIZED:
        raise HTTPException(status_code=401, detail="Spotify access token is no longer valid.")
    if response.status_code == status.HTTP_403_FORBIDDEN:
        detail = _spotify_error_detail(response)
        raise HTTPException(
            status_code=403,
            detail=f"Spotify denied access to this resource{f': {detail}' if detail else ''}.",
        )
    if response.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
        retry_after_header = response.headers.get("Retry-After")
        retry_after_seconds = _parse_retry_after_seconds(retry_after_header)
        _note_spotify_rate_limit(retry_after_seconds)
        raise HTTPException(status_code=429, detail=_spotify_rate_limit_detail("Spotify rate limit reached for this resource."))
    if response.status_code >= 400:
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("error_description") or payload.get("error", {}).get("message") or ""
        except ValueError:
            detail = response.text[:160]
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch Spotify data from {url} (status {response.status_code}){f': {detail}' if detail else ''}",
        )

    return response.json()


async def _spotify_post(access_token: str, url: str, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
    _enforce_spotify_cooldown()
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.post(
            url,
            json=json_body,
            headers={"Authorization": f"Bearer {access_token}"},
        )

    if response.status_code == status.HTTP_401_UNAUTHORIZED:
        raise HTTPException(status_code=401, detail="Spotify access token is no longer valid.")
    if response.status_code == status.HTTP_403_FORBIDDEN:
        detail = _spotify_error_detail(response)
        raise HTTPException(
            status_code=403,
            detail=f"Spotify denied access to this resource{f': {detail}' if detail else ''}.",
        )
    if response.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
        retry_after_header = response.headers.get("Retry-After")
        retry_after_seconds = _parse_retry_after_seconds(retry_after_header)
        _note_spotify_rate_limit(retry_after_seconds)
        raise HTTPException(status_code=429, detail=_spotify_rate_limit_detail("Spotify rate limit reached for this resource."))
    if response.status_code >= 400:
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("error_description") or payload.get("error", {}).get("message") or ""
        except ValueError:
            detail = response.text[:160]
        raise HTTPException(
            status_code=502,
            detail=f"Failed to post Spotify data to {url} (status {response.status_code}){f': {detail}' if detail else ''}",
        )

    if response.status_code == status.HTTP_204_NO_CONTENT or not response.content:
        return {}
    return response.json()


async def _spotify_put(access_token: str, url: str, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
    _enforce_spotify_cooldown()
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.put(
            url,
            json=json_body,
            headers={"Authorization": f"Bearer {access_token}"},
        )

    if response.status_code == status.HTTP_401_UNAUTHORIZED:
        raise HTTPException(status_code=401, detail="Spotify access token is no longer valid.")
    if response.status_code == status.HTTP_403_FORBIDDEN:
        detail = _spotify_error_detail(response)
        raise HTTPException(
            status_code=403,
            detail=f"Spotify denied access to this resource{f': {detail}' if detail else ''}.",
        )
    if response.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
        retry_after_header = response.headers.get("Retry-After")
        retry_after_seconds = _parse_retry_after_seconds(retry_after_header)
        _note_spotify_rate_limit(retry_after_seconds)
        raise HTTPException(status_code=429, detail=_spotify_rate_limit_detail("Spotify rate limit reached for this resource."))
    if response.status_code >= 400:
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("error_description") or payload.get("error", {}).get("message") or ""
        except ValueError:
            detail = response.text[:160]
        raise HTTPException(
            status_code=502,
            detail=f"Failed to put Spotify data to {url} (status {response.status_code}){f': {detail}' if detail else ''}",
        )

    if response.status_code == status.HTTP_204_NO_CONTENT or not response.content:
        return {}
    return response.json()


async def _spotify_get_many(access_token: str, url: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    _enforce_spotify_cooldown()
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {access_token}"},
        )

    if response.status_code == status.HTTP_401_UNAUTHORIZED:
        raise HTTPException(status_code=401, detail="Spotify access token is no longer valid.")
    if response.status_code == status.HTTP_403_FORBIDDEN:
        detail = _spotify_error_detail(response)
        raise HTTPException(
            status_code=403,
            detail=f"Spotify denied access to this resource{f': {detail}' if detail else ''}.",
        )
    if response.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
        retry_after_header = response.headers.get("Retry-After")
        retry_after_seconds = _parse_retry_after_seconds(retry_after_header)
        _note_spotify_rate_limit(retry_after_seconds)
        raise HTTPException(status_code=429, detail=_spotify_rate_limit_detail("Spotify rate limit reached for this resource."))
    if response.status_code >= 400:
        detail = ""
        try:
            payload = response.json()
            detail = payload.get("error_description") or payload.get("error", {}).get("message") or ""
        except ValueError:
            detail = response.text[:160]
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch Spotify data from {url} (status {response.status_code}){f': {detail}' if detail else ''}",
        )

    payload = response.json()
    return payload.get("artists") or []
