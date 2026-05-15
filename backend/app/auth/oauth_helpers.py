from __future__ import annotations

import base64
import hashlib
from urllib.parse import urlencode

from backend.app.config import get_settings

settings = get_settings()


def _is_configured() -> bool:
    return bool(
        settings.spotify_client_id
        and settings.listenlab_token_encryption_key
        and settings.spotify_redirect_uri
        and settings.session_secret
    )


def _callback_redirect_url(
    reason: str,
    detail: str | None = None,
    extra: dict[str, str] | None = None,
) -> str:
    query = {"status": reason}
    if detail:
        query["detail"] = detail
    if extra:
        query.update(extra)
    return f"{settings.frontend_url}/auth/callback?{urlencode(query)}"


def _pkce_code_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")
