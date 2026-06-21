from __future__ import annotations

import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response


logger = logging.getLogger("listenlabs.http")
SLOW_REQUEST_MS = 1_000.0
_LOGGED_QUERY_PARAMETERS = ("mode", "analysis_mode", "recent_range", "local_only", "force_spotify")


def _request_context(request: Request) -> str:
    values = [
        f"{name}={request.query_params[name]}"
        for name in _LOGGED_QUERY_PARAMETERS
        if name in request.query_params
    ]
    return " ".join(values) if values else "none"


class RequestTimingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        started_at = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            elapsed_ms = (time.perf_counter() - started_at) * 1_000
            if "response" in locals():
                response.headers["Server-Timing"] = f'app;dur={elapsed_ms:.1f};desc="ListenLab API"'
                response.headers["X-Request-Duration-Ms"] = f"{elapsed_ms:.1f}"
            log_method = logger.debug if request.method == "OPTIONS" or request.url.path == "/me/progress" else (
                logger.warning if elapsed_ms >= SLOW_REQUEST_MS else logger.info
            )
            log_method(
                "event=http_request_timing method=%s path=%s status=%s elapsed_ms=%.1f context=%s slow=%s",
                request.method,
                request.url.path,
                status_code,
                elapsed_ms,
                _request_context(request),
                str(elapsed_ms >= SLOW_REQUEST_MS).lower(),
            )
