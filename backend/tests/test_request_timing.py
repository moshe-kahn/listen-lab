from __future__ import annotations

import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.request_timing import RequestTimingMiddleware


class RequestTimingMiddlewareTests(unittest.TestCase):
    def test_logs_endpoint_timing_and_adds_response_headers(self) -> None:
        app = FastAPI()
        app.add_middleware(RequestTimingMiddleware)

        @app.get("/probe")
        def probe() -> dict[str, bool]:
            return {"ok": True}

        with self.assertLogs("listenlabs.http", level="INFO") as captured:
            response = TestClient(app).get("/probe?mode=shell&secret=hidden")

        self.assertEqual(200, response.status_code)
        self.assertIn("app;dur=", response.headers["server-timing"])
        self.assertRegex(response.headers["x-request-duration-ms"], r"^\d+\.\d$")
        message = "\n".join(captured.output)
        self.assertIn("event=http_request_timing", message)
        self.assertIn("method=GET", message)
        self.assertIn("path=/probe", message)
        self.assertIn("status=200", message)
        self.assertIn("mode=shell", message)
        self.assertNotIn("hidden", message)


if __name__ == "__main__":
    unittest.main()
