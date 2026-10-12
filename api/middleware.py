"""Request context: request id, access log and (optional) HTTP metrics.  Pure ASGI (no BaseHTTPMiddleware), so the
request-id context variable reliably reaches the route code and every log record emitted while handling the request.

* ``X-Request-ID`` from the client is reused only if it is short and made of safe characters (prevents log injection);
  otherwise a random id is generated. The id is returned in the ``X-Request-ID`` response header.
* The access log never contains the query string, headers or bodies (they may carry tokens or credentials).
* Metric labels use the matched ROUTE TEMPLATE (``/api/v1/predictions/{prediction_id}``), never the raw path, so the
  number of label values stays bounded no matter what clients request.
"""
from __future__ import annotations

import logging
import re
import time
import uuid
from typing import Any

from streaming.logging_setup import request_id_var

access_log = logging.getLogger("api.access")
_SAFE_ID = re.compile(r"^[A-Za-z0-9._-]{8,64}$")


class RequestContextMiddleware:
    def __init__(self, app: Any, metrics: Any | None = None) -> None:
        self.app, self.metrics = app, metrics

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        incoming = next((v.decode("latin-1") for k, v in scope["headers"] if k == b"x-request-id"), "")
        rid = incoming if _SAFE_ID.match(incoming) else uuid.uuid4().hex
        token = request_id_var.set(rid)
        scope.setdefault("state", {})["request_id"] = rid
        started, status = time.perf_counter(), 500

        async def send_wrapper(message: dict) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                message.setdefault("headers", []).append((b"x-request-id", rid.encode()))
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            elapsed = time.perf_counter() - started
            route = getattr(scope.get("route"), "path", None) or "unmatched"
            if self.metrics is not None:
                self.metrics.observe_request(scope["method"], route, status, elapsed)
            access_log.info("%s %s -> %d", scope["method"], scope["path"], status,
                            extra={"method": scope["method"], "path": scope["path"], "route": route, "status": status,
                                   "duration_ms": round(elapsed * 1000, 2)})
            request_id_var.reset(token)
