"""Small HTTP client the Streamlit UI uses to talk to the FastAPI backend (PostgreSQL-backed data only).

Contract (verified against api/): POST /api/v1/auth/login -> {access_token, token_type, expires_in};
every other /api/v1 GET needs ``Authorization: Bearer <token>`` (viewer role is enough); list endpoints return
{data: [...], pagination: {page, page_size, total}}; errors return {error, detail}; /health and /ready are public
(/ready answers 503 with a normal ReadyOut body when a dependency is down).

stdlib only (no new dependency). The token lives in the client instance and is never logged or put in repr().
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Any

DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_TIMEOUT = 5.0
MAX_PAGE_SIZE = 200          # api.dependencies.PageParams: le=200


class ApiError(Exception):
    """kind: unavailable | timeout | unauthorized | forbidden | not_found | rate_limited | validation | server | bad_response"""

    def __init__(self, kind: str, detail: str, status: int | None = None):
        super().__init__(f"{kind}: {detail}")
        self.kind, self.detail, self.status = kind, detail, status


@dataclass(frozen=True)
class Prediction:
    id: int
    machine_id: str
    timestamp: datetime
    prediction: str
    risk_score: float
    rf_probability: float
    xgb_probability: float
    anomaly_score: float
    fault_type: str | None
    model_version: str


@dataclass(frozen=True)
class Alert:
    id: int
    machine_id: str
    prediction_id: int
    fault_type: str
    risk_score: float
    status: str
    created_at: datetime
    resolved_at: datetime | None


@dataclass(frozen=True)
class Pagination:
    page: int
    page_size: int
    total: int


@dataclass(frozen=True)
class Readiness:
    status: str
    database: str
    kafka: str


def _dt(value: Any) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def parse_prediction(d: dict) -> Prediction:
    return Prediction(id=int(d["id"]), machine_id=str(d["machine_id"]), timestamp=_dt(d["timestamp"]),
                      prediction=str(d["prediction"]), risk_score=float(d["risk_score"]),
                      rf_probability=float(d["rf_probability"]), xgb_probability=float(d["xgb_probability"]),
                      anomaly_score=float(d["anomaly_score"]), fault_type=d.get("fault_type"),
                      model_version=str(d["model_version"]))


def parse_alert(d: dict) -> Alert:
    resolved = d.get("resolved_at")
    return Alert(id=int(d["id"]), machine_id=str(d["machine_id"]), prediction_id=int(d["prediction_id"]),
                 fault_type=str(d["fault_type"]), risk_score=float(d["risk_score"]), status=str(d["status"]),
                 created_at=_dt(d["created_at"]), resolved_at=_dt(resolved) if resolved else None)


def parse_page(body: Any, item_parser) -> tuple[list, Pagination]:
    try:
        items = [item_parser(row) for row in body["data"]]
        p = body["pagination"]
        return items, Pagination(int(p["page"]), int(p["page_size"]), int(p["total"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise ApiError("bad_response", f"unexpected response shape ({exc.__class__.__name__}: {exc})") from None


class ApiClient:
    def __init__(self, base_url: str | None = None, timeout: float | None = None, token: str | None = None,
                 token_expires_at: float | None = None):
        self.base_url = (base_url or os.environ.get("API_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.timeout = timeout if timeout is not None else float(os.environ.get("API_TIMEOUT_SECONDS", DEFAULT_TIMEOUT))
        self.token, self.token_expires_at = token, token_expires_at

    def __repr__(self) -> str:                                    # never leak the token
        return f"ApiClient(base_url={self.base_url!r}, authenticated={self.is_authenticated})"

    @property
    def is_authenticated(self) -> bool:
        if not self.token:
            return False
        return self.token_expires_at is None or time.time() < self.token_expires_at

    # ------------------------------------------------------------------ transport
    def _request(self, method: str, path: str, *, params: dict | None = None, body: dict | None = None,
                 auth: bool = True, accept_statuses: tuple[int, ...] = ()) -> tuple[int, Any]:
        url = self.base_url + path + (("?" + urllib.parse.urlencode({k: v for k, v in params.items() if v is not None}))
                                      if params else "")
        headers = {"Accept": "application/json"}
        data = None
        if body is not None:
            data, headers["Content-Type"] = json.dumps(body).encode(), "application/json"
        if auth:
            if not self.is_authenticated:
                raise ApiError("unauthorized", "not signed in or session expired", 401)
            headers["Authorization"] = f"Bearer {self.token}"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:      # noqa: S310 - http(s) base URL from config
                status, raw = resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            status, raw = exc.code, exc.read()
            exc.close()
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, TimeoutError):
                raise ApiError("timeout", f"no response from {self.base_url} within {self.timeout:g}s") from None
            raise ApiError("unavailable", f"cannot reach {self.base_url} ({exc.reason})") from None
        except TimeoutError:
            raise ApiError("timeout", f"no response from {self.base_url} within {self.timeout:g}s") from None
        except OSError as exc:                                                    # reset / refused mid-read
            raise ApiError("unavailable", f"connection to {self.base_url} failed ({exc.__class__.__name__})") from None
        try:
            payload = json.loads(raw) if raw else None
        except ValueError:
            payload = None
        if status in accept_statuses or 200 <= status < 300:
            if payload is None:
                raise ApiError("bad_response", f"HTTP {status} with a non-JSON body", status)
            return status, payload
        raise self._error_for(status, payload)

    @staticmethod
    def _error_for(status: int, payload: Any) -> ApiError:
        detail = payload.get("detail") if isinstance(payload, dict) else None
        detail = detail if isinstance(detail, str) and detail else f"HTTP {status}"
        kind = {401: "unauthorized", 403: "forbidden", 404: "not_found", 422: "validation", 429: "rate_limited"}.get(
            status, "server" if status >= 500 else "bad_response")
        return ApiError(kind, detail, status)

    # ------------------------------------------------------------------ endpoints
    def login(self, username: str, password: str) -> None:
        _, body = self._request("POST", "/api/v1/auth/login", body={"username": username, "password": password}, auth=False)
        try:
            self.token, self.token_expires_at = str(body["access_token"]), time.time() + int(body["expires_in"])
        except (KeyError, TypeError, ValueError):
            raise ApiError("bad_response", "login response missing access_token/expires_in") from None

    def logout(self) -> None:
        self.token = self.token_expires_at = None

    def health(self) -> bool:
        _, body = self._request("GET", "/health", auth=False)
        return isinstance(body, dict) and body.get("status") == "ok"

    def ready(self) -> Readiness:
        """503 is a normal answer here (database/kafka down): returned as data, not raised."""
        _, body = self._request("GET", "/ready", auth=False, accept_statuses=(503,))
        try:
            return Readiness(str(body["status"]), str(body["database"]), str(body["kafka"]))
        except (KeyError, TypeError):
            raise ApiError("bad_response", "unexpected /ready response shape") from None

    def predictions(self, page: int = 1, page_size: int = 50, machine_id: str | None = None):
        path = f"/api/v1/machines/{urllib.parse.quote(machine_id, safe='')}/predictions" if machine_id else "/api/v1/predictions"
        _, body = self._request("GET", path, params={"page": page, "page_size": min(page_size, MAX_PAGE_SIZE)})
        return parse_page(body, parse_prediction)

    def alerts(self, status: str | None = None, page: int = 1, page_size: int = 50):
        _, body = self._request("GET", "/api/v1/alerts", params={"status": status, "page": page,
                                                                   "page_size": min(page_size, MAX_PAGE_SIZE)})
        return parse_page(body, parse_alert)
