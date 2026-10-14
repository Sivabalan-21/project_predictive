"""Focused tests for ui/api_client.py and the Live Monitoring page.

A tiny local HTTP stub speaks the backend's documented contract (api/routes, api/schemas): real sockets, real urllib,
real timeouts. Contract against the REAL FastAPI + PostgreSQL lives in tests/phase4/test_ui_api_contract.py.
"""
from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from ui.api_client import ApiClient, ApiError

PRED = {"id": 7, "sensor_reading_id": 7, "machine_id": "M001", "timestamp": "2026-10-08T10:31:00Z",
        "prediction": "FAILURE", "risk_score": 0.93, "rf_probability": 0.9, "xgb_probability": 0.96,
        "anomaly_score": -0.1, "fault_type": "OSF", "model_version": "v1", "created_at": "2026-10-08T10:31:01Z"}
ALERT = {"id": 3, "machine_id": "M001", "sensor_reading_id": 7, "prediction_id": 7, "fault_type": "OSF",
         "risk_score": 0.93, "status": "ACTIVE", "created_at": "2026-10-08T10:31:01Z", "resolved_at": None}
PAGE = {"page": 1, "page_size": 50, "total": 1}
TOKEN = "tok-123"


class Stub:
    def __init__(self):
        self.routes: dict[tuple[str, str], tuple[int, object, float]] = {}
        self.requests: list[dict] = []
        stub = self

        class H(BaseHTTPRequestHandler):
            def _do(self):
                path = self.path.split("?")[0]
                length = int(self.headers.get("Content-Length") or 0)
                stub.requests.append({"method": self.command, "path": self.path, "auth": self.headers.get("Authorization"),
                                      "body": self.rfile.read(length) if length else b""})
                status, body, delay = stub.routes.get((self.command, path), (404, {"error": "not_found", "detail": "nope"}, 0))
                time.sleep(delay)
                raw = body if isinstance(body, bytes) else json.dumps(body).encode()
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                except OSError:
                    pass
            do_GET = do_POST = _do

            def log_message(self, *a):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture()
def stub():
    s = Stub()
    s.routes[("POST", "/api/v1/auth/login")] = (200, {"access_token": TOKEN, "token_type": "bearer", "expires_in": 900}, 0)
    s.routes[("GET", "/health")] = (200, {"status": "ok"}, 0)
    s.routes[("GET", "/ready")] = (200, {"status": "ready", "database": "ok", "kafka": "ok"}, 0)
    s.routes[("GET", "/api/v1/predictions")] = (200, {"data": [PRED], "pagination": PAGE}, 0)
    s.routes[("GET", "/api/v1/alerts")] = (200, {"data": [ALERT], "pagination": PAGE}, 0)
    yield s
    s.close()


def authed(stub, **kw) -> ApiClient:
    c = ApiClient(stub.url, **kw)
    c.login("u", "p")
    return c


# ---------------------------------------------------------------- authentication / token handling
def test_login_stores_token_and_sends_json_credentials(stub):
    c = authed(stub)
    assert c.is_authenticated and json.loads(stub.requests[0]["body"]) == {"username": "u", "password": "p"}
    assert TOKEN not in repr(c)


def test_bearer_header_sent_on_data_calls_but_not_public_ones(stub):
    c = authed(stub)
    c.health(); c.ready(); c.predictions()
    by_path = {r["path"].split("?")[0]: r["auth"] for r in stub.requests}
    assert by_path["/health"] is None and by_path["/ready"] is None
    assert by_path["/api/v1/predictions"] == f"Bearer {TOKEN}"


def test_login_failure_401_and_429_are_distinct(stub):
    stub.routes[("POST", "/api/v1/auth/login")] = (401, {"error": "unauthorized", "detail": "invalid username or password"}, 0)
    with pytest.raises(ApiError) as e:
        ApiClient(stub.url).login("u", "bad")
    assert e.value.kind == "unauthorized" and "invalid username" in e.value.detail
    stub.routes[("POST", "/api/v1/auth/login")] = (429, {"error": "http_error", "detail": "too many login attempts"}, 0)
    with pytest.raises(ApiError) as e:
        ApiClient(stub.url).login("u", "bad")
    assert e.value.kind == "rate_limited"


def test_login_response_missing_fields_is_bad_response(stub):
    stub.routes[("POST", "/api/v1/auth/login")] = (200, {"token_type": "bearer"}, 0)
    with pytest.raises(ApiError) as e:
        ApiClient(stub.url).login("u", "p")
    assert e.value.kind == "bad_response"


def test_data_call_without_token_fails_locally_without_network(stub):
    with pytest.raises(ApiError) as e:
        ApiClient(stub.url).predictions()
    assert e.value.kind == "unauthorized" and stub.requests == []


def test_expired_token_is_treated_as_signed_out(stub):
    c = authed(stub)
    c.token_expires_at = time.time() - 1
    assert not c.is_authenticated
    with pytest.raises(ApiError) as e:
        c.alerts()
    assert e.value.kind == "unauthorized"


def test_server_side_401_and_403_mapped(stub):
    c = authed(stub)
    stub.routes[("GET", "/api/v1/predictions")] = (401, {"error": "unauthorized", "detail": "invalid or expired token"}, 0)
    with pytest.raises(ApiError) as e:
        c.predictions()
    assert (e.value.kind, e.value.status) == ("unauthorized", 401)
    stub.routes[("GET", "/api/v1/predictions")] = (403, {"error": "forbidden", "detail": "requires role 'viewer' or higher"}, 0)
    with pytest.raises(ApiError) as e:
        c.predictions()
    assert (e.value.kind, e.value.status) == ("forbidden", 403)


def test_logout_clears_token(stub):
    c = authed(stub)
    c.logout()
    assert not c.is_authenticated


# ---------------------------------------------------------------- response parsing
def test_predictions_parsed_into_typed_rows(stub):
    rows, page = authed(stub).predictions(page=1, page_size=50)
    assert rows[0].machine_id == "M001" and rows[0].fault_type == "OSF" and rows[0].timestamp.year == 2026
    assert (page.total, page.page_size) == (1, 50)


def test_alerts_parsed_and_status_filter_sent(stub):
    rows, _ = authed(stub).alerts(status="ACTIVE")
    assert rows[0].status == "ACTIVE" and rows[0].resolved_at is None
    assert "status=ACTIVE" in stub.requests[-1]["path"]


def test_null_fault_type_accepted(stub):
    stub.routes[("GET", "/api/v1/predictions")] = (200, {"data": [{**PRED, "prediction": "NORMAL", "fault_type": None}], "pagination": PAGE}, 0)
    rows, _ = authed(stub).predictions()
    assert rows[0].fault_type is None


def test_machine_filter_uses_machine_route_and_encodes_id(stub):
    stub.routes[("GET", "/api/v1/machines/M001/predictions")] = (200, {"data": [PRED], "pagination": PAGE}, 0)
    authed(stub).predictions(machine_id="M001")
    assert stub.requests[-1]["path"].startswith("/api/v1/machines/M001/predictions")
    with pytest.raises(ApiError):
        authed(stub).predictions(machine_id="../x")
    assert "/api/v1/machines/..%2Fx/predictions" in stub.requests[-1]["path"]


def test_page_size_clamped_to_backend_limit(stub):
    authed(stub).predictions(page_size=5000)
    assert "page_size=200" in stub.requests[-1]["path"]


@pytest.mark.parametrize("body", [{"data": [{"id": 1}], "pagination": PAGE}, {"data": []}, [1, 2], {"data": [PRED], "pagination": {}}])
def test_malformed_page_is_bad_response(stub, body):
    stub.routes[("GET", "/api/v1/predictions")] = (200, body, 0)
    with pytest.raises(ApiError) as e:
        authed(stub).predictions()
    assert e.value.kind == "bad_response"


def test_non_json_success_body_is_bad_response(stub):
    stub.routes[("GET", "/api/v1/predictions")] = (200, b"<html>proxy</html>", 0)
    with pytest.raises(ApiError) as e:
        authed(stub).predictions()
    assert e.value.kind == "bad_response"


# ---------------------------------------------------------------- error handling / availability
def test_404_422_and_5xx_mapped_with_detail(stub):
    c = authed(stub)
    for status, kind in ((404, "not_found"), (422, "validation"), (503, "server"), (500, "server")):
        stub.routes[("GET", "/api/v1/alerts")] = (status, {"error": "x", "detail": "boom"}, 0)
        with pytest.raises(ApiError) as e:
            c.alerts()
        assert (e.value.kind, e.value.detail, e.value.status) == (kind, "boom", status)


def test_5xx_with_non_json_body_has_generic_detail(stub):
    stub.routes[("GET", "/api/v1/alerts")] = (502, b"<html>bad gateway</html>", 0)
    with pytest.raises(ApiError) as e:
        authed(stub).alerts()
    assert e.value.kind == "server" and e.value.detail == "HTTP 502"


def test_ready_503_is_data_not_an_exception(stub):
    stub.routes[("GET", "/ready")] = (503, {"status": "not_ready", "database": "unavailable", "kafka": "ok"}, 0)
    r = ApiClient(stub.url).ready()
    assert (r.status, r.database) == ("not_ready", "unavailable")



def test_unreachable_api_is_unavailable(monkeypatch):
    import urllib.error
    from unittest.mock import patch

    def raise_connection_error(*args, **kwargs):
        raise urllib.error.URLError(ConnectionRefusedError("connection refused"))

    with patch("urllib.request.urlopen", side_effect=raise_connection_error):
        with pytest.raises(ApiError) as e:
            ApiClient("http://127.0.0.1:1", timeout=1).health()

    assert e.value.kind == "unavailable"



def test_slow_api_times_out(stub):
    stub.routes[("GET", "/health")] = (200, {"status": "ok"}, 1.5)
    t0 = time.time()
    with pytest.raises(ApiError) as e:
        ApiClient(stub.url, timeout=0.3).health()
    assert e.value.kind == "timeout" and time.time() - t0 < 1.2


def test_base_url_and_timeout_from_environment(monkeypatch):
    monkeypatch.setenv("API_BASE_URL", "http://example.test:9/")
    monkeypatch.setenv("API_TIMEOUT_SECONDS", "2.5")
    c = ApiClient()
    assert (c.base_url, c.timeout) == ("http://example.test:9", 2.5)


# ---------------------------------------------------------------- Streamlit page
PAGE_FILE = str(Path(__file__).resolve().parent.parent / "app" / "pages" / "5_live_monitoring.py")


def run_page(url: str, client: ApiClient | None = None) -> AppTest:
    at = AppTest.from_file(PAGE_FILE, default_timeout=30)
    at.session_state["api_client"] = client or ApiClient(url, timeout=2)
    return at.run()


def test_page_shows_connection_error_when_api_down():
    at = run_page("http://127.0.0.1:1")
    assert not at.exception
    assert any("API unreachable" in e.value for e in at.error)
    assert not at.dataframe


def test_page_requires_sign_in_then_shows_backend_data(stub):
    at = run_page(stub.url)
    assert not at.exception and any("API reachable" in s.value for s in at.success)
    assert at.text_input and not at.dataframe                      # login form, no data before auth
    at = run_page(stub.url, authed(stub))
    assert not at.exception
    assert [m.value for m in at.metric][:3] == ["1", "1", "FAILURE"]
    assert len(at.dataframe) == 2


def test_page_warns_when_backend_not_ready(stub):
    stub.routes[("GET", "/ready")] = (503, {"status": "not_ready", "database": "unavailable", "kafka": "ok"}, 0)
    at = run_page(stub.url, authed(stub))
    assert any("NOT ready" in w.value for w in at.warning)


def test_page_expired_session_returns_to_login(stub):
    stub.routes[("GET", "/api/v1/predictions")] = (401, {"error": "unauthorized", "detail": "invalid or expired token"}, 0)
    c = authed(stub)
    at = run_page(stub.url, c)
    assert any("Session expired" in w.value for w in at.warning) and not c.is_authenticated


def test_page_shows_empty_states(stub):
    empty = {"data": [], "pagination": {"page": 1, "page_size": 50, "total": 0}}
    stub.routes[("GET", "/api/v1/predictions")] = (200, empty, 0)
    stub.routes[("GET", "/api/v1/alerts")] = (200, empty, 0)
    at = run_page(stub.url, authed(stub))
    assert not at.exception and not at.dataframe and len(at.info) >= 2
