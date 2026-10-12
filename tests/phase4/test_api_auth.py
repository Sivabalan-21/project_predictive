"""Authentication, roles, docs switch, validation contract and request ids. Tests that need data use REAL PostgreSQL."""
import json
import logging
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from api.main import create_app
from auth.security import create_access_token
from core.settings import AppSettings
from services.ingestion_service import IngestionService
from tests.phase3.conftest import NORMAL, OVERSTRAIN, auth_headers, make_event
from tests.phase4.conftest import STRONG_PASSWORD

SECRET = "unit-test-secret-not-for-production-0123456789"
GET_ENDPOINTS = ["/api/v1/predictions", "/api/v1/predictions/1", "/api/v1/machines/M001/predictions",
                 "/api/v1/machines/M001/alerts", "/api/v1/alerts", "/api/v1/alerts/active", "/api/v1/auth/me"]


def dead_db():
    return sessionmaker(bind=create_engine("postgresql+psycopg://u:p@127.0.0.1:1/db", connect_args={"connect_timeout": 1}))


def app_without_db(**kw):
    return create_app(session_factory=dead_db(), kafka_check=None, **kw)


@pytest.fixture()
def client(session_factory):
    return TestClient(create_app(session_factory=session_factory, kafka_check=None))


@pytest.fixture()
def seeded(session_factory, predictor):
    ing = IngestionService(session_factory, predictor)
    ing.ingest(make_event("M001", "2026-10-08T10:30:00Z", NORMAL))
    ing.ingest(make_event("M001", "2026-10-08T10:31:00Z", OVERSTRAIN))
    ing.ingest(make_event("M002", "2026-10-08T10:32:00Z", OVERSTRAIN))


# ------------------------------------------------------------------------------------ public vs protected (no DB needed)
def test_health_and_ready_are_public():
    c = TestClient(app_without_db())
    assert c.get("/health").status_code == 200
    r = c.get("/ready")
    assert r.status_code == 503 and r.json()["database"] == "unavailable"        # 503 because the DB is down - NOT 401


@pytest.mark.parametrize("path", GET_ENDPOINTS)
def test_every_data_endpoint_requires_a_token(path):
    r = TestClient(app_without_db()).get(path)
    assert r.status_code == 401 and r.headers["www-authenticate"] == "Bearer"
    assert r.json() == {"error": "unauthorized", "detail": "authentication required"}


def test_resolve_requires_a_token():
    assert TestClient(app_without_db()).post("/api/v1/alerts/1/resolve").status_code == 401


@pytest.mark.parametrize("header", ["Bearer", "Bearer not-a-jwt", "Basic dXNlcjpwdw==", "bearer", "Token abc"])
def test_malformed_credentials_are_401(header):
    r = TestClient(app_without_db()).get("/api/v1/predictions", headers={"Authorization": header})
    assert r.status_code == 401


def test_expired_and_foreign_tokens_are_401():
    c = TestClient(app_without_db())
    old, _ = create_access_token(1, "admin", SECRET, now=datetime.now(timezone.utc) - timedelta(minutes=16))
    foreign, _ = create_access_token(1, "admin", "another-secret-another-secret-another-1")
    for tok in (old, foreign):
        r = c.get("/api/v1/predictions", headers={"Authorization": f"Bearer {tok}"})
        assert r.status_code == 401 and r.json()["detail"] == "invalid or expired token"


def test_viewer_cannot_resolve_alerts_403_before_touching_the_database():
    r = TestClient(app_without_db()).post("/api/v1/alerts/1/resolve", headers=auth_headers("viewer"))
    assert r.status_code == 403 and r.json()["error"] == "forbidden" and "operator" in r.json()["detail"]


@pytest.mark.parametrize("role", ["operator", "admin"])
def test_operator_and_admin_pass_the_role_check_for_resolve(role):
    r = TestClient(app_without_db()).post("/api/v1/alerts/1/resolve", headers=auth_headers(role))
    assert r.status_code == 503             # got past authorisation; only the (deliberately dead) database failed


def test_app_refuses_to_start_without_a_jwt_secret(monkeypatch):
    monkeypatch.delenv("JWT_SECRET", raising=False)
    from db.config import ConfigurationError
    with pytest.raises(ConfigurationError, match="JWT_SECRET"):
        create_app(session_factory=dead_db(), kafka_check=None)


def test_app_refuses_placeholder_secret_in_production(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("JWT_SECRET", "change-me-change-me-change-me-change-me")
    from db.config import ConfigurationError
    with pytest.raises(ConfigurationError, match="placeholder"):
        create_app(session_factory=dead_db(), kafka_check=None)


# ------------------------------------------------------------------------------------ docs switch
@pytest.mark.parametrize("enabled", [True, False])
def test_enable_docs_switch(enabled):
    c = TestClient(app_without_db(settings=AppSettings(env="development", enable_docs=enabled, jwt_secret=SECRET)))
    codes = {p: c.get(p).status_code for p in ("/docs", "/redoc", "/openapi.json")}
    assert set(codes.values()) == ({200} if enabled else {404})
    assert c.get("/health").status_code == 200


def test_docs_are_off_by_default_in_production_and_on_in_development(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("JWT_SECRET", "k3J9xQ2mVb7LpR4tYw8ZnA5cHd6FsEu1GoIq0XyTjBvC")
    monkeypatch.delenv("ENABLE_DOCS", raising=False)
    assert TestClient(app_without_db()).get("/docs").status_code == 404
    monkeypatch.setenv("ENABLE_DOCS", "true")
    assert TestClient(app_without_db()).get("/docs").status_code == 200
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.delenv("ENABLE_DOCS")
    assert TestClient(app_without_db()).get("/docs").status_code == 200


def test_openapi_documents_the_bearer_scheme_and_the_422_shape():
    spec = TestClient(app_without_db()).get("/openapi.json").json()
    assert "HTTPBearer" in spec["components"]["securitySchemes"]
    assert spec["paths"]["/api/v1/predictions"]["get"]["security"]
    assert "security" not in spec["paths"]["/health"]["get"]
    assert spec["paths"]["/api/v1/predictions"]["get"]["responses"]["422"]["content"]["application/json"]["schema"]["$ref"].endswith("ErrorBody")


# ------------------------------------------------------------------------------------ validation contract (no DB needed)
def test_validation_errors_are_422_with_the_standard_shape_and_never_echo_input():
    c = TestClient(app_without_db(), headers=auth_headers("viewer"))
    r = c.get("/api/v1/predictions?page=0&page_size=abc-secret-looking-value")
    assert r.status_code == 422
    body = r.json()
    assert body["error"] == "validation_error" and body["detail"] == "request validation failed"
    fields = {e["field"] for e in body["errors"]}
    assert fields == {"query.page", "query.page_size"}
    assert all(set(e) == {"field", "message", "type"} for e in body["errors"])
    assert "abc-secret-looking-value" not in r.text and "input" not in r.text


@pytest.mark.parametrize("machine_id", ["bad%20id", "-leading-dash", "_x", "a" * 65, "id;drop", "m.1", "%E2%82%AC"])
@pytest.mark.parametrize("suffix", ["predictions", "alerts"])
def test_machine_id_is_validated_with_the_existing_regex(machine_id, suffix):
    r = TestClient(app_without_db(), headers=auth_headers("viewer")).get(f"/api/v1/machines/{machine_id}/{suffix}")
    assert r.status_code == 422 and r.json()["errors"][0]["field"] == "path.machine_id"


def test_encoded_slash_in_machine_id_never_reaches_a_handler():
    # "m%2Fid" decodes to "m/id": an extra path segment, so no route matches (404 from routing, before validation).
    r = TestClient(app_without_db(), headers=auth_headers("viewer")).get("/api/v1/machines/m%2Fid/predictions")
    assert r.status_code == 404 and r.json()["error"] == "not_found"


@pytest.mark.parametrize("machine_id", ["M001", "a", "line-3_machine", "A" * 64, "9"])
def test_valid_machine_ids_get_past_validation(machine_id):
    r = TestClient(app_without_db(), headers=auth_headers("viewer")).get(f"/api/v1/machines/{machine_id}/predictions")
    assert r.status_code == 503             # passed validation + auth; only the dead database stopped it


def test_machine_id_pattern_is_the_event_schema_pattern():
    from api.routes.machines import MACHINE_ID_PATTERN as api_pattern
    from streaming.schemas import MACHINE_ID_PATTERN
    assert api_pattern is MACHINE_ID_PATTERN


def test_unknown_path_and_wrong_method_use_the_standard_error_shape():
    c = TestClient(app_without_db(), headers=auth_headers("viewer"))
    nf = c.get("/api/v1/nothing-here")
    assert nf.status_code == 404 and set(nf.json()) == {"error", "detail"} and nf.json()["error"] == "not_found"
    mna = c.delete("/health")
    assert mna.status_code == 405 and mna.json()["error"] == "method_not_allowed"


def test_unhandled_exception_returns_a_generic_500_without_internals():
    app = app_without_db()

    @app.get("/boom")
    def boom():
        raise RuntimeError("secret internal detail password=hunter2hunter2")
    r = TestClient(app, raise_server_exceptions=False).get("/boom")
    assert r.status_code == 500 and r.json() == {"error": "internal_error", "detail": "internal server error"}
    assert "hunter2" not in r.text and r.headers.get("x-request-id")


# ------------------------------------------------------------------------------------ request ids
def test_request_id_is_generated_and_returned():
    r = TestClient(app_without_db()).get("/health")
    rid = r.headers["x-request-id"]
    assert len(rid) == 32 and rid.isalnum()
    assert TestClient(app_without_db()).get("/health").headers["x-request-id"] != rid


def test_safe_incoming_request_id_is_propagated_unsafe_one_is_replaced():
    c = TestClient(app_without_db())
    assert c.get("/health", headers={"X-Request-ID": "trace-abc.123_XYZ"}).headers["x-request-id"] == "trace-abc.123_XYZ"
    for bad in ("short", "has space in it 1234", "x" * 200, "inject\"}{newline"):
        assert c.get("/health", headers={"X-Request-ID": bad}).headers["x-request-id"] != bad


def test_request_id_reaches_the_json_access_log_without_query_string_or_token(capsys):
    app = app_without_db(settings=AppSettings(env="test", enable_docs=True, jwt_secret=SECRET, log_format="json"))
    c = TestClient(app, headers=auth_headers("viewer"))
    r = c.get("/api/v1/predictions/12345?token=should-not-be-logged", headers={"X-Request-ID": "req-correlate-001"})
    lines = [json.loads(l) for l in capsys.readouterr().err.splitlines() if l.startswith("{")]
    access = [l for l in lines if l["logger"] == "api.access"]
    assert access and access[-1]["request_id"] == "req-correlate-001" and access[-1]["status"] == r.status_code
    assert access[-1]["route"] == "/api/v1/predictions/{prediction_id}" and access[-1]["method"] == "GET"
    dump = json.dumps(lines)
    assert "should-not-be-logged" not in dump and "Bearer" not in dump and "eyJ" not in dump


# ------------------------------------------------------------------------------------ real PostgreSQL
@pytest.mark.integration
@pytest.mark.postgres
class TestWithPostgres:
    def test_login_success_returns_a_15_minute_token_that_works(self, client, make_user):
        username, password = make_user("viewer")
        r = client.post("/api/v1/auth/login", json={"username": username, "password": password})
        assert r.status_code == 200
        body = r.json()
        assert body["token_type"] == "bearer" and body["expires_in"] == 900 and body["access_token"].count(".") == 2
        me = client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {body['access_token']}"})
        assert me.status_code == 200 and me.json()["data"]["role"] == "viewer"

    def test_username_is_case_insensitive(self, client, make_user):
        username, password = make_user("viewer")
        assert client.post("/api/v1/auth/login", json={"username": username.upper(), "password": password}).status_code == 200

    def test_failures_are_indistinguishable(self, client, make_user):
        username, password = make_user("operator")
        inactive, inactive_pw = make_user("operator", active=False)
        wrong_pw = client.post("/api/v1/auth/login", json={"username": username, "password": "wrong-password-123"})
        unknown = client.post("/api/v1/auth/login", json={"username": "no-such-user", "password": password})
        disabled = client.post("/api/v1/auth/login", json={"username": inactive, "password": inactive_pw})
        for r in (wrong_pw, unknown, disabled):
            assert r.status_code == 401 and r.headers["www-authenticate"] == "Bearer"
        assert wrong_pw.json() == unknown.json() == disabled.json() == {"error": "unauthorized", "detail": "invalid username or password"}

    def test_login_failure_does_not_log_the_attempted_username_or_password(self, client, caplog):
        caplog.set_level(logging.INFO)
        client.post("/api/v1/auth/login", json={"username": "typed-my-password-here", "password": "hunter2hunter2"})
        assert "typed-my-password-here" not in caplog.text and "hunter2hunter2" not in caplog.text
        assert "login failed" in caplog.text

    @pytest.mark.parametrize("payload", [{}, {"username": "x"}, {"password": "x"}, {"username": "", "password": "x"},
                                         {"username": "x" * 65, "password": "x"}, {"username": "x", "password": "p" * 129}])
    def test_login_body_validation_is_422(self, client, payload):
        assert client.post("/api/v1/auth/login", json=payload).status_code == 422

    def test_roles_end_to_end(self, client, make_user, seeded):
        def token(role):
            u, p = make_user(role)
            return {"Authorization": "Bearer " + client.post("/api/v1/auth/login", json={"username": u, "password": p}).json()["access_token"]}
        viewer, operator, admin = token("viewer"), token("operator"), token("admin")
        for headers in (viewer, operator, admin):                                    # every role can read
            assert client.get("/api/v1/predictions", headers=headers).status_code == 200
            assert client.get("/api/v1/alerts/active", headers=headers).status_code == 200
        alert_id = client.get("/api/v1/alerts/active", headers=viewer).json()["data"][0]["id"]
        denied = client.post(f"/api/v1/alerts/{alert_id}/resolve", headers=viewer)
        assert denied.status_code == 403
        assert client.get("/api/v1/alerts/active", headers=viewer).json()["pagination"]["total"] == 2   # untouched
        ok = client.post(f"/api/v1/alerts/{alert_id}/resolve", headers=operator)
        assert ok.status_code == 200 and ok.json()["data"]["status"] == "RESOLVED"
        other = client.get("/api/v1/alerts/active", headers=admin).json()["data"][0]["id"]
        assert client.post(f"/api/v1/alerts/{other}/resolve", headers=admin).status_code == 200
        assert client.post(f"/api/v1/alerts/{other}/resolve", headers=admin).status_code == 409

    def test_unknown_machine_is_still_404_and_validation_still_422(self, client, seeded):
        h = auth_headers("viewer")
        assert client.get("/api/v1/machines/UNKNOWN/predictions", headers=h).status_code == 404
        assert client.get("/api/v1/machines/bad%20id/predictions", headers=h).status_code == 422
        assert client.get("/api/v1/machines/M001/predictions", headers=h).json()["pagination"]["total"] == 2

    def test_ready_ok_without_a_token(self, client):
        assert client.get("/ready").json() == {"status": "ready", "database": "ok", "kafka": "skipped"}
