"""FastAPI tests. Tests using ``client`` run against REAL PostgreSQL; the *unavailable* tests need no database."""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from api.main import create_app
from services.ingestion_service import IngestionService
from tests.phase3.conftest import NORMAL, OVERSTRAIN, make_event

pg = [pytest.mark.integration, pytest.mark.postgres]


@pytest.fixture()
def client(session_factory):
    return TestClient(create_app(session_factory=session_factory, kafka_check=None))


@pytest.fixture()
def seeded(session_factory, predictor):
    ing = IngestionService(session_factory, predictor)
    for i in range(5):
        ing.ingest(make_event("M001", f"2026-10-08T10:30:0{i}Z", NORMAL))
    ing.ingest(make_event("M001", "2026-10-08T10:31:00Z", OVERSTRAIN))
    ing.ingest(make_event("M002", "2026-10-08T10:32:00Z", OVERSTRAIN))


@pytest.mark.parametrize("path", ["/health"])
def test_health_needs_no_dependencies(path):
    bad = sessionmaker(bind=create_engine("postgresql+psycopg://u:p@127.0.0.1:1/db"))
    assert TestClient(create_app(session_factory=bad, kafka_check=None)).get(path).json() == {"status": "ok"}


def test_ready_fails_with_503_when_database_unavailable():
    bad = sessionmaker(bind=create_engine("postgresql+psycopg://u:p@127.0.0.1:1/db", connect_args={"connect_timeout": 1}))
    r = TestClient(create_app(session_factory=bad, kafka_check=None)).get("/ready")
    assert r.status_code == 503 and r.json()["database"] == "unavailable" and r.json()["status"] == "not_ready"


def test_data_endpoints_return_503_not_500_when_database_unavailable():
    bad = sessionmaker(bind=create_engine("postgresql+psycopg://u:p@127.0.0.1:1/db", connect_args={"connect_timeout": 1}))
    r = TestClient(create_app(session_factory=bad, kafka_check=None)).get("/api/v1/predictions")
    assert r.status_code == 503 and r.json()["error"] == "service_unavailable"


@pytest.mark.integration
@pytest.mark.postgres
class TestWithPostgres:
    def test_ready_ok(self, client):
        assert client.get("/ready").json() == {"status": "ready", "database": "ok", "kafka": "skipped"}

    def test_ready_reports_kafka_down(self, session_factory):
        def down(): raise RuntimeError("no broker")
        r = TestClient(create_app(session_factory=session_factory, kafka_check=down)).get("/ready")
        assert r.status_code == 503 and r.json() == {"status": "not_ready", "database": "ok", "kafka": "unavailable"}

    def test_ready_kafka_ok(self, session_factory):
        r = TestClient(create_app(session_factory=session_factory, kafka_check=lambda: True)).get("/ready")
        assert r.status_code == 200 and r.json()["kafka"] == "ok"

    def test_predictions_list_and_pagination(self, client, seeded):
        r = client.get("/api/v1/predictions?page=1&page_size=4").json()
        assert r["pagination"] == {"page": 1, "page_size": 4, "total": 7} and len(r["data"]) == 4
        assert len(client.get("/api/v1/predictions?page=2&page_size=4").json()["data"]) == 3
        assert client.get("/api/v1/predictions?page=9&page_size=4").json()["data"] == []
        row = r["data"][0]
        assert {"risk_score", "rf_probability", "xgb_probability", "anomaly_score", "fault_type", "model_version"} <= set(row)
        assert "confidence" not in row and "calibrated" not in str(row)

    def test_prediction_by_id_and_404(self, client, seeded):
        first = client.get("/api/v1/predictions").json()["data"][0]
        assert client.get(f"/api/v1/predictions/{first['id']}").json()["data"]["id"] == first["id"]
        r = client.get("/api/v1/predictions/999999")
        assert r.status_code == 404 and r.json()["error"] == "not_found"

    def test_machine_predictions(self, client, seeded):
        r = client.get("/api/v1/machines/M001/predictions").json()
        assert r["pagination"]["total"] == 6 and {p["machine_id"] for p in r["data"]} == {"M001"}
        assert client.get("/api/v1/machines/UNKNOWN/predictions").status_code == 404

    def test_alerts_endpoints(self, client, seeded):
        assert client.get("/api/v1/alerts").json()["pagination"]["total"] == 2
        active = client.get("/api/v1/alerts/active").json()
        assert active["pagination"]["total"] == 2 and all(a["status"] == "ACTIVE" for a in active["data"])
        m1 = client.get("/api/v1/machines/M001/alerts").json()
        assert m1["pagination"]["total"] == 1 and m1["data"][0]["machine_id"] == "M001"
        assert client.get("/api/v1/machines/NOPE/alerts").status_code == 404
        assert client.get("/api/v1/alerts?status=RESOLVED").json()["data"] == []

    def test_resolve_alert_flow(self, client, seeded):
        alert_id = client.get("/api/v1/alerts/active").json()["data"][0]["id"]
        r = client.post(f"/api/v1/alerts/{alert_id}/resolve")
        assert r.status_code == 200 and r.json()["data"]["status"] == "RESOLVED" and r.json()["data"]["resolved_at"]
        assert client.post(f"/api/v1/alerts/{alert_id}/resolve").status_code == 409
        assert client.post("/api/v1/alerts/999999/resolve").status_code == 404
        assert client.get("/api/v1/alerts/active").json()["pagination"]["total"] == 1

    @pytest.mark.parametrize("url", ["/api/v1/predictions?page=0", "/api/v1/predictions?page_size=0",
                                     "/api/v1/predictions?page_size=201", "/api/v1/predictions/abc",
                                     "/api/v1/alerts?status=BOGUS", "/api/v1/predictions?page=x"])
    def test_validation_errors_are_422(self, client, url):
        assert client.get(url).status_code == 422

    def test_resolve_with_invalid_id_is_422(self, client):
        assert client.post("/api/v1/alerts/abc/resolve").status_code == 422
