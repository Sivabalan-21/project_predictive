"""UI client vs the REAL FastAPI app + REAL PostgreSQL (real login, real JWT, real rows). Skipped without PostgreSQL."""
from __future__ import annotations

import socket
import threading
import time

import pytest
import uvicorn

from api.main import create_app
from services.ingestion_service import IngestionService
from tests.phase3.conftest import NORMAL, OVERSTRAIN, make_event
from tests.phase4.conftest import STRONG_PASSWORD
from ui.api_client import ApiClient, ApiError

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


@pytest.fixture()
def live_api(session_factory):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(create_app(session_factory=session_factory, kafka_check=None),
                                           host="127.0.0.1", port=port, log_level="error"))
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    t.join(5)


@pytest.fixture()
def seeded(session_factory, predictor):
    ing = IngestionService(session_factory, predictor)
    for i in range(3):
        ing.ingest(make_event("M001", f"2026-10-08T10:30:0{i}Z", NORMAL))
    ing.ingest(make_event("M001", "2026-10-08T10:31:00Z", OVERSTRAIN))


def test_real_backend_login_health_ready_predictions_alerts(live_api, make_user, seeded):
    user, pwd = make_user("viewer")
    c = ApiClient(live_api, timeout=10)
    assert c.health() and c.ready().status == "ready" and c.ready().kafka == "skipped"
    c.login(user, pwd)
    preds, page = c.predictions()
    assert page.total == 4 and preds[0].prediction == "FAILURE" and preds[0].fault_type            # newest first
    alerts, apage = c.alerts(status="ACTIVE")
    assert apage.total == 1 and alerts[0].machine_id == "M001" and alerts[0].status == "ACTIVE"
    assert c.predictions(machine_id="M001")[1].total == 4


def test_real_backend_rejections(live_api, make_user, seeded):
    user, pwd = make_user("viewer")
    with pytest.raises(ApiError) as e:
        ApiClient(live_api, timeout=10).login(user, "wrong-password")
    assert e.value.kind == "unauthorized"
    c = ApiClient(live_api, timeout=10, token="not-a-jwt", token_expires_at=time.time() + 60)
    with pytest.raises(ApiError) as e:
        c.predictions()
    assert e.value.kind == "unauthorized"
    c.login(user, pwd)
    with pytest.raises(ApiError) as e:
        c.predictions(machine_id="NOPE")
    assert e.value.kind == "not_found"
