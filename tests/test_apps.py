"""Smoke/behaviour tests for the legacy Flask and Streamlit UIs (they must use the single service)."""
import sqlite3
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

import src.database as db
from ml.prediction import get_prediction_service
from tests.conftest import NORMAL, OVERSTRAIN

ROOT = Path(__file__).resolve().parent.parent
GOOD_FORM = {"air_temp": "300.5", "proc_temp": "310.8", "rot_speed": "1512", "torque": "39.8", "tool_wear": "104"}
FAIL_FORM = {"air_temp": "300", "proc_temp": "310", "rot_speed": "1300", "torque": "70", "tool_wear": "250"}


@pytest.fixture()
def tmp_db(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "t.db")
    return tmp_path / "t.db"


@pytest.fixture()
def client(tmp_db):
    from flask_app.app import app
    app.config["TESTING"] = True
    return app.test_client()


# ---------------------------------------------------------------- Flask
@pytest.mark.parametrize("path", ["/", "/predict", "/history", "/report"])
def test_flask_pages_render(client, path):
    assert client.get(path).status_code == 200


def test_flask_predict_form_normal_and_failure(client):
    assert b"NORMAL OPERATION" in client.post("/predict", data=GOOD_FORM).data
    body = client.post("/predict", data=FAIL_FORM).data
    assert b"MACHINE FAILURE" in body and b"OSF" in body


def test_flask_invalid_form_is_reported_not_crashing(client):
    r = client.post("/predict", data={**GOOD_FORM, "torque": "abc"})
    assert r.status_code == 200 and b"Invalid input" in r.data
    assert client.post("/predict", data={"air_temp": "300"}).status_code == 200


def test_flask_api_matches_prediction_service(client):
    r = client.post("/api/predict", json=GOOD_FORM)
    assert r.status_code == 200
    expected = get_prediction_service().predict(NORMAL).to_dict()
    assert r.json == expected  # one implementation: identical output


def test_flask_api_errors(client):
    r = client.post("/api/predict", json={"torque": 5})
    assert r.status_code == 422 and r.json["error"] == "invalid_reading"
    assert client.post("/api/predict", data="x", content_type="text/plain").status_code == 400


def test_flask_report_reads_metrics_dynamically(client):
    from ml.reporting import load_metrics
    body = client.get("/report").data.decode()
    assert load_metrics()["test"]["selected"]["name"] in body
    assert "98.58" not in body


def test_flask_predictions_are_persisted(client):
    client.post("/predict", data=GOOD_FORM)
    client.post("/api/predict", json=FAIL_FORM)
    df = db.fetch_history()
    assert list(df["final_status"]) == ["FAILURE", "NORMAL"] and df.loc[0, "risk_score"] > 0


# ---------------------------------------------------------------- Streamlit pages
def _page(name):
    return AppTest.from_file(str(ROOT / "app" / name), default_timeout=60)


def test_streamlit_home_and_report_pages_run(tmp_db):
    for page in ("main.py", "pages/4_model_report.py"):
        at = _page(page).run()
        assert not at.exception, f"{page}: {at.exception}"


def test_streamlit_home_shows_dynamic_metrics(tmp_db):
    from ml.reporting import headline, load_metrics
    at = _page("main.py").run()
    values = " ".join(str(m.value) for m in at.metric)
    assert headline(load_metrics())["method"] in values and "98.58" not in values


def test_streamlit_predict_page_uses_service_and_saves(tmp_db):
    at = _page("pages/2_predict.py").run()
    assert not at.exception
    at.button[0].click().run()
    assert not at.exception, at.exception
    expected = get_prediction_service().predict({"air_temperature": 300.0, "process_temperature": 310.0,
                                                 "rotational_speed": 1500, "torque": 40.0, "tool_wear": 100})
    page_text = " ".join(m.value for m in at.markdown)
    assert ("MACHINE FAILURE" if expected.is_failure else "NORMAL OPERATION") in page_text
    assert len(db.fetch_history()) == 1


def test_streamlit_dashboard_and_history_with_legacy_and_new_rows(tmp_db):
    db.create_tables()
    svc = get_prediction_service()
    db.insert_prediction(NORMAL, svc.predict(NORMAL))
    db.insert_prediction(OVERSTRAIN, svc.predict(OVERSTRAIN))
    with sqlite3.connect(tmp_db) as conn:  # row written by the pre-refactor app (emoji status, long fault label)
        conn.execute("INSERT INTO predictions (air_temp,process_temp,rotational_speed,torque,tool_wear,"
                     "rf_prediction,xgb_prediction,iso_prediction,final_status,fault_type) "
                     "VALUES (300,310,1500,40,100,1,1,1,'🔴 FAILURE','HDF — Heat Dissipation Failure')")
    for page in ("pages/1_dashboard.py", "pages/3_history.py"):
        at = _page(page).run()
        assert not at.exception and not at.error, f"{page}: {at.exception} {[e.value for e in at.error]}"
    df = db.fetch_history()
    assert set(df["final_status"]) == {"NORMAL", "FAILURE"} and "HDF" in db.fault_counts(df)


def test_streamlit_pages_empty_database(tmp_db):
    for page in ("pages/1_dashboard.py", "pages/3_history.py"):
        at = _page(page).run()
        assert not at.exception
