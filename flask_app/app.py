"""Flask demo UI. All prediction logic lives in ml.prediction (single implementation)."""
from __future__ import annotations

import logging
import os
import sys

from flask import Flask, jsonify, render_template, request

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from ml.prediction import InvalidReadingError, ModelLoadError, get_prediction_service  # noqa: E402
from ml.reporting import MISSING_HINT, headline, load_metrics  # noqa: E402
from src import database as db  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("flask_app")

app = Flask(__name__)

# Form/JSON field names used by this demo UI -> canonical sensor names.
FIELD_MAP = {"air_temp": "air_temperature", "proc_temp": "process_temperature",
             "rot_speed": "rotational_speed", "torque": "torque", "tool_wear": "tool_wear"}


def _to_reading(source) -> dict:
    """Accept demo-UI names or canonical names; validation happens in the prediction service."""
    values = {}
    for ui_name, canonical in FIELD_MAP.items():
        value = source.get(ui_name, source.get(canonical))
        if value not in (None, ""):
            values[canonical] = value
    if source.get("product_type"):
        values["product_type"] = source.get("product_type")
    return values


def _run_prediction(values: dict):
    result = get_prediction_service().predict(values)
    db.create_tables()
    db.insert_prediction({k: float(values[k]) for k in FIELD_MAP.values()}, result)
    return result


@app.errorhandler(ModelLoadError)
def model_unavailable(exc):
    log.error("model unavailable: %s", exc)
    return jsonify(error="model_unavailable", detail=str(exc)), 503


@app.route("/")
def index():
    db.create_tables()
    df = db.fetch_history()
    total = len(df)
    failures = int((df["final_status"] == "FAILURE").sum()) if total else 0
    rate = round(failures / total * 100, 1) if total else 0
    trend = {}
    if total:
        for col in ("air_temp", "process_temp", "torque", "tool_wear"):
            trend[col] = df[col].iloc[::-1].tail(20).tolist()
        trend["timestamps"] = df["timestamp"].iloc[::-1].tail(20).astype(str).tolist()
    report = load_metrics()
    return render_template("index.html", total=total, failures=failures, normal=total - failures, rate=rate,
                           fault_counts=db.fault_counts(df), recent=df.head(8).to_dict("records") if total else [],
                           trend_data=trend, headline=headline(report) if report else None)


@app.route("/predict", methods=["GET", "POST"])
def predict():
    result, error = None, None
    if request.method == "POST":
        values = _to_reading(request.form)
        try:
            result = _run_prediction(values).to_dict()
            result["input"] = {k: values[k] for k in FIELD_MAP.values()}
        except InvalidReadingError as exc:
            error = str(exc)
    return render_template("predict.html", result=result, error=error)


@app.route("/api/predict", methods=["POST"])
def api_predict():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify(error="invalid_request", detail="JSON object body required"), 400
    try:
        return jsonify(_run_prediction(_to_reading(payload)).to_dict())
    except InvalidReadingError as exc:
        return jsonify(error="invalid_reading", detail=str(exc)), 422


@app.route("/history")
def history():
    db.create_tables()
    records = db.fetch_history().to_dict("records")
    failures = sum(1 for r in records if r["final_status"] == "FAILURE")
    return render_template("history.html", records=records, total=len(records), failures=failures,
                           normal=len(records) - failures)


@app.route("/report")
def report():
    return render_template("report.html", report=load_metrics(), missing_hint=MISSING_HINT)


if __name__ == "__main__":
    app.run(debug=os.environ.get("FLASK_DEBUG") == "1", port=int(os.environ.get("PORT", 5000)))
