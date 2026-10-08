import json
import shutil

import numpy as np
import pandas as pd
import pytest

from ml import config
from ml.prediction import InvalidReadingError, ModelLoadError, PredictionResult, SensorReading, load_bundle
from ml.training.evaluate import binary_metrics
from ml.training.preprocess import split_dataset
from tests.conftest import NORMAL, OVERSTRAIN


def test_valid_reading_returns_complete_result(service):
    r = service.predict(NORMAL)
    assert isinstance(r, PredictionResult)
    assert r.final_status in {"NORMAL", "FAILURE"} and r.is_failure == (r.final_status == "FAILURE")
    for p in (r.rf_probability, r.xgb_probability, r.risk_score):
        assert 0.0 <= p <= 1.0
    assert r.rf_prediction in (0, 1) and r.xgb_prediction in (0, 1) and r.isolation_prediction in (0, 1)
    assert r.decision_method == service.decision_method and len(r.model_version) == 12
    assert json.dumps(r.to_dict())  # JSON-serialisable (needed by the API / Kafka later)


def test_risk_score_is_mean_of_rf_and_xgb_and_not_a_calibrated_claim(service):
    r = service.predict(OVERSTRAIN)
    assert r.risk_score == pytest.approx((r.rf_probability + r.xgb_probability) / 2)


def test_typical_reading_is_normal_and_has_no_fault(service):
    r = service.predict(NORMAL)
    assert r.final_status == "NORMAL" and r.fault_type is None and r.fault_codes == ()


def test_overstrain_reading_is_failure_with_osf_fault(service):
    r = service.predict({**OVERSTRAIN, "product_type": "H"})
    assert r.final_status == "FAILURE"
    assert "OSF" in r.fault_codes and r.fault_type is not None and "OSF" in r.fault_type


def test_prediction_is_deterministic_and_batch_equals_single(service):
    rows = [NORMAL, OVERSTRAIN, {**NORMAL, "torque": 55.0, "tool_wear": 215}]
    batch = service.predict_many(rows)
    assert batch == service.predict_many(rows)
    assert [service.predict(r) for r in rows] == batch


def test_accepts_sensor_reading_objects_and_empty_batch(service):
    assert service.predict(SensorReading(**NORMAL)) == service.predict(NORMAL)
    assert service.predict_many([]) == []


@pytest.mark.parametrize("bad", [
    {**NORMAL, "torque": None}, {**NORMAL, "air_temperature": float("nan")},
    {**NORMAL, "rotational_speed": -5}, {**NORMAL, "product_type": "Z"},
    {k: v for k, v in NORMAL.items() if k != "tool_wear"},
])
def test_invalid_input_is_rejected_not_silently_predicted(service, bad):
    with pytest.raises(InvalidReadingError):
        service.predict(bad)


def test_one_bad_reading_rejects_the_batch(service):
    with pytest.raises(InvalidReadingError):
        service.predict_many([NORMAL, {**NORMAL, "torque": "x"}])


def test_service_reproduces_reported_test_metrics(service, dataset):
    """Train/serve consistency: the deployed service must reproduce model_metrics.json exactly."""
    report = json.loads(config.METRICS_PATH.read_text())
    df, _ = dataset
    test = split_dataset(df).test
    results = service.predict_many(test[["air_temperature", "process_temperature", "rotational_speed",
                                         "torque", "tool_wear"]].to_dict("records"))
    pred = np.array([r.is_failure for r in results], dtype=int)
    m = binary_metrics(test["machine_failure"].to_numpy(), pred, None)
    reported = report["test"]["selected"]
    for key in ("precision", "recall", "f1", "accuracy"):
        assert m[key] == pytest.approx(reported[key], abs=1e-12), key
    assert m["confusion_matrix"] == reported["confusion_matrix"]
    assert service.decision_method == report["selection"]["selected_decision_method"]


# ---------------------------------------------------------------- artifact integrity
def test_loader_rejects_tampered_artifact(tmp_path):
    shutil.copytree(config.ARTIFACTS_DIR, tmp_path / "a")
    with open(tmp_path / "a" / config.RF_FILE, "ab") as fh:
        fh.write(b"x")
    with pytest.raises(ModelLoadError, match="hash"):
        load_bundle(tmp_path / "a")


def test_loader_reports_missing_manifest_and_files(tmp_path):
    with pytest.raises(ModelLoadError, match="Train first"):
        load_bundle(tmp_path)
    shutil.copytree(config.ARTIFACTS_DIR, tmp_path / "b")
    (tmp_path / "b" / config.XGB_FILE).unlink()
    with pytest.raises(ModelLoadError, match="missing artifact"):
        load_bundle(tmp_path / "b")
