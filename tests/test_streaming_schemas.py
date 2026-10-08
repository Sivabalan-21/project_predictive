import json
from datetime import datetime, timezone

import pytest

from ml.config import KafkaSettings
from streaming.schemas import EventValidationError, PredictionEvent, SensorEvent
from tests.conftest import NORMAL, OVERSTRAIN

VALID = {"machine_id": "MACHINE-001", "timestamp": "2026-10-06T11:30:01Z", "air_temperature": 300.5,
         "process_temperature": 310.8, "rotational_speed": 1512, "torque": 39.8, "tool_wear": 104}


def payload(**changes):
    d = {**VALID, **changes}
    return json.dumps({k: v for k, v in d.items() if v is not ...})


def test_valid_event_roundtrip():
    e = SensorEvent.from_payload(payload())
    assert e.machine_id == "MACHINE-001" and e.timestamp == datetime(2026, 10, 6, 11, 30, 1, tzinfo=timezone.utc)
    again = SensorEvent.from_payload(e.to_json_bytes())
    assert again == e and b'"timestamp":"2026-10-06T11:30:01.000Z"' in e.to_json_bytes()
    assert e.to_reading() == {"air_temperature": 300.5, "process_temperature": 310.8, "rotational_speed": 1512.0,
                              "torque": 39.8, "tool_wear": 104.0}


def test_optional_product_type_is_passed_to_the_service_reading():
    e = SensorEvent.from_payload(payload(product_type="H"))
    assert e.to_reading()["product_type"] == "H"
    with pytest.raises(EventValidationError):
        SensorEvent.from_payload(payload(product_type="X"))


@pytest.mark.parametrize("field", ["machine_id", "timestamp", "air_temperature", "process_temperature",
                                   "rotational_speed", "torque", "tool_wear"])
def test_missing_field_rejected(field):
    with pytest.raises(EventValidationError, match=field):
        SensorEvent.from_payload(payload(**{field: ...}))


@pytest.mark.parametrize("bad", ['"39.8"', "true", "null", "[1]", '"abc"'])
def test_invalid_type_rejected(bad):
    raw = json.dumps(VALID).replace("39.8", "@").replace('"@"', "@").replace("@", bad)
    with pytest.raises(EventValidationError, match="torque"):
        SensorEvent.from_payload(raw)


@pytest.mark.parametrize("field,value", [
    ("air_temperature", 1000.0), ("air_temperature", 100.0), ("process_temperature", 500.0),
    ("rotational_speed", -1), ("rotational_speed", 99999), ("torque", -5), ("torque", 500), ("tool_wear", -1),
    ("tool_wear", 5000)])
def test_out_of_range_sensor_values_rejected(field, value):
    with pytest.raises(EventValidationError, match=field):
        SensorEvent.from_payload(payload(**{field: value}))


@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_numbers_rejected(token):
    with pytest.raises(EventValidationError):
        SensorEvent.from_payload(json.dumps(VALID).replace("39.8", token))


@pytest.mark.parametrize("ts", ["2026-10-06T11:30:01", "not-a-time", 1760000000, None, ""])
def test_bad_timestamps_rejected(ts):
    with pytest.raises(EventValidationError, match="timestamp"):
        SensorEvent.from_payload(payload(timestamp=ts))


def test_timezone_offsets_are_normalised_to_utc():
    e = SensorEvent.from_payload(payload(timestamp="2026-10-06T13:30:01+02:00"))
    assert e.timestamp == datetime(2026, 10, 6, 11, 30, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize("machine_id", ["", "bad id!", "../x", " ", "x" * 65, 5])
def test_bad_machine_ids_rejected(machine_id):
    with pytest.raises(EventValidationError, match="machine_id"):
        SensorEvent.from_payload(payload(machine_id=machine_id))


@pytest.mark.parametrize("raw", ["hello", "", "[]", "null", "{", b"\xff\xfe"])
def test_garbage_payloads_rejected_with_validation_error(raw):
    with pytest.raises(EventValidationError):
        SensorEvent.from_payload(raw)


def test_unknown_fields_rejected():
    with pytest.raises(EventValidationError, match="surprise"):
        SensorEvent.from_payload(payload(surprise=1))


def test_all_problems_are_reported_together():
    with pytest.raises(EventValidationError) as err:
        SensorEvent.from_payload(payload(torque="x", tool_wear=-3, machine_id="!"))
    assert len(err.value.problems) >= 3


# ---------------------------------------------------------------- PredictionEvent
def test_prediction_event_built_from_a_real_service_result(service):
    event = SensorEvent.from_dict({**VALID, **OVERSTRAIN, "product_type": "H",
                                   "machine_id": "MACHINE-003", "timestamp": "2026-10-06T11:30:02Z"})
    result = service.predict(event.to_reading())
    pe = PredictionEvent.from_result(event, result, datetime.now(timezone.utc))
    assert (pe.machine_id, pe.status, pe.fault_type, pe.model_method) == (
        "MACHINE-003", "FAILURE", result.fault_type, service.decision_method)
    assert pe.risk_score == result.risk_score and 0 <= pe.risk_score <= 1
    back = PredictionEvent.from_payload(pe.to_json_bytes())
    assert back == pe
    assert {"machine_id", "timestamp", "status", "risk_score", "fault_type", "model_method"} <= set(json.loads(pe.to_json_bytes()))


def test_prediction_event_rejects_bad_values():
    base = dict(machine_id="MACHINE-001", timestamp="2026-10-06T11:30:01Z", status="NORMAL", risk_score=0.1,
                model_method="m", rf_prediction=0, xgb_prediction=0, isolation_prediction=0, model_version="v",
                processed_at="2026-10-06T11:30:02Z")
    PredictionEvent(**base)
    for change in ({"risk_score": 1.5}, {"status": "MAYBE"}, {"timestamp": "2026-10-06T11:30:01"}):
        with pytest.raises(Exception):
            PredictionEvent(**{**base, **change})


# ---------------------------------------------------------------- configuration
def test_kafka_settings_defaults_and_env_overrides():
    s = KafkaSettings.from_env({})
    assert (s.bootstrap_servers, s.sensor_topic, s.prediction_topic) == ("localhost:9092", "machine-sensor-data", "machine-predictions")
    s2 = KafkaSettings.from_env({"KAFKA_BOOTSTRAP_SERVERS": "broker:9093", "KAFKA_SENSOR_TOPIC": "a", "KAFKA_PREDICTION_TOPIC": "b",
                                 "KAFKA_TOPIC_PARTITIONS": "6", "KAFKA_AUTO_OFFSET_RESET": "LATEST"})
    assert (s2.bootstrap_servers, s2.sensor_topic, s2.prediction_topic, s2.topic_partitions, s2.auto_offset_reset) == \
           ("broker:9093", "a", "b", 6, "latest")


@pytest.mark.parametrize("env", [{"KAFKA_TOPIC_PARTITIONS": "x"}, {"KAFKA_TOPIC_PARTITIONS": "0"}, {"KAFKA_SENSOR_TOPIC": ""},
                                 {"KAFKA_SENSOR_TOPIC": "t", "KAFKA_PREDICTION_TOPIC": "t"}, {"KAFKA_AUTO_OFFSET_RESET": "middle"}])
def test_kafka_settings_reject_bad_values(env):
    with pytest.raises(ValueError):
        KafkaSettings.from_env(env)
