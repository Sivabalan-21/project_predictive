import pytest

from db.config import ConfigurationError, DatabaseSettings
from ml.config import KafkaSettings
from streaming.schemas import AlertEvent, EventValidationError, PredictionEvent, SensorEvent
from tests.phase3.conftest import NORMAL, make_event


def test_database_url_required():
    with pytest.raises(ConfigurationError, match="DATABASE_URL"):
        DatabaseSettings.from_env({})


@pytest.mark.parametrize("url", ["sqlite:///x.db", "mysql://u:p@h/db"])
def test_non_postgres_urls_rejected(url):
    with pytest.raises(ConfigurationError, match="PostgreSQL"):
        DatabaseSettings.from_env({"DATABASE_URL": url})


def test_password_is_masked_when_logged():
    s = DatabaseSettings.from_env({"DATABASE_URL": "postgresql+psycopg://u:s3cret@h:5432/db"})
    assert "s3cret" not in s.safe_url() and "***" in s.safe_url()


def test_alert_topic_setting_and_validation():
    assert KafkaSettings.from_env({}).alert_topic == "alerts"
    s = KafkaSettings.from_env({"KAFKA_SENSOR_TOPIC": "sensor-readings", "KAFKA_PREDICTION_TOPIC": "predictions",
                                "KAFKA_ALERT_TOPIC": "alerts"})
    assert (s.sensor_topic, s.prediction_topic, s.alert_topic) == ("sensor-readings", "predictions", "alerts")
    with pytest.raises(ValueError):
        KafkaSettings.from_env({"KAFKA_ALERT_TOPIC": "machine-predictions"})


def test_event_id_optional_and_validated():
    assert SensorEvent.from_dict({"machine_id": "M1", "timestamp": "2026-10-08T10:30:00Z", **NORMAL}).event_id is None
    assert make_event(event_id="3f2b8c9e-1a2b-4c5d-8e9f-0123456789ab").event_id.startswith("3f2b")
    with pytest.raises(EventValidationError):
        make_event(event_id="bad id with spaces")


def test_alert_event_roundtrip_shape():
    import json
    ev = AlertEvent(alert_id=1, machine_id="M001", fault_type="OSF", risk_score=0.82, status="ACTIVE",
                    timestamp=make_event().timestamp)
    assert json.loads(ev.to_json_bytes()) == {"alert_id": 1, "machine_id": "M001", "fault_type": "OSF", "risk_score": 0.82,
                                              "status": "ACTIVE", "timestamp": "2026-10-08T10:30:00.000Z"}
    with pytest.raises(Exception):
        AlertEvent(alert_id=1, machine_id="M001", fault_type="OSF", risk_score=1.5, status="ACTIVE", timestamp=make_event().timestamp)
