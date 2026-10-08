"""Consumer behaviour. UNIT tests use fakes for Kafka AND the ingestion service.
The two tests marked integration use the REAL PostgreSQL + real ML service but a FAKE Kafka client
(so they are NOT Kafka integration tests)."""
import dataclasses
import json

import pytest
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy import text

from ml.config import KafkaSettings
from services.ingestion_service import IngestionOutcome, IngestionService, PredictionError
from streaming.db_consumer import PersistingSensorConsumer, ProcessingHalted
from streaming.schemas import AlertEvent, PredictionEvent
from tests.phase3.conftest import NORMAL, OVERSTRAIN, make_event
from tests.phase3.fakes import RecordingConsumer, RecordingIngestion, RecordingProducer, msg

SETTINGS = dataclasses.replace(KafkaSettings(), sensor_topic="sensor-readings", prediction_topic="predictions",
                               alert_topic="alerts")


def payload(values=NORMAL, ts="2026-10-08T10:30:00Z", machine="M001"):
    return json.dumps({"machine_id": machine, "timestamp": ts, **values}).encode()


def outcome(alert=False):
    ev = make_event()
    pred = PredictionEvent(machine_id="M001", timestamp=ev.timestamp, status="FAILURE" if alert else "NORMAL",
                           risk_score=0.8, fault_type="OSF" if alert else None, model_method="m", rf_prediction=1,
                           xgb_prediction=1, isolation_prediction=0, model_version="v", processed_at=ev.timestamp,
                           sensor_reading_id=1)
    al = AlertEvent(alert_id=7, machine_id="M001", fault_type="OSF", risk_score=0.8, status="ACTIVE", timestamp=ev.timestamp) if alert else None
    return IngestionOutcome(duplicate=False, sensor_reading_id=1, prediction=pred, alert=al)


def build(messages, behaviours, retries=2, producer_fail=False):
    events = []
    cons = RecordingConsumer(messages, events)
    prod = RecordingProducer(events, fail=producer_fail)
    ing = RecordingIngestion(events, behaviours)
    sleeps = []
    c = PersistingSensorConsumer(SETTINGS, ing, consumer=cons, producer=prod, db_retries=retries, sleep=sleeps.append)
    return c, cons, prod, ing, events, sleeps


def op_err():
    return OperationalError("INSERT", {}, Exception("connection refused"))


def test_offset_is_committed_only_after_db_commit_and_downstream_publish():
    c, cons, prod, ing, events, _ = build([msg(payload(OVERSTRAIN), 5)], [outcome(alert=True)])
    c.run(max_messages=1)
    assert events[:5] == [("db_commit",), ("publish", "predictions"), ("publish", "alerts"), ("flush",), ("offset_commit", 5)]
    assert [t for t, *_ in prod.sent] == ["predictions", "alerts"] and all(k == b"M001" for _, k, _ in prod.sent)
    assert c.stats.persisted == 1 and c.stats.alerts_created == 1


def test_db_failure_does_not_commit_offset_and_halts():
    c, cons, prod, ing, events, sleeps = build([msg(payload(), 1)], [op_err()] * 5, retries=2)
    with pytest.raises(ProcessingHalted):
        c.run(max_messages=1)
    assert cons.committed == [] and prod.sent == [] and ing.calls == 3 and sleeps == [1.0, 2.0]   # 1 try + 2 retries
    assert not any(e[0] == "offset_commit" for e in events)


def test_transient_db_failure_is_retried_then_committed():
    c, cons, prod, ing, events, sleeps = build([msg(payload(), 1)], [op_err(), outcome()])
    c.run(max_messages=1)
    assert cons.committed == [1] and ing.calls == 2 and c.stats.db_retries == 1
    assert events.index(("db_commit",)) < events.index(("offset_commit", 1))


def test_non_retryable_db_error_halts_without_commit():
    err = IntegrityError("INSERT", {}, Exception("fk violation"))
    c, cons, *_ = build([msg(payload(), 1)], [err])
    with pytest.raises(ProcessingHalted):
        c.run(max_messages=1)
    assert cons.committed == []


@pytest.mark.parametrize("raw", [b"not json", b"{}", json.dumps({"machine_id": "M1", "timestamp": "2026-10-08T10:30:00Z",
                                                                  **{**NORMAL, "torque": 9999}}).encode(), b"\xff\xfe"])
def test_invalid_message_is_skipped_without_db_and_without_crash(raw, caplog):
    c, cons, prod, ing, *_ = build([msg(raw, 3), msg(payload(), 4)], [outcome()])
    c.run(max_messages=2)
    assert c.stats.invalid == 1 and ing.calls == 1 and cons.committed == [3, 4]
    assert "invalid sensor event" in caplog.text


def test_duplicate_is_skipped_without_publishing_but_offset_is_committed():
    c, cons, prod, ing, *_ = build([msg(payload(), 9)], [IngestionOutcome(duplicate=True)])
    c.run(max_messages=1)
    assert prod.sent == [] and cons.committed == [9] and c.stats.duplicates == 1 and c.stats.persisted == 0


def test_unpredictable_message_is_skipped_and_committed():
    c, cons, prod, ing, *_ = build([msg(payload(), 2)], [PredictionError("bad")])
    c.run(max_messages=1)
    assert cons.committed == [2] and c.stats.prediction_errors == 1 and prod.sent == []


def test_publish_failure_is_logged_but_does_not_lose_the_db_row(caplog):
    c, cons, *_ = build([msg(payload(), 2)], [outcome()], producer_fail=True)
    c.run(max_messages=1)
    assert c.stats.publish_errors >= 1 and cons.committed == [2] and "could not publish" in caplog.text


def test_consumer_closed_on_halt():
    c, cons, *_ = build([msg(payload(), 1)], [op_err()] * 9, retries=0)
    with pytest.raises(ProcessingHalted):
        c.run()
    assert cons.closed


# ----------------------------------------------------------------- real PostgreSQL, fake Kafka
@pytest.mark.integration
@pytest.mark.postgres
def test_db_down_then_recovery_same_message_redelivered_and_processed(session_factory, predictor):
    """Failure test: DB transaction fails -> offset NOT committed -> after recovery the same message succeeds."""
    message = msg(payload(OVERSTRAIN), 11)
    events = []

    class DownDatabase:                                     # DB transaction fails before anything is written
        def ingest(self, event): raise op_err()
    down = PersistingSensorConsumer(SETTINGS, DownDatabase(), consumer=RecordingConsumer([message], events),
                                    producer=RecordingProducer(events), db_retries=1, sleep=lambda s: None)
    with pytest.raises(ProcessingHalted):
        down.run(max_messages=1)
    assert ("offset_commit", 11) not in events
    with session_factory() as s:
        assert s.execute(text("SELECT count(*) FROM sensor_readings")).scalar_one() == 0

    # restart: Kafka redelivers offset 11; the real database is back
    events2 = []
    cons2 = RecordingConsumer([msg(payload(OVERSTRAIN), 11)], events2)
    up = PersistingSensorConsumer(SETTINGS, IngestionService(session_factory, predictor), consumer=cons2,
                                  producer=RecordingProducer(events2))
    up.run(max_messages=1)
    assert cons2.committed == [11] and up.stats.persisted == 1 and up.stats.alerts_created == 1
    with session_factory() as s:
        n = [s.execute(text(f"SELECT count(*) FROM {t}")).scalar_one() for t in ("sensor_readings", "predictions", "alerts")]
    assert n == [1, 1, 1]


@pytest.mark.integration
@pytest.mark.postgres
def test_redelivered_messages_are_idempotent_end_to_end_with_fake_kafka(session_factory, predictor):
    events = []
    msgs = [msg(payload(OVERSTRAIN), o) for o in (0, 1, 2)]           # same event delivered three times
    cons = RecordingConsumer(msgs, events)
    prod = RecordingProducer(events)
    c = PersistingSensorConsumer(SETTINGS, IngestionService(session_factory, predictor), consumer=cons, producer=prod)
    c.run(max_messages=3)
    assert c.stats.persisted == 1 and c.stats.duplicates == 2 and cons.committed == [0, 1, 2]
    assert [t for t, *_ in prod.sent] == ["predictions", "alerts"]     # downstream published once
    with session_factory() as s:
        assert [s.execute(text(f"SELECT count(*) FROM {t}")).scalar_one() for t in ("sensor_readings", "predictions", "alerts")] == [1, 1, 1]
