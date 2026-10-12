"""Dead-letter queue + bounded downstream retries. Kafka is FAKED here (these are NOT Kafka integration tests); the two tests
marked postgres use the real database."""
import json

import pytest
from confluent_kafka import KafkaError, KafkaException
from sqlalchemy import text

from ml.config import KafkaSettings
from services.ingestion_service import IngestionService, PredictionError
from streaming.db_consumer import PersistingSensorConsumer, ProcessingHalted
from tests.fakes_kafka import FakeKafkaMessage
from tests.phase3.conftest import NORMAL, OVERSTRAIN
from tests.phase3.fakes import RecordingConsumer, RecordingIngestion
from tests.phase3.test_db_consumer import SETTINGS, op_err, outcome, payload

DLQ = SETTINGS.dlq_topic


class ScriptedProducer:
    """plan(topic, n) -> 'ok' | 'fail' (delivery report error) | 'raise' | 'buffer' | 'timeout' (no report), n = 1-based call number per topic."""

    def __init__(self, events, plan=lambda topic, n: "ok"):
        self.events, self.plan, self.calls, self.sent = events, plan, [], []
        self._count, self._unconfirmed = {}, 0

    def produce(self, topic, key=None, value=None, on_delivery=None, headers=None):
        n = self._count[topic] = self._count.get(topic, 0) + 1
        mode = self.plan(topic, n)
        self.calls.append({"topic": topic, "key": key, "value": value, "headers": dict(headers or []), "mode": mode})
        self.events.append(("produce", topic, mode))
        if mode == "raise":
            raise KafkaException(KafkaError(KafkaError._MSG_TIMED_OUT))
        if mode == "buffer":
            raise BufferError("queue full")
        if mode == "timeout":
            self._unconfirmed += 1
            return
        if mode == "fail":
            on_delivery(KafkaError(KafkaError._MSG_TIMED_OUT), None)
            return
        self.sent.append((topic, key, value))
        on_delivery(None, None)

    def poll(self, t=0):
        return 0

    def flush(self, t=None):
        self.events.append(("flush",))
        left, self._unconfirmed = self._unconfirmed, 0
        return left

    def to(self, topic):
        return [c for c in self.calls if c["topic"] == topic]


def kmsg(value, offset=0, key=b"M001"):
    return FakeKafkaMessage(value, topic="sensor-readings", partition=2, offset=offset, key=key)


def build(messages, behaviours=(), plan=lambda t, n: "ok", dlq_retries=2, publish_retries=2):
    events = []
    cons = RecordingConsumer(messages, events)
    prod = ScriptedProducer(events, plan)
    ing = RecordingIngestion(events, list(behaviours))
    sleeps = []
    c = PersistingSensorConsumer(SETTINGS, ing, consumer=cons, producer=prod, db_retries=1, sleep=sleeps.append,
                                 dlq_retries=dlq_retries, publish_retries=publish_retries, publish_backoff_s=0.5)
    return c, cons, prod, ing, events, sleeps


BAD = [b"not json", b"{}", b"\xff\xfe\x00binary", json.dumps({"machine_id": "M1", "timestamp": "2026-10-08T10:30:00Z", **{**NORMAL, "torque": 9999}}).encode(),
       b'{"machine_id": "bad id"}', None]


# =============================================================================================== settings
def test_dlq_topic_default_env_and_validation():
    assert KafkaSettings.from_env({}).dlq_topic == "sensor-readings.dlq"
    assert KafkaSettings.from_env({"KAFKA_DLQ_TOPIC": "my.dlq"}).dlq_topic == "my.dlq"
    for clash in ("KAFKA_SENSOR_TOPIC", "KAFKA_PREDICTION_TOPIC", "KAFKA_ALERT_TOPIC"):
        with pytest.raises(ValueError, match="DLQ"):
            KafkaSettings.from_env({"KAFKA_DLQ_TOPIC": {"KAFKA_SENSOR_TOPIC": "machine-sensor-data", "KAFKA_PREDICTION_TOPIC": "machine-predictions", "KAFKA_ALERT_TOPIC": "alerts"}[clash]})
    with pytest.raises(ValueError, match="dlq_topic"):
        KafkaSettings.from_env({"KAFKA_DLQ_TOPIC": "  "})


# =============================================================================================== DLQ happy path
@pytest.mark.parametrize("raw", BAD)
def test_invalid_message_goes_to_the_dlq_with_original_bytes_and_metadata(raw):
    c, cons, prod, ing, events, _ = build([kmsg(raw, 41)])
    c.run(max_messages=1)
    (call,) = prod.calls
    assert call["topic"] == DLQ and call["value"] == raw and call["key"] == b"M001"          # original bytes, untouched
    h = {k: v.decode() for k, v in call["headers"].items()}
    assert h["dlq.reason"] == "invalid_schema"
    assert (h["dlq.source.topic"], h["dlq.source.partition"], h["dlq.source.offset"]) == ("sensor-readings", "2", "41")
    assert h["dlq.error"] and len(h["dlq.error"]) <= 500 and h["dlq.consumer_group"] == SETTINGS.consumer_group
    assert h["dlq.failed_at"].endswith("Z")
    assert ing.calls == 0 and cons.committed == [41]
    assert c.stats.dead_lettered == 1 and c.stats.invalid == 1


def test_offset_is_committed_only_after_the_dlq_publish_is_confirmed():
    c, cons, prod, ing, events, _ = build([kmsg(b"garbage", 7)])
    c.run(max_messages=1)
    assert events[:3] == [("produce", DLQ, "ok"), ("flush",), ("offset_commit", 7)]


def test_prediction_failure_goes_to_the_dlq_with_its_own_reason_and_redacted_error():
    raw = payload()
    err = PredictionError("prediction failed for M001: boom password=hunter2hunter2 " + "x" * 900)
    c, cons, prod, ing, events, _ = build([kmsg(raw, 3)], [err])
    c.run(max_messages=1)
    (call,) = prod.calls
    h = {k: v.decode() for k, v in call["headers"].items()}
    assert call["topic"] == DLQ and call["value"] == raw and h["dlq.reason"] == "prediction_failed"
    assert "hunter2hunter2" not in h["dlq.error"] and len(h["dlq.error"]) <= 500
    assert cons.committed == [3] and c.stats.prediction_errors == 1 and c.stats.dead_lettered == 1


def test_valid_messages_never_touch_the_dlq_and_poison_does_not_block_the_stream():
    msgs = [kmsg(payload(), 0), kmsg(b"poison", 1), kmsg(payload(ts="2026-10-08T10:30:01Z"), 2)]
    c, cons, prod, ing, *_ = build(msgs, [outcome(), outcome()])
    c.run(max_messages=3)
    assert cons.committed == [0, 1, 2] and ing.calls == 2
    assert [x["topic"] for x in prod.calls].count(DLQ) == 1 and prod.to(DLQ)[0]["value"] == b"poison"
    assert c.stats.persisted == 2 and c.stats.dead_lettered == 1


def test_database_failure_is_not_dead_lettered_it_halts():
    c, cons, prod, ing, *_ = build([kmsg(payload(), 1)], [op_err()] * 5)
    with pytest.raises(ProcessingHalted):
        c.run(max_messages=1)
    assert prod.calls == [] and cons.committed == []


# =============================================================================================== DLQ failure handling
def test_transient_dlq_failure_is_retried_with_backoff_then_committed():
    c, cons, prod, ing, events, sleeps = build([kmsg(b"garbage", 9)], plan=lambda t, n: "ok" if n == 3 else "fail")
    c.run(max_messages=1)
    assert [x["mode"] for x in prod.to(DLQ)] == ["fail", "fail", "ok"]
    assert sleeps == [0.5, 1.0] and c.stats.dlq_retries == 2 and c.stats.dead_lettered == 1
    assert cons.committed == [9]
    assert events.index(("produce", DLQ, "ok")) < events.index(("offset_commit", 9))


@pytest.mark.parametrize("mode", ["fail", "raise", "buffer", "timeout"])
def test_permanent_dlq_failure_stops_processing_without_committing(mode, caplog):
    c, cons, prod, ing, events, sleeps = build([kmsg(b"garbage", 5), kmsg(payload(), 6)], [outcome()],
                                               plan=lambda t, n: mode, dlq_retries=2)
    with pytest.raises(ProcessingHalted, match="dead-letter topic.*offset NOT committed"):
        c.run(max_messages=2)
    assert len(prod.to(DLQ)) == 3 and sleeps == [0.5, 1.0]                      # 1 attempt + 2 bounded retries
    assert cons.committed == [] and not any(e[0] == "offset_commit" for e in events)
    assert ing.calls == 0                                                      # the next message was never processed
    assert cons.closed and c.stats.dead_lettered == 0


def test_dlq_retries_are_bounded_and_zero_means_one_attempt():
    c, cons, prod, *_ = build([kmsg(b"garbage", 1)], plan=lambda t, n: "fail", dlq_retries=0)
    with pytest.raises(ProcessingHalted):
        c.run(max_messages=1)
    assert len(prod.to(DLQ)) == 1


def test_after_a_halt_the_same_message_is_redelivered_and_dead_lettered_on_restart():
    down, cons1, prod1, *_ = build([kmsg(b"garbage", 12)], plan=lambda t, n: "raise", dlq_retries=1)
    with pytest.raises(ProcessingHalted):
        down.run(max_messages=1)
    assert cons1.committed == []
    up, cons2, prod2, *_ = build([kmsg(b"garbage", 12)])                          # restart: Kafka redelivers offset 12
    up.run(max_messages=1)
    assert cons2.committed == [12] and prod2.to(DLQ)[0]["value"] == b"garbage"


def test_dlq_timeout_without_report_is_not_treated_as_success():
    c, cons, prod, *_ = build([kmsg(b"garbage", 1)], plan=lambda t, n: "ok" if n == 2 else "timeout")
    c.run(max_messages=1)
    assert [x["mode"] for x in prod.to(DLQ)] == ["timeout", "ok"] and cons.committed == [1]
    assert c.stats.dlq_retries == 1


# =============================================================================================== bounded downstream retries
def test_failed_downstream_event_is_retried_only_for_the_failed_event():
    plan = lambda t, n: "fail" if (t == SETTINGS.prediction_topic and n <= 2) else "ok"
    c, cons, prod, ing, events, sleeps = build([kmsg(payload(OVERSTRAIN), 4)], [outcome(alert=True)], plan=plan)
    c.run(max_messages=1)
    assert len(prod.to(SETTINGS.prediction_topic)) == 3 and len(prod.to(SETTINGS.alert_topic)) == 1
    assert c.stats.publish_retries == 2 and c.stats.publish_errors == 0 and c.stats.published == 2
    assert sleeps == [0.5, 1.0] and cons.committed == [4]
    assert events.index(("offset_commit", 4)) > max(i for i, e in enumerate(events) if e[0] == "produce")


@pytest.mark.parametrize("mode", ["fail", "raise", "buffer", "timeout"])
def test_exhausted_downstream_retries_are_counted_logged_and_offset_still_committed(mode, caplog):
    c, cons, prod, ing, events, sleeps = build([kmsg(payload(OVERSTRAIN), 8)], [outcome(alert=True)],
                                               plan=lambda t, n: mode, publish_retries=2)
    c.run(max_messages=1)
    assert len(prod.calls) == 6 and c.stats.publish_errors == 2 and c.stats.published == 0   # (1+2) attempts x 2 events
    assert c.stats.publish_retries == 4 and sleeps == [0.5, 1.0]
    assert cons.committed == [8] and c.stats.persisted == 1                                   # DB row is kept; offset moves on
    assert "could not publish" in caplog.text and "LOST" in caplog.text


def test_zero_publish_retries_means_a_single_attempt():
    c, cons, prod, *_ = build([kmsg(payload(), 1)], [outcome()], plan=lambda t, n: "fail", publish_retries=0)
    c.run(max_messages=1)
    assert len(prod.calls) == 1 and c.stats.publish_retries == 0 and c.stats.publish_errors == 1


def test_duplicates_are_never_republished():
    from services.ingestion_service import IngestionOutcome
    c, cons, prod, *_ = build([kmsg(payload(), 1)], [IngestionOutcome(duplicate=True)])
    c.run(max_messages=1)
    assert prod.calls == [] and cons.committed == [1] and c.stats.duplicates == 1


# =============================================================================================== real PostgreSQL, fake Kafka
@pytest.mark.integration
@pytest.mark.postgres
def test_poison_message_between_valid_ones_with_real_database(session_factory, predictor):
    msgs = [kmsg(payload(OVERSTRAIN, ts="2026-10-08T10:30:00Z"), 0), kmsg(b'{"torque": "NaN"}', 1),
            kmsg(payload(NORMAL, ts="2026-10-08T10:30:01Z", machine="M002"), 2)]
    events = []
    cons, prod = RecordingConsumer(msgs, events), ScriptedProducer(events)
    c = PersistingSensorConsumer(SETTINGS, IngestionService(session_factory, predictor), consumer=cons, producer=prod)
    c.run(max_messages=3)
    assert cons.committed == [0, 1, 2] and c.stats.persisted == 2 and c.stats.dead_lettered == 1
    assert [x["value"] for x in prod.to(DLQ)] == [b'{"torque": "NaN"}']
    with session_factory() as s:
        counts = [s.execute(text(f"SELECT count(*) FROM {t}")).scalar_one() for t in ("sensor_readings", "predictions", "alerts")]
    assert counts == [2, 2, 1]


@pytest.mark.integration
@pytest.mark.postgres
def test_dlq_outage_halts_and_leaves_valid_data_intact_then_recovers(session_factory, predictor):
    msgs = [kmsg(payload(OVERSTRAIN), 0), kmsg(b"poison", 1)]
    events = []
    cons = RecordingConsumer(msgs, events)
    prod = ScriptedProducer(events, plan=lambda t, n: "raise" if t == DLQ else "ok")
    c = PersistingSensorConsumer(SETTINGS, IngestionService(session_factory, predictor), consumer=cons, producer=prod,
                                 dlq_retries=1, sleep=lambda s: None)
    with pytest.raises(ProcessingHalted):
        c.run(max_messages=2)
    assert cons.committed == [0]                      # the good message was committed, the poison one was NOT
    cons2, prod2 = RecordingConsumer([kmsg(b"poison", 1)], []), ScriptedProducer([])
    PersistingSensorConsumer(SETTINGS, IngestionService(session_factory, predictor), consumer=cons2, producer=prod2).run(max_messages=1)
    assert cons2.committed == [1] and prod2.to(DLQ)[0]["value"] == b"poison"
