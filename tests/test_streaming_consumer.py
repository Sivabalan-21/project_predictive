import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest
from confluent_kafka import KafkaError, KafkaException

from ml.config import KafkaSettings
from streaming import consumer as consumer_mod
from streaming.consumer import PredictionFailure, PredictionProcessor, SensorConsumer
from streaming.schemas import EventValidationError, PredictionEvent
from tests.conftest import NORMAL, OVERSTRAIN
from tests.fakes_kafka import FakeConsumer, FakeKafkaMessage, FakeProducer

SETTINGS = KafkaSettings()


def event_bytes(machine="MACHINE-001", ts="2026-10-06T11:30:01Z", **sensors):
    return json.dumps({"machine_id": machine, "timestamp": ts, **{**NORMAL, **sensors}}).encode()


def make(service, script, **kw):
    events = []
    cons, prod = FakeConsumer(script, events), FakeProducer(events)
    sc = SensorConsumer(SETTINGS, service, consumer=cons, producer=prod, **kw)
    return sc, cons, prod, events


# ---------------------------------------------------------------- Kafka-free core
def test_processor_returns_prediction_event_from_the_real_service(service):
    event, pe = PredictionProcessor(service).process(event_bytes())
    direct = service.predict(NORMAL)
    assert pe.machine_id == "MACHINE-001" and pe.status == direct.final_status == "NORMAL"
    assert pe.risk_score == direct.risk_score and pe.model_method == service.decision_method
    assert pe.timestamp == event.timestamp and pe.fault_type is None


def test_processor_flags_overstrain_with_fault_type(service):
    _, pe = PredictionProcessor(service).process(event_bytes(**OVERSTRAIN, product_type="H"))
    assert pe.status == "FAILURE" and "OSF" in pe.fault_type


def test_processor_rejects_invalid_messages(service):
    for bad in (b"not json", event_bytes(torque=999), b'{"machine_id": "M"}'):
        with pytest.raises(EventValidationError):
            PredictionProcessor(service).process(bad)


def test_processor_wraps_service_errors():
    class Broken:
        def predict(self, reading):
            raise RuntimeError("model exploded")
    with pytest.raises(PredictionFailure, match="model exploded"):
        PredictionProcessor(Broken()).process(event_bytes())


def test_processor_calls_the_prediction_service_exactly_once_per_event():
    calls = []

    class Stub:
        def predict(self, reading):
            calls.append(reading)
            return type("R", (), dict(final_status="NORMAL", risk_score=0.1, fault_type=None, decision_method="stub",
                                      fault_indicators=(), rf_prediction=0, xgb_prediction=0, isolation_prediction=0,
                                      model_version="v"))()
    PredictionProcessor(Stub()).process(event_bytes())
    assert calls == [{"air_temperature": 300.5, "process_temperature": 310.8, "rotational_speed": 1512.0,
                      "torque": 39.8, "tool_wear": 104.0}]


def test_architecture_rule_no_ml_code_in_the_streaming_package():
    """Phase 2 must not contain a second ML implementation."""
    forbidden = re.compile(r"\b(import|from)\s+(sklearn|xgboost|joblib|imblearn|pickle)\b|\.predict_proba\(|\.fit\(")
    for path in Path(consumer_mod.__file__).parent.glob("*.py"):
        assert not forbidden.search(path.read_text()), f"ML code found in {path.name}"


# ---------------------------------------------------------------- consumer loop
def test_consumer_processes_publishes_and_commits(service, caplog):
    script = [FakeKafkaMessage(event_bytes(), offset=0),
              FakeKafkaMessage(event_bytes("MACHINE-003", **OVERSTRAIN, product_type="H"), offset=1, partition=2)]
    sc, cons, prod, _ = make(service, script)
    with caplog.at_level(logging.INFO, logger="streaming.consumer"):
        stats = sc.run(max_messages=2)
    assert (stats.received, stats.normal, stats.failures_detected, stats.published) == (2, 1, 1, 2)
    assert cons.subscribed == ["machine-sensor-data"] and cons.closed and cons.commits >= 1
    out = [PredictionEvent.from_payload(m["value"]) for m in prod.sent]
    assert {m["topic"] for m in prod.sent} == {"machine-predictions"}
    assert [m["key"] for m in prod.sent] == [b"MACHINE-001", b"MACHINE-003"]
    assert [(p.machine_id, p.status) for p in out] == [("MACHINE-001", "NORMAL"), ("MACHINE-003", "FAILURE")]
    assert "Received sensor reading MACHINE-001" in caplog.text
    assert "Prediction generated for MACHINE-001: NORMAL" in caplog.text
    assert "Prediction generated for MACHINE-003: FAILURE" in caplog.text and "fault_type=" in caplog.text


def test_predictions_are_flushed_before_offsets_are_committed(service):
    sc, cons, prod, events = make(service, [FakeKafkaMessage(event_bytes(), offset=i) for i in range(3)], commit_every=100)
    sc.run(max_messages=3)
    commits = [i for i, e in enumerate(events) if e == "consumer.commit"]
    assert commits and all(events[i - 1] == "producer.flush" for i in commits)   # results out first, then offsets move


def test_commits_happen_every_n_messages(service):
    sc, cons, _, _ = make(service, [FakeKafkaMessage(event_bytes(), offset=i) for i in range(6)], commit_every=2)
    sc.run(max_messages=6)
    assert cons.commits >= 3


def test_invalid_and_failing_messages_are_skipped_not_fatal(service, caplog):
    class FlakyService:
        def __init__(self):
            self.n = 0

        def predict(self, reading):
            self.n += 1
            if self.n == 2:
                raise RuntimeError("transient model error")
            return service.predict(reading)
    script = [FakeKafkaMessage(event_bytes(), offset=0), FakeKafkaMessage(b"garbage", offset=1),
              FakeKafkaMessage(event_bytes(torque=999), offset=2), FakeKafkaMessage(event_bytes("MACHINE-002"), offset=3),
              FakeKafkaMessage(event_bytes("MACHINE-004"), offset=4)]
    sc, cons, prod, _ = make(FlakyService(), script)
    with caplog.at_level(logging.ERROR, logger="streaming.consumer"):
        stats = sc.run(max_messages=5)
    assert (stats.received, stats.invalid, stats.prediction_errors) == (2, 2, 1)
    assert len(prod.sent) == 2 and cons.commits >= 1
    assert "Invalid sensor event at machine-sensor-data[0]@1 rejected" in caplog.text and "garbage" in caplog.text


def test_partition_eof_and_transient_errors_do_not_stop_the_consumer(service, caplog):
    eof = FakeKafkaMessage(None, error=KafkaError(KafkaError._PARTITION_EOF))
    transient = FakeKafkaMessage(None, error=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))
    sc, *_ = make(service, [eof, transient, FakeKafkaMessage(event_bytes())])
    with caplog.at_level(logging.WARNING, logger="streaming.consumer"):
        stats = sc.run(max_messages=1)
    assert stats.received == 1 and "Kafka consumer error" in caplog.text


def test_fatal_kafka_error_propagates_but_resources_are_closed(service):
    fatal = KafkaError(KafkaError._FATAL, fatal=True)
    sc, cons, *_ = make(service, [FakeKafkaMessage(None, error=fatal)])
    with pytest.raises(KafkaException):
        sc.run()
    assert cons.closed


def test_stops_on_request_and_on_idle_timeout(service):
    sc, cons, *_ = make(service, [])
    sc.run(should_stop=lambda: True)
    assert cons.closed
    sc2, cons2, *_ = make(service, [FakeKafkaMessage(event_bytes())])
    stats = sc2.run(idle_timeout_s=0.05)
    assert stats.received == 1 and cons2.closed


def test_idle_timeout_does_not_fire_while_the_group_join_is_still_in_progress(service):
    """Regression: a slow group join (3-25 s depending on the broker) must not count as idle time."""
    cons = FakeConsumer([FakeKafkaMessage(event_bytes())], assign_after_polls=40)   # ~40 empty polls before assignment
    sc = SensorConsumer(SETTINGS, service, consumer=cons, publish_predictions=False)
    stats = sc.run(max_messages=1, idle_timeout_s=0.001)
    assert stats.received == 1


def test_can_run_without_the_prediction_topic(service):
    cons = FakeConsumer([FakeKafkaMessage(event_bytes())])
    sc = SensorConsumer(SETTINGS, service, consumer=cons, publish_predictions=False)
    stats = sc.run(max_messages=1)
    assert stats.received == 1 and stats.published == 0


def test_prediction_delivery_failures_are_counted(service, caplog):
    cons, prod = FakeConsumer([FakeKafkaMessage(event_bytes())]), FakeProducer(fail_delivery=True)
    sc = SensorConsumer(SETTINGS, service, consumer=cons, producer=prod)
    with caplog.at_level(logging.ERROR, logger="streaming.consumer"):
        stats = sc.run(max_messages=1)
    assert stats.publish_errors == 1 and "Prediction delivery failed" in caplog.text


# ---------------------------------------------------------------- CLI
def test_main_exit_codes(monkeypatch, service):
    monkeypatch.setenv("KAFKA_TOPIC_PARTITIONS", "x")
    assert consumer_mod.main(["--max-messages", "1"]) == 2
    monkeypatch.delenv("KAFKA_TOPIC_PARTITIONS")

    def refuse(*a, **k):
        raise consumer_mod.BrokerUnavailableError("down")
    monkeypatch.setattr(consumer_mod, "wait_for_broker", refuse)
    assert consumer_mod.main(["--connect-timeout", "1"]) == 3
