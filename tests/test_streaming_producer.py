import json
import logging
from datetime import datetime, timezone

import pytest
from confluent_kafka import KafkaError, KafkaException

from ml.config import KafkaSettings
from simulation.sensor_simulator import SensorSimulator, SimulatedReading
from streaming import producer as producer_mod
from streaming.kafka_utils import BrokerUnavailableError, ensure_topics, wait_for_broker
from streaming.producer import PublishError, SensorProducer, run_producer
from streaming.schemas import SensorEvent
from tests.fakes_kafka import FakeProducer

SETTINGS = KafkaSettings()
FIXED = datetime(2026, 10, 6, 11, 30, 1, tzinfo=timezone.utc)
EVENT = SensorEvent.from_dict({"machine_id": "MACHINE-001", "timestamp": "2026-10-06T11:30:01Z", "air_temperature": 300.5,
                               "process_temperature": 310.8, "rotational_speed": 1512, "torque": 39.8, "tool_wear": 104})


def test_publish_sends_json_to_the_sensor_topic_keyed_by_machine(caplog):
    fake = FakeProducer()
    sp = SensorProducer(SETTINGS, producer=fake)
    with caplog.at_level(logging.INFO, logger="streaming.producer"):
        sp.publish(EVENT)
    (sent,) = fake.sent
    assert sent["topic"] == "machine-sensor-data" and sent["key"] == b"MACHINE-001"
    assert SensorEvent.from_payload(sent["value"]) == EVENT
    assert (sp.stats.enqueued, sp.stats.delivered, sp.stats.failed) == (1, 1, 0)
    assert "Published sensor reading for MACHINE-001" in caplog.text


def test_topic_comes_from_settings():
    fake = FakeProducer()
    SensorProducer(KafkaSettings(sensor_topic="custom-topic"), producer=fake).publish(EVENT)
    assert fake.sent[0]["topic"] == "custom-topic"


def test_failed_delivery_is_logged_and_counted(caplog):
    sp = SensorProducer(SETTINGS, producer=FakeProducer(fail_delivery=True))
    with caplog.at_level(logging.ERROR, logger="streaming.producer"):
        sp.publish(EVENT)
    assert sp.stats.failed == 1 and sp.stats.delivered == 0 and "Delivery failed for MACHINE-001" in caplog.text


def test_full_queue_is_retried_then_succeeds():
    fake = FakeProducer(buffer_errors=2)
    sp = SensorProducer(SETTINGS, producer=fake)
    sp.publish(EVENT)
    assert len(fake.sent) == 1 and sp.stats.enqueued == 1


def test_persistently_full_queue_raises_publish_error():
    sp = SensorProducer(SETTINGS, producer=FakeProducer(buffer_errors=99), max_enqueue_retries=3)
    with pytest.raises(PublishError, match="queue stayed full"):
        sp.publish(EVENT)
    assert sp.stats.failed == 1


def test_kafka_exception_becomes_publish_error():
    exc = KafkaException(KafkaError(KafkaError._UNKNOWN_TOPIC))
    sp = SensorProducer(SETTINGS, producer=FakeProducer(produce_exception=exc))
    with pytest.raises(PublishError):
        sp.publish(EVENT)


class StubSimulator:
    """Yields scripted SimulatedReadings (one valid, one invalid)."""

    def __init__(self, readings):
        self.readings = readings

    def stream(self, interval, should_stop=None, **_):
        yield from self.readings


def reading(**kw):
    base = dict(machine_id="MACHINE-001", timestamp=FIXED, air_temperature=300.0, process_temperature=310.0,
                rotational_speed=1500.0, torque=40.0, tool_wear=100.0, product_type="L")
    return SimulatedReading(**{**base, **kw})


def test_run_producer_publishes_valid_and_rejects_invalid_events(caplog):
    fake = FakeProducer()
    sp = SensorProducer(SETTINGS, producer=fake)
    sim = StubSimulator([reading(), reading(torque=999.0), reading(machine_id="MACHINE-002")])
    with caplog.at_level(logging.ERROR, logger="streaming.producer"):
        stats = run_producer(SETTINGS, sim, 0, producer=sp)
    assert [json.loads(m["value"])["machine_id"] for m in fake.sent] == ["MACHINE-001", "MACHINE-002"]
    assert stats.invalid == 1 and stats.delivered == 2
    assert "Invalid sensor event rejected" in caplog.text and "torque" in caplog.text


def test_run_producer_with_real_simulator_publishes_only_schema_valid_json():
    fake = FakeProducer()
    sim = SensorSimulator(5, 0.05, seed=3)
    run_producer(SETTINGS, sim, 0, max_readings=60, producer=SensorProducer(SETTINGS, producer=fake))
    assert len(fake.sent) == 60 and {m["topic"] for m in fake.sent} == {"machine-sensor-data"}
    assert {m["key"].decode() for m in fake.sent} == {f"MACHINE-00{i}" for i in range(1, 6)}
    for m in fake.sent:
        SensorEvent.from_payload(m["value"])
        assert "scenario" not in json.loads(m["value"])     # simulator ground truth never leaves the simulator


def test_run_producer_flushes_on_shutdown_even_after_an_error():
    fake = FakeProducer()

    class Exploding(StubSimulator):
        def stream(self, *a, **k):
            yield reading()
            raise RuntimeError("boom")
    with pytest.raises(RuntimeError):
        run_producer(SETTINGS, Exploding([]), 0, producer=SensorProducer(SETTINGS, producer=fake))
    assert fake.flushed == 1 and len(fake.sent) == 1


def test_run_producer_stops_on_request():
    fake = FakeProducer()
    seen = {"n": 0}

    def stop():
        seen["n"] += 1
        return seen["n"] > 7
    run_producer(SETTINGS, SensorSimulator(5, 0.0, seed=1), 0, should_stop=stop, producer=SensorProducer(SETTINGS, producer=fake))
    assert 0 < len(fake.sent) < 100


# ---------------------------------------------------------------- broker connection handling
class FakeAdmin:
    def __init__(self, fail_times=0, topics=(), created=None):
        self.fail_times, self.topic_names, self.created = fail_times, set(topics), created if created is not None else []

    def list_topics(self, timeout=None):
        if self.fail_times > 0:
            self.fail_times -= 1
            raise KafkaException(KafkaError(KafkaError._TRANSPORT))
        return type("MD", (), {"topics": {t: None for t in self.topic_names}})()

    def create_topics(self, new_topics, request_timeout=None):
        out = {}
        for nt in new_topics:
            self.created.append((nt.topic, nt.num_partitions, nt.replication_factor))
            f = type("F", (), {"result": lambda self, timeout=None: None})()
            out[nt.topic] = f
        return out


def test_wait_for_broker_retries_with_backoff_then_connects(caplog):
    admin, sleeps = FakeAdmin(fail_times=3), []
    with caplog.at_level(logging.WARNING, logger="streaming.kafka"):
        wait_for_broker(SETTINGS, timeout_s=60, admin_factory=lambda cfg: admin, sleep=sleeps.append)
    assert len(sleeps) == 3 and sleeps[0] < sleeps[2] and caplog.text.count("Kafka connection unavailable") == 3


def test_wait_for_broker_gives_up_after_timeout():
    t = {"now": 0.0}
    clock = lambda: t["now"]  # noqa: E731

    def sleep(s):
        t["now"] += s
    with pytest.raises(BrokerUnavailableError, match="not reachable"):
        wait_for_broker(SETTINGS, timeout_s=5, admin_factory=lambda cfg: FakeAdmin(fail_times=999), sleep=sleep, clock=clock)


def test_wait_for_broker_returns_immediately_when_asked_to_stop():
    wait_for_broker(SETTINGS, admin_factory=lambda cfg: FakeAdmin(fail_times=999), should_stop=lambda: True)


def test_ensure_topics_creates_only_missing_topics():
    created = []
    admin = FakeAdmin(topics=["machine-sensor-data"], created=created)
    out = ensure_topics(KafkaSettings(topic_partitions=4), ["machine-sensor-data", "machine-predictions"], admin_factory=lambda c: admin)
    assert out == ["machine-predictions"] and created == [("machine-predictions", 4, 1)]


# ---------------------------------------------------------------- CLI exit codes
def test_main_reports_unreachable_broker_with_exit_code_3(monkeypatch):
    def refuse(*a, **k):
        raise BrokerUnavailableError("down")
    monkeypatch.setattr(producer_mod, "wait_for_broker", refuse)
    assert producer_mod.main(["--max-readings", "1", "--connect-timeout", "1"]) == 3


def test_main_rejects_invalid_configuration_with_exit_code_2(monkeypatch):
    monkeypatch.setenv("KAFKA_TOPIC_PARTITIONS", "abc")
    assert producer_mod.main(["--max-readings", "1"]) == 2
    monkeypatch.delenv("KAFKA_TOPIC_PARTITIONS")
    assert producer_mod.main(["--machines", "0"]) == 2


def test_main_wires_everything_together_without_a_broker(monkeypatch):
    calls = {}
    monkeypatch.setattr(producer_mod, "wait_for_broker", lambda *a, **k: calls.setdefault("waited", True))
    monkeypatch.setattr(producer_mod, "ensure_topics", lambda s, topics: calls.setdefault("topics", topics))
    monkeypatch.setattr(producer_mod, "SensorProducer", lambda settings: SensorProducer(settings, producer=FakeProducer()))
    monkeypatch.setenv("KAFKA_SENSOR_TOPIC", "env-topic")
    assert producer_mod.main(["--interval", "0", "--max-readings", "10", "--seed", "1", "--bootstrap-servers", "h:1"]) == 0
    assert calls == {"waited": True, "topics": ["env-topic"]}
