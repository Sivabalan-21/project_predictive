"""End-to-end test against a REAL broker: simulator -> producer -> Kafka -> consumer -> PredictionService.

Skipped unless RUN_KAFKA_INTEGRATION=1. The broker address comes from KAFKA_BOOTSTRAP_SERVERS
(default localhost:9092). Topic names and the consumer group are unique per run, so it never touches
the demo topics.

    PowerShell:   $env:RUN_KAFKA_INTEGRATION = "1"; python -m pytest tests/test_kafka_integration.py -v
"""
import dataclasses
import os
import time
import uuid

import pytest
from confluent_kafka import OFFSET_BEGINNING, Consumer, Producer, TopicPartition

from ml.config import KafkaSettings
from simulation.sensor_simulator import SensorSimulator
from streaming.consumer import SensorConsumer
from streaming.kafka_utils import client_config, ensure_topics, wait_for_broker
from streaming.producer import SensorProducer, run_producer
from streaming.schemas import PredictionEvent, SensorEvent

pytestmark = [pytest.mark.integration,
              pytest.mark.skipif(os.environ.get("RUN_KAFKA_INTEGRATION") != "1", reason="set RUN_KAFKA_INTEGRATION=1 and start a broker")]


@pytest.fixture()
def settings():
    run = uuid.uuid4().hex[:8]
    s = dataclasses.replace(KafkaSettings.from_env(), sensor_topic=f"it-sensor-{run}", prediction_topic=f"it-pred-{run}",
                            consumer_group=f"it-group-{run}", topic_partitions=3)
    wait_for_broker(s, timeout_s=30)
    ensure_topics(s, [s.sensor_topic, s.prediction_topic])
    return s


def read_topic(settings, topic, expected, timeout_s=40.0):
    """Read a whole topic from the beginning (manual partition assignment, no group coordination)."""
    c = Consumer(client_config(settings, **{"group.id": f"reader-{uuid.uuid4().hex[:6]}", "enable.auto.commit": False}))
    meta = c.list_topics(topic, timeout=10).topics[topic]
    c.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in meta.partitions])
    out, end = [], time.monotonic() + timeout_s
    while len(out) < expected and time.monotonic() < end:
        m = c.poll(1.0)
        if m is not None and not m.error():
            out.append(m)
    c.close()
    return out


def test_full_pipeline_normal_and_failure_readings(settings, service):
    # 1. producer: 12 ticks x 5 machines; MACHINE-003 gets an overstrain failure injected
    sim = SensorSimulator(5, failure_probability=0.0, seed=7)
    sim.inject("MACHINE-003", "OSF", ramp=3, hold=6, recover=2)
    stats = run_producer(settings, sim, interval_s=0, max_readings=60, producer=SensorProducer(settings))
    assert (stats.enqueued, stats.delivered, stats.failed, stats.invalid) == (60, 60, 0, 0)

    # 2. Kafka really holds the 60 messages (read back independently)
    sensor_msgs = read_topic(settings, settings.sensor_topic, 60)
    assert len(sensor_msgs) == 60
    sensor_events = {(e.machine_id, e.timestamp): e for e in (SensorEvent.from_payload(m.value()) for m in sensor_msgs)}
    assert {k[0] for k in sensor_events} == {f"MACHINE-00{i}" for i in range(1, 6)}
    assert len({m.partition() for m in sensor_msgs}) >= 2          # keyed by machine -> spread over partitions

    # 3. consumer (real group, real PredictionService)
    cstats = SensorConsumer(settings, service).run(max_messages=60, idle_timeout_s=45)
    assert cstats.received == 60 and cstats.invalid == 0 and cstats.prediction_errors == 0
    assert cstats.normal >= 1 and cstats.failures_detected >= 1     # normal AND failure readings processed

    # 4. predictions on the second topic equal what the PredictionService returns directly
    pred_msgs = read_topic(settings, settings.prediction_topic, 60)
    preds = [PredictionEvent.from_payload(m.value()) for m in pred_msgs]
    assert len(preds) == 60
    for p in preds:
        direct = service.predict(sensor_events[(p.machine_id, p.timestamp)].to_reading())
        assert (p.status, p.fault_type, p.model_method) == (direct.final_status, direct.fault_type, direct.decision_method)
        assert p.risk_score == pytest.approx(direct.risk_score)
    failures = [p for p in preds if p.status == "FAILURE"]
    assert failures and all(p.machine_id == "MACHINE-003" for p in failures)
    assert any(p.fault_type and "OSF" in p.fault_type for p in failures)
    assert any(p.status == "NORMAL" and p.machine_id != "MACHINE-003" for p in preds)


def test_invalid_message_is_rejected_and_stream_continues(settings, service):
    raw = Producer(client_config(settings))
    good = SensorEvent.from_dict({"machine_id": "MACHINE-001", "timestamp": "2026-10-06T11:30:01Z", "air_temperature": 300.5,
                                  "process_temperature": 310.8, "rotational_speed": 1512, "torque": 39.8, "tool_wear": 104})
    raw.produce(settings.sensor_topic, key=b"x", value=b"this is not json")
    raw.produce(settings.sensor_topic, key=b"x", value=b'{"machine_id":"MACHINE-001","torque":5000}')
    raw.produce(settings.sensor_topic, key=b"MACHINE-001", value=good.to_json_bytes())
    assert raw.flush(15) == 0
    stats = SensorConsumer(settings, service).run(max_messages=3, idle_timeout_s=45)
    assert (stats.invalid, stats.received) == (2, 1)


def test_consumer_resumes_from_committed_offsets(settings, service):
    def publish(n, seed):
        run_producer(settings, SensorSimulator(5, 0.0, seed=seed), 0, max_readings=n, producer=SensorProducer(settings))

    publish(10, 1)
    first = SensorConsumer(settings, service, publish_predictions=False).run(max_messages=10, idle_timeout_s=45)
    assert first.received == 10
    publish(5, 2)
    second = SensorConsumer(settings, service, publish_predictions=False).run(max_messages=5, idle_timeout_s=45)
    assert second.received == 5          # same group: only the 5 NEW messages, nothing re-processed or skipped
