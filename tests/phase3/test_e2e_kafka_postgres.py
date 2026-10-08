"""END-TO-END against REAL Kafka + REAL PostgreSQL (needs docker compose or equivalent).

    docker compose up -d
    alembic upgrade head            # only for the dev DB; this test migrates its own temporary database
    $env:RUN_KAFKA_INTEGRATION = "1"
    python -m pytest tests/phase3/test_e2e_kafka_postgres.py -v

Skipped unless RUN_KAFKA_INTEGRATION=1 (and a PostgreSQL is available, see tests/phase3/conftest.py).
Flow: producer -> Kafka(sensor topic) -> PersistingSensorConsumer -> PredictionService -> PostgreSQL
      -> Kafka(predictions, alerts) ; then the SAME event is published again (duplicate delivery).
"""
import dataclasses
import json
import os
import time
import uuid

import pytest
from confluent_kafka import OFFSET_BEGINNING, Consumer, Producer, TopicPartition
from sqlalchemy import text

from ml.config import KafkaSettings
from services.ingestion_service import IngestionService
from streaming.db_consumer import PersistingSensorConsumer
from streaming.kafka_utils import client_config, ensure_topics, wait_for_broker
from tests.phase3.conftest import OVERSTRAIN

pytestmark = [pytest.mark.integration, pytest.mark.postgres,
              pytest.mark.skipif(os.environ.get("RUN_KAFKA_INTEGRATION") != "1", reason="set RUN_KAFKA_INTEGRATION=1 and start Kafka")]


@pytest.fixture()
def settings():
    run = uuid.uuid4().hex[:8]
    s = dataclasses.replace(KafkaSettings.from_env(), sensor_topic=f"e2e-sensor-{run}", prediction_topic=f"e2e-pred-{run}",
                            alert_topic=f"e2e-alert-{run}", consumer_group=f"e2e-group-{run}", topic_partitions=1)
    wait_for_broker(s, timeout_s=30)
    ensure_topics(s, [s.sensor_topic, s.prediction_topic, s.alert_topic])
    return s


def read_topic(settings, topic, expected, timeout_s=30.0):
    c = Consumer(client_config(settings, **{"group.id": f"r-{uuid.uuid4().hex[:6]}", "enable.auto.commit": False}))
    parts = c.list_topics(topic, timeout=10).topics[topic].partitions
    c.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in parts])
    out, end = [], time.monotonic() + timeout_s
    while len(out) < expected and time.monotonic() < end:
        m = c.poll(1.0)
        if m is not None and not m.error():
            out.append(json.loads(m.value()))
    c.close()
    return out


def committed_offset(settings, topic):
    c = Consumer(client_config(settings, **{"group.id": settings.consumer_group}))
    try:
        return c.committed([TopicPartition(topic, 0)], timeout=10)[0].offset
    finally:
        c.close()


def test_kafka_to_postgres_with_duplicate_delivery(settings, session_factory, predictor):
    event = {"event_id": uuid.uuid4().hex, "machine_id": "M001", "timestamp": "2026-10-08T10:30:00Z", **OVERSTRAIN}
    prod = Producer(client_config(settings))
    for _ in range(2):                                  # same event twice = duplicate delivery
        prod.produce(settings.sensor_topic, key=b"M001", value=json.dumps(event).encode())
    prod.flush(10)

    consumer = PersistingSensorConsumer(settings, IngestionService(session_factory, predictor))
    stats = consumer.run(max_messages=2, idle_timeout_s=30)

    assert stats.persisted == 1 and stats.duplicates == 1 and stats.alerts_created == 1
    with session_factory() as s:
        counts = [s.execute(text(f"SELECT count(*) FROM {t}")).scalar_one() for t in ("sensor_readings", "predictions", "alerts")]
    assert counts == [1, 1, 1]
    preds = read_topic(settings, settings.prediction_topic, 1)
    alerts = read_topic(settings, settings.alert_topic, 1)
    assert len(preds) == 1 and preds[0]["status"] == "FAILURE" and len(alerts) == 1 and alerts[0]["status"] == "ACTIVE"
    assert committed_offset(settings, settings.sensor_topic) == 2        # both offsets committed, after the DB commit
