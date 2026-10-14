
"""END-TO-END against REAL Kafka + REAL PostgreSQL.

Requirements:
    docker compose up -d
    $env:RUN_KAFKA_INTEGRATION = "1"
    python -m pytest tests/phase3/test_e2e_kafka_postgres.py -v

The test migrates its own temporary PostgreSQL database.

Flow:
    Producer -> Kafka sensor topic -> PersistingSensorConsumer
    -> IngestionService -> PostgreSQL + outbox
    -> OutboxDispatcher -> Kafka prediction and alert topics
    -> Duplicate delivery verification
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
from streaming.outbox_dispatcher import OutboxDispatcher
from tests.phase3.conftest import OVERSTRAIN


pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    pytest.mark.skipif(
        os.environ.get("RUN_KAFKA_INTEGRATION") != "1",
        reason="set RUN_KAFKA_INTEGRATION=1 and start Kafka",
    ),
]


@pytest.fixture()
def settings():
    run = uuid.uuid4().hex[:8]

    settings = dataclasses.replace(
        KafkaSettings.from_env(),
        sensor_topic=f"e2e-sensor-{run}",
        prediction_topic=f"e2e-pred-{run}",
        alert_topic=f"e2e-alert-{run}",
        consumer_group=f"e2e-group-{run}",
        topic_partitions=1,
    )

    wait_for_broker(settings, timeout_s=30)
    ensure_topics(
        settings,
        [
            settings.sensor_topic,
            settings.prediction_topic,
            settings.alert_topic,
        ],
    )

    return settings


def read_topic(settings, topic, expected, timeout_s=30.0):
    consumer = Consumer(
        client_config(
            settings,
            **{
                "group.id": f"r-{uuid.uuid4().hex[:8]}",
                "enable.auto.commit": False,
            },
        )
    )

    try:
        metadata = consumer.list_topics(topic, timeout=10)
        partitions = metadata.topics[topic].partitions

        consumer.assign(
            [
                TopicPartition(topic, partition, OFFSET_BEGINNING)
                for partition in partitions
            ]
        )

        messages = []
        deadline = time.monotonic() + timeout_s

        while len(messages) < expected and time.monotonic() < deadline:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                continue

            messages.append(json.loads(message.value()))

        return messages

    finally:
        consumer.close()


def committed_offset(settings, topic):
    consumer = Consumer(
        client_config(settings, **{"group.id": settings.consumer_group})
    )

    try:
        offsets = consumer.committed(
            [TopicPartition(topic, 0)],
            timeout=10,
        )
        return offsets[0].offset

    finally:
        consumer.close()


def test_kafka_to_postgres_with_duplicate_delivery(
    settings,
    session_factory,
    predictor,
):
    # Publish the same event twice to verify idempotent ingestion.
    event = {
        "event_id": uuid.uuid4().hex,
        "machine_id": "M001",
        "timestamp": "2026-10-08T10:30:00Z",
        **OVERSTRAIN,
    }

    input_producer = Producer(client_config(settings))

    for _ in range(2):
        input_producer.produce(
            settings.sensor_topic,
            key=b"M001",
            value=json.dumps(event).encode(),
        )

    remaining = input_producer.flush(10)
    assert remaining == 0, "Input producer did not flush all messages"

    # IMPORTANT: route outbox events to this test's unique Kafka topics.
    ingestion = IngestionService(
        session_factory,
        predictor,
        outbox_topics={
            "prediction": settings.prediction_topic,
            "alert": settings.alert_topic,
        },
    )

    consumer = PersistingSensorConsumer(settings, ingestion)
    stats = consumer.run(max_messages=2, idle_timeout_s=30)

    assert stats.persisted == 1
    assert stats.duplicates == 1
    assert stats.alerts_created == 1

    # Verify the database transaction persisted one reading, prediction,
    # and alert despite receiving the same event twice.
    with session_factory() as session:
        counts = [
            session.execute(
                text(f"SELECT count(*) FROM {table}")
            ).scalar_one()
            for table in (
                "sensor_readings",
                "predictions",
                "alerts",
            )
        ]

    assert counts == [1, 1, 1]

    # The consumer writes outbox records; the dispatcher publishes them
    # to Kafka. Use the SAME temporary test database session factory.
    output_producer = Producer(client_config(settings))
    dispatcher = OutboxDispatcher(session_factory, output_producer)

    dispatched = dispatcher.dispatch_once()
    remaining = output_producer.flush(10)

    assert remaining == 0, "Output producer did not flush all messages"
    assert dispatched == 2, (
        f"Expected to dispatch 2 outbox events (prediction + alert); "
        f"dispatched {dispatched}"
    )

    # Verify that the outbox events reached the expected unique topics.
    predictions = read_topic(
        settings,
        settings.prediction_topic,
        expected=1,
    )
    alerts = read_topic(
        settings,
        settings.alert_topic,
        expected=1,
    )

    assert len(predictions) == 1
    assert predictions[0]["status"] == "FAILURE"

    assert len(alerts) == 1
    assert alerts[0]["status"] == "ACTIVE"

    # Both input records should have been committed after DB persistence.
    assert committed_offset(settings, settings.sensor_topic) == 2
