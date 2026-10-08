"""Kafka connection helpers: client configuration, waiting for the broker, creating topics."""
from __future__ import annotations

import logging
import random
import time
from typing import Any, Callable, Sequence

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from ml.config import KafkaSettings

log = logging.getLogger("streaming.kafka")
kafka_client_log = logging.getLogger("kafka")


class BrokerUnavailableError(RuntimeError):
    """The Kafka broker could not be reached within the allowed time."""


def _on_client_error(err: KafkaError) -> None:
    # Called by librdkafka for connection-level problems (e.g. all brokers down). Retries happen inside the client.
    log.warning("Kafka connection unavailable: %s", err)


def client_config(settings: KafkaSettings, **extra: Any) -> dict[str, Any]:
    """Base librdkafka config. No credentials here: add SASL/SSL options via ``extra`` when needed."""
    return {"bootstrap.servers": settings.bootstrap_servers, "logger": kafka_client_log,
            "error_cb": _on_client_error, **extra}


def wait_for_broker(settings: KafkaSettings, timeout_s: float = 60.0, initial_backoff_s: float = 1.0,
                    max_backoff_s: float = 10.0, admin_factory: Callable[[dict], Any] = AdminClient,
                    sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic,
                    should_stop: Callable[[], bool] = lambda: False) -> None:
    """Block until the broker answers a metadata request; retry with exponential backoff.

    Raises BrokerUnavailableError when ``timeout_s`` elapses (or returns quietly if ``should_stop``).
    """
    deadline = clock() + timeout_s
    backoff, attempt = initial_backoff_s, 0
    while not should_stop():
        attempt += 1
        try:
            admin = admin_factory({"bootstrap.servers": settings.bootstrap_servers, "logger": kafka_client_log})
            admin.list_topics(timeout=5)
            log.info("Connected to Kafka at %s", settings.bootstrap_servers)
            return
        except (KafkaException, OSError) as exc:
            remaining = deadline - clock()
            if remaining <= 0:
                raise BrokerUnavailableError(
                    f"Kafka broker at {settings.bootstrap_servers} not reachable after {attempt} attempts: {exc}") from exc
            delay = min(backoff, max_backoff_s, remaining) * random.uniform(0.9, 1.1)
            log.warning("Kafka connection unavailable at %s (attempt %d): %s - retrying in %.1fs",
                        settings.bootstrap_servers, attempt, exc, delay)
            sleep(delay)
            backoff = min(backoff * 2, max_backoff_s)


def ensure_topics(settings: KafkaSettings, topics: Sequence[str],
                  admin_factory: Callable[[dict], Any] = AdminClient, timeout_s: float = 15.0) -> list[str]:
    """Create any missing topics (single-broker local defaults: replication factor 1). Returns created names."""
    admin = admin_factory({"bootstrap.servers": settings.bootstrap_servers, "logger": kafka_client_log})
    existing = set(admin.list_topics(timeout=timeout_s).topics)
    missing = [t for t in dict.fromkeys(topics) if t not in existing]
    created: list[str] = []
    if not missing:
        return created
    futures = admin.create_topics([NewTopic(t, num_partitions=settings.topic_partitions, replication_factor=1)
                                   for t in missing], request_timeout=timeout_s)
    for topic, future in futures.items():
        try:
            future.result(timeout_s)
            created.append(topic)
            log.info("Created topic %s (%d partitions)", topic, settings.topic_partitions)
        except KafkaException as exc:
            if exc.args and exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                continue
            raise
    return created
