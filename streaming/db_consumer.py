"""Persisting Kafka consumer (Phase 3): sensor-readings -> PostgreSQL (+ predictions / alerts topics).

    python -m streaming.db_consumer

Per message, strictly in this order:

    validate -> DB transaction (reading + prediction + alert) -> COMMIT -> publish downstream -> commit Kafka offset

* The offset of a message is committed ONLY after its database transaction committed. A crash before that makes
  Kafka redeliver the message; the database makes the redelivery harmless (ON CONFLICT DO NOTHING).
* Invalid / un-predictable messages are logged and skipped (offset committed) - they can never succeed.
* Duplicates are skipped (offset committed), no second reading/prediction/alert and no downstream re-publish.
* Database failures: retried with backoff; if they persist the consumer STOPS without committing the offset
  (exit code 5) so the message is redelivered after restart. It never skips a message it could not persist.
* Downstream publishing (predictions / alerts topics) is best-effort after the commit: PostgreSQL is the source
  of truth. A failure is logged and counted (see docs/kafka.md "Known limitations").

All ML work happens in ml.prediction.PredictionService (called from services.ingestion_service).
"""
from __future__ import annotations

import argparse
import dataclasses
import logging
import signal
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Sequence

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from db.config import ConfigurationError, DatabaseSettings
from db.session import make_engine, make_session_factory
from ml.config import KafkaSettings
from services.ingestion_service import IngestionService, PredictionError
from streaming.kafka_utils import BrokerUnavailableError, client_config, ensure_topics, wait_for_broker
from streaming.logging_setup import configure_logging
from streaming.schemas import EventValidationError, SensorEvent

log = logging.getLogger("streaming.db_consumer")


class ProcessingHalted(RuntimeError):
    """A message could not be persisted. The offset was NOT committed; restart to retry (redelivery is safe)."""


@dataclass
class DbConsumerStats:
    received: int = 0
    persisted: int = 0
    duplicates: int = 0
    invalid: int = 0
    prediction_errors: int = 0
    alerts_created: int = 0
    published: int = 0
    publish_errors: int = 0
    db_retries: int = 0
    offsets_committed: int = 0


class PersistingSensorConsumer:
    def __init__(self, settings: KafkaSettings, ingestion: IngestionService, consumer: Any | None = None,
                 producer: Any | None = None, db_retries: int = 3, db_backoff_s: float = 1.0,
                 sleep: Callable[[float], None] = time.sleep, flush_timeout_s: float = 10.0):
        self.settings, self.stats = settings, DbConsumerStats()
        self._ingestion, self._retries, self._backoff = ingestion, db_retries, db_backoff_s
        self._sleep, self._flush_timeout = sleep, flush_timeout_s
        self._consumer = consumer if consumer is not None else Consumer(client_config(
            settings, **{"group.id": settings.consumer_group, "enable.auto.commit": False,
                         "auto.offset.reset": settings.auto_offset_reset, "enable.partition.eof": False}))
        self._producer = producer if producer is not None else Producer(client_config(
            settings, **{"acks": "all", "enable.idempotence": True, "linger.ms": 5, "delivery.timeout.ms": 30_000}))

    # ------------------------------------------------------------------ one message
    def handle_message(self, msg: Any) -> None:
        """Process one message and commit its offset only when that is safe. Raises ProcessingHalted otherwise."""
        where = f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"
        self.stats.received += 1
        try:
            event = SensorEvent.from_payload(msg.value())
        except EventValidationError as exc:
            self.stats.invalid += 1
            raw = msg.value()
            preview = raw[:200].decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else str(raw)[:200]
            log.error("invalid sensor event at %s skipped: %s | payload=%s", where, exc, preview)
            self._commit_offset(msg)
            return

        outcome = self._ingest_with_retry(event, where)           # raises ProcessingHalted on persistent DB failure
        if outcome is None:                                       # un-predictable message: skip, can never succeed
            self._commit_offset(msg)
            return
        if outcome.duplicate:
            self.stats.duplicates += 1
        else:
            self.stats.persisted += 1
            log.info("persisted machine_id=%s event_id=%s reading_id=%s status=%s risk_score=%.3f", event.machine_id,
                     event.event_id, outcome.sensor_reading_id, outcome.prediction.status, outcome.prediction.risk_score)
            self._publish(self.settings.prediction_topic, event.machine_id, outcome.prediction.to_json_bytes(), event)
            if outcome.alert is not None:
                self.stats.alerts_created += 1
                log.warning("ALERT created alert_id=%s machine_id=%s fault_type=%s risk_score=%.3f", outcome.alert.alert_id,
                            event.machine_id, outcome.alert.fault_type, outcome.alert.risk_score)
                self._publish(self.settings.alert_topic, event.machine_id, outcome.alert.to_json_bytes(), event)
            self._flush()
        self._commit_offset(msg)                                  # <- only now, after the DB COMMIT

    def _ingest_with_retry(self, event: SensorEvent, where: str):
        attempt = 0
        while True:
            try:
                return self._ingestion.ingest(event)
            except PredictionError as exc:
                self.stats.prediction_errors += 1
                log.error("%s (at %s) - message skipped, transaction rolled back", exc, where, exc_info=exc.__cause__)
                return None
            except OperationalError as exc:                       # connection lost / server restarting: transient
                attempt += 1
                self.stats.db_retries += 1
                if attempt > self._retries:
                    raise ProcessingHalted(
                        f"database unavailable after {self._retries} retries at {where}; offset NOT committed") from exc
                delay = self._backoff * 2 ** (attempt - 1)
                log.warning("database error at %s (attempt %d/%d), retrying in %.1fs: %s", where, attempt,
                            self._retries, delay, exc.__class__.__name__)
                self._sleep(delay)
            except SQLAlchemyError as exc:                        # integrity/data error = a bug, not transient
                raise ProcessingHalted(f"non-retryable database error at {where}; offset NOT committed: {exc}") from exc

    # ------------------------------------------------------------------ downstream + offsets
    def _publish(self, topic: str, key: str, value: bytes, event: SensorEvent) -> None:
        try:
            self._producer.produce(topic, key=key.encode(), value=value,
                                   on_delivery=lambda err, m, t=topic: self._on_delivery(t, key, err))
            self._producer.poll(0)
        except (BufferError, KafkaException) as exc:
            self.stats.publish_errors += 1
            log.error("could not publish to %s machine_id=%s event_id=%s: %s", topic, key, event.event_id, exc)

    def _on_delivery(self, topic: str, key: str, err: Any) -> None:
        if err is not None:
            self.stats.publish_errors += 1
            log.error("delivery to %s failed machine_id=%s: %s", topic, key, err)
        else:
            self.stats.published += 1

    def _flush(self) -> None:
        remaining = self._producer.flush(self._flush_timeout)
        if remaining:
            log.warning("%d downstream event(s) not delivered before offset commit", remaining)

    def _commit_offset(self, msg: Any) -> None:
        try:
            self._consumer.commit(message=msg, asynchronous=False)
            self.stats.offsets_committed += 1
        except KafkaException as exc:
            if exc.args and exc.args[0].code() == KafkaError._NO_OFFSET:
                return
            # Not fatal for correctness: the message will be redelivered and treated as a duplicate.
            log.error("offset commit failed (message will be redelivered; idempotent): %s", exc)

    # ------------------------------------------------------------------ main loop
    def run(self, should_stop: Callable[[], bool] = lambda: False, max_messages: int | None = None,
            idle_timeout_s: float | None = None) -> DbConsumerStats:
        self._consumer.subscribe([self.settings.sensor_topic])
        log.info("consumer subscribed topic=%s group=%s", self.settings.sensor_topic, self.settings.consumer_group)
        handled, last_message = 0, time.monotonic()
        try:
            while not should_stop():
                msg = self._consumer.poll(1.0)
                if msg is None:
                    if idle_timeout_s is not None and time.monotonic() - last_message > idle_timeout_s:
                        log.info("no messages for %.0fs - stopping", idle_timeout_s)
                        break
                    continue
                if msg.error():
                    err = msg.error()
                    if err.code() == KafkaError._PARTITION_EOF:
                        continue
                    if err.fatal():
                        raise KafkaException(err)
                    log.warning("kafka consumer error (will retry): %s", err)
                    continue
                last_message = time.monotonic()
                self.handle_message(msg)
                handled += 1
                if max_messages is not None and handled >= max_messages:
                    break
        finally:
            self._producer.flush(self._flush_timeout)
            self._consumer.close()                  # no extra commit: only per-message post-DB commits move offsets
            log.info("consumer stopped %s", dataclasses.asdict(self.stats))
        return self.stats


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="python -m streaming.db_consumer",
                                 description="Consume sensor events, predict, persist to PostgreSQL, publish downstream.")
    ap.add_argument("--max-messages", type=int, default=None)
    ap.add_argument("--idle-timeout", type=float, default=None)
    ap.add_argument("--connect-timeout", type=float, default=60.0)
    ap.add_argument("--db-retries", type=int, default=3)
    ap.add_argument("--log-level", default=None, help="default: LOG_LEVEL env or INFO")
    return ap


def main(argv: Sequence[str] | None = None) -> int:
    import os
    args = build_arg_parser().parse_args(argv)
    configure_logging(args.log_level or os.environ.get("LOG_LEVEL", "INFO"))
    try:
        settings = KafkaSettings.from_env()
        db_settings = DatabaseSettings.from_env()
    except (ValueError, ConfigurationError) as exc:
        log.error("invalid configuration: %s", exc)
        return 2
    stopping = False

    def _stop(_s, _f):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    try:
        from ml.prediction import ModelLoadError, get_prediction_service
        try:
            predictor = get_prediction_service()
        except ModelLoadError as exc:
            log.error("model artifacts unavailable: %s", exc)
            return 4
        wait_for_broker(settings, args.connect_timeout, should_stop=lambda: stopping)
        ensure_topics(settings, [settings.sensor_topic, settings.prediction_topic, settings.alert_topic])
    except (BrokerUnavailableError, KafkaException) as exc:
        log.error("kafka unavailable: %s", exc)
        return 3
    if stopping:
        return 0
    engine = make_engine(db_settings)
    log.info("database %s", db_settings.safe_url())
    ingestion = IngestionService(make_session_factory(engine), predictor)
    try:
        PersistingSensorConsumer(settings, ingestion, db_retries=args.db_retries).run(
            should_stop=lambda: stopping, max_messages=args.max_messages, idle_timeout_s=args.idle_timeout)
    except ProcessingHalted as exc:
        log.error("%s", exc)
        return 5
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main())
