"""Persisting Kafka consumer (Phase 3, hardened in Phase 4): sensor-readings -> PostgreSQL (+ predictions / alerts topics).

    python -m streaming.db_consumer

Per message, strictly in this order:

    validate -> DB transaction (reading + prediction + alert + outbox intents) -> COMMIT -> commit Kafka offset

* The offset of a message is committed ONLY after it was fully handled: persisted, recognised as a duplicate, or
  safely published to the dead-letter topic. A crash before that makes Kafka redeliver the message; the database makes
  the redelivery harmless (ON CONFLICT DO NOTHING).
* Poison messages (invalid JSON / schema, or a message the PredictionService cannot score) can never succeed on retry.
  They are published to the DEAD-LETTER topic (default ``sensor-readings.dlq``) with the ORIGINAL message bytes untouched
  and the failure reason, source topic, partition and offset in Kafka headers. Publishing is confirmed by a delivery
  report and retried a bounded number of times. If the DLQ publish still fails the consumer STOPS (exit code 5) WITHOUT
  committing that offset, so nothing is ever dropped silently.
* Duplicates are skipped (offset committed), no second reading/prediction/alert and no downstream re-publish.
* Database failures: retried with backoff; if they persist the consumer STOPS without committing the offset
  (exit code 5) so the message is redelivered after restart. It never skips a message it could not persist.
* The independent ``streaming.outbox_dispatcher`` publishes committed prediction / alert intents.  The consumer never
  waits for downstream Kafka delivery, so committing its input offset cannot lose a committed downstream intent.

All ML work happens in ml.prediction.PredictionService (called from services.ingestion_service).
"""
from __future__ import annotations

import argparse
import dataclasses
import ipaddress
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Sequence

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from core.settings import AppSettings
from db.config import ConfigurationError, DatabaseSettings
from db.session import make_engine, make_session_factory
from ml.config import KafkaSettings
from services.ingestion_service import IngestionService, PredictionError
from observability.metrics import ConsumerMetrics, MetricsServer
from streaming.kafka_utils import BrokerUnavailableError, client_config, ensure_topics, wait_for_broker
from streaming.logging_setup import configure_logging, redact_text
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
    publish_errors: int = 0            # downstream events that could NOT be published after all retries
    publish_retries: int = 0           # downstream re-publish attempts (bounded)
    db_retries: int = 0
    offsets_committed: int = 0
    dead_lettered: int = 0             # messages confirmed published to the DLQ
    dlq_retries: int = 0               # DLQ re-publish attempts (bounded)


DLQ_REASON_INVALID = "invalid_schema"
DLQ_REASON_PREDICTION = "prediction_failed"
_MAX_ERROR_CHARS = 500


@dataclass
class _Downstream:
    """One event to publish after the DB commit; ``delivered`` flips when the broker confirms it."""
    kind: str
    topic: str
    key: str
    value: bytes
    delivered: bool = False


class PersistingSensorConsumer:
    def __init__(self, settings: KafkaSettings, ingestion: IngestionService, consumer: Any | None = None,
                 producer: Any | None = None, db_retries: int = 3, db_backoff_s: float = 1.0,
                 sleep: Callable[[float], None] = time.sleep, flush_timeout_s: float = 10.0,
                 publish_retries: int = 3, dlq_retries: int = 3, publish_backoff_s: float = 0.5,
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
                 metrics: ConsumerMetrics | None = None):
        self.settings, self.stats = settings, DbConsumerStats()
        self._ingestion, self._retries, self._backoff = ingestion, db_retries, db_backoff_s
        self._sleep, self._flush_timeout = sleep, flush_timeout_s
        self._publish_retries, self._dlq_retries, self._publish_backoff = publish_retries, dlq_retries, publish_backoff_s
        self._clock = clock
        self.metrics = metrics or ConsumerMetrics()
        self._last_prediction_error = ""
        self._consumer = consumer if consumer is not None else Consumer(client_config(
            settings, **{"group.id": settings.consumer_group, "enable.auto.commit": False,
                         "auto.offset.reset": settings.auto_offset_reset, "enable.partition.eof": False}))
        self._producer = producer if producer is not None else Producer(client_config(
            settings, **{"acks": "all", "enable.idempotence": True, "linger.ms": 5, "delivery.timeout.ms": 30_000}))

    # ------------------------------------------------------------------ one message
    def handle_message(self, msg: Any) -> None:
        """Process one message and commit its offset only when that is safe. Raises ProcessingHalted otherwise."""
        started = time.perf_counter()
        try:
            self._handle(msg)
        except ProcessingHalted:
            self.metrics.messages.labels(outcome="halted").inc()
            raise
        finally:
            self.metrics.processing.observe(time.perf_counter() - started)

    def _handle(self, msg: Any) -> None:
        where = f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"
        self.stats.received += 1
        try:
            event = SensorEvent.from_payload(msg.value())
        except EventValidationError as exc:
            self.stats.invalid += 1
            raw = msg.value()
            preview = raw[:200].decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else str(raw)[:200]
            log.error("invalid sensor event at %s -> dead-letter topic: %s | payload=%s", where, exc, preview)
            self._dead_letter(msg, DLQ_REASON_INVALID, str(exc))      # raises ProcessingHalted if it cannot be stored
            self._commit_offset(msg)
            self.metrics.messages.labels(outcome="dead_lettered").inc()
            return

        outcome = self._ingest_with_retry(event, where)           # raises ProcessingHalted on persistent DB failure
        if outcome is None:                                       # un-predictable message: can never succeed
            self._dead_letter(msg, DLQ_REASON_PREDICTION, self._last_prediction_error)
            self._commit_offset(msg)
            self.metrics.messages.labels(outcome="dead_lettered").inc()
            return
        if outcome.duplicate:
            self.stats.duplicates += 1
            self.metrics.messages.labels(outcome="duplicate").inc()
        else:
            self.stats.persisted += 1
            self.metrics.messages.labels(outcome="persisted").inc()
            log.info("persisted machine_id=%s event_id=%s reading_id=%s status=%s risk_score=%.3f", event.machine_id,
                     event.event_id, outcome.sensor_reading_id, outcome.prediction.status, outcome.prediction.risk_score)
            if outcome.alert is not None:
                self.stats.alerts_created += 1
                self.metrics.alerts_created.inc()
                log.warning("ALERT created alert_id=%s machine_id=%s fault_type=%s risk_score=%.3f", outcome.alert.alert_id,
                            event.machine_id, outcome.alert.fault_type, outcome.alert.risk_score)
            log.info("outbox intents committed reading_id=%s; dispatcher will publish downstream", outcome.sensor_reading_id)
        self._commit_offset(msg)                                  # <- only after durable DB transaction (including outbox)

    def _ingest_with_retry(self, event: SensorEvent, where: str):
        attempt = 0
        while True:
            try:
                return self._ingestion.ingest(event)
            except PredictionError as exc:
                self.stats.prediction_errors += 1
                self._last_prediction_error = str(exc)
                log.error("%s (at %s) - transaction rolled back, message -> dead-letter topic", exc, where,
                          exc_info=exc.__cause__)
                return None
            except OperationalError as exc:                       # connection lost / server restarting: transient
                attempt += 1
                self.stats.db_retries += 1
                self.metrics.db_retries.inc()
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
    def _publish_downstream(self, items: list[_Downstream], event: SensorEvent) -> None:
        """Publish after the DB commit. Each undelivered item is retried up to ``publish_retries`` times (exponential
        backoff). An item still undelivered afterwards is logged and counted - it does NOT stop the consumer, because the
        database already holds the data and the redelivered message would only be treated as a duplicate."""
        pending = items
        for attempt in range(self._publish_retries + 1):
            if attempt:
                self.stats.publish_retries += len(pending)
                self.metrics.publish_retries.inc(len(pending))
                delay = self._publish_backoff * 2 ** (attempt - 1)
                log.warning("retrying %d downstream event(s) in %.1fs (attempt %d/%d)", len(pending), delay, attempt,
                            self._publish_retries)
                self._sleep(delay)
            for item in pending:
                self._produce(item)
            self._flush()
            for done in (i for i in pending if i.delivered):
                self.metrics.downstream.labels(kind=done.kind, result="success").inc()
            pending = [i for i in pending if not i.delivered]
            if not pending:
                return
        for item in pending:
            self.stats.publish_errors += 1
            self.metrics.downstream.labels(kind=item.kind, result="failure").inc()
            log.error("could not publish to %s machine_id=%s event_id=%s after %d attempts - the database row exists but "
                      "this downstream event is LOST (no outbox)", item.topic, item.key, event.event_id,
                      self._publish_retries + 1)

    def _produce(self, item: _Downstream) -> None:
        def on_delivery(err: Any, _msg: Any, it: _Downstream = item) -> None:
            if err is None:
                it.delivered = True
                self.stats.published += 1
            else:
                log.warning("delivery to %s failed machine_id=%s: %s", it.topic, it.key, err)
        try:
            self._producer.produce(item.topic, key=item.key.encode(), value=item.value, on_delivery=on_delivery)
            self._producer.poll(0)
        except (BufferError, KafkaException) as exc:
            log.warning("produce to %s failed machine_id=%s: %s", item.topic, item.key, exc)

    # ------------------------------------------------------------------ dead-letter topic
    def _dead_letter(self, msg: Any, reason: str, error: str) -> None:
        """Publish the ORIGINAL message to the DLQ and wait for the broker's confirmation (bounded retries).
        Raises ProcessingHalted when it cannot be confirmed; the caller then does NOT commit the offset."""
        headers = [(k, v.encode("utf-8")) for k, v in (
            ("dlq.reason", reason), ("dlq.error", redact_text(error)[:_MAX_ERROR_CHARS]),
            ("dlq.source.topic", msg.topic()), ("dlq.source.partition", str(msg.partition())),
            ("dlq.source.offset", str(msg.offset())), ("dlq.consumer_group", self.settings.consumer_group),
            ("dlq.failed_at", self._clock().isoformat(timespec="milliseconds").replace("+00:00", "Z")))]
        where = f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"
        last_error: Any = None
        for attempt in range(self._dlq_retries + 1):
            if attempt:
                self.stats.dlq_retries += 1
                self.metrics.dlq_retries.inc()
                delay = self._publish_backoff * 2 ** (attempt - 1)
                log.warning("dead-letter publish for %s failed (%s) - retry %d/%d in %.1fs", where, last_error, attempt,
                            self._dlq_retries, delay)
                self._sleep(delay)
            result: dict[str, Any] = {}
            try:
                self._producer.produce(self.settings.dlq_topic, key=msg.key(), value=msg.value(), headers=headers,
                                       on_delivery=lambda err, _m, r=result: r.update(done=True, err=err))
                self._producer.poll(0)
                self._producer.flush(self._flush_timeout)
            except (BufferError, KafkaException) as exc:
                last_error = exc
                continue
            if result.get("done") and result.get("err") is None:
                self.stats.dead_lettered += 1
                self.metrics.dlq_messages.labels(reason=reason).inc()
                log.warning("message %s published to %s (reason=%s)", where, self.settings.dlq_topic, reason)
                return
            last_error = result.get("err") or "delivery not confirmed within the flush timeout"
        self.metrics.dlq_failures.inc()
        raise ProcessingHalted(f"could not publish {where} to the dead-letter topic {self.settings.dlq_topic} after "
                               f"{self._dlq_retries + 1} attempts ({last_error}); offset NOT committed")

    def _flush(self) -> None:
        remaining = self._producer.flush(self._flush_timeout)
        if remaining:
            log.warning("%d downstream event(s) not delivered before offset commit", remaining)

    def _commit_offset(self, msg: Any) -> None:
        try:
            self._consumer.commit(message=msg, asynchronous=False)
            self.stats.offsets_committed += 1
            self.metrics.offsets_committed.inc()
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
                self.metrics.heartbeat()
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
    ap.add_argument("--publish-retries", type=int, default=3, help="retries per downstream event (predictions/alerts)")
    ap.add_argument("--dlq-retries", type=int, default=3, help="retries to confirm a dead-letter publish before halting")
    ap.add_argument("--log-level", default=None, help="default: LOG_LEVEL env or INFO")
    return ap


def _metrics_endpoint(env: Mapping[str, str]) -> tuple[str, int | None]:
    """``CONSUMER_METRICS_ADDR`` (default 127.0.0.1 = loopback only) and ``CONSUMER_METRICS_PORT`` (default 9101; 0 disables)."""
    addr = (env.get("CONSUMER_METRICS_ADDR") or "127.0.0.1").strip()
    try:
        ipaddress.ip_address(addr)
        port = int((env.get("CONSUMER_METRICS_PORT") or "9101").strip())
    except ValueError as exc:
        raise ConfigurationError("CONSUMER_METRICS_ADDR must be an IP address and CONSUMER_METRICS_PORT an integer") from exc
    if not 0 <= port <= 65535:
        raise ConfigurationError("CONSUMER_METRICS_PORT must be 0 (disabled) or 1-65535")
    return addr, (port or None)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    configure_logging(args.log_level or os.environ.get("LOG_LEVEL", "INFO"))
    try:
        AppSettings.from_env(require_jwt=False)          # APP_ENV validity + "no placeholder DB password in production"
        settings = KafkaSettings.from_env()
        db_settings = DatabaseSettings.from_env()
        metrics_addr, metrics_port = _metrics_endpoint(os.environ)
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
        ensure_topics(settings, [settings.sensor_topic, settings.prediction_topic, settings.alert_topic,
                                 settings.dlq_topic])
    except (BrokerUnavailableError, KafkaException) as exc:
        log.error("kafka unavailable: %s", exc)
        return 3
    if stopping:
        return 0
    engine = make_engine(db_settings)
    log.info("database %s", db_settings.safe_url())
    ingestion = IngestionService(make_session_factory(engine), predictor,
                                 outbox_topics={"prediction": settings.prediction_topic, "alert": settings.alert_topic})
    metrics = ConsumerMetrics()
    server = MetricsServer(metrics, metrics_addr, metrics_port).start() if metrics_port else None
    try:
        PersistingSensorConsumer(settings, ingestion, metrics=metrics, db_retries=args.db_retries, publish_retries=args.publish_retries,
                                 dlq_retries=args.dlq_retries).run(
            should_stop=lambda: stopping, max_messages=args.max_messages, idle_timeout_s=args.idle_timeout)
    except ProcessingHalted as exc:
        log.error("%s", exc)
        return 5
    finally:
        if server:
            server.stop()
        engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main())
