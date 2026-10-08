"""Kafka consumer: reads ``machine-sensor-data``, calls the Phase 1 PredictionService, publishes results to
``machine-predictions``.

    python -m streaming.consumer

There is NO machine-learning code in this module: every prediction is made by
``ml.prediction.PredictionService`` (the single source of truth for models, thresholds and fault rules).

Delivery model: at-least-once. Offsets are committed only after the results of the messages read so far were
flushed to the prediction topic, so a crash re-processes (never skips) messages. A message that fails schema
validation or prediction is logged, counted and skipped (committed) so one bad message cannot block the stream;
a dead-letter topic is a future improvement.
"""
from __future__ import annotations

import argparse
import dataclasses
import functools
import logging
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Sequence

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from ml.config import KafkaSettings
from streaming.kafka_utils import BrokerUnavailableError, client_config, ensure_topics, wait_for_broker
from streaming.logging_setup import configure_logging
from streaming.schemas import EventValidationError, PredictionEvent, SensorEvent

log = logging.getLogger("streaming.consumer")


class PredictionFailure(RuntimeError):
    """The PredictionService failed on a schema-valid event."""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ConsumerStats:
    received: int = 0
    predicted: int = 0
    normal: int = 0
    failures_detected: int = 0
    invalid: int = 0
    prediction_errors: int = 0
    published: int = 0
    publish_errors: int = 0
    total_latency_s: float = 0.0

    @property
    def mean_latency_ms(self) -> float:
        return 1000.0 * self.total_latency_s / self.predicted if self.predicted else 0.0


class PredictionProcessor:
    """Kafka-free core: raw message bytes -> PredictionEvent. Unit-testable without a broker."""

    def __init__(self, service: Any, clock: Callable[[], datetime] = _utcnow):
        self._service = service
        self._clock = clock

    def process(self, payload: bytes | str) -> tuple[SensorEvent, PredictionEvent]:
        """Raises EventValidationError (bad message) or PredictionFailure (service error)."""
        event = SensorEvent.from_payload(payload)
        try:
            result = self._service.predict(event.to_reading())   # <- the ONLY place a prediction is made
        except Exception as exc:  # noqa: BLE001 - re-raised with context, never swallowed
            raise PredictionFailure(f"prediction failed for {event.machine_id} @ {event.timestamp}: {exc}") from exc
        return event, PredictionEvent.from_result(event, result, self._clock())


class SensorConsumer:
    def __init__(self, settings: KafkaSettings, service: Any, consumer: Any | None = None,
                 producer: Any | None = None, publish_predictions: bool = True, commit_every: int = 10,
                 commit_interval_s: float = 2.0, stats_interval_s: float = 30.0):
        self.settings = settings
        self.stats = ConsumerStats()
        self._processor = PredictionProcessor(service)
        self._publish = publish_predictions
        self._commit_every, self._commit_interval = commit_every, commit_interval_s
        self._stats_interval = stats_interval_s
        self._consumer = consumer if consumer is not None else Consumer(client_config(
            settings, **{"group.id": settings.consumer_group, "enable.auto.commit": False,
                         "auto.offset.reset": settings.auto_offset_reset, "enable.partition.eof": False}))
        self._producer = None
        if publish_predictions:
            self._producer = producer if producer is not None else Producer(client_config(
                settings, **{"acks": "all", "linger.ms": 5, "delivery.timeout.ms": 30_000}))
        self._uncommitted = 0

    # ------------------------------------------------------------------ one message
    def handle_message(self, msg: Any) -> None:
        where = f"{msg.topic()}[{msg.partition()}]@{msg.offset()}"
        started = time.perf_counter()
        try:
            event, prediction = self._processor.process(msg.value())
        except EventValidationError as exc:
            self.stats.invalid += 1
            raw = msg.value()
            preview = raw[:200].decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else str(raw)[:200]
            log.error("Invalid sensor event at %s rejected: %s | payload=%s", where, exc, preview)
            return
        except PredictionFailure as exc:
            self.stats.prediction_errors += 1
            log.error("%s (at %s) - message skipped", exc, where, exc_info=exc.__cause__)
            return

        self.stats.received += 1
        self.stats.predicted += 1
        self.stats.total_latency_s += time.perf_counter() - started
        log.info("Received sensor reading %s @ %s (partition=%s offset=%s)", event.machine_id,
                 event.timestamp.isoformat(timespec="milliseconds"), msg.partition(), msg.offset())
        if prediction.status == "FAILURE":
            self.stats.failures_detected += 1
            log.warning("Prediction generated for %s: FAILURE risk_score=%.2f fault_type=%s method=%s",
                        event.machine_id, prediction.risk_score, prediction.fault_type, prediction.model_method)
        else:
            self.stats.normal += 1
            log.info("Prediction generated for %s: NORMAL risk_score=%.2f method=%s", event.machine_id,
                     prediction.risk_score, prediction.model_method)
        self._publish_prediction(prediction)

    def _publish_prediction(self, prediction: PredictionEvent) -> None:
        if self._producer is None:
            return
        try:
            self._producer.produce(self.settings.prediction_topic, key=prediction.machine_id.encode(),
                                   value=prediction.to_json_bytes(),
                                   on_delivery=functools.partial(self._on_prediction_delivery, prediction.machine_id))
            self._producer.poll(0)
        except (BufferError, KafkaException) as exc:
            self.stats.publish_errors += 1
            log.error("Could not publish prediction for %s: %s", prediction.machine_id, exc)

    def _on_prediction_delivery(self, machine_id: str, err: Any, msg: Any) -> None:
        if err is not None:
            self.stats.publish_errors += 1
            log.error("Prediction delivery failed for %s: %s", machine_id, err)
        else:
            self.stats.published += 1
            log.debug("Prediction for %s published (partition=%s offset=%s)", machine_id, msg.partition(), msg.offset())

    # ------------------------------------------------------------------ offsets
    def _commit(self) -> None:
        if self._uncommitted == 0:
            return
        if self._producer is not None:
            remaining = self._producer.flush(10.0)       # results must be out before offsets move
            if remaining:
                log.warning("%d prediction(s) not yet delivered at commit time", remaining)
        try:
            self._consumer.commit(asynchronous=False)
            self._uncommitted = 0
        except KafkaException as exc:
            if exc.args and exc.args[0].code() == KafkaError._NO_OFFSET:
                self._uncommitted = 0
            else:
                log.error("Offset commit failed (messages will be re-processed after a restart): %s", exc)

    # ------------------------------------------------------------------ main loop
    def run(self, should_stop: Callable[[], bool] = lambda: False, max_messages: int | None = None,
            idle_timeout_s: float | None = None) -> ConsumerStats:
        assigned_at: list[float] = []      # filled when the group join completes

        def on_assign(_c: Any, partitions: Any) -> None:
            if not assigned_at:
                assigned_at.append(time.monotonic())
            log.info("Assigned partitions: %s", [p.partition for p in partitions])

        def on_revoke(_c: Any, partitions: Any) -> None:
            log.info("Partitions revoked: %s", [p.partition for p in partitions])
            self._commit()

        self._consumer.subscribe([self.settings.sensor_topic], on_assign=on_assign, on_revoke=on_revoke)
        log.info("Consumer subscribed to '%s' (group '%s')", self.settings.sensor_topic, self.settings.consumer_group)
        last_commit = last_stats = last_message = last_wait_warning = time.monotonic()
        handled = 0
        try:
            while not should_stop():
                msg = self._consumer.poll(1.0)
                now = time.monotonic()
                if msg is None:
                    if not assigned_at:        # group join still in progress (can take 3-25 s depending on the broker)
                        if now - last_wait_warning >= 15.0:
                            log.warning("Still waiting for partition assignment (consumer group join in progress)")
                            last_wait_warning = now
                    elif idle_timeout_s is not None and now - max(last_message, assigned_at[0]) > idle_timeout_s:
                        log.info("No messages for %.0fs after assignment - stopping", idle_timeout_s)
                        break
                elif msg.error():
                    err = msg.error()
                    if err.code() == KafkaError._PARTITION_EOF:
                        continue
                    if err.fatal():
                        raise KafkaException(err)
                    log.warning("Kafka consumer error (will retry): %s", err)
                else:
                    last_message = now
                    self.handle_message(msg)
                    handled += 1
                    self._uncommitted += 1
                    if max_messages is not None and handled >= max_messages:
                        break
                if self._uncommitted and (self._uncommitted >= self._commit_every or now - last_commit >= self._commit_interval):
                    self._commit()
                    last_commit = now
                if now - last_stats >= self._stats_interval:
                    self._log_stats()
                    last_stats = now
        finally:
            self._commit()
            if self._producer is not None:
                self._producer.flush(10.0)
            self._consumer.close()
            self._log_stats(final=True)
        return self.stats

    def _log_stats(self, final: bool = False) -> None:
        s = self.stats
        log.info("%s: received=%d normal=%d failures=%d invalid=%d prediction_errors=%d published=%d "
                 "publish_errors=%d mean_processing_ms=%.2f", "Consumer stopped" if final else "Stats", s.received,
                 s.normal, s.failures_detected, s.invalid, s.prediction_errors, s.published, s.publish_errors,
                 s.mean_latency_ms)


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="python -m streaming.consumer",
                                 description="Consume sensor events, predict machine health, publish predictions.")
    ap.add_argument("--bootstrap-servers", default=None, help="overrides KAFKA_BOOTSTRAP_SERVERS")
    ap.add_argument("--topic", default=None, help="overrides KAFKA_SENSOR_TOPIC")
    ap.add_argument("--prediction-topic", default=None, help="overrides KAFKA_PREDICTION_TOPIC")
    ap.add_argument("--group", default=None, help="overrides KAFKA_CONSUMER_GROUP")
    ap.add_argument("--offset-reset", choices=("earliest", "latest"), default=None,
                    help="where a NEW group starts reading (overrides KAFKA_AUTO_OFFSET_RESET)")
    ap.add_argument("--max-messages", type=int, default=None, help="stop after N messages")
    ap.add_argument("--idle-timeout", type=float, default=None,
                    help="stop after N seconds without messages (counted from partition assignment)")
    ap.add_argument("--no-publish-predictions", action="store_true", help="log predictions only; do not use the prediction topic")
    ap.add_argument("--connect-timeout", type=float, default=60.0, help="seconds to wait for the broker (default 60)")
    ap.add_argument("--log-level", default="INFO")
    return ap


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    configure_logging(args.log_level)
    try:
        settings = KafkaSettings.from_env()
        mapping = {"bootstrap_servers": args.bootstrap_servers, "sensor_topic": args.topic,
                   "prediction_topic": args.prediction_topic, "consumer_group": args.group,
                   "auto_offset_reset": args.offset_reset}
        settings = dataclasses.replace(settings, **{k: v for k, v in mapping.items() if v})
        settings.validate()
    except ValueError as exc:
        log.error("Invalid configuration: %s", exc)
        return 2

    stopping = False

    def _stop(signum, _frame):
        nonlocal stopping
        if not stopping:
            log.info("Shutdown requested - committing offsets and closing")
        stopping = True
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        from ml.prediction import ModelLoadError, get_prediction_service
        try:
            service = get_prediction_service()
        except ModelLoadError as exc:
            log.error("Model artifacts unavailable: %s", exc)
            return 4
        log.info("PredictionService ready (decision method: %s, model %s)", service.decision_method,
                 service.bundle.model_version)
        wait_for_broker(settings, args.connect_timeout, should_stop=lambda: stopping)
        topics = [settings.sensor_topic] + ([] if args.no_publish_predictions else [settings.prediction_topic])
        ensure_topics(settings, topics)
    except BrokerUnavailableError as exc:
        log.error("%s", exc)
        return 3
    except KafkaException as exc:
        log.error("Kafka error during startup: %s", exc)
        return 3
    if stopping:
        return 0
    SensorConsumer(settings, service, publish_predictions=not args.no_publish_predictions).run(
        should_stop=lambda: stopping, max_messages=args.max_messages, idle_timeout_s=args.idle_timeout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
