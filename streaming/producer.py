"""Kafka producer: runs the sensor simulator and publishes validated events to ``machine-sensor-data``.

Recommended command (the producer contains the simulator):
    python -m streaming.producer --interval 1 --machines 5 --failure-probability 0.005 --seed 42

Delivery model: keyed by machine_id (readings of one machine stay ordered within a partition), acks=all,
at-least-once (a retry after a lost acknowledgement can duplicate a message; downstream consumers should
treat (machine_id, timestamp) as the identity of a reading).
"""
from __future__ import annotations

import argparse
import dataclasses
import functools
import logging
import signal
import sys
from dataclasses import dataclass
from typing import Any, Callable, Sequence

from confluent_kafka import KafkaException, Producer

from ml.config import KafkaSettings
from simulation.sensor_simulator import SensorSimulator
from streaming.kafka_utils import BrokerUnavailableError, client_config, ensure_topics, wait_for_broker
from streaming.logging_setup import configure_logging
from streaming.schemas import EventValidationError, SensorEvent

log = logging.getLogger("streaming.producer")


class PublishError(RuntimeError):
    """An event could not be handed to Kafka."""


@dataclass
class ProducerStats:
    enqueued: int = 0
    delivered: int = 0
    failed: int = 0
    invalid: int = 0


class SensorProducer:
    """Thin, testable wrapper around a confluent_kafka.Producer (inject ``producer`` to mock Kafka)."""

    def __init__(self, settings: KafkaSettings, producer: Any | None = None, max_enqueue_retries: int = 5):
        self.settings = settings
        self.stats = ProducerStats()
        self._max_retries = max_enqueue_retries
        self._producer = producer if producer is not None else Producer(client_config(
            settings, **{"acks": "all", "linger.ms": 5, "delivery.timeout.ms": 30_000,
                         "retry.backoff.ms": 200, "compression.type": "none"}))

    def publish(self, event: SensorEvent, note: str = "") -> None:
        """Enqueue one validated event. Delivery is confirmed asynchronously (see _on_delivery)."""
        payload = event.to_json_bytes()
        callback = functools.partial(self._on_delivery, event.machine_id, note)
        for attempt in range(1, self._max_retries + 1):
            try:
                self._producer.produce(self.settings.sensor_topic, key=event.machine_id.encode(),
                                       value=payload, on_delivery=callback)
                break
            except BufferError:                      # local queue full: serve callbacks, then retry
                log.warning("Producer queue full (attempt %d/%d) - waiting for deliveries", attempt, self._max_retries)
                self._producer.poll(0.5)
            except KafkaException as exc:
                self.stats.failed += 1
                raise PublishError(f"could not publish reading for {event.machine_id}: {exc}") from exc
        else:
            self.stats.failed += 1
            raise PublishError(f"producer queue stayed full; reading for {event.machine_id} dropped")
        self.stats.enqueued += 1
        self._producer.poll(0)                       # serve delivery callbacks

    def _on_delivery(self, machine_id: str, note: str, err: Any, msg: Any) -> None:
        if err is not None:
            self.stats.failed += 1
            log.error("Delivery failed for %s: %s", machine_id, err)
            return
        self.stats.delivered += 1
        log.info("Published sensor reading for %s (partition=%s offset=%s)%s", machine_id, msg.partition(),
                 msg.offset(), f" {note}" if note else "")

    def flush(self, timeout_s: float = 10.0) -> int:
        """Wait for outstanding deliveries. Returns the number of messages still undelivered."""
        return self._producer.flush(timeout_s)


def run_producer(settings: KafkaSettings, simulator: SensorSimulator, interval_s: float,
                 max_readings: int | None = None, should_stop: Callable[[], bool] = lambda: False,
                 producer: SensorProducer | None = None) -> ProducerStats:
    """Generate readings, validate, publish. Returns when stopped, or after ``max_readings`` readings."""
    sp = producer or SensorProducer(settings)
    sent = 0
    try:
        for reading in simulator.stream(interval_s, should_stop=should_stop):
            try:
                event = SensorEvent.from_dict(reading.to_event_dict())
            except EventValidationError as exc:
                sp.stats.invalid += 1
                log.error("Invalid sensor event rejected (not published) for %s: %s", reading.machine_id, exc)
                continue
            note = f"[simulated scenario: {reading.scenario}/{reading.phase}]" if reading.scenario else ""
            try:
                sp.publish(event, note)
            except PublishError as exc:
                log.error("%s", exc)
            sent += 1
            if max_readings is not None and sent >= max_readings:
                break
    finally:
        remaining = sp.flush(10.0)
        if remaining:
            log.warning("%d message(s) were not delivered before shutdown", remaining)
        s = sp.stats
        log.info("Producer stopped: enqueued=%d delivered=%d failed=%d invalid=%d", s.enqueued, s.delivered, s.failed, s.invalid)
    return sp.stats


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="python -m streaming.producer",
                                 description="Run the (simulated) sensor feed and publish it to Kafka.")
    ap.add_argument("--interval", type=float, default=1.0, help="seconds between readings of each machine (default 1.0)")
    ap.add_argument("--machines", type=int, default=5, help="number of simulated machines (default 5)")
    ap.add_argument("--failure-probability", type=float, default=0.005,
                    help="per-machine, per-reading probability of a failure scenario (default 0.005)")
    ap.add_argument("--degradation-rate", type=float, default=0.3, help="simulated tool-wear minutes per reading (default 0.3)")
    ap.add_argument("--seed", type=int, default=None, help="random seed for reproducible simulation")
    ap.add_argument("--max-readings", type=int, default=None, help="stop after N readings (default: run until Ctrl+C)")
    ap.add_argument("--bootstrap-servers", default=None, help="overrides KAFKA_BOOTSTRAP_SERVERS")
    ap.add_argument("--topic", default=None, help="overrides KAFKA_SENSOR_TOPIC")
    ap.add_argument("--connect-timeout", type=float, default=60.0, help="seconds to wait for the broker (default 60)")
    ap.add_argument("--no-create-topics", action="store_true", help="do not create missing topics")
    ap.add_argument("--log-level", default="INFO")
    return ap


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    configure_logging(args.log_level)
    try:
        settings = KafkaSettings.from_env()
        overrides = {k: v for k, v in (("bootstrap_servers", args.bootstrap_servers), ("sensor_topic", args.topic)) if v}
        settings = dataclasses.replace(settings, **overrides)
        settings.validate()
        simulator = SensorSimulator(args.machines, args.failure_probability, args.degradation_rate, args.seed)
    except ValueError as exc:
        log.error("Invalid configuration: %s", exc)
        return 2

    stopping = False

    def _stop(signum, _frame):
        nonlocal stopping
        if not stopping:
            log.info("Shutdown requested - finishing in-flight messages")
        stopping = True
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    log.info("Producer starting: %d simulated machines -> topic '%s' on %s (SIMULATED DATA, not real sensors)",
             args.machines, settings.sensor_topic, settings.bootstrap_servers)
    try:
        wait_for_broker(settings, args.connect_timeout, should_stop=lambda: stopping)
        if not args.no_create_topics and not stopping:
            ensure_topics(settings, [settings.sensor_topic])
    except BrokerUnavailableError as exc:
        log.error("%s", exc)
        return 3
    except KafkaException as exc:
        log.error("Kafka error during startup: %s", exc)
        return 3
    if stopping:
        return 0
    run_producer(settings, simulator, args.interval, args.max_readings, should_stop=lambda: stopping)
    return 0


if __name__ == "__main__":
    sys.exit(main())
