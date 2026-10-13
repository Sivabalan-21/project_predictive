
"""Transactional outbox relay: PostgreSQL is durable first, Kafka publication is at-least-once.

The dispatcher leases rows in a short PostgreSQL transaction, waits for Kafka outside any DB transaction, then opens a
second short transaction to record the result. A crash after Kafka acknowledgement but before ``mark_delivered`` can
publish a duplicate; consumers must use the ``event_id`` Kafka header for idempotency where they have business effects.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from confluent_kafka import KafkaException, Producer
from sqlalchemy.orm import Session, sessionmaker

from db.config import DatabaseSettings
from db.repositories import OutboxRepository
from db.session import make_engine, make_session_factory
from observability.metrics import MetricsServer, OutboxMetrics
from streaming.kafka_utils import client_config, wait_for_broker
from streaming.logging_setup import configure_logging, redact_text
from ml.config import KafkaSettings

log = logging.getLogger("streaming.outbox_dispatcher")


@dataclass(frozen=True)
class LeasedEvent:
    id: str
    event_type: str
    schema_version: int
    topic: str
    key: str
    payload: dict


class OutboxDispatcher:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        producer: Any,
        *,
        repository: OutboxRepository | None = None,
        owner: str | None = None,
        batch_size: int = 50,
        poll_interval_s: float = 1.0,
        lease_seconds: float = 60.0,
        max_attempts: int = 20,
        retry_delay_s: float = 5.0,
        flush_timeout_s: float = 10.0,
        metrics: OutboxMetrics | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._sf = session_factory
        self._producer = producer
        self._repo = repository or OutboxRepository()
        self.owner = owner or f"{socket.gethostname()}-{uuid.uuid4().hex[:12]}"
        self.batch_size = batch_size
        self.poll_interval_s = poll_interval_s
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.retry_delay_s = retry_delay_s
        self.flush_timeout_s = flush_timeout_s
        self.metrics = metrics or OutboxMetrics()
        self._sleep = sleep

    def dispatch_once(self) -> int:
        with self._sf() as session, session.begin():
            claimed = self._repo.claim(
                session,
                owner=self.owner,
                limit=self.batch_size,
                lease_seconds=self.lease_seconds,
            )
            events = [
                LeasedEvent(
                    r.id,
                    r.event_type,
                    r.schema_version,
                    r.topic,
                    r.message_key,
                    r.payload,
                )
                for r in claimed
            ]

        for event in events:
            self._dispatch(event)

        self._refresh_backlog()
        return len(events)

    def _dispatch(self, event: LeasedEvent) -> None:
        delivered: dict[str, Any] = {}

        try:
            self._producer.produce(
                event.topic,
                key=event.key.encode("utf-8"),
                value=json.dumps(
                    event.payload,
                    separators=(",", ":"),
                    ensure_ascii=True,
                ).encode("utf-8"),
                headers=[
                    ("event_id", event.id.encode()),
                    ("event_type", event.event_type.encode()),
                    ("schema_version", str(event.schema_version).encode()),
                ],
                on_delivery=lambda err, _msg: delivered.update(
                    done=True,
                    error=err,
                ),
            )

            remaining = self._producer.flush(self.flush_timeout_s)

            error = (
                delivered.get("error")
                if delivered.get("done")
                else f"delivery not confirmed (remaining={remaining})"
            )

            if error is None:
                with self._sf() as session, session.begin():
                    if self._repo.mark_delivered(
                        session,
                        event_id=event.id,
                        owner=self.owner,
                    ):
                        self.metrics.publications.labels(
                            kind=event.event_type,
                            result="success",
                        ).inc()
                        log.info(
                            "outbox event delivered event_type=%s event_id=%s",
                            event.event_type,
                            event.id,
                        )
                return

        except (BufferError, KafkaException) as exc:
            error = exc

        self._record_failure(event, str(error))

    def _record_failure(self, event: LeasedEvent, error: str) -> None:
        with self._sf() as session, session.begin():
            status = self._repo.mark_failed(
                session,
                event_id=event.id,
                owner=self.owner,
                error=redact_text(error),
                max_attempts=self.max_attempts,
                retry_delay_s=self.retry_delay_s,
            )

        if status is None:
            return

        self.metrics.publications.labels(
            kind=event.event_type,
            result="failure",
        ).inc()

        if status == "FAILED":
            self.metrics.permanent_failures.labels(
                kind=event.event_type,
            ).inc()
            log.error(
                "outbox event permanently failed event_type=%s event_id=%s; "
                "inspect outbox_events and reset only after fixing Kafka",
                event.event_type,
                event.id,
            )
        else:
            self.metrics.retries.inc()
            log.warning(
                "outbox delivery failed event_type=%s event_id=%s; retained for retry",
                event.event_type,
                event.id,
            )

    def _refresh_backlog(self) -> None:
        with self._sf() as session:
            pending, oldest = self._repo.backlog(session)

        self.metrics.pending.set(pending)
        self.metrics.oldest_age.set(oldest)

    def run(self, should_stop: Callable[[], bool] = lambda: False) -> None:
        while not should_stop():
            # Refresh the heartbeat before each dispatch iteration.
            self.metrics.heartbeat()
            count = self.dispatch_once()

            if not count:
                self._sleep(self.poll_interval_s)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Publish committed transactional outbox events to Kafka."
    )
    ap.add_argument(
        "--batch-size",
        type=int,
        default=int(os.environ.get("OUTBOX_BATCH_SIZE", "50")),
    )
    ap.add_argument(
        "--poll-interval",
        type=float,
        default=float(os.environ.get("OUTBOX_POLL_INTERVAL_SECONDS", "1")),
    )
    ap.add_argument(
        "--max-attempts",
        type=int,
        default=int(os.environ.get("OUTBOX_MAX_ATTEMPTS", "20")),
    )
    args = ap.parse_args(argv)

    if args.batch_size < 1 or args.poll_interval <= 0 or args.max_attempts < 1:
        ap.error("batch-size and max-attempts must be positive; poll-interval must be > 0")

    settings = KafkaSettings.from_env()
    db = DatabaseSettings.from_env()

    configure_logging(
        os.environ.get("LOG_LEVEL", "INFO"),
        os.environ.get("LOG_FORMAT", "text"),
    )

    wait_for_broker(settings, timeout_s=60)

    producer = Producer(
        client_config(
            settings,
            **{
                "acks": "all",
                "enable.idempotence": True,
                "delivery.timeout.ms": 30_000,
            },
        )
    )

    metrics = OutboxMetrics()
    metrics_host = os.environ.get("OUTBOX_METRICS_ADDR", "127.0.0.1")
    metrics_port = int(os.environ.get("OUTBOX_METRICS_PORT", "9102"))

    metrics_server = None

    try:
        metrics_server = MetricsServer(
            metrics,
            host=metrics_host,
            port=metrics_port,
        ).start()

        dispatcher = OutboxDispatcher(
            make_session_factory(make_engine(db)),
            producer,
            batch_size=args.batch_size,
            poll_interval_s=args.poll_interval,
            max_attempts=args.max_attempts,
            metrics=metrics,
        )

        dispatcher.run()
        return 0

    finally:
        if metrics_server is not None:
            metrics_server.stop()

        remaining = producer.flush(10)
        if remaining:
            log.warning(
                "Kafka producer shutdown with %s message(s) still pending",
                remaining,
            )


if __name__ == "__main__":
    raise SystemExit(main())
