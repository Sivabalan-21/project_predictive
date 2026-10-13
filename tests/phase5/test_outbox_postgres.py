"""Real-PostgreSQL coverage for the transactional outbox; Kafka itself is deliberately faked here."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from db.models import OUTBOX_DELIVERED, OUTBOX_FAILED, OUTBOX_PENDING
from db.repositories import OutboxRepository
from services.ingestion_service import IngestionService, PredictionError
from streaming.outbox_dispatcher import OutboxDispatcher
from tests.phase3.conftest import NORMAL, make_event

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


class Producer:
    def __init__(self, fail: bool = False):
        self.fail, self.sent = fail, []

    def produce(self, topic, key, value, headers, on_delivery):
        self.sent.append((topic, key, value, headers))
        on_delivery(RuntimeError("broker unavailable") if self.fail else None, None)

    def poll(self, _timeout): pass
    def flush(self, _timeout): return 0


def outbox_rows(sf):
    with sf() as s:
        return s.execute(text("SELECT id, status, attempt_count FROM outbox_events ORDER BY created_at, id")).all()


def test_business_transaction_and_outbox_commit_together(session_factory, predictor):
    outcome = IngestionService(session_factory, predictor).ingest(make_event(values=NORMAL, event_id="phase5-evt-1"))
    assert not outcome.duplicate
    rows = outbox_rows(session_factory)
    assert len(rows) == 1 and rows[0].status == OUTBOX_PENDING


def test_business_failure_leaves_no_outbox_intent(session_factory):
    class Broken:
        def predict(self, _reading): raise ValueError("model unavailable")
    with pytest.raises(PredictionError):
        IngestionService(session_factory, Broken()).ingest(make_event(event_id="phase5-evt-2"))
    with session_factory() as s:
        assert s.execute(text("SELECT count(*) FROM sensor_readings")).scalar_one() == 0
        assert s.execute(text("SELECT count(*) FROM outbox_events")).scalar_one() == 0


def test_confirmed_dispatch_marks_row_delivered_and_carries_stable_header(session_factory, predictor):
    IngestionService(session_factory, predictor).ingest(make_event(event_id="phase5-evt-3"))
    producer = Producer()
    assert OutboxDispatcher(session_factory, producer, poll_interval_s=0.01).dispatch_once() == 1
    rows = outbox_rows(session_factory)
    assert rows[0].status == OUTBOX_DELIVERED and producer.sent[0][3][0][0] == "event_id"


def test_failed_dispatch_stays_recoverable_then_becomes_observable_failure(session_factory, predictor):
    IngestionService(session_factory, predictor).ingest(make_event(event_id="phase5-evt-4"))
    dispatcher = OutboxDispatcher(session_factory, Producer(fail=True), max_attempts=2, retry_delay_s=0, poll_interval_s=0.01)
    dispatcher.dispatch_once()
    assert outbox_rows(session_factory)[0].status == OUTBOX_PENDING
    dispatcher.dispatch_once()
    row = outbox_rows(session_factory)[0]
    assert row.status == OUTBOX_FAILED and row.attempt_count == 2


def test_second_worker_cannot_claim_an_existing_lease(session_factory, predictor):
    IngestionService(session_factory, predictor).ingest(make_event(event_id="phase5-evt-5"))
    repo = OutboxRepository()
    with session_factory() as s, s.begin():
        first = repo.claim(s, owner="worker-one", limit=10, lease_seconds=60)
    with session_factory() as s, s.begin():
        second = repo.claim(s, owner="worker-two", limit=10, lease_seconds=60)
    assert len(first) == 1 and second == []
