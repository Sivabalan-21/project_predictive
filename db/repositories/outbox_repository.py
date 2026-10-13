"""PostgreSQL transactional-outbox persistence and short lease operations."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy import or_, select, update
from sqlalchemy.orm import Session

from db.models import OUTBOX_DELIVERED, OUTBOX_FAILED, OUTBOX_IN_FLIGHT, OUTBOX_PENDING, OutboxEvent


class OutboxRepository:
    def enqueue(self, session: Session, *, event_id: str, event_type: str, topic: str, message_key: str,
                payload: dict, schema_version: int = 1) -> None:
        session.add(OutboxEvent(id=event_id, event_type=event_type, schema_version=schema_version, topic=topic,
                                message_key=message_key, payload=payload, status=OUTBOX_PENDING))

    def claim(self, session: Session, *, owner: str, limit: int, lease_seconds: float,
              now: datetime | None = None) -> list[OutboxEvent]:
        """Lease a bounded batch using SKIP LOCKED; this transaction never performs network I/O."""
        now = now or datetime.now(timezone.utc)
        stale = now - timedelta(seconds=lease_seconds)
        rows = session.execute(
            select(OutboxEvent).where(
                or_(
                    (OutboxEvent.status == OUTBOX_PENDING) & (OutboxEvent.available_at <= now),
                    (OutboxEvent.status == OUTBOX_IN_FLIGHT) & (OutboxEvent.locked_at < stale),
                )
            ).order_by(OutboxEvent.created_at, OutboxEvent.id).limit(limit).with_for_update(skip_locked=True)
        ).scalars().all()
        for row in rows:
            row.status, row.lock_owner, row.locked_at = OUTBOX_IN_FLIGHT, owner, now
        session.flush()
        return list(rows)

    def mark_delivered(self, session: Session, *, event_id: str, owner: str, when: datetime | None = None) -> bool:
        result = session.execute(update(OutboxEvent).where(OutboxEvent.id == event_id,
            OutboxEvent.status == OUTBOX_IN_FLIGHT, OutboxEvent.lock_owner == owner).values(
                status=OUTBOX_DELIVERED, delivered_at=when or datetime.now(timezone.utc), locked_at=None,
                lock_owner=None, last_error=None))
        return result.rowcount == 1

    def mark_failed(self, session: Session, *, event_id: str, owner: str, error: str, max_attempts: int,
                    retry_delay_s: float, when: datetime | None = None) -> str | None:
        row = session.get(OutboxEvent, event_id)
        if row is None or row.status != OUTBOX_IN_FLIGHT or row.lock_owner != owner:
            return None
        now = when or datetime.now(timezone.utc)
        row.attempt_count += 1
        row.last_error = error[:500]
        row.lock_owner, row.locked_at = None, None
        if row.attempt_count >= max_attempts:
            row.status = OUTBOX_FAILED
        else:
            row.status, row.available_at = OUTBOX_PENDING, now + timedelta(seconds=retry_delay_s)
        session.flush()
        return row.status

    def backlog(self, session: Session, now: datetime | None = None) -> tuple[int, float]:
        now = now or datetime.now(timezone.utc)
        rows: Iterable[OutboxEvent] = session.execute(select(OutboxEvent).where(OutboxEvent.status.in_((OUTBOX_PENDING, OUTBOX_IN_FLIGHT)))).scalars()
        rows = list(rows)
        oldest = max((now - r.created_at).total_seconds() for r in rows) if rows else 0.0
        return len(rows), max(0.0, oldest)
