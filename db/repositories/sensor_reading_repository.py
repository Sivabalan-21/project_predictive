from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from db.models import SensorReading


class SensorReadingRepository:
    """Persistence only - no business rules."""

    def insert_if_absent(self, session: Session, values: dict[str, Any]) -> int | None:
        """INSERT ... ON CONFLICT DO NOTHING (any unique key: (machine_id, timestamp) or event_id).

        Returns the new row id, or None when the reading already exists (duplicate delivery).
        """
        stmt = insert(SensorReading).values(**values).on_conflict_do_nothing().returning(SensorReading.id)
        return session.execute(stmt).scalar_one_or_none()

    def machine_exists(self, session: Session, machine_id: str) -> bool:
        return session.execute(select(SensorReading.id).where(SensorReading.machine_id == machine_id).limit(1)).first() is not None
