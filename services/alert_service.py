from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from db.models import Alert
from db.repositories import AlertRepository
from services.errors import ConflictError, NotFoundError

UNSPECIFIED_FAULT = "UNSPECIFIED"


class AlertPolicy:
    """Raise an alert exactly when the Phase 2 decision says FAILURE. No new threshold is introduced here."""

    @staticmethod
    def should_alert(result: Any) -> bool:
        return bool(result.is_failure)

    @staticmethod
    def fault_key(result: Any) -> str:
        return result.fault_type or UNSPECIFIED_FAULT      # the dedup index needs a non-NULL fault type


class AlertService:
    def __init__(self, alerts: AlertRepository | None = None):
        self._alerts = alerts or AlertRepository()

    def list(self, session: Session, *, machine_id: str | None = None, status: str | None = None,
             page: int = 1, page_size: int = 50) -> tuple[list[Alert], int]:
        return self._alerts.list(session, machine_id=machine_id, status=status, page=page, page_size=page_size)

    def resolve(self, session: Session, alert_id: int) -> Alert:
        with session.begin():
            alert = self._alerts.get(session, alert_id)
            if alert is None:
                raise NotFoundError(f"alert {alert_id} not found")
            if not self._alerts.resolve(session, alert_id, datetime.now(timezone.utc)):
                raise ConflictError(f"alert {alert_id} is already {alert.status}")
        session.refresh(alert)
        return alert
