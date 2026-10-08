from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from db.models import ALERT_ACTIVE, ALERT_RESOLVED, Alert


class AlertRepository:
    def create_active_if_absent(self, session: Session, *, machine_id: str, fault_type: str, sensor_reading_id: int,
                                prediction_id: int, risk_score: float) -> Alert | None:
        """Insert an ACTIVE alert; None if one already exists for (machine_id, fault_type).

        Atomic: relies on the partial unique index ``uq_alerts_active_machine_fault`` (WHERE status='ACTIVE'),
        so concurrent consumers cannot create two active alerts.
        """
        stmt = (insert(Alert).values(machine_id=machine_id, fault_type=fault_type, sensor_reading_id=sensor_reading_id,
                                     prediction_id=prediction_id, risk_score=risk_score, status=ALERT_ACTIVE)
                .on_conflict_do_nothing(index_elements=[Alert.machine_id, Alert.fault_type],
                                        index_where=Alert.status == ALERT_ACTIVE)
                .returning(Alert.id))
        alert_id = session.execute(stmt).scalar_one_or_none()
        return session.get(Alert, alert_id) if alert_id is not None else None

    def get(self, session: Session, alert_id: int) -> Alert | None:
        return session.get(Alert, alert_id)

    def resolve(self, session: Session, alert_id: int, when: datetime) -> bool:
        """Mark ACTIVE -> RESOLVED. False if the alert is not ACTIVE (already resolved or missing)."""
        res = session.execute(update(Alert).where(Alert.id == alert_id, Alert.status == ALERT_ACTIVE)
                              .values(status=ALERT_RESOLVED, resolved_at=when))
        return res.rowcount == 1

    def list(self, session: Session, *, machine_id: str | None = None, status: str | None = None,
             page: int = 1, page_size: int = 50) -> tuple[list[Alert], int]:
        where = []
        if machine_id:
            where.append(Alert.machine_id == machine_id)
        if status:
            where.append(Alert.status == status)
        total = session.execute(select(func.count()).select_from(Alert).where(*where)).scalar_one()
        rows = session.execute(select(Alert).where(*where).order_by(Alert.created_at.desc(), Alert.id.desc())
                               .limit(page_size).offset((page - 1) * page_size)).scalars().all()
        return list(rows), total
