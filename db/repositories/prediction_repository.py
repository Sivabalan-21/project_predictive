from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from db.models import Prediction


class PredictionRepository:
    def add(self, session: Session, values: dict[str, Any]) -> Prediction:
        row = Prediction(**values)
        session.add(row)
        session.flush()
        return row

    def get(self, session: Session, prediction_id: int) -> Prediction | None:
        return session.get(Prediction, prediction_id)

    def list(self, session: Session, *, machine_id: str | None = None, page: int = 1, page_size: int = 50) -> tuple[list[Prediction], int]:
        where = [Prediction.machine_id == machine_id] if machine_id else []
        total = session.execute(select(func.count()).select_from(Prediction).where(*where)).scalar_one()
        rows = session.execute(select(Prediction).where(*where).order_by(Prediction.timestamp.desc(), Prediction.id.desc())
                               .limit(page_size).offset((page - 1) * page_size)).scalars().all()
        return list(rows), total
