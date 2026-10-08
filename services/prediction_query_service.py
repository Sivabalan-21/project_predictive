"""Read side for persisted predictions. (The ML engine is ml/prediction/prediction_service.py - not this.)"""
from __future__ import annotations

from sqlalchemy.orm import Session

from db.models import Prediction
from db.repositories import PredictionRepository, SensorReadingRepository
from services.errors import NotFoundError


class PredictionQueryService:
    def __init__(self, predictions: PredictionRepository | None = None, readings: SensorReadingRepository | None = None):
        self._predictions = predictions or PredictionRepository()
        self._readings = readings or SensorReadingRepository()

    def list(self, session: Session, *, page: int, page_size: int) -> tuple[list[Prediction], int]:
        return self._predictions.list(session, page=page, page_size=page_size)

    def get(self, session: Session, prediction_id: int) -> Prediction:
        row = self._predictions.get(session, prediction_id)
        if row is None:
            raise NotFoundError(f"prediction {prediction_id} not found")
        return row

    def list_for_machine(self, session: Session, machine_id: str, *, page: int, page_size: int) -> tuple[list[Prediction], int]:
        self.require_machine(session, machine_id)
        return self._predictions.list(session, machine_id=machine_id, page=page, page_size=page_size)

    def require_machine(self, session: Session, machine_id: str) -> None:
        if not self._readings.machine_exists(session, machine_id):
            raise NotFoundError(f"machine {machine_id!r} not found")
