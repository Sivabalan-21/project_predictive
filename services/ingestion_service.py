"""Persist one sensor event: reading + prediction + alert in ONE PostgreSQL transaction.

The prediction itself is made by ml.prediction.PredictionService (single source of truth) - this module only
persists its output. A duplicate event (same machine_id+timestamp or event_id) is detected by
INSERT ... ON CONFLICT DO NOTHING and produces no second reading, prediction or alert.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from sqlalchemy.orm import Session, sessionmaker

from db.repositories import AlertRepository, PredictionRepository, SensorReadingRepository
from services.alert_service import AlertPolicy
from streaming.schemas import AlertEvent, PredictionEvent, SensorEvent

log = logging.getLogger("services.ingestion")


class PredictionError(RuntimeError):
    """The PredictionService failed on a schema-valid event (the transaction is rolled back)."""


@dataclass(frozen=True)
class IngestionOutcome:
    duplicate: bool
    sensor_reading_id: int | None = None
    prediction: PredictionEvent | None = None
    alert: AlertEvent | None = None            # only when a NEW active alert was created


class IngestionService:
    def __init__(self, session_factory: sessionmaker[Session], predictor: Any,
                 readings: SensorReadingRepository | None = None, predictions: PredictionRepository | None = None,
                 alerts: AlertRepository | None = None, policy: AlertPolicy | None = None,
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)):
        self._sf, self._predictor = session_factory, predictor
        self._readings = readings or SensorReadingRepository()
        self._predictions = predictions or PredictionRepository()
        self._alerts = alerts or AlertRepository()
        self._policy, self._clock = policy or AlertPolicy(), clock

    def ingest(self, event: SensorEvent) -> IngestionOutcome:
        """All-or-nothing. Database errors propagate (the caller must NOT commit the Kafka offset)."""
        with self._sf() as session, session.begin():
            reading_id = self._readings.insert_if_absent(session, {
                "event_id": event.event_id, "machine_id": event.machine_id, "timestamp": event.timestamp,
                "air_temperature": event.air_temperature, "process_temperature": event.process_temperature,
                "rotational_speed": event.rotational_speed, "torque": event.torque, "tool_wear": event.tool_wear,
                "product_type": event.product_type})
            if reading_id is None:
                log.info("duplicate event ignored machine_id=%s event_id=%s timestamp=%s", event.machine_id,
                         event.event_id, event.timestamp.isoformat())
                return IngestionOutcome(duplicate=True)

            try:
                result = self._predictor.predict(event.to_reading())      # <- ml.prediction.PredictionService
            except Exception as exc:  # noqa: BLE001 - re-raised with context; the transaction rolls back
                raise PredictionError(f"prediction failed for {event.machine_id} @ {event.timestamp}: {exc}") from exc

            prediction = self._predictions.add(session, {
                "sensor_reading_id": reading_id, "machine_id": event.machine_id, "timestamp": event.timestamp,
                "prediction": result.final_status, "risk_score": result.risk_score,
                "rf_probability": result.rf_probability, "xgb_probability": result.xgb_probability,
                "anomaly_score": result.isolation_anomaly_score, "fault_type": result.fault_type,
                "model_version": result.model_version})

            alert_event = None
            if self._policy.should_alert(result):
                alert = self._alerts.create_active_if_absent(
                    session, machine_id=event.machine_id, fault_type=self._policy.fault_key(result),
                    sensor_reading_id=reading_id, prediction_id=prediction.id, risk_score=result.risk_score)
                if alert is not None:
                    alert_event = AlertEvent(alert_id=alert.id, machine_id=alert.machine_id, fault_type=alert.fault_type,
                                             risk_score=alert.risk_score, status="ACTIVE", timestamp=event.timestamp)
                else:
                    log.info("active alert already exists machine_id=%s fault_type=%s - no new alert", event.machine_id,
                             self._policy.fault_key(result))
            prediction_event = PredictionEvent.from_result(event, result, self._clock(), sensor_reading_id=reading_id)
        # session.begin() committed above; only now may downstream publishing / offset commit happen
        return IngestionOutcome(duplicate=False, sensor_reading_id=reading_id, prediction=prediction_event, alert=alert_event)
