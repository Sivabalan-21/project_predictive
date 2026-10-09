"""ORM models. The authoritative DDL lives in alembic/versions; constraints here mirror the migration."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (BigInteger, CheckConstraint, DateTime, Float, ForeignKey, Index, String, UniqueConstraint, func,
                        text)
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base

ALERT_ACTIVE, ALERT_RESOLVED = "ACTIVE", "RESOLVED"


class SensorReading(Base):
    __tablename__ = "sensor_readings"
    __table_args__ = (UniqueConstraint("machine_id", "timestamp", name="uq_sensor_readings_machine_id_timestamp"),
                      Index("ix_sensor_readings_machine_id_timestamp", "machine_id", "timestamp"))
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    event_id: Mapped[str | None] = mapped_column(String(64), unique=True)
    machine_id: Mapped[str] = mapped_column(String(64), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    air_temperature: Mapped[float] = mapped_column(Float, nullable=False)
    process_temperature: Mapped[float] = mapped_column(Float, nullable=False)
    rotational_speed: Mapped[float] = mapped_column(Float, nullable=False)
    torque: Mapped[float] = mapped_column(Float, nullable=False)
    tool_wear: Mapped[float] = mapped_column(Float, nullable=False)
    product_type: Mapped[str | None] = mapped_column(String(1))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class Prediction(Base):
    """``risk_score`` = mean of RF and XGBoost failure probabilities (Phase 1 definition). NOT calibrated."""
    __tablename__ = "predictions"
    __table_args__ = (CheckConstraint("prediction IN ('NORMAL','FAILURE')", name="prediction_values"),
                      CheckConstraint("risk_score >= 0 AND risk_score <= 1", name="risk_score_range"),
                      Index("ix_predictions_machine_id_timestamp", "machine_id", "timestamp"))
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    sensor_reading_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("sensor_readings.id", ondelete="CASCADE"), nullable=False, unique=True)
    machine_id: Mapped[str] = mapped_column(String(64), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    prediction: Mapped[str] = mapped_column(String(16), nullable=False)
    risk_score: Mapped[float] = mapped_column(Float, nullable=False)
    rf_probability: Mapped[float] = mapped_column(Float, nullable=False)
    xgb_probability: Mapped[float] = mapped_column(Float, nullable=False)
    anomaly_score: Mapped[float] = mapped_column(Float, nullable=False)
    fault_type: Mapped[str | None] = mapped_column(String(64))
    model_version: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class Alert(Base):
    __tablename__ = "alerts"
    __table_args__ = (
        CheckConstraint("status IN ('ACTIVE','RESOLVED')", name="status_values"),
        CheckConstraint("(status = 'RESOLVED') = (resolved_at IS NOT NULL)", name="resolved_at_matches_status"),
        # At most ONE active alert per machine + fault type, enforced by PostgreSQL itself.
        Index("uq_alerts_active_machine_fault", "machine_id", "fault_type", unique=True,
              postgresql_where=text("status = 'ACTIVE'")),
        Index("ix_alerts_machine_id_created_at", "machine_id", "created_at"),
    )
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    machine_id: Mapped[str] = mapped_column(String(64), nullable=False)
    sensor_reading_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("sensor_readings.id", ondelete="CASCADE"), nullable=False)
    prediction_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("predictions.id", ondelete="CASCADE"), nullable=False)
    fault_type: Mapped[str] = mapped_column(String(64), nullable=False)
    risk_score: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=ALERT_ACTIVE)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
