"""ORM models. The authoritative DDL lives in alembic/versions; constraints here mirror the migration."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (BigInteger, Boolean, CheckConstraint, DateTime, Float, ForeignKey, Index, Integer, JSON, String, UniqueConstraint,
                        func, text)
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base

ALERT_ACTIVE, ALERT_RESOLVED = "ACTIVE", "RESOLVED"
USER_ROLES = ("viewer", "operator", "admin")
OUTBOX_PENDING, OUTBOX_IN_FLIGHT, OUTBOX_DELIVERED, OUTBOX_FAILED = "PENDING", "IN_FLIGHT", "DELIVERED", "FAILED"


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


class User(Base):
    """API user (Phase 4). ``password_hash`` is an Argon2id hash; the plaintext password is never stored or logged.
    ``username`` is stored lower-case (enforced by a CHECK constraint)."""
    __tablename__ = "users"
    __table_args__ = (CheckConstraint("role IN ('viewer','operator','admin')", name="role_values"),
                      CheckConstraint("username = lower(username)", name="username_lowercase"))
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class OutboxEvent(Base):
    """A Kafka publication intent, committed with its business records (at-least-once relay)."""
    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint("status IN ('PENDING','IN_FLIGHT','DELIVERED','FAILED')", name="outbox_status_values"),
        CheckConstraint("attempt_count >= 0", name="outbox_attempt_count_nonnegative"),
        Index("ix_outbox_events_dispatch", "status", "available_at", "created_at"),
        Index("ix_outbox_events_lock", "status", "locked_at"),
    )
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")
    topic: Mapped[str] = mapped_column(String(249), nullable=False)
    message_key: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=OUTBOX_PENDING)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lock_owner: Mapped[str | None] = mapped_column(String(64))
    last_error: Mapped[str | None] = mapped_column(String(500))
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
