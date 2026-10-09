"""initial schema: sensor_readings, predictions, alerts

Revision ID: 0001
Revises:
Create Date: 2026-10-08
"""
from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

_TS = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "sensor_readings",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("event_id", sa.String(64), nullable=True),
        sa.Column("machine_id", sa.String(64), nullable=False),
        sa.Column("timestamp", _TS, nullable=False),
        sa.Column("air_temperature", sa.Float(), nullable=False),
        sa.Column("process_temperature", sa.Float(), nullable=False),
        sa.Column("rotational_speed", sa.Float(), nullable=False),
        sa.Column("torque", sa.Float(), nullable=False),
        sa.Column("tool_wear", sa.Float(), nullable=False),
        sa.Column("product_type", sa.String(1), nullable=True),
        sa.Column("created_at", _TS, nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_sensor_readings"),
        sa.UniqueConstraint("event_id", name="uq_sensor_readings_event_id"),
        sa.UniqueConstraint("machine_id", "timestamp", name="uq_sensor_readings_machine_id_timestamp"),
    )
    op.create_index("ix_sensor_readings_machine_id_timestamp", "sensor_readings", ["machine_id", "timestamp"])

    op.create_table(
        "predictions",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("sensor_reading_id", sa.BigInteger(), nullable=False),
        sa.Column("machine_id", sa.String(64), nullable=False),
        sa.Column("timestamp", _TS, nullable=False),
        sa.Column("prediction", sa.String(16), nullable=False),
        sa.Column("risk_score", sa.Float(), nullable=False),
        sa.Column("rf_probability", sa.Float(), nullable=False),
        sa.Column("xgb_probability", sa.Float(), nullable=False),
        sa.Column("anomaly_score", sa.Float(), nullable=False),
        sa.Column("fault_type", sa.String(64), nullable=True),
        sa.Column("model_version", sa.String(128), nullable=False),
        sa.Column("created_at", _TS, nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_predictions"),
        sa.ForeignKeyConstraint(["sensor_reading_id"], ["sensor_readings.id"], ondelete="CASCADE",
                                name="fk_predictions_sensor_reading_id_sensor_readings"),
        sa.UniqueConstraint("sensor_reading_id", name="uq_predictions_sensor_reading_id"),
        sa.CheckConstraint("prediction IN ('NORMAL','FAILURE')", name="ck_predictions_prediction_values"),
        sa.CheckConstraint("risk_score >= 0 AND risk_score <= 1", name="ck_predictions_risk_score_range"),
    )
    op.create_index("ix_predictions_machine_id_timestamp", "predictions", ["machine_id", "timestamp"])

    op.create_table(
        "alerts",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("machine_id", sa.String(64), nullable=False),
        sa.Column("sensor_reading_id", sa.BigInteger(), nullable=False),
        sa.Column("prediction_id", sa.BigInteger(), nullable=False),
        sa.Column("fault_type", sa.String(64), nullable=False),
        sa.Column("risk_score", sa.Float(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="ACTIVE"),
        sa.Column("created_at", _TS, nullable=False, server_default=sa.func.now()),
        sa.Column("resolved_at", _TS, nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_alerts"),
        sa.ForeignKeyConstraint(["sensor_reading_id"], ["sensor_readings.id"], ondelete="CASCADE",
                                name="fk_alerts_sensor_reading_id_sensor_readings"),
        sa.ForeignKeyConstraint(["prediction_id"], ["predictions.id"], ondelete="CASCADE",
                                name="fk_alerts_prediction_id_predictions"),
        sa.CheckConstraint("status IN ('ACTIVE','RESOLVED')", name="ck_alerts_status_values"),
        sa.CheckConstraint("(status = 'RESOLVED') = (resolved_at IS NOT NULL)", name="ck_alerts_resolved_at_matches_status"),
    )
    op.create_index("ix_alerts_machine_id_created_at", "alerts", ["machine_id", "created_at"])
    # At most one ACTIVE alert per machine + fault type (partial unique index, enforced by PostgreSQL).
    op.create_index("uq_alerts_active_machine_fault", "alerts", ["machine_id", "fault_type"], unique=True,
                    postgresql_where=sa.text("status = 'ACTIVE'"))


def downgrade() -> None:
    op.drop_index("uq_alerts_active_machine_fault", table_name="alerts")
    op.drop_table("alerts")
    op.drop_table("predictions")
    op.drop_table("sensor_readings")
