"""transactional outbox for durable Kafka relay

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-12
"""
from alembic import op
import sqlalchemy as sa

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.String(64), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("topic", sa.String(249), nullable=False),
        sa.Column("message_key", sa.String(64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="PENDING"),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("locked_at", sa.DateTime(timezone=True)),
        sa.Column("lock_owner", sa.String(64)),
        sa.Column("last_error", sa.String(500)),
        sa.Column("delivered_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_outbox_events"),
        sa.CheckConstraint("status IN ('PENDING','IN_FLIGHT','DELIVERED','FAILED')", name="ck_outbox_status_values"),
        sa.CheckConstraint("attempt_count >= 0", name="ck_outbox_attempt_count_nonnegative"),
    )
    op.create_index("ix_outbox_events_dispatch", "outbox_events", ["status", "available_at", "created_at"])
    op.create_index("ix_outbox_events_lock", "outbox_events", ["status", "locked_at"])


def downgrade() -> None:
    op.drop_table("outbox_events")
