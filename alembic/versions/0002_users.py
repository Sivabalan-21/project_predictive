"""users table for API authentication (Phase 4)

Additive only: creates ONE new table. It does not touch sensor_readings, predictions or alerts, so existing data is
unaffected. ``downgrade`` drops only the ``users`` table (and with it the accounts - recreate them with the CLI).

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-09
"""
from alembic import op
import sqlalchemy as sa

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("username", sa.String(64), nullable=False),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("role", sa.String(16), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_users"),
        sa.UniqueConstraint("username", name="uq_users_username"),
        sa.CheckConstraint("role IN ('viewer','operator','admin')", name="ck_users_role_values"),
        sa.CheckConstraint("username = lower(username)", name="ck_users_username_lowercase"),
    )


def downgrade() -> None:
    op.drop_table("users")
