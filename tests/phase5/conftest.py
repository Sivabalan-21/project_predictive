"""Reuse the real-PostgreSQL migration fixtures; Phase 5 integration tests must never fall back to SQLite."""
from tests.phase3.conftest import engine, migrated_url, pg_admin_url, predictor, session_factory  # noqa: F401
