"""Phase 4 fixtures. Real-PostgreSQL fixtures are reused from tests/phase3/conftest.py (same rules: TEST_DATABASE_URL or the
embedded ``pgserver``; skipped, never faked, when neither exists)."""
from __future__ import annotations

import logging
import os
import uuid

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from tests.phase3.conftest import (ROOT, auth_headers, engine, make_event, migrated_url, pg_admin_url,  # noqa: F401
                                   predictor, session_factory)

STRONG_PASSWORD = "correct-horse-battery-staple-42"


@pytest.fixture(autouse=True)
def _restore_logging():
    """configure_logging() replaces root handlers; give every test a clean slate and restore afterwards."""
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield
    for h in list(root.handlers):
        root.removeHandler(h)
    for h in handlers:
        root.addHandler(h)
    root.setLevel(level)


@pytest.fixture()
def scratch_db_url(pg_admin_url):
    """A brand-new EMPTY database (no migrations applied) for migration tests; dropped afterwards."""
    name = f"pm_mig_{uuid.uuid4().hex[:8]}"
    admin = create_engine(pg_admin_url, isolation_level="AUTOCOMMIT")
    with admin.connect() as c:
        c.execute(text(f'CREATE DATABASE "{name}"'))
    url = make_url(pg_admin_url).set(database=name).render_as_string(hide_password=False)
    try:
        yield url
    finally:
        with admin.connect() as c:
            c.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


def alembic_config(url: str) -> Config:
    os.environ["DATABASE_URL"] = url
    return Config(str(ROOT / "alembic.ini"))


@pytest.fixture()
def make_user(session_factory):
    """Create a real user row through the service layer; returns (username, password)."""
    from services.auth_service import AuthService

    def _make(role: str = "viewer", active: bool = True, password: str = STRONG_PASSWORD):
        username = f"user-{uuid.uuid4().hex[:8]}"
        with session_factory() as s:
            user = AuthService().create_user(s, username, password, role)
        if not active:
            with session_factory() as s, s.begin():
                s.execute(text("UPDATE users SET is_active = false WHERE id = :i"), {"i": user.id})
        return username, password
    return _make
