"""Fixtures for Phase 3. PostgreSQL tests run against a REAL PostgreSQL server:

* TEST_DATABASE_URL  - admin URL of a server where CREATE DATABASE is allowed (e.g. the docker-compose postgres), or
* the optional ``pgserver`` package (embedded PostgreSQL, no Docker needed).

If neither is available the PostgreSQL tests are SKIPPED (never faked, never run against SQLite).
"""
from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def pg_admin_url(tmp_path_factory) -> str:
    explicit = os.environ.get("TEST_DATABASE_URL")
    if explicit:
        return explicit
    try:
        import pgserver
    except ImportError:
        pytest.skip("no PostgreSQL available: set TEST_DATABASE_URL or `pip install pgserver`")
    server = pgserver.get_server(tmp_path_factory.mktemp("pgdata"), cleanup_mode="delete")
    return f"postgresql+psycopg://postgres@/postgres?host={server.get_uri().split('host=')[-1]}"


@pytest.fixture(scope="session")
def migrated_url(pg_admin_url):
    """A brand-new empty database brought to schema head ONLY through Alembic."""
    name = f"pm_it_{uuid.uuid4().hex[:8]}"
    admin = create_engine(pg_admin_url, isolation_level="AUTOCOMMIT")
    with admin.connect() as c:
        c.execute(text(f'CREATE DATABASE "{name}"'))
    url = make_url(pg_admin_url).set(database=name).render_as_string(hide_password=False)
    old = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url
    try:
        command.upgrade(Config(str(ROOT / "alembic.ini")), "head")
        yield url
    finally:
        if old is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = old
        with admin.connect() as c:
            c.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


@pytest.fixture(scope="session")
def engine(migrated_url):
    eng = create_engine(migrated_url, pool_pre_ping=True)
    yield eng
    eng.dispose()


@pytest.fixture()
def session_factory(engine):
    with engine.begin() as c:
        c.execute(text("TRUNCATE alerts, predictions, sensor_readings RESTART IDENTITY CASCADE"))
    return sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture(scope="session")
def predictor():
    from ml.prediction import get_prediction_service
    return get_prediction_service()


NORMAL = {"air_temperature": 300.5, "process_temperature": 310.8, "rotational_speed": 1512, "torque": 39.8, "tool_wear": 104}
OVERSTRAIN = {"air_temperature": 300.0, "process_temperature": 310.0, "rotational_speed": 1300, "torque": 70.0, "tool_wear": 250}


def make_event(machine_id="M001", ts="2026-10-08T10:30:00Z", values=None, event_id=None):
    from streaming.schemas import SensorEvent
    return SensorEvent.from_dict({"machine_id": machine_id, "timestamp": ts, **(values or NORMAL),
                                  **({"event_id": event_id} if event_id else {})})
