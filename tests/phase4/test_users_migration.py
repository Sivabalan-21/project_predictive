"""Migration 0002 against REAL PostgreSQL, applied through Alembic only."""
import pytest
from alembic import command
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError

from tests.phase4.conftest import alembic_config

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


def tables(url):
    eng = create_engine(url)
    try:
        return set(inspect(eng).get_table_names())
    finally:
        eng.dispose()


def test_0002_is_additive_and_downgrade_only_drops_users(scratch_db_url):
    cfg = alembic_config(scratch_db_url)
    command.upgrade(cfg, "0001")
    eng = create_engine(scratch_db_url)
    with eng.begin() as c:                                    # data that exists BEFORE the migration
        c.execute(text("INSERT INTO sensor_readings (machine_id, timestamp, air_temperature, process_temperature, "
                       "rotational_speed, torque, tool_wear) VALUES ('M001', now(), 300, 310, 1500, 40, 100)"))
    assert "users" not in tables(scratch_db_url)

    command.upgrade(cfg, "0002")
    assert {"users", "sensor_readings", "predictions", "alerts"} <= tables(scratch_db_url)
    with eng.connect() as c:
        assert c.execute(text("SELECT count(*) FROM sensor_readings")).scalar_one() == 1          # untouched

    command.downgrade(cfg, "0001")                            # rollback path
    assert "users" not in tables(scratch_db_url) and {"sensor_readings", "predictions", "alerts"} <= tables(scratch_db_url)
    with eng.connect() as c:
        assert c.execute(text("SELECT count(*) FROM sensor_readings")).scalar_one() == 1          # data survived

    command.upgrade(cfg, "head")                              # and forward again
    assert "users" in tables(scratch_db_url)
    eng.dispose()


def test_head_is_0002_and_orm_matches_migration(migrated_url):
    from alembic.script import ScriptDirectory
    assert ScriptDirectory.from_config(alembic_config(migrated_url)).get_current_head() == "0002"
    eng = create_engine(migrated_url)
    insp = inspect(eng)
    cols = {c["name"]: c for c in insp.get_columns("users")}
    assert set(cols) == {"id", "username", "password_hash", "role", "is_active", "created_at"}
    assert cols["password_hash"]["nullable"] is False and cols["is_active"]["nullable"] is False
    assert any(set(u["column_names"]) == {"username"} for u in insp.get_unique_constraints("users"))
    eng.dispose()


def test_database_enforces_roles_lowercase_and_uniqueness(migrated_url):
    eng = create_engine(migrated_url)
    ins = text("INSERT INTO users (username, password_hash, role) VALUES (:u, 'h', :r)")
    try:
        with eng.begin() as c:
            c.execute(ins, {"u": "ok-user-1", "r": "viewer"})
        for params in ({"u": "ok-user-1", "r": "viewer"},          # duplicate
                       {"u": "Mixed-Case", "r": "viewer"},         # not lower-case
                       {"u": "ok-user-2", "r": "root"}):           # unknown role
            with pytest.raises(IntegrityError), eng.begin() as c:
                c.execute(ins, params)
        with eng.begin() as c:
            assert c.execute(text("SELECT is_active FROM users WHERE username='ok-user-1'")).scalar_one() is True
    finally:
        with eng.begin() as c:
            c.execute(text("DELETE FROM users WHERE username LIKE 'ok-user-%'"))
        eng.dispose()
