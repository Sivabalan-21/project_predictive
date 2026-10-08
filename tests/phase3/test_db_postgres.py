"""INTEGRATION (real PostgreSQL): schema, constraints, repositories."""
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import DataError, IntegrityError

from db.models import Alert, Prediction
from db.repositories import AlertRepository, PredictionRepository, SensorReadingRepository

pytestmark = [pytest.mark.integration, pytest.mark.postgres]
T0 = datetime(2026, 10, 8, 10, 30, tzinfo=timezone.utc)


def reading(**kw):
    base = dict(event_id=None, machine_id="M001", timestamp=T0, air_temperature=300.0, process_temperature=310.0,
                rotational_speed=1500.0, torque=40.0, tool_wear=100.0, product_type=None)
    return {**base, **kw}


def pred(reading_id, **kw):
    base = dict(sensor_reading_id=reading_id, machine_id="M001", timestamp=T0, prediction="FAILURE", risk_score=0.8,
                rf_probability=0.9, xgb_probability=0.7, anomaly_score=0.5, fault_type="OSF", model_version="v")
    return {**base, **kw}


def seed(sf, ts=T0, **kw):
    with sf() as s, s.begin():
        rid = SensorReadingRepository().insert_if_absent(s, reading(timestamp=ts, **kw))
        p = PredictionRepository().add(s, pred(rid, timestamp=ts))
        return rid, p.id


def test_migrated_schema_has_tables_indexes_and_partial_unique_index(engine):
    insp = inspect(engine)
    assert {"sensor_readings", "predictions", "alerts"} <= set(insp.get_table_names())
    with engine.connect() as c:
        defn = c.execute(text("SELECT indexdef FROM pg_indexes WHERE indexname='uq_alerts_active_machine_fault'")).scalar_one()
    assert "UNIQUE" in defn and "(machine_id, fault_type)" in defn and "status" in defn and "ACTIVE" in defn
    uniques = {tuple(u["column_names"]) for u in insp.get_unique_constraints("sensor_readings")}
    assert ("machine_id", "timestamp") in uniques and ("event_id",) in uniques
    assert insp.get_foreign_keys("predictions") and len(insp.get_foreign_keys("alerts")) == 2


def test_timestamps_are_timezone_aware(engine):
    with engine.connect() as c:
        types = dict(c.execute(text("SELECT column_name, data_type FROM information_schema.columns WHERE table_name IN "
                                    "('sensor_readings','predictions','alerts') AND column_name IN "
                                    "('timestamp','created_at','resolved_at')")).all())
    assert set(types.values()) == {"timestamp with time zone"}


def test_duplicate_reading_is_ignored_by_machine_and_timestamp(session_factory):
    repo = SensorReadingRepository()
    with session_factory() as s, s.begin():
        first = repo.insert_if_absent(s, reading())
        second = repo.insert_if_absent(s, reading(air_temperature=999.0))     # same key, different payload
        other = repo.insert_if_absent(s, reading(timestamp=T0 + timedelta(seconds=1)))
        count = s.execute(text("SELECT count(*) FROM sensor_readings")).scalar_one()
    assert first is not None and second is None and other is not None and count == 2


def test_duplicate_event_id_is_ignored_even_with_new_timestamp(session_factory):
    repo = SensorReadingRepository()
    with session_factory() as s, s.begin():
        assert repo.insert_if_absent(s, reading(event_id="evt-1")) is not None
        assert repo.insert_if_absent(s, reading(event_id="evt-1", timestamp=T0 + timedelta(minutes=5))) is None


def test_timestamp_roundtrip_is_utc(session_factory):
    from sqlalchemy import select
    from db.models import SensorReading
    ist = timezone(timedelta(hours=5, minutes=30))
    with session_factory() as s, s.begin():
        SensorReadingRepository().insert_if_absent(s, reading(timestamp=datetime(2026, 10, 8, 16, 0, tzinfo=ist)))
    with session_factory() as s:
        ts = s.execute(select(SensorReading.timestamp)).scalar_one()
    assert ts.utcoffset() == timedelta(0) and ts == datetime(2026, 10, 8, 10, 30, tzinfo=timezone.utc)


def test_foreign_keys_enforced(session_factory):
    with pytest.raises(IntegrityError), session_factory() as s, s.begin():
        PredictionRepository().add(s, pred(reading_id=424242))
    rid, _ = seed(session_factory)
    with pytest.raises(IntegrityError), session_factory() as s, s.begin():
        s.add(Alert(machine_id="M001", sensor_reading_id=rid, prediction_id=999999, fault_type="OSF", risk_score=0.5))


def test_one_prediction_per_reading(session_factory):
    rid, _ = seed(session_factory)
    with pytest.raises(IntegrityError), session_factory() as s, s.begin():
        PredictionRepository().add(s, pred(rid))


@pytest.mark.parametrize("override", [{"risk_score": 1.5}, {"risk_score": -0.1}, {"prediction": "MAYBE"}])
def test_check_constraints_on_predictions(session_factory, override):
    with session_factory() as s, s.begin():
        rid = SensorReadingRepository().insert_if_absent(s, reading())
    with pytest.raises(IntegrityError), session_factory() as s, s.begin():
        PredictionRepository().add(s, pred(rid, **override))


def test_active_alert_unique_per_machine_and_fault(session_factory):
    rid, pid = seed(session_factory)
    repo = AlertRepository()
    args = dict(machine_id="M001", sensor_reading_id=rid, prediction_id=pid, risk_score=0.8)
    with session_factory() as s, s.begin():
        a1 = repo.create_active_if_absent(s, fault_type="OSF", **args)
        a2 = repo.create_active_if_absent(s, fault_type="OSF", **args)          # duplicate -> None
        a3 = repo.create_active_if_absent(s, fault_type="HDF", **args)          # other fault type OK
        a4 = repo.create_active_if_absent(s, fault_type="OSF", **{**args, "machine_id": "M002"})   # other machine OK
    assert a1 is not None and a2 is None and a3 is not None and a4 is not None
    # The database itself (not just the repository) rejects a raw duplicate:
    with pytest.raises(IntegrityError), session_factory() as s, s.begin():
        s.add(Alert(fault_type="OSF", status="ACTIVE", **args))


def test_resolved_alert_allows_new_active_alert_and_check_constraint(session_factory):
    rid, pid = seed(session_factory)
    repo = AlertRepository()
    args = dict(machine_id="M001", fault_type="OSF", sensor_reading_id=rid, prediction_id=pid, risk_score=0.8)
    with session_factory() as s, s.begin():
        first = repo.create_active_if_absent(s, **args)
        assert repo.resolve(s, first.id, T0) is True
        assert repo.resolve(s, first.id, T0) is False                        # not ACTIVE any more
        assert repo.create_active_if_absent(s, **args) is not None
    with pytest.raises(IntegrityError), session_factory() as s, s.begin():   # RESOLVED requires resolved_at
        s.add(Alert(status="RESOLVED", resolved_at=None, **{**args, "fault_type": "HDF"}))


def test_concurrent_alert_creation_yields_exactly_one_active_alert(session_factory):
    rid, pid = seed(session_factory)

    def attempt(_):
        with session_factory() as s, s.begin():
            return AlertRepository().create_active_if_absent(s, machine_id="M001", fault_type="OSF", sensor_reading_id=rid,
                                                             prediction_id=pid, risk_score=0.9) is not None
    with ThreadPoolExecutor(8) as ex:
        created = list(ex.map(attempt, range(8)))
    with session_factory() as s:
        n = s.execute(text("SELECT count(*) FROM alerts WHERE status='ACTIVE'")).scalar_one()
    assert sum(created) == 1 and n == 1


def test_transaction_rollback_leaves_nothing(session_factory):
    with pytest.raises(RuntimeError), session_factory() as s, s.begin():
        SensorReadingRepository().insert_if_absent(s, reading())
        raise RuntimeError("boom")
    with session_factory() as s:
        assert s.execute(text("SELECT count(*) FROM sensor_readings")).scalar_one() == 0


def test_pagination_and_ordering(session_factory):
    for i in range(7):
        seed(session_factory, ts=T0 + timedelta(seconds=i))
    with session_factory() as s:
        repo = PredictionRepository()
        rows, total = repo.list(s, page=1, page_size=3)
        rows3, _ = repo.list(s, page=3, page_size=3)
        none, _ = repo.list(s, page=9, page_size=3)
        mine, mt = repo.list(s, machine_id="M001", page=1, page_size=50)
        other, ot = repo.list(s, machine_id="NOPE", page=1, page_size=50)
    assert total == 7 and len(rows) == 3 and len(rows3) == 1 and none == [] and mt == 7 and ot == 0 and other == []
    assert rows[0].timestamp > rows[1].timestamp                       # newest first


def test_alembic_downgrade_and_upgrade_roundtrip(pg_admin_url):
    """Fresh database: upgrade -> downgrade -> upgrade works from zero."""
    import os, uuid
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import create_engine
    from sqlalchemy.engine import make_url
    from tests.phase3.conftest import ROOT
    name = f"pm_mig_{uuid.uuid4().hex[:8]}"
    admin = create_engine(pg_admin_url, isolation_level="AUTOCOMMIT")
    with admin.connect() as c:
        c.execute(text(f'CREATE DATABASE "{name}"'))
    url = make_url(pg_admin_url).set(database=name).render_as_string(hide_password=False)
    old = os.environ.get("DATABASE_URL"); os.environ["DATABASE_URL"] = url
    try:
        cfg = Config(str(ROOT / "alembic.ini"))
        command.upgrade(cfg, "head"); command.downgrade(cfg, "base")
        eng = create_engine(url)
        assert not {"alerts", "predictions", "sensor_readings"} & set(inspect(eng).get_table_names())
        command.upgrade(cfg, "head")
        assert {"alerts", "predictions", "sensor_readings"} <= set(inspect(eng).get_table_names())
        eng.dispose()
    finally:
        os.environ["DATABASE_URL"] = old if old else ""
        if not old: os.environ.pop("DATABASE_URL")
        with admin.connect() as c:
            c.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
