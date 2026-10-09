"""INTEGRATION (real PostgreSQL + the real ML PredictionService): the transaction + idempotency + alert policy."""
from datetime import timedelta

import pytest
from sqlalchemy import text

from services.ingestion_service import IngestionService, PredictionError
from tests.phase3.conftest import NORMAL, OVERSTRAIN, make_event

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


def counts(sf):
    with sf() as s:
        return tuple(s.execute(text(f"SELECT count(*) FROM {t}")).scalar_one() for t in ("sensor_readings", "predictions", "alerts"))


@pytest.fixture()
def ingestion(session_factory, predictor):
    return IngestionService(session_factory, predictor)


def test_normal_event_persists_reading_and_prediction_but_no_alert(ingestion, session_factory):
    out = ingestion.ingest(make_event())
    assert not out.duplicate and out.alert is None and out.prediction.status == "NORMAL"
    assert out.prediction.sensor_reading_id == out.sensor_reading_id
    assert counts(session_factory) == (1, 1, 0)


def test_failure_event_creates_prediction_and_one_active_alert(ingestion, session_factory, predictor):
    out = ingestion.ingest(make_event(values=OVERSTRAIN, event_id="e-1"))
    expected = predictor.predict(OVERSTRAIN)                     # same single source of truth
    assert out.prediction.status == "FAILURE" and out.prediction.risk_score == pytest.approx(expected.risk_score)
    assert out.alert is not None and out.alert.status == "ACTIVE" and out.alert.fault_type == expected.fault_type
    with session_factory() as s:
        row = s.execute(text("SELECT prediction, risk_score, rf_probability, xgb_probability, model_version FROM predictions")).one()
    assert row.prediction == "FAILURE" and row.risk_score == pytest.approx(expected.risk_score)
    assert row.model_version == expected.model_version
    assert counts(session_factory) == (1, 1, 1)


def test_duplicate_delivery_changes_nothing(ingestion, session_factory):
    ev = make_event(values=OVERSTRAIN)
    assert not ingestion.ingest(ev).duplicate
    before = counts(session_factory)
    for _ in range(3):
        again = ingestion.ingest(ev)
        assert again.duplicate and again.prediction is None and again.alert is None
    assert counts(session_factory) == before == (1, 1, 1)


def test_new_failure_for_same_machine_and_fault_does_not_create_second_active_alert(ingestion, session_factory):
    first = ingestion.ingest(make_event(ts="2026-10-08T10:30:00Z", values=OVERSTRAIN))
    second = ingestion.ingest(make_event(ts="2026-10-08T10:30:01Z", values=OVERSTRAIN))
    assert first.alert is not None and second.alert is None and not second.duplicate
    assert counts(session_factory) == (2, 2, 1)


def test_different_machine_gets_its_own_alert(ingestion, session_factory):
    ingestion.ingest(make_event(machine_id="M001", values=OVERSTRAIN))
    out = ingestion.ingest(make_event(machine_id="M002", values=OVERSTRAIN))
    assert out.alert is not None and counts(session_factory)[2] == 2


def test_prediction_failure_rolls_back_the_reading(session_factory):
    class Broken:
        def predict(self, _): raise ValueError("model exploded")
    with pytest.raises(PredictionError):
        IngestionService(session_factory, Broken()).ingest(make_event())
    assert counts(session_factory) == (0, 0, 0)          # reading insert was rolled back with the transaction


def test_failure_after_prediction_insert_rolls_back_everything(session_factory, predictor):
    class BoomAlerts:
        def create_active_if_absent(self, *a, **k): raise RuntimeError("db died mid-transaction")
    with pytest.raises(RuntimeError):
        IngestionService(session_factory, predictor, alerts=BoomAlerts()).ingest(make_event(values=OVERSTRAIN))
    assert counts(session_factory) == (0, 0, 0)


def test_timestamps_with_offsets_are_stored_as_the_same_instant(ingestion, session_factory):
    ingestion.ingest(make_event(ts="2026-10-08T10:30:00Z"))
    assert ingestion.ingest(make_event(ts="2026-10-08T16:00:00+05:30")).duplicate
