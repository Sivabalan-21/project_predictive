import json
from datetime import datetime, timezone

import numpy as np
import pytest

from ml.fault_classification import fault_indicators
from ml.preprocessing import RAW_FEATURES, SANITY_BOUNDS
from simulation.sensor_simulator import FAULT_CODES, SensorSimulator, main
from streaming.schemas import SensorEvent

FIXED = datetime(2026, 10, 6, 11, 30, 1, tzinfo=timezone.utc)
clock = lambda: FIXED  # noqa: E731


def sensors(r):
    return {k: getattr(r, k) for k in RAW_FEATURES}


def run(sim, n):
    return [r for _ in range(n) for r in sim.step()]


def machine_run(sim, machine_id, n):
    return [r for _ in range(n) for r in sim.step() if r.machine_id == machine_id]


# ---------------------------------------------------------------- machines / ranges / timestamps
def test_default_machines_are_the_five_required_ids():
    assert SensorSimulator(seed=1).machine_ids == [f"MACHINE-00{i}" for i in range(1, 6)]


@pytest.mark.parametrize("kwargs", [{"n_machines": 0}, {"n_machines": 1000}, {"failure_probability": 1.5},
                                    {"failure_probability": -0.1}, {"degradation_rate": -1}, {"scenario_weights": {"XXX": 1}}])
def test_invalid_configuration_rejected(kwargs):
    with pytest.raises(ValueError):
        SensorSimulator(**kwargs)


def test_every_reading_is_a_valid_event_within_ranges():
    sim = SensorSimulator(5, failure_probability=0.05, seed=7, clock=clock)   # lots of scenarios
    for r in run(sim, 1500):
        SensorEvent.from_dict(r.to_event_dict())      # schema + SANITY_BOUNDS
        for name in RAW_FEATURES:
            low, high = SANITY_BOUNDS[name]
            assert low <= getattr(r, name) <= high


def test_normal_operation_resembles_ai4i_ranges():
    rows = run(SensorSimulator(5, 0.0, seed=3, clock=clock), 2000)
    air = np.array([r.air_temperature for r in rows])
    assert 295.0 <= air.min() and air.max() <= 305.0 and abs(air.mean() - 300.0) < 1.0
    assert 305.0 <= min(r.process_temperature for r in rows) and max(r.process_temperature for r in rows) <= 315.0
    assert all(18 <= r.torque <= 70 for r in rows) and all(r.tool_wear <= 200.0 for r in rows)


def test_sensor_values_are_correlated_not_independent():
    rows = run(SensorSimulator(5, 0.0, seed=3, clock=clock), 2000)
    col = lambda n: np.array([getattr(r, n) for r in rows])  # noqa: E731
    assert np.corrcoef(col("torque"), col("rotational_speed"))[0, 1] < -0.8        # AI4I: -0.89
    assert np.corrcoef(col("air_temperature"), col("process_temperature"))[0, 1] > 0.7   # AI4I: 0.88


def test_normal_operation_stays_clear_of_fault_rules():
    rows = run(SensorSimulator(5, 0.0, seed=11, clock=clock), 3000)
    flagged = sum(1 for r in rows if fault_indicators(sensors(r), r.product_type))
    assert flagged / len(rows) < 0.002 and all(r.scenario is None and r.phase == "normal" for r in rows)


def test_timestamps_are_timezone_aware_utc_and_serialised_with_z():
    r = SensorSimulator(1, seed=1, clock=clock).step()[0]
    assert r.timestamp == FIXED and r.to_event_dict()["timestamp"] == "2026-10-06T11:30:01.000Z"
    live = SensorSimulator(1, seed=1).step()[0]
    assert live.timestamp.tzinfo is not None and abs((datetime.now(timezone.utc) - live.timestamp).total_seconds()) < 5


def test_timestamps_strictly_increase_so_machine_and_timestamp_identify_a_reading():
    for sim in (SensorSimulator(5, 0.0, seed=1, clock=clock), SensorSimulator(5, 0.0, seed=1)):   # frozen AND real clock
        rows = run(sim, 300)
        keys = [(r.machine_id, r.timestamp) for r in rows]
        assert len(set(keys)) == len(keys)
        steps = [sim.step()[0].timestamp for _ in range(20)]
        assert all(b > a for a, b in zip(steps, steps[1:]))


# ---------------------------------------------------------------- determinism
def test_same_seed_gives_identical_output_and_different_seed_differs():
    a = run(SensorSimulator(5, 0.05, seed=42, clock=clock), 200)
    b = run(SensorSimulator(5, 0.05, seed=42, clock=clock), 200)
    c = run(SensorSimulator(5, 0.05, seed=43, clock=clock), 200)
    assert a == b and a != c


def test_machine_output_does_not_depend_on_how_many_machines_run():
    five = machine_run(SensorSimulator(5, 0.02, seed=5, clock=clock), "MACHINE-002", 300)
    three = machine_run(SensorSimulator(3, 0.02, seed=5, clock=clock), "MACHINE-002", 300)
    assert five == three


# ---------------------------------------------------------------- failure injection
@pytest.mark.parametrize("code", ["TWF", "HDF", "PWF", "OSF"])
def test_injected_fault_meets_the_phase1_rule_for_that_fault(code):
    """The scenario must satisfy ml/fault_classification.py (no second, incompatible rule set)."""
    for seed in range(15):
        sim = SensorSimulator(5, 0.0, seed=seed, clock=clock)
        run(sim, 3)
        sim.inject("MACHINE-002", code, ramp=20, hold=10, recover=5)
        rs = machine_run(sim, "MACHINE-002", 20 + 10 + 5 + 5)
        hold = [r for r in rs if r.phase == "fault"]
        assert len(hold) == 10 and all(r.scenario == code for r in hold)
        assert all(code in fault_indicators(sensors(r), r.product_type) for r in hold), (code, seed)


def test_phases_follow_the_life_cycle_and_machine_returns_to_normal():
    sim = SensorSimulator(5, 0.0, seed=2, clock=clock)
    sim.inject("MACHINE-001", "HDF", ramp=6, hold=4, recover=3)
    rs = machine_run(sim, "MACHINE-001", 20)
    assert [r.phase for r in rs[:13]] == ["degrading"] * 6 + ["fault"] * 4 + ["recovering"] * 3
    assert all(r.phase == "normal" and r.scenario is None for r in rs[13:])
    assert not any(fault_indicators(sensors(r), r.product_type) for r in rs[16:])


def test_degradation_is_gradual():
    sim = SensorSimulator(5, 0.0, seed=4, clock=clock)
    sim.inject("MACHINE-003", "HDF", ramp=30, hold=5, recover=5)
    rs = machine_run(sim, "MACHINE-003", 40)
    diffs = [r.process_temperature - r.air_temperature for r in rs[:36]]
    assert np.mean(diffs[:5]) > np.mean(diffs[15:20]) > np.mean(diffs[-5:])        # temperature gap shrinks steadily


def test_rnf_has_no_sensor_signature():
    sim = SensorSimulator(5, 0.0, seed=2, clock=clock)
    sim.inject("MACHINE-001", "RNF", ramp=5, hold=5, recover=2)
    rs = machine_run(sim, "MACHINE-001", 12)
    assert {r.scenario for r in rs[:12]} == {"RNF"}
    assert not any(fault_indicators(sensors(r), r.product_type) for r in rs)


def test_ground_truth_is_not_part_of_the_kafka_event():
    sim = SensorSimulator(5, 0.0, seed=2, clock=clock)
    sim.inject("MACHINE-001", "OSF", ramp=3, hold=3, recover=2)
    r = [x for x in sim.step() if x.machine_id == "MACHINE-001"][0]
    assert r.scenario == "OSF" and "scenario" not in r.to_event_dict() and "phase" not in r.to_event_dict()


def test_random_failure_probability_controls_injection():
    never = run(SensorSimulator(5, 0.0, seed=9, clock=clock), 500)
    always = run(SensorSimulator(5, 1.0, seed=9, clock=clock), 50)
    assert not any(r.scenario for r in never)
    assert {r.scenario for r in always} - {None} >= {"TWF", "HDF", "PWF", "OSF"} - {"RNF"} or len({r.scenario for r in always} - {None}) >= 3


def test_inject_validates_arguments():
    sim = SensorSimulator(5, seed=1, clock=clock)
    with pytest.raises(KeyError):
        sim.inject("MACHINE-099", "TWF")
    with pytest.raises(ValueError):
        sim.inject("MACHINE-001", "XYZ")
    assert set(FAULT_CODES) == {"TWF", "HDF", "PWF", "OSF", "RNF"}


def test_tool_wear_grows_with_degradation_rate_and_tools_get_replaced():
    fast = machine_run(SensorSimulator(1, 0.0, degradation_rate=2.0, seed=1, clock=clock), "MACHINE-001", 300)
    wear = [r.tool_wear for r in fast]
    assert max(wear) <= 200.0 and any(b < a - 50 for a, b in zip(wear, wear[1:]))     # replaced after reaching 200
    slow = machine_run(SensorSimulator(1, 0.0, degradation_rate=0.0, seed=1, clock=clock), "MACHINE-001", 50)
    assert len({r.tool_wear for r in slow}) == 1


# ---------------------------------------------------------------- streaming + CLI
def test_stream_respects_max_ticks_interval_and_stop():
    sleeps = []
    sim = SensorSimulator(5, 0.0, seed=1, clock=clock)
    out = list(sim.stream(interval=0.5, max_ticks=3, sleep=sleeps.append))
    assert len(out) == 15 and all(0 < s <= 0.2 for s in sleeps)
    stopped = {"n": 0}

    def stop():
        stopped["n"] += 1
        return stopped["n"] > 2
    assert len(list(SensorSimulator(5, 0.0, seed=1, clock=clock).stream(0.0, should_stop=stop))) == 5


def test_cli_prints_valid_json_lines_and_is_reproducible(capsys):
    assert main(["--interval", "0", "--machines", "5", "--seed", "42", "--max-readings", "12"]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 12
    events = [SensorEvent.from_payload(line) for line in lines]
    assert [e.machine_id for e in events[:5]] == [f"MACHINE-00{i}" for i in range(1, 6)]
    assert main(["--interval", "0", "--machines", "5", "--seed", "42", "--max-readings", "12"]) == 0
    again = [json.loads(x) for x in capsys.readouterr().out.strip().splitlines()]
    assert [{k: v for k, v in json.loads(a).items() if k != "timestamp"} for a in lines] == \
           [{k: v for k, v in b.items() if k != "timestamp"} for b in again]


def test_cli_rejects_bad_configuration(capsys):
    assert main(["--machines", "0", "--max-readings", "1"]) == 2
