"""Machine sensor simulator.

*** THIS IS A SIMULATION. It produces synthetic readings for demonstrating the streaming pipeline;
*** it is not real industrial sensor data and says nothing about real machines.

Model of normal operation (fitted to the normal rows of the AI4I 2020 dataset)
    air temperature      slow mean-reverting random walk, mean 300.0 K, sd 2.0 K
    process temperature  310.0 K + 0.657 * (air - 300) + small noise   (corr with air = 0.88, diff ~ 10 K)
    torque               mean-reverting around 39.6 Nm, sd 9.5 Nm
    rotational speed     2162.7 - 15.71 * torque + noise (sd 77 rpm)    (torque/speed corr = -0.89)
    tool wear            grows every reading; tool replaced at TOOL_CHANGE_WEAR_MIN
Readings are therefore correlated (never independent random numbers), and normal operation is kept clear of
the failure rules (power within range, no overstrain, no heat-dissipation condition).

Machine life cycle (per machine):  normal -> degrading (gradual drift) -> fault (condition fully
developed) -> recovering (maintenance) -> normal.  A scenario starts at random with ``failure_probability``
per reading, or on demand with ``SensorSimulator.inject()``.

Fault scenarios use the thresholds in ``ml/fault_classification.py`` (single source of truth - no second rule set):
    TWF  tool wear drifts into the 200-240 min window
    HDF  process-air temperature difference falls below 8.6 K while speed falls below 1380 rpm
    PWF  power leaves [3500, 9000] W (overload or under-power)
    OSF  tool wear x torque exceeds the product-type limit
    RNF  no sensor signature at all (random by definition) - a model that sees only sensors cannot flag it
The injected scenario is the simulator's own ground truth. It is logged but NOT included in the Kafka event.

Run standalone (events as JSON lines on stdout, logs on stderr):
    python -m simulation.sensor_simulator --interval 1 --machines 5 --failure-probability 0.005 --seed 42
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator, Mapping, Sequence

from ml import fault_classification as fc
from ml.preprocessing import SANITY_BOUNDS

log = logging.getLogger("simulation")

# ---- normal-operation model (fitted to AI4I normal rows; see module docstring)
AIR_MEAN, AIR_SD, AIR_PHI = 300.0, 2.0, 0.985
AIR_MIN, AIR_MAX = 295.4, 304.4
PROC_BASE, PROC_AIR_SLOPE, PROC_NOISE_SD, PROC_PHI = 310.0, 0.657, 0.71, 0.80
TORQUE_MEAN, TORQUE_SD, TORQUE_PHI = 39.6, 9.5, 0.88
TORQUE_MIN, TORQUE_MAX = 18.0, 70.0
RPM_INTERCEPT, RPM_SLOPE, RPM_NOISE_SD, RPM_PHI = 2162.7, -15.71, 77.0, 0.5
TOOL_CHANGE_WEAR_MIN = 200.0      # preventive tool change in normal operation (just below the TWF window)
NORMAL_POWER_RANGE_W = (fc.PWF_MIN_POWER_W + 100.0, fc.PWF_MAX_POWER_W - 200.0)
OSF_SAFETY_FACTOR = 0.95          # normal operation keeps wear x torque below 95% of the limit
PRODUCT_TYPE_CYCLE = ("L", "M", "L", "H", "L")

FAULT_CODES = ("TWF", "HDF", "PWF", "OSF", "RNF")
DEFAULT_WEIGHTS = {"TWF": 0.20, "HDF": 0.25, "PWF": 0.25, "OSF": 0.20, "RNF": 0.10}


@dataclass(frozen=True)
class SimulatedReading:
    machine_id: str
    timestamp: datetime
    air_temperature: float
    process_temperature: float
    rotational_speed: float
    torque: float
    tool_wear: float
    product_type: str
    scenario: str | None = None   # simulator ground truth (NOT sent to Kafka)
    phase: str = "normal"         # normal | degrading | fault | recovering

    def to_event_dict(self) -> dict:
        """The Kafka event payload (sensor fields only - no ground truth)."""
        return {
            "machine_id": self.machine_id,
            "timestamp": self.timestamp.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "air_temperature": self.air_temperature, "process_temperature": self.process_temperature,
            "rotational_speed": self.rotational_speed, "torque": self.torque, "tool_wear": self.tool_wear,
            "product_type": self.product_type,
        }


def _ar1(rng: random.Random, x: float, mean: float, sd: float, phi: float) -> float:
    return mean + phi * (x - mean) + sd * math.sqrt(1.0 - phi * phi) * rng.gauss(0.0, 1.0)


def _clip(x: float, low: float, high: float) -> float:
    return max(low, min(high, x))


def _smooth(p: float) -> float:
    return p * p * (3.0 - 2.0 * p)


def _power_w(torque: float, rpm: float) -> float:
    return fc.power_watts(torque, rpm)


@dataclass
class _Scenario:
    code: str
    ramp: int
    hold: int
    recover: int
    targets: dict
    wear_start: float = 0.0
    tick: int = 0
    tool_replaced: bool = False

    @property
    def phase(self) -> str:
        if self.tick < self.ramp:
            return "degrading"
        if self.tick < self.ramp + self.hold:
            return "fault"
        return "recovering"

    @property
    def progress(self) -> float:
        if self.tick < self.ramp:
            return _smooth(self.tick / max(self.ramp, 1))
        if self.tick < self.ramp + self.hold:
            return 1.0
        return _smooth(max(0.0, 1.0 - (self.tick - self.ramp - self.hold) / max(self.recover, 1)))

    @property
    def finished(self) -> bool:
        return self.tick >= self.ramp + self.hold + self.recover


class _Machine:
    def __init__(self, machine_id: str, index: int, seed: int | str | None, degradation_rate: float,
                 failure_probability: float, weights: Mapping[str, float]):
        self.id = machine_id
        # one RNG per machine (seeded from the master seed + id): output is independent of machine order
        self.rng = random.Random(f"{seed}/{machine_id}" if seed is not None else None)
        self.product_type = PRODUCT_TYPE_CYCLE[index % len(PRODUCT_TYPE_CYCLE)]
        self.osf_limit = fc.OSF_WEAR_TORQUE_LIMITS[self.product_type]
        self.rate, self.p_fail, self.weights = degradation_rate, failure_probability, weights
        r = self.rng
        self.air = AIR_MEAN + r.gauss(0, AIR_SD * 0.5)
        self.proc_noise = 0.0
        self.torque_state = TORQUE_MEAN + r.gauss(0, 3)
        self.rpm_noise = 0.0
        self.wear = r.uniform(0.0, 150.0)     # machines start at different points of tool life
        self.scenario: _Scenario | None = None

    # -------------------------------------------------------------- scenarios
    def start_scenario(self, code: str, ramp: int | None = None, hold: int | None = None,
                       recover: int | None = None) -> None:
        if code not in FAULT_CODES:
            raise ValueError(f"unknown fault code {code!r}; choose from {FAULT_CODES}")
        r = self.rng
        targets: dict = {}
        if code == "TWF":
            targets["wear"] = min(max(self.wear + 10.0, r.uniform(fc.TWF_WINDOW_START_MIN + 5, fc.TWF_WINDOW_END_MIN - 2)), 245.0)
        elif code == "HDF":
            # Real AI4I HDF rows sit just inside the rule (diff ~8.2 K, rpm ~1340) with HIGH torque (~53 Nm)
            # because torque and speed are coupled; the scenario keeps that coupling.
            targets.update(diff=fc.HDF_MAX_TEMP_DIFF_K - 0.6 + r.uniform(-0.4, 0.3),
                           rpm=fc.HDF_MAX_RPM - 60.0 + r.uniform(-80.0, 40.0))
        elif code == "PWF":
            if r.random() < 0.5:   # overload: torque high while speed is held -> power above the upper limit
                targets.update(torque=66.0, rpm=1500.0, variant="overload")
            else:                  # under-power: low torque at moderate speed -> power below the lower limit
                targets.update(torque=20.0, rpm=1420.0, variant="underpower")
        elif code == "OSF":
            wear_t = max(self.wear, self.osf_limit / 66.0)
            torque_t = min(TORQUE_MAX, 1.12 * self.osf_limit / wear_t)
            targets.update(wear=wear_t, torque=torque_t, rpm=RPM_INTERCEPT + RPM_SLOPE * torque_t)
        self.scenario = _Scenario(
            code=code, ramp=ramp if ramp is not None else r.randint(15, 35),
            hold=hold if hold is not None else r.randint(8, 16),
            recover=recover if recover is not None else r.randint(4, 8), targets=targets, wear_start=self.wear)
        log.info("%s: failure injected (%s) - gradual degradation begins [simulation]", self.id, code)

    def _maybe_start_random(self) -> None:
        if self.scenario is None and self.p_fail > 0 and self.rng.random() < self.p_fail:
            codes = list(self.weights)
            self.start_scenario(self.rng.choices(codes, weights=[self.weights[c] for c in codes])[0])

    # -------------------------------------------------------------- one reading
    def next_reading(self, timestamp: datetime) -> SimulatedReading:
        r = self.rng
        self._maybe_start_random()
        sc = self.scenario

        # --- baseline (normal) values from correlated processes
        self.air = _clip(_ar1(r, self.air, AIR_MEAN, AIR_SD, AIR_PHI), AIR_MIN, AIR_MAX)
        self.proc_noise = _ar1(r, self.proc_noise, 0.0, PROC_NOISE_SD, PROC_PHI)
        self.torque_state = _clip(_ar1(r, self.torque_state, TORQUE_MEAN, TORQUE_SD, TORQUE_PHI), TORQUE_MIN, TORQUE_MAX)
        self.rpm_noise = _ar1(r, self.rpm_noise, 0.0, RPM_NOISE_SD, RPM_PHI)

        worn_tool_scenario = sc is not None and sc.code in ("TWF", "OSF") and not sc.tool_replaced
        if not worn_tool_scenario:
            self.wear += self.rate * r.uniform(0.8, 1.2)
            if self.wear >= TOOL_CHANGE_WEAR_MIN:
                self.wear = r.uniform(0.0, 3.0)  # tool change
                log.debug("%s: tool changed", self.id)

        air = self.air
        torque = min(self.torque_state, max(TORQUE_MIN, OSF_SAFETY_FACTOR * self.osf_limit / max(self.wear, 1.0)))
        rpm = RPM_INTERCEPT + RPM_SLOPE * torque + self.rpm_noise
        process = PROC_BASE + PROC_AIR_SLOPE * (air - AIR_MEAN) + self.proc_noise
        if process - air < fc.HDF_MAX_TEMP_DIFF_K + 0.2 and rpm < fc.HDF_MAX_RPM + 30.0:
            process = air + fc.HDF_MAX_TEMP_DIFF_K + 0.2 + abs(r.gauss(0, 0.2))   # keep clear of the HDF rule
        low_w, high_w = NORMAL_POWER_RANGE_W
        k = 2.0 * math.pi / 60.0
        rpm = _clip(rpm, low_w / (torque * k), high_w / (torque * k))             # keep power in range
        wear = self.wear

        # --- apply the scenario (blend baseline -> fault targets)
        scenario_code, phase = None, "normal"
        if sc is not None:
            scenario_code, phase = sc.code, sc.phase
            if sc.tick == sc.ramp + sc.hold and sc.code in ("TWF", "OSF") and not sc.tool_replaced:
                self.wear, sc.tool_replaced = r.uniform(0.0, 3.0), True     # maintenance replaces the worn tool
                wear = self.wear
            p, t = sc.progress, sc.targets
            if sc.code in ("TWF", "OSF") and not sc.tool_replaced:
                wear = max(self.wear, sc.wear_start + p * (t["wear"] - sc.wear_start))   # wear drifts up
                self.wear = wear
            elif sc.tool_replaced:
                wear = self.wear
            if sc.code == "HDF":
                diff = (process - air) + p * (t["diff"] - (process - air))
                process = air + diff
                rpm = rpm + p * (t["rpm"] - rpm)
                cap = max(TORQUE_MIN, OSF_SAFETY_FACTOR * self.osf_limit / max(wear, 1.0))
                coupled = _clip((RPM_INTERCEPT - rpm) / -RPM_SLOPE, TORQUE_MIN, cap)  # torque follows speed
                torque = torque + p * (coupled - torque)
            elif sc.code == "PWF":
                torque = torque + p * (t["torque"] - torque)
                rpm = rpm + p * (t["rpm"] - rpm)
            elif sc.code == "OSF":
                torque = torque + p * (t["torque"] - torque)
                rpm = rpm + p * (t["rpm"] - rpm)
            if sc.code != "HDF" and process - air < fc.HDF_MAX_TEMP_DIFF_K + 0.2 and rpm < fc.HDF_MAX_RPM + 30.0:
                process = air + fc.HDF_MAX_TEMP_DIFF_K + 0.2 + abs(r.gauss(0, 0.2))  # no accidental HDF side effect
            sc.tick += 1
            if sc.finished:
                log.info("%s: %s scenario over - maintenance done, back to normal [simulation]", self.id, sc.code)
                self.scenario = None
            elif sc.tick == sc.ramp:
                log.info("%s: %s fault fully developed [simulation]", self.id, sc.code)

        def bounded(name: str, value: float, digits: int) -> float:
            low, high = SANITY_BOUNDS[name]
            return round(_clip(value, low, high), digits)

        return SimulatedReading(
            machine_id=self.id, timestamp=timestamp, product_type=self.product_type,
            air_temperature=bounded("air_temperature", air, 1),
            process_temperature=bounded("process_temperature", process, 1),
            rotational_speed=bounded("rotational_speed", rpm, 0), torque=bounded("torque", torque, 1),
            tool_wear=bounded("tool_wear", wear, 1), scenario=scenario_code, phase=phase)


class SensorSimulator:
    """Generates one reading per machine per step. Deterministic for a given ``seed`` (timestamps excepted
    unless a fixed ``clock`` is supplied)."""

    def __init__(self, n_machines: int = 5, failure_probability: float = 0.005, degradation_rate: float = 0.3,
                 seed: int | None = None, clock: Callable[[], datetime] | None = None,
                 scenario_weights: Mapping[str, float] | None = None):
        if not 1 <= n_machines <= 999:
            raise ValueError("n_machines must be between 1 and 999")
        if not 0.0 <= failure_probability <= 1.0:
            raise ValueError("failure_probability must be between 0 and 1")
        if degradation_rate < 0:
            raise ValueError("degradation_rate must be >= 0 (simulated tool-wear minutes per reading)")
        weights = dict(DEFAULT_WEIGHTS if scenario_weights is None else scenario_weights)
        if not weights or set(weights) - set(FAULT_CODES) or sum(weights.values()) <= 0:
            raise ValueError(f"scenario_weights must be positive weights over {FAULT_CODES}")
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._last_ts: datetime | None = None
        self._machines = {f"MACHINE-{i + 1:03d}": _Machine(f"MACHINE-{i + 1:03d}", i, seed, degradation_rate,
                                                             failure_probability, weights)
                          for i in range(n_machines)}

    @property
    def machine_ids(self) -> list[str]:
        return list(self._machines)

    def inject(self, machine_id: str, code: str, **timing: int) -> None:
        """Start a failure scenario now (``ramp``/``hold``/``recover`` = length in readings)."""
        if machine_id not in self._machines:
            raise KeyError(f"unknown machine {machine_id!r}")
        self._machines[machine_id].start_scenario(code, **timing)

    def step(self) -> list[SimulatedReading]:
        """One reading per machine. Timestamps (UTC, millisecond resolution) strictly increase from step to
        step, so (machine_id, timestamp) uniquely identifies a reading even with a very small interval."""
        now = self._clock().astimezone(timezone.utc)
        now = now.replace(microsecond=now.microsecond // 1000 * 1000)
        if self._last_ts is not None and now <= self._last_ts:
            now = self._last_ts + timedelta(milliseconds=1)
        self._last_ts = now
        return [m.next_reading(now) for m in self._machines.values()]

    def stream(self, interval: float = 1.0, max_ticks: int | None = None,
               should_stop: Callable[[], bool] | None = None,
               sleep: Callable[[float], None] = time.sleep) -> Iterator[SimulatedReading]:
        """Yield readings forever (or ``max_ticks`` steps), one step every ``interval`` seconds."""
        if interval < 0:
            raise ValueError("interval must be >= 0")
        stop = should_stop or (lambda: False)
        tick = 0
        next_at = time.monotonic()
        while not stop() and (max_ticks is None or tick < max_ticks):
            for reading in self.step():
                yield reading
            tick += 1
            next_at += interval
            while not stop():                       # sleep in short slices so Ctrl+C / SIGTERM is prompt
                remaining = next_at - time.monotonic()
                if remaining <= 0:
                    break
                sleep(min(remaining, 0.2))


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="python -m simulation.sensor_simulator",
                                 description="Synthetic machine sensor simulator (JSON lines on stdout). SIMULATION ONLY.")
    ap.add_argument("--interval", type=float, default=1.0, help="seconds between readings of each machine (default 1.0)")
    ap.add_argument("--machines", type=int, default=5, help="number of machines (default 5)")
    ap.add_argument("--failure-probability", type=float, default=0.005,
                    help="per-machine, per-reading probability of starting a failure scenario (default 0.005)")
    ap.add_argument("--degradation-rate", type=float, default=0.3,
                    help="simulated tool-wear minutes added per reading (default 0.3; time-compressed)")
    ap.add_argument("--seed", type=int, default=None, help="random seed for reproducible output")
    ap.add_argument("--max-readings", type=int, default=None, help="stop after this many readings (default: run until Ctrl+C)")
    ap.add_argument("--log-level", default="INFO")
    return ap


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(), stream=sys.stderr,
                        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
    try:
        sim = SensorSimulator(args.machines, args.failure_probability, args.degradation_rate, args.seed)
    except ValueError as exc:
        log.error("invalid configuration: %s", exc)
        return 2
    stopping = False

    def _stop(*_):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    log.info("Simulator started: %d machines, interval %.2fs, failure_probability %.4f, seed %s (SIMULATED DATA)",
             args.machines, args.interval, args.failure_probability, args.seed)
    count = 0
    for reading in sim.stream(args.interval, should_stop=lambda: stopping or (
            args.max_readings is not None and count >= args.max_readings)):
        sys.stdout.write(json.dumps(reading.to_event_dict()) + "\n")   # data output of this CLI (not logging)
        sys.stdout.flush()
        count += 1
        if args.max_readings is not None and count >= args.max_readings:
            break
    log.info("Simulator stopped after %d readings", count)
    return 0


if __name__ == "__main__":
    sys.exit(main())
