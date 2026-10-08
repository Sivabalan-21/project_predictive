"""Rule-based fault classification (kept separate from the binary failure models).

Rules follow the AI4I 2020 dataset description (UCI). The dataset is SYNTHETIC: it was
generated from these very rules, so they reproduce the dataset labels exactly
(see ``fault_rules_validation`` in reports/model_metrics.json). That does NOT prove
real machines obey the same thresholds.

Each rule carries a ``basis`` tag so the origin of every threshold is explicit:
  - "dataset_definition": stated in the AI4I 2020 dataset description
  - "engineering_assumption": chosen by us, not validated
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping

# --- Thresholds (all "dataset_definition" unless noted) -----------------------------
HDF_MAX_TEMP_DIFF_K = 8.6        # HDF: (process - air temperature) below this ...
HDF_MAX_RPM = 1380.0             # ... AND rotational speed below this
PWF_MIN_POWER_W = 3500.0         # PWF: power outside [3500, 9000] W
PWF_MAX_POWER_W = 9000.0
OSF_WEAR_TORQUE_LIMITS = {       # OSF: tool_wear x torque above (minNm) by product type
    "L": 11_000.0,
    "M": 12_000.0,
    "H": 13_000.0,
}
TWF_WINDOW_START_MIN = 200.0     # TWF: tool replaced/fails at a RANDOM time in 200-240 min
TWF_WINDOW_END_MIN = 240.0

# Assumption: the stream schema carries no product type, so when it is missing we test
# OSF against the most sensitive (lowest) limit instead of guessing a type.
OSF_DEFAULT_LIMIT = min(OSF_WEAR_TORQUE_LIMITS.values())

FAULT_NAMES: dict[str, str] = {
    "TWF": "Tool Wear Failure",
    "HDF": "Heat Dissipation Failure",
    "PWF": "Power Failure",
    "OSF": "Overstrain Failure",
    "RNF": "Random Failure",
}

RULE_BASIS: dict[str, dict[str, str]] = {
    "TWF": {
        "rule": f"tool_wear >= {TWF_WINDOW_START_MIN:.0f} min (entered the 200-240 min failure window)",
        "basis": "dataset_definition",
        "caveat": "Failure time inside the window is random, so this flags RISK only; "
                  "precision is low by construction.",
    },
    "HDF": {
        "rule": f"(process_temp - air_temp) < {HDF_MAX_TEMP_DIFF_K} K AND rpm < {HDF_MAX_RPM:.0f}",
        "basis": "dataset_definition",
        "caveat": "Exact for the synthetic dataset; unvalidated on real machines.",
    },
    "PWF": {
        "rule": f"power = torque x rpm x 2*pi/60 [W] outside [{PWF_MIN_POWER_W:.0f}, {PWF_MAX_POWER_W:.0f}]",
        "basis": "dataset_definition",
        "caveat": "Exact for the synthetic dataset; unvalidated on real machines.",
    },
    "OSF": {
        "rule": "tool_wear x torque > 11000 (L) / 12000 (M) / 13000 (H) minNm",
        "basis": "dataset_definition",
        "caveat": f"Product type is optional in the stream; if absent the L limit "
                  f"({OSF_DEFAULT_LIMIT:.0f}) is used (engineering assumption).",
    },
    "RNF": {
        "rule": "no sensor signature (0.1% random chance per run in the dataset)",
        "basis": "dataset_definition",
        "caveat": "Not detectable from sensors. We only assign RNF as a RESIDUAL label "
                  "when a failure is flagged and no other rule explains it "
                  "(engineering assumption).",
    },
}


@dataclass(frozen=True)
class FaultAssessment:
    """Result of evaluating all rules for one reading."""
    indicators: tuple[str, ...]   # codes whose sensor condition holds (TWF/HDF/PWF/OSF)
    codes: tuple[str, ...]        # codes reported for a flagged failure (adds RNF residual)

    @property
    def fault_type(self) -> str | None:
        return "+".join(self.codes) if self.codes else None


def power_watts(torque_nm: float, rpm: float) -> float:
    return torque_nm * rpm * 2.0 * math.pi / 60.0


def fault_indicators(reading: Mapping[str, float], product_type: str | None = None) -> tuple[str, ...]:
    """Return the deterministic fault codes whose sensor conditions hold (no RNF)."""
    found: list[str] = []
    if reading["tool_wear"] >= TWF_WINDOW_START_MIN:
        found.append("TWF")
    if (reading["process_temperature"] - reading["air_temperature"]) < HDF_MAX_TEMP_DIFF_K \
            and reading["rotational_speed"] < HDF_MAX_RPM:
        found.append("HDF")
    power = power_watts(reading["torque"], reading["rotational_speed"])
    if power < PWF_MIN_POWER_W or power > PWF_MAX_POWER_W:
        found.append("PWF")
    limit = OSF_WEAR_TORQUE_LIMITS.get(product_type or "", OSF_DEFAULT_LIMIT)
    if reading["tool_wear"] * reading["torque"] > limit:
        found.append("OSF")
    return tuple(found)


def classify(reading: Mapping[str, float], product_type: str | None, is_failure: bool) -> FaultAssessment:
    """Fault assessment for one reading.

    ``indicators`` is always computed. ``codes`` is only populated when the failure
    models flagged a failure; if no rule explains it, the residual code RNF is used.
    """
    indicators = fault_indicators(reading, product_type)
    if not is_failure:
        return FaultAssessment(indicators=indicators, codes=())
    return FaultAssessment(indicators=indicators, codes=indicators or ("RNF",))
