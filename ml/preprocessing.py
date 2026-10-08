"""Feature definitions, input validation and feature engineering.

Shared by the training pipeline AND the prediction service, so both transform data
in exactly the same way (no train/serve skew).
"""
from __future__ import annotations

import math
from typing import Iterable, Mapping

import numpy as np
import pandas as pd

# Canonical (snake_case) sensor names, used everywhere inside this project.
RAW_FEATURES: list[str] = [
    "air_temperature",      # K
    "process_temperature",  # K
    "rotational_speed",     # rpm
    "torque",               # Nm
    "tool_wear",            # min
]

# Column names in the original AI4I 2020 CSV -> canonical names.
DATASET_COLUMN_MAP: dict[str, str] = {
    "Air temperature": "air_temperature",
    "Process temperature": "process_temperature",
    "Rotational speed": "rotational_speed",
    "Torque": "torque",
    "Tool wear": "tool_wear",
    "Type": "product_type",
    "Machine failure": "machine_failure",
}
FAULT_LABEL_COLUMNS: list[str] = ["TWF", "HDF", "PWF", "OSF", "RNF"]
TARGET_COLUMN = "machine_failure"
PRODUCT_TYPES = ("L", "M", "H")

# Derived features. Definitions follow the AI4I 2020 dataset description (UCI),
# where HDF depends on the temperature difference, PWF on mechanical power and
# OSF on tool wear x torque.
ENGINEERED_FEATURES: list[str] = ["temp_diff", "power_w", "wear_torque"]

FEATURE_SETS: dict[str, list[str]] = {
    "raw": RAW_FEATURES,
    "engineered": RAW_FEATURES + ENGINEERED_FEATURES,
}

# Plausibility bounds for REJECTING garbage input (sensor glitches, unit errors).
# These are deliberately wider than the AI4I observed ranges: they are an
# engineering assumption, NOT statistics of the training data.
SANITY_BOUNDS: dict[str, tuple[float, float]] = {
    "air_temperature": (250.0, 350.0),
    "process_temperature": (250.0, 400.0),
    "rotational_speed": (0.0, 10_000.0),
    "torque": (0.0, 200.0),
    "tool_wear": (0.0, 1_000.0),
}


class InvalidReadingError(ValueError):
    """A sensor reading (or dataset) failed validation."""


def validate_values(values: Mapping[str, object]) -> dict[str, float]:
    """Validate one reading; return clean float values. Raises InvalidReadingError."""
    problems: list[str] = []
    clean: dict[str, float] = {}
    for name in RAW_FEATURES:
        if name not in values or values[name] is None:
            problems.append(f"{name}: missing")
            continue
        raw = values[name]
        if isinstance(raw, bool):
            problems.append(f"{name}: boolean is not a valid number")
            continue
        try:
            number = float(raw)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            problems.append(f"{name}: not a number ({raw!r})")
            continue
        if not math.isfinite(number):
            problems.append(f"{name}: must be finite")
            continue
        low, high = SANITY_BOUNDS[name]
        if not low <= number <= high:
            problems.append(f"{name}: {number} outside plausible range [{low}, {high}]")
            continue
        clean[name] = number
    if problems:
        raise InvalidReadingError("; ".join(problems))
    return clean


def validate_product_type(product_type: str | None) -> str | None:
    if product_type is None:
        return None
    value = str(product_type).strip().upper()
    if value not in PRODUCT_TYPES:
        raise InvalidReadingError(f"product_type: {product_type!r} not in {PRODUCT_TYPES}")
    return value


def add_engineered_features(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of ``df`` with the derived features appended."""
    out = df.copy()
    out["temp_diff"] = out["process_temperature"] - out["air_temperature"]
    # mechanical power in watts: torque [Nm] x angular speed [rad/s]
    out["power_w"] = out["torque"] * out["rotational_speed"] * 2.0 * np.pi / 60.0
    out["wear_torque"] = out["tool_wear"] * out["torque"]
    return out


def build_feature_frame(df: pd.DataFrame, feature_set: str) -> pd.DataFrame:
    """Select / derive the model input columns, in a fixed order."""
    if feature_set not in FEATURE_SETS:
        raise ValueError(f"unknown feature_set {feature_set!r}; choose from {list(FEATURE_SETS)}")
    missing = [c for c in RAW_FEATURES if c not in df.columns]
    if missing:
        raise InvalidReadingError(f"missing columns: {missing}")
    frame = add_engineered_features(df[RAW_FEATURES].astype("float64"))
    return frame[FEATURE_SETS[feature_set]]


def validate_frame(df: pd.DataFrame, required: Iterable[str] = RAW_FEATURES) -> None:
    """Vectorised validation for a whole DataFrame of readings."""
    required = list(required)
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise InvalidReadingError(f"missing columns: {missing}")
    problems: list[str] = []
    for name in RAW_FEATURES:
        if name not in required:
            continue
        col = pd.to_numeric(df[name], errors="coerce")
        if col.isna().any():
            problems.append(f"{name}: {int(col.isna().sum())} missing/non-numeric values")
            continue
        if not np.isfinite(col.to_numpy(dtype="float64")).all():
            problems.append(f"{name}: contains infinite values")
            continue
        low, high = SANITY_BOUNDS[name]
        bad = int(((col < low) | (col > high)).sum())
        if bad:
            problems.append(f"{name}: {bad} values outside plausible range [{low}, {high}]")
    if problems:
        raise InvalidReadingError("; ".join(problems))
