"""Load and validate the raw AI4I 2020 dataset."""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from ml.config import RAW_DATASET_PATH
from ml.preprocessing import (DATASET_COLUMN_MAP, FAULT_LABEL_COLUMNS, PRODUCT_TYPES, RAW_FEATURES,
                              TARGET_COLUMN, InvalidReadingError, validate_frame)

log = logging.getLogger(__name__)


@dataclass
class DataReport:
    """What load_data found and did. Stored in model_metrics.json."""
    path: str
    sha256: str
    rows_raw: int
    rows_after_cleaning: int
    missing_rows_dropped: int
    duplicate_rows_dropped: int
    failure_rows: int
    failure_rate: float
    label_inconsistencies: dict = field(default_factory=dict)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_raw_dataset(path: Path = RAW_DATASET_PATH) -> tuple[pd.DataFrame, DataReport]:
    """Read, validate and clean the dataset. Returns canonical column names.

    Policy:
      * missing values in required columns -> row dropped (logged)
      * exact duplicate rows -> dropped (logged)
      * implausible sensor values -> hard error (we do not silently repair data)
      * label inconsistencies (Machine failure vs. the five fault flags) -> REPORTED,
        labels left untouched
    """
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found at {path}")
    df = pd.read_csv(path).rename(columns=DATASET_COLUMN_MAP)

    required = RAW_FEATURES + [TARGET_COLUMN, "product_type"] + FAULT_LABEL_COLUMNS
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        raise InvalidReadingError(f"dataset is missing columns: {missing_cols}")
    df = df[required].copy()
    rows_raw = len(df)

    n_before = len(df)
    df = df.dropna(subset=required)
    missing_dropped = n_before - len(df)

    n_before = len(df)
    df = df.drop_duplicates()
    dup_dropped = n_before - len(df)

    validate_frame(df)
    bad_types = set(df["product_type"].unique()) - set(PRODUCT_TYPES)
    if bad_types:
        raise InvalidReadingError(f"unexpected product types: {sorted(bad_types)}")
    for col in [TARGET_COLUMN] + FAULT_LABEL_COLUMNS:
        if not set(df[col].unique()) <= {0, 1}:
            raise InvalidReadingError(f"{col} must be binary 0/1")
    df = df.reset_index(drop=True)

    any_flag = df[FAULT_LABEL_COLUMNS].max(axis=1)
    inconsistencies = {
        "failure_without_any_fault_flag": int(((df[TARGET_COLUMN] == 1) & (any_flag == 0)).sum()),
        "fault_flag_without_machine_failure": int(((df[TARGET_COLUMN] == 0) & (any_flag == 1)).sum()),
    }
    if any(inconsistencies.values()):
        log.warning("Label inconsistencies in raw data (left unchanged): %s", inconsistencies)

    report = DataReport(
        path=str(path.relative_to(path.parents[1])) if path.is_relative_to(path.parents[1]) else str(path),
        sha256=file_sha256(path), rows_raw=rows_raw, rows_after_cleaning=len(df),
        missing_rows_dropped=missing_dropped, duplicate_rows_dropped=dup_dropped,
        failure_rows=int(df[TARGET_COLUMN].sum()), failure_rate=float(df[TARGET_COLUMN].mean()),
        label_inconsistencies=inconsistencies,
    )
    log.info("Loaded %d rows (%d failures, %.2f%%)", len(df), report.failure_rows, 100 * report.failure_rate)
    return df, report
