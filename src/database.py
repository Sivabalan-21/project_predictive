"""LEGACY SQLite store used by the Streamlit and Flask demos.

Replaced by PostgreSQL + SQLAlchemy + Alembic in Phase 3. Conventions kept for the legacy UIs:
  * final_status is stored as plain "NORMAL" / "FAILURE" (no emoji)
  * a missing fault is stored as the string "None"
"""
from __future__ import annotations

import logging
import sqlite3
from contextlib import closing
from typing import Any, Mapping

import pandas as pd

from ml import config
from ml.prediction import PredictionResult

log = logging.getLogger(__name__)

DB_PATH = config.PROJECT_ROOT / "data" / "predictions.db"

_COLUMNS = ["air_temp", "process_temp", "rotational_speed", "torque", "tool_wear", "rf_prediction",
            "xgb_prediction", "iso_prediction", "final_status", "fault_type", "risk_score", "decision_method"]


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def create_tables() -> None:
    with closing(get_connection()) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                air_temp REAL, process_temp REAL, rotational_speed REAL, torque REAL, tool_wear REAL,
                rf_prediction INTEGER, xgb_prediction INTEGER, iso_prediction INTEGER,
                final_status TEXT, fault_type TEXT, risk_score REAL, decision_method TEXT
            )""")
        existing = {row[1] for row in conn.execute("PRAGMA table_info(predictions)")}
        for col, ddl in (("risk_score", "REAL"), ("decision_method", "TEXT")):  # upgrade pre-refactor DBs
            if col not in existing:
                conn.execute(f"ALTER TABLE predictions ADD COLUMN {col} {ddl}")
        conn.commit()


def insert_prediction(reading: Mapping[str, Any], result: PredictionResult) -> None:
    """Store one prediction. ``reading`` uses canonical sensor names (ml.preprocessing.RAW_FEATURES)."""
    row = (reading["air_temperature"], reading["process_temperature"], reading["rotational_speed"],
           reading["torque"], reading["tool_wear"], result.rf_prediction, result.xgb_prediction,
           result.isolation_prediction, result.final_status, result.fault_type or "None",
           result.risk_score, result.decision_method)
    with closing(get_connection()) as conn:
        conn.execute(f"INSERT INTO predictions ({', '.join(_COLUMNS)}) VALUES ({', '.join('?' * len(_COLUMNS))})", row)
        conn.commit()
    log.info("prediction stored status=%s fault=%s", result.final_status, result.fault_type)


def fetch_history(limit: int = 100) -> pd.DataFrame:
    with closing(get_connection()) as conn:
        df = pd.read_sql_query("SELECT * FROM predictions ORDER BY id DESC LIMIT ?", conn, params=(limit,))
    if not df.empty:  # rows written before the refactor may contain emoji status strings
        df["final_status"] = df["final_status"].map(
            lambda s: "FAILURE" if "FAILURE" in str(s) else ("NORMAL" if "NORMAL" in str(s) else str(s)))
        df["fault_type"] = df["fault_type"].fillna("None").map(lambda s: str(s).split("—")[0].strip())
    return df


def fault_counts(df: pd.DataFrame) -> dict[str, int]:
    """Count individual fault codes (a combined 'HDF+PWF' row counts for both)."""
    if df.empty:
        return {}
    codes = df.loc[df["fault_type"] != "None", "fault_type"].str.split("+").explode()
    return codes.value_counts().to_dict()
