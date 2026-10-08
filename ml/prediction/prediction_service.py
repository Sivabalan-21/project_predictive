"""THE single prediction implementation.

Streamlit, Flask and (later) the Kafka consumer / FastAPI backend must all call this
class. Do not re-implement prediction logic anywhere else.
"""
from __future__ import annotations

import functools
import logging
import threading
from dataclasses import dataclass, asdict
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from ml.ensemble import decide, risk_score
from ml.fault_classification import classify
from ml.prediction.model_loader import ModelBundle, load_bundle
from ml.preprocessing import (RAW_FEATURES, InvalidReadingError, build_feature_frame, validate_product_type,
                              validate_values)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SensorReading:
    air_temperature: float       # K
    process_temperature: float   # K
    rotational_speed: float      # rpm
    torque: float                # Nm
    tool_wear: float             # min
    product_type: str | None = None  # optional "L" / "M" / "H" (only refines the OSF rule)

    @staticmethod
    def from_mapping(values: Mapping[str, Any]) -> "SensorReading":
        """Validate and build. Raises InvalidReadingError with every problem found."""
        clean = validate_values(values)
        return SensorReading(**clean, product_type=validate_product_type(values.get("product_type")))


@dataclass(frozen=True)
class PredictionResult:
    final_status: str                    # "NORMAL" | "FAILURE"
    is_failure: bool
    risk_score: float                    # mean(RF, XGB) failure probability; NOT calibrated
    rf_probability: float
    xgb_probability: float
    isolation_anomaly_score: float       # higher = more anomalous (uncalibrated)
    rf_prediction: int                   # each model at its own default 0.5 rule / IF flag
    xgb_prediction: int
    isolation_prediction: int
    fault_type: str | None               # e.g. "HDF" or "HDF+PWF"; None when status is NORMAL
    fault_codes: tuple[str, ...]
    fault_indicators: tuple[str, ...]    # rule conditions that hold, regardless of status
    decision_method: str
    model_version: str

    def to_dict(self) -> dict:
        d = asdict(self)
        d["fault_codes"] = list(self.fault_codes)
        d["fault_indicators"] = list(self.fault_indicators)
        return d


class PredictionService:
    def __init__(self, bundle: ModelBundle):
        self._b = bundle

    @property
    def bundle(self) -> ModelBundle:
        return self._b

    @property
    def decision_method(self) -> str:
        return self._b.spec.name

    def _coerce(self, item: SensorReading | Mapping[str, Any]) -> SensorReading:
        return item if isinstance(item, SensorReading) else SensorReading.from_mapping(item)

    def predict(self, reading: SensorReading | Mapping[str, Any]) -> PredictionResult:
        return self.predict_many([reading])[0]

    def predict_many(self, readings: Sequence[SensorReading | Mapping[str, Any]]) -> list[PredictionResult]:
        """Vectorised, deterministic prediction. Invalid readings raise InvalidReadingError."""
        if not readings:
            return []
        items = [self._coerce(r) for r in readings]
        frame = pd.DataFrame([{k: getattr(r, k) for k in RAW_FEATURES} for r in items])
        X = self._b.scaler.transform(build_feature_frame(frame, self._b.feature_set))

        rf_p = self._b.rf.predict_proba(X)[:, 1]
        xgb_p = self._b.xgb.predict_proba(X)[:, 1]
        iso_flag = (self._b.iso.predict(X) == -1).astype(int)
        iso_score = -self._b.iso.score_samples(X)
        outputs = {"rf": rf_p, "xgb": xgb_p, "iso": iso_flag}
        final, _ = decide(self._b.spec, outputs)
        risk = risk_score(outputs)

        results = []
        for i, r in enumerate(items):
            is_fail = bool(final[i])
            fa = classify({k: getattr(r, k) for k in RAW_FEATURES}, r.product_type, is_fail)
            results.append(PredictionResult(
                final_status="FAILURE" if is_fail else "NORMAL", is_failure=is_fail,
                risk_score=float(risk[i]), rf_probability=float(rf_p[i]), xgb_probability=float(xgb_p[i]),
                isolation_anomaly_score=float(iso_score[i]),
                rf_prediction=int(rf_p[i] >= 0.5), xgb_prediction=int(xgb_p[i] >= 0.5),
                isolation_prediction=int(iso_flag[i]),
                fault_type=fa.fault_type, fault_codes=fa.codes, fault_indicators=fa.indicators,
                decision_method=self.decision_method, model_version=self._b.model_version,
            ))
        return results


_lock = threading.Lock()


@functools.lru_cache(maxsize=1)
def _cached_service() -> PredictionService:
    return PredictionService(load_bundle())


def get_prediction_service() -> PredictionService:
    """Process-wide singleton (models are loaded once)."""
    with _lock:
        return _cached_service()
