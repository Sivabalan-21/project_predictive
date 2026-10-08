"""Ensemble decision methods.

One implementation used by BOTH model selection (training) and the prediction service,
so the decision rule that was evaluated is exactly the one that is deployed.

Component outputs (arrays, one entry per sample):
    rf   : failure probability from the selected Random Forest
    xgb  : failure probability from XGBoost
    iso  : 0/1 anomaly flag from Isolation Forest (1 = anomaly)

Kinds:
    single     one probability component compared with a threshold
    vote       each component votes (probability >= its threshold, or iso flag);
               failure when votes >= min_votes
    prob_mean  mean of the probability components compared with a threshold
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Mapping

import numpy as np

PROB_COMPONENTS = ("rf", "xgb")


@dataclass(frozen=True)
class EnsembleSpec:
    name: str
    kind: str                                   # "single" | "vote" | "prob_mean"
    components: tuple[str, ...]
    thresholds: Mapping[str, float] = field(default_factory=dict)  # per prob component
    min_votes: int = 1
    threshold: float = 0.5                      # used by "prob_mean" and "single"
    description: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d["components"] = list(self.components)
        d["thresholds"] = dict(self.thresholds)
        return d

    @staticmethod
    def from_dict(d: Mapping) -> "EnsembleSpec":
        return EnsembleSpec(
            name=d["name"], kind=d["kind"], components=tuple(d["components"]),
            thresholds=dict(d.get("thresholds", {})), min_votes=int(d.get("min_votes", 1)),
            threshold=float(d.get("threshold", 0.5)), description=d.get("description", ""),
        )

    @property
    def uses_isolation_forest(self) -> bool:
        return "iso" in self.components

    @property
    def has_continuous_score(self) -> bool:
        """True when ``score`` is a probability-like value usable for ROC-AUC."""
        return self.kind in ("single", "prob_mean")


def decide(spec: EnsembleSpec, outputs: Mapping[str, np.ndarray]) -> tuple[np.ndarray, np.ndarray]:
    """Apply ``spec``. Returns (prediction 0/1, score).

    score: probability for single/prob_mean; fraction of components voting failure for vote.
    """
    if spec.kind == "single":
        (comp,) = spec.components
        score = np.asarray(outputs[comp], dtype="float64")
        return (score >= spec.threshold).astype(int), score

    if spec.kind == "prob_mean":
        score = np.mean([np.asarray(outputs[c], dtype="float64") for c in spec.components], axis=0)
        return (score >= spec.threshold).astype(int), score

    if spec.kind == "vote":
        votes = []
        for comp in spec.components:
            values = np.asarray(outputs[comp], dtype="float64")
            if comp == "iso":
                votes.append(values.astype(int))
            else:
                votes.append((values >= spec.thresholds.get(comp, 0.5)).astype(int))
        total = np.sum(votes, axis=0)
        return (total >= spec.min_votes).astype(int), total / len(spec.components)

    raise ValueError(f"unknown ensemble kind {spec.kind!r}")


def risk_score(outputs: Mapping[str, np.ndarray]) -> np.ndarray:
    """Model-derived risk score = mean of the RF and XGBoost failure probabilities.

    NOT a calibrated probability or confidence: no calibration is applied. It is a
    monotone, bounded [0, 1] summary of how strongly the supervised models lean to failure.
    """
    return np.mean([np.asarray(outputs[c], dtype="float64") for c in PROB_COMPONENTS], axis=0)
