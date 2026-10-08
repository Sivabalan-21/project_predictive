"""Metrics, candidate evaluation, fault-rule validation and report plots."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Mapping, Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from sklearn.metrics import (accuracy_score, average_precision_score, brier_score_loss,  # noqa: E402
                             confusion_matrix, f1_score, matthews_corrcoef, precision_recall_curve,
                             precision_score, recall_score, roc_auc_score)

from ml.ensemble import EnsembleSpec, decide
from ml.fault_classification import fault_indicators
from ml.preprocessing import FAULT_LABEL_COLUMNS, RAW_FEATURES

log = logging.getLogger(__name__)


def binary_metrics(y_true: np.ndarray, pred: np.ndarray, score: np.ndarray | None) -> dict:
    """Standard binary metrics. ROC-AUC / PR-AUC only when a continuous score exists."""
    tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
    out = {
        "accuracy": float(accuracy_score(y_true, pred)),
        "precision": float(precision_score(y_true, pred, zero_division=0)),
        "recall": float(recall_score(y_true, pred, zero_division=0)),
        "f1": float(f1_score(y_true, pred, zero_division=0)),
        "mcc": float(matthews_corrcoef(y_true, pred)),
        "roc_auc": None, "pr_auc": None,
        "confusion_matrix": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
    }
    if score is not None and len(np.unique(score)) > 2:
        out["roc_auc"] = float(roc_auc_score(y_true, score))
        out["pr_auc"] = float(average_precision_score(y_true, score))
    return out


def tune_threshold(y_true: np.ndarray, score: np.ndarray) -> float:
    """Threshold maximising F1 on the supplied (out-of-fold) scores."""
    precision, recall, thresholds = precision_recall_curve(y_true, score)
    f1 = 2 * precision[:-1] * recall[:-1] / np.clip(precision[:-1] + recall[:-1], 1e-12, None)
    return float(thresholds[int(np.argmax(f1))])


def evaluate_specs(specs: Sequence[EnsembleSpec], outputs: Mapping[str, np.ndarray],
                   y_true: np.ndarray, fold_ids: np.ndarray | None = None) -> dict[str, dict]:
    """Metrics for every candidate. With ``fold_ids`` also reports per-fold F1 mean/std."""
    results: dict[str, dict] = {}
    for spec in specs:
        pred, score = decide(spec, outputs)
        m = binary_metrics(y_true, pred, score if spec.has_continuous_score else None)
        if fold_ids is not None:
            f1s = [f1_score(y_true[fold_ids == k], pred[fold_ids == k], zero_division=0)
                   for k in np.unique(fold_ids)]
            m["fold_f1_mean"], m["fold_f1_std"] = float(np.mean(f1s)), float(np.std(f1s))
        m["spec"] = spec.to_dict()
        results[spec.name] = m
    return results


def bootstrap_ci(y_true: np.ndarray, pred: np.ndarray, n: int, seed: int) -> dict:
    """95% percentile bootstrap CIs. Test sets with few failures give WIDE intervals."""
    rng = np.random.default_rng(seed)
    idx = np.arange(len(y_true))
    stats: dict[str, list[float]] = {"precision": [], "recall": [], "f1": []}
    for _ in range(n):
        s = rng.choice(idx, size=len(idx), replace=True)
        stats["precision"].append(precision_score(y_true[s], pred[s], zero_division=0))
        stats["recall"].append(recall_score(y_true[s], pred[s], zero_division=0))
        stats["f1"].append(f1_score(y_true[s], pred[s], zero_division=0))
    return {k: [float(np.percentile(v, 2.5)), float(np.percentile(v, 97.5))] for k, v in stats.items()} \
        | {"resamples": n}


def risk_score_report(y_true: np.ndarray, risk: np.ndarray) -> dict:
    """Quality of the (uncalibrated) risk score. Brier score is informational only."""
    return {
        "definition": "mean of RF and XGBoost failure probabilities",
        "calibrated": False,
        "brier_score": float(brier_score_loss(y_true, risk)),
        "roc_auc": float(roc_auc_score(y_true, risk)),
        "pr_auc": float(average_precision_score(y_true, risk)),
        "note": "Not a calibrated probability: no calibration step was fitted or evaluated.",
    }


def fault_rules_validation(df: pd.DataFrame) -> dict:
    """Compare each deterministic fault rule with the dataset's own fault labels."""
    flagged = {code: [] for code in ("TWF", "HDF", "PWF", "OSF")}
    for row in df[RAW_FEATURES + ["product_type"]].itertuples(index=False):
        reading = dict(zip(RAW_FEATURES, row[:5]))
        hits = fault_indicators(reading, row[5])
        for code in flagged:
            flagged[code].append(code in hits)
    out = {}
    for code, hits in flagged.items():
        hit = np.array(hits)
        lab = df[code].to_numpy() == 1
        tp, fp, fn = int((hit & lab).sum()), int((hit & ~lab).sum()), int((~hit & lab).sum())
        out[code] = {"tp": tp, "fp": fp, "fn": fn, "label_count": int(lab.sum()),
                     "precision": tp / (tp + fp) if tp + fp else None,
                     "recall": tp / (tp + fn) if tp + fn else None}
    out["RNF"] = {"label_count": int(df["RNF"].sum()), "note": "No sensor rule exists (random by definition)."}
    out["_scope"] = "Evaluated on the full synthetic dataset; rules are fixed from the dataset description, not fitted."
    return out


# ------------------------------------------------------------------ plots
def plot_confusion_matrices(panels: Sequence[tuple[str, dict]], path: Path) -> None:
    fig, axes = plt.subplots(1, len(panels), figsize=(4.2 * len(panels), 3.9))
    axes = np.atleast_1d(axes)
    for ax, (title, m) in zip(axes, panels):
        cm = m["confusion_matrix"]
        grid = np.array([[cm["tn"], cm["fp"]], [cm["fn"], cm["tp"]]])
        ax.imshow(grid, cmap="Blues")
        for (i, j), v in np.ndenumerate(grid):
            ax.text(j, i, str(v), ha="center", va="center", color="white" if v > grid.max() / 2 else "black")
        ax.set_xticks([0, 1], ["Normal", "Failure"])
        ax.set_yticks([0, 1], ["Normal", "Failure"])
        ax.set_xlabel("Predicted")
        ax.set_ylabel("Actual")
        ax.set_title(f"{title}\nP={m['precision']:.2f}  R={m['recall']:.2f}  F1={m['f1']:.2f}", fontsize=9)
    fig.suptitle("Held-out test set (never used for training or model selection)", fontsize=10)
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    plt.close(fig)


def plot_feature_importance(importances: Mapping[str, np.ndarray], names: Sequence[str], path: Path) -> None:
    fig, axes = plt.subplots(1, len(importances), figsize=(5.5 * len(importances), 3.8), sharey=True)
    axes = np.atleast_1d(axes)
    for ax, (title, values) in zip(axes, importances.items()):
        order = np.argsort(values)
        ax.barh(np.array(names)[order], np.asarray(values)[order], color="#2196F3")
        ax.set_title(title, fontsize=10)
        ax.set_xlabel("Importance (impurity/gain based)")
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    plt.close(fig)
