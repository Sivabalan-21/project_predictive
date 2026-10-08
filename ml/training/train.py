"""Reproducible training pipeline.  Run from the project root:

    python -m ml.training.train

Order of operations (no step ever sees data it should not):
  raw data -> validate -> stratified train/test split
  -> [on TRAIN only] 5-fold cross-validation: per fold, scaler fitted on the fold's training
     rows, SMOTE on the fold's training rows only, models trained, fold's validation rows scored
  -> model / feature-set / ensemble selection and threshold tuning from these OUT-OF-FOLD scores
  -> final fit on the full training set (scaler fitted on train only, SMOTE on train only)
  -> ONE evaluation on the untouched test set (reported, never used for any choice)
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import platform
import sys
from dataclasses import asdict
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
import xgboost
import imblearn
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import StratifiedKFold
from xgboost import XGBClassifier

from ml import config, persistence
from ml.ensemble import EnsembleSpec, decide, risk_score
from ml.preprocessing import FEATURE_SETS, TARGET_COLUMN
from ml.training import evaluate as ev
from ml.training.load_data import load_raw_dataset
from ml.training.preprocess import PreparedFold, prepare_training_data, split_dataset, transform_eval_data

log = logging.getLogger("ml.training.train")

N_TREES = 300  # Hyperparameters are reasonable defaults, NOT tuned (see docs/ml_pipeline.md).


# ------------------------------------------------------------------ models
def fit_models(prep: PreparedFold, seed: int) -> dict:
    pos_rate = float(prep.y_train.mean())
    scale_pos_weight = float((prep.y_train == 0).sum() / max((prep.y_train == 1).sum(), 1))
    rf_cw = RandomForestClassifier(n_estimators=N_TREES, class_weight="balanced", random_state=seed, n_jobs=-1)
    rf_cw.fit(prep.X_train, prep.y_train)
    rf_smote = RandomForestClassifier(n_estimators=N_TREES, random_state=seed, n_jobs=-1)
    rf_smote.fit(prep.X_train_smote, prep.y_train_smote)
    xgb = XGBClassifier(n_estimators=N_TREES, max_depth=4, learning_rate=0.1, subsample=0.8,
                        colsample_bytree=0.8, scale_pos_weight=scale_pos_weight, eval_metric="logloss",
                        tree_method="hist", random_state=seed, n_jobs=1, verbosity=0)
    xgb.fit(prep.X_train, prep.y_train)
    # Unsupervised: labels are used only to set the expected anomaly fraction (train prevalence).
    iso = IsolationForest(n_estimators=N_TREES, contamination=pos_rate, random_state=seed, n_jobs=-1)
    iso.fit(prep.X_train)
    return {"rf_cw": rf_cw, "rf_smote": rf_smote, "xgb": xgb, "iso": iso}


def component_outputs(models: dict, X: np.ndarray) -> dict[str, np.ndarray]:
    return {
        "rf_cw": models["rf_cw"].predict_proba(X)[:, 1],
        "rf_smote": models["rf_smote"].predict_proba(X)[:, 1],
        "xgb": models["xgb"].predict_proba(X)[:, 1],
        "iso": (models["iso"].predict(X) == -1).astype(int),
        "iso_score": -models["iso"].score_samples(X),  # higher = more anomalous
    }


def cross_validated_outputs(train: pd.DataFrame, feature_set: str, seed: int, folds: int):
    n = len(train)
    oof = {k: np.zeros(n) for k in ("rf_cw", "rf_smote", "xgb", "iso", "iso_score")}
    fold_ids = np.zeros(n, dtype=int)
    skf = StratifiedKFold(n_splits=folds, shuffle=True, random_state=seed)
    for k, (tr_idx, va_idx) in enumerate(skf.split(train, train[TARGET_COLUMN])):
        tr, va = train.iloc[tr_idx], train.iloc[va_idx]
        prep = prepare_training_data(tr, feature_set, seed)       # scaler + SMOTE: fold-train only
        models = fit_models(prep, seed)
        out = component_outputs(models, transform_eval_data(va, prep.scaler, feature_set))
        for key, values in out.items():
            oof[key][va_idx] = values
        fold_ids[va_idx] = k
        log.info("  [%s] fold %d/%d done", feature_set, k + 1, folds)
    return oof, fold_ids


# ------------------------------------------------------------------ selection
def reporting_specs() -> list[EnsembleSpec]:
    """The four models the project compares, each alone at its default decision rule."""
    return [
        EnsembleSpec("rf_class_weight", "single", ("rf_cw",), threshold=0.5, description="Random Forest, class_weight='balanced'"),
        EnsembleSpec("rf_smote", "single", ("rf_smote",), threshold=0.5, description="Random Forest on SMOTE-resampled training data"),
        EnsembleSpec("xgboost", "single", ("xgb",), threshold=0.5, description="XGBoost, scale_pos_weight"),
        EnsembleSpec("isolation_forest", "single", ("iso",), threshold=0.5, description="Isolation Forest anomaly flag"),
    ]


def deployable_specs(thr_rf: float, thr_xgb: float, thr_mean: float) -> list[EnsembleSpec]:
    """Candidates the prediction service can actually run ('rf' = the selected RF variant)."""
    t = {"rf": thr_rf, "xgb": thr_xgb}
    d = {"rf": 0.5, "xgb": 0.5}
    return [
        EnsembleSpec("rf_only", "single", ("rf",), threshold=0.5),
        EnsembleSpec("rf_only_tuned_threshold", "single", ("rf",), threshold=thr_rf),
        EnsembleSpec("xgb_only", "single", ("xgb",), threshold=0.5),
        EnsembleSpec("xgb_only_tuned_threshold", "single", ("xgb",), threshold=thr_xgb),
        EnsembleSpec("vote_rf_xgb_both", "vote", ("rf", "xgb"), d, min_votes=2,
                     description="failure only if RF and XGBoost both vote failure"),
        EnsembleSpec("vote_rf_xgb_either", "vote", ("rf", "xgb"), d, min_votes=1,
                     description="failure if RF or XGBoost votes failure"),
        EnsembleSpec("vote_rf_xgb_iso_2of3", "vote", ("rf", "xgb", "iso"), d, min_votes=2,
                     description="original project idea: 2-of-3 majority incl. Isolation Forest"),
        EnsembleSpec("vote_rf_xgb_iso_2of3_tuned_thresholds", "vote", ("rf", "xgb", "iso"), t, min_votes=2,
                     description="2-of-3 majority, RF/XGB thresholds tuned on out-of-fold scores"),
        EnsembleSpec("mean_prob_rf_xgb_t0.5", "prob_mean", ("rf", "xgb"), threshold=0.5,
                     description="mean(RF, XGB) probability >= 0.5"),
        EnsembleSpec("mean_prob_rf_xgb_tuned", "prob_mean", ("rf", "xgb"), threshold=thr_mean,
                     description="mean(RF, XGB) probability >= threshold tuned on out-of-fold scores"),
    ]


def select_from_oof(oof: dict, fold_ids: np.ndarray, y: np.ndarray) -> dict:
    """Pick RF variant, tune thresholds, evaluate candidates, choose the winner - OOF data only."""
    rf_variant = max(("rf_cw", "rf_smote"), key=lambda k: average_precision_score(y, oof[k]))
    outputs = {**oof, "rf": oof[rf_variant]}
    thr_rf = ev.tune_threshold(y, outputs["rf"])
    thr_xgb = ev.tune_threshold(y, outputs["xgb"])
    thr_mean = ev.tune_threshold(y, risk_score(outputs))
    specs = deployable_specs(thr_rf, thr_xgb, thr_mean)
    candidates = ev.evaluate_specs(specs, outputs, y, fold_ids)
    models = ev.evaluate_specs(reporting_specs(), outputs, y, fold_ids)
    _attach_iso_auc(models, y, oof["iso_score"])
    # Selection rule: highest pooled out-of-fold F1; ties broken by recall. Nothing else.
    best = max(candidates, key=lambda n: (round(candidates[n]["f1"], 12), candidates[n]["recall"]))
    return {"rf_variant": rf_variant, "thresholds": {"rf": thr_rf, "xgb": thr_xgb, "mean": thr_mean},
            "specs": specs, "candidates": candidates, "models": models, "selected": best}


def _attach_iso_auc(models: dict, y: np.ndarray, iso_score: np.ndarray) -> None:
    models["isolation_forest"]["roc_auc"] = float(roc_auc_score(y, iso_score))
    models["isolation_forest"]["pr_auc"] = float(average_precision_score(y, iso_score))
    models["isolation_forest"]["auc_source"] = "continuous anomaly score (not the 0/1 flag)"


# ------------------------------------------------------------------ main
def _versions() -> dict:
    return {"python": platform.python_version(), "scikit-learn": sklearn.__version__,
            "xgboost": xgboost.__version__, "imbalanced-learn": imblearn.__version__,
            "numpy": np.__version__, "pandas": pd.__version__, "joblib": joblib.__version__}


def _environment() -> dict:
    return {"os": platform.system(), "platform": platform.platform(), "machine": platform.machine(),
            "python_implementation": platform.python_implementation()}


def _compact(table: dict) -> dict:
    return {n: {k: m[k] for k in ("precision", "recall", "f1", "roc_auc", "pr_auc") if k in m} for n, m in table.items()}


def main(seed: int = config.SEED) -> dict:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config.ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    df, data_report = load_raw_dataset()
    split = split_dataset(df, config.TEST_SIZE, seed)
    log.info("Split: train=%d (failures=%d)  test=%d (failures=%d)", len(split.train),
             split.y_train.sum(), len(split.test), split.y_test.sum())

    # ---- 1. cross-validation on TRAIN only, for each candidate feature set
    per_set: dict[str, dict] = {}
    for fs in FEATURE_SETS:
        log.info("Cross-validating feature set %r", fs)
        oof, fold_ids = cross_validated_outputs(split.train, fs, seed, config.CV_FOLDS)
        per_set[fs] = {"oof": oof, "fold_ids": fold_ids, "sel": select_from_oof(oof, fold_ids, split.y_train)}
        best = per_set[fs]["sel"]
        log.info("  [%s] best OOF candidate: %s (F1=%.4f)", fs, best["selected"], best["candidates"][best["selected"]]["f1"])
    feature_set = max(per_set, key=lambda fs: per_set[fs]["sel"]["candidates"][per_set[fs]["sel"]["selected"]]["f1"])
    sel = per_set[feature_set]["sel"]
    spec: EnsembleSpec = next(s for s in sel["specs"] if s.name == sel["selected"])
    log.info("Selected feature set=%s  RF variant=%s  decision method=%s", feature_set, sel["rf_variant"], spec.name)

    # ---- 2. final fit on the full training set (scaler/SMOTE: train only)
    prep = prepare_training_data(split.train, feature_set, seed)
    models = fit_models(prep, seed)

    # ---- 3. one evaluation on the untouched test set
    X_test = transform_eval_data(split.test, prep.scaler, feature_set)
    raw_out = component_outputs(models, X_test)
    outputs = {**raw_out, "rf": raw_out[sel["rf_variant"]]}
    y_test = split.y_test
    test_models = ev.evaluate_specs(reporting_specs(), outputs, y_test)
    _attach_iso_auc(test_models, y_test, raw_out["iso_score"])
    test_candidates = ev.evaluate_specs(sel["specs"], outputs, y_test)
    final_pred, _ = decide(spec, outputs)
    final = dict(test_candidates[spec.name])
    final["bootstrap_95ci"] = ev.bootstrap_ci(y_test, final_pred, config.BOOTSTRAP_RESAMPLES, seed)
    risk = ev.risk_score_report(y_test, risk_score(outputs))

    # ---- 4. persist artifacts (only the selected RF variant is shipped), then PROVE they reload.
    # A manifest describes validated artifacts only: drop any stale one first, write the new one last.
    manifest_path = config.ARTIFACTS_DIR / config.MANIFEST_FILE
    manifest_path.unlink(missing_ok=True)
    files = {config.SCALER_FILE: prep.scaler, config.RF_FILE: models[sel["rf_variant"]],
             config.XGB_FILE: models["xgb"], config.ISO_FILE: models["iso"]}
    validation = persistence.save_and_validate(
        files, config.ARTIFACTS_DIR, probe_scaled=X_test, probe_raw=prep.scaler.inverse_transform(X_test))
    hashes = validation.pop("sha256")
    log.info("Artifacts saved and verified by reload (in-process + clean process): %s", sorted(hashes))
    manifest = {
        "trained_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "seed": seed, "feature_set": feature_set, "features": prep.feature_names,
        "rf_variant": sel["rf_variant"], "decision": spec.to_dict(),
        "risk_score": "mean of RF and XGBoost failure probabilities (NOT calibrated)",
        "dataset_sha256": data_report.sha256, "library_versions": _versions(),
        "environment": _environment(), "xgboost_serialization": persistence.XGBOOST_SERIALIZATION,
        "artifact_sha256": hashes, "validation": validation,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))

    # ---- 5. report + plots
    importances = {"Random Forest": models[sel["rf_variant"]].feature_importances_,
                   "XGBoost": models["xgb"].feature_importances_}
    ev.plot_feature_importance(importances, prep.feature_names, config.REPORTS_DIR / "feature_importance.png")
    ev.plot_confusion_matrices(
        [("Random Forest", test_candidates["rf_only"]), ("XGBoost", test_candidates["xgb_only"]),
         ("Isolation Forest", test_models["isolation_forest"]), (f"FINAL: {spec.name}", final)],
        config.REPORTS_DIR / "confusion_matrix.png")

    report = {
        "generated_at_utc": manifest["trained_at_utc"], "seed": seed,
        "pipeline_order": ["raw data", "validate", "stratified train/test split",
                           "5-fold CV on train only (scaler + SMOTE inside each fold)",
                           "selection + threshold tuning from out-of-fold scores",
                           "final fit on train (scaler + SMOTE on train only)",
                           "single evaluation on untouched test set"],
        "dataset": asdict(data_report),
        "split": {"test_size": config.TEST_SIZE, "train_rows": len(split.train), "test_rows": len(split.test),
                  "train_failures": int(split.y_train.sum()), "test_failures": int(y_test.sum())},
        "library_versions": _versions(),
        "feature_set": {"selected": feature_set, "features": prep.feature_names,
                        "cv_comparison": {fs: {"best_candidate": d["sel"]["selected"],
                                               "best_oof_f1": d["sel"]["candidates"][d["sel"]["selected"]]["f1"]}
                                          for fs, d in per_set.items()}},
        "selection": {"criterion": f"highest pooled out-of-fold F1 (ties: recall); {config.CV_FOLDS}-fold CV on train only",
                      "rf_variant": sel["rf_variant"], "rf_variant_criterion": "higher out-of-fold PR-AUC",
                      "tuned_thresholds": sel["thresholds"], "selected_decision_method": spec.name,
                      "decision_spec": spec.to_dict()},
        "cross_validation": {"models": sel["models"], "decision_candidates": sel["candidates"]},
        "test": {"models": test_models, "decision_candidates": test_candidates,
                 "selected": {"name": spec.name, **final}},
        "risk_score": risk,
        "fault_rules_validation": ev.fault_rules_validation(df),
        "limitations": [
            "Single dataset (synthetic AI4I 2020, 10,000 rows, ~3.4% failures): results do not transfer to real machines.",
            f"Test set has only {int(y_test.sum())} failures: metrics are noisy; see bootstrap_95ci.",
            "Hyperparameters were not tuned; thresholds were tuned on out-of-fold scores.",
            "risk_score is not calibrated.",
        ],
    }
    config.METRICS_PATH.write_text(json.dumps(report, indent=2))
    log.info("Wrote %s", config.METRICS_PATH)
    _print_summary(report)
    return report


def _print_summary(report: dict) -> None:
    t = report["test"]
    log.info("=== TEST SET (untouched) ===")
    for group in ("models", "decision_candidates"):
        for name, m in t[group].items():
            log.info("%-42s P=%.3f R=%.3f F1=%.3f AUC=%s", name, m["precision"], m["recall"], m["f1"],
                     "n/a" if m["roc_auc"] is None else f"{m['roc_auc']:.3f}")
    s = t["selected"]
    log.info("SELECTED: %s  P=%.3f R=%.3f F1=%.3f  (95%% CI F1 %.3f-%.3f)", s["name"], s["precision"], s["recall"],
             s["f1"], *s["bootstrap_95ci"]["f1"])


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
