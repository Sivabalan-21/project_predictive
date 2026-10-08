# ML pipeline

Run:  `python -m ml.training.train`  (about 1.5 min on one CPU; deterministic, seed 42)

## Order of operations (no step sees data it should not)

```
raw CSV -> validate/clean -> stratified 80/20 split
  -> 5-fold CV on TRAIN only (per fold: scaler fit on fold-train, SMOTE on fold-train)
  -> choose feature set, RF variant, thresholds, decision method from OUT-OF-FOLD scores
  -> final fit on all TRAIN (scaler + SMOTE on train only)
  -> ONE evaluation on the untouched test set
```

Data facts: 10,000 rows, 339 failures (3.39%), 0 duplicates, 0 missing. 27 rows have `Machine failure`
disagreeing with the five fault flags (9 failures with no flag, 18 flags with no failure); labels are
reported, not altered.

## Models compared

| Name | Description |
|---|---|
| `rf_class_weight` | Random Forest, `class_weight="balanced"` |
| `rf_smote` | Random Forest on SMOTE-resampled training data |
| `xgboost` | XGBoost, `scale_pos_weight` = negatives/positives |
| `isolation_forest` | Unsupervised; `contamination` = training failure rate; AUC from continuous anomaly score |

Hyperparameters are reasonable defaults (300 trees, XGB depth 4, lr 0.1); **they were not tuned.**

## Features

Input schema is the 5 sensors. The feature-set choice was made by cross-validation on training data:

| Feature set | Best out-of-fold F1 |
|---|---|
| `raw` (5 sensors) | 0.722 |
| `engineered` (+ `temp_diff`, `power_w`, `wear_torque`) | 0.847 |

The engineered features are the quantities the AI4I failure modes are defined on (temperature difference,
mechanical power in W, wear x torque). Computed in `ml/preprocessing.py`, shared by training and serving.

## Decision method (final)

Candidates compared (all in `reports/model_metrics.json`): each model alone (default and tuned threshold),
RF AND XGB, RF OR XGB, 2-of-3 vote with Isolation Forest (default and tuned thresholds), mean-probability
(0.5 and tuned). **Rule: highest pooled out-of-fold F1; ties broken by recall.**

Selected: **`rf_only_tuned_threshold`**: Random Forest (class weights) failure probability >= 0.36
(threshold tuned on out-of-fold scores).

Important reading of this result:
* The candidates' cross-validated F1 values lie within about one fold standard deviation (about 0.04) of each
  other (0.79-0.85). The selection rule picked RF-only, but multi-model ensembles are **statistically
  indistinguishable** from it on this data.
* The original 2-of-3 majority vote with default thresholds scored no better than RF alone, and Isolation
  Forest alone is weak (F1 about 0.32). It adds little as a voter.
* All three models still run for every prediction; their outputs are returned and stored for transparency.

## Risk score

`risk_score = mean(RF probability, XGBoost probability)`. Model-derived, in [0, 1], **not calibrated** (no
calibration was fitted or evaluated). Brier score is reported for information only. Because the decision
uses RF at threshold 0.36, a FAILURE can have a risk score below 0.5.

## Fault classification

Separate from the failure models (`ml/fault_classification.py`). Rules follow the AI4I 2020 description:

| Code | Rule | Basis | Agreement with dataset labels |
|---|---|---|---|
| HDF | (process - air) < 8.6 K AND rpm < 1380 | dataset definition | exact (115/115, 0 FP) |
| PWF | power = torque x rpm x 2pi/60 outside [3500, 9000] W | dataset definition | exact (95/95, 0 FP) |
| OSF | wear x torque > 11000 (L) / 12000 (M) / 13000 (H) | dataset definition | exact (98/98, 0 FP) |
| TWF | tool_wear >= 200 min | dataset definition (random failure inside 200-240) | recall 98%, precision 6%: flags **risk**, not failure |
| RNF | none (random by definition) | n/a | residual label only when a failure is flagged and no rule matches (assumption) |

Exact agreement reflects how the **synthetic** dataset was generated; it does not validate the thresholds on
real machines. If no `product_type` is supplied, OSF uses the lowest (L) limit (engineering assumption).

## Evaluation caveats

* Test set has 68 failures; see `bootstrap_95ci` in the report (F1 interval about 0.79-0.92).
* Threshold tuning used the same out-of-fold scores as method selection (mild optimism for tuned candidates in
  the CV table); the test set is the unbiased estimate.
* Single synthetic dataset: results do not transfer to real machines.

## Serving

`ml/prediction/prediction_service.py` is the only prediction implementation. The loader verifies SHA-256
hashes from `ml/artifacts/manifest.json` and warns on scikit-learn/xgboost version mismatch. Pickle/joblib
files can execute code when loaded: only load artifacts you trained yourself.

## Artifacts, portability and round-trip validation

`python -m ml.training.train` saves four joblib artifacts and then **proves they reload before it writes
`manifest.json`** (`ml/persistence.py: save_and_validate`). For each artifact it checks that (1) the file holds no
platform-dependent bytes, (2) `joblib.load` in the same process returns the same type and bit-identical outputs on
a 2,000-row probe, and (3) the same holds in a clean child interpreter. Any failure raises
`ArtifactValidationError`, and a stale manifest is deleted first so an unvalidated artifact set can never look valid.

**Why XGBoost is not saved with a plain `joblib.dump`.** Pickling an `xgboost.Booster` stores the training
configuration, including `rng_state`, a *text dump of a C++ `std::mt19937`* (XGBoost `src/common/random.cc`:
`ss << std::hex << rng` / `ss >> std::hex >> rng`). libstdc++ (Linux) always writes decimal; MSVC STL (Windows)
honours `std::hex`. A model pickled on Linux and loaded on Windows therefore fails with
`xgboost._c_api.XGBoostError: input stream corrupted` even though the file is byte-identical. Any model trained
with `subsample` / `colsample_*` < 1 (this one: 0.8 / 0.8) is affected. The pipeline now stores the booster as
XGBoost's portable UBJSON model (`Booster.save_raw("ubj")`) inside the joblib file; `xgboost.joblib` still loads
with a plain `joblib.load` into an `XGBClassifier`, with no project imports required.
